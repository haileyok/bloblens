package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gorilla/websocket"
	"github.com/qdrant/go-client/qdrant"
)

const (
	pdqHashSize = 256
)

type Server struct {
	logger *slog.Logger

	dbClient   *qdrant.Client
	httpClient *http.Client

	retinaHost    string
	websocketHost string

	qdrantCollection string

	minEuclidianDistance float32

	maxSearchTime time.Duration
	maxLimit      int
	seenThreshold int
}

type Args struct {
	Logger             *slog.Logger
	RetinaHost         string
	WebsocketHost      string
	MaxSearchTime      time.Duration
	MaxLimit           int
	SeenThreshold      int
	MaxHammingDistance float64

	QdrantHost      string
	QdrantPort      int
	QdrantColletion string
}

func New(ctx context.Context, args *Args) (*Server, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if args.Logger != nil {
		args.Logger = slog.Default()
	}

	qdrantConfig := qdrant.Config{
		Host: args.QdrantHost,
		Port: args.QdrantPort,
	}
	dbClient, err := qdrant.NewClient(&qdrantConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create qdrant client: %w", err)
	}

	exists, err := dbClient.CollectionExists(ctx, args.QdrantColletion)
	if err != nil {
		return nil, fmt.Errorf("failed to check if collection exists: %w", err)
	}

	if !exists {
		args.Logger.Info("collection does not exist, creating", "collection", args.QdrantColletion)

		if err := dbClient.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: args.QdrantColletion,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     pdqHashSize,
				Distance: qdrant.Distance_Euclid,
			}),
		}); err != nil {
			return nil, fmt.Errorf("failed to create flagged image collection: %w", err)
		}

		if _, err := dbClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: args.QdrantColletion,
			FieldName:      "timestamp",
			FieldType:      qdrant.PtrOf(qdrant.FieldType_FieldTypeInteger),
		}); err != nil {
			return nil, fmt.Errorf("failed to create flagged image collection timestamp index: %w", err)
		}

		args.Logger.Info("Successfully created flagged image vector collection in Milvus")

	} else {
		args.Logger.Info("collection already exists", "collection", args.QdrantColletion)
	}

	server := Server{
		retinaHost:       args.RetinaHost,
		websocketHost:    args.WebsocketHost,
		qdrantCollection: args.QdrantColletion,
		maxSearchTime:    args.MaxSearchTime,
		maxLimit:         args.MaxLimit,
		seenThreshold:    args.SeenThreshold,

		// PDQ distance should be measured with hamming distance. Because Qdrant doesn't support this (?)
		// we'll use the sqrt of the given hamming distance as the euclidean distance we search for
		minEuclidianDistance: float32(math.Sqrt(args.MaxHammingDistance)),

		logger: args.Logger,

		dbClient: dbClient,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}

	return &server, nil
}

func (s *Server) Run(ctx context.Context) error {
	logger := s.logger.With("name", "Run")

	wsDialer := websocket.DefaultDialer
	u, err := url.Parse(s.websocketHost)
	if err != nil {
		return fmt.Errorf("failed to parse websocket host: %w", err)
	}

	// run the consumer in a goroutine and wait for close
	shutdownConsumer := make(chan struct{}, 1)
	consumerShutdown := make(chan struct{}, 1)
	consumerErr := make(chan error, 1)
	go func() {
		logger := s.logger.With("component", "consumer")

		logger.Info("subscribing to repo event stream", "url", u.String())

		// dial the websocket
		conn, _, err := wsDialer.Dial(u.String(), http.Header{
			"User-Agent": []string{"bloblens/0.0.0"},
		})
		if err != nil {
			logger.Error("error dialing websocket", "err", err)
			close(shutdownConsumer)
			return
		}

		// setup a new event scheduler
		parallelism := 400

		scheduler := parallel.NewScheduler(parallelism, 1000, s.websocketHost, s.handleEvent)

		// run the consumer and wait for it to be shut down
		go func() {
			if err := events.HandleRepoStream(ctx, conn, scheduler, logger); err != nil {
				logger.Error("error handling repo stream", "err", err)
				consumerErr <- err
				return
			}

			consumerErr <- nil
		}()

		select {
		case <-shutdownConsumer:
		case err := <-consumerErr:
			if err != nil {
				logger.Error("consumer error", "err", err)
			}
		}

		if err := conn.Close(); err != nil {
			logger.Error("error closing websocket", "err", err)
		} else {
			logger.Info("websocket closed")
		}

		close(consumerShutdown)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	// wait for any of the following to arise
	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
		close(shutdownConsumer)
	case <-ctx.Done():
		logger.Info("main context cancelled")
		close(shutdownConsumer)
	case <-consumerShutdown:
		logger.Warn("consumer shutdown unexpectedly, forcing exit")
	}

	select {
	case <-consumerShutdown:
	case <-time.After(5 * time.Second):
		logger.Warn("websocket did not shut down within five seconds, forcefully shutting down")
	}

	logger.Info("shutdown successfully")

	return nil
}

func (s *Server) handleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	logger := s.logger.With("name", "handleEvent")

	if evt.RepoCommit == nil {
		return nil
	}

	logger = logger.With("did", evt.RepoCommit.Repo)

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.RepoCommit.Blocks))
	if err != nil {
		logger.Error("failed to read repo from car", "did", evt.RepoCommit.Repo, "err", err)
		return nil
	}

	for _, op := range evt.RepoCommit.Ops {
		if op.Action != "create" && op.Action != "update" {
			continue
		}

		pts := strings.Split(op.Path, "/")
		if len(pts) != 2 {
			continue
		}

		collection := pts[0]

		if collection != "app.bsky.actor.profile" {
			continue
		}

		rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
		if err != nil {
			logger.Error("failed to read record bytes", "err", err)
			continue
		}

		recCid := rcid.String()
		if recCid != op.Cid.String() {
			logger.Error("record cid mismatch", "expected", *op.Cid, "actual", recCid)
			continue
		}

		var profile bsky.ActorProfile
		profile.UnmarshalCBOR(bytes.NewReader(*recB))

		go func() {
			s.handleProfile(ctx, evt.RepoCommit.Repo, &profile)
		}()
	}

	return nil
}
