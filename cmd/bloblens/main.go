package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/haileyok/bloblens/server"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "bloblens",
		Usage: "a lil guy that tracks similar avatars and banners in app.bsky.actor.profile records",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			&cli.StringFlag{
				Name:  "retina-host",
				Usage: "host of the retina hashing server",
			},
			&cli.StringFlag{
				Name:  "websocket-host",
				Usage: "host for the atproto firehose",
			},
			&cli.DurationFlag{
				Name:  "max-search-time",
				Usage: "max search time",
			},
			&cli.IntFlag{
				Name:  "max-limit",
				Usage: "the most hashes to look for when searching a hash",
			},
			&cli.IntFlag{
				Name:  "seen-threshold",
				Usage: "the number of similar matches that will trigger a log",
			},
			&cli.Float64Flag{
				Name:  "max-hamming-distance",
				Usage: "the max hamming distance that is considered similar",
				Value: 31, // PDQ suggested default
			},
			&cli.StringFlag{
				Name:  "qdrant-host",
				Usage: "host for the qdrant grpc server without port",
			},
			&cli.IntFlag{
				Name:  "qdrant-port",
				Usage: "port for the qdrant grpc server",
			},
			&cli.StringFlag{
				Name:  "qdrant-collection",
				Usage: "name of the collection hashes are stored inside of",
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cli.Context) error {
	ctx := context.Background()

	logger := telemetry.StartLogger(cmd)

	server, err := server.New(ctx, &server.Args{
		Logger:             logger,
		RetinaHost:         cmd.String("retina-host"),
		WebsocketHost:      cmd.String("websocket-host"),
		MaxSearchTime:      cmd.Duration("max-search-time"),
		MaxLimit:           cmd.Int("max-limit"),
		SeenThreshold:      cmd.Int("seen-threshold"),
		MaxHammingDistance: cmd.Float64("max-hamming-distance"),
		QdrantHost:         cmd.String("qdrant-host"),
		QdrantPort:         cmd.Int("qdrant-port"),
		QdrantColletion:    cmd.String("qdrant-collection"),
	})
	if err != nil {
		return fmt.Errorf("failed to create new server: %w", err)
	}

	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("failed to run server: %w", err)
	}

	return nil
}
