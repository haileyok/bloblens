package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

func (s *Server) handleProfile(ctx context.Context, did string, profile *bsky.ActorProfile) {
	logger := s.logger.With("name", "handleProfile", "did", did)
	searchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var avatarCid string
	if profile.Avatar != nil {
		avatarCid = profile.Avatar.Ref.String()
	}

	var bannerCid string
	if profile.Banner != nil {
		bannerCid = profile.Banner.Ref.String()
	}

	if avatarCid == "" && bannerCid == "" {
		return
	}

	var avatarHash string
	var bannerHash string
	getHashResult := func(kind, cid string) {
		logger := logger.With("kind", kind, "cid", cid)
		hashResult, err := s.getImageHash(searchCtx, did, cid)
		if err != nil {
			logger.Error("failed to get hash result", "err", err)
			return
		}
		if hashResult.QualityTooLow {
			logger.Info("quality too low")
			return
		}
		if hashResult.Hash == nil {
			logger.Warn("nil hash")
			return
		}

		switch kind {
		case "avatar":
			avatarHash = *hashResult.Hash
		case "banner":
			bannerHash = *hashResult.Hash
		}
	}

	var wg sync.WaitGroup
	if avatarCid != "" {
		wg.Go(func() {
			getHashResult("avatar", avatarCid)
		})
	}
	if bannerCid != "" {
		wg.Go(func() {
			getHashResult("banner", bannerCid)
		})
	}
	wg.Wait()

	var avatarVector []float32
	var bannerVector []float32
	if avatarHash != "" {
		vec, err := convertHash(avatarHash)
		if err != nil {
			logger.Error("error getting vector", "err", err)
		} else {
			avatarVector = vec
		}
	}
	if bannerHash != "" {
		vec, err := convertHash(bannerHash)
		if err != nil {
			logger.Error("error getting vector", "err", err)
		} else {
			bannerVector = vec
		}
	}

	now := time.Now().UTC().UnixMilli()
	nowF64 := float64(now)
	startRange := float64(now - s.maxSearchTime.Milliseconds())

	var similarAvatarDids []string
	var similarBannerDids []string

	if avatarVector != nil {
		wg.Go(func() {
			query := &qdrant.QueryPoints{
				CollectionName: s.qdrantCollection,
				Query:          qdrant.NewQueryDense(avatarVector),
				Filter: &qdrant.Filter{
					Must: []*qdrant.Condition{
						qdrant.NewRange("timestamp", &qdrant.Range{
							Lte: &nowF64,
							Gte: &startRange,
						}),
						qdrant.NewMatch("kind", "avatar"),
					},
				},
				WithPayload:    qdrant.NewWithPayload(true),
				Limit:          qdrant.PtrOf(uint64(s.maxLimit)),
				ScoreThreshold: qdrant.PtrOf(s.minEuclidianDistance),
			}

			results, err := s.dbClient.Query(searchCtx, query)
			if err != nil {
				logger.Error("failed to search for vectors", "err", err)
				return
			}

			seenDids := make(map[string]bool)
			for _, r := range results {
				rDidVal, ok := r.Payload["did"]
				if !ok {
					continue
				}
				rDid := rDidVal.GetStringValue()
				if !seenDids[rDid] {
					seenDids[rDid] = true
					similarAvatarDids = append(similarAvatarDids, rDid)
				}
			}
		})
	}

	if bannerVector != nil {
		wg.Go(func() {
			query := &qdrant.QueryPoints{
				CollectionName: s.qdrantCollection,
				Query:          qdrant.NewQueryDense(bannerVector),
				Filter: &qdrant.Filter{
					Must: []*qdrant.Condition{
						qdrant.NewRange("timestamp", &qdrant.Range{
							Lte: &nowF64,
							Gte: &startRange,
						}),
						qdrant.NewMatch("kind", "banner"),
					},
				},
				WithPayload:    qdrant.NewWithPayload(true),
				Limit:          qdrant.PtrOf(uint64(s.maxLimit)),
				ScoreThreshold: qdrant.PtrOf(s.minEuclidianDistance),
			}

			results, err := s.dbClient.Query(searchCtx, query)
			if err != nil {
				logger.Error("failed to search for vectors", "err", err)
				return
			}

			seenDids := make(map[string]bool)
			for _, r := range results {
				rDidVal, ok := r.Payload["did"]
				if !ok {
					continue
				}
				rDid := rDidVal.GetStringValue()
				if !seenDids[rDid] {
					seenDids[rDid] = true
					similarBannerDids = append(similarBannerDids, rDid)
				}
			}
		})
	}

	wg.Wait()

	// inserts can happen in a different goroutine so we don't block returning these results
	go func() {
		insertCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		points := make([]*qdrant.PointStruct, 0, 2)
		if avatarVector != nil {
			point := &qdrant.PointStruct{
				Id:      qdrant.NewIDUUID(uuid.NewString()),
				Vectors: qdrant.NewVectors(avatarVector...),
				Payload: qdrant.NewValueMap(map[string]any{
					"did":       did,
					"cid":       avatarCid,
					"kind":      "avatar",
					"timestamp": now,
				}),
			}
			points = append(points, point)
		}
		if bannerVector != nil {
			point := &qdrant.PointStruct{
				Id:      qdrant.NewIDUUID(uuid.NewString()),
				Vectors: qdrant.NewVectors(bannerVector...),
				Payload: qdrant.NewValueMap(map[string]any{
					"did":       did,
					"cid":       bannerCid,
					"kind":      "banner",
					"timestamp": now,
				}),
			}
			points = append(points, point)
		}

		if len(points) > 0 {
			if _, err := s.dbClient.Upsert(insertCtx, &qdrant.UpsertPoints{
				CollectionName: s.qdrantCollection,
				Points:         points,
			}); err != nil {
				logger.Error("failed to insert hashes", "err", err)
			}
		}
	}()

	if len(similarAvatarDids) >= s.seenThreshold {
		logger = logger.With("kind", "avatar", "did", did, "cid", avatarCid, "hash", avatarHash)
		logger.Info("found users with similar avatars within search duration", "dids", similarAvatarDids)
	}
	if len(similarBannerDids) >= s.seenThreshold {
		logger = logger.With("kind", "banner", "did", did, "cid", avatarCid, "hash", bannerHash)
		logger.Info("found users with similar banners within search duration", "dids", similarBannerDids)
	}
}

func convertHash(hash string) ([]float32, error) {
	hashBin, err := HexToBinary(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to binary: %w", err)
	}
	return BinaryToFloatVector(hashBin), nil
}

func HexToBinary(input string) ([]byte, error) {
	hashb, err := hex.DecodeString(input)
	if err != nil {
		return nil, err
	}
	return hashb, nil
}

func BinaryToFloatVector(bin []byte) []float32 {
	vectorData := make([]float32, len(bin)*8)
	for i, b := range bin {
		for j := range 8 {
			if (b>>(7-j))&1 == 1 {
				vectorData[i*8+j] = 1.0
			} else {
				vectorData[i*8+j] = 0.0
			}
		}
	}
	return vectorData
}
