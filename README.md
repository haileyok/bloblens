# bloblens

Tracks similar avatars and banners across Bluesky by hashing profile images and comparing them.

## What it does

Listens to the ATProto firehose for profile updates, hashes any avatar/banner images using PDQ perceptual hashing, and stores them in a vector database. When it finds multiple profiles using similar images, it logs them.

Useful for finding coordinated behavior, tracking ban evasion, or whatever else

## Setup

You'll need three things running:
- **bloblens** (this)
- **Qdrant** - vector database (stores and searches hashes)

### With Docker Compose

```bash
docker-compose up -d
```

That's it. It'll create the Qdrant collection automatically on first run.

### Manual

Build:
```bash
go build -o bloblens ./cmd/bloblens
```

Run:
```bash
./bloblens \
  --websocket-host=wss://jetstream.atproto.tools/subscribe \
  --qdrant-host=localhost \
  --qdrant-port=6334 \
  --qdrant-collection=blobs \
  --max-hamming-distance=31 \
  --seen-threshold=5
```

## Configuration

| Flag | Description | Default |
|------|-------------|---------|
| `--websocket-host` | ATProto firehose URL | required |
| `--qdrant-host` | Qdrant server hostname | required |
| `--qdrant-port` | Qdrant gRPC port | required |
| `--qdrant-collection` | Collection name for storing hashes | required |
| `--max-hamming-distance` | Max distance to consider images "similar" | 31 |
| `--seen-threshold` | How many matches before logging | required |
| `--max-limit` | Max results to fetch from Qdrant per search | required |
| `--max-search-time` | Timeout for vector searches | required |


## How it works

1. Watches the firehose for `app.bsky.actor.profile` records
2. Extracts avatar and banner blob refs
4. Stores the 256-dimensional vector in Qdrant
5. Searches Qdrant for similar existing hashes
6. Logs when threshold is exceeded
