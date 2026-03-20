# ekeid

*Pronounced "eek-id" — from the archaic "ekename" (an additional name), the word that eventually became "nickname".*

A self-hosted service that builds and maintains a local cache of external identifier mappings from [Wikidata](https://www.wikidata.org/). Look up any entity by one external ID (IMDb, TMDB, MusicBrainz, etc.) and get back its Wikidata QID along with all other known external IDs for that entity.

## Why

Wikidata stores millions of cross-references between external identifiers — for example, a single movie entity might link its IMDb ID, TMDB ID, Rotten Tomatoes slug, and dozens more. Querying these mappings live via the Wikidata API or SPARQL endpoint can be slow and rate-limited. ekeid solves this by maintaining a local SQLite database that is seeded from a full Wikidata dump and kept up to date in real time via the Wikimedia EventStreams API.

## Architecture

ekeid consists of two services:

- **Watcher** — Seeds the database from the latest Wikidata JSON dump (gzip or bzip2), then subscribes to the [Wikimedia EventStreams](https://stream.wikimedia.org/) SSE feed to process entity changes in real time. Changed entities are enqueued and fetched from the Wikidata API in batches. Failed fetches are retried automatically with backoff.
- **API** — A read-only HTTP server that serves lookup queries against the local database. Includes per-IP rate limiting, CORS support, health checks, and statistics.

Both services share a single SQLite database file (`ekeid.db`) using WAL mode. The watcher writes; the API reads.

## API Endpoints

All endpoints are prefixed with `/v1`.

### Lookup by External ID

```
GET /v1/lookup/{property}/{value}
```

Look up an entity by a Wikidata property and value. The property must be in `P` format (e.g. `P345` for IMDb ID).

**Example** — Find the entity with IMDb ID `tt0111161`:

```bash
curl http://localhost:8080/v1/lookup/P345/tt0111161
```

```json
{
  "wikidata_id": "Q172241",
  "mappings": {
    "P345": ["tt0111161"],
    "P4947": ["278"],
    "P4835": ["2095"],
    "P8013": ["the-shawshank-redemption"]
  }
}
```

### Lookup by Wikidata ID

```
GET /v1/lookup/{qid}
```

Look up all external ID mappings for a Wikidata entity.

**Example:**

```bash
curl http://localhost:8080/v1/lookup/Q172241
```

### Health

```
GET /v1/health
```

Returns service status, database size, dump time, and last event sync time. Suitable for monitoring and liveness probes.

### Statistics

```
GET /v1/stats
```

Returns mapping, pending, and failed entity counts along with database metadata. This endpoint executes `COUNT` queries and is more expensive than `/v1/health`.

> **Note:** If exposing the API publicly, consider restricting access to `/v1/stats` (and potentially `/v1/health`) to operators only — for example, via a reverse proxy. The stats endpoint is expensive and the operational details it exposes are generally not relevant to end users.

## Getting Started

### Prerequisites

- An OCI-compatible container runtime (e.g. [Podman](https://podman.io/) or [Docker](https://docs.docker.com/get-docker/)) with Compose support

### Configuration

Configuration is done through environment variables:

| Variable | Service | Default | Description |
|---|---|---|---|
| `CONTACT` | Watcher | *(required)* | URL or email for the [Wikimedia User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy) |
| `DB_PATH` | Both | `/data/ekeid.db` | Path to the SQLite database file |
| `DUMP_FORMAT` | Watcher | `gz` | Dump compression format: `gz` (~150 GB) or `bz2` (~100 GB) |
| `LISTEN_ADDR` | API | `:8080` | Address for the API server to listen on |
| `RATE_LIMIT` | API | `10` | Token bucket refill rate (requests per second per IP) |
| `RATE_BURST` | API | `20` | Token bucket capacity (max burst per IP) |

### Running

1. Set the required `CONTACT` variable:

   ```bash
   export CONTACT="you@example.com"  # or a URL, per Wikimedia policy
   ```

2. Start both services:

   ```bash
   docker compose up -d
   ```

On first start the watcher will download and process the latest Wikidata dump. This is a large download (100+ GB compressed) and initial seeding will take a significant amount of time. Once seeded, the watcher switches to real-time streaming and subsequent restarts will not re-download the dump unless it is deemed necessary.

The API will be available at `http://localhost:8080`.

### Building from Source

Requires Go 1.26+.

```bash
# Build the API server
go build -o api-server ./cmd/api

# Build the watcher
go build -o watcher ./cmd/watcher
```

## Running Tests

```bash
go test ./...
```

## How It Works

1. **Seeding** — The watcher downloads the latest `latest-all.json.gz` (or `.bz2`) dump from [Wikidata](https://dumps.wikimedia.org/wikidatawiki/entities/). It streams and decompresses the dump, extracting all external-id claims for every entity. These are inserted into the SQLite `mapping` table in batches. The dump's ETag is tracked to support resumable downloads if the connection is interrupted.

2. **Streaming** — After seeding completes, the watcher connects to the [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange) SSE endpoint starting from slightly before the dump timestamp. Wikidata entity edits (namespace 0) are collected, deduplicated, and enqueued into a `pending_entity` table. A background goroutine drains this queue by fetching entities from the Wikidata `wbgetentities` API in batches of up to 50 and upserting their external ID mappings.

3. **Resilience** — If an entity fetch fails, it is recorded in a `failed_entity` table and retried periodically with backoff. If the EventStream connection drops, the watcher reconnects automatically. If the stream cannot go far enough back in time to cover the last sync point, a full reseed is triggered.

4. **Serving** — The API server opens the same database in read-only mode and serves lookup queries. Responses are cached for 1 hour (`Cache-Control: public, max-age=3600`). Per-IP rate limiting uses a token bucket algorithm.

## Technology

- **Go** — Standard library HTTP server and SSE client, no web framework
- **SQLite** (via [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) — Pure-Go SQLite driver, no CGO required

## License

This project is released into the public domain under [CC0 1.0 Universal](LICENSE).

## Disclosure

AI coding agents (GitHub Copilot) were used in the development of this project.
