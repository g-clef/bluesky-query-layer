# bluesky-query-layer

A query layer that exposes archived Bluesky post data to LLM agents. Accepts arbitrary SQL against Parquet files stored in MinIO and returns JSON rows, via both a REST API and an MCP server interface.

## Architecture

- **FastAPI service** — K8s Deployment that connects to the existing homelab Ray cluster on startup, creates a named `DuckDBQueryActor` on a Ray worker, and serves HTTP on port 8000.
- **DuckDB Ray Actor** — runs on the existing `homelab-compute` Ray cluster, owns the DuckDB connection and MinIO/httpfs configuration, executes all SQL.

Data source: `s3://bluesky-data/year=YYYY/month=MM/day=DD/hour=HH/*.parquet` with Hive-style partition pruning.

## API

### REST

```
POST /query
Content-Type: application/json

{"sql": "SELECT did, COUNT(*) FROM posts WHERE year=2025 AND month=1 GROUP BY did"}
```

Response: `{"rows": [{"did": "...", "count_star()": 42}, ...]}`

HTTP status codes: 200 ok, 400 invalid SQL, 408 query timeout, 503 S3 or Ray unavailable.

### MCP (JSON-RPC 2.0)

```
POST /mcp
Content-Type: application/json
```

Implements MCP protocol version `2024-11-05` with a single tool: `query_posts(sql: string)`.

Supported methods: `initialize`, `tools/list`, `tools/call`.

### Health

```
GET /health  →  {"status": "ok"}
```

## Schema

Table name: `posts`

| Column | Type | Notes |
|---|---|---|
| `collection_timestamp` | timestamp | When the event was collected |
| `event_timestamp` | timestamp | When the event occurred |
| `did` | varchar | Bluesky DID of the author |
| `event_type` | varchar | |
| `collection` | varchar | |
| `action` | varchar | |
| `record_text` | varchar | Post text |
| `reply_parent` | varchar | |
| `reply_root` | varchar | |
| `embed_type` | varchar | |
| `raw_event` | varchar | Raw event JSON |

Hive partition columns usable in `WHERE` for scan pruning: `year`, `month`, `day`, `hour` (all int).

## Development

Requires Python 3.11 and a virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```bash
pytest
```

The test suite (37 tests) covers the actor, MCP handler, FastAPI service, and end-to-end integration against fixture Parquet files — no live Ray cluster or MinIO required.

## Deployment

### Environment variables

| Variable | Source | Description |
|---|---|---|
| `RAY_ADDRESS` | hardcoded in deployment.yaml | Ray cluster head address |
| `PARQUET_GLOB` | hardcoded in deployment.yaml | S3 glob for Parquet files |
| `S3_ENDPOINT` | `bluesky-query-minio-secret` | MinIO endpoint |
| `S3_ACCESS_KEY` | `bluesky-query-minio-secret` | MinIO access key |
| `S3_SECRET_KEY` | `bluesky-query-minio-secret` | MinIO secret key |

Create the secret before first sync:

```bash
kubectl create secret generic bluesky-query-minio-secret \
  --namespace bluesky-query \
  --from-literal=S3_ENDPOINT=<minio-host:port> \
  --from-literal=S3_ACCESS_KEY=<key> \
  --from-literal=S3_SECRET_KEY=<secret>
```

### ArgoCD

Follows the App of Apps pattern. Register the parent app with your ArgoCD instance:

```bash
kubectl apply -f argocd/apps/bluesky-query.yaml -n argocd
```

Sync waves: parent (-2) → namespace (-1) → service (0).

### Docker image

```
gclef/bluesky-query-service:latest
```