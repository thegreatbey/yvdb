# yvdb (learning vector DB)

Small, educational vector database: single-node, in-memory store with append-only durability and brute-force top-k search.

## Run

```bash
# Windows PowerShell
# from repo root
$env:RUST_LOG="info"; cargo run
```

Server listens on `127.0.0.1:8080`.

## HTTP API

- POST `/collections/{name}/upsert`

Request
```json
{
  "dimension": 3,
  "metric": "cosine",
  "records": [
    {"id": "a", "vector": [1,0,0], "metadata": {"tag": "alpha"}}
  ]
}
```

Response
```json
{"upserted": 1}
```

- POST `/collections/{name}/query`

Request
```json
{
  "vector": [0.9, 0.1, 0],
  "k": 2,
  "filter": {"key": "tag", "equals": "alpha"}
}
```
Range filter (numeric) example
```json
{
  "vector": [0.9, 0.1, 0],
  "k": 10,
  "filter": {"key": "price", "range": {"min": 9.0, "max": 15.0}}
}
```

Return L2 distance in results
```json
{
  "vector": [0.9, 0.1, 0],
  "k": 3,
  "return_distance": true
}
```
When the collection metric is L2, responses will include an optional `distance` field per result. For cosine metric, `distance` is omitted.

Response
```json
{"results": [{"id":"a", "score":0.99, "metadata": {"tag":"alpha"}}]}
```

Errors (uniform shape)
```json
{"code":"bad_request","message":"vector dimension mismatch"}
```
Codes used: `bad_request`, `not_found`, `internal`.

- GET `/collections/{name}/stats`

Response
```json
{"collection":"demo","count":1,"dimension":3,"metric":"cosine"}
```

- DELETE `/collections/{name}/records/{id}`

Response
```json
{"deleted": true}
```
Idempotent: returns `{"deleted": false}` if the record was already absent.

## Data & Durability

- WAL file at `data/wal.log` (JSON Lines). On startup, the server replays the log.
- Snapshots: periodic full snapshots under `data/snapshots/snapshot-<unix>.json` to speed up restart.

## Notes

- First upsert to a new collection must include `dimension` and `metric`.
- Limits and timeouts (env overrides):
  - `YVDB_MAX_REQUEST_BYTES` (default 1,048,576)
  - `YVDB_REQUEST_TIMEOUT_MS` (default 2000)
  - `YVDB_SNAPSHOT_ON_SHUTDOWN` (default false)
- Metrics supported: `cosine`, `l2`.

Scoring semantics
- `score` is always larger-is-better.
- `cosine`: standard cosine similarity in [-1, 1].
- `l2`: `score = -distance`, so closer vectors have higher scores.

`k` behavior
- `k == 0` is rejected with 400.
- `k > count` is allowed; results size is `min(k, count)`.

## Environment variables

- `YVDB_DATA_DIR` (default: `data`) — folder for WAL/snapshots.
- `YVDB_MAX_DIMENSION` (default: `4096`) — upper bound for collection dimension.
- `YVDB_MAX_BATCH` (default: `1024`) — max records per upsert request.
- `YVDB_MAX_K` (default: `1000`) — max `k` per query.
- `YVDB_SNAPSHOT_INTERVAL_SECS` (default: `30`) — snapshot frequency.
- `YVDB_WAL_ROTATE_MAX_BYTES` (default: `0`) — rotate WAL when size exceeds this (0 disables).

