# yvdb (an intro vector DB)
[![CI](https://github.com/thegreatbey/yvdb/actions/workflows/ci.yml/badge.svg)](https://github.com/thegreatbey/yvdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/yvdb.svg)](https://crates.io/crates/yvdb)
[![Docs.rs](https://docs.rs/yvdb/badge.svg)](https://docs.rs/yvdb)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/thegreatbey/yvdb/blob/main/LICENSE)

Small, educational vector database: single-node, **in-memory store** with **disk durability**, **IVF bucket search**, and a **TOON** query response format for LLM-friendly output.

## What is in this version

| Piece | What it does |
|-------|----------------|
| **In-memory store** | Fast reads/writes while the server is running (`Store` in RAM). |
| **WAL** (`data/wal.log`) | Append-only log of inserts; replayed on startup so inserts survive restarts. |
| **Binary snapshots** (`data/snapshots/snapshot-*.bin`) | Periodic full saves using **bincode** + a 4-byte **`TOON`** magic header for file integrity. |
| **IVF index** | K-means centroids + buckets in `src/store/mod.rs`; queries scan the nearest bucket when the collection is large enough. |
| **REST API (Axum)** | Three flat POST routes (see below). |
| **TOON export** | `POST /vectors/query` returns plain text using a TOON-style layout (see below). |

## What is TOON (Token-Oriented Object Notation)?

**TOON** stands for **Token-Oriented Object Notation**. It is a text format meant to carry the same kind of data as JSON, but with **fewer repeated words** so LLM prompts stay smaller and easier to read.

### Global spec (verified)

There is an official, public spec (not invented only for yvdb):

- **Specification:** [github.com/toon-format/spec](https://github.com/toon-format/spec/blob/main/SPEC.md) (Working Draft v3.0, 2025-11-24)
- **Reference tooling:** [github.com/toon-format/toon](https://github.com/toon-format/toon) (TypeScript encode/decode)
- **Docs / overview:** [toonformat.dev](https://toonformat.dev/)

That spec defines a full grammar: line-oriented layout, array headers with lengths and field lists, delimiters, indentation for objects, and rules for quoting. It targets **uniform tables** (many rows, same columns) and **LLM-facing** payloads.

### What yvdb implements (query results only)

yvdb uses the **same idea** (declare column names once, then row values only) but a **small, fixed layout** for vector search results — not the full spec parser.

| | Official TOON spec | yvdb query response |
|--|-------------------|-------------------|
| **Used where** | General JSON ↔ TOON conversion | Only `POST /vectors/query` **response body** |
| **Header style** | Normative headers like `key[N]{field1,field2}:` | Project banner + `fields: id, score, metadata` + `---` |
| **Row style** | Spec delimiter rules (comma/tab/pipe) | Comma-separated values per line |
| **Requests** | Typically JSON in | **JSON in** (unchanged) |

Example **yvdb** query output (this is what the server returns):

```text
[yvdb_query_results]
fields: id, score, metadata
---
item_abc, 1.000, {"category":"sports"}
```

Same information as JSON, different packaging:

```json
{"results":[{"id":"item_abc","score":1.0,"metadata":{"category":"sports"}}]}
```

If you need strict compatibility with [toon-format/spec](https://github.com/toon-format/spec), use their encoder on JSON; yvdb does not run that encoder today.

### Benefits (why bother?)

1. **Fewer tokens for LLMs** — Column names (`id`, `score`, `metadata`) appear once in `fields:`, not on every row. That can reduce cost and noise when you paste search hits into a chat model.
2. **Readable tables** — Humans see a clear header and one line per hit, similar to CSV with a declared schema.
3. **Still structured** — Rows stay ordered; scores and metadata remain machine-parseable (metadata is still JSON text on each line).
4. **JSON where it fits** — Create, insert, errors, and health routes stay JSON because tools and clients already expect that shape.

### Not the same as snapshot `TOON` bytes

Binary files under `data/snapshots/` start with the four ASCII bytes `TOON` as a **file magic** marker (like a signature). That is only for **detecting valid snapshot files** on disk. It is **not** the text format above and is **not** human-readable.

## Run

Use **two terminals** while learning: one runs the server (blocking), the other sends HTTP requests.

**Terminal 1 — server**

```powershell
cd C:\Users\rcabe\Coding\qdrant-exp\yvdb
cargo run
```

Wait for: `listening on 127.0.0.1:8080`

`RUST_LOG` is optional; the server defaults to `info` if unset.

```powershell
# optional: change bind address
$env:YVDB_BIND_ADDR="127.0.0.1:8080"
cargo run
```

**Terminal 2 — client (PowerShell)**

Prefer `ConvertTo-Json` + `Invoke-RestMethod` / `Invoke-WebRequest` on Windows (avoids `curl.exe` quoting issues).

---

## Quick start: full workflow

Each step shows **JSON** (what the server expects) and **PowerShell** (recommended on Windows).

### 1) Create collection

Registers namespace `demo2`, dimension `4`, metric `cosine`.

JSON:

```json
{"name":"demo2","dimension":4,"metric":"cosine"}
```

PowerShell:

```powershell
$body = @{ name = "demo2"; dimension = 4; metric = "cosine" } | ConvertTo-Json -Compress
Invoke-RestMethod -Uri "http://localhost:8080/collection/create" -Method POST -ContentType "application/json" -Body $body
```

Response: `{"created":"demo2"}` or `{"exists":"demo2"}` if the name is already loaded from disk.

### 2) Insert vectors (+ WAL)

Each insert is appended to `data/wal.log` then applied to memory.

JSON:

```json
{
  "collection": "demo2",
  "records": [
    {"id":"item_abc","vector":[0.15,0.22,0.34,0.89],"metadata":{"category":"sports"}}
  ]
}
```

PowerShell:

```powershell
$body = @{
  collection = "demo2"
  records = @(
    @{
      id = "item_abc"
      vector = @(0.15, 0.22, 0.34, 0.89)
      metadata = @{ category = "sports" }
    }
  )
} | ConvertTo-Json -Depth 5 -Compress

Invoke-RestMethod -Uri "http://localhost:8080/vectors/insert" -Method POST -ContentType "application/json" -Body $body
```

Response: `{"inserted":1}`

Batch must have **1 to `YVDB_MAX_BATCH` records** (default 1024); empty or oversized batches return `invalid batch size`.

Vector length must match the collection dimension (4 here) or the server returns `vector dimension mismatch`.

### 3) Query → TOON

JSON:

```json
{"collection":"demo2","vector":[0.15,0.22,0.34,0.89],"k":3}
```

PowerShell:

```powershell
$body = @{ collection = "demo2"; vector = @(0.15, 0.22, 0.34, 0.89); k = 3 } | ConvertTo-Json -Compress
(Invoke-WebRequest -Uri "http://localhost:8080/vectors/query" -Method POST -ContentType "application/json" -Body $body -UseBasicParsing).Content
```

Example response (`text/plain`):

```text
[yvdb_query_results]
fields: id, score, metadata
---
item_abc, 1.000, {"category":"sports"}
```

Use `-UseBasicParsing` on `Invoke-WebRequest` to skip PowerShell’s HTML parsing warning.

### 4) View (discovery, health, version)

Read-only GET routes; no body required.

| Path | Purpose |
|------|---------|
| `GET /` | Lists endpoint paths |
| `GET /healthz` | Liveness, uptime, and per-collection stats (`collections` array) |
| `GET /version` | Crate name and version |

PowerShell:

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/" -Method GET
Invoke-RestMethod -Uri "http://localhost:8080/healthz" -Method GET
Invoke-RestMethod -Uri "http://localhost:8080/version" -Method GET
```

### 5) Restart → query without insert (persistence test)

1. **Terminal 1:** `Ctrl+C` to stop the server, then `cargo run` again.
2. **Terminal 2:** Run only the **query** PowerShell block from step 3 (no create/insert).

If you still see `item_abc`, WAL and/or snapshots restored your data from `data/`.

---

## HTTP API (active routes)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/collection/create` | Create a collection (name, dimension, metric). |
| POST | `/vectors/insert` | Insert or update records (WAL + memory). |
| POST | `/vectors/query` | Top-k search; response body is **TOON** text. |
| GET | `/` | Discovery JSON (lists endpoints). |
| GET | `/healthz` | Health check (JSON: `status`, `uptime_secs`, `collections`). |
| HEAD | `/healthz` | Same liveness as GET, no response body. |
| GET | `/version` | Package name and version. |

### POST `/collection/create`

JSON:

```json
{"name":"demo2","dimension":4,"metric":"cosine"}
```

PowerShell:

```powershell
$body = @{ name = "demo2"; dimension = 4; metric = "cosine" } | ConvertTo-Json -Compress
Invoke-RestMethod -Uri "http://localhost:8080/collection/create" -Method POST -ContentType "application/json" -Body $body
```

Response: `{"created":"demo2"}` or `{"exists":"demo2"}`.

Metrics: `cosine`, `l2` (or `euclidean`).

### POST `/vectors/insert`

JSON:

```json
{
  "collection": "demo2",
  "records": [
    {"id":"item_abc","vector":[0.15,0.22,0.34,0.89],"metadata":{"category":"sports"}}
  ]
}
```

PowerShell:

```powershell
$body = @{
  collection = "demo2"
  records = @(
    @{
      id = "item_abc"
      vector = @(0.15, 0.22, 0.34, 0.89)
      metadata = @{ category = "sports" }
    }
  )
} | ConvertTo-Json -Depth 5 -Compress

Invoke-RestMethod -Uri "http://localhost:8080/vectors/insert" -Method POST -ContentType "application/json" -Body $body
```

Response: `{"inserted":1}`

Batch must have **1 to `YVDB_MAX_BATCH` records** (default 1024); empty or oversized batches return `invalid batch size`.

Do not send a single top-level `id` / `vector` without `collection` and `records` (that is not this API).

### POST `/vectors/query`

JSON:

```json
{"collection":"demo2","vector":[0.15,0.22,0.34,0.89],"k":3}
```

PowerShell:

```powershell
$body = @{ collection = "demo2"; vector = @(0.15, 0.22, 0.34, 0.89); k = 3 } | ConvertTo-Json -Compress
(Invoke-WebRequest -Uri "http://localhost:8080/vectors/query" -Method POST -ContentType "application/json" -Body $body -UseBasicParsing).Content
```

Response (`text/plain`):

```text
[yvdb_query_results]
fields: id, score, metadata
---
item_abc, 1.000, {"category":"sports"}
```

### GET view routes

| Path | Response |
|------|----------|
| `GET /` | JSON map of endpoint paths |
| `GET /healthz` | `{"status":"ok","uptime_secs":...,"collections":[...]}` |
| `GET /version` | `{"name":"yvdb","version":"0.1.4"}` |

PowerShell:

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/" -Method GET
Invoke-RestMethod -Uri "http://localhost:8080/healthz" -Method GET
Invoke-RestMethod -Uri "http://localhost:8080/version" -Method GET
```

### Errors (JSON)

```json
{"code":"bad_request","message":"vector dimension mismatch"}
```

Codes: `bad_request`, `not_found`, `internal`, `timeout`, `method_not_allowed`, `payload_too_large`.

---

## Data and durability

While the server runs, all collections live in **RAM**. On disk:

| Path | Format | Role |
|------|--------|------|
| `data/wal.log` | JSON Lines | Every insert is logged; replayed after snapshots on startup. |
| `data/snapshots/snapshot-<unix>.bin` | `TOON` (4 bytes) + bincode | Periodic full snapshot (default every 30s). |

**Startup order:** load latest snapshot → replay WAL → serve requests.

**Stopping the server** clears memory but does **not** delete `data/` unless you remove it manually.

**Reset local data (fresh start):**

```powershell
Remove-Item -Force .\data\wal.log -ErrorAction SilentlyContinue
Remove-Item -Force .\data\snapshots\* -ErrorAction SilentlyContinue
```

**Note:** `POST /collection/create` only writes memory until a snapshot runs or you insert (inserts go to the WAL). For a clean persistence test, insert at least one record before restarting.

---

## Architecture (how modules connect)

```text
HTTP (Axum main.rs)
  → routes.rs handlers
    → Store (in-memory, src/store/mod.rs)
         → IVF: centroids + buckets (K-means when enough vectors)
    → Wal append on insert (src/persist/wal.rs)
  → snapshot.rs: bincode + TOON header → data/snapshots/*.bin
```

---

## Windows: sending JSON reliably

**Recommended:** hashtable + `ConvertTo-Json`:

```powershell
$body = @{ name = "demo2"; dimension = 4; metric = "cosine" } | ConvertTo-Json -Compress
Invoke-RestMethod -Uri "http://localhost:8080/collection/create" -Method POST -ContentType "application/json" -Body $body
```

**Alternative:** write a UTF-8 file (no BOM) and use `curl.exe --data-binary "@file.json"`.

Avoid `curl.exe -d "{\"name\":...}"` in PowerShell without `--%`; inner quotes are often stripped.

---

## Scoring

- `score` is larger-is-better.
- **cosine:** similarity in roughly `[-1, 1]` (1.0 = same direction).
- **l2:** `score = -distance` (closer vectors score higher).

`k == 0` is rejected. `k` greater than the number of records returns all available matches.

---

## Env variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `YVDB_DATA_DIR` | `data` | WAL and snapshot folder. |
| `YVDB_BIND_ADDR` | `127.0.0.1:8080` | Listen address. |
| `YVDB_MAX_DIMENSION` | `4096` | Max vector length. |
| `YVDB_MAX_BATCH` | `1024` | Max records per insert request. |
| `YVDB_MAX_K` | `1000` | Max `k` per query. |
| `YVDB_SNAPSHOT_INTERVAL_SECS` | `30` | Snapshot interval. |
| `YVDB_SNAPSHOT_RETENTION` | `3` | Snapshots kept on disk. |
| `YVDB_SNAPSHOT_ON_SHUTDOWN` | `false` | Write a snapshot when the server exits. |
| `YVDB_WAL_ROTATE_MAX_BYTES` | `0` | WAL rotation size (0 = off). |
| `YVDB_MAX_REQUEST_BYTES` | `1048576` | Max HTTP body size. |
| `YVDB_REQUEST_TIMEOUT_MS` | `2000` | Request timeout. |

---

## Notes

- Educational project: not a distributed production database.
- Old nested routes (`/collections/{name}/upsert`, etc.) and the adaptive **heartbeat** (`min_score` relaxation) were removed in v0.1.2.
- IVF builds lazily after a collection has more than 32 vectors; smaller collections use flat search.
