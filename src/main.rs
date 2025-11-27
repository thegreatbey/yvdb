use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::http::StatusCode;
use axum::{
    body::Body as AxumBody,
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    middleware,
    response::Response,
    routing::{delete, get, head, post},
    Json, Router,
};
use std::time::Duration;
use tower::timeout::TimeoutLayer;
use tower::BoxError;
use tower::ServiceBuilder;
use tracing_subscriber::{fmt, EnvFilter};

mod api;
mod config;
mod persist;
mod store;

use crate::api::types::ErrorResponse;
use api::routes::{delete_handler, query_handler, stats_handler, upsert_handler};
use config::Config;
use persist::{
    snapshot::{load_latest_snapshot_into, write_snapshot},
    wal::Wal,
};
use store::Store;

#[derive(Clone)]
pub struct AppState {
    //store shared across handlers
    pub store: Arc<Store>,
    //wal writer for durability
    pub wal: Arc<Wal>,
    //config to control limits
    pub config: Arc<Config>,
}

//quick check that server is running; returns a tiny OK so scripts and load balancers can probe readiness fast
async fn healthz() -> &'static str {
    "ok"
}

//head variant for health checks that want no body; reduces bytes on the wire
async fn healthz_head() -> StatusCode {
    StatusCode::OK
}

//exposes package name and version so scripts and humans can verify binaries and builds
async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "name": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION")
    }))
}

//index route returns a short banner with useful links so new users can discover endpoints fast
async fn index() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "name": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
        "endpoints": {
            "health": "/healthz",
            "version": "/version",
            "upsert": "/collections/:name/upsert",
            "query": "/collections/:name/query",
            "stats": "/collections/:name/stats"
        }
    }))
}

//uniform JSON 404 for unknown routes; keeps clients from seeing plain-text errors
async fn not_found(uri: axum::http::Uri) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            code: "not_found".into(),
            message: format!("route {} not found", uri),
        }),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //logs with level from RUST_LOG env or default to info
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(env_filter).init();
    tracing::info!(
        name = env!("CARGO_PKG_NAME"),
        version = env!("CARGO_PKG_VERSION"),
        "starting up"
    );

    //config and data directory
    let config = Arc::new(Config::from_env());
    let data_dir_path = PathBuf::from(&config.data_dir);
    std::fs::create_dir_all(&data_dir_path)?;
    let wal_path = data_dir_path.join("wal.log");

    //init store and wal
    let store = Arc::new(Store::new());
    let wal = Arc::new(Wal::open(&wal_path)?);
    //configure wal rotation from config to keep file sizes bounded
    wal.set_rotate_max_bytes(config.wal_rotate_max_bytes);

    //load the latest snapshot if present, then replay wal
    let snaps_dir = data_dir_path.join("snapshots");
    if let Ok(Some(path)) = load_latest_snapshot_into(&store, &snaps_dir) {
        tracing::info!("loaded snapshot from {}", path.display());
    }
    wal.replay_into(&store)?;

    //prepare state and router; also capture clones for background tasks
    let snap_store = store.clone();
    let interval = config.snapshot_interval_secs;
    //read config values before moving the Arc into state
    let max_request_bytes = config.max_request_bytes;
    let request_timeout_ms = config.request_timeout_ms;
    let snapshot_on_shutdown = config.snapshot_on_shutdown;
    //server bind address may be changed via env for container or remote deployment
    let bind_addr = config.bind_addr.clone();
    //keep only N snapshots based on config so disk usage stays predictable
    let snapshot_retention = config.snapshot_retention;
    let state = AppState { store, wal, config };
    let app = Router::new()
        .route("/collections/:name/upsert", post(upsert_handler))
        .route("/collections/:name/query", post(query_handler))
        .route("/collections/:name/stats", get(stats_handler))
        //list all collections for discovery
        .route("/collections", get(api::routes::collections_handler))
        //discovery-friendly index
        .route("/", get(index))
        .route("/collections/:name/records/:id", delete(delete_handler))
        //health check endpoint for quick "are you up?" probes
        .route("/healthz", get(healthz))
        //head variant for health probes that do not need a body
        .route("/healthz", head(healthz_head))
        //version endpoint returns name and version for quick binary identification
        .route("/version", get(version))
        //fallback ensures 404s return JSON with the same error shape
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(max_request_bytes))
        //map 405 Method Not Allowed into a uniform JSON error body so clients always get {code,message}
        .layer(middleware::map_response(|res: Response| async move {
            match res.status() {
                StatusCode::METHOD_NOT_ALLOWED => {
                    let body = serde_json::json!({
                        "code": "method_not_allowed",
                        "message": "method not allowed"
                    });
                    let (mut parts, _old_body) = res.into_parts();
                    parts.headers.insert(
                        axum::http::header::CONTENT_TYPE,
                        "application/json".parse().unwrap(),
                    );
                    Response::from_parts(parts, AxumBody::from(serde_json::to_vec(&body).unwrap()))
                }
                StatusCode::PAYLOAD_TOO_LARGE => {
                    let body = serde_json::json!({
                        "code": "payload_too_large",
                        "message": "payload too large"
                    });
                    let (mut parts, _old_body) = res.into_parts();
                    parts.headers.insert(
                        axum::http::header::CONTENT_TYPE,
                        "application/json".parse().unwrap(),
                    );
                    Response::from_parts(parts, AxumBody::from(serde_json::to_vec(&body).unwrap()))
                }
                _ => res,
            }
        }))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|err: BoxError| async move {
                    if err.is::<tower::timeout::error::Elapsed>() {
                        Ok::<_, std::convert::Infallible>((
                            StatusCode::REQUEST_TIMEOUT,
                            Json(ErrorResponse {
                                code: "timeout".into(),
                                message: "request timed out".into(),
                            }),
                        ))
                    } else {
                        Ok::<_, std::convert::Infallible>((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                code: "internal".into(),
                                message: "internal error".into(),
                            }),
                        ))
                    }
                }))
                .layer(TimeoutLayer::new(Duration::from_millis(request_timeout_ms)))
                .into_inner(),
        )
        .with_state(state);

    //bind and serve
    let addr: SocketAddr = bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    //background snapshot task saves state periodically to speed up restarts
    let snap_dir_bg = snaps_dir.clone();
    let snap_store_bg = snap_store.clone();
    let ticker_handle = tokio::spawn(async move {
        //keep only N of recent snapshots to reduce disk usage based on configured retention
        let retention = snapshot_retention;
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
        loop {
            ticker.tick().await;
            if let Err(e) = write_snapshot(&snap_store_bg, &snap_dir_bg) {
                tracing::warn!("snapshot failed: {}", e);
            } else {
                //clean up older snapshots beyond retention
                match std::fs::read_dir(&snap_dir_bg) {
                    Ok(read_dir) => {
                        let mut entries: Vec<(std::time::SystemTime, std::path::PathBuf)> =
                            read_dir
                                .filter_map(|e| e.ok())
                                .filter(|e| e.path().is_file())
                                .filter(|e| {
                                    e.file_name().to_string_lossy().starts_with("snapshot-")
                                })
                                .filter_map(|e| {
                                    let modified = e.metadata().and_then(|m| m.modified()).ok()?;
                                    Some((modified, e.path()))
                                })
                                .collect();
                        entries.sort_by_key(|(modified, _)| *modified);
                        while entries.len() > retention {
                            let (_t, path) = entries.remove(0);
                            let _ = std::fs::remove_file(path);
                        }
                    }
                    Err(err) => {
                        tracing::warn!("snapshot retention scan failed: {}", err);
                    }
                }
            }
        }
    });
    tracing::info!("listening on {}", addr);
    //server stops when a shutdown signal arrives, which lets the process exit with code 0
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    //stop background snapshotting before taking the final one
    ticker_handle.abort();
    let _ = ticker_handle.await;
    if snapshot_on_shutdown {
        match write_snapshot(&snap_store, &snaps_dir) {
            Ok(path) => tracing::info!("Final snapshot written in ({})", path.display()),
            Err(e) => tracing::warn!("final snapshot failed: {}", e),
        }
    }
    tracing::info!("Shutdown complete.");
    Ok(())
}

//listens for OS signals and resolves so the server can stop cleanly
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received shutdown request (Ctrl+C)");
        }
        _ = term.recv() => {
            tracing::info!("received shutdown request (SIGTERM)");
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("received shutdown request (Ctrl+C)");
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt as _;
    use tower::util::ServiceExt as _;

    #[tokio::test]
    async fn upsert_then_query_works() {
        //isolated data dir and wal path under temp directory
        let tmpdir = tempfile::tempdir().unwrap();
        let wal_path = tmpdir.path().join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config {
            data_dir: tmpdir.path().to_string_lossy().to_string(),
            max_dimension: 8,
            max_batch: 100,
            max_k: 10,
            snapshot_interval_secs: 60,
            snapshot_retention: 3,
            wal_rotate_max_bytes: 0,
            request_timeout_ms: 2000,
            max_request_bytes: 1_048_576,
            snapshot_on_shutdown: false,
            bind_addr: "127.0.0.1:8080".to_string(),
        });

        let state = AppState { store, wal, config };
        let app = Router::new()
            .route("/collections/:name/upsert", post(upsert_handler))
            .route("/collections/:name/query", post(query_handler))
            .route("/collections/:name/stats", get(stats_handler))
            .with_state(state);

        //create collection and upsert two points
        let upsert_body = serde_json::json!({
            "records": [
                {"id": "a", "vector": [1.0, 0.0]},
                {"id": "b", "vector": [0.0, 1.0]}
            ],
            "dimension": 2,
            "metric": "cosine"
        })
        .to_string();

        let req = Request::post("/collections/demo/upsert")
            .header("content-type", "application/json")
            .body(Body::from(upsert_body))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        //query top-1 near [1,0]
        let query_body = serde_json::json!({"vector": [1.0, 0.0], "k": 1}).to_string();
        let req = Request::post("/collections/demo/query")
            .header("content-type", "application/json")
            .body(Body::from(query_body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: crate::api::types::QueryResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.results.len(), 1);
        assert_eq!(parsed.results[0].id, "a");
    }
}
