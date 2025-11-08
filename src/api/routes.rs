use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use tracing::instrument;

use crate::{
    api::types::{
        DeleteResponse, ErrorResponse, QueryFilter, QueryRequest, QueryResponse, StatsResponse,
        UpsertRequest, UpsertResponse,
    },
    store::{Metric, Store},
    AppState,
};

//upsert records into a collection; creates the collection if it does not exist
//span carries collection and batch size so logs show request scale
#[instrument(level = "info", skip_all, fields(collection = %name, batch = %payload.records.len()))]
pub async fn upsert_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<UpsertRequest>,
) -> Result<Json<UpsertResponse>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let store: Arc<Store> = state.store.clone();
    let wal = state.wal.clone();

    let (dimension, _metric) = match store.get_or_create_collection_config(&name) {
        Some((dim, m)) => (dim, m),
        None => {
            let dim = payload.dimension.ok_or((
                axum::http::StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    code: "bad_request".into(),
                    message: "missing dimension".into(),
                }),
            ))?;
            if dim == 0 || dim > state.config.max_dimension {
                return Err((
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        code: "bad_request".into(),
                        message: format!("invalid dimension; max {}", state.config.max_dimension),
                    }),
                ));
            }
            let metric_str = payload.metric.clone().ok_or((
                axum::http::StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    code: "bad_request".into(),
                    message: "missing metric".into(),
                }),
            ))?;
            let metric = Metric::from_str(&metric_str).map_err(|e| {
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        code: "bad_request".into(),
                        message: e,
                    }),
                )
            })?;
            store.ensure_collection(&name, dim, metric.clone());
            (dim, metric)
        }
    };

    //validate vectors match dimension
    if payload.records.iter().any(|r| r.vector.len() != dimension) {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: "vector dimension mismatch".into(),
            }),
        ));
    }

    //batch size guard
    if payload.records.len() > state.config.max_batch {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: format!("batch too large; max {}", state.config.max_batch),
            }),
        ));
    }

    //append to wal and apply to store
    let mut upserted = 0usize;
    for rec in payload.records {
        wal.append_upsert(&name, &rec).map_err(internal_error)?;
        store.upsert(&name, rec);
        upserted += 1;
    }

    Ok(Json(UpsertResponse { upserted }))
}

//query top-k using brute-force search
//span includes k to make logs reflect result size intent
#[instrument(level = "info", skip_all, fields(collection = %name, k = %payload.k))]
pub async fn query_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let store = state.store.clone();
    let (dimension, metric) = store.get_or_create_collection_config(&name).ok_or((
        axum::http::StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            code: "not_found".into(),
            message: "collection not found".into(),
        }),
    ))?;

    if payload.vector.len() != dimension {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: "vector dimension mismatch".into(),
            }),
        ));
    }
    if payload.k == 0 || payload.k > state.config.max_k {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: format!("invalid k; max {}", state.config.max_k),
            }),
        ));
    }

    /*if a filter is provided, score all vectors and apply the filter
    keep only those whose metadata has key == value, and returns the first k.
    otherwise, use the top_k function to get the top k results*/
    let mut results = if let Some(ref filter) = payload.filter {
        let all = store
            .score_all_sorted(&name, &payload.vector)
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        code: "internal".into(),
                        message: e,
                    }),
                )
            })?;
        let mut out = Vec::with_capacity(payload.k.min(all.len()));
        for sp in all.into_iter() {
            if metadata_matches_filter(&sp.metadata, filter) {
                out.push(sp);
                if out.len() == payload.k {
                    break;
                }
            }
        }
        out
    } else {
        store
            .top_k(&name, &payload.vector, payload.k)
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        code: "internal".into(),
                        message: e,
                    }),
                )
            })?
    };
    if payload.return_distance {
        if let Metric::L2 = metric {
            for r in &mut results {
                r.distance = Some(-r.score);
            }
        }
    }
    Ok(Json(QueryResponse { results }))
}

//collection stats for visibility
#[instrument(level = "info", skip_all, fields(collection = %name))]
pub async fn stats_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<StatsResponse>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let store = state.store.clone();
    let stats = store.stats(&name).ok_or((
        axum::http::StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            code: "not_found".into(),
            message: "collection not found".into(),
        }),
    ))?;
    Ok(Json(StatsResponse {
        collection: name,
        count: stats.count,
        dimension: stats.dimension,
        metric: stats.metric,
    }))
}

fn internal_error(err: anyhow::Error) -> (axum::http::StatusCode, Json<ErrorResponse>) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            code: "internal".into(),
            message: err.to_string(),
        }),
    )
}

fn metadata_matches_filter(meta: &Option<serde_json::Value>, filter: &QueryFilter) -> bool {
    if let Some(serde_json::Value::Object(map)) = meta {
        if let Some(v) = map.get(&filter.key) {
            //equals match when provided
            let equals_ok = match &filter.equals {
                Some(expect) => v == expect,
                None => true,
            };

            //range match when provided; only applies if metadata value is numeric
            let range_ok = match &filter.range {
                Some(r) => {
                    if let Some(n) = v.as_f64() {
                        let min_ok = r.min.map(|m| n >= m).unwrap_or(true);
                        let max_ok = r.max.map(|m| n <= m).unwrap_or(true);
                        min_ok && max_ok
                    } else {
                        false
                    }
                }
                None => true,
            };

            return equals_ok && range_ok;
        }
    }
    false
}

//delete a record; idempotent: returns deleted=false when the record was not present
#[instrument(level = "info", skip_all, fields(collection = %name, id = %id))]
pub async fn delete_handler(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<DeleteResponse>, (axum::http::StatusCode, Json<ErrorResponse>)> {
    let store = state.store.clone();
    //ensure collection exists for consistent 404 semantics
    store.get_or_create_collection_config(&name).ok_or((
        axum::http::StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            code: "not_found".into(),
            message: "collection not found".into(),
        }),
    ))?;

    //append delete intent regardless of existence so recovery converges
    state
        .wal
        .append_delete(&name, &id)
        .map_err(internal_error)?;
    let deleted = store.delete(&name, &id);
    Ok(Json(DeleteResponse { deleted }))
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::{delete, get, post},
        Router,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt; // for oneshot

    use crate::{
        api::types::{
            DeleteResponse, ErrorResponse, QueryFilter, QueryRequest, QueryResponse, Record,
            StatsResponse, UpsertRequest, UpsertResponse,
        },
        config::Config,
        persist::wal::Wal,
        store::Store,
        AppState,
    };

    #[tokio::test]
    async fn upsert_query_stats_end_to_end() {
        //temporary wal location to isolate test data
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = PathBuf::from(tmp.path()).join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config::from_env());
        let state = AppState {
            store: store.clone(),
            wal: wal.clone(),
            config: config.clone(),
        };

        //build a router identical to main
        let app = Router::new()
            .route("/collections/:name/upsert", post(super::upsert_handler))
            .route("/collections/:name/query", post(super::query_handler))
            .route("/collections/:name/stats", get(super::stats_handler))
            .with_state(state);

        //first upsert creates collection with dim/metric
        let up_body = serde_json::to_vec(&UpsertRequest {
            records: vec![
                Record {
                    id: "a".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    metadata: None,
                },
                Record {
                    id: "b".into(),
                    vector: vec![0.0, 1.0, 0.0],
                    metadata: None,
                },
            ],
            dimension: Some(3),
            metric: Some("cosine".into()),
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(up_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let up: UpsertResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(up.upserted, 2);

        //query returns a first hit for id "a"
        let q_body = serde_json::to_vec(&QueryRequest {
            vector: vec![0.9, 0.1, 0.0],
            k: 2,
            filter: None,
            return_distance: false,
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/query")
                    .header("content-type", "application/json")
                    .body(Body::from(q_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let qr: QueryResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(qr.results[0].id, "a");

        //stats shows two items
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/collections/demo/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let s: StatsResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(s.count, 2);
    }

    #[tokio::test]
    async fn query_k_zero_rejected_with_json_error() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = PathBuf::from(tmp.path()).join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config::from_env());
        let state = AppState {
            store: store.clone(),
            wal: wal.clone(),
            config: config.clone(),
        };

        let app = Router::new()
            .route("/collections/:name/upsert", post(super::upsert_handler))
            .route("/collections/:name/query", post(super::query_handler))
            .with_state(state);

        //create collection first
        let up_body = serde_json::to_vec(&UpsertRequest {
            records: vec![Record {
                id: "a".into(),
                vector: vec![1.0, 0.0],
                metadata: None,
            }],
            dimension: Some(2),
            metric: Some("cosine".into()),
        })
        .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(up_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        //query with k==0 should be 400 with structured error
        let q_body = serde_json::to_vec(&QueryRequest {
            vector: vec![1.0, 0.0],
            k: 0,
            filter: None,
            return_distance: false,
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/query")
                    .header("content-type", "application/json")
                    .body(Body::from(q_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let err: ErrorResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("invalid k"));
    }

    #[tokio::test]
    async fn query_k_greater_than_count_returns_min() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = PathBuf::from(tmp.path()).join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config::from_env());
        let state = AppState {
            store: store.clone(),
            wal: wal.clone(),
            config: config.clone(),
        };

        let app = Router::new()
            .route("/collections/:name/upsert", post(super::upsert_handler))
            .route("/collections/:name/query", post(super::query_handler))
            .with_state(state);

        //create collection and insert a single record
        let up_body = serde_json::to_vec(&UpsertRequest {
            records: vec![Record {
                id: "a".into(),
                vector: vec![1.0, 0.0],
                metadata: None,
            }],
            dimension: Some(2),
            metric: Some("cosine".into()),
        })
        .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(up_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        //request k much larger than count should not error and should return 1 result
        let q_body = serde_json::to_vec(&QueryRequest {
            vector: vec![1.0, 0.0],
            k: 10,
            filter: None,
            return_distance: false,
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/query")
                    .header("content-type", "application/json")
                    .body(Body::from(q_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let qr: QueryResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(qr.results.len(), 1);
        assert_eq!(qr.results[0].id, "a");
    }

    #[tokio::test]
    async fn delete_endpoint_removes_record_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = PathBuf::from(tmp.path()).join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config::from_env());
        let state = AppState {
            store: store.clone(),
            wal: wal.clone(),
            config: config.clone(),
        };

        let app = Router::new()
            .route("/collections/:name/upsert", post(super::upsert_handler))
            .route("/collections/:name/query", post(super::query_handler))
            .route(
                "/collections/:name/records/:id",
                delete(super::delete_handler),
            )
            .with_state(state);

        //create collection and insert a record
        let up_body = serde_json::to_vec(&UpsertRequest {
            records: vec![Record {
                id: "a".into(),
                vector: vec![1.0, 0.0],
                metadata: None,
            }],
            dimension: Some(2),
            metric: Some("cosine".into()),
        })
        .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(up_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        //delete the record
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/collections/demo/records/a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let del: DeleteResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(del.deleted);

        //deleting again returns deleted=false
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/collections/demo/records/a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let del2: DeleteResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(!del2.deleted);

        //query should return zero results
        let q_body = serde_json::to_vec(&QueryRequest {
            vector: vec![1.0, 0.0],
            k: 1,
            filter: None,
            return_distance: false,
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/query")
                    .header("content-type", "application/json")
                    .body(Body::from(q_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let qr: QueryResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(qr.results.len(), 0);
    }

    #[tokio::test]
    async fn query_with_filter_narrows_results() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = PathBuf::from(tmp.path()).join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config::from_env());
        let state = AppState {
            store: store.clone(),
            wal: wal.clone(),
            config: config.clone(),
        };

        let app = Router::new()
            .route("/collections/:name/upsert", post(super::upsert_handler))
            .route("/collections/:name/query", post(super::query_handler))
            .with_state(state);

        //insert two records with different metadata tags
        let up_body = serde_json::to_vec(&UpsertRequest {
            records: vec![
                Record {
                    id: "a".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    metadata: Some(serde_json::json!({"tag":"alpha"})),
                },
                Record {
                    id: "b".into(),
                    vector: vec![0.0, 1.0, 0.0],
                    metadata: Some(serde_json::json!({"tag":"beta"})),
                },
            ],
            dimension: Some(3),
            metric: Some("cosine".into()),
        })
        .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(up_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        //apply filter to only return tag==alpha
        let q_body = serde_json::to_vec(&QueryRequest {
            vector: vec![0.9, 0.1, 0.0],
            k: 5,
            filter: Some(QueryFilter {
                key: "tag".into(),
                equals: Some(serde_json::json!("alpha")),
                range: None,
            }),
            return_distance: false,
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/query")
                    .header("content-type", "application/json")
                    .body(Body::from(q_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let qr: QueryResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(qr.results.len(), 1);
        assert_eq!(qr.results[0].id, "a");
    }

    #[tokio::test]
    async fn query_with_numeric_range_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = PathBuf::from(tmp.path()).join("wal.log");

        let store = Arc::new(Store::new());
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let config = Arc::new(Config::from_env());
        let state = AppState {
            store: store.clone(),
            wal: wal.clone(),
            config: config.clone(),
        };

        let app = Router::new()
            .route("/collections/:name/upsert", post(super::upsert_handler))
            .route("/collections/:name/query", post(super::query_handler))
            .with_state(state);

        //insert three records with numeric metadata 'price'
        let up_body = serde_json::to_vec(&UpsertRequest {
            records: vec![
                Record {
                    id: "a".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    metadata: Some(serde_json::json!({"price": 5.0})),
                },
                Record {
                    id: "b".into(),
                    vector: vec![0.9, 0.1, 0.0],
                    metadata: Some(serde_json::json!({"price": 10.0})),
                },
                Record {
                    id: "c".into(),
                    vector: vec![0.8, 0.2, 0.0],
                    metadata: Some(serde_json::json!({"price": 20.0})),
                },
            ],
            dimension: Some(3),
            metric: Some("cosine".into()),
        })
        .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(up_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        //filter price in [9, 15] should include only id "b"
        let q_body = serde_json::to_vec(&QueryRequest {
            vector: vec![0.9, 0.1, 0.0],
            k: 10,
            filter: Some(QueryFilter {
                key: "price".into(),
                equals: None,
                range: Some(crate::api::types::QueryRange {
                    min: Some(9.0),
                    max: Some(15.0),
                }),
            }),
            return_distance: false,
        })
        .unwrap();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/demo/query")
                    .header("content-type", "application/json")
                    .body(Body::from(q_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let qr: QueryResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(qr.results.len(), 1);
        assert_eq!(qr.results[0].id, "b");
    }
}
