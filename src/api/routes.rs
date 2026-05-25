use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};

use crate::{
    api::types::{
        CreateCollectionRequest, ErrorResponse, InsertVectorsRequest, QueryVectorsRequest,
        ScoredPoint,
    },
    app_state::AppState,
    store::Metric,
};

fn internal_error(err: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            code: "internal".into(),
            message: err.to_string(),
        }),
    )
}

//POST /collection/create registers name, dimension, and distance metric before any inserts
pub async fn create_collection_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateCollectionRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if payload.dimension == 0 || payload.dimension > state.config.max_dimension {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: format!("invalid dimension; max {}", state.config.max_dimension),
            }),
        ));
    }
    let metric = payload.metric.parse::<Metric>().map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: e,
            }),
        )
    })?;
    //or_insert does not update an existing collection; stale wal may leave the wrong dimension
    if let Some((existing_dim, _)) = state.store.get_or_create_collection_config(&payload.name) {
        if existing_dim != payload.dimension {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    code: "bad_request".into(),
                    message: format!(
                        "collection '{}' already exists with dimension {}; delete data dir or use another name",
                        payload.name, existing_dim
                    ),
                }),
            ));
        }
        return Ok(Json(serde_json::json!({"exists": payload.name})));
    }
    state
        .store
        .ensure_collection(&payload.name, payload.dimension, metric);
    Ok(Json(serde_json::json!({"created": payload.name})))
}

//POST /vectors/insert writes wal then memory so restarts replay missed in-memory state
pub async fn insert_vectors_handler(
    State(state): State<AppState>,
    Json(payload): Json<InsertVectorsRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let count = payload.records.len();
    if count == 0 || count > state.config.max_batch {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: format!("invalid batch size; max {}", state.config.max_batch),
            }),
        ));
    }
    let wal = state.wal.clone();
    for r in payload.records {
        let dimension = match state
            .store
            .get_or_create_collection_config(&payload.collection)
        {
            None => {
                let dim = r.vector.len();
                if dim == 0 || dim > state.config.max_dimension {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            code: "bad_request".into(),
                            message: format!(
                                "invalid dimension; max {}",
                                state.config.max_dimension
                            ),
                        }),
                    ));
                }
                state
                    .store
                    .ensure_collection(&payload.collection, dim, Metric::Cosine);
                dim
            }
            Some((dim, _)) => dim,
        };
        if r.vector.len() != dimension {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    code: "bad_request".into(),
                    message: "vector dimension mismatch".into(),
                }),
            ));
        }
        wal.append_upsert(&payload.collection, &r)
            .map_err(internal_error)?;
        state.store.upsert(&payload.collection, r);
    }
    Ok(Json(serde_json::json!({"inserted": count})))
}

//TOON: one header line for field names, then rows only (fewer tokens for LLM prompts than json)
fn to_toon(results: &[ScoredPoint]) -> String {
    let mut s = String::from("[yvdb_query_results]\nfields: id, score, metadata\n---\n");
    for p in results.iter() {
        let meta = p
            .metadata
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into());
        s.push_str(&format!("{}, {:.3}, {}\n", p.id, p.score, meta));
    }
    s
}

//POST /vectors/query returns TOON text (not json) for token-efficient LLM downstream use
pub async fn query_vectors_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryVectorsRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let (dimension, _metric) = state
        .store
        .get_or_create_collection_config(&payload.collection)
        .ok_or((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "not_found".into(),
                message: "collection not found".into(),
            }),
        ))?;
    if payload.vector.len() != dimension {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: "vector dimension mismatch".into(),
            }),
        ));
    }
    if payload.k == 0 || payload.k > state.config.max_k {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                code: "bad_request".into(),
                message: format!("invalid k; max {}", state.config.max_k),
            }),
        ));
    }
    let results = state
        .store
        .top_k(&payload.collection, &payload.vector, payload.k)
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    code: "not_found".into(),
                    message: e,
                }),
            )
        })?;
    let body = to_toon(&results);
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    ))
}
