use serde::{Deserialize, Serialize};
use serde_json::Value;

//record carries id, vector and optional metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub vector: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

//upsert request can create a collection by providing dimension and metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertRequest {
    pub records: Vec<Record>,
    #[serde(default)]
    pub dimension: Option<usize>,
    #[serde(default)]
    pub metric: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertResponse {
    pub upserted: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<QueryFilter>,
    #[serde(default)]
    pub return_distance: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredPoint {
    pub id: String,
    pub score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub results: Vec<ScoredPoint>,
}

//simple equality filter on a top-level metadata key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    pub key: String,
    //equals filter on a top-level metadata key
    //QueryFilter to support numeric ranges in the future
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equals: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<QueryRange>,
}

//numeric range filter; min/max are inclusive and optional to allow half-open ranges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsResponse {
    pub collection: String,
    pub count: usize,
    pub dimension: usize,
    pub metric: String,
}

//uniform error shape for all non-success responses so clients can rely on structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

//delete response indicates whether a record existed and was removed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: bool,
}
