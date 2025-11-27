use std::{collections::HashMap, sync::RwLock};

use serde_json::Value;

use crate::api::types::{Record, ScoredPoint};

#[derive(Debug, Clone)]
pub enum Metric {
    Cosine,
    L2,
}

impl Metric {
    //metric name parsing that is forgiving about case
    pub fn from_str(s: &str) -> Result<Metric, String> {
        match s.to_lowercase().as_str() {
            "cosine" => Ok(Metric::Cosine),
            "l2" | "euclidean" => Ok(Metric::L2),
            other => Err(format!("unknown metric: {}", other)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
        }
    }
}

struct Collection {
    dimension: usize,
    metric: Metric,
    ids: Vec<String>,
    vectors: Vec<Vec<f32>>,
    metadata: Vec<Option<Value>>,
}

impl Collection {
    fn new(dimension: usize, metric: Metric) -> Self {
        Self {
            dimension,
            metric,
            ids: Vec::new(),
            vectors: Vec::new(),
            metadata: Vec::new(),
        }
    }

    //insert or update by id
    fn upsert(&mut self, rec: Record) {
        if let Some(pos) = self.ids.iter().position(|id| id == &rec.id) {
            self.vectors[pos] = rec.vector;
            self.metadata[pos] = rec.metadata;
            return;
        }
        self.ids.push(rec.id);
        self.vectors.push(rec.vector);
        self.metadata.push(rec.metadata);
    }

    //remove by id if present; returns true when a record was removed
    fn remove(&mut self, id: &str) -> bool {
        if let Some(pos) = self.ids.iter().position(|x| x == id) {
            self.ids.remove(pos);
            self.vectors.remove(pos);
            self.metadata.remove(pos);
            return true;
        }
        false
    }
}

#[derive(Default)]
pub struct Store {
    //collections protected by a rwlock for concurrent reads and serialized writes
    collections: RwLock<HashMap<String, Collection>>,
}

pub struct Stats {
    pub count: usize,
    pub dimension: usize,
    pub metric: String,
}

impl Store {
    pub fn new() -> Self {
        Self {
            collections: RwLock::new(HashMap::new()),
        }
    }

    //returns all collections with basic statistics for discovery and UI listings
    pub fn list_all_stats(&self) -> Vec<(String, Stats)> {
        let guard = self.collections.read().unwrap();
        let mut out = Vec::with_capacity(guard.len());
        for (name, c) in guard.iter() {
            out.push((
                name.clone(),
                Stats {
                    count: c.ids.len(),
                    dimension: c.dimension,
                    metric: c.metric.as_str().to_string(),
                },
            ));
        }
        out
    }

    //get current config for a collection
    pub fn get_or_create_collection_config(&self, name: &str) -> Option<(usize, Metric)> {
        let guard = self.collections.read().unwrap();
        guard.get(name).map(|c| (c.dimension, c.metric.clone()))
    }

    //initialize a collection if missing
    pub fn ensure_collection(&self, name: &str, dimension: usize, metric: Metric) {
        let mut guard = self.collections.write().unwrap();
        guard
            .entry(name.to_string())
            .or_insert_with(|| Collection::new(dimension, metric));
    }

    pub fn upsert(&self, name: &str, rec: Record) {
        let mut guard = self.collections.write().unwrap();
        if let Some(c) = guard.get_mut(name) {
            c.upsert(rec);
        }
    }

    //delete a record by id; safe to call repeatedly
    pub fn delete(&self, name: &str, id: &str) -> bool {
        let mut guard = self.collections.write().unwrap();
        if let Some(c) = guard.get_mut(name) {
            return c.remove(id);
        }
        false
    }

    pub fn top_k(&self, name: &str, query: &[f32], k: usize) -> Result<Vec<ScoredPoint>, String> {
        let guard = self.collections.read().unwrap();
        let c = guard
            .get(name)
            .ok_or_else(|| "collection not found".to_string())?;

        let mut scored: Vec<(usize, f32)> = Vec::with_capacity(c.ids.len());
        for (idx, v) in c.vectors.iter().enumerate() {
            let score = match c.metric {
                Metric::Cosine => cosine_similarity(query, v),
                Metric::L2 => -l2_distance(query, v),
            };
            scored.push((idx, score));
        }
        /*
        Implemented tie-breaker: if the scores are equal, sort by id ascending
        This is a stable sort, so the order of equal scores is guaranteed to be consistent.
        This is important for the API contract, which guarantees that the order of the results is consistent.
        If the scores are not equal, the order of the results is guaranteed to be descending by score.
        */
        scored.sort_by(
            |a, b| match b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal) {
                std::cmp::Ordering::Equal => c.ids[a.0].cmp(&c.ids[b.0]),
                other => other,
            },
        );
        let take = k.min(scored.len());

        let mut results = Vec::with_capacity(take);
        for (idx, score) in scored.into_iter().take(take) {
            results.push(ScoredPoint {
                id: c.ids[idx].clone(),
                score,
                metadata: c.metadata[idx].clone(),
                distance: None,
            });
        }
        Ok(results)
    }

    //scores all vectors and returns them sorted by score descending
    pub fn score_all_sorted(&self, name: &str, query: &[f32]) -> Result<Vec<ScoredPoint>, String> {
        let guard = self.collections.read().unwrap();
        let c = guard
            .get(name)
            .ok_or_else(|| "collection not found".to_string())?;

        let mut scored: Vec<(usize, f32)> = Vec::with_capacity(c.ids.len());
        for (idx, v) in c.vectors.iter().enumerate() {
            let score = match c.metric {
                Metric::Cosine => cosine_similarity(query, v),
                Metric::L2 => -l2_distance(query, v),
            };
            scored.push((idx, score));
        }

        scored.sort_by(
            |a, b| match b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal) {
                std::cmp::Ordering::Equal => c.ids[a.0].cmp(&c.ids[b.0]),
                other => other,
            },
        );

        let mut results = Vec::with_capacity(scored.len());
        for (idx, score) in scored.into_iter() {
            results.push(ScoredPoint {
                id: c.ids[idx].clone(),
                score,
                metadata: c.metadata[idx].clone(),
                distance: None,
            });
        }
        Ok(results)
    }

    pub fn stats(&self, name: &str) -> Option<Stats> {
        let guard = self.collections.read().unwrap();
        guard.get(name).map(|c| Stats {
            count: c.ids.len(),
            dimension: c.dimension,
            metric: c.metric.as_str().to_string(),
        })
    }

    //export all collections for snapshotting
    pub fn export_all(&self) -> Vec<CollectionExport> {
        let guard = self.collections.read().unwrap();
        let mut out = Vec::with_capacity(guard.len());
        for (name, c) in guard.iter() {
            let mut records = Vec::with_capacity(c.ids.len());
            for idx in 0..c.ids.len() {
                records.push(Record {
                    id: c.ids[idx].clone(),
                    vector: c.vectors[idx].clone(),
                    metadata: c.metadata[idx].clone(),
                });
            }
            out.push(CollectionExport {
                name: name.clone(),
                dimension: c.dimension,
                metric: c.metric.clone(),
                records,
            });
        }
        out
    }
}

//structure used by snapshot logic to serialize full collections
pub struct CollectionExport {
    pub name: String,
    pub dimension: usize,
    pub metric: Metric,
    pub records: Vec<Record>,
}

//dot product based similarity that stays simple for the first version
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = (na.sqrt() * nb.sqrt()).max(1e-12);
    dot / denom
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    /*
    Tests for cosine_similarity, l2_distance, and basic store ops.

    Test Cases Covered:
    - Cosine: parallel ≈ 1, orthogonal ≈ 0
    - L2: unit distance along one axis
    - Store: upsert, top_k ordering, delete idempotency
    - Sorting: deterministic tie-break by ID on equal scores
    */

    #[test]
    fn cosine_similarity_basic() {
        //same direction gives similarity near 1
        let a = [1.0f32, 0.0, 0.0];
        let b = [2.0f32, 0.0, 0.0];
        let s = super::cosine_similarity(&a, &b);
        assert!((s - 1.0).abs() < 1e-6);

        //orthogonal vectors give similarity near 0
        let c = [0.0f32, 1.0, 0.0];
        let s2 = super::cosine_similarity(&a, &c);
        assert!(s2.abs() < 1e-6);
    }

    #[test]
    fn l2_distance_basic() {
        //unit distance across one axis
        let a = [1.0f32, 0.0, 0.0];
        let b = [0.0f32, 0.0, 0.0];
        let d = super::l2_distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-6);
    }

    #[test]
    fn store_upsert_and_top_k() {
        let store = Store::new();
        store.ensure_collection("demo", 3, Metric::Cosine);

        store.upsert(
            "demo",
            Record {
                id: "a".into(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: None,
            },
        );
        store.upsert(
            "demo",
            Record {
                id: "b".into(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: None,
            },
        );

        let results = store.top_k("demo", &[0.9, 0.1, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "a");
        assert!(results[0].score >= results[1].score);
    }

    #[test]
    fn store_delete_removes_and_is_idempotent() {
        let store = Store::new();
        store.ensure_collection("demo", 2, Metric::Cosine);
        store.upsert(
            "demo",
            Record {
                id: "a".into(),
                vector: vec![1.0, 0.0],
                metadata: None,
            },
        );
        let stats = store.stats("demo").unwrap();
        assert_eq!(stats.count, 1);

        let first = store.delete("demo", "a");
        assert!(first);
        let second = store.delete("demo", "a");
        assert!(!second);

        let stats2 = store.stats("demo").unwrap();
        assert_eq!(stats2.count, 0);
    }

    #[test]
    fn equal_scores_sort_by_id() {
        let store = Store::new();
        store.ensure_collection("demo", 2, Metric::Cosine);
        //two records with identical vectors give equal scores for many queries
        store.upsert(
            "demo",
            Record {
                id: "a".into(),
                vector: vec![1.0, 0.0],
                metadata: None,
            },
        );
        store.upsert(
            "demo",
            Record {
                id: "b".into(),
                vector: vec![1.0, 0.0],
                metadata: None,
            },
        );

        let results = store.top_k("demo", &[1.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        //stable ordering on ties should use id ascending
        assert_eq!(results[0].id, "a");
        assert_eq!(results[1].id, "b");
    }
}
