use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::{Metric, Store};

#[derive(Serialize, Deserialize)]
struct Snapshot {
    //format version lets future readers know how to interpret fields
    #[serde(default)]
    version: u32,
    collections: Vec<CollectionSnap>,
}

#[derive(Serialize, Deserialize)]
struct CollectionSnap {
    name: String,
    dimension: usize,
    metric: String,
    records: Vec<RecordSnap>,
}

#[derive(Serialize, Deserialize)]
struct RecordSnap {
    id: String,
    vector: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
}

pub fn write_snapshot(store: &Store, dir: &Path) -> anyhow::Result<PathBuf> {
    //take a consistent view by reading under the store's read lock via export
    let exports = store.export_all();

    let mut collections = Vec::with_capacity(exports.len());
    for ex in exports {
        let mut records = Vec::with_capacity(ex.records.len());
        for r in ex.records {
            records.push(RecordSnap {
                id: r.id,
                vector: r.vector,
                metadata: r.metadata,
            });
        }
        collections.push(CollectionSnap {
            name: ex.name,
            dimension: ex.dimension,
            metric: ex.metric.as_str().to_string(),
            records,
        });
    }

    let snap = Snapshot {
        version: 1,
        collections,
    };
    fs::create_dir_all(dir)?;
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let path = dir.join(format!("snapshot-{}.json", ts));
    let mut f = fs::File::create(&path)?;
    let json = serde_json::to_vec_pretty(&snap)?;
    f.write_all(&json)?;
    f.sync_data()?;
    Ok(path)
}

pub fn load_latest_snapshot_into(store: &Store, dir: &Path) -> anyhow::Result<Option<PathBuf>> {
    let mut latest: Option<(PathBuf, std::time::SystemTime)> = None;
    if !dir.exists() {
        return Ok(None);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            if let Ok(modified) = meta.modified() {
                match latest {
                    None => latest = Some((path, modified)),
                    Some((_, t)) if modified > t => latest = Some((path, modified)),
                    _ => {}
                }
            }
        }
    }

    let Some((path, _)) = latest else {
        return Ok(None);
    };
    let bytes = fs::read(&path)?;
    let snap: Snapshot = serde_json::from_slice(&bytes)?;

    for col in snap.collections {
        let metric = Metric::from_str(&col.metric).map_err(|e| anyhow::anyhow!(e))?;
        store.ensure_collection(&col.name, col.dimension, metric);
        for r in col.records {
            store.upsert(
                &col.name,
                crate::api::types::Record {
                    id: r.id,
                    vector: r.vector,
                    metadata: r.metadata,
                },
            );
        }
    }
    Ok(Some(path))
}

#[cfg(test)]
mod tests {
    use crate::store::{Metric, Store};

    use super::*;

    #[test]
    fn snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new();
        store.ensure_collection("demo", 3, Metric::Cosine);
        store.upsert(
            "demo",
            crate::api::types::Record {
                id: "a".into(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: None,
            },
        );
        store.upsert(
            "demo",
            crate::api::types::Record {
                id: "b".into(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: None,
            },
        );

        let path = write_snapshot(&store, dir.path()).unwrap();
        assert!(path.exists());

        let store2 = Store::new();
        let loaded = load_latest_snapshot_into(&store2, dir.path()).unwrap();
        assert!(loaded.is_some());
        let stats = store2.stats("demo").unwrap();
        assert_eq!(stats.count, 2);
    }

    #[test]
    fn snapshot_loads_without_version_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new();

        //write a minimal old-format snapshot without version
        std::fs::create_dir_all(dir.path()).unwrap();
        let path = dir.path().join("snapshot-old.json");
        let old = serde_json::json!({
            "collections": [
                {
                    "name": "demo",
                    "dimension": 2,
                    "metric": "cosine",
                    "records": [
                        {"id":"a","vector":[1.0,0.0]}
                    ]
                }
            ]
        });
        std::fs::write(&path, serde_json::to_vec_pretty(&old).unwrap()).unwrap();

        let loaded = super::load_latest_snapshot_into(&store, dir.path()).unwrap();
        assert!(loaded.is_some());
        let stats = store.stats("demo").unwrap();
        assert_eq!(stats.count, 1);
    }
}
