use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::store::{Metric, Store};

//fixed-width ints so bincode round-trips match on read and write
fn serialize_snap(snap: &Snapshot) -> anyhow::Result<Vec<u8>> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(snap)
        .map_err(Into::into)
}

fn deserialize_snap(bytes: &[u8]) -> anyhow::Result<Snapshot> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .deserialize(bytes)
        .map_err(Into::into)
}

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
    //metadata stored as json text so bincode can round-trip without serde_json::Value
    #[serde(default)]
    metadata: Option<String>,
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
                //string form works with bincode; Value would break deserialize
                metadata: r.metadata.map(|v| v.to_string()),
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
    //.bin + bincode keeps files smaller than json and loads faster on restart
    let path = dir.join(format!("snapshot-{}.bin", ts));
    let mut f = fs::File::create(&path)?;
    //magic bytes let startup reject corrupted or wrong file types before bincode decode
    let mut data = b"TOON".to_vec();
    let bin = serialize_snap(&snap)?;
    data.extend_from_slice(&bin);
    f.write_all(&data)?;
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
    //first 4 bytes must match TOON so we never feed garbage into bincode
    if bytes.len() < 4 || &bytes[0..4] != b"TOON" {
        return Err(anyhow::anyhow!("invalid snapshot magic header"));
    }
    let snap: Snapshot = deserialize_snap(&bytes[4..])?;

    for col in snap.collections {
        let metric = col
            .metric
            .parse::<Metric>()
            .map_err(|e| anyhow::anyhow!(e))?;
        store.ensure_collection(&col.name, col.dimension, metric);
        for r in col.records {
            store.upsert(
                &col.name,
                crate::api::types::Record {
                    id: r.id,
                    vector: r.vector,
                    //turn stored json text back into Value for the live store
                    metadata: r.metadata.and_then(|s| serde_json::from_str(&s).ok()),
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
        let count = store2
            .list_all_stats()
            .into_iter()
            .find(|(n, _)| n == "demo")
            .map(|(_, s)| s.count)
            .unwrap_or(0);
        assert_eq!(count, 2);
    }

    #[test]
    fn snapshot_loads_without_version_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new();

        //write a minimal binary snapshot with default version (0) and TOON magic
        std::fs::create_dir_all(dir.path()).unwrap();
        let path = dir.path().join("snapshot-old.bin");
        let snap = Snapshot {
            version: 0,
            collections: vec![CollectionSnap {
                name: "demo".into(),
                dimension: 2,
                metric: "cosine".into(),
                records: vec![RecordSnap {
                    id: "a".into(),
                    vector: vec![1.0, 0.0],
                    metadata: None,
                }],
            }],
        };
        let mut data = b"TOON".to_vec();
        data.extend_from_slice(&serialize_snap(&snap).unwrap());
        std::fs::write(&path, data).unwrap();

        let loaded = super::load_latest_snapshot_into(&store, dir.path()).unwrap();
        assert!(loaded.is_some());
        let count = store
            .list_all_stats()
            .into_iter()
            .find(|(n, _)| n == "demo")
            .map(|(_, s)| s.count)
            .unwrap_or(0);
        assert_eq!(count, 1);
    }
}
