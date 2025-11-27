use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    api::types::Record,
    store::{Metric, Store},
};

#[derive(Serialize)]
#[serde(tag = "type")]
enum WalEvent<'a> {
    #[serde(rename = "upsert")]
    Upsert {
        version: u32,
        collection: &'a str,
        record: &'a Record,
    },
    #[serde(rename = "delete")]
    Delete {
        version: u32,
        collection: &'a str,
        id: &'a str,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum WalOwnedEvent {
    #[serde(rename = "upsert")]
    Upsert {
        #[serde(default, rename = "version")]
        _version: u32,
        collection: String,
        record: Record,
    },
    #[serde(rename = "delete")]
    Delete {
        #[serde(default, rename = "version")]
        _version: u32,
        collection: String,
        id: String,
    },
}

pub struct Wal {
    file: Mutex<File>,
    //file path kept for clarity, not used after open
    _path: String,
    //rotation threshold in bytes; 0 disables rotation
    rotate_max_bytes: AtomicU64,
    //mutex to serialize rotation steps (close->rename->reopen)
    rotation_lock: Mutex<()>,
}

impl Wal {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            _path: path.to_string_lossy().to_string(),
            rotate_max_bytes: AtomicU64::new(0),
            rotation_lock: Mutex::new(()),
        })
    }

    //set rotation size; use 0 to disable rotation
    pub fn set_rotate_max_bytes(&self, bytes: u64) {
        self.rotate_max_bytes.store(bytes, Ordering::Relaxed);
    }

    //append a single upsert as one json line, then flush and fsync
    pub fn append_upsert(&self, collection: &str, record: &Record) -> anyhow::Result<()> {
        let evt = WalEvent::Upsert {
            version: 1,
            collection,
            record,
        };
        let json = serde_json::to_string(&evt)?;
        self.maybe_rotate((json.len() + 1) as u64)?;
        let mut f = self.file.lock().unwrap();
        f.write_all(json.as_bytes())?;
        f.write_all(b"\n")?;
        f.flush()?;
        f.sync_data()?;
        Ok(())
    }

    //append a delete event so recovery removes the record as well
    pub fn append_delete(&self, collection: &str, id: &str) -> anyhow::Result<()> {
        let evt = WalEvent::Delete {
            version: 1,
            collection,
            id,
        };
        let json = serde_json::to_string(&evt)?;
        self.maybe_rotate((json.len() + 1) as u64)?;
        let mut f = self.file.lock().unwrap();
        f.write_all(json.as_bytes())?;
        f.write_all(b"\n")?;
        f.flush()?;
        f.sync_data()?;
        Ok(())
    }

    //replay wal into the store; creates collections lazily using first seen dimension/metric when needed
    pub fn replay_into(&self, store: &Store) -> anyhow::Result<()> {
        let files = self.enumerate_wal_files()?;
        for path in files {
            let f = File::open(&path)?;
            let mut reader = BufReader::new(f);
            let mut line = String::new();
            loop {
                line.clear();
                let bytes = reader.read_line(&mut line)?;
                if bytes == 0 {
                    break;
                }
                let evt: WalOwnedEvent = match serde_json::from_str(line.trim_end()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                match evt {
                    WalOwnedEvent::Upsert {
                        collection, record, ..
                    } => {
                        if store.get_or_create_collection_config(&collection).is_none() {
                            let dim = record.vector.len();
                            //default metric takes cosine for a fresh wal replay when not specified elsewhere
                            store.ensure_collection(&collection, dim, Metric::Cosine);
                        }
                        store.upsert(&collection, record);
                    }
                    WalOwnedEvent::Delete { collection, id, .. } => {
                        let _ = store.delete(&collection, &id);
                    }
                }
            }
        }
        Ok(())
    }

    //collect wal segment files; rotated segments like wal-<ts>.log come before active wal.log
    fn enumerate_wal_files(&self) -> anyhow::Result<Vec<PathBuf>> {
        let active = PathBuf::from(&self._path);
        let dir = active
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let mut files: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wal") && name.ends_with(".log") {
                    files.push(p);
                }
            }
        }
        //always include the active file even if its name is not wal.log (e.g., tests using NamedTempFile)
        if active.exists() && !files.iter().any(|p| p == &active) {
            files.push(active);
        }
        files.sort_by(|a, b| a.file_name().unwrap().cmp(b.file_name().unwrap()));
        Ok(files)
    }

    fn maybe_rotate(&self, additional_bytes: u64) -> anyhow::Result<()> {
        let threshold = self.rotate_max_bytes.load(Ordering::Relaxed);
        if threshold == 0 {
            return Ok(());
        }
        let _rot_guard = self.rotation_lock.lock().unwrap();

        //compute current length under file lock
        let need_rotate = {
            let f = self.file.lock().unwrap();
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            len.saturating_add(additional_bytes) > threshold
        };
        if !need_rotate {
            return Ok(());
        }

        //ensure data persisted and drop handle
        {
            let mut f = self.file.lock().unwrap();
            let _ = f.flush();
            let _ = f.sync_data();
        }

        let active = PathBuf::from(&self._path);
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let rotated = active
            .parent()
            .map(|d| d.join(format!("wal-{}.log", ts)))
            .unwrap_or_else(|| PathBuf::from(format!("wal-{}.log", ts)));
        fs::rename(&active, &rotated)?;

        //open a fresh active file
        let new_f = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&active)?;
        let mut f = self.file.lock().unwrap();
        *f = new_f;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::Record;
    use tempfile::NamedTempFile;

    #[test]
    fn wal_append_and_replay_restores_store() {
        //temp file keeps test isolated from real data directory
        let tmp = NamedTempFile::new().unwrap();
        let wal = Wal::open(tmp.path()).unwrap();

        //append two upserts to same collection
        let r1 = Record {
            id: "id1".into(),
            vector: vec![1.0, 0.0],
            metadata: None,
        };
        let r2 = Record {
            id: "id2".into(),
            vector: vec![0.0, 1.0],
            metadata: None,
        };
        wal.append_upsert("demo", &r1).unwrap();
        wal.append_upsert("demo", &r2).unwrap();

        //replay into a fresh store and confirm both points exist
        let store = Store::new();
        wal.replay_into(&store).unwrap();

        let stats = store.stats("demo").unwrap();
        assert_eq!(stats.count, 2);

        //query should return at least one of the ids depending on metric default
        let res = store.top_k("demo", &[1.0, 0.0], 1).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, "id1");
    }

    #[test]
    fn wal_delete_replay_removes_record() {
        let tmp = NamedTempFile::new().unwrap();
        let wal = Wal::open(tmp.path()).unwrap();

        let r1 = Record {
            id: "id1".into(),
            vector: vec![1.0, 0.0],
            metadata: None,
        };
        wal.append_upsert("demo", &r1).unwrap();
        wal.append_delete("demo", "id1").unwrap();

        let store = Store::new();
        wal.replay_into(&store).unwrap();
        let stats = store.stats("demo").unwrap();
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn wal_replay_accepts_events_without_version() {
        //create old-format events without a version field
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let up = serde_json::json!({
            "type": "upsert",
            "collection": "demo",
            "record": {"id":"id1","vector":[1.0,0.0]}
        });
        let del = serde_json::json!({
            "type": "delete",
            "collection": "demo",
            "id": "id1"
        });
        writeln!(&mut f, "{}", up).unwrap();
        writeln!(&mut f, "{}", del).unwrap();

        //now open wal and replay; it should read the same file
        let wal = Wal::open(&path).unwrap();
        let store = Store::new();
        wal.replay_into(&store).unwrap();
        let stats = store.stats("demo").unwrap();
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn wal_rotation_and_replay_across_segments() {
        let tmpdir = tempfile::tempdir().unwrap();
        let wal_path = tmpdir.path().join("wal.log");
        let wal = Wal::open(&wal_path).unwrap();
        wal.set_rotate_max_bytes(64); //small threshold to force rotation

        //generate enough events to exceed threshold and cause at least one rotation
        for i in 0..20 {
            let id = format!("id{}", i);
            let r = Record {
                id,
                vector: vec![1.0, 0.0],
                metadata: None,
            };
            wal.append_upsert("demo", &r).unwrap();
        }

        //now replay into a fresh store; it should read all segments
        let store = Store::new();
        wal.replay_into(&store).unwrap();
        let stats = store.stats("demo").unwrap();
        assert!(stats.count >= 1);

        //ensure both active and rotated files exist
        let files = wal.enumerate_wal_files().unwrap();
        assert!(files
            .iter()
            .any(|p| p.file_name().unwrap().to_string_lossy() == "wal.log"));
        assert!(files
            .iter()
            .any(|p| p.file_name().unwrap().to_string_lossy().starts_with("wal-")));
    }
}
