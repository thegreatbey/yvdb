pub struct Config {
    pub data_dir: String,
    pub max_dimension: usize,
    pub max_batch: usize,
    pub max_k: usize,
    pub snapshot_interval_secs: u64,
    //number of snapshots kept on disk; older snapshots are pruned
    pub snapshot_retention: usize,
    pub wal_rotate_max_bytes: u64,
    pub request_timeout_ms: u64,
    pub max_request_bytes: usize,
    pub snapshot_on_shutdown: bool,
    //address and port used by the HTTP server; defaults to localhost:8080 for safety
    pub bind_addr: String,
}

impl Config {
    pub fn from_env() -> Self {
        //defaults are small to protect memory on early experiments
        let data_dir = std::env::var("YVDB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        let max_dimension = env_usize("YVDB_MAX_DIMENSION", 4096);
        let max_batch = env_usize("YVDB_MAX_BATCH", 1024);
        let max_k = env_usize("YVDB_MAX_K", 1000);
        let snapshot_interval_secs = env_u64("YVDB_SNAPSHOT_INTERVAL_SECS", 30);
        let snapshot_retention = env_usize("YVDB_SNAPSHOT_RETENTION", 3);
        let wal_rotate_max_bytes = env_u64("YVDB_WAL_ROTATE_MAX_BYTES", 0);
        let request_timeout_ms = env_u64("YVDB_REQUEST_TIMEOUT_MS", 2000);
        let max_request_bytes = env_usize("YVDB_MAX_REQUEST_BYTES", 1_048_576);
        let snapshot_on_shutdown = env_bool("YVDB_SNAPSHOT_ON_SHUTDOWN", false);
        //allow overriding the listen address for Docker or remote deployment
        let bind_addr =
            std::env::var("YVDB_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        Self {
            data_dir,
            max_dimension,
            max_batch,
            max_k,
            snapshot_interval_secs,
            snapshot_retention,
            wal_rotate_max_bytes,
            request_timeout_ms,
            max_request_bytes,
            snapshot_on_shutdown,
            bind_addr,
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}
