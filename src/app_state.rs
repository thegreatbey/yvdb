//src/app_state.rs
//shared handles passed into every axum handler (store, wal, config, server start time)
use std::sync::Arc;
use std::time::Instant;

use crate::config::Config;
use crate::persist::wal::Wal;
use crate::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub wal: Arc<Wal>,
    pub config: Arc<Config>,
    pub start_time: Instant,
}
