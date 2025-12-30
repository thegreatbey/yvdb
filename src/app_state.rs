// src/app_state.rs
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::store::Store;
use crate::persist::wal::Wal;
use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub wal: Arc<Wal>,
    pub config: Arc<Config>,
    pub query_stats: Arc<RwLock<QueryStats>>,
    pub start_time: Instant,
}

#[derive(Default, Clone)]
pub struct QueryStats {
    pub total: usize,
    pub success: usize,
}