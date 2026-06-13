#![doc = include_str!("../README.md")]

/// HTTP route handlers and request/response types for the REST API.
pub mod api;
/// Shared Axum handler state: store, WAL, config, and server start time.
pub mod app_state;
/// Environment-driven server limits and paths (`YVDB_*` variables).
pub mod config;
/// WAL append log and bincode snapshots for disk durability.
pub mod persist;
/// In-memory vector collections, IVF index, and similarity search.
pub mod store;
