//! High-throughput gRPC key-value service backed by RocksDB.

pub mod config;
pub mod db;
pub mod error;
pub mod server;

pub mod pb {
    tonic::include_proto!("kv");
}

/// Serialized FileDescriptorSet for gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/kv_descriptor.bin"));

pub use config::{Config, DatabaseConfig, ServerConfig};
pub use db::{Database, StatsSnapshot};
pub use error::{Error, Result};
pub use server::KvServiceImpl;
