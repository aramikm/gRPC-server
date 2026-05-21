//! High-throughput gRPC key-value service with Kafka + RocksDB fallback.

pub mod config;
pub mod error;
pub mod kafka_config;
pub mod server;
pub mod stats;
pub mod storage;

pub mod pb {
    tonic::include_proto!("kv");
}

/// Serialized FileDescriptorSet for gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/kv_descriptor.bin"));

pub use config::{Config, DatabaseConfig, ServerConfig};
pub use error::{Error, Result};
pub use kafka_config::KafkaConfig;
pub use server::KvServiceImpl;
pub use stats::StatsSnapshot;
pub use storage::{RocksDbBackend, Storage, StorageManager};
