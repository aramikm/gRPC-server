//! Unified storage layer. RocksDB is the durable backing store; when Kafka is
//! enabled and reachable, it acts as the canonical read/write path and RocksDB
//! serves as a write-buffer during Kafka outages.

mod rocksdb;

#[cfg(feature = "kafka")]
mod availability;

#[cfg(feature = "kafka")]
mod kafka;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::config::Config;
use crate::error::Result;
use crate::stats::{Stats, StatsSnapshot};

pub use rocksdb::RocksDbBackend;

#[cfg(feature = "kafka")]
use availability::AvailabilityManager;
#[cfg(feature = "kafka")]
pub use kafka::KafkaBackend;

/// Unified storage interface used by `KvServiceImpl`. Backends — RocksDB,
/// Kafka, the orchestrating `StorageManager` — all implement this.
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<bool>;
    async fn scan_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn stats(&self) -> StatsSnapshot;
}

/// Coordinator that picks the active backend per call.
///
/// Lifecycle:
/// 1. Open RocksDB unconditionally.
/// 2. If Kafka is enabled, try to connect (with retries). On success, drain
///    any leftover RocksDB entries into Kafka so the in-memory cache becomes
///    the single source of truth.
/// 3. Spawn an availability watcher that latches Kafka off on the first
///    health-check failure (preventing stale reads from the now-divergent
///    cache). Re-enabling Kafka requires a process restart.
pub struct StorageManager {
    rocksdb: Arc<RocksDbBackend>,
    #[cfg(feature = "kafka")]
    kafka: Option<Arc<KafkaBackend>>,
    #[cfg(feature = "kafka")]
    availability: AvailabilityManager,
    /// User-visible operation counters. Tracked at the dispatch layer so the
    /// counts reflect actual served operations regardless of which backend
    /// (Kafka or RocksDB) handled the request.
    counters: Stats,
}

impl StorageManager {
    #[cfg(feature = "kafka")]
    pub async fn new(config: &Config) -> Result<Self> {
        let rocksdb = Arc::new(RocksDbBackend::open(&config.db)?);

        if !config.kafka.enabled {
            tracing::info!("Kafka disabled in config; running RocksDB-only.");
            return Ok(Self {
                rocksdb,
                kafka: None,
                availability: AvailabilityManager::disabled(),
                counters: Stats::default(),
            });
        }

        let kafka = match connect_kafka_with_retry(&config.kafka).await {
            Some(k) => Arc::new(k),
            None => {
                tracing::warn!(
                    "Kafka unreachable at startup; serving from RocksDB. \
                     Restart once Kafka is reachable to enable it."
                );
                return Ok(Self {
                    rocksdb,
                    kafka: None,
                    availability: AvailabilityManager::disabled(),
                    counters: Stats::default(),
                });
            }
        };

        // Drain leftover RocksDB entries (buffered during a previous outage)
        // into Kafka before we start serving reads from the Kafka cache.
        drain_rocksdb_into_kafka(&rocksdb, &kafka).await?;

        let availability = AvailabilityManager::watching(config.kafka.clone(), kafka.clone());

        Ok(Self {
            rocksdb,
            kafka: Some(kafka),
            availability,
            counters: Stats::default(),
        })
    }

    #[cfg(not(feature = "kafka"))]
    pub async fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            rocksdb: Arc::new(RocksDbBackend::open(&config.db)?),
            counters: Stats::default(),
        })
    }

    #[cfg(feature = "kafka")]
    fn active_backend(&self) -> &dyn Storage {
        match (&self.kafka, self.availability.kafka_available()) {
            (Some(k), true) => k.as_ref(),
            _ => self.rocksdb.as_ref(),
        }
    }

    #[cfg(not(feature = "kafka"))]
    fn active_backend(&self) -> &dyn Storage {
        self.rocksdb.as_ref()
    }
}

#[async_trait::async_trait]
impl Storage for StorageManager {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let v = self.active_backend().get(key).await?;
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        Ok(v)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.active_backend().put(key, value).await?;
        self.counters.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<bool> {
        let existed = self.active_backend().delete(key).await?;
        if existed {
            self.counters.deletes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(existed)
    }

    async fn scan_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let r = self
            .active_backend()
            .scan_prefix(prefix, start_key, limit)
            .await?;
        self.counters.lists.fetch_add(1, Ordering::Relaxed);
        Ok(r)
    }

    fn stats(&self) -> StatsSnapshot {
        self.counters.snapshot()
    }
}

#[cfg(feature = "kafka")]
async fn connect_kafka_with_retry(
    config: &crate::kafka_config::KafkaConfig,
) -> Option<KafkaBackend> {
    use std::time::Duration;

    // ~30s total: 8 attempts at 0, 1, 2, 4, 8, 8, 8, 8 seconds.
    let delays = [0u64, 1, 2, 4, 8, 8, 8, 8];
    for (i, delay) in delays.iter().enumerate() {
        if *delay > 0 {
            tokio::time::sleep(Duration::from_secs(*delay)).await;
        }
        match KafkaBackend::open(config).await {
            Ok(k) => {
                tracing::info!("Connected to Kafka after {} attempt(s)", i + 1);
                return Some(k);
            }
            Err(e) => {
                tracing::warn!("Kafka connect attempt {} failed: {e}", i + 1);
            }
        }
    }
    None
}

#[cfg(feature = "kafka")]
async fn drain_rocksdb_into_kafka(
    rocksdb: &Arc<RocksDbBackend>,
    kafka: &Arc<KafkaBackend>,
) -> Result<()> {
    let entries = rocksdb.iter_all().await?;
    if entries.is_empty() {
        return Ok(());
    }
    tracing::info!(
        count = entries.len(),
        "draining buffered RocksDB entries into Kafka"
    );
    for (k, v) in entries {
        kafka.put(&k, &v).await?;
        // Delete via Storage trait so the deletes counter is updated.
        Storage::delete(rocksdb.as_ref(), &k).await?;
    }
    Ok(())
}
