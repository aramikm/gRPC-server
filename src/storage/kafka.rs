//! Kafka-backed view of the KV state.
//!
//! Records are produced to a single topic (one partition for ordering) with
//! `key = namespace\0id` and `value = Some(encoded DataEntry)` for puts or
//! `value = None` for tombstones (deletes). On startup we consume the topic
//! from offset 0 to the high watermark to rebuild an in-memory sorted map
//! that backs `Get` and `scan_prefix`. Subsequent writes update the map
//! locally after the produce is acknowledged — this assumes single-writer.

use std::collections::BTreeMap;
use std::sync::RwLock;

use chrono::Utc;
use rskafka::client::error::ProtocolError;
use rskafka::client::{
    partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
    Client, ClientBuilder,
};
use rskafka::record::Record;

use crate::error::{Error, Result};
use crate::kafka_config::KafkaConfig;
use crate::stats::StatsSnapshot;
use crate::storage::Storage;

/// Single partition because we rely on total ordering of Put/Delete records
/// per key. Multi-partition would require per-key partition routing and
/// per-partition state rebuilds.
const KAFKA_PARTITION: i32 = 0;

pub struct KafkaBackend {
    client: Client,
    partition_client: PartitionClient,
    cache: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl KafkaBackend {
    pub async fn open(config: &KafkaConfig) -> Result<Self> {
        let brokers: Vec<String> = config
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        if brokers.is_empty() {
            return Err(Error::Kafka("no bootstrap_servers configured".into()));
        }

        let client = ClientBuilder::new(brokers)
            .max_message_size(config.message_max_bytes)
            .build()
            .await
            .map_err(|e| Error::Kafka(format!("connect: {e}")))?;

        // Idempotent topic create. `TopicAlreadyExists` from a previous run
        // is the expected path on subsequent boots.
        let controller = client
            .controller_client()
            .map_err(|e| Error::Kafka(format!("controller: {e}")))?;
        match controller
            .create_topic(config.topic.clone(), 1, config.replication_factor, 5_000)
            .await
        {
            Ok(()) => tracing::info!("created Kafka topic {}", config.topic),
            Err(rskafka::client::error::Error::ServerError {
                protocol_error: ProtocolError::TopicAlreadyExists,
                ..
            }) => {}
            Err(e) => return Err(Error::Kafka(format!("create topic: {e}"))),
        }

        let partition_client = client
            .partition_client(
                config.topic.clone(),
                KAFKA_PARTITION,
                UnknownTopicHandling::Retry,
            )
            .await
            .map_err(|e| Error::Kafka(format!("partition client: {e}")))?;

        let cache = load_topic_into_cache(&partition_client, config.message_max_bytes).await?;

        Ok(Self {
            client,
            partition_client,
            cache: RwLock::new(cache),
        })
    }

    /// Liveness probe used by the availability watcher.
    pub async fn check_connection(&self) -> Result<()> {
        self.client
            .list_topics()
            .await
            .map(|_| ())
            .map_err(|e| Error::Kafka(format!("list_topics: {e}")))
    }

    async fn produce(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        let record = Record {
            key: Some(key),
            value,
            headers: Default::default(),
            timestamp: Utc::now(),
        };
        self.partition_client
            .produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| Error::Kafka(format!("produce: {e}")))?;
        Ok(())
    }
}

async fn load_topic_into_cache(
    partition_client: &PartitionClient,
    max_message_size: usize,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let high_watermark = partition_client
        .get_offset(OffsetAt::Latest)
        .await
        .map_err(|e| Error::Kafka(format!("get_offset latest: {e}")))?;
    let earliest = partition_client
        .get_offset(OffsetAt::Earliest)
        .await
        .map_err(|e| Error::Kafka(format!("get_offset earliest: {e}")))?;

    let mut next_offset = earliest;
    let mut cache = BTreeMap::new();
    let fetch_max = max_message_size.min(i32::MAX as usize) as i32;

    while next_offset < high_watermark {
        let (records, _hw) = partition_client
            .fetch_records(next_offset, 1..fetch_max, 1_000)
            .await
            .map_err(|e| Error::Kafka(format!("fetch_records @ {next_offset}: {e}")))?;
        if records.is_empty() {
            // Shouldn't happen below the high watermark, but break to avoid a
            // stuck loop if the broker disagrees with the watermark.
            break;
        }
        for ro in records {
            next_offset = ro.offset + 1;
            let Some(key) = ro.record.key else { continue };
            match ro.record.value {
                Some(v) => {
                    cache.insert(key, v);
                }
                None => {
                    cache.remove(&key);
                }
            }
        }
    }

    Ok(cache)
}

#[async_trait::async_trait]
impl Storage for KafkaBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .cache
            .read()
            .expect("cache lock poisoned")
            .get(key)
            .cloned())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.produce(key.to_vec(), Some(value.to_vec())).await?;
        self.cache
            .write()
            .expect("cache lock poisoned")
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<bool> {
        let existed = self
            .cache
            .read()
            .expect("cache lock poisoned")
            .contains_key(key);
        if !existed {
            return Ok(false);
        }
        self.produce(key.to_vec(), None).await?;
        self.cache.write().expect("cache lock poisoned").remove(key);
        Ok(true)
    }

    async fn scan_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let cache = self.cache.read().expect("cache lock poisoned");
        let start = start_key.unwrap_or(prefix).to_vec();
        let mut out = Vec::with_capacity(limit.min(1024));
        for (k, v) in cache.range(start..) {
            if !k.starts_with(prefix) {
                break;
            }
            if out.len() >= limit {
                break;
            }
            out.push((k.clone(), v.clone()));
        }
        Ok(out)
    }

    fn stats(&self) -> StatsSnapshot {
        // User-visible op counters live on `StorageManager`, which dispatches
        // to whichever backend is currently active.
        StatsSnapshot::default()
    }
}
