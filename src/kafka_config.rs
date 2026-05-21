use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Toggle the Kafka layer entirely. With `false` the server runs as a
    /// pure RocksDB KV store.
    pub enabled: bool,
    /// Comma-separated bootstrap servers (`host:port,host:port`).
    pub bootstrap_servers: String,
    /// Single topic holding every Put/Delete as a record. Records use
    /// `key = namespace\0id` and `value = Some(encoded DataEntry)` for puts
    /// or `None` for tombstones (deletes).
    pub topic: String,
    /// Replication factor used when the topic does not yet exist. Ignored on
    /// existing topics. Must be `<= broker count`.
    pub replication_factor: i16,
    /// How often the availability watcher pings Kafka, in seconds.
    pub health_check_interval_s: u64,
    /// How often the drain task replays RocksDB-buffered entries to Kafka
    /// after a recovery, in seconds.
    pub drain_interval_s: u64,
    /// Maximum produce/fetch message size in bytes.
    pub message_max_bytes: usize,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bootstrap_servers: "localhost:9092".into(),
            topic: "kv_entries".into(),
            replication_factor: 1,
            health_check_interval_s: 10,
            drain_interval_s: 5,
            message_max_bytes: 10 * 1024 * 1024,
        }
    }
}
