//! Tracks whether the Kafka backend is currently the canonical store.
//!
//! Latches downward: once a health check fails the flag flips to false and
//! stays there until the process restarts. Recovering mid-run would require
//! draining the RocksDB fallback buffer into Kafka first to avoid serving
//! stale reads — that path is intentionally out of scope.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::kafka_config::KafkaConfig;
use crate::storage::kafka::KafkaBackend;

pub struct AvailabilityManager {
    kafka_available: Arc<AtomicBool>,
    _health_task: Option<tokio::task::JoinHandle<()>>,
}

impl AvailabilityManager {
    /// Build a manager that's permanently in the "Kafka not in use" state.
    /// Used when kafka.enabled = false or the startup connection failed.
    pub fn disabled() -> Self {
        Self {
            kafka_available: Arc::new(AtomicBool::new(false)),
            _health_task: None,
        }
    }

    /// Build a manager that starts as `available = true` and downgrades to
    /// false the first time `check_connection` fails.
    pub fn watching(config: KafkaConfig, kafka: Arc<KafkaBackend>) -> Self {
        let flag = Arc::new(AtomicBool::new(true));
        let interval = Duration::from_secs(config.health_check_interval_s.max(1));
        let task_flag = flag.clone();
        let task_kafka = kafka.clone();

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate first tick — we just connected.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if !task_flag.load(Ordering::Relaxed) {
                    // Already latched off. Nothing more to do — keep the
                    // task alive only so its JoinHandle is well-defined.
                    continue;
                }
                if let Err(e) = task_kafka.check_connection().await {
                    tracing::warn!(
                        "Kafka health check failed: {e}. Failing over to RocksDB. \
                         Restart to re-enable Kafka."
                    );
                    task_flag.store(false, Ordering::Relaxed);
                }
            }
        });

        Self {
            kafka_available: flag,
            _health_task: Some(task),
        }
    }

    pub fn kafka_available(&self) -> bool {
        self.kafka_available.load(Ordering::Relaxed)
    }
}
