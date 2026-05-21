//! RocksDB storage backend. Blocking RocksDB calls are offloaded to
//! `tokio::task::spawn_blocking` to avoid stalling the async runtime.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocksdb::{
    DBCompressionType, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
};

use crate::config::{Compression, DatabaseConfig};
use crate::error::{Error, Result};
use crate::stats::{Stats, StatsSnapshot};

pub struct RocksDbBackend {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    stats: Arc<Stats>,
}

impl RocksDbBackend {
    pub fn open(config: &DatabaseConfig) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(config.create_if_missing);
        opts.set_max_open_files(config.max_open_files);
        opts.set_write_buffer_size(config.write_buffer_size);
        opts.set_compression_type(match config.compression {
            Compression::None => DBCompressionType::None,
            Compression::Snappy => DBCompressionType::Snappy,
            Compression::Lz4 => DBCompressionType::Lz4,
            Compression::Zstd => DBCompressionType::Zstd,
        });

        let db = DBWithThreadMode::<MultiThreaded>::open(&opts, &config.path)?;

        Ok(Self {
            db: Arc::new(db),
            stats: Arc::new(Stats::default()),
        })
    }

    /// Iterate every entry. Used by the drain task; not part of the Storage trait.
    pub async fn iter_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let mut out = Vec::new();
            for item in db.iterator(IteratorMode::Start) {
                let (k, v) = item?;
                out.push((k.into_vec(), v.into_vec()));
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(format!("task failed: {e}")))?
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }
}

#[async_trait::async_trait]
impl crate::storage::Storage for RocksDbBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = key.to_vec();
        let db = self.db.clone();
        let stats = self.stats.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let v = db.get(&key)?;
            stats.reads.fetch_add(1, Ordering::Relaxed);
            Ok(v)
        })
        .await
        .map_err(|e| Error::Internal(format!("task failed: {e}")))?
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = key.to_vec();
        let value = value.to_vec();
        let db = self.db.clone();
        let stats = self.stats.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            db.put(&key, &value)?;
            stats.writes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(format!("task failed: {e}")))?
    }

    async fn delete(&self, key: &[u8]) -> Result<bool> {
        let key = key.to_vec();
        let db = self.db.clone();
        let stats = self.stats.clone();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            // Existence probe uses the inner handle directly so it does NOT
            // advance the public `reads` counter. There's a unit test pinning
            // this — if you change it, update the test.
            let existed = db.get(&key)?.is_some();
            if existed {
                db.delete(&key)?;
                stats.deletes.fetch_add(1, Ordering::Relaxed);
            }
            Ok(existed)
        })
        .await
        .map_err(|e| Error::Internal(format!("task failed: {e}")))?
    }

    async fn scan_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix = prefix.to_vec();
        let start_key = start_key.map(|k| k.to_vec());
        let db = self.db.clone();
        let stats = self.stats.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let mode = match start_key.as_deref() {
                Some(k) => IteratorMode::From(k, Direction::Forward),
                None => IteratorMode::From(prefix.as_slice(), Direction::Forward),
            };

            let mut out = Vec::with_capacity(limit.min(1024));
            for item in db.iterator(mode) {
                let (k, v) = item?;
                if !k.starts_with(&prefix) {
                    break;
                }
                if out.len() >= limit {
                    break;
                }
                out.push((k.into_vec(), v.into_vec()));
            }
            stats.lists.fetch_add(1, Ordering::Relaxed);
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(format!("task failed: {e}")))?
    }

    fn stats(&self) -> StatsSnapshot {
        self.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::storage::Storage as _;

    fn temp_db() -> (RocksDbBackend, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cfg = DatabaseConfig {
            path: tmp.path().join("rocksdb").to_string_lossy().into_owned(),
            ..Default::default()
        };
        (RocksDbBackend::open(&cfg).unwrap(), tmp)
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let (db, _t) = temp_db();
        db.put(b"k", b"v").await.unwrap();
        assert_eq!(db.get(b"k").await.unwrap().as_deref(), Some(b"v".as_ref()));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (db, _t) = temp_db();
        assert_eq!(db.get(b"missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_reports_existed() {
        let (db, _t) = temp_db();
        db.put(b"k", b"v").await.unwrap();
        assert!(db.delete(b"k").await.unwrap());
        assert!(!db.delete(b"k").await.unwrap());
    }

    #[tokio::test]
    async fn scan_prefix_matches_only_prefix() {
        let (db, _t) = temp_db();
        db.put(b"a/1", b"1").await.unwrap();
        db.put(b"a/2", b"2").await.unwrap();
        db.put(b"b/1", b"3").await.unwrap();

        let r = db.scan_prefix(b"a/", None, 10).await.unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, b"a/1");
        assert_eq!(r[1].0, b"a/2");
    }

    #[tokio::test]
    async fn scan_prefix_respects_limit() {
        let (db, _t) = temp_db();
        for i in 0..10 {
            db.put(format!("k{i:02}").as_bytes(), b"v").await.unwrap();
        }
        let r = db.scan_prefix(b"k", None, 3).await.unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, b"k00");
        assert_eq!(r[2].0, b"k02");
    }

    #[tokio::test]
    async fn scan_prefix_resumes_from_start_key() {
        let (db, _t) = temp_db();
        for i in 0..5 {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        let r = db.scan_prefix(b"k", Some(b"k2"), 10).await.unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, b"k2");
    }

    #[tokio::test]
    async fn stats_track_operations() {
        let (db, _t) = temp_db();
        db.put(b"k", b"v").await.unwrap();
        db.get(b"k").await.unwrap();
        db.get(b"missing").await.unwrap();
        db.scan_prefix(b"", None, 10).await.unwrap();
        let before_delete = RocksDbBackend::stats(&db);
        assert_eq!(before_delete.writes, 1);
        assert_eq!(before_delete.reads, 2);
        assert_eq!(before_delete.lists, 1);

        db.delete(b"k").await.unwrap();
        let after = RocksDbBackend::stats(&db);
        assert_eq!(after.deletes, 1);
        // delete uses the inner rocksdb handle for its existence probe, so the
        // public read counter does not advance.
        assert_eq!(after.reads, before_delete.reads);
    }
}
