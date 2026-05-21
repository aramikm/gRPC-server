use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{
    DBCompressionType, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
};

use crate::config::{Compression, DatabaseConfig};
use crate::error::Result;

/// Thread-safe RocksDB wrapper. All methods take `&self`, so wrap in `Arc` and
/// share across tasks.
pub struct Database {
    db: DBWithThreadMode<MultiThreaded>,
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    reads: AtomicU64,
    writes: AtomicU64,
    deletes: AtomicU64,
    lists: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub lists: u64,
}

impl Database {
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
            db,
            stats: Stats::default(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let v = self.db.get(key)?;
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        Ok(v)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)?;
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Delete `key`. Returns whether the key existed prior to deletion.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let existed = self.db.get(key)?.is_some();
        if existed {
            self.db.delete(key)?;
            self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(existed)
    }

    /// Iterate over keys beginning with `prefix`. If `start_key` is provided,
    /// iteration begins at it (inclusive); otherwise it begins at the first key
    /// matching the prefix. Returns at most `limit` pairs.
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        start_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mode = match start_key {
            Some(k) => IteratorMode::From(k, Direction::Forward),
            None => IteratorMode::From(prefix, Direction::Forward),
        };

        let mut out = Vec::with_capacity(limit.min(1024));
        for item in self.db.iterator(mode) {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            if out.len() >= limit {
                break;
            }
            out.push((k.into_vec(), v.into_vec()));
        }
        self.stats.lists.fetch_add(1, Ordering::Relaxed);
        Ok(out)
    }

    pub fn stats(&self) -> StatsSnapshot {
        StatsSnapshot {
            reads: self.stats.reads.load(Ordering::Relaxed),
            writes: self.stats.writes.load(Ordering::Relaxed),
            deletes: self.stats.deletes.load(Ordering::Relaxed),
            lists: self.stats.lists.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_db() -> (Database, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cfg = DatabaseConfig {
            path: tmp.path().join("rocksdb").to_string_lossy().into_owned(),
            ..Default::default()
        };
        (Database::open(&cfg).unwrap(), tmp)
    }

    #[test]
    fn put_get_roundtrip() {
        let (db, _t) = temp_db();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn get_missing_returns_none() {
        let (db, _t) = temp_db();
        assert_eq!(db.get(b"missing").unwrap(), None);
    }

    #[test]
    fn delete_reports_existed() {
        let (db, _t) = temp_db();
        db.put(b"k", b"v").unwrap();
        assert!(db.delete(b"k").unwrap());
        assert!(!db.delete(b"k").unwrap());
    }

    #[test]
    fn scan_prefix_matches_only_prefix() {
        let (db, _t) = temp_db();
        db.put(b"a/1", b"1").unwrap();
        db.put(b"a/2", b"2").unwrap();
        db.put(b"b/1", b"3").unwrap();

        let r = db.scan_prefix(b"a/", None, 10).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, b"a/1");
        assert_eq!(r[1].0, b"a/2");
    }

    #[test]
    fn scan_prefix_respects_limit() {
        let (db, _t) = temp_db();
        for i in 0..10 {
            db.put(format!("k{i:02}").as_bytes(), b"v").unwrap();
        }
        let r = db.scan_prefix(b"k", None, 3).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, b"k00");
        assert_eq!(r[2].0, b"k02");
    }

    #[test]
    fn scan_prefix_resumes_from_start_key() {
        let (db, _t) = temp_db();
        for i in 0..5 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        let r = db.scan_prefix(b"k", Some(b"k2"), 10).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, b"k2");
    }

    #[test]
    fn stats_track_operations() {
        let (db, _t) = temp_db();
        db.put(b"k", b"v").unwrap();
        db.get(b"k").unwrap();
        db.get(b"missing").unwrap();
        db.scan_prefix(b"", None, 10).unwrap();
        let before_delete = db.stats();
        assert_eq!(before_delete.writes, 1);
        assert_eq!(before_delete.reads, 2);
        assert_eq!(before_delete.lists, 1);

        db.delete(b"k").unwrap();
        let after = db.stats();
        assert_eq!(after.deletes, 1);
        // delete uses the inner rocksdb handle for its existence probe, so the
        // public read counter does not advance.
        assert_eq!(after.reads, before_delete.reads);
    }
}
