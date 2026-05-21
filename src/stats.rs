use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Stats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub lists: AtomicU64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub lists: u64,
}
