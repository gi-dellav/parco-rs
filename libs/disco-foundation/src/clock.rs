use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A point on a logical timeline, expressed in milliseconds.
///
/// It is intentionally opaque and cheap so it can travel inside every message.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp(0);

    pub const fn from_millis(ms: u64) -> Self {
        Timestamp(ms)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Saturating addition so a bad clock can never overflow into the past.
    pub const fn saturating_add(self, ms: u64) -> Self {
        Timestamp(self.0.saturating_add(ms))
    }

    pub const fn saturating_sub(self, ms: u64) -> Self {
        Timestamp(self.0.saturating_sub(ms))
    }

    /// Returns `true` when `self` has advanced past `earlier` by more than `timeout_ms`.
    pub const fn elapsed_since(self, earlier: Timestamp) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Wall-clock source used by devices and schedulers.
///
/// The trait exists so tests (and simulations in `disco-tester`) can substitute a
/// deterministic clock for [`SystemClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// Reads the host's monotonic-ish wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl SystemClock {
    pub const fn new() -> Self {
        SystemClock
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Timestamp(millis)
    }
}

/// A clock that only moves when it is told to. Intended for tests.
#[derive(Debug, Default)]
pub struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    pub fn new(start: Timestamp) -> Self {
        ManualClock {
            now_ms: AtomicU64::new(start.0),
        }
    }

    pub fn advance(&self, ms: u64) -> Timestamp {
        let next = self.now_ms.fetch_add(ms, Ordering::SeqCst) + ms;
        Timestamp(next)
    }

    pub fn set(&self, at: Timestamp) {
        self.now_ms.store(at.0, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now_ms.load(Ordering::SeqCst))
    }
}

/// A monotonic logical clock, as classically used to order events across nodes.
pub trait LogicalClock: Send + Sync {
    /// Advance the local time and return the new value.
    fn tick(&self) -> Timestamp;
    /// Merge a remote timestamp on message receipt, returning the new local value.
    fn observe(&self, remote: Timestamp) -> Timestamp;
    /// Read the current value without advancing it.
    fn now(&self) -> Timestamp;
}

/// A Lamport clock: `max(local, remote) + 1` on `observe`.
#[derive(Debug, Default)]
pub struct LamportClock {
    counter: AtomicU64,
}

impl LamportClock {
    pub const fn new() -> Self {
        LamportClock {
            counter: AtomicU64::new(0),
        }
    }
}

impl LogicalClock for LamportClock {
    fn tick(&self) -> Timestamp {
        Timestamp(self.counter.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn observe(&self, remote: Timestamp) -> Timestamp {
        let mut current = self.counter.load(Ordering::SeqCst);
        loop {
            let next = current.max(remote.0) + 1;
            match self
                .counter
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Timestamp(next),
                Err(actual) => current = actual,
            }
        }
    }

    fn now(&self) -> Timestamp {
        Timestamp(self.counter.load(Ordering::SeqCst))
    }
}

/// A hybrid logical clock that stays close to wall time while remaining monotonic.
#[derive(Debug, Default)]
pub struct HybridLogicalClock {
    last: AtomicU64,
}

impl HybridLogicalClock {
    pub const fn new() -> Self {
        HybridLogicalClock {
            last: AtomicU64::new(0),
        }
    }

    fn bump(&self, candidate: u64) -> Timestamp {
        let mut current = self.last.load(Ordering::SeqCst);
        loop {
            let next = current.max(candidate);
            match self
                .last
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Timestamp(next),
                Err(actual) => current = actual,
            }
        }
    }
}

impl LogicalClock for HybridLogicalClock {
    fn tick(&self) -> Timestamp {
        self.bump(SystemClock.now().0 + 1)
    }

    fn observe(&self, remote: Timestamp) -> Timestamp {
        self.bump(remote.0 + 1)
    }

    fn now(&self) -> Timestamp {
        Timestamp(self.last.load(Ordering::SeqCst))
    }
}

/// Placeholder for a named logical era (e.g. a leader term).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Epoch(pub u64);
