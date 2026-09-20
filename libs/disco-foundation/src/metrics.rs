use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Name under which a metric is reported.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MetricKey(pub String);

impl MetricKey {
    pub fn new(name: impl Into<String>) -> Self {
        MetricKey(name.into())
    }
}

impl From<&str> for MetricKey {
    fn from(value: &str) -> Self {
        MetricKey(value.to_string())
    }
}

/// A single metric reading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    /// Accumulated observations (e.g. latencies in milliseconds).
    Histogram(Vec<f64>),
}

/// Point-in-time view of every metric, suitable for shipping to a leader.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub values: BTreeMap<String, MetricValue>,
}

impl MetricsSnapshot {
    pub fn counter(&self, key: &str) -> Option<u64> {
        match self.values.get(key) {
            Some(MetricValue::Counter(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn gauge(&self, key: &str) -> Option<i64> {
        match self.values.get(key) {
            Some(MetricValue::Gauge(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn histogram(&self, key: &str) -> Option<&[f64]> {
        match self.values.get(key) {
            Some(MetricValue::Histogram(values)) => Some(values),
            _ => None,
        }
    }
}

/// Sink for runtime metrics. Implement this to plug in your observability stack.
pub trait Metrics: Send + Sync {
    fn increment(&self, key: &MetricKey, by: u64);
    fn gauge(&self, key: &MetricKey, value: i64);
    fn observe(&self, key: &MetricKey, value: f64);
    fn snapshot(&self) -> MetricsSnapshot;
}

/// Drops every reading. Useful as the default when no collector is configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn increment(&self, _key: &MetricKey, _by: u64) {}
    fn gauge(&self, _key: &MetricKey, _value: i64) {}
    fn observe(&self, _key: &MetricKey, _value: f64) {}
    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot::default()
    }
}

/// A simple in-process [`Metrics`] implementation.
#[derive(Debug, Default)]
pub struct InMemoryMetrics {
    counters: Mutex<BTreeMap<String, u64>>,
    gauges: Mutex<BTreeMap<String, i64>>,
    histograms: Mutex<BTreeMap<String, Vec<f64>>>,
}

impl Metrics for InMemoryMetrics {
    fn increment(&self, key: &MetricKey, by: u64) {
        *self
            .counters
            .lock()
            .unwrap()
            .entry(key.0.clone())
            .or_insert(0) += by;
    }

    fn gauge(&self, key: &MetricKey, value: i64) {
        self.gauges.lock().unwrap().insert(key.0.clone(), value);
    }

    fn observe(&self, key: &MetricKey, value: f64) {
        self.histograms
            .lock()
            .unwrap()
            .entry(key.0.clone())
            .or_default()
            .push(value);
    }

    fn snapshot(&self) -> MetricsSnapshot {
        let mut values = BTreeMap::new();
        for (key, value) in self.counters.lock().unwrap().iter() {
            values.insert(key.clone(), MetricValue::Counter(*value));
        }
        for (key, value) in self.gauges.lock().unwrap().iter() {
            values.insert(key.clone(), MetricValue::Gauge(*value));
        }
        for (key, value) in self.histograms.lock().unwrap().iter() {
            values.insert(key.clone(), MetricValue::Histogram(value.clone()));
        }
        MetricsSnapshot { values }
    }
}

/// Counters describing task processing on a device.
#[derive(Debug, Default)]
pub struct TaskMetrics {
    pub dispatched: AtomicU64,
    pub completed: AtomicU64,
    pub failed: AtomicU64,
    pub cancelled: AtomicU64,
    pub in_flight: AtomicI64,
}

impl TaskMetrics {
    pub fn on_dispatch(&self) {
        self.dispatched.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_complete(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn on_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn on_cancel(&self) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Counters describing device liveness and transport activity.
#[derive(Debug, Default)]
pub struct DeviceMetrics {
    pub heartbeats_sent: AtomicU64,
    pub heartbeats_received: AtomicU64,
    pub heartbeats_missed: AtomicU64,
    pub reconnects: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

/// Small helper for measuring durations and recording them as a metric.
#[derive(Debug)]
pub struct Timer {
    started: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Timer {
            started: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.started.elapsed().as_secs_f64() * 1000.0
    }

    pub fn observe(&self, metrics: &dyn Metrics, key: &MetricKey) {
        metrics.observe(key, self.elapsed_ms());
    }
}
