use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::capability::CapabilitySet;
use crate::clock::Timestamp;
use crate::error::TransportError;
use crate::message::Envelope;

/// Network-unique identifier for a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub uuid::Uuid);

impl DeviceId {
    /// Mint a new time-ordered (v7) identifier.
    pub fn new() -> Self {
        DeviceId(uuid::Uuid::now_v7())
    }

    pub fn as_u128(self) -> u128 {
        self.0.as_u128()
    }
}

impl Default for DeviceId {
    fn default() -> Self {
        DeviceId::new()
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Part a device plays in the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRole {
    Leader,
    Worker,
}

impl NodeRole {
    pub fn is_leader(self) -> bool {
        matches!(self, NodeRole::Leader)
    }

    pub fn is_worker(self) -> bool {
        matches!(self, NodeRole::Worker)
    }
}

/// Coarse liveness/connection state of a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeviceState {
    Connecting,
    Online,
    Degraded,
    Offline,
    ShuttingDown,
}

/// Static description of a known device plus its most recent liveness data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: DeviceId,
    pub role: NodeRole,
    pub addr: SocketAddr,
    pub capabilities: CapabilitySet,
    pub state: DeviceState,
    pub last_seen: Timestamp,
    pub metadata: HashMap<String, String>,
}

impl DeviceInfo {
    pub fn new(id: DeviceId, role: NodeRole, addr: SocketAddr) -> Self {
        DeviceInfo {
            id,
            role,
            addr,
            capabilities: CapabilitySet::new(),
            state: DeviceState::Connecting,
            last_seen: Timestamp::ZERO,
            metadata: HashMap::new(),
        }
    }

    pub fn supports(&self, required: &CapabilitySet) -> bool {
        self.capabilities.satisfies(required)
    }
}

/// Anything that can act as a network node.
pub trait Device: Send + Sync + 'static {
    fn id(&self) -> DeviceId;
    fn role(&self) -> NodeRole;
    fn capabilities(&self) -> &CapabilitySet;
    fn state(&self) -> DeviceState;
}

impl Device for DeviceInfo {
    fn id(&self) -> DeviceId {
        self.id
    }

    fn role(&self) -> NodeRole {
        self.role
    }

    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }

    fn state(&self) -> DeviceState {
        self.state
    }
}

// ---------------------------------------------------------------------------
// Heartbeats
// ---------------------------------------------------------------------------

/// Tuning knobs for heartbeat emission and expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// How often the node emits a heartbeat.
    pub interval_ms: u64,
    /// How long without a heartbeat before a peer is considered suspect.
    pub timeout_ms: u64,
    /// Consecutive missed timeouts before a peer is declared dead.
    pub max_missed: u32,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        HeartbeatConfig {
            interval_ms: 1_000,
            timeout_ms: 3_000,
            max_missed: 3,
        }
    }
}

/// A single heartbeat pulse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub device: DeviceId,
    pub seq: u64,
    pub sent_at: Timestamp,
}

impl Heartbeat {
    pub fn new(device: DeviceId, seq: u64, sent_at: Timestamp) -> Self {
        Heartbeat {
            device,
            seq,
            sent_at,
        }
    }
}

/// Result of evaluating a peer's heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Liveness {
    Alive,
    Suspect,
    Dead,
}

/// Emits a monotonically increasing heartbeat for a device.
pub trait HeartbeatSource: Send + Sync {
    fn next_heartbeat(&self) -> Heartbeat;
    fn config(&self) -> HeartbeatConfig;
}

/// Receives heartbeats from peers.
pub trait HeartbeatSink: Send + Sync {
    fn on_heartbeat(&self, heartbeat: Heartbeat, received_at: Timestamp) -> Liveness;
}

/// Tracks last-seen data for a set of peers and derives [`Liveness`].
#[derive(Debug)]
pub struct HeartbeatMonitor {
    config: HeartbeatConfig,
    tracked: RwLock<HashMap<DeviceId, Tracked>>,
}

#[derive(Debug, Clone, Copy)]
struct Tracked {
    last_seen: Timestamp,
    last_seq: u64,
    missed: u32,
}

impl HeartbeatMonitor {
    pub fn new(config: HeartbeatConfig) -> Self {
        HeartbeatMonitor {
            config,
            tracked: RwLock::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> HeartbeatConfig {
        self.config
    }

    pub fn register(&self, id: DeviceId, now: Timestamp) {
        self.tracked.write().unwrap().insert(
            id,
            Tracked {
                last_seen: now,
                last_seq: 0,
                missed: 0,
            },
        );
    }

    pub fn remove(&self, id: DeviceId) -> bool {
        self.tracked.write().unwrap().remove(&id).is_some()
    }

    pub fn observe(&self, id: DeviceId, heartbeat: &Heartbeat, received_at: Timestamp) -> Liveness {
        let mut tracked = self.tracked.write().unwrap();
        let entry = tracked.entry(id).or_insert(Tracked {
            last_seen: received_at,
            last_seq: heartbeat.seq,
            missed: 0,
        });
        entry.last_seen = received_at;
        entry.last_seq = heartbeat.seq;
        entry.missed = 0;
        Liveness::Alive
    }

    pub fn liveness(&self, id: DeviceId, now: Timestamp) -> Liveness {
        match self.tracked.read().unwrap().get(&id) {
            Some(entry) => evaluate(*entry, self.config, now),
            None => Liveness::Dead,
        }
    }

    /// Snapshot liveness for every tracked peer.
    pub fn poll(&self, now: Timestamp) -> Vec<(DeviceId, Liveness)> {
        self.tracked
            .read()
            .unwrap()
            .iter()
            .map(|(id, entry)| (*id, evaluate(*entry, self.config, now)))
            .collect()
    }

    pub fn expired(&self, now: Timestamp) -> Vec<DeviceId> {
        self.poll(now)
            .into_iter()
            .filter_map(|(id, liveness)| (liveness == Liveness::Dead).then_some(id))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.tracked.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn evaluate(entry: Tracked, config: HeartbeatConfig, now: Timestamp) -> Liveness {
    let elapsed = now.elapsed_since(entry.last_seen);
    if elapsed <= config.timeout_ms {
        Liveness::Alive
    } else if elapsed <= config.timeout_ms.saturating_mul(config.max_missed as u64) {
        Liveness::Suspect
    } else {
        Liveness::Dead
    }
}

// ---------------------------------------------------------------------------
// Device registry
// ---------------------------------------------------------------------------

/// Book-keeping for every device a node currently knows about.
pub trait DeviceRegistry: Send + Sync {
    fn register(&self, info: DeviceInfo);
    fn unregister(&self, id: DeviceId) -> Option<DeviceInfo>;
    fn get(&self, id: DeviceId) -> Option<DeviceInfo>;
    fn heartbeat(&self, id: DeviceId, heartbeat: &Heartbeat, now: Timestamp) -> Liveness;
    fn all(&self) -> Vec<DeviceInfo>;
    fn by_role(&self, role: NodeRole) -> Vec<DeviceInfo>;
    fn with_capabilities(&self, required: &CapabilitySet) -> Vec<DeviceInfo>;
    fn expired(&self, now: Timestamp) -> Vec<DeviceId>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-process [`DeviceRegistry`] backed by a `RwLock<HashMap<..>>`.
pub struct InMemoryDeviceRegistry {
    devices: RwLock<HashMap<DeviceId, DeviceInfo>>,
    monitor: HeartbeatMonitor,
}

impl Default for InMemoryDeviceRegistry {
    fn default() -> Self {
        InMemoryDeviceRegistry::new(HeartbeatConfig::default())
    }
}

impl InMemoryDeviceRegistry {
    pub fn new(config: HeartbeatConfig) -> Self {
        InMemoryDeviceRegistry {
            devices: RwLock::new(HashMap::new()),
            monitor: HeartbeatMonitor::new(config),
        }
    }

    pub fn monitor(&self) -> &HeartbeatMonitor {
        &self.monitor
    }
}

impl DeviceRegistry for InMemoryDeviceRegistry {
    fn register(&self, info: DeviceInfo) {
        self.monitor.register(info.id, info.last_seen);
        self.devices.write().unwrap().insert(info.id, info);
    }

    fn unregister(&self, id: DeviceId) -> Option<DeviceInfo> {
        self.monitor.remove(id);
        self.devices.write().unwrap().remove(&id)
    }

    fn get(&self, id: DeviceId) -> Option<DeviceInfo> {
        self.devices.read().unwrap().get(&id).cloned()
    }

    fn heartbeat(&self, id: DeviceId, heartbeat: &Heartbeat, now: Timestamp) -> Liveness {
        let liveness = self.monitor.observe(id, heartbeat, now);
        if let Some(info) = self.devices.write().unwrap().get_mut(&id) {
            info.last_seen = now;
        }
        liveness
    }

    fn all(&self) -> Vec<DeviceInfo> {
        self.devices.read().unwrap().values().cloned().collect()
    }

    fn by_role(&self, role: NodeRole) -> Vec<DeviceInfo> {
        self.devices
            .read()
            .unwrap()
            .values()
            .filter(|info| info.role == role)
            .cloned()
            .collect()
    }

    fn with_capabilities(&self, required: &CapabilitySet) -> Vec<DeviceInfo> {
        self.devices
            .read()
            .unwrap()
            .values()
            .filter(|info| info.supports(required))
            .cloned()
            .collect()
    }

    fn expired(&self, now: Timestamp) -> Vec<DeviceId> {
        self.monitor.expired(now)
    }

    fn len(&self) -> usize {
        self.devices.read().unwrap().len()
    }
}

// ---------------------------------------------------------------------------
// Transport (abstraction only)
// ---------------------------------------------------------------------------

/// TCP tuning shared by clients and listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TcpConfig {
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub nodelay: bool,
}

impl Default for TcpConfig {
    fn default() -> Self {
        TcpConfig {
            connect_timeout_ms: 5_000,
            read_timeout_ms: 10_000,
            write_timeout_ms: 10_000,
            nodelay: true,
        }
    }
}

/// A single framed connection to a peer.
pub trait Connection: Send + Sync {
    fn peer(&self) -> DeviceId;
    fn send(&self, envelope: &Envelope) -> Result<(), TransportError>;
    fn close(&self) -> Result<(), TransportError>;
}

/// Sends envelopes to known devices. TCP is the first target, but the trait is
/// transport-agnostic so tests can use an in-memory implementation.
pub trait Transport: Send + Sync {
    fn send(&self, to: DeviceId, envelope: &Envelope) -> Result<(), TransportError>;
    fn is_connected(&self, id: DeviceId) -> bool;
    fn connected(&self) -> Vec<DeviceId>;
    fn disconnect(&self, id: DeviceId) -> Result<(), TransportError>;
}

/// A device that participates in the network and emits heartbeats.
pub trait NetworkDevice: Device + HeartbeatSource {
    fn addr(&self) -> SocketAddr;
    fn registry(&self) -> &dyn DeviceRegistry;
}
