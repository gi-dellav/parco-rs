//! Core types, traits and macros shared by every Disco component.
//!
//! `disco-foundation` deliberately contains no runtime policy: it defines the
//! vocabulary (tasks, capabilities, devices, messages, queues, metrics) that
//! `disco-leader`, `disco-worker` and `disco-tester` build on.
//!
//! The procedural macros live in `disco-macros` and are re-exported here, so
//! downstream crates only ever need to depend on `disco-foundation`.

pub mod capability;
pub mod clock;
pub mod device;
pub mod error;
pub mod ids;
pub mod message;
pub mod metrics;
pub mod protocol;
pub mod queue;
pub mod task;

pub use disco_macros::{Capability, task, task_dispatcher, task_registry};

/// Common imports for building Disco networks.
pub mod prelude {
    pub use crate::capability::{Capability, CapabilityId, CapabilitySet};
    pub use crate::clock::{Clock, LogicalClock, SystemClock, Timestamp};
    pub use crate::device::{
        Device, DeviceId, DeviceInfo, DeviceRegistry, DeviceState, Heartbeat, HeartbeatConfig,
        HeartbeatSink, HeartbeatSource, Liveness, NodeRole, Transport,
    };
    pub use crate::error::{DiscoError, DispatchError, ProtocolError, TaskError, TransportError};
    pub use crate::message::{
        BincodeCodec, Envelope, Message, MessageCodec, MessageId, Register, RegisterAck,
    };
    pub use crate::metrics::{MetricKey, Metrics, MetricsSnapshot, NoopMetrics};
    pub use crate::protocol::ProtocolVersion;
    pub use crate::queue::{Priority, TaskEnvelope, TaskQueue};
    pub use crate::task::{
        CancelReason, CancellationToken, LocalDispatcher, Task, TaskAttempt, TaskContext,
        TaskDispatch, TaskId, TaskRegistry, TaskRequest, TaskResponse, TaskStatus,
    };
    pub use disco_macros::{Capability, task, task_dispatcher, task_registry};
}
