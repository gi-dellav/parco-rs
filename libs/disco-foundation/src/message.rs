use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::capability::{CapabilityAdvert, CapabilitySet};
use crate::clock::Timestamp;
use crate::device::{DeviceId, Heartbeat, NodeRole};
use crate::error::ProtocolError;
use crate::protocol::{ErrorCode, ProtocolVersion, Versioned};
use crate::task::{CancelReason, TaskId, TaskRequest, TaskResponse};

/// Unique identifier for an envelope, used for correlation and de-duplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MessageId(pub uuid::Uuid);

impl MessageId {
    pub fn new() -> Self {
        MessageId(uuid::Uuid::now_v7())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        MessageId::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// First message a node sends when joining the network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Register {
    pub device: DeviceId,
    pub role: NodeRole,
    pub capabilities: CapabilitySet,
    pub listen_addr: SocketAddr,
    pub protocol: ProtocolVersion,
}

/// Leader's answer to [`Register`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterAck {
    pub accepted: bool,
    pub leader: DeviceId,
    pub protocol: ProtocolVersion,
    pub reason: Option<String>,
}

/// Reply to a [`crate::device::Heartbeat`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatAck {
    pub seq: u64,
    pub at: Timestamp,
}

/// Ask a worker to stop a running task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancel {
    pub task_id: TaskId,
    pub reason: CancelReason,
}

/// Worker's acknowledgement that a task is (or will be) cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelled {
    pub task_id: TaskId,
    pub reason: CancelReason,
}

/// Graceful shutdown notice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shutdown {
    pub reason: String,
}

/// Structured protocol-level error reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub code: ErrorCode,
    pub message: String,
}

/// Everything that can travel over the wire, independent of the transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    Register(Register),
    RegisterAck(RegisterAck),
    Heartbeat(Heartbeat),
    HeartbeatAck(HeartbeatAck),
    TaskRequest(TaskRequest),
    TaskResponse(TaskResponse),
    TaskCancel(TaskCancel),
    TaskCancelled(TaskCancelled),
    CapabilityUpdate(CapabilityAdvert),
    Shutdown(Shutdown),
    Error(ErrorMessage),
}

impl Message {
    /// `true` for messages that must be answered.
    pub fn expects_reply(&self) -> bool {
        matches!(
            self,
            Message::Register(_)
                | Message::Heartbeat(_)
                | Message::TaskRequest(_)
                | Message::TaskCancel(_)
        )
    }

    /// `true` for messages that are safe to drop when the network is saturated.
    pub fn is_lossy(&self) -> bool {
        matches!(self, Message::Heartbeat(_) | Message::HeartbeatAck(_))
    }
}

/// Routing wrapper carrying addressing, ordering and protocol metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: MessageId,
    pub from: DeviceId,
    /// `None` means broadcast to every known peer.
    pub to: Option<DeviceId>,
    pub correlation: Option<MessageId>,
    pub sent_at: Timestamp,
    /// Time-to-live in milliseconds; `None` means "no expiry".
    pub ttl_ms: Option<u64>,
    pub protocol: ProtocolVersion,
    pub payload: Message,
}

impl Envelope {
    pub fn new(from: DeviceId, to: DeviceId, payload: Message, sent_at: Timestamp) -> Self {
        Envelope {
            id: MessageId::new(),
            from,
            to: Some(to),
            correlation: None,
            sent_at,
            ttl_ms: None,
            protocol: ProtocolVersion::CURRENT,
            payload,
        }
    }

    pub fn broadcast(from: DeviceId, payload: Message, sent_at: Timestamp) -> Self {
        Envelope {
            to: None,
            ..Envelope::new(from, from, payload, sent_at)
        }
    }

    pub fn in_reply_to(mut self, correlation: MessageId) -> Self {
        self.correlation = Some(correlation);
        self
    }

    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = Some(ttl_ms);
        self
    }

    pub fn is_expired(&self, now: Timestamp) -> bool {
        match self.ttl_ms {
            Some(ttl) => now.elapsed_since(self.sent_at) > ttl,
            None => false,
        }
    }
}

impl Versioned for Envelope {
    fn version(&self) -> ProtocolVersion {
        self.protocol
    }
}

/// Swappable serialization strategy for envelopes.
///
/// The default is [`BincodeCodec`]; a JSON or postcard codec can implement this
/// same trait later without touching the transport.
pub trait MessageCodec: Send + Sync {
    fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, ProtocolError>;
    fn decode(&self, bytes: &[u8]) -> Result<Envelope, ProtocolError>;
}

/// Compact binary codec backed by `bincode`.
#[derive(Debug, Default, Clone, Copy)]
pub struct BincodeCodec;

impl BincodeCodec {
    pub const fn new() -> Self {
        BincodeCodec
    }
}

impl MessageCodec for BincodeCodec {
    fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(envelope).map_err(|e| ProtocolError::Decode(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Envelope, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Decode(e.to_string()))
    }
}

/// Prepends a 4-byte big-endian length so the stream can be framed.
pub fn encode_frame(
    codec: &dyn MessageCodec,
    envelope: &Envelope,
) -> Result<Vec<u8>, ProtocolError> {
    let body = codec.encode(envelope)?;
    let len = (body.len() as u32).to_be_bytes();
    let mut framed = Vec::with_capacity(body.len() + 4);
    framed.extend_from_slice(&len);
    framed.extend_from_slice(&body);
    Ok(framed)
}
