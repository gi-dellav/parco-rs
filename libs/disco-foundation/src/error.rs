use std::io;

use serde::{Deserialize, Serialize};

use crate::device::DeviceId;
use crate::queue::QueueError;

/// Errors produced while executing a [`crate::task::Task`].
///
/// This type is serializable so it can be carried back to the Leader in a
/// [`crate::task::TaskResponse`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum TaskError {
    #[error("task execution failed: {0}")]
    Execution(String),
    #[error("task was cancelled")]
    Cancelled,
    #[error("failed to encode task output: {0}")]
    Encode(String),
    #[error("failed to decode task input: {0}")]
    Decode(String),
}

/// Errors produced by a [`crate::task::TaskDispatch`] implementation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum DispatchError {
    #[error("no task registered under name `{0}`")]
    TaskNotFound(String),
    #[error("failed to encode request: {0}")]
    Encoding(String),
    #[error("failed to decode request: {0}")]
    Decoding(String),
    #[error("task execution failed: {0}")]
    Execution(String),
    #[error("task `{0}` timed out")]
    Timeout(String),
    #[error("task was cancelled")]
    Cancelled,
    #[error("task does not support cancellation")]
    CancellationUnsupported,
    #[error("dispatcher is overloaded")]
    Overloaded,
}

/// Errors produced by the on-the-wire protocol / codec layer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum ProtocolError {
    #[error("incompatible protocol version (local {local}, remote {remote})")]
    BadVersion { local: String, remote: String },
    #[error("unknown or malformed message")]
    UnknownMessage,
    #[error("failed to decode message: {0}")]
    Decode(String),
    #[error("message expired before it could be delivered")]
    Expired,
    #[error("operation is not supported by this protocol revision")]
    Unsupported,
}

/// Errors produced by the transport layer (TCP, in-memory, ...).
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("connection closed by peer")]
    Closed,
    #[error("not connected to device {0}")]
    NotConnected(DeviceId),
    #[error(transparent)]
    Codec(#[from] ProtocolError),
}

/// Top-level error that aggregates every failure mode exposed by the foundation.
#[derive(Debug, thiserror::Error)]
pub enum DiscoError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Task(#[from] TaskError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Queue(#[from] QueueError),
}

pub type Result<T, E = DiscoError> = std::result::Result<T, E>;
