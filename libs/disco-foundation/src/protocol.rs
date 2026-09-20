use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;

/// Wire protocol revision negotiated during registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const CURRENT: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };

    pub const fn new(major: u16, minor: u16) -> Self {
        ProtocolVersion { major, minor }
    }

    /// Two versions are compatible when their major revisions match and the
    /// remote minor is not newer than ours.
    pub const fn is_compatible_with(self, remote: ProtocolVersion) -> bool {
        self.major == remote.major && remote.minor <= self.minor
    }

    pub fn ensure_compatible(
        self,
        remote: ProtocolVersion,
    ) -> Result<ProtocolVersion, ProtocolError> {
        if self.is_compatible_with(remote) {
            Ok(self.min(remote))
        } else {
            Err(ProtocolError::BadVersion {
                local: self.to_string(),
                remote: remote.to_string(),
            })
        }
    }

    pub const fn max(self, other: ProtocolVersion) -> Self {
        if self.major > other.major || (self.major == other.major && self.minor >= other.minor) {
            self
        } else {
            other
        }
    }

    pub const fn min(self, other: ProtocolVersion) -> Self {
        if self.major < other.major || (self.major == other.major && self.minor <= other.minor) {
            self
        } else {
            other
        }
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        ProtocolVersion::CURRENT
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Implemented by things whose serialized form carries a protocol revision.
pub trait Versioned {
    fn version(&self) -> ProtocolVersion;
}

/// Codes attached to a [`crate::message::Message::Error`] reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    Protocol,
    UnknownMessage,
    TaskNotFound,
    TaskRejected,
    Unauthorised,
    Internal,
}

impl From<&ProtocolError> for ErrorCode {
    fn from(error: &ProtocolError) -> Self {
        match error {
            ProtocolError::BadVersion { .. } => ErrorCode::Protocol,
            ProtocolError::UnknownMessage => ErrorCode::UnknownMessage,
            ProtocolError::Decode(_) => ErrorCode::Protocol,
            ProtocolError::Expired => ErrorCode::TaskRejected,
            ProtocolError::Unsupported => ErrorCode::Protocol,
        }
    }
}
