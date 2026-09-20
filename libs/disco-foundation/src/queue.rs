use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::capability::CapabilitySet;
use crate::clock::Timestamp;
use crate::task::{TaskAttempt, TaskId};

/// Scheduling priority. Higher values are dequeued first.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Priority(pub i32);

impl Priority {
    pub const LOW: Priority = Priority(-10);
    pub const NORMAL: Priority = Priority(0);
    pub const HIGH: Priority = Priority(10);
}

/// A unit of deferred work: a task request plus scheduling metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEnvelope {
    pub task_id: TaskId,
    pub name: String,
    pub payload: Vec<u8>,
    pub attempt: TaskAttempt,
    pub priority: Priority,
    pub enqueued_at: Timestamp,
    pub deadline: Option<Timestamp>,
    pub required: CapabilitySet,
    pub cancellable: bool,
}

impl TaskEnvelope {
    pub fn new(task_id: TaskId, name: impl Into<String>, payload: Vec<u8>) -> Self {
        TaskEnvelope {
            task_id,
            name: name.into(),
            payload,
            attempt: TaskAttempt::FIRST,
            priority: Priority::NORMAL,
            enqueued_at: Timestamp::ZERO,
            deadline: None,
            required: CapabilitySet::new(),
            cancellable: false,
        }
    }
}

impl Ord for TaskEnvelope {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            // Earlier arrivals win ties so the heap stays FIFO within a priority.
            .then_with(|| other.enqueued_at.cmp(&self.enqueued_at))
            .then_with(|| other.task_id.cmp(&self.task_id))
    }
}

impl PartialOrd for TaskEnvelope {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum QueueError {
    #[error("queue is empty")]
    Empty,
    #[error("queue is full")]
    Full,
    #[error("task {0} is already enqueued")]
    Duplicate(TaskId),
    #[error("task {0} was not found")]
    NotFound(TaskId),
}

/// Abstract work queue used to hold pending [`TaskEnvelope`]s.
///
/// Kept object-safe so transports can hold a `Box<dyn TaskQueue>`.
pub trait TaskQueue: Send + Sync {
    fn push(&self, envelope: TaskEnvelope) -> Result<(), QueueError>;
    /// Remove and return the highest-priority envelope, marking it in-flight.
    fn pop(&self) -> Option<TaskEnvelope>;
    fn peek(&self) -> Option<TaskEnvelope>;
    /// Mark an in-flight envelope as completed.
    fn ack(&self, task_id: TaskId) -> Result<(), QueueError>;
    /// Return an in-flight envelope to the queue, or drop it if `requeue` is false.
    fn nack(&self, task_id: TaskId, requeue: bool) -> Result<(), QueueError>;
    /// Remove a pending (not yet popped) envelope.
    fn cancel(&self, task_id: TaskId) -> Result<(), QueueError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-process priority queue with an in-flight set for ack/nack semantics.
pub struct InMemoryTaskQueue {
    capacity: Option<usize>,
    state: Mutex<QueueState>,
}

struct QueueState {
    ready: BinaryHeap<TaskEnvelope>,
    inflight: HashMap<TaskId, TaskEnvelope>,
}

impl Default for InMemoryTaskQueue {
    fn default() -> Self {
        InMemoryTaskQueue::new()
    }
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        InMemoryTaskQueue::unbounded()
    }

    pub fn unbounded() -> Self {
        InMemoryTaskQueue {
            capacity: None,
            state: Mutex::new(QueueState {
                ready: BinaryHeap::new(),
                inflight: HashMap::new(),
            }),
        }
    }

    pub fn bounded(capacity: usize) -> Self {
        InMemoryTaskQueue {
            capacity: Some(capacity),
            state: Mutex::new(QueueState {
                ready: BinaryHeap::new(),
                inflight: HashMap::new(),
            }),
        }
    }

    pub fn inflight(&self) -> usize {
        self.state.lock().unwrap().inflight.len()
    }
}

impl TaskQueue for InMemoryTaskQueue {
    fn push(&self, envelope: TaskEnvelope) -> Result<(), QueueError> {
        let mut state = self.state.lock().unwrap();
        if let Some(capacity) = self.capacity
            && state.ready.len() >= capacity
        {
            return Err(QueueError::Full);
        }
        if state.inflight.contains_key(&envelope.task_id)
            || state.ready.iter().any(|e| e.task_id == envelope.task_id)
        {
            return Err(QueueError::Duplicate(envelope.task_id));
        }
        state.ready.push(envelope);
        Ok(())
    }

    fn pop(&self) -> Option<TaskEnvelope> {
        let mut state = self.state.lock().unwrap();
        let envelope = state.ready.pop()?;
        state.inflight.insert(envelope.task_id, envelope.clone());
        Some(envelope)
    }

    fn peek(&self) -> Option<TaskEnvelope> {
        self.state.lock().unwrap().ready.peek().cloned()
    }

    fn ack(&self, task_id: TaskId) -> Result<(), QueueError> {
        self.state
            .lock()
            .unwrap()
            .inflight
            .remove(&task_id)
            .map(|_| ())
            .ok_or(QueueError::NotFound(task_id))
    }

    fn nack(&self, task_id: TaskId, requeue: bool) -> Result<(), QueueError> {
        let mut state = self.state.lock().unwrap();
        let envelope = state
            .inflight
            .remove(&task_id)
            .ok_or(QueueError::NotFound(task_id))?;
        if requeue {
            state.ready.push(envelope);
        }
        Ok(())
    }

    fn cancel(&self, task_id: TaskId) -> Result<(), QueueError> {
        let mut state = self.state.lock().unwrap();
        let before = state.ready.len();
        let retained: BinaryHeap<TaskEnvelope> = state
            .ready
            .drain()
            .filter(|envelope| envelope.task_id != task_id)
            .collect();
        state.ready = retained;
        if state.ready.len() != before {
            Ok(())
        } else {
            Err(QueueError::NotFound(task_id))
        }
    }

    fn len(&self) -> usize {
        self.state.lock().unwrap().ready.len()
    }
}

/// Holds envelopes that exhausted their retries or were rejected.
#[derive(Default)]
pub struct DeadLetterQueue {
    items: Mutex<Vec<TaskEnvelope>>,
}

impl DeadLetterQueue {
    pub fn new() -> Self {
        DeadLetterQueue::default()
    }

    pub fn push(&self, envelope: TaskEnvelope) {
        self.items.lock().unwrap().push(envelope);
    }

    pub fn drain(&self) -> Vec<TaskEnvelope> {
        std::mem::take(&mut *self.items.lock().unwrap())
    }

    pub fn len(&self) -> usize {
        self.items.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
