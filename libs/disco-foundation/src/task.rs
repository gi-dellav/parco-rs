use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::capability::CapabilitySet;
use crate::clock::{Clock, SystemClock, Timestamp};
use crate::error::{DispatchError, TaskError};

/// Unique identifier for a single task execution (not for a task *type*).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId(pub uuid::Uuid);

impl TaskId {
    pub fn new() -> Self {
        TaskId(uuid::Uuid::now_v7())
    }

    pub fn as_u128(self) -> u128 {
        self.0.as_u128()
    }
}

impl Default for TaskId {
    fn default() -> Self {
        TaskId::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Retry attempt counter, starting at `1` for the first try.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskAttempt(pub u32);

impl TaskAttempt {
    pub const FIRST: TaskAttempt = TaskAttempt(1);

    pub fn next(self) -> Self {
        TaskAttempt(self.0.saturating_add(1))
    }
}

impl fmt::Display for TaskAttempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Lifecycle of a task execution as tracked by the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelling,
    Cancelled,
}

/// Why a task execution was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CancelReason {
    /// A leader or client explicitly asked for it.
    Requested,
    /// The execution exceeded its deadline.
    Timeout,
    /// The leader that owned the task went away.
    LeaderLost,
    /// The whole node is shutting down.
    Shutdown,
    /// A newer attempt superseded this one.
    Superseded,
}

/// Returned by [`TaskContext::check_cancelled`] when cancellation was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("task was cancelled")]
pub struct Cancelled;

/// Cooperative cancellation primitive shared with a running task.
///
/// It is runtime-agnostic: `wait` blocks the current thread, and tasks that need
/// to remain busy should poll [`CancellationToken::is_cancelled`] instead.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<CancellationInner>,
}

struct CancellationInner {
    cancelled: Mutex<bool>,
    waiters: Condvar,
}

impl CancellationToken {
    pub fn new() -> Self {
        CancellationToken {
            inner: Arc::new(CancellationInner {
                cancelled: Mutex::new(false),
                waiters: Condvar::new(),
            }),
        }
    }

    /// A token that is already cancelled.
    pub fn cancelled() -> Self {
        let token = CancellationToken::new();
        token.cancel();
        token
    }

    pub fn cancel(&self) {
        let mut cancelled = self.inner.cancelled.lock().unwrap();
        if !*cancelled {
            *cancelled = true;
            self.inner.waiters.notify_all();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        *self.inner.cancelled.lock().unwrap()
    }

    /// Block until the token is cancelled.
    pub fn wait(&self) {
        let mut cancelled = self.inner.cancelled.lock().unwrap();
        while !*cancelled {
            cancelled = self.inner.waiters.wait(cancelled).unwrap();
        }
    }

    /// Block until cancelled or `timeout` elapses. Returns the cancellation state.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let cancelled = self.inner.cancelled.lock().unwrap();
        if *cancelled {
            return true;
        }
        let (cancelled, _) = self.inner.waiters.wait_timeout(cancelled, timeout).unwrap();
        *cancelled
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        CancellationToken::new()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Execution environment handed to every task.
///
/// Tasks that are not cancellable can simply ignore it; tasks that opt in via
/// [`Task::CANCELLABLE`] should poll [`TaskContext::check_cancelled`] at safe
/// points.
pub struct TaskContext {
    pub task_id: TaskId,
    pub attempt: TaskAttempt,
    pub deadline: Option<Timestamp>,
    pub cancellation: CancellationToken,
    pub clock: Arc<dyn Clock>,
}

impl TaskContext {
    pub fn new(task_id: TaskId, attempt: TaskAttempt) -> Self {
        TaskContext {
            task_id,
            attempt,
            deadline: None,
            cancellation: CancellationToken::new(),
            clock: Arc::new(SystemClock),
        }
    }

    pub fn with_deadline(mut self, deadline: Timestamp) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Cheap guard a cancellable task should call between work units.
    pub fn check_cancelled(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            Err(Cancelled)
        } else {
            Ok(())
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.deadline {
            Some(deadline) => self.clock.now() >= deadline,
            None => false,
        }
    }
}

impl fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContext")
            .field("task_id", &self.task_id)
            .field("attempt", &self.attempt)
            .field("deadline", &self.deadline)
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// The unit of computation dispatched across the network.
///
/// `CANCELLABLE` is deliberately opt-in: a scheduler may only deliver a cancel
/// request to tasks that declare it, and non-cancellable tasks keep the simple
/// "run to completion" contract.
pub trait Task: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send;
    type Output: Serialize + DeserializeOwned + Send;
    type Error: std::error::Error + Serialize + DeserializeOwned + Send;

    /// Stable name used for dynamic dispatch on the worker side.
    const NAME: &'static str;
    /// The scheduler may auto-retry this task without side effects.
    const IDEMPOTENT: bool;
    /// Whether the scheduler may request cancellation of a running attempt.
    const CANCELLABLE: bool = false;

    fn execute(&self, input: Self::Input, ctx: &TaskContext) -> Result<Self::Output, Self::Error>;
}

/// Serializable request for one task execution, carried inside an envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRequest {
    pub task_id: TaskId,
    pub name: String,
    pub payload: Vec<u8>,
    pub attempt: TaskAttempt,
    pub deadline: Option<Timestamp>,
    pub required: CapabilitySet,
}

impl TaskRequest {
    pub fn new(task_id: TaskId, name: impl Into<String>, payload: Vec<u8>) -> Self {
        TaskRequest {
            task_id,
            name: name.into(),
            payload,
            attempt: TaskAttempt::FIRST,
            deadline: None,
            required: CapabilitySet::new(),
        }
    }
}

/// Serializable result of one task execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResponse {
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub payload: Vec<u8>,
    pub error: Option<String>,
}

impl TaskResponse {
    pub fn success(task_id: TaskId, payload: Vec<u8>) -> Self {
        TaskResponse {
            task_id,
            status: TaskStatus::Succeeded,
            payload,
            error: None,
        }
    }

    pub fn failure(task_id: TaskId, error: impl Into<String>) -> Self {
        TaskResponse {
            task_id,
            status: TaskStatus::Failed,
            payload: Vec::new(),
            error: Some(error.into()),
        }
    }

    pub fn cancelled(task_id: TaskId) -> Self {
        TaskResponse {
            task_id,
            status: TaskStatus::Cancelled,
            payload: Vec::new(),
            error: None,
        }
    }
}

/// Type-erased view of a [`Task`], allowing heterogeneous registration.
pub trait ErasedTask: Send + Sync {
    fn name(&self) -> &'static str;
    fn idempotent(&self) -> bool;
    fn cancellable(&self) -> bool;
    fn call(&self, input: &[u8], ctx: &TaskContext) -> Result<Vec<u8>, TaskError>;
}

impl<T: Task> ErasedTask for T {
    fn name(&self) -> &'static str {
        T::NAME
    }

    fn idempotent(&self) -> bool {
        T::IDEMPOTENT
    }

    fn cancellable(&self) -> bool {
        T::CANCELLABLE
    }

    fn call(&self, input: &[u8], ctx: &TaskContext) -> Result<Vec<u8>, TaskError> {
        if T::CANCELLABLE {
            ctx.check_cancelled().map_err(|_| TaskError::Cancelled)?;
        }
        let decoded: T::Input =
            bincode::deserialize(input).map_err(|e| TaskError::Decode(e.to_string()))?;
        let output = self
            .execute(decoded, ctx)
            .map_err(|e| TaskError::Execution(e.to_string()))?;
        bincode::serialize(&output).map_err(|e| TaskError::Encode(e.to_string()))
    }
}

/// A collection of registered tasks keyed by [`Task::NAME`].
pub trait TaskRegistry: Send + Sync {
    fn dispatch(
        &self,
        name: &str,
        payload: &[u8],
        ctx: &TaskContext,
    ) -> Result<Vec<u8>, DispatchError>;
    fn contains(&self, name: &str) -> bool;
    fn names(&self) -> Vec<&'static str>;
    fn cancellable(&self, name: &str) -> bool;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Default in-process registry. This is what the `task_registry!` macro builds.
#[derive(Default)]
pub struct TaskRegistryImpl {
    handlers: HashMap<&'static str, Arc<dyn ErasedTask>>,
}

impl TaskRegistryImpl {
    pub fn new() -> Self {
        TaskRegistryImpl::default()
    }

    pub fn register<T: Task>(&mut self, task: T) {
        self.register_arc(Arc::new(task));
    }

    pub fn register_arc(&mut self, handler: Arc<dyn ErasedTask>) {
        self.handlers.insert(handler.name(), handler);
    }

    pub fn with<T: Task>(mut self, task: T) -> Self {
        self.register(task);
        self
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ErasedTask>> {
        self.handlers.get(name).cloned()
    }
}

impl TaskRegistry for TaskRegistryImpl {
    fn dispatch(
        &self,
        name: &str,
        payload: &[u8],
        ctx: &TaskContext,
    ) -> Result<Vec<u8>, DispatchError> {
        let handler = self
            .handlers
            .get(name)
            .ok_or_else(|| DispatchError::TaskNotFound(name.to_string()))?;
        handler.call(payload, ctx).map_err(|e| match e {
            TaskError::Cancelled => DispatchError::Cancelled,
            TaskError::Decode(m) => DispatchError::Decoding(m),
            TaskError::Encode(m) => DispatchError::Encoding(m),
            TaskError::Execution(m) => DispatchError::Execution(m),
        })
    }

    fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }

    fn names(&self) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = self.handlers.keys().copied().collect();
        names.sort_unstable();
        names
    }

    fn cancellable(&self, name: &str) -> bool {
        self.handlers
            .get(name)
            .map(|handler| handler.cancellable())
            .unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.handlers.len()
    }
}

/// Runtime entry point for executing task requests on a device.
pub trait TaskDispatch: Send + Sync {
    fn dispatch(&self, request: &TaskRequest) -> Result<TaskResponse, DispatchError>;

    /// Request cancellation of a running task.
    ///
    /// The default implementation refuses, which is the correct behaviour for
    /// non-cancellable tasks and for dispatchers without a cancellation registry.
    fn cancel(&self, _task_id: TaskId) -> Result<(), DispatchError> {
        Err(DispatchError::CancellationUnsupported)
    }

    fn supports_cancellation(&self, name: &str) -> bool;

    fn active_tasks(&self) -> Vec<TaskId> {
        Vec::new()
    }
}

/// A [`TaskDispatch`] that executes tasks locally and tracks cancellable attempts.
pub struct LocalDispatcher {
    registry: TaskRegistryImpl,
    active: Arc<Mutex<HashMap<TaskId, CancellationToken>>>,
    clock: Arc<dyn Clock>,
}

impl LocalDispatcher {
    pub fn new(registry: TaskRegistryImpl, clock: Arc<dyn Clock>) -> Self {
        LocalDispatcher {
            registry,
            active: Arc::new(Mutex::new(HashMap::new())),
            clock,
        }
    }

    pub fn with_system_clock(registry: TaskRegistryImpl) -> Self {
        LocalDispatcher::new(registry, Arc::new(SystemClock))
    }

    pub fn registry(&self) -> &TaskRegistryImpl {
        &self.registry
    }
}

impl TaskDispatch for LocalDispatcher {
    fn dispatch(&self, request: &TaskRequest) -> Result<TaskResponse, DispatchError> {
        let cancellable = self.registry.cancellable(&request.name);
        let context = TaskContext {
            task_id: request.task_id,
            attempt: request.attempt,
            deadline: request.deadline,
            cancellation: CancellationToken::new(),
            clock: self.clock.clone(),
        };

        if cancellable {
            self.active
                .lock()
                .unwrap()
                .insert(request.task_id, context.cancellation.clone());
        }

        let result = self
            .registry
            .dispatch(&request.name, &request.payload, &context);

        if cancellable {
            self.active.lock().unwrap().remove(&request.task_id);
        }

        match result {
            Ok(payload) => Ok(TaskResponse::success(request.task_id, payload)),
            Err(DispatchError::Cancelled) => Ok(TaskResponse::cancelled(request.task_id)),
            Err(err) => Ok(TaskResponse::failure(request.task_id, err.to_string())),
        }
    }

    fn cancel(&self, task_id: TaskId) -> Result<(), DispatchError> {
        let active = self.active.lock().unwrap();
        match active.get(&task_id) {
            Some(token) => {
                token.cancel();
                Ok(())
            }
            None => Err(DispatchError::CancellationUnsupported),
        }
    }

    fn supports_cancellation(&self, name: &str) -> bool {
        self.registry.cancellable(name)
    }

    fn active_tasks(&self) -> Vec<TaskId> {
        self.active.lock().unwrap().keys().copied().collect()
    }
}
