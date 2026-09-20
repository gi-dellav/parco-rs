use disco_foundation::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MulInput {
    pub value: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MulOutput(pub i64);

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
#[error("mul error: {0}")]
pub struct MulError(pub String);

pub struct MultiplyTask;

#[disco_foundation::task(name = "multiply", idempotent)]
impl MultiplyTask {
    fn run(input: MulInput, _ctx: &TaskContext) -> Result<MulOutput, MulError> {
        Ok(MulOutput(input.value * 2))
    }
}

disco_foundation::task_registry! {
    pub MyRegistry {
        MultiplyTask,
    }
}

#[derive(disco_foundation::Capability)]
#[capability(name = "gpu", version = 2)]
pub struct GpuCapability;

#[test]
fn task_attribute_and_capability_derive() {
    assert_eq!(<MultiplyTask as Task>::NAME, "multiply");
    const { assert!(<MultiplyTask as Task>::IDEMPOTENT) };
    const { assert!(!<MultiplyTask as Task>::CANCELLABLE) };
    assert_eq!(<GpuCapability as Capability>::NAME, "gpu");
    assert_eq!(<GpuCapability as Capability>::VERSION, 2);
}

#[test]
fn registry_macro_dispatches() {
    let registry = MyRegistry::new();
    assert!(registry.contains("multiply"));
    assert_eq!(registry.names(), vec!["multiply"]);

    let ctx = TaskContext::new(TaskId::new(), TaskAttempt::FIRST);
    let payload = bincode::serialize(&MulInput { value: 21 }).unwrap();
    let out = registry.dispatch("multiply", &payload, &ctx).unwrap();
    assert_eq!(
        bincode::deserialize::<MulOutput>(&out).unwrap(),
        MulOutput(42)
    );
}

#[test]
fn dispatcher_macro_runs_request() {
    let dispatcher = disco_foundation::task_dispatcher![MultiplyTask];
    let payload = bincode::serialize(&MulInput { value: 5 }).unwrap();
    let request = TaskRequest::new(TaskId::new(), "multiply", payload);

    let response = dispatcher.dispatch(&request).unwrap();
    assert_eq!(response.status, TaskStatus::Succeeded);
    assert_eq!(
        bincode::deserialize::<MulOutput>(&response.payload).unwrap(),
        MulOutput(10)
    );
    assert!(matches!(
        TaskDispatch::cancel(&dispatcher, request.task_id),
        Err(DispatchError::CancellationUnsupported)
    ));
}

#[test]
fn capability_set_matching() {
    let mut available = CapabilitySet::new();
    available.insert(GpuCapability);

    let mut required = CapabilitySet::new();
    required.insert_id("gpu");

    assert!(available.satisfies(&required));
    assert!(available.contains::<GpuCapability>());
}

#[test]
fn device_registry_tracks_heartbeats() {
    use std::net::SocketAddr;

    let registry = disco_foundation::device::InMemoryDeviceRegistry::default();
    let id = DeviceId::new();
    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    registry.register(DeviceInfo::new(id, NodeRole::Worker, addr));

    let heartbeat = Heartbeat::new(id, 1, Timestamp::from_millis(100));
    let liveness = registry.heartbeat(id, &heartbeat, Timestamp::from_millis(100));
    assert_eq!(liveness, Liveness::Alive);
    assert_eq!(registry.by_role(NodeRole::Worker).len(), 1);
    assert!(
        registry
            .expired(Timestamp::from_millis(100_000))
            .contains(&id)
    );
}
