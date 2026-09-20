use serde::{Serialize, de::DeserializeOwned};

pub trait Task: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send;
    type Output: Serialize + DeserializeOwned + Send;
    type Error: std::error::Error + Serialize + DeserializeOwned + Send;

    const NAME: &'static str; // used for dynamic dispatch on the worker side
    const IDEMPOTENT: bool; // scheduler can auto-retry only if true

    fn execute(&self, input: Self::Input) -> Result<Self::Output, Self::Error>;
}

pub trait TaskRegistry: Send + Sync {
    fn dispatch(&self, task_name: &str, payload: &[u8]) -> Result<Vec<u8>, DispatchError>;
}

pub enum DispatchError {
    TaskNotFound,
}
