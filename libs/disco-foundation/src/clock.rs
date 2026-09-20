pub trait LogicalClock: Send + Sync {
    fn tick(&self) -> Timestamp;
    fn observe(&self, remote: Timestamp) -> Timestamp; // merge on message receipt
}

pub struct Epoch(u64);
