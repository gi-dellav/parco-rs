use crate::task::Task;

pub struct Queue<T: Task> {
    queue: Vec<T>,
}
