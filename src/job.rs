use tsuki_scheduler::runtime::Runtime;
use tsuki_scheduler::Task;

#[derive(Debug)]
pub struct Job<R: Runtime> {
    pub id: u128,
    pub name: String,
    pub task: Task<R>,
}

impl<R> Job<R> where R: Runtime {
    pub fn new(id: u128, name: String, task: Task<R>) -> Self {
        Self {
            id,
            name,
            task,
        }
    }
}
