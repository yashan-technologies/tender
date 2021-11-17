use crate::error::Result;

/// Spawner for internal raft task.
pub trait TaskSpawner {
    fn spawn<F, T>(&self, name: Option<String>, f: F) -> Result<()>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static;
}
