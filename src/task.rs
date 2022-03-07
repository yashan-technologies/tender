use crate::error::Result;

/// Thread used for raft main task.
pub trait Thread: Sized {
    fn spawn<F>(name: String, f: F) -> Result<Self>
    where
        F: FnOnce(),
        F: Send + 'static;
    fn join(self);
}

/// Spawner for internal short-time task.
pub trait TaskSpawner {
    fn spawn<F>(&self, name: String, f: F) -> Result<()>
    where
        F: FnOnce(),
        F: Send + 'static;
}
