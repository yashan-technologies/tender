use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

/// Error related to election.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to allocate {0} bytes memory")]
    MemAllocError(usize),
    #[error("{0}")]
    ThreadError(String),
    #[error("{0}")]
    TaskError(String),
    #[error("{0}")]
    StorageError(String),
    #[error("{0}")]
    RpcError(String),
    #[error("{0}")]
    InvalidTarget(String),
    #[error("the requested action is not allowed due to the node's current state: {0}")]
    NotAllowed(String),
    #[error("{0}")]
    InvalidOptions(String),
    #[error("{0}")]
    ChannelError(String),
    #[error("{0}")]
    EventError(String),
}
