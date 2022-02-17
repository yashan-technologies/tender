use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

/// Error related to the raft.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to allocate {0} bytes memory")]
    MemAllocError(usize),
    #[error("{0}")]
    TaskError(String),
    #[error("{0}")]
    StorageError(String),
    #[error("{0}")]
    RpcError(String),
    #[error("{0}")]
    InvalidTarget(String),
    #[error("the requested action is not allowed due to the raft node's current state: {0}")]
    NotAllowed(String),
    #[error("election timeout min({min}) & max({max}) are invalid: max must be greater than min")]
    InvalidElectionTimeout { min: u64, max: u64 },
    #[error("{0}")]
    ChannelError(String),
}
