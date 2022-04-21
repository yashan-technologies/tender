use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

/// Error related to election.
#[derive(Debug)]
pub enum Error {
    MemAllocError(usize),
    ThreadError(String),
    TaskError(String),
    StorageError(String),
    RpcError(String),
    InvalidTarget(String),
    NotAllowed(String),
    InvalidOptions(String),
    ChannelError(String),
    EventError(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MemAllocError(size) => {
                write!(f, "failed to allocate {} bytes memory", size)
            }
            Error::ThreadError(s) => f.write_str(s),
            Error::TaskError(s) => f.write_str(s),
            Error::StorageError(s) => f.write_str(s),
            Error::RpcError(s) => f.write_str(s),
            Error::InvalidTarget(s) => f.write_str(s),
            Error::NotAllowed(s) => f.write_str(s),
            Error::InvalidOptions(s) => f.write_str(s),
            Error::ChannelError(s) => f.write_str(s),
            Error::EventError(s) => f.write_str(s),
        }
    }
}
