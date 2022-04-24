use crate::{ElectionType, Rpc, Storage};
use std::collections::TryReserveError;
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
    FormatError(fmt::Error),
    TryReserveError(TryReserveError),
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
            Error::FormatError(e) => write!(f, "{}", e),
            Error::TryReserveError(e) => write!(f, "{}", e),
        }
    }
}

impl From<fmt::Error> for Error {
    #[inline]
    fn from(e: fmt::Error) -> Self {
        Error::FormatError(e)
    }
}

impl From<TryReserveError> for Error {
    #[inline]
    fn from(e: TryReserveError) -> Self {
        Error::TryReserveError(e)
    }
}

#[inline]
pub fn to_storage_error<T: ElectionType>(e: <T::Storage as Storage<T>>::Err) -> Error {
    match try_format!("{}", e) {
        Ok(s) => Error::StorageError(s),
        Err(e) => e,
    }
}

#[inline]
pub fn to_rpc_error<T: ElectionType>(e: <T::Rpc as Rpc<T>>::Err) -> Error {
    match try_format!("{}", e) {
        Ok(s) => Error::RpcError(s),
        Err(e) => e,
    }
}
