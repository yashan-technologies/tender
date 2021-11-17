//! Raft-based election framework.

#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

mod core;
mod error;
mod event;
mod metrics;
mod msg;
mod options;
mod rpc;
mod storage;
mod task;

pub use crate::core::State;
pub use crate::error::{Error, Result};
pub use crate::event::{Event, EventListener};
pub use crate::metrics::{Metrics, MetricsWatcher};
pub use crate::options::{Options, OptionsBuilder};
pub use crate::rpc::{HeartbeatRequest, HeartbeatResponse, Rpc, VoteRequest, VoteResponse};
pub use crate::storage::{HardState, Storage};
pub use crate::task::TaskSpawner;

use crate::core::RaftCore;
use crate::metrics::metrics_channel;
use crate::msg::Message;
use crossbeam_channel::Sender;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;
use std::thread::JoinHandle;

/// Application specific data involved in voting.
pub trait VoteFactor<T: RaftType> {
    fn vote(&self, other: &Self) -> bool;
}

/// A trait defining application specific data type.
pub trait RaftType: 'static + Sized + Clone + Debug {
    /// Unique id used to identify the raft group.
    type GroupId: Display + Debug + PartialEq + Clone + Send;
    /// Unique id used to identify the raft node.
    type NodeId: Display + Debug + Eq + Hash + Clone + Send;
    /// Application specific data involved in voting.
    type VoteFactor: VoteFactor<Self> + Debug + Clone + Send;
    /// Spawner for internal raft task.
    type TaskSpawner: TaskSpawner + Send + Sync;
    /// Storage interfaces used by raft.
    type Storage: Storage<Self> + Send;
    /// RPC interfaces used by raft.
    type Rpc: Rpc<Self> + Send + Sync;
}

/// The Raft API.
///
/// Applications building on top of Raft will use this to spawn a Raft task and interact with
/// the spawned task.
///
pub struct Raft<T: RaftType> {
    raft_handle: Option<JoinHandle<Result<()>>>,
    msg_tx: Sender<Message<T>>,
    metrics_watcher: MetricsWatcher<T>,
}

impl<T: RaftType> Drop for Raft<T> {
    #[inline]
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl<T: RaftType> Raft<T> {
    /// Starts a new raft thread.
    #[inline]
    pub fn start(
        options: Options,
        group_id: T::GroupId,
        node_id: T::NodeId,
        task_spawner: Arc<T::TaskSpawner>,
        storage: T::Storage,
        rpc: Arc<T::Rpc>,
        event_listeners: Vec<Arc<dyn EventListener<T>>>,
    ) -> Result<Self> {
        let (msg_tx, msg_rx) = crossbeam_channel::bounded(64);
        let (metrics_reporter, metrics_watcher) = metrics_channel();
        let raft_core = RaftCore::new(
            options,
            group_id,
            node_id,
            task_spawner,
            storage,
            rpc,
            msg_tx.clone(),
            msg_rx,
            event_listeners,
            metrics_reporter,
        );
        let raft_handle = raft_core.spawn()?;
        Ok(Raft {
            msg_tx,
            raft_handle: Some(raft_handle),
            metrics_watcher,
        })
    }

    #[inline]
    fn shutdown(&mut self) -> Result<()> {
        // ignore closed channel error
        let _ = self.msg_tx.send(Message::Shutdown);
        if let Some(handle) = self.raft_handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    /// Gets a metrics watcher of this raft node.
    #[inline]
    pub fn metrics_watcher(&self) -> MetricsWatcher<T> {
        self.metrics_watcher.clone()
    }

    /// Initialize this raft node.
    #[inline]
    pub fn initialize(&self, members: HashSet<T::NodeId>) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::Initialize { members, tx })
            .map_err(|e| Error::ChannelError(format!("failed to send initialize to message channel: {}", e)))?;
        rx.recv()
            .map_err(|e| Error::ChannelError(format!("failed to receive initialize result from channel: {}", e)))
            .and_then(|res| res)?;
        Ok(())
    }

    /// Submits a `HeartbeatRequest` RPC to this raft node.
    #[inline]
    pub fn heartbeat(&self, req: HeartbeatRequest<T>) -> Result<HeartbeatResponse<T>> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::Heartbeat { req, tx })
            .map_err(|e| Error::ChannelError(format!("failed to send heartbeat request to message channel: {}", e)))?;
        let resp = rx
            .recv()
            .map_err(|e| Error::ChannelError(format!("failed to receive heartbeat response from channel: {}", e)))
            .and_then(|res| res)?;
        Ok(resp)
    }

    /// Submits a `VoteRequest` RPC to this raft node.
    #[inline]
    pub fn vote(&self, req: VoteRequest<T>) -> Result<VoteResponse<T>> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::VoteRequest { req, tx })
            .map_err(|e| Error::ChannelError(format!("failed to send vote request to message channel: {}", e)))?;
        let resp = rx
            .recv()
            .map_err(|e| Error::ChannelError(format!("failed to receive vote response from channel: {}", e)))
            .and_then(|res| res)?;
        Ok(resp)
    }
}
