//! Raft-based election framework.

#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

#[macro_use]
mod util;

mod core;
mod error;
mod event;
mod metrics;
mod msg;
mod options;
mod rpc;
mod storage;
mod task;
mod wait_group;

pub use crate::core::State;
pub use crate::error::{Error, Result};
pub use crate::event::{Event, EventHandler};
pub use crate::metrics::{Metrics, MetricsWatcher};
pub use crate::options::{Options, OptionsBuilder, Quorum};
pub use crate::rpc::{HeartbeatRequest, HeartbeatResponse, MoveLeaderRequest, Rpc, VoteRequest, VoteResponse};
pub use crate::storage::{HardState, Storage};
pub use crate::task::{TaskSpawner, Thread};

use crate::core::ElectionCore;
use crate::metrics::metrics_channel;
use crate::msg::Message;
use crossbeam_channel::Sender;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;

/// Unique id used to identify the node.
pub trait NodeId {
    /// Unique id used to identify the group.
    type GroupId: Display + PartialEq;

    /// Get group id of this node.
    fn group_id(&self) -> Self::GroupId;
}

/// Vote result.
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum VoteResult {
    /// Dissenting vote.
    NotGranted = 0,
    /// Affirmative vote.
    Granted = 1,
}

impl VoteResult {
    /// Indicates whether the vote has been granted.
    #[inline]
    pub fn is_granted(self) -> bool {
        self == VoteResult::Granted
    }
}

/// Application specific data involved in voting.
pub trait VoteFactor<T: ElectionType> {
    /// Votes according to the voting factor.
    fn vote(&self, other: &Self) -> VoteResult;
}

/// A trait defining application specific data type for election.
pub trait ElectionType: 'static + Sized + Clone + Debug {
    /// Unique id used to identify the node.
    type NodeId: NodeId + Display + Debug + Eq + Hash + Clone + Send;
    /// Application specific data involved in voting.
    type VoteFactor: VoteFactor<Self> + Debug + Clone + Send;
    /// Thread interfaces used by election.
    type Thread: Thread;
    /// Spawner for internal short-time election task.
    type TaskSpawner: TaskSpawner + Send + Sync;
    /// Storage interfaces used by election.
    type Storage: Storage<Self> + Send;
    /// RPC interfaces used by election.
    type Rpc: Rpc<Self> + Send + Sync;
}

/// Initial mode when initialize election.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum InitialMode {
    /// Normal election process.
    Normal,
    /// Force the node to be leader.
    AsLeader,
    /// Force the node to be observer.
    AsObserver,
    /// Force the node to be candidate(pre-candidate at first).
    AsCandidate,
}

/// The election API.
///
/// Applications building on top of `Tender` will use this to spawn a election main thread and interact with
/// the spawned thread.
///
pub struct Election<T: ElectionType> {
    main_thread: Option<T::Thread>,
    msg_tx: Sender<Message<T>>,
    metrics_watcher: MetricsWatcher<T>,
}

impl<T: ElectionType> Drop for Election<T> {
    #[inline]
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl<T: ElectionType> Election<T> {
    /// Starts a new election thread.
    #[inline]
    pub fn start(
        options: Options,
        node_id: T::NodeId,
        task_spawner: Arc<T::TaskSpawner>,
        storage: T::Storage,
        rpc: Arc<T::Rpc>,
        event_handler: Arc<dyn EventHandler<T>>,
    ) -> Result<Self> {
        let (msg_tx, msg_rx) = crossbeam_channel::bounded(64);
        let (metrics_reporter, metrics_watcher) = metrics_channel();
        let election_core = ElectionCore::new(
            options,
            node_id,
            task_spawner,
            storage,
            rpc,
            msg_tx.clone(),
            msg_rx,
            event_handler,
            metrics_reporter,
        );
        let election_thread = election_core.spawn()?;
        Ok(Election {
            msg_tx,
            main_thread: Some(election_thread),
            metrics_watcher,
        })
    }

    #[inline]
    fn shutdown(&mut self) -> Result<()> {
        // ignore closed channel error
        let _ = self.msg_tx.send(Message::Shutdown);
        if let Some(thread) = self.main_thread.take() {
            thread.join();
        }
        Ok(())
    }

    /// Gets a metrics watcher of this node.
    #[inline]
    pub fn metrics_watcher(&self) -> MetricsWatcher<T> {
        self.metrics_watcher.clone()
    }

    /// Initialize this node.
    #[inline]
    pub fn initialize(&self, members: HashSet<T::NodeId>, initial_mode: InitialMode) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::Initialize {
                members,
                initial_mode,
                tx,
            })
            .map_err(|e| try_format_error!(ChannelError, "failed to send initialize to message channel: {}", e))?;
        rx.recv()
            .map_err(|e| try_format_error!(ChannelError, "failed to receive initialize result from channel: {}", e))
            .and_then(|res| res)?;
        Ok(())
    }

    /// Submits a `HeartbeatRequest` RPC to this node.
    #[inline]
    pub fn submit_heartbeat(&self, req: HeartbeatRequest<T>) -> Result<HeartbeatResponse<T>> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx.send(Message::HeartbeatRequest { req, tx }).map_err(|e| {
            try_format_error!(
                ChannelError,
                "failed to send heartbeat request to message channel: {}",
                e
            )
        })?;
        let resp = rx
            .recv()
            .map_err(|e| try_format_error!(ChannelError, "failed to receive heartbeat response from channel: {}", e))
            .and_then(|res| res)?;
        Ok(resp)
    }

    /// Submits a `VoteRequest` RPC to this node.
    #[inline]
    pub fn submit_vote(&self, req: VoteRequest<T>) -> Result<VoteResponse<T>> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::VoteRequest { req, tx })
            .map_err(|e| try_format_error!(ChannelError, "failed to send vote request to message channel: {}", e))?;
        let resp = rx
            .recv()
            .map_err(|e| try_format_error!(ChannelError, "failed to receive vote response from channel: {}", e))
            .and_then(|res| res)?;
        Ok(resp)
    }

    /// Updates `Options` of this node.
    #[inline]
    pub fn update_options(&self, options: Options) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::UpdateOptions { options, tx })
            .map_err(|e| try_format_error!(ChannelError, "failed to send update options to message channel: {}", e))?;
        rx.recv()
            .map_err(|e| {
                try_format_error!(
                    ChannelError,
                    "failed to receive update options result from channel: {}",
                    e
                )
            })
            .and_then(|res| res)?;
        Ok(())
    }

    /// Moves leader to `target_node`.
    #[inline]
    pub fn move_leader(&self, target_node: T::NodeId) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx
            .send(Message::MoveLeader { target_node, tx })
            .map_err(|e| try_format_error!(ChannelError, "failed to send move_leader to message channel: {}", e))?;
        rx.recv()
            .map_err(|e| try_format_error!(ChannelError, "failed to receive move_leader result from channel: {}", e))
            .and_then(|res| res)?;
        Ok(())
    }

    /// Submits a `MoveLeaderRequest` RPC to this node.
    #[inline]
    pub fn submit_move_leader_request(&self, req: MoveLeaderRequest<T>) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.msg_tx.send(Message::MoveLeaderRequest { req, tx }).map_err(|e| {
            try_format_error!(
                ChannelError,
                "failed to send move_leader request to message channel: {}",
                e
            )
        })?;
        rx.recv()
            .map_err(|e| try_format_error!(ChannelError, "failed to receive move_leader result from channel: {}", e))
            .and_then(|res| res)?;
        Ok(())
    }
}
