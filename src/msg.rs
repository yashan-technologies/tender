use crate::error::{Error, Result};
use crate::rpc::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse};
use crate::{Event, Options, RaftType};
use crossbeam_channel::Sender;
use std::collections::HashSet;

/// Message processed by raft main thread.
pub enum Message<T: RaftType> {
    HeartbeatRequest {
        req: HeartbeatRequest<T>,
        tx: Sender<Result<HeartbeatResponse<T>>>,
    },
    HeartbeatResponse(HeartbeatResponse<T>),
    VoteRequest {
        req: VoteRequest<T>,
        tx: Sender<Result<VoteResponse<T>>>,
    },
    VoteResponse(VoteResponse<T>),
    Initialize {
        members: HashSet<T::NodeId>,
        force_leader: bool,
        tx: Sender<Result<()>>,
    },
    UpdateOptions {
        options: Options,
        tx: Sender<Result<()>>,
    },
    Shutdown,
    EventHandlingResult {
        event: Event<T>,
        error: Option<Error>,
        term: u64,
        state_id: u64,
    },
}
