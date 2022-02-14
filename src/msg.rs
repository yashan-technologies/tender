use crate::error::Result;
use crate::rpc::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse};
use crate::{Options, RaftType};
use crossbeam_channel::Sender;
use std::collections::HashSet;

/// Message processed by raft main thread.
pub enum Message<T: RaftType> {
    Heartbeat {
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
        tx: Sender<Result<()>>,
    },
    UpdateOptions {
        options: Options,
        tx: Sender<Result<()>>,
    },
    Shutdown,
}
