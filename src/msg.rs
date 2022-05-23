use crate::error::{Error, Result};
use crate::rpc::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse};
use crate::{ElectionType, Event, InitialMode, MoveLeaderRequest, Options};
use crossbeam_channel::Sender;

/// Message processed by election main thread.
pub enum Message<T: ElectionType> {
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
        members: Vec<T::NodeId>,
        initial_mode: InitialMode,
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
    MoveLeader {
        target_node: T::NodeId,
        tx: Sender<Result<()>>,
    },
    MoveLeaderRequest {
        req: MoveLeaderRequest<T>,
        tx: Sender<Result<()>>,
    },
    StepUpToLeader {
        increase_term: bool,
        tx: Sender<Result<()>>,
    },
    StepDownToFollower {
        tx: Sender<Result<()>>,
    },
}
