use crate::{RaftType, VoteResult};
use std::error::Error;

/// Heartbeat request.
#[derive(Debug)]
pub struct HeartbeatRequest<T: RaftType> {
    pub target_node_id: T::NodeId,
    pub leader_id: T::NodeId,
    pub term: u64,
}

/// Heartbeat response.
#[derive(Debug)]
pub struct HeartbeatResponse<T: RaftType> {
    pub node_id: T::NodeId,
    pub term: u64,
}

/// Vote request.
#[derive(Debug)]
pub struct VoteRequest<T: RaftType> {
    pub target_node_id: T::NodeId,
    pub candidate_id: T::NodeId,
    pub pre_vote: bool,
    pub vote_id: u64,
    pub term: u64,
    pub factor: T::VoteFactor,
}

/// Vote response.
#[derive(Debug)]
pub struct VoteResponse<T: RaftType> {
    pub node_id: T::NodeId,
    pub candidate_id: T::NodeId,
    pub pre_vote: bool,
    pub vote_id: u64,
    pub term: u64,
    pub vote_result: VoteResult,
}

/// RPC interfaces used by raft.
pub trait Rpc<T: RaftType> {
    type Err: Error;

    fn heartbeat(&self, msg: HeartbeatRequest<T>) -> Result<HeartbeatResponse<T>, Self::Err>;
    fn vote(&self, msg: VoteRequest<T>) -> Result<VoteResponse<T>, Self::Err>;
}
