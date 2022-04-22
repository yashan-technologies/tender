use crate::{ElectionType, VoteResult};
use std::error::Error;

/// Heartbeat request.
#[derive(Debug)]
pub struct HeartbeatRequest<T: ElectionType> {
    pub target_node_id: T::NodeId,
    pub leader_id: T::NodeId,
    pub term: u64,
}

/// Heartbeat response.
#[derive(Debug)]
pub struct HeartbeatResponse<T: ElectionType> {
    pub node_id: T::NodeId,
    pub term: u64,
}

/// Vote request.
#[derive(Debug)]
pub struct VoteRequest<T: ElectionType> {
    pub target_node_id: T::NodeId,
    pub candidate_id: T::NodeId,
    pub vote_id: u64,
    pub term: u64,
    pub factor: T::VoteFactor,
    pub pre_vote: bool,
    pub move_leader: bool,
}

/// Vote response.
#[derive(Debug)]
pub struct VoteResponse<T: ElectionType> {
    pub node_id: T::NodeId,
    pub candidate_id: T::NodeId,
    pub vote_id: u64,
    pub term: u64,
    pub pre_vote: bool,
    pub vote_result: VoteResult,
}

/// MoveLeader request.
#[derive(Debug)]
pub struct MoveLeaderRequest<T: ElectionType> {
    pub target_node_id: T::NodeId,
    pub term: u64,
}

/// RPC interfaces used by election.
pub trait Rpc<T: ElectionType> {
    type Err: Error;

    fn heartbeat(&self, msg: HeartbeatRequest<T>) -> Result<HeartbeatResponse<T>, Self::Err>;
    fn vote(&self, msg: VoteRequest<T>) -> Result<VoteResponse<T>, Self::Err>;
    fn move_leader(&self, msg: MoveLeaderRequest<T>) -> Result<(), Self::Err>;
}
