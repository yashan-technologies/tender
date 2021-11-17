use crate::error::Result;
use crate::RaftType;

/// Heartbeat request.
#[derive(Debug)]
pub struct HeartbeatRequest<T: RaftType> {
    pub group_id: T::GroupId,
    pub target_node_id: T::NodeId,
    pub leader_id: T::NodeId,
    pub term: u64,
}

/// Heartbeat response.
#[derive(Debug)]
pub struct HeartbeatResponse<T: RaftType> {
    pub group_id: T::GroupId,
    pub node_id: T::NodeId,
    pub term: u64,
}

/// Vote request.
#[derive(Debug)]
pub struct VoteRequest<T: RaftType> {
    pub group_id: T::GroupId,
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
    pub group_id: T::GroupId,
    pub node_id: T::NodeId,
    pub candidate_id: T::NodeId,
    pub pre_vote: bool,
    pub vote_id: u64,
    pub term: u64,
    pub vote_granted: bool,
}

/// RPC interfaces used by raft.
pub trait Rpc<T: RaftType> {
    fn heartbeat(&self, msg: HeartbeatRequest<T>) -> Result<HeartbeatResponse<T>>;
    fn vote(&self, msg: VoteRequest<T>) -> Result<VoteResponse<T>>;
}
