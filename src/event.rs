use crate::RaftType;
use std::collections::HashSet;

/// The event of the raft node.
#[derive(Clone, Debug)]
pub enum Event<T: RaftType> {
    Startup,
    TransitToLeader { members: HashSet<T::NodeId>, term: u64 },
    TransitToFollower,
    TransitToPreCandidate,
    TransitToCandidate,
    ChangeLeader(T::NodeId),
    Shutdown,
}

/// The event listener of the raft node.
pub trait EventListener<T: RaftType>: Send + Sync {
    /// Notifies that a raft event is happened.
    fn event_performed(&self, event: Event<T>);
}
