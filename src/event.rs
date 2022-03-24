use crate::error::Result;
use crate::{RaftType, State};

/// The event of the raft node.
#[derive(Clone, Debug)]
pub enum Event<T: RaftType> {
    Startup,
    TransitToLeader { term: u64 },
    TransitToFollower { term: u64, prev_state: State },
    TransitToPreCandidate,
    TransitToCandidate,
    ChangeLeader(T::NodeId),
    Shutdown,
}

/// The event handler of the raft node.
pub trait EventHandler<T: RaftType>: Send + Sync {
    /// Handle the given raft event.
    fn handle_event(&self, event: Event<T>) -> Result<()>;
}
