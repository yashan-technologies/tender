use crate::error::Result;
use crate::{ElectionType, State};

/// The event of election.
#[derive(Clone, Debug)]
pub enum Event<T: ElectionType> {
    Startup,
    TransitToLeader {
        term: u64,
        caused_by_step_up: bool,
    },
    TransitToFollower {
        term: u64,
        prev_state: State,
        caused_by_step_down: bool,
    },
    TransitToObserver {
        term: u64,
        prev_state: State,
    },
    TransitToPreCandidate,
    TransitToCandidate,
    ChangeLeader(T::NodeId),
    Shutdown,
}

/// The event handler of election.
pub trait EventHandler<T: ElectionType>: Send + Sync {
    /// Handle the given election event.
    fn handle_event(&self, event: Event<T>) -> Result<()>;
}
