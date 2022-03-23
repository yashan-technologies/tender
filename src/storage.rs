use crate::RaftType;
use std::error::Error;

/// Raft persistent state.
#[derive(Clone)]
pub struct HardState<T: RaftType> {
    /// Current term of raft.
    pub current_term: u64,
    /// The id of the node voted for in `current_term`.
    pub voted_for: Option<T::NodeId>,
}

impl<T: RaftType> Default for HardState<T> {
    #[inline]
    fn default() -> Self {
        HardState {
            current_term: 0,
            voted_for: None,
        }
    }
}

/// Storage interfaces used by raft.
pub trait Storage<T: RaftType> {
    type Err: Error;

    fn load_hard_state(&self) -> Result<HardState<T>, Self::Err>;
    fn save_hard_state(&self, hard_state: &HardState<T>) -> Result<(), Self::Err>;
    fn load_vote_factor(&self) -> Result<T::VoteFactor, Self::Err>;
}
