use crate::ElectionType;
use std::error::Error;

/// Election persistent state.
#[derive(Clone)]
pub struct HardState<T: ElectionType> {
    /// Current term of election.
    pub current_term: u64,
    /// The id of the node voted for in `current_term`.
    pub voted_for: Option<T::NodeId>,
}

impl<T: ElectionType> Default for HardState<T> {
    #[inline]
    fn default() -> Self {
        HardState {
            current_term: 0,
            voted_for: None,
        }
    }
}

/// Storage interfaces used by election.
pub trait Storage<T: ElectionType> {
    type Err: Error;

    fn load_hard_state(&self) -> Result<HardState<T>, Self::Err>;
    fn save_hard_state(&self, hard_state: &HardState<T>) -> Result<(), Self::Err>;
    fn load_vote_factor(&self) -> Result<T::VoteFactor, Self::Err>;
    fn prepare_step_down(&self) {}
}
