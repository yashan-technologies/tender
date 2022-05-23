use crate::core::{ElectionCore, State};
use crate::error::{to_storage_error, Result};
use crate::msg::Message;
use crate::{ElectionType, Event, HardState, InitialMode, Storage};
use crossbeam_channel::RecvTimeoutError;
use fxhash::FxHashSet;

pub struct Startup<'a, T: ElectionType> {
    core: &'a mut ElectionCore<T>,
}

impl<'a, T: ElectionType> Startup<'a, T> {
    #[inline]
    pub fn new(core: &'a mut ElectionCore<T>) -> Self {
        Self { core }
    }

    /// Note: No field will be changed If it returns error.
    #[inline]
    fn init_with_members(
        &mut self,
        members: Vec<T::NodeId>,
        initial_mode: InitialMode,
        set_prev_state: Option<&mut bool>,
    ) -> Result<()> {
        let mut peers = FxHashSet::with_hasher(Default::default());
        for m in members.into_iter().filter(|m| m != self.core.node_id()) {
            peers.try_reserve(1)?;
            peers.insert(m);
        }
        let member_num = peers.len() + 1;

        if initial_mode == InitialMode::AsObserver {
            self.core.set_state(State::Observer, set_prev_state);
            info!(
                "[{}][Term({})] this node is initialized with {} members, and transit to observer",
                self.core.node_id(),
                self.core.current_term(),
                member_num
            );
        } else if initial_mode == InitialMode::AsCandidate {
            self.core.set_state(State::PreCandidate, set_prev_state);
            info!(
                "[{}][Term({})] this node is initialized with {} members, and transit to pre-candidate",
                self.core.node_id(),
                self.core.current_term(),
                member_num
            );
        } else if initial_mode == InitialMode::AsLeader || peers.is_empty() {
            let hard_state = HardState {
                current_term: self.core.current_term() + 1,
                voted_for: Some(self.core.node_id().clone()),
            };
            self.core
                .storage
                .save_hard_state(&hard_state)
                .map_err(to_storage_error::<T>)?;
            self.core.hard_state = hard_state;
            self.core.set_state(State::Leader, set_prev_state);
            if initial_mode == InitialMode::AsLeader {
                info!(
                    "[{}][Term({})] this node is forced to be leader",
                    self.core.node_id(),
                    self.core.current_term()
                );
            } else {
                info!(
                    "[{}][Term({})] this node is initialized without other members, so directly transit to leader",
                    self.core.node_id(),
                    self.core.current_term()
                );
            }
        } else {
            // Do not to change to PreCandidate/Candidate,
            // because we need to ensure that restarted nodes don't disrupt a stable cluster.
            self.core.set_state(State::Follower, set_prev_state);
            info!(
                "[{}][Term({})] this node is initialized with {} members, and transit to follower",
                self.core.node_id(),
                self.core.current_term(),
                member_num
            );
        }

        self.core.members.init_peers(peers);
        // The metrics will be updated in next state.

        Ok(())
    }

    pub fn run(mut self) {
        self.core.increase_state_id();

        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);

        assert!(self.core.is_state(State::Startup));
        let _result = self.core.spawn_event_handling_task(Event::Startup);

        let state = match self.core.storage.load_hard_state() {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "[{}][Term({})] this node is shutting down caused by fatal storage error: {}",
                    self.core.node_id(),
                    self.core.current_term(),
                    e
                );
                self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                return;
            }
        };
        self.core.set_hard_state(state);
        self.core.report_metrics();

        info!(
            "[{}][Term({})] start the startup loop",
            self.core.node_id(),
            self.core.current_term()
        );

        loop {
            if !self.core.is_state(State::Startup) {
                return;
            }

            let election_timeout = self.core.next_election_timeout();

            match self.core.msg_rx.recv_deadline(election_timeout) {
                Ok(msg) => {
                    match msg {
                        Message::Initialize {
                            members,
                            initial_mode,
                            tx,
                        } => {
                            let _ = tx.send(self.init_with_members(members, initial_mode, set_prev_state.as_mut()));
                        }
                        Message::UpdateOptions { options, tx } => {
                            info!(
                                "[{}][Term({})] election update options: {:?}",
                                self.core.node_id(),
                                self.core.current_term(),
                                options
                            );
                            self.core.update_options(options);
                            let _ = tx.send(Ok(()));
                        }
                        Message::Shutdown => {
                            info!(
                                "[{}][Term({})] election received shutdown message",
                                self.core.node_id(),
                                self.core.current_term()
                            );
                            self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                        }
                        Message::EventHandlingResult { event, error, term, .. } => {
                            if let Some(e) = error {
                                error!(
                                    "[{}][Term({})] failed to handle event ({:?}) in term {}: {} ",
                                    self.core.node_id(),
                                    self.core.current_term(),
                                    event,
                                    term,
                                    e
                                );
                            }
                        }
                        Message::MoveLeader { tx, .. } => {
                            self.core.reject_move_leader(tx);
                        }
                        Message::MoveLeaderRequest { tx, .. } => {
                            self.core.reject_move_leader(tx);
                        }
                        Message::StepUpToLeader { tx, .. } => {
                            self.core.reject_step_up_to_leader(tx);
                        }
                        Message::StepDownToFollower { tx } => {
                            self.core.reject_step_down_to_follower(tx);
                        }
                        Message::HeartbeatRequest { .. } => {
                            // ignore heart message in startup state
                            // tx is dropped here, so user will receive a error
                        }
                        Message::HeartbeatResponse(_) => {
                            // ignore heartbeat response in startup state
                        }
                        Message::VoteRequest { .. } => {
                            // ignore vote request message in startup state
                            // tx is dropped here, so user will receive a error
                        }
                        Message::VoteResponse(_) => {
                            // ignore vote response in startup state
                        }
                    }
                }
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        self.core.next_election_timeout = None;
                        continue;
                    }
                    RecvTimeoutError::Disconnected => {
                        info!(
                            "[{}][Term({})] the election message channel is disconnected",
                            self.core.node_id(),
                            self.core.current_term()
                        );
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                },
            }
        }
    }
}
