use crate::core::{MemberConfig, RaftCore, State};
use crate::error::Result;
use crate::msg::Message;
use crate::{Error, Event, HardState, RaftType, Storage};
use crossbeam_channel::RecvTimeoutError;
use std::collections::HashSet;

pub struct Startup<'a, T: RaftType> {
    core: &'a mut RaftCore<T>,
}

impl<'a, T: RaftType> Startup<'a, T> {
    #[inline]
    pub fn new(core: &'a mut RaftCore<T>) -> Self {
        Self { core }
    }

    /// Note: No field will be changed If it returns error.
    #[inline]
    fn init_with_members(
        &mut self,
        mut members: HashSet<T::NodeId>,
        force_leader: bool,
        set_prev_state: Option<&mut bool>,
    ) -> Result<()> {
        if !members.contains(&self.core.node_id) {
            members.insert(self.core.node_id.clone());
        }

        if force_leader || members.len() == 1 {
            let hard_state = HardState {
                current_term: self.core.hard_state.current_term + 1,
                voted_for: Some(self.core.node_id.clone()),
            };
            self.core
                .storage
                .save_hard_state(&hard_state)
                .map_err(|e| Error::StorageError(e.to_string()))?;
            self.core.hard_state = hard_state;
            self.core.set_state(State::Leader, set_prev_state);
            info!(
                "[Node({})] raft is initialized without other members, so directly transit to leader",
                self.core.node_id
            );
        } else {
            // Do not to change to PreCandidate/Candidate,
            // because we need to ensure that restarted nodes don't disrupt a stable cluster.
            self.core.set_state(State::Follower, set_prev_state);
            info!(
                "[Node({})] raft is initialized with {} members, so transit to follower",
                self.core.node_id,
                members.len()
            );
        }

        self.core.members = MemberConfig {
            members,
            members_after_consensus: None,
        };

        self.core.report_metrics();

        Ok(())
    }

    pub fn run(mut self) {
        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);

        assert!(self.core.is_state(State::Startup));
        let _result = self.core.spawn_event_handling_task(Event::Startup);

        let state = match self.core.storage.load_hard_state() {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "[Node({})] raft is shutting down caused by fatal storage error: {}",
                    self.core.node_id, e
                );
                self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                return;
            }
        };
        self.core.set_hard_state(state);
        self.core.report_metrics();

        info!("[Node({})] start the startup loop", self.core.node_id);

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
                            force_leader,
                            tx,
                        } => {
                            let _ = tx.send(self.init_with_members(members, force_leader, set_prev_state.as_mut()));
                        }
                        Message::UpdateOptions { options, tx } => {
                            info!("[Node({})] raft update options: {:?}", self.core.node_id, options);
                            self.core.update_options(options);
                            let _ = tx.send(Ok(()));
                        }
                        Message::Shutdown => {
                            info!("[Node({})] raft received shutdown message", self.core.node_id);
                            self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                        }
                        Message::EventHandlingResult { event, error, term } => {
                            if let Some(e) = error {
                                error!(
                                    "[Node({})] raft failed to handle event ({:?}) in term {}: {} ",
                                    self.core.node_id, event, term, e
                                );
                            }
                        }
                        Message::Heartbeat { .. } => {
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
                        info!("[Node({})] the raft message channel is disconnected", self.core.node_id);
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                },
            }
        }
    }
}
