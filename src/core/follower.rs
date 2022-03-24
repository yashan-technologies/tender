use crate::core::{RaftCore, State};
use crate::msg::Message;
use crate::{Event, RaftType};
use crossbeam_channel::RecvTimeoutError;

pub struct Follower<'a, T: RaftType> {
    core: &'a mut RaftCore<T>,
    transit_event_finished: bool,
}

impl<'a, T: RaftType> Follower<'a, T> {
    #[inline]
    pub fn new(core: &'a mut RaftCore<T>) -> Self {
        Self {
            core,
            transit_event_finished: false,
        }
    }

    pub fn run(mut self) {
        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);

        assert!(self.core.is_state(State::Follower));
        self.core.next_election_timeout = None;
        let _result = self.core.spawn_event_handling_task(Event::TransitToFollower {
            term: self.core.hard_state.current_term,
            prev_state: self.core.prev_state(),
        });
        self.core.report_metrics();

        info!(
            "[Node({})][Term({})] start the follower loop",
            self.core.node_id, self.core.hard_state.current_term
        );

        loop {
            if !self.core.is_state(State::Follower) {
                return;
            }

            let election_timeout = self.core.next_election_timeout();

            match self.core.msg_rx.recv_deadline(election_timeout) {
                Ok(msg) => match msg {
                    Message::Heartbeat { req, tx } => {
                        trace!(
                            "[Node({})][Term({})] received heartbeat: {:?}",
                            self.core.node_id,
                            self.core.hard_state.current_term,
                            req
                        );

                        let result = self.core.handle_heartbeat(req, set_prev_state.as_mut());
                        if let Err(ref e) = result {
                            debug!(
                                "[Node({})][Term({})] failed to handle heartbeat request: {}",
                                self.core.node_id, self.core.hard_state.current_term, e
                            );
                        }
                        let _ = tx.send(result);
                    }
                    Message::HeartbeatResponse(_) => {
                        // ignore heartbeat response
                    }
                    Message::VoteRequest { req, tx } => {
                        let result = self.core.handle_vote_request(req, set_prev_state.as_mut());
                        if let Err(ref e) = result {
                            debug!(
                                "[Node({})][Term({})] failed to handle vote request: {}",
                                self.core.node_id, self.core.hard_state.current_term, e
                            );
                        }
                        let _ = tx.send(result);
                    }
                    Message::VoteResponse { .. } => {
                        // ignore vote response
                    }
                    Message::Initialize { tx, .. } => {
                        self.core.reject_init_with_members(tx);
                    }
                    Message::UpdateOptions { options, tx } => {
                        info!(
                            "[Node({})][Term({})] raft update options: {:?}",
                            self.core.node_id, self.core.hard_state.current_term, options
                        );
                        self.core.update_options(options);
                        let _ = tx.send(Ok(()));
                    }
                    Message::Shutdown => {
                        info!(
                            "[Node({})][Term({})] raft received shutdown message",
                            self.core.node_id, self.core.hard_state.current_term
                        );
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                    Message::EventHandlingResult { event, error, term } => {
                        if let Some(e) = error {
                            error!(
                                "[Node({})][Term({})] raft failed to handle event ({:?}) in term {}: {} ",
                                self.core.node_id, self.core.hard_state.current_term, event, term, e
                            );
                        } else if matches!(event, Event::TransitToFollower { .. })
                            && term == self.core.hard_state.current_term
                        {
                            self.transit_event_finished = true;
                        }
                    }
                },
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        if self.transit_event_finished && !self.core.options.disable_candidate() {
                            self.core.set_state(State::PreCandidate, set_prev_state.as_mut());
                            self.core.current_leader = None;
                            info!(
                                "[Node({})][Term({})] an election timeout is hit, need to transit to pre-candidate",
                                self.core.node_id, self.core.hard_state.current_term
                            );
                        } else {
                            self.core.next_election_timeout = None;

                            if !self.transit_event_finished {
                                debug!(
                                    "[Node({})][Term({})] an election timeout is hit, but TransitToFollower is not finished",
                                    self.core.node_id,self.core.hard_state.current_term
                                );
                            }

                            if self.core.options.disable_candidate() {
                                debug!(
                                    "[Node({})][Term({})] an election timeout is hit, but raft can not be candidate",
                                    self.core.node_id, self.core.hard_state.current_term
                                );
                            }
                        }
                    }
                    RecvTimeoutError::Disconnected => {
                        info!(
                            "[Node({})][Term({})] the raft message channel is disconnected",
                            self.core.node_id, self.core.hard_state.current_term
                        );
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                },
            }
        }
    }
}
