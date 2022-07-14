use crate::core::{ElectionCore, State};
use crate::error::{Error, Result};
use crate::msg::Message;
use crate::util::TryToString;
use crate::{ElectionType, Event, MoveLeaderRequest};
use crossbeam_channel::RecvTimeoutError;

pub struct Follower<'a, T: ElectionType> {
    core: &'a mut ElectionCore<T>,
    transit_event_finished: bool,
}

impl<'a, T: ElectionType> Follower<'a, T> {
    #[inline]
    pub fn new(core: &'a mut ElectionCore<T>) -> Self {
        Self {
            core,
            transit_event_finished: false,
        }
    }

    fn handle_move_leader_request(
        &mut self,
        msg: MoveLeaderRequest<T>,
        set_prev_state: Option<&mut bool>,
    ) -> Result<()> {
        self.core.check_node(&msg.target_node_id)?;

        if msg.term > self.core.current_term() {
            self.core.update_current_term(msg.term, None)?;
            self.core.update_metrics(|metrics| {
                metrics.current_term = msg.term;
            });
        }

        if self.transit_event_finished {
            self.core.set_state(State::Candidate, set_prev_state);
            self.core.in_moving_leader = true;
            self.core.current_leader = None;
            info!(
                "[{}][Term({})] receive move leader request, need to transit to candidate",
                self.core.node_id(),
                self.core.current_term()
            );
            Ok(())
        } else {
            Err(Error::NotAllowed(
                "transit to follower is not finished".try_to_string()?,
            ))
        }
    }

    fn step_up_to_leader(&mut self, increase_term: bool, set_prev_state: Option<&mut bool>) -> Result<()> {
        if self.transit_event_finished {
            if increase_term {
                let term = self.core.current_term();
                self.core.update_current_term(term + 1, None)?;
            }

            self.core.set_state(State::Leader, set_prev_state);
            self.core.step_up_or_down = true;
            // The metrics will be updated in Leader state.
            Ok(())
        } else {
            Err(Error::NotAllowed(
                "can't step up to leader because TransitToFollower is not finished".try_to_string()?,
            ))
        }
    }

    pub fn run(mut self) {
        self.core.increase_state_id();

        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);
        self.core.in_moving_leader = false;

        assert!(self.core.is_state(State::Follower));
        self.core.next_election_timeout = None;
        let _result = self.core.spawn_event_handling_task(Event::TransitToFollower {
            term: self.core.current_term(),
            prev_state: self.core.prev_state(),
            caused_by_step_down: self.core.step_up_or_down,
        });
        self.core.step_up_or_down = false;
        self.core.report_metrics();

        info!(
            "[{}][Term({})] start the follower loop",
            self.core.node_id(),
            self.core.current_term()
        );

        loop {
            if !self.core.is_state(State::Follower) {
                return;
            }

            let election_timeout = self.core.next_election_timeout();

            match self.core.msg_rx.recv_deadline(election_timeout) {
                Ok(msg) => match msg {
                    Message::HeartbeatRequest { req, tx } => {
                        trace!(
                            "[{}][Term({})] received heartbeat: {:?}",
                            self.core.node_id(),
                            self.core.current_term(),
                            req
                        );

                        let result = self.core.handle_heartbeat(req, set_prev_state.as_mut());
                        if let Err(ref e) = result {
                            debug!(
                                "[{}][Term({})] failed to handle heartbeat request: {}",
                                self.core.node_id(),
                                self.core.current_term(),
                                e
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
                                "[{}][Term({})] failed to handle vote request: {}",
                                self.core.node_id(),
                                self.core.current_term(),
                                e
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
                    Message::EventHandlingResult {
                        event,
                        error,
                        term,
                        state_id,
                    } => {
                        if let Some(e) = error {
                            error!(
                                "[{}][Term({})] failed to handle event ({:?}) in term {}: {} ",
                                self.core.node_id(),
                                self.core.current_term(),
                                event,
                                term,
                                e
                            );
                        } else if matches!(event, Event::TransitToFollower { .. }) && state_id == self.core.state_id() {
                            self.transit_event_finished = true;
                        } else {
                            debug!(
                                "[{}][Term({})] event ({:?}) in term {} is handled",
                                self.core.node_id(),
                                self.core.current_term(),
                                event,
                                term,
                            );
                        }
                    }
                    Message::MoveLeader { tx, .. } => {
                        self.core.reject_move_leader(tx);
                    }
                    Message::MoveLeaderRequest { req, tx } => {
                        let result = self.handle_move_leader_request(req, set_prev_state.as_mut());
                        let _ = tx.send(result);
                    }
                    Message::StepUpToLeader { increase_term, tx } => {
                        debug!(
                            "[{}][Term({})] step up to leader",
                            self.core.node_id(),
                            self.core.current_term(),
                        );
                        let _ = tx.send(self.step_up_to_leader(increase_term, set_prev_state.as_mut()));
                    }
                    Message::StepDownToFollower { tx } => {
                        // Current state is follower, nothing to do.
                        let _ = tx.send(Ok(()));
                    }
                },
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        if self.transit_event_finished {
                            self.core.set_state(State::PreCandidate, set_prev_state.as_mut());
                            self.core.current_leader = None;
                            info!(
                                "[{}][Term({})] an election timeout is hit, need to transit to pre-candidate",
                                self.core.node_id(),
                                self.core.current_term()
                            );
                        } else {
                            self.core.next_election_timeout = None;
                            info!(
                                "[{}][Term({})] an election timeout is hit, but TransitToFollower is not finished",
                                self.core.node_id(),
                                self.core.current_term()
                            );
                        }
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
