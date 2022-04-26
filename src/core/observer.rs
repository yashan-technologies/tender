use crate::core::{ElectionCore, State};
use crate::msg::Message;
use crate::{ElectionType, Event};
use crossbeam_channel::RecvTimeoutError;

/// Observer is similar like follower, but can not to be candidate.
pub struct Observer<'a, T: ElectionType> {
    core: &'a mut ElectionCore<T>,
}

impl<'a, T: ElectionType> Observer<'a, T> {
    #[inline]
    pub fn new(core: &'a mut ElectionCore<T>) -> Self {
        Self { core }
    }

    pub fn run(mut self) {
        self.core.increase_state_id();

        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);

        assert!(self.core.is_state(State::Observer));
        self.core.next_election_timeout = None;
        let _result = self.core.spawn_event_handling_task(Event::TransitToObserver {
            term: self.core.hard_state.current_term,
            prev_state: self.core.prev_state(),
        });
        self.core.report_metrics();

        info!(
            "[{}][Term({})] start the observer loop",
            self.core.node_id(),
            self.core.hard_state.current_term
        );

        loop {
            if !self.core.is_state(State::Observer) {
                return;
            }

            let election_timeout = self.core.next_election_timeout();

            match self.core.msg_rx.recv_deadline(election_timeout) {
                Ok(msg) => match msg {
                    Message::HeartbeatRequest { req, tx } => {
                        trace!(
                            "[{}][Term({})] receive heartbeat: {:?}",
                            self.core.node_id(),
                            self.core.hard_state.current_term,
                            req
                        );

                        let result = self.core.handle_heartbeat(req, set_prev_state.as_mut());
                        if let Err(ref e) = result {
                            debug!(
                                "[{}][Term({})] failed to handle heartbeat request: {}",
                                self.core.node_id(),
                                self.core.hard_state.current_term,
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
                                self.core.hard_state.current_term,
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
                            self.core.hard_state.current_term,
                            options
                        );
                        self.core.update_options(options);
                        let _ = tx.send(Ok(()));
                    }
                    Message::Shutdown => {
                        info!(
                            "[{}][Term({})] election received shutdown message",
                            self.core.node_id(),
                            self.core.hard_state.current_term
                        );
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                    Message::EventHandlingResult {
                        event,
                        error,
                        term,
                        state_id: _,
                    } => {
                        if let Some(e) = error {
                            error!(
                                "[{}][Term({})] failed to handle event ({:?}) in term {}: {} ",
                                self.core.node_id(),
                                self.core.hard_state.current_term,
                                event,
                                term,
                                e
                            );
                        } else {
                            debug!(
                                "[{}][Term({})] event ({:?}) in term {} is handled",
                                self.core.node_id(),
                                self.core.hard_state.current_term,
                                event,
                                term,
                            );
                        }
                    }
                    Message::MoveLeader { tx, .. } => {
                        self.core.reject_move_leader(tx);
                    }
                    Message::MoveLeaderRequest { tx, .. } => {
                        self.core.reject_move_leader(tx);
                    }
                },
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        self.core.next_election_timeout = None;
                    }
                    RecvTimeoutError::Disconnected => {
                        info!(
                            "[{}][Term({})] the election message channel is disconnected",
                            self.core.node_id(),
                            self.core.hard_state.current_term
                        );
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                },
            }
        }
    }
}
