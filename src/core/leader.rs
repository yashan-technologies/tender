use crate::core::{ElectionCore, State};
use crate::error::{to_rpc_error, Result};
use crate::msg::Message;
use crate::rpc::HeartbeatRequest;
use crate::{ElectionType, Event, HeartbeatResponse, MoveLeaderRequest, Rpc};
use crossbeam_channel::{RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

pub struct Leader<'a, T: ElectionType> {
    core: &'a mut ElectionCore<T>,
    next_heartbeat_timeout: Option<Instant>,
}

impl<'a, T: ElectionType> Leader<'a, T> {
    #[inline]
    pub fn new(core: &'a mut ElectionCore<T>) -> Self {
        Self {
            core,
            next_heartbeat_timeout: None,
        }
    }

    #[inline]
    fn update_next_heartbeat_timeout(&mut self) {
        let now = Instant::now();
        self.next_heartbeat_timeout = Some(now + Duration::from_millis(self.core.options.heartbeat_interval()));
    }

    #[inline]
    fn next_heartbeat_timeout(&mut self) -> Instant {
        match self.next_heartbeat_timeout {
            Some(instant) => instant,
            None => {
                self.update_next_heartbeat_timeout();
                self.next_heartbeat_timeout.unwrap()
            }
        }
    }

    fn spawn_parallel_heartbeat(&self) {
        let current_term = self.core.current_term();

        for member in self.core.members.peers() {
            let req = HeartbeatRequest {
                target_node_id: member.clone(),
                leader_id: self.core.node_id().clone(),
                term: current_term,
            };

            let rpc = self.core.rpc.clone();
            let tx = self.core.msg_tx.clone();
            let node_id = self.core.node_id().clone();
            let target_node_id = member.clone();

            trace!(
                "[{}][Term({})] send heartbeat to node({})",
                node_id,
                current_term,
                member
            );

            let _ = self.core.spawn_task("election-heartbeat", move || {
                match rpc.heartbeat(req) {
                    Ok(resp) => {
                        // ignore send error
                        let _ = tx.send(Message::HeartbeatResponse(resp));
                    }
                    Err(e) => {
                        warn!(
                            "[{}][Term({})] failed to send heartbeat request to {}: {}",
                            node_id, current_term, target_node_id, e
                        );
                    }
                }
            });
        }
    }

    #[inline]
    fn handle_heartbeat_response(&mut self, resp: HeartbeatResponse<T>, set_prev_state: Option<&mut bool>) {
        if self.core.node_id().ne(&resp.node_id) {
            // If the heartbeat is not send by this node, ignore the response.
            return;
        }

        if resp.term > self.core.current_term() {
            info!(
                "[{}][Term({})] revert to follower due to a newer term from heartbeat response",
                self.core.node_id(),
                self.core.current_term()
            );
            self.core.set_state(State::Follower, set_prev_state);
        }
    }

    fn spawn_move_leader(&self, target_node_id: T::NodeId, tx: Sender<Result<()>>) {
        let current_term = self.core.current_term();

        let rpc = self.core.rpc.clone();
        let node_id = self.core.node_id().clone();
        let tx2 = tx.clone();

        debug!(
            "[{}][Term({})] send move_leader to node({})",
            node_id, current_term, target_node_id
        );

        let req = MoveLeaderRequest {
            target_node_id,
            term: current_term,
        };

        let result = self.core.spawn_task("election-move-leader", move || {
            match rpc.move_leader(req) {
                Ok(_) => {
                    // ignore send error
                    let _ = tx.send(Ok(()));
                }
                Err(e) => {
                    // ignore send error
                    let _ = tx.send(Err(to_rpc_error::<T>(e)));
                }
            }
        });

        if let Err(e) = result {
            // ignore send error
            let _ = tx2.send(Err(e));
        }
    }

    pub fn run(mut self) {
        self.core.increase_state_id();

        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);
        self.core.in_moving_leader = false;

        assert!(self.core.is_state(State::Leader));
        let result = self.core.spawn_event_handling_task(Event::TransitToLeader {
            term: self.core.current_term(),
        });
        if result.is_err() {
            error!(
                "[{}][Term({})] failed to handle TransitToLeader event, so transit to follower",
                self.core.node_id(),
                self.core.current_term()
            );
            // Do not set prev_state, because we want keep prev_state unchanged
            self.core.set_state(State::Follower, None);
            return;
        }

        // Setup state as leader
        self.core.last_heartbeat_time = None;
        self.core.next_election_timeout = None;
        self.next_heartbeat_timeout = None;
        self.core.current_leader = Some(self.core.node_id().clone());
        self.core.report_metrics();

        info!(
            "[{}][Term({})] start the leader loop",
            self.core.node_id(),
            self.core.current_term()
        );

        // Send the first heartbeat
        self.spawn_parallel_heartbeat();

        loop {
            if !self.core.is_state(State::Leader) {
                return;
            }

            let heartbeat_timeout = self.next_heartbeat_timeout();

            match self.core.msg_rx.recv_deadline(heartbeat_timeout) {
                Ok(msg) => match msg {
                    Message::HeartbeatRequest { req, tx } => {
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
                    Message::HeartbeatResponse(resp) => {
                        self.handle_heartbeat_response(resp, set_prev_state.as_mut());
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
                    Message::VoteResponse(_) => {
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
                        term: _,
                        state_id,
                    } => {
                        if let Some(e) = error {
                            error!(
                                "[{}][Term({})] failed to handle event ({:?}): {}",
                                self.core.node_id(),
                                self.core.current_term(),
                                event,
                                e
                            );
                            if let Event::TransitToLeader { .. } = event {
                                if self.core.state_id() == state_id {
                                    error!(
                                        "[{}][Term({})] failed to handle TransitToLeader event, so transit to follower",
                                        self.core.node_id(),
                                        self.core.current_term()
                                    );
                                    // Do not set prev_state, because we want keep prev_state unchanged
                                    self.core.set_state(State::Follower, None);
                                }
                            }
                        }
                    }
                    Message::MoveLeader { target_node, tx } => {
                        if !self.core.members.contains(&target_node) {
                            let _ = tx.send(Err(try_format_error!(NotAllowed, "{} is not a member", target_node)));
                        } else {
                            self.spawn_move_leader(target_node, tx);
                        }
                    }
                    Message::MoveLeaderRequest { tx, .. } => {
                        self.core.reject_move_leader(tx);
                    }
                },
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        // Time to send heartbeat
                        self.spawn_parallel_heartbeat();
                        self.next_heartbeat_timeout = None;
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
