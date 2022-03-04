use crate::core::{RaftCore, State};
use crate::msg::Message;
use crate::rpc::HeartbeatRequest;
use crate::{Event, HeartbeatResponse, RaftType, Rpc};
use crossbeam_channel::RecvTimeoutError;
use std::collections::HashSet;
use std::time::{Duration, Instant};

pub struct Leader<'a, T: RaftType> {
    core: &'a mut RaftCore<T>,
    next_heartbeat_timeout: Option<Instant>,
}

impl<'a, T: RaftType> Leader<'a, T> {
    #[inline]
    pub fn new(core: &'a mut RaftCore<T>) -> Self {
        Self {
            core,
            next_heartbeat_timeout: None,
        }
    }

    #[inline]
    fn target_members(&self) -> HashSet<T::NodeId> {
        let mut members = self.core.members.all_members();
        members.remove(&self.core.node_id);
        members
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
        for member in self
            .core
            .members
            .all_members()
            .into_iter()
            .filter(|member| member != &self.core.node_id)
        {
            let req = HeartbeatRequest {
                group_id: self.core.group_id.clone(),
                target_node_id: member.clone(),
                leader_id: self.core.node_id.clone(),
                term: self.core.hard_state.current_term,
            };

            let rpc = self.core.rpc.clone();
            let tx = self.core.msg_tx.clone();
            let node_id = self.core.node_id.clone();

            trace!("[Node({})] send heartbeat to node({})", node_id, member);

            let _ = self.core.spawn_task("raft-heartbeat", move || {
                match rpc.heartbeat(req) {
                    Ok(resp) => {
                        // ignore send error
                        let _ = tx.send(Message::HeartbeatResponse(resp));
                    }
                    Err(e) => {
                        warn!("[Node({})] failed to send vote request to {}: {}", node_id, member, e);
                    }
                }
            });
        }
    }

    #[inline]
    fn handle_heartbeat_response(&mut self, resp: HeartbeatResponse<T>, set_prev_state: Option<&mut bool>) {
        if self.core.group_id != resp.group_id {
            return;
        }

        if self.core.node_id != resp.node_id {
            return;
        }

        if resp.term > self.core.hard_state.current_term {
            info!(
                "[Node({})] revert to follower due to a newer term from heartbeat response",
                self.core.node_id
            );
            self.core.set_state(State::Follower, set_prev_state);
        }
    }

    pub fn run(mut self) {
        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);

        assert!(self.core.is_state(State::Leader));
        let result = self.core.handle_event(Event::TransitToLeader {
            members: self.target_members(),
            term: self.core.hard_state.current_term,
        });
        if result.is_err() {
            error!(
                "[Node({})] failed to handle TransitToLeader event, so transit to follower",
                self.core.node_id,
            );
            // Do not set prev_state, because we want keep prev_state unchanged
            self.core.set_state(State::Follower, None);
            return;
        }

        // Setup state as leader
        self.core.last_heartbeat = None;
        self.core.next_election_timeout = None;
        self.next_heartbeat_timeout = None;
        self.core.current_leader = Some(self.core.node_id.clone());
        self.core.report_metrics();

        info!(
            "[Node({})] start the leader loop with term({})",
            self.core.node_id, self.core.hard_state.current_term
        );

        loop {
            if !self.core.is_state(State::Leader) {
                return;
            }

            let heartbeat_timeout = self.next_heartbeat_timeout();

            match self.core.msg_rx.recv_deadline(heartbeat_timeout) {
                Ok(msg) => match msg {
                    Message::Heartbeat { req, tx } => {
                        let result = self.core.handle_heartbeat(req, set_prev_state.as_mut());
                        if let Err(ref e) = result {
                            debug!(
                                "[Node({})] failed to handle heartbeat request: {}",
                                self.core.node_id, e
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
                            debug!("[Node({})] failed to handle vote request: {}", self.core.node_id, e);
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
                        info!("[Node({})] raft update options: {:?}", self.core.node_id, options);
                        self.core.update_options(options);
                        let _ = tx.send(Ok(()));
                    }
                    Message::Shutdown => {
                        info!("[Node({})] raft received shutdown message", self.core.node_id);
                        self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    }
                    Message::EventHandlingError { event, error } => {
                        error!(
                            "[Node({})] raft failed to handle event ({:?}): {}",
                            self.core.node_id, event, error
                        );
                        if let Event::TransitToLeader { term, .. } = event {
                            if self.core.hard_state.current_term == term {
                                error!(
                                    "[Node({})] failed to handle TransitToLeader event, so transit to follower",
                                    self.core.node_id,
                                );
                                // Do not set prev_state, because we want keep prev_state unchanged
                                self.core.set_state(State::Follower, None);
                            }
                        }
                    }
                },
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        // Time to send heartbeat
                        self.spawn_parallel_heartbeat();
                        self.next_heartbeat_timeout = None;
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
