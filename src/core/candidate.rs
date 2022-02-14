use crate::core::{RaftCore, State};
use crate::error::Result;
use crate::msg::Message;
use crate::rpc::{Rpc, VoteRequest, VoteResponse};
use crate::storage::Storage;
use crate::{Event, RaftType, TaskSpawner};
use crossbeam_channel::RecvTimeoutError;

pub struct Candidate<'a, T: RaftType> {
    core: &'a mut RaftCore<T>,
    pre_vote: bool,
    // The number of votes needed from the old (current) member config in order to become the raft leader.
    votes_needed_old: usize,
    // The number of votes which have been granted by peer nodes of the old (current) member config.
    votes_granted_old: usize,
    // The number of votes needed from the new member config in order to become the raft leader (if applicable).
    votes_needed_new: usize,
    // The number of votes which have been granted by peer nodes of the new member config (if applicable).
    votes_granted_new: usize,
}

impl<'a, T: RaftType> Candidate<'a, T> {
    #[inline]
    pub fn new(core: &'a mut RaftCore<T>, pre_vote: bool) -> Self {
        Self {
            core,
            pre_vote,
            votes_granted_old: 0,
            votes_needed_new: 0,
            votes_needed_old: 0,
            votes_granted_new: 0,
        }
    }

    #[inline]
    fn is_specified_candidate(&self) -> bool {
        if self.pre_vote {
            if self.core.state == State::PreCandidate {
                return true;
            }
        } else if self.core.state == State::Candidate {
            return true;
        }

        false
    }

    pub fn run(mut self) {
        if self.pre_vote {
            assert_eq!(self.core.state, State::PreCandidate);
            self.core.notify_event(Event::TransitToPreCandidate);
            info!("[Node({})] start the pre-candidate loop", self.core.node_id);
        } else {
            assert_eq!(self.core.state, State::Candidate);
            self.core.notify_event(Event::TransitToCandidate);
            info!("[Node({})] start the candidate loop", self.core.node_id);
        }

        self.core.report_metrics();

        loop {
            if !self.is_specified_candidate() {
                return;
            }

            self.votes_needed_old = self.core.members.members.len() / 2 + 1;
            self.votes_granted_old = 1; // vote for ourselves
            if let Some(members) = &self.core.members.members_after_consensus {
                self.votes_needed_new = members.len() / 2 + 1;
                self.votes_granted_new = 1; // vote for ourselves
            }

            self.core.update_next_election_timeout(false);
            if !self.pre_vote {
                self.core.current_leader = None;
                self.core.hard_state.current_term += 1;
                self.core.hard_state.voted_for = Some(self.core.node_id.clone());
                if let Err(e) = self.core.storage.save_hard_state(&self.core.hard_state) {
                    error!(
                        "[Node({})] raft is shutting down caused by fatal storage error: {}",
                        self.core.node_id, e
                    );
                    self.core.state = State::Shutdown;
                    return;
                }
                self.core.report_metrics();
            }

            self.spawn_parallel_vote_request();

            loop {
                if !self.is_specified_candidate() {
                    return;
                }

                let election_timeout = self.core.next_election_timeout();

                match self.core.msg_rx.recv_deadline(election_timeout) {
                    Ok(msg) => match msg {
                        Message::Heartbeat { req, tx } => {
                            let result = self.core.handle_heartbeat(req);
                            if let Err(ref e) = result {
                                debug!(
                                    "[Node({})] failed to handle heartbeat request: {}",
                                    self.core.node_id, e
                                );
                            }
                            let _ = tx.send(result);
                        }
                        Message::HeartbeatResponse(_) => {
                            // ignore heartbeat response
                        }
                        Message::VoteRequest { req, tx } => {
                            debug!("[Node({})] received vote request: {:?}", self.core.node_id, req);

                            let result = self.core.handle_vote_request(req);
                            if let Err(ref e) = result {
                                debug!("[Node({})] failed to handle vote request: {}", self.core.node_id, e);
                            }
                            let _ = tx.send(result);
                        }
                        Message::VoteResponse(resp) => {
                            debug!("[Node({})] received vote response : {:?}", self.core.node_id, resp);
                            if let Err(e) = self.handle_vote_response(resp) {
                                debug!("[Node({})] failed to handle vote response: {}", self.core.node_id, e);
                            }
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
                            self.core.state = State::Shutdown;
                        }
                    },
                    Err(e) => match e {
                        RecvTimeoutError::Timeout => {
                            // This election has timed-out. Break to outer loop, which starts a new vote.
                            break;
                        }
                        RecvTimeoutError::Disconnected => {
                            info!("[Node({})] the raft message channel is disconnected", self.core.node_id);
                            self.core.state = State::Shutdown;
                        }
                    },
                }
            }
        }
    }

    fn spawn_parallel_vote_request(&mut self) {
        let vote_factor = match self.core.storage.load_vote_factor() {
            Ok(factor) => factor,
            Err(e) => {
                warn!("[Node({})] failed to get vote factor: {}", self.core.node_id, e);
                return;
            }
        };

        let mut members = self.core.members.all_members();
        members.remove(&self.core.node_id);
        if members.is_empty() {
            return;
        }

        if self.pre_vote {
            debug!(
                "[Node({})] start to send pre-vote request to all nodes({})",
                self.core.node_id,
                members.len()
            );
        } else {
            debug!(
                "[Node({})] start to send vote request to all nodes({})",
                self.core.node_id,
                members.len()
            );
        }

        self.core.vote_id += 1;
        let vote_id = self.core.vote_id;

        for member in members.into_iter() {
            let req = VoteRequest {
                group_id: self.core.group_id.clone(),
                target_node_id: member.clone(),
                candidate_id: self.core.node_id.clone(),
                pre_vote: self.pre_vote,
                vote_id,
                term: self.core.hard_state.current_term,
                factor: vote_factor.clone(),
            };

            let rpc = self.core.rpc.clone();
            let tx = self.core.msg_tx.clone();
            let node_id = self.core.node_id.clone();

            let _ = self
                .core
                .task_spawner
                .spawn(Some(String::from("raft-vote")), move || match rpc.vote(req) {
                    Ok(resp) => {
                        let _ = tx.send(Message::VoteResponse(resp));
                    }
                    Err(e) => {
                        warn!(
                            "[Node({})] failed to send vote request to node({}): {}",
                            node_id, member, e
                        );
                    }
                });
        }
    }

    fn handle_vote_response(&mut self, msg: VoteResponse<T>) -> Result<()> {
        self.core.check_group(&msg.group_id)?;
        self.core.check_node(&msg.candidate_id)?;

        if self.core.vote_id != msg.vote_id {
            debug!(
                "[Node({})] vote id is {}, so ignore vote response: {:?}",
                self.core.node_id, self.core.vote_id, msg
            );
            return Ok(());
        }

        // If peer's term is greater than current term, revert to follower state.
        if msg.term > self.core.hard_state.current_term {
            self.core.update_current_term(msg.term, None)?;
            self.core.current_leader = None;
            self.core.state = State::Follower;
            self.core.report_metrics();
            info!(
                "[Node({})] revert to follower due to greater term({}) observed in vote response then current term({})",
                self.core.node_id, msg.term, self.core.hard_state.current_term
            );
            return Ok(());
        }

        if msg.vote_granted {
            if self.core.members.contains(&msg.node_id) {
                self.votes_granted_old += 1;
            }
            if self
                .core
                .members
                .members_after_consensus
                .as_ref()
                .map(|m| m.contains(&msg.node_id))
                .unwrap_or(false)
            {
                self.votes_granted_new += 1;
            }

            if self.votes_granted_old >= self.votes_needed_old && self.votes_granted_new >= self.votes_needed_new {
                if self.pre_vote {
                    info!(
                        "[Node({})] minimum number of pre-votes have been received, so transit to candidate",
                        self.core.node_id
                    );
                    self.core.state = State::Candidate;
                } else {
                    info!(
                        "[Node({})] minimum number of votes have been received, so transit to leader",
                        self.core.node_id
                    );
                    self.core.state = State::Leader;
                }
                self.core.report_metrics();
                return Ok(());
            }
        }

        Ok(())
    }
}
