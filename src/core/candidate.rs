use crate::core::{ElectionCore, State};
use crate::error::Result;
use crate::msg::Message;
use crate::rpc::{Rpc, VoteRequest, VoteResponse};
use crate::storage::Storage;
use crate::{ElectionType, Event, Quorum};
use crossbeam_channel::RecvTimeoutError;

pub struct Candidate<'a, T: ElectionType> {
    core: &'a mut ElectionCore<T>,
    pre_vote: bool,
    // The number of votes needed from the member config in order to become leader.
    votes_needed: usize,
    // The number of votes which have been granted by peer nodes of the member config.
    votes_granted: usize,
}

impl<'a, T: ElectionType> Candidate<'a, T> {
    #[inline]
    pub fn new(core: &'a mut ElectionCore<T>, pre_vote: bool) -> Self {
        Self {
            core,
            pre_vote,
            votes_granted: 0,
            votes_needed: 0,
        }
    }

    #[inline]
    fn is_specified_candidate(&self) -> bool {
        if self.pre_vote {
            self.core.is_state(State::PreCandidate)
        } else {
            self.core.is_state(State::Candidate)
        }
    }

    pub fn run(mut self) {
        self.core.increase_state_id();

        // Use set_prev_state to ensure prev_state can be set at most once.
        let mut set_prev_state = Some(true);

        if self.pre_vote {
            assert!(self.core.is_state(State::PreCandidate));
            let _result = self.core.spawn_event_handling_task(Event::TransitToPreCandidate);
            info!(
                "[{}][Term({})] start the pre-candidate loop",
                self.core.node_id(),
                self.core.current_term()
            );
        } else {
            assert!(self.core.is_state(State::Candidate));
            let _result = self.core.spawn_event_handling_task(Event::TransitToCandidate);
            info!(
                "[{}][Term({})] start the candidate loop",
                self.core.node_id(),
                self.core.current_term()
            );
        }

        self.core.report_metrics();

        loop {
            if !self.is_specified_candidate() {
                return;
            }

            self.calculate_needed_votes();

            self.core.update_next_election_timeout(false);
            if !self.pre_vote {
                self.core.current_leader = None;
                self.core.hard_state.current_term += 1;
                self.core.hard_state.voted_for = Some(self.core.node_id().clone());
                let current_term = self.core.current_term();
                if let Err(e) = self.core.storage.save_hard_state(&self.core.hard_state) {
                    error!(
                        "[{}][Term({})] election is shutting down caused by fatal storage error: {}",
                        self.core.node_id(),
                        current_term,
                        e
                    );
                    self.core.set_state(State::Shutdown, set_prev_state.as_mut());
                    return;
                }
                self.core.update_metrics(|metrics| {
                    metrics.current_leader = None;
                    metrics.current_term = current_term;
                });
            }

            self.spawn_parallel_vote_request();

            loop {
                if !self.is_specified_candidate() {
                    return;
                }

                let election_timeout = self.core.next_election_timeout();

                match self.core.msg_rx.recv_deadline(election_timeout) {
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
                        Message::HeartbeatResponse(_) => {
                            // ignore heartbeat response
                        }
                        Message::VoteRequest { req, tx } => {
                            debug!(
                                "[{}][Term({})] received vote request: {:?}",
                                self.core.node_id(),
                                self.core.current_term(),
                                req
                            );

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
                        Message::VoteResponse(resp) => {
                            debug!(
                                "[{}][Term({})] received vote response : {:?}",
                                self.core.node_id(),
                                self.core.current_term(),
                                resp
                            );
                            if let Err(e) = self.handle_vote_response(resp, set_prev_state.as_mut()) {
                                debug!(
                                    "[{}][Term({})] failed to handle vote response: {}",
                                    self.core.node_id(),
                                    self.core.current_term(),
                                    e
                                );
                            }
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
                    },
                    Err(e) => match e {
                        RecvTimeoutError::Timeout => {
                            // This election has timed-out. Break to outer loop, which starts a new vote.
                            break;
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

    fn calculate_needed_votes(&mut self) {
        let major_old = self.core.members.all_members_num() / 2 + 1;
        self.votes_needed = match self.core.options.quorum() {
            Quorum::Major => major_old,
            Quorum::Any(n) => (n as usize).max(major_old).min(self.core.members.all_members_num()),
        };
        self.votes_granted = 1; // vote for ourselves

        debug!(
            "[{}][Term({})] quorum is {:?}, votes granted({}/{})",
            self.core.node_id(),
            self.core.current_term(),
            self.core.options.quorum(),
            self.votes_granted,
            self.votes_needed,
        );
    }

    fn spawn_parallel_vote_request(&mut self) {
        let current_term = self.core.current_term();

        let vote_factor = match self.core.storage.load_vote_factor() {
            Ok(factor) => factor,
            Err(e) => {
                warn!(
                    "[{}][Term({})] failed to get vote factor: {}",
                    self.core.node_id(),
                    current_term,
                    e
                );
                return;
            }
        };

        let peers = self.core.members.peers();
        if peers.is_empty() {
            return;
        }

        if self.pre_vote {
            debug!(
                "[{}][Term({})] start to send pre-vote request to all nodes({})",
                self.core.node_id(),
                current_term,
                peers.len()
            );
        } else {
            debug!(
                "[{}][Term({})] start to send vote request to all nodes({})",
                self.core.node_id(),
                current_term,
                peers.len()
            );
        }

        self.core.vote_id += 1;
        let vote_id = self.core.vote_id;

        for member in peers {
            let req = VoteRequest {
                target_node_id: member.clone(),
                candidate_id: self.core.node_id().clone(),
                vote_id,
                term: current_term,
                factor: vote_factor.clone(),
                pre_vote: self.pre_vote,
                move_leader: self.core.in_moving_leader,
            };

            let rpc = self.core.rpc.clone();
            let tx = self.core.msg_tx.clone();
            let node_id = self.core.node_id().clone();
            let target_node_id = member.clone();

            let _ = self.core.spawn_task("election-vote", move || match rpc.vote(req) {
                Ok(resp) => {
                    let _ = tx.send(Message::VoteResponse(resp));
                }
                Err(e) => {
                    warn!(
                        "[{}][Term({})] failed to send vote request to node({}): {}",
                        node_id, current_term, target_node_id, e
                    );
                }
            });
        }
    }

    fn handle_vote_response(&mut self, msg: VoteResponse<T>, set_prev_state: Option<&mut bool>) -> Result<()> {
        self.core.check_node(&msg.candidate_id)?;

        if self.core.vote_id != msg.vote_id {
            debug!(
                "[{}][Term({})] vote id is {}, so ignore vote response: {:?}",
                self.core.node_id(),
                self.core.current_term(),
                self.core.vote_id,
                msg
            );
            return Ok(());
        }

        // If peer's term is greater than current term, revert to follower state.
        if msg.term > self.core.current_term() {
            self.core.update_current_term(msg.term, None)?;
            self.core.current_leader = None;
            self.core.set_state(State::Follower, set_prev_state);
            // The metrics will be updated in Follower state.
            info!(
                "[{}][Term({})] revert to follower due to greater term({}) observed in vote response then current term({})",
                self.core.node_id(), self.core.current_term(), msg.term, self.core.current_term()
            );
            return Ok(());
        }

        if msg.vote_result.is_granted() {
            if self.core.members.peers().contains(&msg.node_id) {
                self.votes_granted += 1;
            }

            debug!(
                "[{}][Term({})] votes granted({}/{})",
                self.core.node_id(),
                self.core.current_term(),
                self.votes_granted,
                self.votes_needed,
            );

            if self.votes_granted >= self.votes_needed {
                if self.pre_vote {
                    info!(
                        "[{}][Term({})] minimum number of pre-votes have been received, so transit to candidate",
                        self.core.node_id(),
                        self.core.current_term()
                    );
                    self.core.set_state(State::Candidate, set_prev_state);
                } else {
                    info!(
                        "[{}][Term({})] minimum number of votes have been received, so transit to leader",
                        self.core.node_id(),
                        self.core.current_term()
                    );
                    self.core.set_state(State::Leader, set_prev_state);
                }
                self.core.in_moving_leader = false;
                // The metrics will be updated in Candidate or Leader state.
                return Ok(());
            }
        }

        Ok(())
    }
}
