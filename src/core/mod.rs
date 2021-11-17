use crate::core::candidate::Candidate;
use crate::core::follower::Follower;
use crate::core::leader::Leader;
use crate::core::startup::Startup;
use crate::error::{Error, Result};
use crate::metrics::{Metrics, MetricsReporter};
use crate::msg::Message;
use crate::rpc::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse};
use crate::storage::{HardState, Storage};
use crate::task::TaskSpawner;
use crate::{Event, EventListener, Options, RaftType, VoteFactor};
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

mod candidate;
mod follower;
mod leader;
mod startup;

/// The state of the raft node.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum State {
    Startup,
    Follower,
    PreCandidate,
    Candidate,
    Leader,
    Shutdown,
}

pub struct MemberConfig<T: RaftType> {
    pub(crate) members: HashSet<T::NodeId>,
    pub(crate) members_after_consensus: Option<HashSet<T::NodeId>>,
}

impl<T: RaftType> MemberConfig<T> {
    #[inline]
    pub fn all_members(&self) -> HashSet<T::NodeId> {
        let mut all = self.members.clone();
        if let Some(members) = &self.members_after_consensus {
            all.extend(members.iter().cloned());
        }
        all
    }

    #[inline]
    pub fn contains(&self, node_id: &T::NodeId) -> bool {
        self.members.contains(node_id)
            || self
                .members_after_consensus
                .as_ref()
                .map_or(false, |m| m.contains(node_id))
    }

    #[allow(dead_code)]
    #[inline]
    pub fn is_in_joint_consensus(&self) -> bool {
        self.members_after_consensus.is_some()
    }

    #[inline]
    pub fn with_node(node_id: T::NodeId) -> Self {
        let mut members = HashSet::new();
        members.insert(node_id);
        Self {
            members,
            members_after_consensus: None,
        }
    }
}

pub struct RaftCore<T: RaftType> {
    options: Options,
    group_id: T::GroupId,
    node_id: T::NodeId,
    members: MemberConfig<T>,

    task_spawner: Arc<T::TaskSpawner>,
    storage: T::Storage,
    rpc: Arc<T::Rpc>,

    state: State,
    hard_state: HardState<T>,
    current_leader: Option<T::NodeId>,
    vote_id: u64,

    /// The last time a heartbeat was received.
    last_heartbeat: Option<Instant>,
    /// The duration until the next election timeout.
    next_election_timeout: Option<Instant>,

    msg_tx: Sender<Message<T>>,
    msg_rx: Receiver<Message<T>>,
    event_listeners: Vec<Arc<dyn EventListener<T>>>,
    metrics_reporter: MetricsReporter<T>,
}

impl<T: RaftType> RaftCore<T> {
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn new(
        options: Options,
        group_id: T::GroupId,
        node_id: T::NodeId,
        task_spawner: Arc<T::TaskSpawner>,
        storage: T::Storage,
        rpc: Arc<T::Rpc>,
        msg_tx: Sender<Message<T>>,
        msg_rx: Receiver<Message<T>>,
        event_listeners: Vec<Arc<dyn EventListener<T>>>,
        metrics_reporter: MetricsReporter<T>,
    ) -> Self {
        RaftCore {
            options,
            group_id,
            node_id: node_id.clone(),
            members: MemberConfig::with_node(node_id),
            state: State::Startup,
            hard_state: HardState {
                current_term: 0,
                voted_for: None,
            },
            current_leader: None,
            vote_id: 0,
            task_spawner,
            storage,
            rpc,
            last_heartbeat: None,
            next_election_timeout: None,
            msg_tx,
            msg_rx,
            event_listeners,
            metrics_reporter,
        }
    }

    #[inline]
    pub fn spawn(self) -> Result<JoinHandle<Result<()>>> {
        std::thread::Builder::new()
            .name(String::from("raft-main"))
            .spawn(move || self.main())
            .map_err(|e| Error::TaskError(format!("failed to spawn raft main task: {}", e)))
    }

    fn main(mut self) -> Result<()> {
        info!("[Node({})] start raft main task", self.node_id);
        self.state = State::Startup;

        loop {
            match &self.state {
                State::Startup => Startup::new(&mut self).run()?,
                State::Follower => Follower::new(&mut self).run()?,
                State::PreCandidate => Candidate::new(&mut self, true).run()?,
                State::Candidate => Candidate::new(&mut self, false).run()?,
                State::Leader => Leader::new(&mut self).run()?,
                State::Shutdown => {
                    self.notify_event(Event::Shutdown);
                    info!("[Node({})] Raft has shutdown", self.node_id);
                    return Ok(());
                }
            }
        }
    }

    #[inline]
    fn map_fatal_storage_error(&mut self, error: Error) -> Error {
        error!(
            "[Node({})] raft is shutting down caused by fatal storage error: {}",
            self.node_id, error
        );
        self.state = State::Shutdown;
        error
    }

    #[inline]
    fn set_hard_state(&mut self, state: HardState<T>) {
        self.hard_state = state;
    }

    #[inline]
    fn next_election_timeout(&mut self) -> Instant {
        match self.next_election_timeout {
            Some(instant) => instant,
            None => {
                let timeout = self.options.random_election_timeout();
                let instant = Instant::now() + Duration::from_millis(timeout);
                self.next_election_timeout = Some(instant);
                instant
            }
        }
    }

    #[inline]
    fn update_next_election_timeout(&mut self, heartbeat: bool) {
        let now = Instant::now();
        self.next_election_timeout = Some(now + Duration::from_millis(self.options.random_election_timeout()));
        if heartbeat {
            self.last_heartbeat = Some(now);
        }
    }

    #[inline]
    fn check_group(&self, group_id: &T::GroupId) -> Result<()> {
        if self.group_id.ne(group_id) {
            return Err(Error::InvalidTarget(format!(
                "given group({}) is not the same as this raft group({})",
                group_id, self.group_id
            )));
        } else {
            Ok(())
        }
    }

    #[inline]
    fn check_node(&self, node_id: &T::NodeId) -> Result<()> {
        if self.node_id.ne(node_id) {
            return Err(Error::InvalidTarget(format!(
                "given node id({}) is not the same as this node({})",
                node_id, self.node_id
            )));
        } else {
            Ok(())
        }
    }

    #[inline]
    fn update_current_term(&mut self, new_term: u64, voted_for: Option<T::NodeId>) {
        if new_term > self.hard_state.current_term {
            self.hard_state.current_term = new_term;
            self.hard_state.voted_for = voted_for;
        }
    }

    #[inline]
    fn save_hard_state(&mut self) -> Result<()> {
        self.storage.save_hard_state(&self.hard_state)
    }

    fn handle_heartbeat(&mut self, msg: HeartbeatRequest<T>) -> Result<HeartbeatResponse<T>> {
        self.check_group(&msg.group_id)?;
        self.check_node(&msg.target_node_id)?;

        if msg.term < self.hard_state.current_term {
            debug!(
                "[Node({})] heartbeat term({}) from leader({}) is less than current term({})",
                self.node_id, msg.term, msg.leader_id, self.hard_state.current_term
            );
            return Ok(HeartbeatResponse {
                group_id: self.group_id.clone(),
                node_id: self.node_id.clone(),
                term: self.hard_state.current_term,
            });
        }

        self.update_next_election_timeout(true);
        let mut report_metrics = false;

        // update current term if needed
        if self.hard_state.current_term != msg.term {
            self.update_current_term(msg.term, None);
            self.save_hard_state()?;
            report_metrics = true;
        }

        // update current leader if needed
        if self.current_leader.as_ref() != Some(&msg.leader_id) {
            match self.current_leader.as_ref() {
                None => {
                    info!("[Node({})] change leader to {}", self.node_id, msg.leader_id);
                }
                Some(old_leader) => {
                    info!(
                        "[Node({})] change leader from {} to {}",
                        self.node_id, old_leader, msg.leader_id
                    );
                }
            }
            self.current_leader = Some(msg.leader_id.clone());
            self.notify_event(Event::ChangeLeader(msg.leader_id));
            report_metrics = true;
        }

        // transition to follower state if needed
        if self.state != State::Follower {
            info!(
                "[Node({})] raft received valid heartbeat in {:?} state, so transit to follower",
                self.node_id, self.state
            );
            self.state = State::Follower;
            report_metrics = true;
        }

        if report_metrics {
            self.report_metrics();
        }

        Ok(HeartbeatResponse {
            group_id: self.group_id.clone(),
            node_id: self.node_id.clone(),
            term: self.hard_state.current_term,
        })
    }

    fn handle_vote_request(&mut self, msg: VoteRequest<T>) -> Result<VoteResponse<T>> {
        self.check_group(&msg.group_id)?;
        self.check_node(&msg.target_node_id)?;

        if msg.term < self.hard_state.current_term {
            debug!(
                "[Node({})] vote term({}) from candidate({}) is less than current term({})",
                self.node_id, msg.term, msg.candidate_id, self.hard_state.current_term
            );
            return Ok(VoteResponse {
                group_id: self.group_id.clone(),
                node_id: self.node_id.clone(),
                candidate_id: msg.candidate_id,
                pre_vote: msg.pre_vote,
                vote_id: msg.vote_id,
                term: self.hard_state.current_term,
                vote_granted: false,
            });
        }

        // Do not respond to the request if we've received a heartbeat within the election timeout minimum.
        if let Some(instant) = self.last_heartbeat {
            let now = Instant::now();
            let delta = now.duration_since(instant);
            if (delta.as_millis() as u64) <= self.options.election_timeout_min() {
                debug!(
                    "[Node({})] reject vote request received within election timeout minimum",
                    self.node_id
                );
                return Ok(VoteResponse {
                    group_id: self.group_id.clone(),
                    node_id: self.node_id.clone(),
                    candidate_id: msg.candidate_id,
                    pre_vote: msg.pre_vote,
                    vote_id: msg.vote_id,
                    term: self.hard_state.current_term,
                    vote_granted: false,
                });
            }
        }

        // Per spec, if we observe a term greater than our own outside of the election timeout
        // minimum, then we must update term & immediately become follower. We still need to
        // do vote checking after this.
        if msg.term > self.hard_state.current_term {
            info!(
                "[Node({})] vote request term({}) is greater than current term({}), so transit to follower",
                self.node_id, msg.term, self.hard_state.current_term
            );
            self.update_current_term(msg.term, None);
            self.update_next_election_timeout(false);
            self.state = State::Follower;
            self.save_hard_state()?;
            self.report_metrics();
        }

        // Check if candidate's vote factor can be granted.
        // If candidate's vote factor is not granted, then reject.
        let current_vote_factor = self.storage.get_vote_factor()?;
        let candidate_is_granted = current_vote_factor.vote(&msg.factor);
        if !candidate_is_granted {
            debug!(
                "[Node({})] reject vote request as candidate({})'s vote factor is not granted",
                self.node_id, msg.candidate_id
            );
            return Ok(VoteResponse {
                group_id: self.group_id.clone(),
                node_id: self.node_id.clone(),
                candidate_id: msg.candidate_id,
                pre_vote: msg.pre_vote,
                vote_id: msg.vote_id,
                term: self.hard_state.current_term,
                vote_granted: false,
            });
        }

        // If the request is a PreVote, then at this point we can respond
        // to the candidate telling them that we would vote for them.
        if msg.pre_vote {
            debug!("[Node({})] voted for pre-candidate({})", self.node_id, msg.candidate_id);
            return Ok(VoteResponse {
                group_id: self.group_id.clone(),
                node_id: self.node_id.clone(),
                candidate_id: msg.candidate_id,
                pre_vote: msg.pre_vote,
                vote_id: msg.vote_id,
                term: self.hard_state.current_term,
                vote_granted: true,
            });
        }

        match &self.hard_state.voted_for {
            None => {
                // This node has not yet voted for the current term, so vote for the candidate.
                self.hard_state.voted_for = Some(msg.candidate_id.clone());
                self.state = State::Follower;
                self.update_next_election_timeout(false);
                self.save_hard_state()?;
                self.report_metrics();
                debug!("[Node({})] voted for candidate({})", self.node_id, msg.candidate_id);
                Ok(VoteResponse {
                    group_id: self.group_id.clone(),
                    node_id: self.node_id.clone(),
                    candidate_id: msg.candidate_id,
                    pre_vote: msg.pre_vote,
                    vote_id: msg.vote_id,
                    term: self.hard_state.current_term,
                    vote_granted: true,
                })
            }
            Some(candidate_id) => {
                let vote_granted = msg.candidate_id.eq(candidate_id);
                if vote_granted {
                    debug!("[Node({})] voted for candidate({})", self.node_id, msg.candidate_id);
                } else {
                    debug!(
                        "[Node({})] reject vote request for candidate({}) because already voted for node({})",
                        self.node_id, msg.candidate_id, candidate_id
                    );
                }
                Ok(VoteResponse {
                    group_id: self.group_id.clone(),
                    node_id: self.node_id.clone(),
                    candidate_id: msg.candidate_id,
                    pre_vote: msg.pre_vote,
                    vote_id: msg.vote_id,
                    term: self.hard_state.current_term,
                    vote_granted,
                })
            }
        }
    }

    fn notify_event(&self, event: Event<T>) {
        for listener in &self.event_listeners {
            let listener = listener.clone();
            let event = event.clone();
            // TODO: handle error
            let _ = self
                .task_spawner
                .spawn(Some(String::from("raft-event-listener")), move || {
                    listener.event_performed(event);
                });
        }
    }

    #[inline]
    fn reject_init_with_members(&self, tx: Sender<Result<()>>) {
        let _ = tx.send(Err(Error::NotAllowed(format!(
            "can't init with members in {:?} state",
            self.state,
        ))));
    }

    #[inline]
    fn report_metrics(&mut self) {
        self.metrics_reporter.report(Metrics {
            state: self.state,
            current_term: self.hard_state.current_term,
            current_leader: self.current_leader.clone(),
        })
    }
}
