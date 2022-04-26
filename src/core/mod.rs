use crate::core::candidate::Candidate;
use crate::core::follower::Follower;
use crate::core::leader::Leader;
use crate::core::observer::Observer;
use crate::core::startup::Startup;
use crate::error::{to_storage_error, Error, Result};
use crate::member::MemberConfig;
use crate::metrics::{Metrics, MetricsReporter};
use crate::msg::Message;
use crate::rpc::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse};
use crate::storage::{HardState, Storage};
use crate::task::TaskSpawner;
use crate::util::TryToString;
use crate::wait_group::WaitGroup;
use crate::{ElectionType, Event, EventHandler, Options, Thread, VoteFactor, VoteResult};
use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod candidate;
mod follower;
mod leader;
mod observer;
mod startup;

/// The state of the node.
#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum State {
    Shutdown = 0,
    Startup = 1,
    Follower = 2,
    PreCandidate = 3,
    Candidate = 4,
    Leader = 5,
    Observer = 6,
}

pub struct ElectionCore<T: ElectionType> {
    options: Options,
    node_id: T::NodeId,
    members: MemberConfig<T>,

    task_spawner: Arc<T::TaskSpawner>,
    storage: T::Storage,
    rpc: Arc<T::Rpc>,

    state: State,
    prev_state: State,
    state_id: u64,
    hard_state: HardState<T>,
    current_leader: Option<T::NodeId>,
    vote_id: u64,
    in_moving_leader: bool,

    /// The last time a heartbeat was received.
    last_heartbeat: Option<Instant>,
    /// The duration until the next election timeout.
    next_election_timeout: Option<Instant>,

    msg_tx: Sender<Message<T>>,
    msg_rx: Receiver<Message<T>>,
    event_handler: Arc<dyn EventHandler<T>>,
    metrics_reporter: MetricsReporter<T>,

    task_wait_group: WaitGroup,
}

impl<T: ElectionType> ElectionCore<T> {
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn new(
        options: Options,
        node_id: T::NodeId,
        task_spawner: Arc<T::TaskSpawner>,
        storage: T::Storage,
        rpc: Arc<T::Rpc>,
        msg_tx: Sender<Message<T>>,
        msg_rx: Receiver<Message<T>>,
        event_handler: Arc<dyn EventHandler<T>>,
        metrics_reporter: MetricsReporter<T>,
    ) -> Self {
        ElectionCore {
            options,
            node_id: node_id.clone(),
            members: MemberConfig::new(node_id),
            state: State::Startup,
            prev_state: State::Startup,
            state_id: 0,
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
            event_handler,
            metrics_reporter,
            task_wait_group: WaitGroup::new(),
            in_moving_leader: false,
        }
    }

    #[inline]
    pub fn spawn(self) -> Result<T::Thread> {
        T::Thread::spawn("election-main".try_to_string()?, move || self.main())
    }

    fn main(mut self) {
        info!(
            "[Node({})][Term({})] start election main thread",
            self.node_id, self.hard_state.current_term
        );
        self.set_state(State::Startup, None);

        loop {
            match self.state {
                State::Shutdown => {
                    let _result = self.spawn_event_handling_task(Event::Shutdown);
                    self.task_wait_group.wait();
                    info!(
                        "[Node({})][Term({})] election has shutdown",
                        self.node_id, self.hard_state.current_term
                    );
                    return;
                }
                State::Startup => Startup::new(&mut self).run(),
                State::Follower => Follower::new(&mut self).run(),
                State::PreCandidate => Candidate::new(&mut self, true).run(),
                State::Candidate => Candidate::new(&mut self, false).run(),
                State::Leader => Leader::new(&mut self).run(),
                State::Observer => Observer::new(&mut self).run(),
            }
        }
    }

    #[inline]
    fn update_options(&mut self, options: Options) {
        self.options = options;
    }

    #[inline]
    fn state(&self) -> State {
        self.state
    }

    #[inline]
    fn prev_state(&self) -> State {
        self.prev_state
    }

    #[inline]
    fn is_state(&self, state: State) -> bool {
        self.state == state
    }

    #[inline]
    fn set_state(&mut self, state: State, set_prev_state: Option<&mut bool>) {
        if let Some(set_prev) = set_prev_state {
            if *set_prev {
                self.prev_state = self.state;
                *set_prev = false;
            }
        }
        self.state = state;
    }

    #[inline]
    fn increase_state_id(&mut self) {
        self.state_id += 1;
    }

    #[inline]
    fn state_id(&self) -> u64 {
        self.state_id
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
    fn check_node(&self, node_id: &T::NodeId) -> Result<()> {
        if self.node_id.ne(node_id) {
            return Err(Error::InvalidTarget(try_format!(
                "given node id({}) is not the same as this node({})",
                node_id,
                self.node_id
            )?));
        } else {
            Ok(())
        }
    }

    #[inline]
    fn update_current_term(&mut self, new_term: u64, voted_for: Option<T::NodeId>) -> Result<()> {
        if new_term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: new_term,
                voted_for,
            };
            self.storage
                .save_hard_state(&hard_state)
                .map_err(to_storage_error::<T>)?;
            self.hard_state = hard_state;
        }

        Ok(())
    }

    #[inline]
    fn save_hard_state(&mut self) -> Result<()> {
        self.storage
            .save_hard_state(&self.hard_state)
            .map_err(to_storage_error::<T>)
    }

    #[inline]
    fn spawn_task<F>(&self, name: &str, f: F) -> Result<()>
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        let wg = self.task_wait_group.clone();
        self.task_spawner.spawn(name.try_to_string()?, move || {
            f();
            drop(wg);
        })
    }

    fn handle_heartbeat(
        &mut self,
        msg: HeartbeatRequest<T>,
        set_prev_state: Option<&mut bool>,
    ) -> Result<HeartbeatResponse<T>> {
        self.check_node(&msg.target_node_id)?;

        if msg.term < self.hard_state.current_term {
            let current_term = self.hard_state.current_term;
            debug!(
                "[Node({})][Term({})] heartbeat term({}) from leader({}) is less than current term({})",
                self.node_id, current_term, msg.term, msg.leader_id, current_term
            );
            return Ok(HeartbeatResponse {
                node_id: self.node_id.clone(),
                term: current_term,
            });
        }

        self.update_next_election_timeout(true);
        let mut report_metrics = false;

        // update current term if needed
        if self.hard_state.current_term != msg.term {
            self.update_current_term(msg.term, None)?;
            report_metrics = true;
        }

        // update current leader if needed
        if self.current_leader.as_ref() != Some(&msg.leader_id) {
            match self.current_leader.as_ref() {
                None => {
                    info!(
                        "[Node({})][Term({})] change leader to {}",
                        self.node_id, self.hard_state.current_term, msg.leader_id
                    );
                }
                Some(old_leader) => {
                    info!(
                        "[Node({})][Term({})] change leader from {} to {}",
                        self.node_id, self.hard_state.current_term, old_leader, msg.leader_id
                    );
                }
            }
            self.current_leader = Some(msg.leader_id.clone());
            let _result = self.spawn_event_handling_task(Event::ChangeLeader(msg.leader_id));
            report_metrics = true;
        }

        // transition to follower state if needed
        if !self.is_state(State::Follower) && !self.is_state(State::Observer) {
            info!(
                "[Node({})][Term({})] received valid heartbeat in {:?} state, so transit to follower",
                self.node_id,
                self.hard_state.current_term,
                self.state()
            );
            self.set_state(State::Follower, set_prev_state);
            report_metrics = true;
        }

        if report_metrics {
            self.report_metrics();
        }

        Ok(HeartbeatResponse {
            node_id: self.node_id.clone(),
            term: self.hard_state.current_term,
        })
    }

    #[inline]
    fn create_vote_response(&self, req: VoteRequest<T>, vote_result: VoteResult) -> VoteResponse<T> {
        VoteResponse {
            node_id: self.node_id.clone(),
            candidate_id: req.candidate_id,
            vote_id: req.vote_id,
            term: self.hard_state.current_term,
            pre_vote: req.pre_vote,
            vote_result,
        }
    }

    fn handle_vote_request(
        &mut self,
        msg: VoteRequest<T>,
        mut set_prev_state: Option<&mut bool>,
    ) -> Result<VoteResponse<T>> {
        self.check_node(&msg.target_node_id)?;

        if msg.term < self.hard_state.current_term {
            debug!(
                "[Node({})][Term({})] vote term({}) from candidate({}) is less than current term({})",
                self.node_id, self.hard_state.current_term, msg.term, msg.candidate_id, self.hard_state.current_term
            );
            return Ok(self.create_vote_response(msg, VoteResult::NotGranted));
        }

        // Ignore heartbeat time when moving leader
        if !msg.move_leader {
            // Do not respond to the request if we've received a heartbeat within the election timeout minimum.
            if let Some(instant) = self.last_heartbeat {
                let now = Instant::now();
                let delta = now.duration_since(instant);
                if (delta.as_millis() as u64) <= self.options.election_timeout_min() {
                    debug!(
                        "[Node({})][Term({})] reject vote request received within election timeout minimum",
                        self.node_id, self.hard_state.current_term
                    );
                    return Ok(self.create_vote_response(msg, VoteResult::NotGranted));
                }
            }
        }

        // Per spec, if we observe a term greater than our own outside of the election timeout
        // minimum, then we must update term & immediately become follower. We still need to
        // do vote checking after this.
        if msg.term > self.hard_state.current_term {
            self.update_current_term(msg.term, None)?;
            if !self.is_state(State::Follower) && !self.is_state(State::Observer) {
                #[allow(clippy::needless_option_as_deref)]
                self.set_state(State::Follower, set_prev_state.as_deref_mut());
                self.update_next_election_timeout(false);
                info!(
                "[Node({})][Term({})] vote request term({}) is greater than current term({}), so transit to follower",
                self.node_id, self.hard_state.current_term, msg.term, self.hard_state.current_term);
            } else {
                info!(
                    "[Node({})][Term({})] vote request term({}) is greater than current term({})",
                    self.node_id, self.hard_state.current_term, msg.term, self.hard_state.current_term
                );
            }
            self.report_metrics();
        }

        // Check if candidate's vote factor can be granted.
        // If candidate's vote factor is not granted, then reject.
        let current_vote_factor = self.storage.load_vote_factor().map_err(to_storage_error::<T>)?;
        let vote_result = current_vote_factor.vote(&msg.factor);
        if !vote_result.is_granted() {
            debug!(
                "[Node({})][Term({})] reject vote request as candidate({})'s vote result is {:?}",
                self.node_id, self.hard_state.current_term, msg.candidate_id, vote_result
            );
            return Ok(self.create_vote_response(msg, vote_result));
        }

        // If the request is a PreVote, then at this point we can respond
        // to the candidate telling them that we would vote for them.
        if msg.pre_vote {
            debug!(
                "[Node({})][Term({})] voted for pre-candidate({})",
                self.node_id, self.hard_state.current_term, msg.candidate_id
            );
            return Ok(self.create_vote_response(msg, vote_result));
        }

        match &self.hard_state.voted_for {
            None => {
                // This node has not yet voted for the current term, so vote for the candidate.
                if !self.is_state(State::Follower) && !self.is_state(State::Observer) {
                    self.set_state(State::Follower, set_prev_state);
                    self.update_next_election_timeout(false);
                    debug!(
                        "[Node({})][Term({})] granted vote for candidate({}) and revert to follower",
                        self.node_id, self.hard_state.current_term, msg.candidate_id
                    );
                } else {
                    debug!(
                        "[Node({})][Term({})] granted vote for candidate({})",
                        self.node_id, self.hard_state.current_term, msg.candidate_id
                    );
                }
                self.hard_state.voted_for = Some(msg.candidate_id.clone());
                self.save_hard_state()?;
                self.report_metrics();
                Ok(self.create_vote_response(msg, vote_result))
            }
            Some(candidate_id) => {
                debug!(
                    "[Node({})][Term({})] reject vote request for candidate({}) because already voted for node({})",
                    self.node_id, self.hard_state.current_term, msg.candidate_id, candidate_id
                );
                Ok(self.create_vote_response(msg, VoteResult::NotGranted))
            }
        }
    }

    #[inline]
    fn spawn_event_handling_task(&self, event: Event<T>) -> Result<()> {
        let handler = self.event_handler.clone();
        let ev = event.clone();
        let tx = self.msg_tx.clone();
        let term = self.hard_state.current_term;
        let state_id = self.state_id();
        let result = self.spawn_task("election-event-handler", move || {
            let result = handler.handle_event(ev.clone());
            let error = result.err();
            let _ = tx.send(Message::EventHandlingResult {
                event: ev,
                error,
                term,
                state_id,
            });
        });
        if let Err(ref e) = result {
            error!(
                "[Node({})][Term({})] failed to spawn task to for event ({:?}): {}",
                self.node_id, self.hard_state.current_term, event, e
            );
        }
        result
    }

    #[inline]
    fn reject_init_with_members(&self, tx: Sender<Result<()>>) {
        let _ = tx.send(Err(try_format_error!(
            NotAllowed,
            "can't init with members in {:?} state",
            self.state(),
        )));
    }

    #[inline]
    fn reject_move_leader(&self, tx: Sender<Result<()>>) {
        let _ = tx.send(Err(try_format_error!(
            NotAllowed,
            "can't move leader in {:?} state",
            self.state(),
        )));
    }

    #[inline]
    fn report_metrics(&mut self) {
        self.metrics_reporter.report(Metrics {
            state: self.state(),
            current_term: self.hard_state.current_term,
            current_leader: self.current_leader.clone(),
        })
    }
}
