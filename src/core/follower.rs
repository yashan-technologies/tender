use crate::core::{RaftCore, State};
use crate::msg::Message;
use crate::{Event, RaftType};
use crossbeam_channel::RecvTimeoutError;

pub struct Follower<'a, T: RaftType> {
    core: &'a mut RaftCore<T>,
}

impl<'a, T: RaftType> Follower<'a, T> {
    #[inline]
    pub fn new(core: &'a mut RaftCore<T>) -> Self {
        Self { core }
    }

    pub fn run(mut self) {
        assert_eq!(self.core.state, State::Follower);
        self.core.next_election_timeout = None;
        self.core.notify_event(Event::TransitToFollower {
            term: self.core.hard_state.current_term,
        });
        self.core.report_metrics();

        info!("[Node({})] start the follower loop", self.core.node_id);

        loop {
            if self.core.state != State::Follower {
                return;
            }

            let election_timeout = self.core.next_election_timeout();

            match self.core.msg_rx.recv_deadline(election_timeout) {
                Ok(msg) => match msg {
                    Message::Heartbeat { req, tx } => {
                        trace!("[Node({})] received heartbeat: {:?}", self.core.node_id, req);

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
                        let result = self.core.handle_vote_request(req);
                        if let Err(ref e) = result {
                            debug!("[Node({})] failed to handle vote request: {}", self.core.node_id, e);
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
                        self.core.state = State::PreCandidate;
                        self.core.current_leader = None;
                        info!(
                            "[Node({})] an election timeout is hit, need to transit to pre-candidate",
                            self.core.node_id
                        );
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
