use crate::core::{MemberConfig, RaftCore, State};
use crate::error::Result;
use crate::msg::Message;
use crate::{Event, RaftType, Storage};
use crossbeam_channel::RecvTimeoutError;
use std::collections::HashSet;

pub struct Startup<'a, T: RaftType> {
    core: &'a mut RaftCore<T>,
}

impl<'a, T: RaftType> Startup<'a, T> {
    #[inline]
    pub fn new(core: &'a mut RaftCore<T>) -> Self {
        Self { core }
    }

    #[inline]
    fn init_with_members(&mut self, mut members: HashSet<T::NodeId>) -> Result<()> {
        if !members.contains(&self.core.node_id) {
            members.insert(self.core.node_id.clone());
        }

        self.core.members = MemberConfig {
            members,
            members_after_consensus: None,
        };

        if self.core.members.members.len() == 1 {
            self.core.hard_state.current_term += 1;
            self.core.hard_state.voted_for = Some(self.core.node_id.clone());
            self.core.state = State::Leader;
            info!(
                "[Node({})] raft is initialized without other members, so directly transit to leader",
                self.core.node_id
            );
            self.core.save_hard_state()?;
        } else {
            self.core.state = State::PreCandidate;
            info!(
                "[Node({})] raft is initialized with {} members, so transit to pre-candidate",
                self.core.node_id,
                self.core.members.members.len()
            );
        }
        self.core.report_metrics();

        Ok(())
    }

    pub fn run(mut self) -> Result<()> {
        assert_eq!(self.core.state, State::Startup);
        self.core.notify_event(Event::Startup);

        let state = self
            .core
            .storage
            .load_hard_state()
            .map_err(|e| self.core.map_fatal_storage_error(e))?;
        self.core.set_hard_state(state);
        self.core.report_metrics();

        info!("[Node({})] start the startup loop", self.core.node_id);

        loop {
            if self.core.state != State::Startup {
                return Ok(());
            }

            let election_timeout = self.core.next_election_timeout();

            match self.core.msg_rx.recv_deadline(election_timeout) {
                Ok(msg) => {
                    if let Message::Initialize { members, tx } = msg {
                        let _ = tx.send(self.init_with_members(members));
                    } else {
                        // ignore other messages in startup state
                    }
                }
                Err(e) => match e {
                    RecvTimeoutError::Timeout => {
                        self.core.next_election_timeout = None;
                        continue;
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
