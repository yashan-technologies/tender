#![allow(dead_code)]

use log::LevelFilter;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use tender::{
    Error, Event, EventHandler, HardState, HeartbeatRequest, HeartbeatResponse, Metrics, NodeId as RaftNodeId, Options,
    Quorum, Raft, RaftType, Result, Rpc, State, Storage, TaskSpawner, Thread, VoteFactor, VoteRequest, VoteResponse,
    VoteResult,
};

pub type MemRaft = Raft<MemRaftType>;
pub type GroupId = u32;
pub type GroupNodeId = u32;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub struct NodeId {
    group_id: GroupId,
    group_node_id: u32,
}

impl NodeId {
    pub fn new(group_id: GroupId, group_node_id: GroupNodeId) -> Self {
        NodeId {
            group_id,
            group_node_id,
        }
    }
}

impl RaftNodeId for NodeId {
    type GroupId = GroupId;

    fn group_id(&self) -> Self::GroupId {
        self.group_id
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.group_id, self.group_node_id)
    }
}

pub fn init_log() {
    let _ = env_logger::builder()
        .filter_level(LevelFilter::Trace)
        .is_test(true)
        .try_init();
}

#[derive(Clone, Debug, Default)]
pub struct MemRaftType;

impl RaftType for MemRaftType {
    type NodeId = NodeId;
    type VoteFactor = MemVoteFactor<Self>;
    type Thread = RaftThread;
    type TaskSpawner = ThreadSpawner;
    type Storage = MemStore<Self>;
    type Rpc = MemRouter;
}

#[derive(Debug, Clone)]
pub struct MemVoteFactor<T: RaftType> {
    pub priority: i32,
    _marker: PhantomData<T>,
}

impl<T: RaftType> MemVoteFactor<T> {
    pub fn new(priority: i32) -> Self {
        MemVoteFactor {
            priority,
            _marker: PhantomData,
        }
    }
}

impl<T: RaftType> VoteFactor<T> for MemVoteFactor<T> {
    fn vote(&self, other: &Self) -> VoteResult {
        if other.priority >= self.priority {
            VoteResult::Granted
        } else {
            VoteResult::NotGranted
        }
    }
}

pub struct RaftThread(std::thread::JoinHandle<()>);

impl Thread for RaftThread {
    fn spawn<F>(name: String, f: F) -> Result<Self>
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        let mut builder = std::thread::Builder::new();
        builder = builder.name(name);
        let t = builder
            .spawn(f)
            .map_err(|e| Error::ThreadError(format!("failed to spawn thread: {}", e)))?;
        Ok(RaftThread(t))
    }

    fn join(self) {
        self.0.join().expect("failed to join thread")
    }
}

pub struct ThreadSpawner;

impl TaskSpawner for ThreadSpawner {
    fn spawn<F>(&self, name: String, f: F) -> Result<()>
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        let mut builder = std::thread::Builder::new();
        builder = builder.name(name);
        let _ = builder
            .spawn(f)
            .map_err(|e| Error::TaskError(format!("failed to spawn task: {}", e)))?;
        Ok(())
    }
}

pub struct MemStore<T: RaftType> {
    hard_state: Mutex<HardState<T>>,
    vote_factor: T::VoteFactor,
}

impl<T: RaftType> MemStore<T> {
    pub fn new(hard_state: HardState<T>, vote_factor: T::VoteFactor) -> Self {
        MemStore {
            hard_state: Mutex::new(hard_state),
            vote_factor,
        }
    }
}

impl<T: RaftType> Storage<T> for MemStore<T> {
    type Err = std::convert::Infallible;

    fn load_hard_state(&self) -> std::result::Result<HardState<T>, Self::Err> {
        Ok(self.hard_state.lock().clone())
    }

    fn save_hard_state(&self, hard_state: &HardState<T>) -> std::result::Result<(), Self::Err> {
        let mut s = self.hard_state.lock();
        *s = hard_state.clone();
        Ok(())
    }

    fn load_vote_factor(&self) -> std::result::Result<T::VoteFactor, Self::Err> {
        Ok(self.vote_factor.clone())
    }
}

#[derive(Default)]
pub struct MemRouter {
    group_id: GroupId,
    quorum: RwLock<Quorum>,
    routing_table: RwLock<HashMap<NodeId, Raft<MemRaftType>>>,
}

impl MemRouter {
    pub fn new(group_id: GroupId) -> Self {
        MemRouter {
            group_id,
            quorum: RwLock::new(Quorum::Major),
            routing_table: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_quorum(group_id: GroupId, quorum: Quorum) -> Self {
        MemRouter {
            group_id,
            quorum: RwLock::new(quorum),
            routing_table: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_node(self: &Arc<Self>, node_id: NodeId, vote_factor: MemVoteFactor<MemRaftType>) {
        assert_eq!(self.group_id, node_id.group_id);
        {
            let rt = self.routing_table.read();
            assert!(!rt.contains_key(&node_id), "node({}) is already existing", node_id);
        }

        let options = Options::builder()
            .election_timeout_min(1500)
            .election_timeout_max(1600)
            .heartbeat_interval(500)
            .quorum(*self.quorum.read())
            .build()
            .unwrap();
        let task_spawner = Arc::new(ThreadSpawner);
        let mem_store = MemStore::new(HardState::default(), vote_factor);
        let event_listener = Arc::new(LoggingEventListener::new(node_id)) as Arc<dyn EventHandler<MemRaftType>>;
        let raft = MemRaft::start(options, node_id, task_spawner, mem_store, self.clone(), event_listener).unwrap();

        let mut rt = self.routing_table.write();
        rt.insert(node_id, raft);
    }

    pub fn init_node(&self, node_id: NodeId, members: HashSet<NodeId>, force_leader: bool) -> Result<()> {
        assert_eq!(self.group_id, node_id.group_id);
        let rt = self.routing_table.read();
        rt.get(&node_id).unwrap().initialize(members, force_leader)
    }

    pub fn remove_node(&self, node_id: NodeId) -> Option<MemRaft> {
        assert_eq!(self.group_id, node_id.group_id);
        self.routing_table.write().remove(&node_id)
    }

    pub fn update_quorum(&self, quorum: Quorum) {
        let options = Options::builder()
            .election_timeout_min(1500)
            .election_timeout_max(1600)
            .heartbeat_interval(500)
            .quorum(quorum)
            .build()
            .unwrap();
        *self.quorum.write() = quorum;
        let rt = self.routing_table.write();
        for (_node_id, raft) in rt.iter() {
            raft.update_options(options.clone()).unwrap();
        }
    }

    pub fn update_node_options(&self, node_id: NodeId, options: Options) {
        assert_eq!(self.group_id, node_id.group_id);
        let rt = self.routing_table.write();
        rt.get(&node_id).unwrap().update_options(options).unwrap();
    }

    pub fn metrics(&self, node_id: NodeId) -> Metrics<MemRaftType> {
        let mut metrics_watcher = {
            let rt = self.routing_table.read();
            rt.get(&node_id).unwrap().metrics_watcher()
        };
        metrics_watcher.metrics()
    }

    pub fn assert_node_state(&self, node_id: NodeId, state: State, current_term: u64, current_leader: Option<NodeId>) {
        assert_eq!(self.group_id, node_id.group_id);

        let mut metrics_watcher = {
            let rt = self.routing_table.read();
            rt.get(&node_id).unwrap().metrics_watcher()
        };
        let metrics = metrics_watcher.metrics();
        assert_eq!(metrics.state, state);
        assert_eq!(metrics.current_term, current_term);
        assert_eq!(metrics.current_leader, current_leader);
    }
}

#[derive(Debug)]
pub struct RpcError(String);

impl Display for RpcError {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC error: {}", self.0)
    }
}

impl std::error::Error for RpcError {}

impl Rpc<MemRaftType> for MemRouter {
    type Err = RpcError;

    fn heartbeat(
        &self,
        msg: HeartbeatRequest<MemRaftType>,
    ) -> std::result::Result<HeartbeatResponse<MemRaftType>, RpcError> {
        let rt = self.routing_table.read();
        let node = match rt.get(&msg.target_node_id) {
            None => {
                return Err(RpcError(format!(
                    "target node({}) not found in routing table",
                    msg.target_node_id
                )))
            }
            Some(n) => n,
        };

        let resp = node.submit_heartbeat(msg).map_err(|e| RpcError(e.to_string()))?;
        Ok(resp)
    }

    fn vote(&self, msg: VoteRequest<MemRaftType>) -> std::result::Result<VoteResponse<MemRaftType>, RpcError> {
        let rt = self.routing_table.read();
        let node = match rt.get(&msg.target_node_id) {
            None => {
                return Err(RpcError(format!(
                    "target node({}) not found in routing table",
                    msg.target_node_id
                )))
            }
            Some(n) => n,
        };

        let resp = node.submit_vote(msg).map_err(|e| RpcError(e.to_string()))?;
        Ok(resp)
    }
}

pub struct LoggingEventListener<T: RaftType> {
    node_id: T::NodeId,
}

impl<T: RaftType> LoggingEventListener<T> {
    pub fn new(node_id: T::NodeId) -> Self {
        Self { node_id }
    }
}

impl<T: RaftType> EventHandler<T> for LoggingEventListener<T>
where
    T::NodeId: Sync,
{
    fn handle_event(&self, event: Event<T>) -> Result<()> {
        log::info!("[Node({})] an event has happened: {:?}", self.node_id, event);
        Ok(())
    }
}
