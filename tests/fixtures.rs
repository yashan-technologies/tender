#![allow(dead_code)]

use log::LevelFilter;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};
use tender::{
    Error, Event, EventListener, HardState, HeartbeatRequest, HeartbeatResponse, Metrics, Options, Raft, RaftType,
    Result, Rpc, State, Storage, TaskSpawner, VoteFactor, VoteRequest, VoteResponse,
};

pub type MemRaft = Raft<MemRaftType>;
pub type GroupId = u32;
pub type NodeId = u32;

pub fn init_log() {
    let _ = env_logger::builder()
        .filter_level(LevelFilter::Trace)
        .is_test(true)
        .try_init();
}

#[derive(Clone, Debug, Default)]
pub struct MemRaftType;

impl RaftType for MemRaftType {
    type GroupId = GroupId;
    type NodeId = NodeId;
    type VoteFactor = MemVoteFactor<Self>;
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
    fn vote(&self, other: &Self) -> bool {
        other.priority >= self.priority
    }
}

pub struct ThreadSpawner;

impl TaskSpawner for ThreadSpawner {
    fn spawn<F, T>(&self, name: Option<String>, f: F) -> Result<()>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let mut builder = std::thread::Builder::new();
        if let Some(s) = name {
            builder = builder.name(s);
        }
        let _ = builder
            .spawn(f)
            .map_err(|e| Error::TaskError(format!("failed to spawn thread: {}", e)))?;
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
        Ok(self.hard_state.lock().unwrap().clone())
    }

    fn save_hard_state(&self, hard_state: &HardState<T>) -> std::result::Result<(), Self::Err> {
        let mut s = self.hard_state.lock().unwrap();
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
    routing_table: RwLock<HashMap<NodeId, Raft<MemRaftType>>>,
}

impl MemRouter {
    pub fn new(group_id: GroupId) -> Self {
        MemRouter {
            group_id,
            routing_table: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_node(self: &Arc<Self>, node_id: NodeId, vote_factor: MemVoteFactor<MemRaftType>) {
        {
            let rt = self.routing_table.read().unwrap();
            assert!(!rt.contains_key(&node_id), "node {} is already existing", node_id);
        }

        let options = Options::builder()
            .election_timeout_min(1500)
            .election_timeout_max(1600)
            .heartbeat_interval(500)
            .build()
            .unwrap();
        let task_spawner = Arc::new(ThreadSpawner);
        let mem_store = MemStore::new(HardState::default(), vote_factor);
        let event_listener = Arc::new(LoggingEventListener::new(node_id)) as Arc<dyn EventListener<MemRaftType>>;
        let raft = MemRaft::start(
            options,
            self.group_id,
            node_id,
            task_spawner,
            mem_store,
            self.clone(),
            vec![event_listener],
        )
        .unwrap();

        let mut rt = self.routing_table.write().unwrap();
        rt.insert(node_id, raft);
    }

    pub fn init_node(&self, node_id: NodeId, members: HashSet<NodeId>) -> Result<()> {
        let rt = self.routing_table.read().unwrap();
        rt.get(&node_id).unwrap().initialize(members)
    }

    pub fn remove_node(&self, node_id: NodeId) -> Option<MemRaft> {
        self.routing_table.write().unwrap().remove(&node_id)
    }

    pub fn metrics(&self, node_id: NodeId) -> Metrics<MemRaftType> {
        let mut metrics_watcher = {
            let rt = self.routing_table.read().unwrap();
            rt.get(&node_id).unwrap().metrics_watcher()
        };
        metrics_watcher.metrics()
    }

    pub fn assert_node_state(&self, node_id: NodeId, state: State, current_term: u64, current_leader: Option<NodeId>) {
        let mut metrics_watcher = {
            let rt = self.routing_table.read().unwrap();
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
        let rt = self.routing_table.read().unwrap();
        let node = match rt.get(&msg.target_node_id) {
            None => {
                return Err(RpcError(format!(
                    "target node({}) not found in routing table",
                    msg.target_node_id
                )))
            }
            Some(n) => n,
        };

        let resp = node.heartbeat(msg).map_err(|e| RpcError(e.to_string()))?;
        Ok(resp)
    }

    fn vote(&self, msg: VoteRequest<MemRaftType>) -> std::result::Result<VoteResponse<MemRaftType>, RpcError> {
        let rt = self.routing_table.read().unwrap();
        let node = match rt.get(&msg.target_node_id) {
            None => {
                return Err(RpcError(format!(
                    "target node({}) not found in routing table",
                    msg.target_node_id
                )))
            }
            Some(n) => n,
        };

        let resp = node.vote(msg).map_err(|e| RpcError(e.to_string()))?;
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

impl<T: RaftType> EventListener<T> for LoggingEventListener<T>
where
    T::NodeId: Sync,
{
    fn event_performed(&self, event: Event<T>) {
        log::info!("[Node({})] an event has happened: {:?}", self.node_id, event);
    }
}
