use crate::ElectionType;
use fxhash::FxHashSet;

pub struct MemberConfig<T: ElectionType> {
    current: T::NodeId,
    peers: FxHashSet<T::NodeId>,
}

impl<T: ElectionType> MemberConfig<T> {
    #[inline]
    pub fn new(current: T::NodeId) -> Self {
        Self {
            current,
            peers: FxHashSet::with_hasher(Default::default()),
        }
    }

    #[inline]
    pub fn init_peers(&mut self, peers: FxHashSet<T::NodeId>) {
        debug_assert!(!peers.contains(&self.current));
        self.peers = peers;
    }

    #[allow(dead_code)]
    #[inline]
    pub fn current(&self) -> &T::NodeId {
        &self.current
    }

    #[inline]
    pub fn peers(&self) -> &FxHashSet<T::NodeId> {
        &self.peers
    }

    #[inline]
    pub fn all_members_num(&self) -> usize {
        self.peers.len() + 1
    }

    #[inline]
    pub fn contains(&self, node_id: &T::NodeId) -> bool {
        self.peers.contains(node_id) || self.current.eq(node_id)
    }
}
