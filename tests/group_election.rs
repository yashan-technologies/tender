mod fixtures;

use fixtures::{init_log, MemRouter, MemVoteFactor, NodeId};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tender::State;

/// Raft group initialization test.
///
/// What does this test do?
///
/// - brings 3 nodes online with only knowledge of themselves.
/// - asserts that they remain in startup state with no activity (they should be completely passive).
/// - initializes the group with membership config including all nodes.
/// - asserts that the group was able to come online, elect a leader and maintain a stable state.
#[test]
fn test_election() {
    init_log();

    let group_id = 1000;
    let node1 = NodeId::new(group_id, 1001);
    let node2 = NodeId::new(group_id, 1002);
    let node3 = NodeId::new(group_id, 1003);

    let mem_router = Arc::new(MemRouter::new(group_id));
    mem_router.new_node(node1, MemVoteFactor::new(1));
    mem_router.new_node(node2, MemVoteFactor::new(1));
    mem_router.new_node(node3, MemVoteFactor::new(0));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone(), true).unwrap();
    mem_router.init_node(node2, members.clone(), false).unwrap();
    mem_router.init_node(node3, members, false).unwrap();
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::Leader, 1, Some(node1));
    mem_router.assert_node_state(node2, State::Follower, 1, Some(node1));
    mem_router.assert_node_state(node3, State::Follower, 1, Some(node1));

    // remove node 1 to trigger a new election
    {
        log::info!("--- remove node {}", node1);
        let _ = mem_router.remove_node(node1);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node2, State::Leader, 2, Some(node2));
    mem_router.assert_node_state(node3, State::Follower, 2, Some(node2));

    // remove node 2
    {
        log::info!("--- remove node {}", node2);
        let _ = mem_router.remove_node(node2);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node3, State::PreCandidate, 2, None);
}
