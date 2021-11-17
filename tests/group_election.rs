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
    let state = |node: NodeId, leader: NodeId| if node == leader { State::Leader } else { State::Follower };

    init_log();

    let (node1, node2, node3) = (1001, 1002, 1003);

    let mem_router = Arc::new(MemRouter::new(1000));
    mem_router.new_node(node1, MemVoteFactor::new(1));
    mem_router.new_node(node2, MemVoteFactor::new(1));
    mem_router.new_node(node3, MemVoteFactor::new(0));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone()).unwrap();
    mem_router.init_node(node2, members.clone()).unwrap();
    mem_router.init_node(node3, members).unwrap();
    sleep(Duration::from_secs(3));
    let leader_id = mem_router.metrics(node1).current_leader.expect("no leader is elected");
    mem_router.assert_node_state(node1, state(node1, leader_id), 1, Some(leader_id));
    mem_router.assert_node_state(node2, state(node2, leader_id), 1, Some(leader_id));
    mem_router.assert_node_state(node3, state(node3, leader_id), 1, Some(leader_id));

    // remove node 1 to trigger a new election
    {
        log::info!("--- remove node {}", node1);
        let _ = mem_router.remove_node(node1);
    }
    sleep(Duration::from_secs(3));
    let term = if leader_id == node2 { 1 } else { 2 };
    mem_router.assert_node_state(node2, State::Leader, term, Some(node2));
    mem_router.assert_node_state(node3, State::Follower, term, Some(node2));

    // remove node 2
    {
        log::info!("--- remove node {}", node2);
        let _ = mem_router.remove_node(node2);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node3, State::PreCandidate, term, None);
}
