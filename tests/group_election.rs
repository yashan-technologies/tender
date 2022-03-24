mod fixtures;

use fixtures::{init_log, MemRouter, MemVoteFactor, NodeId};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tender::State;

/// Raft group basic election test.
///
/// What does this test do?
///
/// - brings 3 nodes online with only knowledge of themselves.
/// - asserts that they remain in startup state with no activity (they should be completely passive).
/// - initializes the group with membership config including all nodes.
/// - asserts that the group was able to come online, elect a leader and maintain a stable state.
/// - remove the leader node, asserts that the remain 2 nodes will elect a leader again.
/// - remove the leader node again, asserts that the last node will keep in PreCandidate state.
#[test]
fn test_basic_election() {
    init_log();

    let group_id = 1000;
    let node1 = NodeId::new(group_id, 1001);
    let node2 = NodeId::new(group_id, 1002);
    let node3 = NodeId::new(group_id, 1003);

    let mem_router = Arc::new(MemRouter::new(group_id));
    mem_router.new_node(node1, MemVoteFactor::new(0));
    mem_router.new_node(node2, MemVoteFactor::new(0));
    mem_router.new_node(node3, MemVoteFactor::new(0));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone(), false).unwrap();
    mem_router.init_node(node2, members.clone(), false).unwrap();
    mem_router.init_node(node3, members, false).unwrap();
    sleep(Duration::from_secs(3));
    let leader = mem_router.metrics(node1).current_leader.unwrap();
    let state = |leader, node| if node == leader { State::Leader } else { State::Follower };
    mem_router.assert_node_state(node1, state(leader, node1), 1, Some(leader));
    mem_router.assert_node_state(node2, state(leader, node2), 1, Some(leader));
    mem_router.assert_node_state(node3, state(leader, node3), 1, Some(leader));

    // remove leader to trigger a new election
    {
        log::info!("--- remove node {}", leader);
        let _ = mem_router.remove_node(leader);
    }
    sleep(Duration::from_secs(3));
    let (rem_node1, rem_node2) = if node1 == leader {
        (node2, node3)
    } else if node2 == leader {
        (node1, node3)
    } else {
        (node1, node2)
    };
    let rem_leader = mem_router.metrics(rem_node1).current_leader.unwrap();
    let rem_state = |leader, node| if node == leader { State::Leader } else { State::Follower };
    mem_router.assert_node_state(rem_node1, rem_state(rem_leader, rem_node1), 2, Some(rem_leader));
    mem_router.assert_node_state(rem_node2, rem_state(rem_leader, rem_node2), 2, Some(rem_leader));

    // remove rem_leader
    {
        log::info!("--- remove node {}", rem_leader);
        let _ = mem_router.remove_node(rem_leader);
    }
    sleep(Duration::from_secs(3));
    let rem_node = if rem_node1 == rem_leader { rem_node2 } else { rem_node1 };
    mem_router.assert_node_state(rem_node, State::PreCandidate, 2, None);
}

/// Raft group priority election test.
///
/// What does this test do?
///
/// - brings 3 nodes online with only knowledge of themselves and different priorities (should enable veto).
/// - asserts that they remain in startup state with no activity (they should be completely passive).
/// - initializes the group with membership config including all nodes.
/// - asserts that the group was able to come online, elect a leader which has the highest priority and maintain a stable state.
/// - remove the leader node, asserts that the remain 2 nodes will elect a leader which has the highest priority again.
/// - remove the leader node again, asserts that the last node will keep in PreCandidate state.
#[test]
fn test_priority_election() {
    init_log();

    let group_id = 1000;
    let node1 = NodeId::new(group_id, 1001);
    let node2 = NodeId::new(group_id, 1002);
    let node3 = NodeId::new(group_id, 1003);

    let mem_router = Arc::new(MemRouter::new(group_id));
    mem_router.enable_veto();
    mem_router.new_node(node1, MemVoteFactor::new(1));
    mem_router.new_node(node2, MemVoteFactor::new(2));
    mem_router.new_node(node3, MemVoteFactor::new(3));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone(), false).unwrap();
    mem_router.init_node(node2, members.clone(), false).unwrap();
    mem_router.init_node(node3, members, false).unwrap();
    sleep(Duration::from_secs(3));
    let leader = mem_router.metrics(node1).current_leader.unwrap();
    assert_eq!(leader, node3);
    mem_router.assert_node_state(node1, State::Follower, 1, Some(node3));
    mem_router.assert_node_state(node2, State::Follower, 1, Some(node3));
    mem_router.assert_node_state(node3, State::Leader, 1, Some(node3));

    // remove leader to trigger a new election
    {
        log::info!("--- remove node {}", node3);
        let _ = mem_router.remove_node(node3);
    }
    sleep(Duration::from_secs(3));
    let rem_leader = mem_router.metrics(node1).current_leader.unwrap();
    assert_eq!(rem_leader, node2);
    mem_router.assert_node_state(node1, State::Follower, 2, Some(node2));
    mem_router.assert_node_state(node2, State::Leader, 2, Some(node2));

    // remove rem_leader
    {
        log::info!("--- remove node {}", node2);
        let _ = mem_router.remove_node(node2);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::PreCandidate, 2, None);
}
