mod fixtures;

use fixtures::{init_log, MemRouter, MemVoteFactor, NodeId};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tender::{InitialMode, Quorum, State};

/// Group election test.
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

    mem_router.init_node(node1, members.clone(), InitialMode::AsLeader);
    mem_router.init_node(node2, members.clone(), InitialMode::Normal);
    mem_router.init_node(node3, members, InitialMode::Normal);
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

/// Group election test: quorum.
#[test]
fn test_election_with_quorum() {
    init_log();

    let group_id = 1000;
    let node1 = NodeId::new(group_id, 1001);
    let node2 = NodeId::new(group_id, 1002);
    let node3 = NodeId::new(group_id, 1003);

    let mem_router = Arc::new(MemRouter::with_quorum(group_id, Quorum::Any(3)));
    mem_router.new_node(node1, MemVoteFactor::new(1));
    mem_router.new_node(node2, MemVoteFactor::new(1));
    mem_router.new_node(node3, MemVoteFactor::new(0));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone(), InitialMode::AsLeader);
    mem_router.init_node(node2, members.clone(), InitialMode::Normal);
    mem_router.init_node(node3, members, InitialMode::Normal);
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
    // Quorum is 3, so no leader will be elected.
    mem_router.assert_node_state(node2, State::PreCandidate, 1, None);
    mem_router.assert_node_state(node3, State::PreCandidate, 1, None);

    mem_router.update_quorum(Quorum::Major);
    sleep(Duration::from_secs(3));
    // Quorum is Major, so node2 will be elected as leader.
    mem_router.assert_node_state(node2, State::Leader, 2, Some(node2));
    mem_router.assert_node_state(node3, State::Follower, 2, Some(node2));
}

/// Group election test: observer.
#[test]
fn test_election_with_observer() {
    init_log();

    let group_id = 1000;
    let node1 = NodeId::new(group_id, 1001);
    let node2 = NodeId::new(group_id, 1002);
    let node3 = NodeId::new(group_id, 1003);

    let mem_router = Arc::new(MemRouter::new(group_id));
    mem_router.new_node(node1, MemVoteFactor::new(1));
    mem_router.new_node(node2, MemVoteFactor::new(1));
    mem_router.new_node(node3, MemVoteFactor::new(1));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone(), InitialMode::AsLeader);
    mem_router.init_node(node2, members.clone(), InitialMode::Normal);
    mem_router.init_node(node3, members, InitialMode::AsObserver);
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::Leader, 1, Some(node1));
    mem_router.assert_node_state(node2, State::Follower, 1, Some(node1));
    mem_router.assert_node_state(node3, State::Observer, 1, Some(node1));

    // remove node 1 to trigger a new election
    {
        log::info!("--- remove node {}", node1);
        let _ = mem_router.remove_node(node1);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node2, State::Leader, 2, Some(node2));
    mem_router.assert_node_state(node3, State::Observer, 2, Some(node2));

    // remove node 2
    {
        log::info!("--- remove node {}", node2);
        let _ = mem_router.remove_node(node2);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node3, State::Observer, 2, Some(node2));
}

/// Group election test: initial candidate.
#[test]
fn test_election_with_initial_candidate() {
    init_log();

    let group_id = 1000;
    let node1 = NodeId::new(group_id, 1001);
    let node2 = NodeId::new(group_id, 1002);
    let node3 = NodeId::new(group_id, 1003);

    let mem_router = Arc::new(MemRouter::new(group_id));
    mem_router.new_node(node1, MemVoteFactor::new(1));
    mem_router.new_node(node2, MemVoteFactor::new(1));
    mem_router.new_node(node3, MemVoteFactor::new(1));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node1, State::Startup, 0, None);
    mem_router.assert_node_state(node2, State::Startup, 0, None);
    mem_router.assert_node_state(node3, State::Startup, 0, None);

    let members: HashSet<_> = [node1, node2, node3].into_iter().collect();

    mem_router.init_node(node1, members.clone(), InitialMode::Normal);
    mem_router.init_node(node2, members.clone(), InitialMode::Normal);
    mem_router.init_node(node3, members, InitialMode::AsCandidate);
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::Follower, 1, Some(node3));
    mem_router.assert_node_state(node2, State::Follower, 1, Some(node3));
    mem_router.assert_node_state(node3, State::Leader, 1, Some(node3));
}

/// Move leader test.
#[test]
fn test_move_leader() {
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

    mem_router.init_node(node1, members.clone(), InitialMode::AsLeader);
    mem_router.init_node(node2, members.clone(), InitialMode::Normal);
    mem_router.init_node(node3, members, InitialMode::Normal);

    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::Leader, 1, Some(node1));
    mem_router.assert_node_state(node2, State::Follower, 1, Some(node1));
    mem_router.assert_node_state(node3, State::Follower, 1, Some(node1));

    {
        log::info!("--- move leader to node {}", node2);
        let _ = mem_router.move_leader(node1, node2);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::Follower, 2, Some(node2));
    mem_router.assert_node_state(node2, State::Leader, 2, Some(node2));
    mem_router.assert_node_state(node3, State::Follower, 2, Some(node2));

    {
        log::info!("--- move leader to node {}", node3);
        let _ = mem_router.move_leader(node2, node3);
    }
    sleep(Duration::from_secs(3));
    mem_router.assert_node_state(node1, State::Follower, 3, Some(node3));
    mem_router.assert_node_state(node2, State::Follower, 3, Some(node3));
    mem_router.assert_node_state(node3, State::Leader, 3, Some(node3));
}
