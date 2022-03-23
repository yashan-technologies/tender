mod fixtures;

use fixtures::{init_log, MemRouter, MemVoteFactor, NodeId};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tender::{Options, State};

/// Single-node initialization test.
///
/// What does this test do?
///
/// - brings 1 node online with only knowledge of itself.
/// - asserts that it remains in startup state with no activity (it should be completely passive).
/// - initializes the group with membership config including just the one node.
/// - asserts that the group was able to come online, and that the one node became leader.
#[test]
fn test_single_node() {
    init_log();

    let mem_router = Arc::new(MemRouter::new(1));
    let node = NodeId::new(1, 1);
    mem_router.new_node(node, MemVoteFactor::new(0));

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node, State::Startup, 0, None);

    let options = Options::builder()
        .election_timeout_min(1000)
        .election_timeout_max(1100)
        .heartbeat_interval(300)
        .build()
        .unwrap();
    mem_router.update_options(node, options);

    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node, State::Startup, 0, None);

    mem_router.init_node(node, HashSet::new(), false).unwrap();
    sleep(Duration::from_secs(1));
    mem_router.assert_node_state(node, State::Leader, 1, Some(node));
}
