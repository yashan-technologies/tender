use crate::{RaftType, State};
use watch::{WatchReceiver, WatchSender};

/// A set of metrics describing the current state of the raft node.
#[derive(Clone)]
pub struct Metrics<T: RaftType> {
    /// The state of the raft node.
    pub state: State,
    /// Current term of the raft node.
    pub current_term: u64,
    /// Current leader of the raft group.
    pub current_leader: Option<T::NodeId>,
}

impl<T: RaftType> Metrics<T> {
    #[inline]
    pub(crate) fn new() -> Self {
        Metrics {
            state: State::Startup,
            current_term: 0,
            current_leader: None,
        }
    }
}

pub(crate) struct MetricsReporter<T: RaftType> {
    metrics_tx: WatchSender<Metrics<T>>,
}

impl<T: RaftType> MetricsReporter<T> {
    #[inline]
    pub(crate) fn new(metrics_tx: WatchSender<Metrics<T>>) -> Self {
        MetricsReporter { metrics_tx }
    }

    #[inline]
    pub(crate) fn report(&mut self, metrics: Metrics<T>) {
        self.metrics_tx.send(metrics);
    }
}

/// The metrics watcher of the raft node.
#[derive(Clone)]
pub struct MetricsWatcher<T: RaftType> {
    metrics_rx: WatchReceiver<Metrics<T>>,
}

impl<T: RaftType> MetricsWatcher<T> {
    #[inline]
    pub(crate) fn new(metrics_rx: WatchReceiver<Metrics<T>>) -> Self {
        MetricsWatcher { metrics_rx }
    }

    /// A set of metrics describing the current state of the raft node.
    #[inline]
    pub fn metrics(&mut self) -> Metrics<T> {
        self.metrics_rx.get()
    }
}

#[inline]
pub(crate) fn metrics_channel<T: RaftType>() -> (MetricsReporter<T>, MetricsWatcher<T>) {
    let (tx, rx) = watch::channel(Metrics::new());
    (MetricsReporter::new(tx), MetricsWatcher::new(rx))
}
