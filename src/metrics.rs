use crate::{ElectionType, State};
use watch::{WatchReceiver, WatchSender};

/// A set of metrics describing the current state of election.
#[derive(Clone)]
pub struct Metrics<T: ElectionType> {
    /// The state of the node.
    pub state: State,
    /// Current term of the node.
    pub current_term: u64,
    /// Current leader of the group.
    pub current_leader: Option<T::NodeId>,
}

impl<T: ElectionType> Metrics<T> {
    #[inline]
    pub(crate) fn new() -> Self {
        Metrics {
            state: State::Startup,
            current_term: 0,
            current_leader: None,
        }
    }
}

pub(crate) struct MetricsReporter<T: ElectionType> {
    metrics_tx: WatchSender<Metrics<T>>,
}

impl<T: ElectionType> MetricsReporter<T> {
    #[inline]
    pub(crate) fn new(metrics_tx: WatchSender<Metrics<T>>) -> Self {
        MetricsReporter { metrics_tx }
    }

    #[inline]
    pub(crate) fn report(&mut self, metrics: Metrics<T>) {
        self.metrics_tx.send(metrics);
    }
}

/// The metrics watcher of the node.
#[derive(Clone)]
pub struct MetricsWatcher<T: ElectionType> {
    metrics_rx: WatchReceiver<Metrics<T>>,
}

impl<T: ElectionType> MetricsWatcher<T> {
    #[inline]
    pub(crate) fn new(metrics_rx: WatchReceiver<Metrics<T>>) -> Self {
        MetricsWatcher { metrics_rx }
    }

    /// A set of metrics describing the current state of the node.
    #[inline]
    pub fn metrics(&mut self) -> Metrics<T> {
        self.metrics_rx.get()
    }
}

#[inline]
pub(crate) fn metrics_channel<T: ElectionType>() -> (MetricsReporter<T>, MetricsWatcher<T>) {
    let (tx, rx) = watch::channel(Metrics::new());
    (MetricsReporter::new(tx), MetricsWatcher::new(rx))
}
