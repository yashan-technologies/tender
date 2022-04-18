use crate::error::{Error, Result};

/// Default election timeout minimum, in milliseconds.
const DEFAULT_ELECTION_TIMEOUT_MIN: u64 = 1500;
/// Default election timeout maximum, in milliseconds.
const DEFAULT_ELECTION_TIMEOUT_MAX: u64 = 3000;
/// Default heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL: u64 = 500;

/// The raft runtime configurations.
#[derive(Debug, Clone)]
pub struct Options {
    /// The minimum election timeout in milliseconds.
    election_timeout_min: u64,
    /// The maximum election timeout in milliseconds.
    election_timeout_max: u64,

    /// The heartbeat interval in milliseconds at which leaders will send heartbeats to followers.
    ///
    /// Defaults to 500 milliseconds.
    ///
    /// **NOTE WELL:** it is very important that this value be greater than the amount if time
    /// it will take on average for heartbeat frames to be sent between nodes. No data processing
    /// is performed for heartbeats, so the main item of concern here is network latency. This
    /// value is also used as the default timeout for sending heartbeats.
    heartbeat_interval: u64,

    /// The minimum number of votes that a node has to obtain in order to be the leader.
    quorum: Quorum,
}

/// The minimum number of votes that a node has to obtain in order to be the leader.
#[derive(Debug, Copy, Clone)]
pub enum Quorum {
    /// A node has to obtain (N/2 + 1) votes in order to be the leader.
    Major,
    /// A node has to obtain `Any` votes in order to be the leader.
    /// When `Any` < (N/2 + 1), it will be forced equivalent to `Major`.
    Any(u32),
}

impl Default for Quorum {
    #[inline]
    fn default() -> Self {
        Quorum::Major
    }
}

impl Options {
    /// Creates a new `Options` with default values.
    #[inline]
    pub const fn new() -> Self {
        Options {
            election_timeout_min: DEFAULT_ELECTION_TIMEOUT_MIN,
            election_timeout_max: DEFAULT_ELECTION_TIMEOUT_MAX,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            quorum: Quorum::Major,
        }
    }

    /// Generates a new random election timeout within the [election_timeout_min, election_timeout_max).
    #[inline]
    pub fn random_election_timeout(&self) -> u64 {
        fastrand::u64(self.election_timeout_min..self.election_timeout_max)
    }

    /// The minimum election timeout in milliseconds.
    #[inline]
    pub const fn election_timeout_min(&self) -> u64 {
        self.election_timeout_min
    }

    /// The heartbeat interval in milliseconds at which leaders will send heartbeats to followers.
    #[inline]
    pub const fn heartbeat_interval(&self) -> u64 {
        self.heartbeat_interval
    }

    /// The minimum number of votes that a node has to obtain in order to be the leader.
    #[inline]
    pub const fn quorum(&self) -> Quorum {
        self.quorum
    }

    /// Creates a new `Options` builder.
    #[inline]
    pub const fn builder() -> OptionsBuilder {
        OptionsBuilder::new()
    }
}

impl Default for Options {
    #[inline]
    fn default() -> Self {
        Options::new()
    }
}

/// The builder of raft runtime configurations.
#[derive(Debug, Default)]
pub struct OptionsBuilder {
    election_timeout_min: Option<u64>,
    election_timeout_max: Option<u64>,
    heartbeat_interval: Option<u64>,
    quorum: Option<Quorum>,
}

impl OptionsBuilder {
    /// Creates a new `Options` builder.
    #[inline]
    pub const fn new() -> Self {
        OptionsBuilder {
            election_timeout_min: None,
            election_timeout_max: None,
            heartbeat_interval: None,
            quorum: None,
        }
    }

    /// The minimum election timeout in milliseconds.
    #[inline]
    pub const fn election_timeout_min(mut self, val: u64) -> Self {
        self.election_timeout_min = Some(val);
        self
    }

    /// The maximum election timeout in milliseconds.
    #[inline]
    pub const fn election_timeout_max(mut self, val: u64) -> Self {
        self.election_timeout_max = Some(val);
        self
    }

    /// The heartbeat interval in milliseconds at which leaders will send heartbeats to followers.
    #[inline]
    pub const fn heartbeat_interval(mut self, val: u64) -> Self {
        self.heartbeat_interval = Some(val);
        self
    }

    /// The minimum number of votes that a node has to obtain in order to be the leader.
    #[inline]
    pub const fn quorum(mut self, quorum: Quorum) -> Self {
        self.quorum = Some(quorum);
        self
    }

    /// Builds a new `Options`.
    #[inline]
    pub fn build(self) -> Result<Options> {
        let election_timeout_min = self.election_timeout_min.unwrap_or(DEFAULT_ELECTION_TIMEOUT_MIN);
        let election_timeout_max = self.election_timeout_max.unwrap_or(DEFAULT_ELECTION_TIMEOUT_MAX);
        if election_timeout_min >= election_timeout_max {
            return Err(Error::InvalidOptions(format!(
                "election timeout min({}) & max({}) are invalid: max must be greater than min",
                election_timeout_min, election_timeout_max
            )));
        }

        let heartbeat_interval = self.heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
        if election_timeout_min <= heartbeat_interval {
            return Err(Error::InvalidOptions(format!(
                "election_timeout_min({}) must be greater than heartbeat_interval({})",
                election_timeout_min, heartbeat_interval
            )));
        }

        let quorum = self.quorum.unwrap_or(Quorum::Major);

        Ok(Options {
            election_timeout_min,
            election_timeout_max,
            heartbeat_interval,
            quorum,
        })
    }
}
