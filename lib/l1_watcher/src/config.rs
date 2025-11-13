use std::time::Duration;

/// Configuration of L1 watcher.
#[derive(Clone, Debug)]
pub struct L1WatcherConfig {
    /// Max number of L1 blocks to be processed at a time.
    pub max_blocks_to_process: u64,

    /// How often to poll L1 for new priority requests.
    pub poll_interval: Duration,

    /// Grace period for proof storage lookups on External Nodes.
    /// When a batch is discovered on L1 but not yet in local proof storage,
    /// the node will retry for this duration before panicking.
    /// This allows time for a sidecar sync process to fetch proofs from the main node.
    pub proof_storage_grace_period: Duration,
}
