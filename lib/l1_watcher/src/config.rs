use std::time::Duration;

/// Configuration of L1 watcher.
#[derive(Clone, Debug)]
pub struct L1WatcherConfig {
    /// Max number of L1 blocks to be processed at a time.
    pub max_blocks_to_process: u64,

    /// Number of latest L1 blocks to leave unprocessed in order to reduce reorg risk.
    pub confirmations: u64,

    /// How often to poll L1 for the latest block.
    pub poll_interval: Duration,

    /// How often to poll L1 for the latest finalized block.
    /// Note: Finalization advances at epoch boundaries. Which is every ~6.4 minutes on L1.
    pub finalized_poll_interval: Duration,

    /// Number of recent blocks retained in the shared logs cache.
    pub logs_cache_capacity: usize,

    /// Grace period for proof storage lookups on External Nodes.
    /// When a batch is discovered on L1 but not yet in local proof storage,
    /// the node will retry for this duration before panicking.
    /// This allows time for a sidecar sync process to fetch proofs from the main node.
    pub proof_storage_grace_period: Duration,
}
