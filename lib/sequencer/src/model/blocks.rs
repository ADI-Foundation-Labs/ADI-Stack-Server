use alloy::primitives::{B256, U256};
use futures::Stream;
use std::collections::VecDeque;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use zksync_os_interface::types::BlockContext;
use zksync_os_mempool::TxStream;
use zksync_os_storage_api::ReplayRecord;
use zksync_os_types::{L1TxSerialId, ZkTransaction};

/// `BlockCommand`s drive the sequencer execution.
/// Produced by `CommandProducer` - first blocks are `Replay`ed from block replay storage
/// and then `Produce`d indefinitely.
///
/// Downstream transform:
/// `BlockTransactionProvider: (L1Mempool/L1Watcher, L2Mempool, BlockCommand) -> (PreparedBlockCommand)`
#[derive(Debug)]
pub enum BlockCommand {
    /// Replay a block from block replay storage.
    Replay(Box<ReplayRecord>),
    /// Produce a new block from the mempool.
    /// Second argument - local seal criteria - target block time and max transaction number
    /// (Avoid container struct for now)
    Produce(ProduceCommand),
    Rebuild(RebuildCommand),
}

/// Command to produce a new block.
#[derive(Clone, Debug)]
pub struct ProduceCommand {
    pub block_time: Duration,
    pub max_transactions_in_block: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartialBlockContext {
    pub timestamp: u64,
    pub eip1559_basefee: U256,
    pub pubdata_price: U256,
    pub native_price: U256,
    pub execution_version: u32,
}

#[derive(Debug)]
pub struct TxRebuildStream {
    pub txs: Arc<Mutex<VecDeque<ZkTransaction>>>,
}

impl Stream for TxRebuildStream {
    type Item = ZkTransaction;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut txs_lock = this.txs.lock().unwrap();
        if let Some(tx) = txs_lock.pop_front() {
            Poll::Ready(Some(tx))
        } else {
            Poll::Ready(None)
        }
    }
}

impl TxStream for TxRebuildStream {
    fn mark_last_tx_as_invalid(self: Pin<&mut Self>) {
        // No-op for rebuild stream
    }
}

#[derive(Debug)]
pub struct RebuildCommand {
    pub partial_block_context: PartialBlockContext,
    pub txs: Pin<Box<TxRebuildStream>>,
}

impl Display for BlockCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockCommand::Replay(record) => write!(
                f,
                "Replay block {} ({} txs); strating l1 priority id: {}",
                record.block_context.block_number,
                record.transactions.len(),
                record.starting_l1_priority_id,
            ),
            BlockCommand::Produce(command) => write!(f, "Produce block: {command:?}"),
            BlockCommand::Rebuild(command) => todo!()
        }
    }
}

/// BlockCommand + Tx Source = PreparedBlockCommand
/// We use `BlockCommand` upstream (`CommandProducer`, `BlockTransactionProvider`),
/// while doing all preparations that depend on command type (replay vs produce).
/// Then we switch to `PreparedBlockCommand` in `BlockExecutor`,
/// which should handle them uniformly.
///
/// Downstream transform:
/// `BlockExecutor: (State, PreparedBlockCommand) -> (BlockOutput, ReplayRecord)`
pub struct PreparedBlockCommand<'a> {
    pub block_context: BlockContext,
    pub seal_policy: SealPolicy,
    pub invalid_tx_policy: InvalidTxPolicy,
    pub tx_source: Pin<Box<dyn TxStream<Item = ZkTransaction> + Send + 'a>>,
    /// L1 transaction serial id expected at the beginning of this block.
    /// Not used in execution directly, but required to construct ReplayRecord
    pub starting_l1_priority_id: L1TxSerialId,
    pub metrics_label: &'static str,
    pub node_version: semver::Version,
    /// Expected hash of the block output (missing for command generated from `BlockCommand::Produce`)
    pub expected_block_output_hash: Option<B256>,
    pub previous_block_timestamp: u64,
}

/// Behaviour when VM returns an InvalidTransaction error.
#[derive(Clone, Copy, Debug)]
pub enum InvalidTxPolicy {
    /// Invalid tx is skipped in block and discarded from mempool. Used when building a block.
    RejectAndContinue,
    /// Bubble the error up and abort the whole block. Used when replaying a block (ReplayLog / Replica / EN)
    Abort,
}

#[derive(Clone, Copy, Debug)]
pub enum SealPolicy {
    /// Seal non-empty blocks after deadline or N transactions. Used when building a block
    /// (Block Deadline, Block Size)
    Decide(Duration, usize),
    /// Seal when all txs from tx source are executed. Used when replaying a block (ReplayLog / Replica / EN)
    UntilExhausted,
}
