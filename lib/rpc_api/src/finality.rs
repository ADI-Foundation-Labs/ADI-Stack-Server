//! `zks` finality methods: report how far a block / transaction / batch has progressed
//! through the L1 finality pipeline (sealed → committed → executed).
//!
//! These live in a dedicated trait (rather than in [`crate::zks::ZksApi`]) so this file can be
//! added or removed without touching the upstream `zks` definitions, keeping merges conflict-free.

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::TxHash;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};

/// Stage reached in the L1 finality pipeline.
///
/// Progression, from least to most final: `Pending` → `Committed` → `Executed`.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum FinalityStage {
    /// Produced by the sequencer but not yet committed to L1; matches the `latest` block tag.
    Pending,
    /// Committed to L1 (matches the `safe` block tag).
    Committed,
    /// Executed on L1; matches the `finalized` block tag.
    Executed,
}

/// Finality status of a single block, transaction, or batch.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FinalityResponse {
    /// Highest finality stage reached.
    pub stage: FinalityStage,
    /// L2 block number: the queried block, the transaction's block, or — for a batch query — the
    /// batch's last block. The batch's last block is populated only once the batch is executed.
    pub block_number: Option<u64>,
    /// L1 batch number: the batch containing the queried block/transaction, or the queried batch
    /// itself. For a block/transaction query this is populated only once the block is executed.
    pub batch_number: Option<u64>,
}

/// Snapshot of every finality frontier the node currently tracks, in a single call.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NodeFinalityStatus {
    /// Latest block sealed by the sequencer (matches `latest`).
    pub last_sealed_block: u64,
    /// Last block committed to L1 (matches `safe`).
    pub last_committed_block: u64,
    /// Last batch committed to L1.
    pub last_committed_batch: u64,
    /// Last block whose batch was executed on L1 (matches `finalized`).
    pub last_executed_block: u64,
    /// Last batch executed on L1.
    pub last_executed_batch: u64,
}

#[cfg_attr(not(feature = "server"), rpc(client, namespace = "zks"))]
#[cfg_attr(feature = "server", rpc(server, client, namespace = "zks"))]
pub trait ZksFinalityApi {
    /// Returns the finality status of a block (by number or tag), or `null` if the block does not
    /// exist on this node.
    #[method(name = "getBlockFinality")]
    async fn get_block_finality(
        &self,
        number: BlockNumberOrTag,
    ) -> RpcResult<Option<FinalityResponse>>;

    /// Returns the finality status of a transaction (by hash), or `null` if it is unknown to this
    /// node.
    #[method(name = "getTransactionFinality")]
    async fn get_transaction_finality(
        &self,
        tx_hash: TxHash,
    ) -> RpcResult<Option<FinalityResponse>>;

    /// Returns the finality status of an L1 batch (by number). Batches above the committed frontier
    /// report [`FinalityStage::Pending`].
    #[method(name = "getBatchFinality")]
    async fn get_batch_finality(&self, batch_number: u64) -> RpcResult<FinalityResponse>;

    /// Returns every finality frontier the node tracks (sealed / committed / executed) in a single
    /// call.
    #[method(name = "getFinalityStatus")]
    async fn get_finality_status(&self) -> RpcResult<NodeFinalityStatus>;
}
