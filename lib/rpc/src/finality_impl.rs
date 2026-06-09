use crate::ReadRpcStorage;
use crate::result::ToRpcResult;
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::TxHash;
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use zksync_os_rpc_api::finality::{
    FinalityResponse, FinalityStage, NodeFinalityStatus, ZksFinalityApiServer,
};
use zksync_os_storage_api::RepositoryError;

pub struct FinalityNamespace<RpcStorage> {
    storage: RpcStorage,
}

impl<RpcStorage> FinalityNamespace<RpcStorage> {
    pub fn new(storage: RpcStorage) -> Self {
        Self { storage }
    }
}

impl<RpcStorage: ReadRpcStorage> FinalityNamespace<RpcStorage> {
    /// Builds a [`FinalityResponse`] for a block known to exist, enriching it with the containing
    /// batch number once the block is executed.
    async fn build_block_finality(&self, block_number: u64) -> FinalityResult<FinalityResponse> {
        let finality_status = self.storage.finality().get_finality_status();
        let stage = finality_stage(
            block_number,
            finality_status.last_committed_block,
            finality_status.last_executed_block,
        );
        // The batch<->block mapping is only resolvable once the batch is executed (the same
        // precondition `zks_getL2ToL1LogProof` enforces), so only enrich then. This also keeps
        // not-yet-executed blocks off the (object-store-backed) batch lookup entirely.
        let batch_number = if matches!(stage, FinalityStage::Executed) {
            self.storage
                .batch()
                .get_batch_by_block_number(block_number)?
                .map(|batch| batch.number())
        } else {
            None
        };
        Ok(FinalityResponse {
            stage,
            block_number: Some(block_number),
            batch_number,
        })
    }

    async fn get_block_finality_impl(
        &self,
        number: BlockNumberOrTag,
    ) -> FinalityResult<Option<FinalityResponse>> {
        let Some(block_number) = self.storage.resolve_block_number(BlockId::Number(number))? else {
            return Ok(None);
        };
        // `resolve_block_number` does not check existence for an explicit number, so bound it by the
        // latest sealed block; tags always resolve to an existing block.
        if block_number > self.storage.repository().get_latest_block() {
            return Ok(None);
        }
        Ok(Some(self.build_block_finality(block_number).await?))
    }

    async fn get_transaction_finality_impl(
        &self,
        tx_hash: TxHash,
    ) -> FinalityResult<Option<FinalityResponse>> {
        let Some(meta) = self.storage.repository().get_transaction_meta(tx_hash)? else {
            return Ok(None);
        };
        Ok(Some(self.build_block_finality(meta.block_number).await?))
    }

    async fn get_batch_finality_impl(&self, batch_number: u64) -> FinalityResult<FinalityResponse> {
        let finality_status = self.storage.finality().get_finality_status();
        let stage = finality_stage(
            batch_number,
            finality_status.last_committed_batch,
            finality_status.last_executed_batch,
        );
        // The block range is only resolvable once the batch is executed; bounding the lookup this
        // way also keeps unbounded/future batch numbers off the object store.
        let block_number = if matches!(stage, FinalityStage::Executed) {
            self.storage
                .batch()
                .get_batch_by_number(batch_number)?
                .map(|batch| batch.last_block_number())
        } else {
            None
        };
        Ok(FinalityResponse {
            stage,
            block_number,
            batch_number: Some(batch_number),
        })
    }
}

#[async_trait]
impl<RpcStorage: ReadRpcStorage> ZksFinalityApiServer for FinalityNamespace<RpcStorage> {
    async fn get_block_finality(
        &self,
        number: BlockNumberOrTag,
    ) -> RpcResult<Option<FinalityResponse>> {
        self.get_block_finality_impl(number).await.to_rpc_result()
    }

    async fn get_transaction_finality(
        &self,
        tx_hash: TxHash,
    ) -> RpcResult<Option<FinalityResponse>> {
        self.get_transaction_finality_impl(tx_hash)
            .await
            .to_rpc_result()
    }

    async fn get_batch_finality(&self, batch_number: u64) -> RpcResult<FinalityResponse> {
        self.get_batch_finality_impl(batch_number)
            .await
            .to_rpc_result()
    }

    async fn get_finality_status(&self) -> RpcResult<NodeFinalityStatus> {
        let finality_status = self.storage.finality().get_finality_status();
        Ok(NodeFinalityStatus {
            last_sealed_block: self.storage.repository().get_latest_block(),
            last_committed_block: finality_status.last_committed_block,
            last_committed_batch: finality_status.last_committed_batch,
            last_executed_block: finality_status.last_executed_block,
            last_executed_batch: finality_status.last_executed_batch,
        })
    }
}

/// Classifies a block or batch number against the committed and executed frontiers. Callers must
/// pass matching units (block frontiers for a block number, batch frontiers for a batch number).
fn finality_stage(number: u64, committed: u64, executed: u64) -> FinalityStage {
    debug_assert!(
        executed <= committed,
        "executed frontier is ahead of committed"
    );
    if number <= executed {
        FinalityStage::Executed
    } else if number <= committed {
        FinalityStage::Committed
    } else {
        FinalityStage::Pending
    }
}

/// `zks` finality result type.
pub type FinalityResult<Ok> = Result<Ok, FinalityError>;

/// General `zks` finality errors.
#[derive(Debug, thiserror::Error)]
pub enum FinalityError {
    #[error(transparent)]
    Batch(#[from] anyhow::Error),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}
