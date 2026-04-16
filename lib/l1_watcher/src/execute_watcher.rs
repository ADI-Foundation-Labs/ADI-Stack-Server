use crate::watcher::{L1Watcher, L1WatcherError};
use crate::{CommittedBatchProvider, L1WatcherConfig, ProcessL1Event, util};
use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::Log;
use std::time::Duration;
use zksync_os_contract_interface::IExecutor::BlockExecution;
use zksync_os_contract_interface::ZkChain;
use zksync_os_storage_api::WriteFinality;

pub struct L1ExecuteWatcher<Finality> {
    contract_address: Address,
    next_batch_number: u64,
    committed_batch_provider: CommittedBatchProvider,
    finality: Finality,
    grace_period: std::time::Duration,
}

impl<Finality: WriteFinality> L1ExecuteWatcher<Finality> {
    pub async fn create_watcher(
        config: L1WatcherConfig,
        zk_chain: ZkChain<DynProvider>,
        committed_batch_provider: CommittedBatchProvider,
        finality: Finality,
        l1_chain_id: u64,
    ) -> anyhow::Result<L1Watcher> {
        let current_l1_block = zk_chain.provider().get_block_number().await?;
        let last_executed_batch = finality.get_finality_status().last_executed_batch;
        tracing::info!(
            current_l1_block,
            last_executed_batch,
            config.max_blocks_to_process,
            ?config.poll_interval,
            zk_chain_address = ?zk_chain.address(),
            "initializing L1 execute watcher"
        );
        let last_l1_block =
            util::find_l1_execute_block_by_batch_number(zk_chain.clone(), last_executed_batch)
                .await?;
        tracing::info!(last_l1_block, "resolved on L1");

        let this = Self {
            contract_address: *zk_chain.address(),
            next_batch_number: last_executed_batch + 1,
            committed_batch_provider,
            finality,
            grace_period: config.proof_storage_grace_period,
        };
        let l1_watcher = L1Watcher::new(
            zk_chain.provider().clone(),
            // We start from last L1 block as it may contain more executed batches apart from the last
            // one.
            last_l1_block,
            config.max_blocks_to_process,
            config.confirmations,
            l1_chain_id,
            config.poll_interval,
            this.into(),
        )
        .await?;

        Ok(l1_watcher)
    }
}

#[async_trait::async_trait]
impl<Finality: WriteFinality> ProcessL1Event for L1ExecuteWatcher<Finality> {
    const NAME: &'static str = "block_execution";

    type SolEvent = BlockExecution;
    type WatchedEvent = BlockExecution;

    fn contract_address(&self) -> Address {
        self.contract_address
    }

    async fn process_event(
        &mut self,
        batch_execute: BlockExecution,
        _log: Log,
    ) -> Result<(), L1WatcherError> {
        let batch_number = batch_execute.batchNumber.to::<u64>();
        let batch_hash = batch_execute.batchHash;
        let batch_commitment = batch_execute.commitment;
        if batch_number < self.next_batch_number {
            tracing::debug!(
                batch_number,
                ?batch_hash,
                ?batch_commitment,
                "skipping already processed executed batch",
            );
        } else {
            // We might discover batch execute event before we discover commit event. In this case
            // we retry for up to 30s (enough for two L1 blocks to be mined).
            let committed_batch_provider = &self.committed_batch_provider;
            let discovered_batch = util::retry_with_grace_period(
                || async {
                    committed_batch_provider
                        .get(batch_number)
                        .ok_or_else(|| L1WatcherError::BatchNotCommitted(batch_number))
                        .map(Some)
                },
                self.grace_period,
                Duration::from_secs(5),
                &format!("executed batch {}", batch_number),
            )
            .await?;
            let last_executed_block = discovered_batch.last_block_number();
            let current = self.finality.get_finality_status();
            if batch_number <= current.last_executed_batch
                || last_executed_block <= current.last_executed_block
            {
                return Err(L1WatcherError::Other(anyhow::anyhow!(
                    "non-monotonous executed event: batch {batch_number} block {last_executed_block}, \
                     current batch {} block {}",
                    current.last_executed_batch,
                    current.last_executed_block,
                )).into());
            }
            self.finality.update_finality_status(|finality| {
                finality.last_executed_batch = batch_number;
                finality.last_executed_block = last_executed_block;
            });
            tracing::debug!(
                batch_number,
                ?batch_hash,
                ?batch_commitment,
                last_executed_block,
                "discovered executed batch"
            );
        }
        Ok(())
    }
}
