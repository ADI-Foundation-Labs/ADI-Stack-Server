use alloy::primitives::map::foldhash::HashMap;
use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use reth_revm::db::CacheDB;
use reth_revm::state::Account;

use reth_revm::InspectCommitEvm;
use reth_revm::InspectEvm;
use reth_revm::context::Context;
use revm_inspectors::tracing::{TracingInspector, TracingInspectorConfig};
use tokio::sync::mpsc::Sender;
use zksync_os_interface::types::BlockOutput;
use zksync_os_pipeline::{PeekableReceiver, PipelineComponent};
use zksync_os_storage_api::{ReadStateHistory, ReplayRecord};
use zksync_revm::{DefaultZk, ZkBuilder};

use crate::revm_two_fa::helpers::zk_tx_try_into_revm_tx;
use crate::revm_two_fa::revm_state_db::RevmStateDb;
use crate::revm_two_fa::storage_diff_comp::{accumulate_revm_state_diffs, compare_state_diffs};

pub struct RevmTwoFa<State>
where
    State: ReadStateHistory + Clone + Send + 'static,
{
    state: RevmStateDb<State>,
}

impl<State> RevmTwoFa<State>
where
    State: ReadStateHistory + Clone + Send + 'static,
{
    pub fn new(state: State) -> Self {
        Self {
            state: RevmStateDb::new(state, 0, Default::default()),
        }
    }
}

#[async_trait]
impl<State> PipelineComponent for RevmTwoFa<State>
where
    State: ReadStateHistory + Clone + Send + 'static,
{
    type Input = (BlockOutput, ReplayRecord);
    type Output = (BlockOutput, ReplayRecord);

    const NAME: &'static str = "REVM 2FA";
    const OUTPUT_BUFFER_SIZE: usize = 5;

    async fn run(
        mut self,
        mut input: PeekableReceiver<Self::Input>, // PeekableReceiver<(BlockOutput, ReplayRecord)>
        output: Sender<Self::Output>,             // Sender<(BlockOutput, ReplayRecord)>
    ) -> anyhow::Result<()> {
        let mut tracer = TracingInspector::new(TracingInspectorConfig::none().with_state_diffs());
        loop {
            let Some((block_output, replay_record)) = input.recv().await else {
                anyhow::bail!("inbound channel closed");
            };

            // Immediately send the output to avoid blocking the next pipeline steps
            if output
                .send((block_output.clone(), replay_record.clone()))
                .await
                .is_err()
            {
                anyhow::bail!("Outbound channel closed");
            }

            self.state.set_latest_block(
                replay_record.block_context.block_number - 1,
                replay_record.block_context.block_hashes,
            );
            // For each block, we create an in-memory cache database to accumulate transaction state changes separately
            let mut cache_db = CacheDB::new(self.state.clone());
            let mut evm = Context::default()
                .with_db(&mut cache_db)
                .modify_cfg_chained(|cfg| {
                    cfg.chain_id = replay_record.block_context.chain_id;
                })
                .modify_block_chained(|block| {
                    block.number = U256::from(replay_record.block_context.block_number);
                    block.timestamp = U256::from(replay_record.block_context.timestamp);
                    block.beneficiary = replay_record.block_context.coinbase;
                })
                .build_zk_with_inspector(&mut tracer);

            let mut state_changes_in_txs: Vec<HashMap<Address, Account>> = Default::default();
            for (transaction, tx_output_raw) in replay_record
                .transactions
                .iter()
                .zip(block_output.tx_results)
            {
                let tx_output = match tx_output_raw {
                    Ok(tx_output) => tx_output,
                    _ => continue, // Skip invalid transaction as they are not included in the batch
                };

                // Skip transactions that failed on the ZKsync OS node
                // We will only charge fees and bump nonce for such transactions
                if !tx_output.is_success() {
                    // TODO: charge gas and bump nonce
                    continue;
                }

                let zk_tx = zk_tx_try_into_revm_tx(&transaction)?;
                let ref_tx = evm.inspect_tx(zk_tx.clone())?;
                evm.inspect_tx_commit(zk_tx)?;
                // Move to separate function
                state_changes_in_txs.push(ref_tx.state);
            }
            compare_state_diffs(
                &accumulate_revm_state_diffs(&state_changes_in_txs),
                &block_output.storage_writes,
                &block_output.account_diffs,
            );
        }
    }
}
