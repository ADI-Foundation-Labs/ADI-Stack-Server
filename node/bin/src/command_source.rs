use crate::replay_transport::replay_receiver;
use alloy::primitives::U256;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use zksync_os_pipeline::{PeekableReceiver, PipelineComponent};
use zksync_os_sequencer::model::blocks::{
    BlockCommand, PartialBlockContext, ProduceCommand, RebuildCommand, TxRebuildStream,
};
use zksync_os_storage_api::{ReadReplay, ReadReplayExt};
use zksync_os_types::{L2Envelope, ZkTransaction};

/// Main node command source
#[derive(Debug)]
pub struct MainNodeCommandSource<Replay> {
    pub block_replay_storage: Replay,
    pub starting_block: u64,
    pub block_time: Duration,
    pub max_transactions_in_block: usize,
}

/// External node command source
#[derive(Debug)]
pub struct ExternalNodeCommandSource {
    pub starting_block: u64,
    pub replay_download_address: String,
}

#[async_trait]
impl<Replay: ReadReplay> PipelineComponent for MainNodeCommandSource<Replay> {
    type Input = ();
    type Output = BlockCommand;

    const NAME: &'static str = "command_source";
    const OUTPUT_BUFFER_SIZE: usize = 5;

    async fn run(
        self,
        _input: PeekableReceiver<()>,
        output: mpsc::Sender<BlockCommand>,
    ) -> anyhow::Result<()> {
        // TODO: no need for a Stream in `command_source` - just send to channel right away instead
        let mut stream = command_source(
            &self.block_replay_storage,
            self.starting_block,
            self.block_time,
            self.max_transactions_in_block,
            None
        );

        while let Some(command) = stream.next().await {
            tracing::debug!(?command, "Sending block command");
            if output.send(command).await.is_err() {
                tracing::warn!("Command output channel closed, stopping source");
                break;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl PipelineComponent for ExternalNodeCommandSource {
    type Input = ();
    type Output = BlockCommand;

    const NAME: &'static str = "external_node_command_source";
    const OUTPUT_BUFFER_SIZE: usize = 5;

    async fn run(
        self,
        _input: PeekableReceiver<()>,
        output: mpsc::Sender<BlockCommand>,
    ) -> anyhow::Result<()> {
        // TODO: no need for a Stream in `replay_receiver` - just send to channel right away instead
        let mut stream = replay_receiver(self.starting_block, self.replay_download_address.clone())
            .await
            .map_err(|err| {
                tracing::error!(?err, "Failed to connect to main node to receive blocks");
                err
            })?;

        while let Some(command) = stream.next().await {
            tracing::debug!(?command, "Received block command from main node");
            if output.send(command).await.is_err() {
                tracing::warn!("Command output channel closed, stopping source");
                break;
            }
        }

        Ok(())
    }
}

pub struct BlockRebuildStream {
    pub data: BTreeMap<PartialBlockContext, Arc<Mutex<VecDeque<ZkTransaction>>>>,
}

impl Stream for BlockRebuildStream {
    type Item = RebuildCommand;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let Some(entry) = this.data.first_entry() else {
                return Poll::Ready(None);
            };
            let partial_block_context = *entry.key();
            let txs = entry.get().lock().unwrap();
            if txs.is_empty() {
                continue;
            } else {
                return Poll::Ready(Some(RebuildCommand {
                    partial_block_context,
                    txs: Box::pin(TxRebuildStream {
                        txs: entry.get().clone(),
                    }),
                }));
            }
        }
    }
}

fn command_source(
    block_replay_wal: &impl ReadReplay,
    block_to_start: u64,
    block_time: Duration,
    max_transactions_in_block: usize,
    block_to_rebuild_from: Option<u64>,
) -> BoxStream<BlockCommand> {
    assert!(block_to_start >= 1);

    let last_block_in_wal = block_replay_wal.latest_record();
    tracing::info!(last_block_in_wal, block_to_start, "starting command source");

    let (replay_end, rebuild_stream): (u64, BoxStream<BlockCommand>) =
        if let Some(block_to_rebuild_from) = block_to_rebuild_from {
            assert!(block_to_start < block_to_rebuild_from);
            assert!(block_to_rebuild_from <= last_block_in_wal);

            let mut map: BTreeMap<PartialBlockContext, Arc<Mutex<VecDeque<ZkTransaction>>>> =
                BTreeMap::new();
            for block in block_to_rebuild_from..=last_block_in_wal {
                let record = block_replay_wal.get_replay_record(block).unwrap();
                let partial_block_context = PartialBlockContext {
                    timestamp: record.block_context.timestamp,
                    eip1559_basefee: record.block_context.eip1559_basefee,
                    pubdata_price: record.block_context.pubdata_price,
                    native_price: record.block_context.native_price,
                    execution_version: record.block_context.execution_version,
                };
                map.entry(partial_block_context)
                    .or_default()
                    .lock()
                    .unwrap()
                    .extend(record.transactions);
            }
            let block_rebuild_stream = BlockRebuildStream { data: map }
                .map(|rebuild_command| BlockCommand::Rebuild(rebuild_command))
                .boxed();
            (block_to_rebuild_from - 1, block_rebuild_stream)
        } else {
            (last_block_in_wal, futures::stream::empty().boxed())
        };

    // Stream of replay commands from WAL
    // Guaranteed to stream exactly `[block_to_start; replay_end]` and have no extra records
    // in it when it finishes. Reasoning:
    // * WriteReplay guarantees immutability if `append` returns `false`
    // * `append` returns `false` for all `BlockCommand::Replay` commands as the record was taken
    //   from the storage
    let replay_wal_stream = block_replay_wal
        .stream_from(block_to_start, replay_end)
        .map(|record| BlockCommand::Replay(Box::new(record)));

    // Combined source: run WAL replay first, then produce blocks from mempool
    let produce_stream: BoxStream<BlockCommand> =
        futures::stream::repeat(ProduceCommand {
            block_time,
            max_transactions_in_block,
        })
        .map(|c| BlockCommand::Produce(c))
        .boxed();
    let stream = replay_wal_stream
        .chain(rebuild_stream)
        .chain(produce_stream);
    stream.boxed()
}
