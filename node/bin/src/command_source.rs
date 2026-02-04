use crate::replay_transport::replay_receiver;
use alloy::primitives::Bytes;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use zksync_os_pipeline::{PeekableReceiver, PipelineComponent};
use zksync_os_sequencer::model::blocks::{BlockCommand, ProduceCommand, RebuildCommand};
use zksync_os_storage_api::{ReadReplay, ReadReplayExt};

/// Configuration for replay stream retry behavior
const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(120);
const RETRY_BACKOFF_FACTOR: u32 = 2;

/// Main node command source
#[derive(Debug)]
pub struct MainNodeCommandSource<Replay> {
    pub block_replay_storage: Replay,
    pub starting_block: u64,
    pub rebuild_options: Option<RebuildOptions>,
    pub block_time: Duration,
    pub max_transactions_in_block: usize,
}

#[derive(Debug)]
pub struct RebuildOptions {
    pub rebuild_from_block: u64,
    pub blocks_to_empty: HashSet<u64>,
}

/// External node command source
#[derive(Debug)]
pub struct ExternalNodeCommandSource {
    pub starting_block: u64,
    pub record_overrides: Vec<(u64, Bytes)>,
    pub up_to_block: Option<u64>,
    pub replay_download_address: String,
    pub stop_receiver: watch::Receiver<bool>,
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
            self.rebuild_options,
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
        mut self,
        _input: PeekableReceiver<()>,
        output: mpsc::Sender<BlockCommand>,
    ) -> anyhow::Result<()> {
        let mut current_block = self.starting_block;
        let mut retry_delay = RETRY_INITIAL_DELAY;

        loop {
            tracing::info!(starting_block = current_block, "Connecting to main node");

            let mut stream = match replay_receiver(
                current_block,
                self.record_overrides.clone(),
                &self.replay_download_address,
            )
            .await
            {
                Ok(s) => {
                    retry_delay = RETRY_INITIAL_DELAY;
                    s
                }
                Err(err) => {
                    tracing::warn!(?err, ?retry_delay, "Connection failed, retrying...");
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * RETRY_BACKOFF_FACTOR).min(RETRY_MAX_DELAY);
                    continue;
                }
            };

            loop {
                match stream.next().await {
                    Some(Ok(command)) => {
                        let block_number = command.block_number();

                        if self.up_to_block.is_some_and(|up_to| block_number > up_to) {
                            tracing::info!(block_number, "Reached up_to_block, halting");
                            let _ = self.stop_receiver.wait_for(|stop| *stop).await;
                        }

                        if output.send(command).await.is_err() {
                            tracing::warn!("Output channel closed, stopping");
                            return Ok(());
                        }
                        current_block = block_number + 1;
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, current_block, ?retry_delay, "Stream error, reconnecting...");
                        break;
                    }
                    None => {
                        tracing::warn!(current_block, "Stream ended unexpectedly");
                        break;
                    }
                }
            }

            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * RETRY_BACKOFF_FACTOR).min(RETRY_MAX_DELAY);
        }
    }
}

fn command_source(
    block_replay_wal: &impl ReadReplay,
    block_to_start: u64,
    block_time: Duration,
    max_transactions_in_block: usize,
    rebuild_options: Option<RebuildOptions>,
) -> BoxStream<BlockCommand> {
    let last_block_in_wal = block_replay_wal.latest_record();
    tracing::info!(
        last_block_in_wal,
        block_to_start,
        ?rebuild_options,
        "starting command source"
    );

    let (replay_end, rebuild_stream): (u64, BoxStream<BlockCommand>) =
        if let Some(rebuild_options) = rebuild_options {
            assert!(
                rebuild_options.rebuild_from_block >= block_to_start,
                "rebuild_from_block must be >= block_to_start, got {} < {}",
                rebuild_options.rebuild_from_block,
                block_to_start
            );

            assert!(
                rebuild_options.rebuild_from_block <= last_block_in_wal,
                "rebuild_from_block must be <= last_block_in_wal, got {} > {}",
                rebuild_options.rebuild_from_block,
                last_block_in_wal
            );

            let command_iterator =
                (rebuild_options.rebuild_from_block..=last_block_in_wal).map(move |block_number| {
                    let replay_record = block_replay_wal
                        .get_replay_record(block_number)
                        .expect("Replay record must exist for rebuild");
                    let make_empty = rebuild_options.blocks_to_empty.contains(&block_number);
                    BlockCommand::Rebuild(Box::new(RebuildCommand {
                        replay_record,
                        make_empty,
                    }))
                });
            (
                rebuild_options.rebuild_from_block - 1,
                futures::stream::iter(command_iterator).boxed(),
            )
        } else {
            (last_block_in_wal, futures::stream::empty().boxed())
        };

    // Stream of replay commands from WAL
    // Guaranteed to stream exactly `[block_to_start; replay_end]`.
    let replay_wal_stream = block_replay_wal
        .stream(block_to_start, replay_end)
        .map(|record| BlockCommand::Replay(Box::new(record)));

    let produce_stream: BoxStream<BlockCommand> =
        futures::stream::unfold(last_block_in_wal + 1, move |block_number| async move {
            Some((
                BlockCommand::Produce(ProduceCommand {
                    block_number,
                    block_time,
                    max_transactions_in_block,
                }),
                block_number + 1,
            ))
        })
        .boxed();
    // Combined source: run WAL replay first, then rebuild (normally empty), then produce blocks from mempool
    let stream = replay_wal_stream
        .chain(rebuild_stream)
        .chain(produce_stream);
    stream.boxed()
}
