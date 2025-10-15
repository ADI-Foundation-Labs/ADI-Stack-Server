use crate::ReplayRecord;
use alloy::primitives::BlockNumber;
use futures::Stream;
use futures::stream::{BoxStream, StreamExt};
use pin_project::pin_project;
use std::task::Poll;
use std::time::Duration;
use tokio::time::{Instant, Sleep};
use zksync_os_interface::types::BlockContext;

/// Read-only view on block replay data.
///
/// Two main purposes:
/// * Sequencer's state recovery (provides all information needed to replay a block after restart).
/// * Execution environment for historical blocks (e.g., as required in `eth_call`).
pub trait ReadReplay: Send + Sync + 'static {
    /// Get block's execution context.
    fn get_context(&self, block_number: BlockNumber) -> Option<BlockContext>;

    /// Get full data needed to replay a block by its number.
    fn get_replay_record(&self, block_number: BlockNumber) -> Option<ReplayRecord>;

    // todo: can this ever return `None`? since we have genesis logic now we might be able to remove Option here
    /// Returns the greatest block number that has been appended, or None if empty.
    fn latest_record(&self) -> Option<BlockNumber>;
}

/// Extension methods for `ReadReplay`.
pub trait ReadReplayExt: ReadReplay {
    /// Streams all replay records with block_number ≥ `start`, in ascending block order. Finishes
    /// when after reaching the latest stored record.
    fn stream_from(&self, start: u64) -> BoxStream<ReplayRecord> {
        let latest = self.latest_record().unwrap_or(0);
        let stream = futures::stream::iter(start..=latest).filter_map(move |block_num| {
            let record = self.get_replay_record(block_num);
            match record {
                Some(record) => futures::future::ready(Some(record)),
                None => futures::future::ready(None),
            }
        });
        Box::pin(stream)
    }

    /// Streams all replay records with block_number ≥ `start`, in ascending block order. On reaching
    /// the latest stored record continuously waits for new records to appear.
    fn stream_from_forever(&self, start: BlockNumber) -> BoxStream<ReplayRecord>
    where
        Self: Clone,
    {
        #[pin_project]
        struct BlockStream<Replay: ReadReplay> {
            replays: Replay,
            current_block: BlockNumber,
            #[pin]
            sleep: Sleep,
        }
        impl<Replay: ReadReplay> Stream for BlockStream<Replay> {
            type Item = ReplayRecord;

            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                let mut this = self.project();
                if let Some(record) = this.replays.get_replay_record(*this.current_block) {
                    *this.current_block += 1;
                    Poll::Ready(Some(record))
                } else {
                    // TODO: would be nice to be woken up only when the next block is available
                    this.sleep
                        .as_mut()
                        .reset(Instant::now() + Duration::from_millis(50));
                    assert_eq!(this.sleep.poll(cx), Poll::Pending);
                    Poll::Pending
                }
            }
        }

        Box::pin(BlockStream {
            replays: self.clone(),
            current_block: start,
            sleep: tokio::time::sleep(Duration::from_millis(50)),
        })
    }
}

impl<T: ReadReplay> ReadReplayExt for T {}

pub trait WriteReplay: ReadReplay {
    fn append(&self, record: ReplayRecord);
}
