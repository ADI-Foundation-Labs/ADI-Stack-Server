use crate::PipelineComponent;
use crate::peekable_receiver::PeekableReceiver;
use anyhow::Result;
use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::{mpsc, watch};

/// A named pipeline task: component name and its spawnable task function
type PipelineTask = (&'static str, BoxFuture<'static, Result<()>>);

/// Pipeline with an active output stream that can be piped to more components
pub struct Pipeline<Output: Send + 'static> {
    tasks: Vec<PipelineTask>,
    receiver: PeekableReceiver<Output>,
    stop_receiver: watch::Receiver<bool>,
}

// todo: uncomment?
// impl Default for Pipeline<()> {
//     fn default() -> Self {
//         Self::new()
//     }
// }

impl Pipeline<()> {
    pub fn new(stop_receiver: watch::Receiver<bool>) -> Self {
        let (_sender, receiver) = mpsc::channel(1);
        Self {
            tasks: vec![],
            receiver: PeekableReceiver::new(receiver),
            stop_receiver,
        }
    }

    /// Spawn all pipeline component tasks into a JoinSet
    pub fn spawn(self, tasks: &mut tokio::task::JoinSet<()>) {
        // Spawn all component tasks into JoinSet (these run indefinitely)
        for (name, task_fn) in self.tasks {
            tasks.spawn(async move {
                match task_fn.await {
                    Ok(_) => tracing::warn!("{name} unexpectedly exited"),
                    Err(e) => tracing::error!("{name} failed: {e:#?}"),
                }
            });
        }
        // Drop the receiver - for terminal pipelines we don't need it
        drop(self.receiver);
    }
}

impl<Output: Send + 'static> Pipeline<Output> {
    /// Add a transformer component to the pipeline
    pub fn pipe<C>(mut self, component: C) -> Pipeline<C::Output>
    where
        C: PipelineComponent<Input = Output>,
    {
        let (output_sender, output_receiver) = mpsc::channel(C::OUTPUT_BUFFER_SIZE);
        let input_receiver = self.receiver;
        let stop_receiver = self.stop_receiver.clone();
    
        self.tasks.push((
            C::NAME,
            async move { component.run(input_receiver, output_sender, stop_receiver).await }.boxed(),
        ));

        Pipeline {
            tasks: self.tasks,
            receiver: PeekableReceiver::new(output_receiver),
            stop_receiver: self.stop_receiver,
        }
    }

    /// Add a transformer component to the pipeline with prepended messages
    ///
    /// This is useful when you need to reschedule messages at the start of the pipeline.
    /// The prepended messages are sent to the component before any messages from the pipeline.
    pub fn pipe_with_prepend<C>(mut self, component: C, prepend: Vec<Output>) -> Pipeline<C::Output>
    where
        C: PipelineComponent<Input = Output>,
    {
        let (output_sender, output_receiver) = mpsc::channel(C::OUTPUT_BUFFER_SIZE);
        let input_receiver = self.receiver.prepend(prepend);
        let stop_receiver = self.stop_receiver.clone();

        self.tasks.push((
            C::NAME,
            async move { component.run(input_receiver, output_sender, stop_receiver).await }.boxed(),
        ));

        Pipeline {
            tasks: self.tasks,
            receiver: PeekableReceiver::new(output_receiver),
            stop_receiver: self.stop_receiver,
        }
    }
}
