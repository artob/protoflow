use protoflow::derive::Block;
use protoflow::types::Any;
use protoflow::{blocks::*, Message};
use protoflow::{Block, BlockResult, BlockRuntime, InputPort, OutputPort};

use async_trait::async_trait;
use std::time::Duration;

#[cfg(feature = "tokio")]
use protoflow::AsyncBlock;

#[derive(Block, Clone)]
pub struct AsyncDelay<T: Message = Any> {
    #[input]
    pub input: InputPort<T>,

    #[output]
    pub output: OutputPort<T>,

    #[parameter]
    pub delay: Duration,
}

impl<T: Message> AsyncDelay<T> {
    pub fn with_params(input: InputPort<T>, output: OutputPort<T>, delay: Duration) -> Self {
        Self {
            input,
            output,
            delay,
        }
    }
}

impl<T: Message + 'static> AsyncDelay<T> {
    pub fn with_system(system: &System, delay: Duration) -> Self {
        use crate::SystemBuilding;
        Self::with_params(system.input(), system.output(), delay)
    }
}

#[cfg(not(feature = "tokio"))]
impl<T: Message> Block for AsyncDelay<T> {
    fn execute(&mut self, runtime: &dyn BlockRuntime) -> BlockResult {
        while let Some(s) = self.input.recv()? {
            runtime.sleep_for(self.delay)?;
            self.output.send(&s)?;
        }

        Ok(())
    }
}

#[cfg(feature = "tokio")]
#[async_trait]
impl<T: Message> AsyncBlock for AsyncDelay<T> {
    async fn execute_async(&mut self, _runtime: &dyn BlockRuntime) -> BlockResult {
        while let Some(s) = self.input.recv()? {
            tokio::time::sleep(self.delay).await;
            self.output.send(&s)?;
        }

        Ok(())
    }
}

fn main() -> BlockResult {
    let f = |s: &mut System| {
        let greeting = s.const_string("Hello, world!");

        let line_encoder = s.encode_lines();
        s.connect(&greeting.output, &line_encoder.input);

        let duration = Duration::from_secs(1);

        #[cfg(not(feature = "tokio"))]
        let transform = s.block(AsyncDelay::with_system(s, duration));
        #[cfg(feature = "tokio")]
        let transform = s.block_async(AsyncDelay::with_system(s, duration));

        s.connect(&line_encoder.output, &transform.input);

        let stdout = s.write_stdout();
        s.connect(&transform.output, &stdout.input);
    };

    #[cfg(feature = "tokio")]
    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let rt_handle = rt.handle();

        System::run_async(rt_handle.clone(), f)
    }
    #[cfg(not(feature = "tokio"))]
    System::run(f)
}
