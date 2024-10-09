// This is free and unencumbered software released into the public domain.

use crate::prelude::Box;
use crate::{BlockDescriptor, BlockHooks, BlockResult, BlockRuntime};

use async_trait::async_trait;
use core::fmt;

/// An async version of a regular block.
#[async_trait]
pub trait AsyncBlock: BlockDescriptor + BlockHooks + Send + Sync {
    /// Prepares this block for execution.
    ///
    /// This is called once before the first call to `execute`.
    /// This is where to open ports and allocate resources.
    fn prepare(&mut self, _runtime: &dyn BlockRuntime) -> BlockResult {
        Ok(())
    }

    /// Executes this block's computation asynchronously.
    async fn execute_async(&mut self, runtime: &dyn BlockRuntime) -> BlockResult;
}

impl fmt::Debug for dyn AsyncBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AsyncBlock")
            //.field("name", &self.name())
            //.field("label", &self.label())
            .field("inputs", &self.inputs())
            .field("outputs", &self.outputs())
            .field("parameters", &self.parameters())
            .finish()
    }
}
