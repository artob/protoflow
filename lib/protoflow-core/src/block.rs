// This is free and unencumbered software released into the public domain.

use crate::prelude::{fmt, Box};
use crate::{BlockDescriptor, BlockResult, BlockRuntime};

#[cfg(feature = "tokio")]
use crate::AsyncBlock;

/// A machine-readable identifier for a block in a system.
///
/// Only valid within the scope of that system.
pub type BlockID = usize;

pub type BoxedBlock = Box<dyn Block>;
#[cfg(feature = "tokio")]
pub type BoxedAsyncBlock = Box<dyn AsyncBlock>;

#[derive(Debug)]
pub enum BoxedBlockType {
    Normal(BoxedBlock),
    #[cfg(feature = "tokio")]
    Async(BoxedAsyncBlock),
}

/// A block is an autonomous unit of computation in a system.
pub trait Block: AsBlock + BlockDescriptor + BlockHooks + Send + Sync {
    /// Prepares this block for execution.
    ///
    /// This is called once before the first call to `execute`.
    /// This is where to open ports and allocate resources.
    fn prepare(&mut self, _runtime: &dyn BlockRuntime) -> BlockResult {
        Ok(())
    }

    /// Executes this block's computation.
    fn execute(&mut self, runtime: &dyn BlockRuntime) -> BlockResult;
}

/// Hooks for `#[derive(Block)]` to tap into block execution.
#[doc(hidden)]
pub trait BlockHooks {
    fn pre_execute(&mut self, _runtime: &dyn BlockRuntime) -> BlockResult {
        Ok(()) // implemented by protoflow_derive
    }

    fn post_execute(&mut self, _runtime: &dyn BlockRuntime) -> BlockResult {
        Ok(()) // implemented by protoflow_derive
    }
}

pub trait AsBlock {
    fn as_block(&self) -> &dyn Block;
}

impl<T: Block + Sized> AsBlock for T {
    fn as_block(&self) -> &dyn Block {
        self
    }
}

impl fmt::Debug for dyn Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Block")
            //.field("name", &self.name())
            //.field("label", &self.label())
            .field("inputs", &self.inputs())
            .field("outputs", &self.outputs())
            .field("parameters", &self.parameters())
            .finish()
    }
}
