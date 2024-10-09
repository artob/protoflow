// This is free and unencumbered software released into the public domain.

use crate::{
    prelude::{Box, Rc},
    Block, BlockResult, BoxedBlockType, Process, System, Transport,
};

pub trait Runtime {
    //fn execute<T: Block + 'static>(&mut self, block: T) -> BlockResult<Rc<dyn Process>> {
    //    self.execute_block(Box::new(block))
    //}

    fn execute_block(&mut self, block: BoxedBlockType) -> BlockResult<Rc<dyn Process>>;

    fn execute<X: Transport + Default>(
        &mut self,
        system: System<X>,
    ) -> BlockResult<Rc<dyn Process>>;
}
