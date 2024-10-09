// This is free and unencumbered software released into the public domain.

use crate::{
    prelude::{
        Arc, AtomicBool, AtomicUsize, Box, Cow, Duration, Instant, Ordering, Range, Rc, RefCell,
        ToString, Vec,
    },
    transport::Transport,
    transports::MpscTransport,
    Block, BlockError, BlockResult, BlockRuntime, BoxedBlockType, Port, Process, ProcessID,
    Runtime, System,
};

#[cfg(feature = "tokio")]
use crate::AsyncBlock;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "tokio")]
pub type TokioRuntime = tokio::runtime::Handle;

#[allow(unused)]
pub struct StdRuntime<T: Transport = MpscTransport> {
    pub(crate) transport: Arc<T>,

    #[cfg(feature = "tokio")]
    pub(crate) tokio_handle: Option<TokioRuntime>,

    is_alive: AtomicBool,
    process_id: AtomicUsize,
}

#[allow(unused)]
impl<T: Transport> StdRuntime<T> {
    pub fn new(transport: T) -> Result<Arc<Self>, BlockError> {
        Ok(Arc::new(Self {
            transport: Arc::new(transport),
            #[cfg(feature = "tokio")]
            tokio_handle: None,
            is_alive: AtomicBool::new(true),
            process_id: AtomicUsize::new(1),
        }))
    }

    #[cfg(feature = "tokio")]
    pub fn new_async(transport: T, tokio_handle: TokioRuntime) -> Result<Arc<Self>, BlockError> {
        Ok(Arc::new(Self {
            transport: Arc::new(transport),
            #[cfg(feature = "tokio")]
            tokio_handle: Some(tokio_handle),
            is_alive: AtomicBool::new(true),
            process_id: AtomicUsize::new(1),
        }))
    }
}

impl<T: Transport + 'static> Runtime for Arc<StdRuntime<T>> {
    fn execute_block(&mut self, block: BoxedBlockType) -> BlockResult<Rc<dyn Process>> {
        let std_runtime = Arc::new((*self).clone());
        let block_process = Rc::new(RunningBlock {
            id: self.process_id.fetch_add(1, Ordering::SeqCst),
            runtime: self.clone(),
            handle: RefCell::new(Some(
                std::thread::Builder::new()
                    .name(
                        match &block {
                            BoxedBlockType::Normal(block) => block.name(),
                            #[cfg(feature = "tokio")]
                            BoxedBlockType::Async(block) => block.name(),
                        }
                        .unwrap_or(Cow::Borrowed("<unnamed>"))
                        .to_string(),
                    )
                    .spawn(move || {
                        let mut block = block;
                        std::thread::park();

                        #[cfg(feature = "tokio")]
                        let tokio_handle = std_runtime.tokio_handle.clone();

                        let block_runtime = std_runtime as Arc<dyn BlockRuntime>;
                        let block_runtime_ref = block_runtime.as_ref();

                        match block {
                            BoxedBlockType::Normal(ref mut block) => {
                                let block_mut = block.as_mut();
                                Block::prepare(block_mut, block_runtime_ref)
                                    .and_then(|_| {
                                        <dyn Block>::pre_execute(block_mut, block_runtime_ref)
                                    })
                                    .and_then(|_| Block::execute(block_mut, block_runtime_ref))
                                    .and_then(|_| {
                                        <dyn Block>::post_execute(block_mut, block_runtime_ref)
                                    })
                            }
                            #[cfg(feature = "tokio")]
                            BoxedBlockType::Async(ref mut block) => {
                                if let Some(handle) = tokio_handle {
                                    let block_mut = block.as_mut();
                                    AsyncBlock::prepare(block_mut, block_runtime_ref)
                                        .and_then(|_| {
                                            <dyn AsyncBlock>::pre_execute(
                                                block_mut,
                                                block_runtime_ref,
                                            )
                                        })
                                        .and_then(|_| {
                                            let future = <dyn AsyncBlock>::execute_async(
                                                block_mut,
                                                block_runtime_ref,
                                            );

                                            handle.block_on(future)
                                        })
                                        .and_then(|_| {
                                            <dyn AsyncBlock>::post_execute(
                                                block_mut,
                                                block_runtime_ref,
                                            )
                                        })
                                } else {
                                    panic!("Tried to run async block without tokio runtime!");
                                }
                            }
                        }
                    })
                    .unwrap(),
            )),
        });
        block_process
            .handle
            .borrow()
            .as_ref()
            .unwrap()
            .thread()
            .unpark();
        Ok(block_process)
    }

    fn execute<X: Transport + Default>(
        &mut self,
        mut system: System<X>,
    ) -> BlockResult<Rc<dyn Process>> {
        let mut system_process = RunningSystem {
            id: self.process_id.fetch_add(1, Ordering::SeqCst),
            runtime: self.clone(),
            transport: self.transport.clone(),
            blocks: Vec::new(),
        };

        while let Some(block) = system.blocks.pop_front() {
            system_process.blocks.push(self.execute_block(block)?);
        }

        Ok(Rc::new(system_process))
    }
}

impl<T: Transport> BlockRuntime for Arc<StdRuntime<T>> {
    fn is_alive(&self) -> bool {
        self.is_alive.load(Ordering::SeqCst)
    }

    fn sleep_for(&self, duration: Duration) -> BlockResult {
        #[cfg(feature = "std")]
        std::thread::sleep(duration);
        #[cfg(not(feature = "std"))]
        unimplemented!("std::thread::sleep requires the 'std' feature");
        Ok(())
    }

    fn sleep_until(&self, _instant: Instant) -> BlockResult {
        todo!() // TODO
    }

    fn wait_for(&self, port: &dyn Port) -> BlockResult {
        loop {
            if !self.is_alive() {
                return Err(BlockError::Terminated);
            }
            if port.is_connected() {
                return Ok(());
            }
            self.yield_now()?;
        }
    }

    fn yield_now(&self) -> Result<(), BlockError> {
        #[cfg(feature = "std")]
        std::thread::yield_now();
        #[cfg(not(feature = "std"))]
        unimplemented!("std::thread::yield_now requires the 'std' feature");
        Ok(())
    }

    fn random_duration(&self, range: Range<Duration>) -> Duration {
        #[cfg(all(feature = "std", feature = "rand"))]
        {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let low = range.start.as_nanos() as u64;
            let high = range.end.as_nanos() as u64;
            Duration::from_nanos(rng.gen_range(low..high))
        }
        #[cfg(not(all(feature = "std", feature = "rand")))]
        {
            drop(range);
            let mut _rng = todo!();
        }
    }
}

#[allow(unused)]
struct RunningBlock<T: Transport> {
    id: ProcessID,
    runtime: Arc<StdRuntime<T>>,
    handle: RefCell<Option<std::thread::JoinHandle<BlockResult>>>,
}

#[allow(unused)]
impl<T: Transport> RunningBlock<T> {
    //fn thread(&self) -> Option<&std::thread::Thread> {
    //    self.handle.borrow().as_ref().map(|handle| handle.thread())
    //}
}

impl<T: Transport> Process for RunningBlock<T> {
    fn id(&self) -> ProcessID {
        self.id
    }

    fn is_alive(&self) -> bool {
        self.handle
            .borrow()
            .as_ref()
            .map(|handle| !handle.is_finished())
            .unwrap_or(false)
    }

    fn join(&self) -> BlockResult {
        let handle = self.handle.take().unwrap();
        handle
            .join()
            .map_err(<Box<dyn core::any::Any + Send>>::from)?
    }
}

#[allow(unused)]
struct RunningSystem<T: Transport> {
    id: ProcessID,
    runtime: Arc<StdRuntime<T>>,
    transport: Arc<T>,
    blocks: Vec<Rc<dyn Process>>,
}

impl<T: Transport> Process for RunningSystem<T> {
    fn id(&self) -> ProcessID {
        self.id
    }

    fn is_alive(&self) -> bool {
        self.blocks.iter().any(|block| block.is_alive())
    }

    fn join(&self) -> BlockResult {
        for block in self.blocks.iter() {
            block.join()?;
        }
        Ok(())
    }
}
