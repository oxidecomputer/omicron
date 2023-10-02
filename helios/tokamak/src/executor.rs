// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A "fake" [Executor] implementation, which can respond to host requests.

use crate::shared_byte_queue::SharedByteQueue;

use async_trait::async_trait;
use helios_fusion::{
    log_input, log_output, BoxedChild, BoxedExecutor, Child, ExecutionError,
    Executor, Input, Output, OutputExt,
};
use slog::Logger;
use std::io::{Read, Write};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Handler called when spawning a fake child process
type SpawnFn = dyn FnMut(&mut FakeChild) + Send + Sync;
type BoxedSpawnFn = Box<SpawnFn>;

/// Handler called when awaiting a fake child process
type WaitFn = dyn FnMut(&mut FakeChild) -> Output + Send + Sync;
type BoxedWaitFn = Box<WaitFn>;

pub struct FakeExecutorBuilder {
    log: Logger,
    spawn_handler: Option<BoxedSpawnFn>,
    wait_handler: Option<BoxedWaitFn>,
}

impl FakeExecutorBuilder {
    pub fn new(log: Logger) -> Self {
        Self { log, spawn_handler: None, wait_handler: None }
    }

    pub fn spawn_handler(mut self, f: BoxedSpawnFn) -> Self {
        self.spawn_handler = Some(f);
        self
    }

    pub fn wait_handler(mut self, f: BoxedWaitFn) -> Self {
        self.wait_handler = Some(f);
        self
    }

    /// Convenience function to register the sequence with a [FakeExecutor].
    pub fn with_sequence(mut self, mut sequence: CommandSequence) -> Self {
        self.wait_handler =
            Some(Box::new(move |child: &mut FakeChild| -> Output {
                sequence.execute(child.command())
            }));
        self
    }

    pub fn build(self) -> Arc<FakeExecutor> {
        FakeExecutor::new(
            self.log,
            self.spawn_handler.unwrap_or_else(|| Box::new(|_cmd| ())),
            self.wait_handler
                .unwrap_or_else(|| Box::new(|_cmd| Output::success())),
        )
    }
}

pub(crate) struct FakeExecutorInner {
    log: Logger,
    counter: AtomicU64,
    spawn_handler: Mutex<BoxedSpawnFn>,
    wait_handler: Mutex<BoxedWaitFn>,
}

/// An executor which can expect certain inputs, and respond with specific outputs.
pub struct FakeExecutor {
    inner: Arc<FakeExecutorInner>,
}

impl FakeExecutor {
    pub fn new(
        log: Logger,
        s: BoxedSpawnFn,
        w: BoxedWaitFn,
    ) -> Arc<FakeExecutor> {
        Arc::new(Self {
            inner: Arc::new(FakeExecutorInner {
                log,
                counter: AtomicU64::new(0),
                spawn_handler: Mutex::new(s),
                wait_handler: Mutex::new(w),
            }),
        })
    }

    /// Perform some type coercion to access a commonly-used trait object.
    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }

    fn execute_internal(
        &self,
        command: &Command,
    ) -> Result<Output, ExecutionError> {
        let id = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        log_input(&self.inner.log, id, command);

        let mut child = FakeChild::new(id, command, self.inner.clone());

        // Call our handler function with the caller-provided functions.
        //
        // This performs both the "spawn" and "wait" actions back-to-back.
        self.inner.spawn_handler.lock().unwrap()(&mut child);
        let output = self.inner.wait_handler.lock().unwrap()(&mut child);
        log_output(&self.inner.log, id, &output);

        if !output.status.success() {
            return Err(ExecutionError::from_output(command, &output));
        }
        Ok(output)
    }
}

#[async_trait]
impl Executor for FakeExecutor {
    // NOTE: We aren't actually performing any async operations -- it's up to
    // the caller to control the (synchronous) handlers.
    //
    // However, this still provides testability, while letting the "real
    // executor" make truly async calls while launching processes.
    async fn execute_async(
        &self,
        command: &mut tokio::process::Command,
    ) -> Result<Output, ExecutionError> {
        self.execute_internal(command.as_std())
    }

    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        self.execute_internal(command)
    }

    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError> {
        let id = self.inner.counter.fetch_add(1, Ordering::SeqCst);
        log_input(&self.inner.log, id, command);

        Ok(FakeChild::new(id, command, self.inner.clone()))
    }
}

/// A child spawned by a [FakeExecutor].
pub struct FakeChild {
    id: u64,
    command: Command,
    executor: Arc<FakeExecutorInner>,
    stdin: SharedByteQueue,
    stdout: SharedByteQueue,
    stderr: SharedByteQueue,
}

impl FakeChild {
    fn new(
        id: u64,
        command: &Command,
        executor: Arc<FakeExecutorInner>,
    ) -> Box<Self> {
        // std::process::Command -- somewhat reasonably - doesn't implement Copy
        // or Clone. However, we'd like to be able to reference it in the
        // FakeChild, independently of where it was spawned.
        //
        // Manually copy the relevant pieces of the incoming command.
        let mut copy_command = Command::new(command.get_program());
        copy_command.args(command.get_args());
        copy_command.envs(command.get_envs().filter_map(|(k, v)| {
            if let Some(v) = v {
                Some((k, v))
            } else {
                None
            }
        }));

        Box::new(FakeChild {
            id,
            command: copy_command,
            executor,
            stdin: SharedByteQueue::new(),
            stdout: SharedByteQueue::new(),
            stderr: SharedByteQueue::new(),
        })
    }

    pub fn command(&self) -> &Command {
        &self.command
    }
}

impl Child for FakeChild {
    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        Some(Box::new(self.stdin.clone()))
    }

    fn take_stdout(&mut self) -> Option<Box<dyn Read + Send>> {
        Some(Box::new(self.stdout.clone()))
    }

    fn take_stderr(&mut self) -> Option<Box<dyn Read + Send>> {
        Some(Box::new(self.stderr.clone()))
    }

    fn id(&self) -> u32 {
        self.id.try_into().expect("u32 overflow")
    }

    fn wait(mut self: Box<Self>) -> Result<Output, ExecutionError> {
        let executor = self.executor.clone();
        let output = executor.wait_handler.lock().unwrap()(&mut self);
        log_output(&self.executor.log, self.id, &output);
        if !output.status.success() {
            return Err(ExecutionError::from_output(&self.command, &output));
        }
        Ok(output)
    }
}

type DynamicHandler = Box<dyn FnMut(Input) -> Output + Send + Sync>;

enum HandledCommand {
    Static { input: Input, output: Output },
    Dynamic { handler: DynamicHandler },
}

/// A handler that may be used for setting inputs/outputs to the executor
/// when these commands are known ahead-of-time.
///
/// See: [FakeExecutorBuilder::with_sequence] for integration with a [FakeExecutor].
pub struct CommandSequence {
    expected: Vec<HandledCommand>,
    index: usize,
}

impl CommandSequence {
    pub fn new() -> Self {
        Self { expected: Vec::new(), index: 0 }
    }

    /// Expects a static "input" to exactly produce some "output".
    pub fn expect(&mut self, input: Input, output: Output) {
        self.expected.push(HandledCommand::Static { input, output });
    }

    /// A helper for [Self::expect] which quietly succeeds.
    pub fn expect_ok<S: AsRef<str>>(&mut self, input: S) {
        self.expect(Input::shell(input), Output::success())
    }

    /// A helper for [Self::expect] which quietly fails.
    pub fn expect_fail<S: AsRef<str>>(&mut self, input: S) {
        self.expect(Input::shell(input), Output::failure())
    }

    /// Expects a dynamic handler to be invoked to dynamically
    /// determine the output of this call.
    pub fn expect_dynamic(&mut self, handler: DynamicHandler) {
        self.expected.push(HandledCommand::Dynamic { handler });
    }

    fn execute(&mut self, command: &Command) -> Output {
        let observed_input = Input::from(command);
        let expected = &mut self
            .expected
            .get_mut(self.index)
            .unwrap_or_else(|| panic!("Unexpected command: {observed_input}"));
        self.index += 1;

        match expected {
            HandledCommand::Static { input, output } => {
                assert_eq!(&observed_input, input, "Unexpected input command");
                output.clone()
            }
            HandledCommand::Dynamic { ref mut handler } => {
                handler(observed_input)
            }
        }
    }
}

impl Drop for CommandSequence {
    fn drop(&mut self) {
        let expected = self.expected.len();
        let actual = self.index;
        if actual < expected {
            let next = &self.expected[actual];
            let tip = match next {
                HandledCommand::Static { input, .. } => input.to_string(),
                HandledCommand::Dynamic { .. } => {
                    "<dynamic handler>".to_string()
                }
            };
            let errmsg = format!("Only saw {actual} calls, expected {expected}\nNext would have been: {tip}");
            if !std::thread::panicking() {
                assert!(false, "{errmsg}");
            } else {
                eprintln!("{errmsg}");
            }
        }
    }
}
