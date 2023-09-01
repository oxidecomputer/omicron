// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A "real" [Executor] implementation, which sends commands to the host.

use helios_fusion::{
    log_input, log_output, AsCommandStr, BoxedChild, BoxedExecutor, Child,
    ExecutionError, Executor, Input, Output,
};

use async_trait::async_trait;
use slog::{error, Logger};
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Implements [Executor] by running commands against the host system.
pub struct HostExecutor {
    log: slog::Logger,
    counter: std::sync::atomic::AtomicU64,
}

impl HostExecutor {
    pub fn new(log: Logger) -> Arc<Self> {
        Arc::new(Self { log, counter: AtomicU64::new(0) })
    }

    pub fn as_executor(self: Arc<Self>) -> BoxedExecutor {
        self
    }

    fn prepare(&self, command: &Command) -> u64 {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        log_input(&self.log, id, command);
        id
    }

    fn finalize(
        &self,
        command: &Command,
        id: u64,
        output: Output,
    ) -> Result<Output, ExecutionError> {
        log_output(&self.log, id, &output);
        if !output.status.success() {
            return Err(ExecutionError::from_output(command, &output));
        }
        Ok(output)
    }
}

#[async_trait]
impl Executor for HostExecutor {
    async fn execute_async(
        &self,
        command: &mut tokio::process::Command,
    ) -> Result<Output, ExecutionError> {
        let id = self.prepare(command.as_std());
        let output = command.output().await.map_err(|err| {
            error!(self.log, "Could not start program asynchronously!"; "id" => id);
            ExecutionError::ExecutionStart {
                command: Input::from(command.as_std()).to_string(),
                err,
            }
        })?;
        self.finalize(command.as_std(), id, output)
    }

    fn execute(&self, command: &mut Command) -> Result<Output, ExecutionError> {
        let id = self.prepare(command);
        let output = command.output().map_err(|err| {
            error!(self.log, "Could not start program!"; "id" => id);
            ExecutionError::ExecutionStart {
                command: Input::from(&*command).to_string(),
                err,
            }
        })?;
        self.finalize(command, id, output)
    }

    fn spawn(
        &self,
        command: &mut Command,
    ) -> Result<BoxedChild, ExecutionError> {
        let command_str = (&*command).into_str();
        Ok(Box::new(SpawnedChild {
            child: Some(
                command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|err| ExecutionError::ExecutionStart {
                    command: command_str.clone(),
                    err,
                })?,
            ),
            command_str,
        }))
    }
}

/// A real, host-controlled child process
pub struct SpawnedChild {
    command_str: String,
    child: Option<std::process::Child>,
}

impl Child for SpawnedChild {
    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        self.child
            .as_mut()?
            .stdin
            .take()
            .map(|s| Box::new(s) as Box<dyn Write + Send>)
    }

    fn take_stdout(&mut self) -> Option<Box<dyn Read + Send>> {
        self.child
            .as_mut()?
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>)
    }

    fn take_stderr(&mut self) -> Option<Box<dyn Read + Send>> {
        self.child
            .as_mut()?
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>)
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("No child").id()
    }

    fn wait(mut self: Box<Self>) -> Result<Output, ExecutionError> {
        let output =
            self.child.take().unwrap().wait_with_output().map_err(|err| {
                ExecutionError::ExecutionStart {
                    command: self.command_str.clone(),
                    err,
                }
            })?;

        if !output.status.success() {
            return Err(ExecutionError::from_output(self.command_str, &output));
        }

        Ok(output)
    }
}
