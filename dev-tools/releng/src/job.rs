// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A quick-and-dirty job runner.
//!
//! Jobs are async functions given a name. All jobs must be described before the
//! jobs can be run (`Jobs::run_all` consumes the job runner). Jobs can depend
//! on other jobs, which is implemented via `tokio::sync::oneshot` channels; a
//! completed job sends a message to all registered receivers, which are waiting
//! on the messages in order to run. This essentially creates a DAG, except
//! instead of us having to keep track of it, we make it Tokio's problem.
//!
//! A `tokio::sync::Semaphore` is used to restrict the number of jobs to
//! `std::thread::available_parallelism`, except for a hardcoded list of
//! prioritized job names that are allowed to ignore this.

use std::collections::HashMap;
use std::future::Future;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use fs_err::tokio::File;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::stream::FuturesUnordered;
use futures::stream::TryStreamExt;
use slog::info;
use slog::Logger;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Semaphore;

use crate::cmd::Command;

// We want these two jobs to run without delay because they take the longest
// amount of time, so we allow them to run without taking a permit first.
const PERMIT_NOT_REQUIRED: [&str; 2] = ["host-package", "host-image"];

pub(crate) struct Jobs {
    logger: Logger,
    permits: Arc<Semaphore>,
    log_dir: Utf8PathBuf,
    map: HashMap<String, Job>,
}

struct Job {
    future: BoxFuture<'static, Result<()>>,
    wait_for: Vec<oneshot::Receiver<()>>,
    notify: Vec<oneshot::Sender<()>>,
}

pub(crate) struct Selector<'a> {
    jobs: &'a mut Jobs,
    name: String,
}

impl Jobs {
    pub(crate) fn new(
        logger: &Logger,
        permits: Arc<Semaphore>,
        log_dir: &Utf8Path,
    ) -> Jobs {
        Jobs {
            logger: logger.clone(),
            permits,
            log_dir: log_dir.to_owned(),
            map: HashMap::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        name: impl AsRef<str>,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Selector<'_> {
        let name = name.as_ref().to_owned();
        assert!(!self.map.contains_key(&name), "duplicate job name {}", name);
        self.map.insert(
            name.clone(),
            Job {
                future: run_job(
                    self.logger.clone(),
                    self.permits.clone(),
                    name.clone(),
                    future,
                )
                .boxed(),
                wait_for: Vec::new(),
                notify: Vec::new(),
            },
        );
        Selector { jobs: self, name }
    }

    pub(crate) fn push_command(
        &mut self,
        name: impl AsRef<str>,
        command: Command,
    ) -> Selector<'_> {
        let name = name.as_ref().to_owned();
        assert!(!self.map.contains_key(&name), "duplicate job name {}", name);
        self.map.insert(
            name.clone(),
            Job {
                future: spawn_with_output(
                    command,
                    self.logger.clone(),
                    self.permits.clone(),
                    name.clone(),
                    self.log_dir.join(&name).with_extension("log"),
                )
                .boxed(),
                wait_for: Vec::new(),
                notify: Vec::new(),
            },
        );
        Selector { jobs: self, name }
    }

    pub(crate) fn select(&mut self, name: impl AsRef<str>) -> Selector<'_> {
        Selector { jobs: self, name: name.as_ref().to_owned() }
    }

    pub(crate) async fn run_all(self) -> Result<()> {
        self.map
            .into_values()
            .map(Job::run)
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>()
            .await
    }
}

impl Job {
    async fn run(self) -> Result<()> {
        let result: Result<(), RecvError> = self
            .wait_for
            .into_iter()
            .collect::<FuturesUnordered<_>>()
            .try_collect::<()>()
            .await;
        result.map_err(|_| anyhow!("dependency failed"))?;

        self.future.await?;
        for sender in self.notify {
            // Ignore the error here -- the only reason we should fail to send
            // our message is if a task has failed or the user hit Ctrl-C, at
            // which point a bunch of error logging is not particularly useful.
            sender.send(()).ok();
        }
        Ok(())
    }
}

impl<'a> Selector<'a> {
    #[track_caller]
    pub(crate) fn after(self, other: impl AsRef<str>) -> Self {
        let (sender, receiver) = oneshot::channel();
        self.jobs
            .map
            .get_mut(&self.name)
            .expect("invalid job name")
            .wait_for
            .push(receiver);
        self.jobs
            .map
            .get_mut(other.as_ref())
            .expect("invalid job name")
            .notify
            .push(sender);
        self
    }
}

macro_rules! info_or_error {
    ($logger:expr, $result:expr, $($tt:tt)*) => {
        if $result.is_ok() {
            ::slog::info!($logger, $($tt)*);
        } else {
            ::slog::error!($logger, $($tt)*);
        }
    };
}

async fn run_job(
    logger: Logger,
    permits: Arc<Semaphore>,
    name: String,
    future: impl Future<Output = Result<()>> + Send + 'static,
) -> Result<()> {
    if !PERMIT_NOT_REQUIRED.contains(&name.as_str()) {
        let _ = permits.acquire_owned().await?;
    }

    info!(logger, "[{}] running task", name);
    let start = Instant::now();
    let result = tokio::spawn(future).await?;
    let duration = Instant::now().saturating_duration_since(start);
    info_or_error!(
        logger,
        result,
        "[{}] task {} ({:?})",
        name,
        if result.is_ok() { "succeeded" } else { "failed" },
        duration
    );
    result
}

async fn spawn_with_output(
    command: Command,
    logger: Logger,
    permits: Arc<Semaphore>,
    name: String,
    log_path: Utf8PathBuf,
) -> Result<()> {
    if !PERMIT_NOT_REQUIRED.contains(&name.as_str()) {
        let _ = permits.acquire_owned().await?;
    }

    let (command_desc, mut command) = command.into_parts();

    let log_file_1 = File::create(log_path).await?;
    let log_file_2 = log_file_1.try_clone().await?;

    info!(logger, "[{}] running: {}", name, command_desc);
    let start = Instant::now();
    let mut child = command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to exec `{}`", command_desc))?;

    let stdout = spawn_reader(
        format!("[{:>16}] ", name),
        child.stdout.take().unwrap(),
        tokio::io::stdout(),
        log_file_1,
    );
    let stderr = spawn_reader(
        format!("[{:>16}] ", name),
        child.stderr.take().unwrap(),
        tokio::io::stderr(),
        log_file_2,
    );

    let status = child.wait().await.with_context(|| {
        format!("I/O error while waiting for job {:?} to complete", name)
    })?;
    let result = command_desc.check_status(status);
    info_or_error!(
        logger,
        result,
        "[{}] process exited with {} ({:?})",
        name,
        status,
        Instant::now().saturating_duration_since(start)
    );

    // bubble up any errors from `spawn_reader`
    stdout.await??;
    stderr.await??;

    result
}

fn spawn_reader(
    prefix: String,
    reader: impl AsyncRead + Send + Unpin + 'static,
    mut terminal_writer: impl AsyncWrite + Send + Unpin + 'static,
    logfile_writer: File,
) -> tokio::task::JoinHandle<Result<()>> {
    let mut reader = BufReader::new(reader);
    let mut logfile_writer = tokio::fs::File::from(logfile_writer);
    let mut buf = prefix.into_bytes();
    let prefix_len = buf.len();
    tokio::spawn(async move {
        loop {
            buf.truncate(prefix_len);
            // We have no particular control over the output from the child
            // processes we run, so we read until a newline character without
            // relying on valid UTF-8 output.
            let size = reader.read_until(b'\n', &mut buf).await?;
            if size == 0 {
                return Ok(());
            }
            terminal_writer.write_all(&buf).await?;
            logfile_writer.write_all(&buf[prefix_len..]).await?;
        }
    })
}
