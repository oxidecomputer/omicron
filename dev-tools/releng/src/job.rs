// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use fs_err::tokio::File;
use futures::stream::FuturesUnordered;
use futures::stream::TryStreamExt;
use slog::info;
use slog::Logger;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Semaphore;

use crate::cmd::CommandExt;

pub(crate) struct Jobs {
    logger: Logger,
    permits: Arc<Semaphore>,
    log_dir: Utf8PathBuf,
    map: HashMap<String, Job>,
}

struct Job {
    future: Pin<Box<dyn Future<Output = Result<()>>>>,
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

    pub(crate) fn push<F>(
        &mut self,
        name: impl AsRef<str>,
        future: F,
    ) -> Selector<'_>
    where
        F: Future<Output = Result<()>> + 'static,
    {
        let name = name.as_ref().to_owned();
        assert!(!self.map.contains_key(&name), "duplicate job name {}", name);
        self.map.insert(
            name.clone(),
            Job {
                future: Box::pin(run_job(
                    self.logger.clone(),
                    self.permits.clone(),
                    name.clone(),
                    future,
                )),
                wait_for: Vec::new(),
                notify: Vec::new(),
            },
        );
        Selector { jobs: self, name }
    }

    pub(crate) fn push_command(
        &mut self,
        name: impl AsRef<str>,
        command: &mut Command,
    ) -> Selector<'_> {
        let name = name.as_ref().to_owned();
        assert!(!self.map.contains_key(&name), "duplicate job name {}", name);
        self.map.insert(
            name.clone(),
            Job {
                future: Box::pin(spawn_with_output(
                    // terrible hack to deal with the `Command` builder
                    // returning &mut
                    std::mem::replace(command, Command::new("false")),
                    self.logger.clone(),
                    self.permits.clone(),
                    name.clone(),
                    self.log_dir.join(&name).with_extension("log"),
                )),
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

async fn run_job<F>(
    logger: Logger,
    permits: Arc<Semaphore>,
    name: String,
    future: F,
) -> Result<()>
where
    F: Future<Output = Result<()>> + 'static,
{
    let _ = permits.acquire_owned().await?;

    info!(logger, "[{}] running task", name);
    let start = Instant::now();
    let result = future.await;
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
    mut command: Command,
    logger: Logger,
    permits: Arc<Semaphore>,
    name: String,
    log_path: Utf8PathBuf,
) -> Result<()> {
    let _ = permits.acquire_owned().await?;

    let log_file_1 = File::create(log_path).await?;
    let log_file_2 = log_file_1.try_clone().await?;

    info!(logger, "[{}] running: {}", name, command.to_string());
    let start = Instant::now();
    let mut child = command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to exec `{}`", command.to_string()))?;

    let stdout = reader(
        &name,
        child.stdout.take().unwrap(),
        tokio::io::stdout(),
        log_file_1,
    );
    let stderr = reader(
        &name,
        child.stderr.take().unwrap(),
        tokio::io::stderr(),
        log_file_2,
    );
    match tokio::try_join!(child.wait(), stdout, stderr) {
        Ok((status, (), ())) => {
            let result = command.check_status(status);
            info_or_error!(
                logger,
                result,
                "[{}] process exited with {} ({:?})",
                name,
                status,
                Instant::now().saturating_duration_since(start)
            );
            result
        }
        Err(err) => Err(err).with_context(|| {
            format!("I/O error while waiting for job {:?} to complete", name)
        }),
    }
}

async fn reader(
    name: &str,
    reader: impl AsyncRead + Unpin,
    mut terminal_writer: impl AsyncWrite + Unpin,
    logfile_writer: File,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(reader);
    let mut logfile_writer = tokio::fs::File::from(logfile_writer);
    let mut buf = format!("[{:>16}] ", name).into_bytes();
    let prefix_len = buf.len();
    loop {
        buf.truncate(prefix_len);
        let size = reader.read_until(b'\n', &mut buf).await?;
        if size == 0 {
            return Ok(());
        }
        terminal_writer.write_all(&buf).await?;
        logfile_writer.write_all(&buf[prefix_len..]).await?;
    }
}
