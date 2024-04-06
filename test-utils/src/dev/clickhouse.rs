// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for managing ClickHouse during development

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Context};
use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::{Builder, Utf8TempDir};
use dropshot::test_util::{log_prefix_for_test, LogContext};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    time::{sleep, Instant},
};

use crate::dev::poll;

// Timeout used when starting up ClickHouse subprocess.
const CLICKHOUSE_TIMEOUT: Duration = Duration::from_secs(30);

// Timeout used when starting a ClickHouse keeper subprocess.
const CLICKHOUSE_KEEPER_TIMEOUT: Duration = Duration::from_secs(30);

// The string to look for in a keeper log file that indicates that the server
// is ready.
const KEEPER_READY: &'static str = "Server initialized, waiting for quorum";

// The string to look for in a clickhouse log file that indicates that the
// server is ready.
const CLICKHOUSE_READY: &'static str =
    "<Information> Application: Ready for connections";

// The string to look for in a clickhouse log file when trying to determine the
// port number on which it is listening.
const CLICKHOUSE_PORT: &'static str = "Application: Listening for http://[::1]";

/// A `ClickHouseInstance` is used to start and manage a ClickHouse single node server process.
#[derive(Debug)]
pub struct ClickHouseInstance {
    // Directory in which all data, logs, etc are stored.
    data_dir: Option<ClickHouseDataDir>,
    data_path: Utf8PathBuf,
    // The HTTP port the server is listening on
    port: u16,
    // The address the server is listening on
    pub address: SocketAddr,
    // Full list of command-line arguments
    args: Vec<String>,
    // Subprocess handle
    child: Option<tokio::process::Child>,
}

#[derive(Debug, Error)]
pub enum ClickHouseError {
    #[error("Failed to open ClickHouse log file")]
    Io(#[from] std::io::Error),

    #[error("Invalid ClickHouse port number")]
    InvalidPort,

    #[error("Invalid ClickHouse listening address")]
    InvalidAddress,

    #[error("Invalid ClickHouse Keeper ID")]
    InvalidKeeperId,

    #[error("Failed to detect ClickHouse subprocess within timeout")]
    Timeout,
}

impl ClickHouseInstance {
    /// Start a new single node ClickHouse server on the given IPv6 port.
    pub async fn new_single_node(
        logctx: &LogContext,
        port: u16,
    ) -> Result<Self, anyhow::Error> {
        let data_dir = ClickHouseDataDir::new(logctx)?;
        let args = vec![
            "server".to_string(),
            "--log-file".to_string(),
            data_dir.log_path().to_string(),
            "--errorlog-file".to_string(),
            data_dir.err_log_path().to_string(),
            "--".to_string(),
            "--http_port".to_string(),
            format!("{}", port),
            "--path".to_string(),
            data_dir.datastore_path().to_string(),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            // ClickHouse internally tees its logs to a file, so we throw away
            // std{in,out,err}
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .current_dir(data_dir.cwd_path())
            // By default ClickHouse forks a child if it's been explicitly
            // requested via the following environment variable, _or_ if it's
            // not attached to a TTY. Avoid this behavior, so that we can
            // correctly deliver SIGINT. The "watchdog" masks SIGINT, meaning
            // we'd have to deliver that to the _child_, which is more
            // complicated.
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .spawn()
            .with_context(|| {
                format!("failed to spawn `clickhouse` (with args: {:?})", &args)
            })?;

        let data_path = data_dir.root_path().to_path_buf();
        let port = wait_for_port(data_dir.log_path()).await?;

        let address = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port);

        Ok(Self {
            data_dir: Some(data_dir),
            data_path,
            port,
            address,
            args,
            child: Some(child),
        })
    }

    /// Start a new replicated ClickHouse server on the given IPv6 port.
    pub async fn new_replicated(
        logctx: &LogContext,
        port: u16,
        tcp_port: u16,
        interserver_port: u16,
        name: String,
        r_number: String,
        config_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        let data_dir = ClickHouseDataDir::new(logctx)?;
        let args = vec![
            "server".to_string(),
            "--config-file".to_string(),
            format!("{}", config_path.display()),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .current_dir(data_dir.cwd_path())
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .env("CH_LOG", data_dir.log_path())
            .env("CH_ERROR_LOG", data_dir.err_log_path())
            .env("CH_REPLICA_DISPLAY_NAME", name)
            .env("CH_LISTEN_ADDR", "::")
            .env("CH_LISTEN_PORT", port.to_string())
            .env("CH_TCP_PORT", tcp_port.to_string())
            .env("CH_INTERSERVER_PORT", interserver_port.to_string())
            .env("CH_DATASTORE", data_dir.datastore_path())
            .env("CH_TMP_PATH", data_dir.tmp_path())
            .env("CH_USER_FILES_PATH", data_dir.user_files_path())
            .env("CH_USER_LOCAL_DIR", data_dir.access_path())
            .env("CH_FORMAT_SCHEMA_PATH", data_dir.format_schemas_path())
            .env("CH_REPLICA_NUMBER", r_number)
            .env("CH_REPLICA_HOST_01", "::1")
            .env("CH_REPLICA_HOST_02", "::1")
            // ClickHouse servers have a small quirk, where when setting the
            // keeper hosts as IPv6 localhost addresses in the replica
            // configuration file, they must be wrapped in square brackets
            // Otherwise, when running any query, a "Service not found" error
            // appears.
            .env("CH_KEEPER_HOST_01", "[::1]")
            .env("CH_KEEPER_HOST_02", "[::1]")
            .env("CH_KEEPER_HOST_03", "[::1]")
            .spawn()
            .with_context(|| {
                format!("failed to spawn `clickhouse` (with args: {:?})", &args)
            })?;

        let data_path = data_dir.root_path().to_path_buf();
        let address = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port);

        let result = wait_for_ready(
            data_dir.log_path(),
            CLICKHOUSE_TIMEOUT,
            CLICKHOUSE_READY,
        )
        .await;
        match result {
            Ok(()) => Ok(Self {
                data_dir: Some(data_dir),
                data_path,
                port,
                address,
                args,
                child: Some(child),
            }),
            Err(e) => Err(e),
        }
    }

    /// Start a new ClickHouse keeper on the given IPv6 port.
    pub async fn new_keeper(
        logctx: &LogContext,
        port: u16,
        k_id: u16,
        config_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        // We assume that only 3 keepers will be run, and the ID of the keeper
        // can only be one of "1", "2" or "3". This is to avoid having to pass
        // the IDs of the other keepers as part of the function's parameters.
        if ![1, 2, 3].contains(&k_id) {
            return Err(ClickHouseError::InvalidKeeperId.into());
        }
        let data_dir = ClickHouseDataDir::new(logctx)?;

        let args = vec![
            "keeper".to_string(),
            "--config-file".to_string(),
            format!("{}", config_path.display()),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .env("CH_LOG", data_dir.keeper_log_path())
            .env("CH_ERROR_LOG", data_dir.keeper_err_log_path())
            .env("CH_LISTEN_ADDR", "::")
            .env("CH_LISTEN_PORT", port.to_string())
            .env("CH_KEEPER_ID_CURRENT", k_id.to_string())
            .env("CH_DATASTORE", data_dir.datastore_path())
            .env("CH_LOG_STORAGE_PATH", data_dir.keeper_log_storage_path())
            .env(
                "CH_SNAPSHOT_STORAGE_PATH",
                data_dir.keeper_snapshot_storage_path(),
            )
            .env("CH_KEEPER_ID_01", "1")
            .env("CH_KEEPER_ID_02", "2")
            .env("CH_KEEPER_ID_03", "3")
            .env("CH_KEEPER_HOST_01", "::1")
            .env("CH_KEEPER_HOST_02", "::1")
            .env("CH_KEEPER_HOST_03", "::1")
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn `clickhouse keeper` (with args: {:?})",
                    &args
                )
            })?;

        let data_path = data_dir.root_path().to_path_buf();
        let address = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port);

        let result = wait_for_ready(
            data_dir.keeper_log_path(),
            CLICKHOUSE_KEEPER_TIMEOUT,
            KEEPER_READY,
        )
        .await;
        match result {
            Ok(()) => Ok(Self {
                data_dir: Some(data_dir),
                data_path,
                port,
                address,
                args,
                child: Some(child),
            }),
            Err(e) => Err(e),
        }
    }

    /// Wait for the ClickHouse server process to shutdown, after it's been killed.
    pub async fn wait_for_shutdown(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.wait().await?;
        }
        self.cleanup().await
    }

    /// Kill the ClickHouse server process and cleanup the data directory.
    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.start_kill().context("Sending SIGKILL to child")?;
            child.wait().await.context("waiting for child")?;
        }
        if let Some(dir) = self.data_dir.take() {
            dir.close_clean()?;
        }
        Ok(())
    }

    /// Return the full path to the directory used for the server's data.
    pub fn data_path(&self) -> &Utf8Path {
        &self.data_path
    }

    /// Return the command-line used to start the ClickHouse server process
    pub fn cmdline(&self) -> &Vec<String> {
        &self.args
    }

    /// Return the child PID, if any
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.id())
    }

    /// Return the HTTP port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Debug)]
struct ClickHouseDataDir {
    dir: Utf8TempDir,
}

impl ClickHouseDataDir {
    fn new(logctx: &LogContext) -> Result<Self, anyhow::Error> {
        let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());

        let dir = Builder::new()
            .prefix(&format!("{prefix}-clickhouse-"))
            .tempdir_in(parent_dir)
            .context("failed to create tempdir for ClickHouse data")?;

        let ret = Self { dir };
        // Create some of the directories. We specify a custom cwd because
        // clickhouse doesn't always respect the --path flag and stores, in
        // particular, files in `preprocessed_configs/`.
        std::fs::create_dir(ret.datastore_path())
            .context("failed to create datastore directory")?;
        std::fs::create_dir(ret.cwd_path())
            .context("failed to create cwd directory")?;
        std::fs::create_dir(ret.keeper_log_storage_path())
            .context("failed to create keeper log directory")?;
        std::fs::create_dir(ret.keeper_snapshot_storage_path())
            .context("failed to create keeper snapshot directory")?;

        Ok(ret)
    }

    fn root_path(&self) -> &Utf8Path {
        self.dir.path()
    }

    fn datastore_path(&self) -> Utf8PathBuf {
        self.dir.path().join("datastore/")
    }

    fn cwd_path(&self) -> Utf8PathBuf {
        self.dir.path().join("cwd/")
    }

    fn log_path(&self) -> Utf8PathBuf {
        self.dir.path().join("clickhouse-server.log")
    }

    fn err_log_path(&self) -> Utf8PathBuf {
        self.dir.path().join("clickhouse-server.errlog")
    }

    fn tmp_path(&self) -> Utf8PathBuf {
        self.dir.path().join("tmp/")
    }

    fn user_files_path(&self) -> Utf8PathBuf {
        self.dir.path().join("user_files/")
    }

    fn access_path(&self) -> Utf8PathBuf {
        self.dir.path().join("access/")
    }

    fn format_schemas_path(&self) -> Utf8PathBuf {
        self.dir.path().join("format_schemas/")
    }

    fn keeper_log_path(&self) -> Utf8PathBuf {
        self.dir.path().join("clickhouse-keeper.log")
    }

    fn keeper_err_log_path(&self) -> Utf8PathBuf {
        self.dir.path().join("clickhouse-keeper.errlog")
    }

    fn keeper_log_storage_path(&self) -> Utf8PathBuf {
        // ClickHouse keeper chokes on log paths having trailing slashes,
        // producing messages like:
        //
        // <Error> Application: DB::Exception: Invalid changelog
        // /tmp/clickhouse-lSv3IU/log/uuid
        //
        // So we don't include a trailing slash for this specific path.
        self.dir.path().join("log")
    }

    fn keeper_snapshot_storage_path(&self) -> Utf8PathBuf {
        self.dir.path().join("snapshots/")
    }

    fn close_clean(self) -> Result<(), anyhow::Error> {
        self.dir.close().context("failed to delete ClickHouse data dir")
    }

    /// Closes this data directory during a test failure, or other unclean
    /// shutdown.
    ///
    /// Removes all files except those in any of the log directories.
    fn close_unclean(self) -> Result<(), anyhow::Error> {
        let keep_prefixes = [
            self.log_path(),
            self.err_log_path(),
            self.keeper_log_path(),
            self.keeper_err_log_path(),
            self.keeper_log_storage_path(),
        ];
        // Persist this temporary directory since we're going to be doing the
        // cleanup ourselves.
        let dir = self.dir.into_path();

        let mut error_paths = BTreeMap::new();
        // contents_first = true ensures that we delete inner files before
        // outer directories.
        for entry in walkdir::WalkDir::new(&dir).contents_first(true) {
            match entry {
                Ok(entry) => {
                    // If it matches any of the prefixes, skip it.
                    if keep_prefixes
                        .iter()
                        .any(|prefix| entry.path().starts_with(prefix))
                    {
                        continue;
                    }
                    if entry.file_type().is_dir() {
                        if let Err(error) = std::fs::remove_dir(entry.path()) {
                            // Ignore ENOTEMPTY errors because they're likely
                            // generated from parents of files we've kept, or
                            // were unable to delete for other reasons.
                            if error.raw_os_error() != Some(libc::ENOTEMPTY) {
                                error_paths.insert(
                                    entry.path().to_owned(),
                                    anyhow!(error),
                                );
                            }
                        }
                    } else {
                        if let Err(error) = std::fs::remove_file(entry.path()) {
                            error_paths.insert(
                                entry.path().to_owned(),
                                anyhow!(error),
                            );
                        }
                    }
                }
                Err(error) => {
                    if let Some(path) = error.path() {
                        error_paths.insert(path.to_owned(), anyhow!(error));
                    }
                }
            }
        }

        // Are there any error paths?
        if !error_paths.is_empty() {
            let error_paths = error_paths
                .into_iter()
                .map(|(path, error)| format!("- {}: {}", path.display(), error))
                .collect::<Vec<_>>();
            let error_paths = error_paths.join("\n");
            return Err(anyhow!(
                "failed to clean up ClickHouse data dir:\n{}",
                error_paths
            ));
        }

        Ok(())
    }
}

impl Drop for ClickHouseInstance {
    fn drop(&mut self) {
        if self.child.is_some() || self.data_dir.is_some() {
            eprintln!(
                "WARN: dropped ClickHouseInstance without cleaning it up first \
                (there may still be a child process running and a \
                temporary directory leaked)"
            );
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
            if let Some(dir) = self.data_dir.take() {
                if let Err(e) = dir.close_unclean() {
                    eprintln!("{}", e);
                }
            }
        }
    }
}

/// A `ClickHouseCluster` is used to start and manage a 2 replica 3 keeper ClickHouse cluster.
#[derive(Debug)]
pub struct ClickHouseCluster {
    pub replica_1: ClickHouseInstance,
    pub replica_2: ClickHouseInstance,
    pub keeper_1: ClickHouseInstance,
    pub keeper_2: ClickHouseInstance,
    pub keeper_3: ClickHouseInstance,
    pub replica_config_path: PathBuf,
    pub keeper_config_path: PathBuf,
}

impl ClickHouseCluster {
    pub async fn new(
        logctx: &LogContext,
        replica_config: PathBuf,
        keeper_config: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        // Start all Keeper coordinator nodes
        let keeper_amount = 3;
        let mut keepers =
            Self::new_keeper_set(logctx, keeper_amount, &keeper_config).await?;

        // Start all replica nodes
        let replica_amount = 2;
        let mut replicas =
            Self::new_replica_set(logctx, replica_amount, &replica_config)
                .await?;

        let r1 = replicas.swap_remove(0);
        let r2 = replicas.swap_remove(0);
        let k1 = keepers.swap_remove(0);
        let k2 = keepers.swap_remove(0);
        let k3 = keepers.swap_remove(0);

        Ok(Self {
            replica_1: r1,
            replica_2: r2,
            keeper_1: k1,
            keeper_2: k2,
            keeper_3: k3,
            replica_config_path: replica_config,
            keeper_config_path: keeper_config,
        })
    }

    pub async fn new_keeper_set(
        logctx: &LogContext,
        keeper_amount: u16,
        config_path: &PathBuf,
    ) -> Result<Vec<ClickHouseInstance>, anyhow::Error> {
        let mut keepers = vec![];

        for i in 1..=keeper_amount {
            let k_port = 9180 + i;
            let k_id = i;

            let k = ClickHouseInstance::new_keeper(
                logctx,
                k_port,
                k_id,
                config_path.clone(),
            )
            .await
            .map_err(|e| {
                anyhow!("Failed to start ClickHouse keeper {}: {}", i, e)
            })?;
            keepers.push(k)
        }

        Ok(keepers)
    }

    pub async fn new_replica_set(
        logctx: &LogContext,
        replica_amount: u16,
        config_path: &PathBuf,
    ) -> Result<Vec<ClickHouseInstance>, anyhow::Error> {
        let mut replicas = vec![];

        for i in 1..=replica_amount {
            let r_port = 8122 + i;
            let r_tcp_port = 9000 + i;
            let r_interserver_port = 9008 + i;
            let r_name = format!("oximeter_cluster node {}", i);
            let r_number = format!("0{}", i);
            let r = ClickHouseInstance::new_replicated(
                logctx,
                r_port,
                r_tcp_port,
                r_interserver_port,
                r_name,
                r_number,
                config_path.clone(),
            )
            .await
            .map_err(|e| {
                anyhow!("Failed to start ClickHouse node {}: {}", i, e)
            })?;
            replicas.push(r)
        }

        Ok(replicas)
    }

    pub fn replica_config_path(&self) -> &Path {
        &self.replica_config_path
    }

    pub fn keeper_config_path(&self) -> &Path {
        &self.keeper_config_path
    }
}

// Wait for the ClickHouse log file to become available, including the
// port number.
//
// We extract the port number from the log-file regardless of whether we
// know it already, as this is a more reliable check that the server is
// up and listening. Previously we only did this in the case we need to
// _learn_ the port, which introduces the possibility that we return
// from this function successfully, but the server itself is not yet
// ready to accept connections.
pub async fn wait_for_port(
    log_path: Utf8PathBuf,
) -> Result<u16, anyhow::Error> {
    let p = poll::wait_for_condition(
        || async {
            let result =
                discover_local_listening_port(&log_path, CLICKHOUSE_TIMEOUT)
                    .await;
            match result {
                // Successfully extracted the port, return it.
                Ok(port) => Ok(port),
                Err(e) => {
                    match e {
                        ClickHouseError::Io(ref inner) => {
                            if matches!(
                                inner.kind(),
                                std::io::ErrorKind::NotFound
                            ) {
                                return Err(poll::CondCheckError::NotYet);
                            }
                        }
                        _ => {}
                    }
                    Err(poll::CondCheckError::from(e))
                }
            }
        },
        &Duration::from_millis(500),
        &CLICKHOUSE_TIMEOUT,
    )
    .await
    .context("waiting to discover ClickHouse port")?;
    Ok(p)
}

// Parse the ClickHouse log file at the given path, looking for a line
// reporting the port number of the HTTP server. This is only used if the port
// is chosen by the OS, not the caller.
async fn discover_local_listening_port(
    path: &Utf8Path,
    timeout: Duration,
) -> Result<u16, ClickHouseError> {
    let timeout = Instant::now() + timeout;
    tokio::time::timeout_at(timeout, find_clickhouse_port_in_log(path))
        .await
        .map_err(|_| ClickHouseError::Timeout)?
}

// Parse the clickhouse log for a port number.
//
// NOTE: This function loops forever until the expected line is found. It
// should be run under a timeout, or some other mechanism for cancelling it.
async fn find_clickhouse_port_in_log(
    path: &Utf8Path,
) -> Result<u16, ClickHouseError> {
    let mut reader = BufReader::new(File::open(path).await?);
    let mut lines = reader.lines();
    loop {
        let line = lines.next_line().await?;
        match line {
            Some(line) => {
                if let Some(needle_start) = line.find(CLICKHOUSE_PORT) {
                    // Our needle ends with `http://[::1]`; we'll split on the
                    // colon we expect to follow it to find the port.
                    let address_start = needle_start + CLICKHOUSE_PORT.len();
                    return line[address_start..]
                        .trim()
                        .split(':')
                        .last()
                        .ok_or_else(|| ClickHouseError::InvalidAddress)?
                        .parse()
                        .map_err(|_| ClickHouseError::InvalidPort);
                }
            }
            None => {
                // Reached EOF, just sleep for an interval and check again.
                sleep(Duration::from_millis(10)).await;

                // We might have gotten a partial line; close the file, reopen
                // it, and start reading again from the beginning.
                reader = BufReader::new(File::open(path).await?);
                lines = reader.lines();
            }
        }
    }
}

// Wait for the ClickHouse log file to report it is ready to receive connections
pub async fn wait_for_ready(
    log_path: Utf8PathBuf,
    timeout: Duration,
    needle: &str,
) -> Result<(), anyhow::Error> {
    let p = poll::wait_for_condition(
        || async {
            let result = discover_ready(&log_path, timeout, needle).await;
            match result {
                Ok(ready) => Ok(ready),
                Err(e) => {
                    match e {
                        ClickHouseError::Io(ref inner) => {
                            if matches!(
                                inner.kind(),
                                std::io::ErrorKind::NotFound
                            ) {
                                return Err(poll::CondCheckError::NotYet);
                            }
                        }
                        _ => {}
                    }
                    Err(poll::CondCheckError::from(e))
                }
            }
        },
        &Duration::from_millis(500),
        &timeout,
    )
    .await
    .context("waiting to discover if ClickHouse is ready for connections")?;
    Ok(p)
}

// Parse the ClickHouse log file at the given path, looking for a line
// reporting that the server is ready for connections.
async fn discover_ready(
    path: &Utf8Path,
    timeout: Duration,
    needle: &str,
) -> Result<(), ClickHouseError> {
    let timeout = Instant::now() + timeout;
    tokio::time::timeout_at(timeout, clickhouse_ready_from_log(path, needle))
        .await
        .map_err(|_| ClickHouseError::Timeout)?
}

// Parse the clickhouse log to know if the server is ready for connections.
//
// NOTE: This function loops forever until the expected line is found. It
// should be run under a timeout, or some other mechanism for cancelling it.
async fn clickhouse_ready_from_log(
    path: &Utf8Path,
    needle: &str,
) -> Result<(), ClickHouseError> {
    let mut reader = BufReader::new(File::open(path).await?);
    let mut lines = reader.lines();
    loop {
        let line = lines.next_line().await?;
        match line {
            Some(line) => {
                if let Some(_) = line.find(needle) {
                    return Ok(());
                }
            }
            None => {
                // Reached EOF, just sleep for an interval and check again.
                sleep(Duration::from_millis(10)).await;

                // We might have gotten a partial line; close the file, reopen
                // it, and start reading again from the beginning.
                reader = BufReader::new(File::open(path).await?);
                lines = reader.lines();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        discover_local_listening_port, discover_ready, ClickHouseError,
        CLICKHOUSE_PORT, CLICKHOUSE_READY, CLICKHOUSE_TIMEOUT,
    };
    use camino_tempfile::NamedUtf8TempFile;
    use std::process::Stdio;
    use std::{io::Write, sync::Arc, time::Duration};
    use tokio::{sync::Mutex, task::spawn, time::sleep};

    const EXPECTED_PORT: u16 = 12345;

    #[tokio::test]
    async fn test_clickhouse_in_path() {
        // With no arguments, we expect to see the default help message.
        tokio::process::Command::new("clickhouse")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Cannot find 'clickhouse' on PATH. Refer to README.md for installation instructions");
    }

    #[tokio::test]
    async fn test_discover_local_listening_port() {
        // Write some data to a fake log file
        let mut file = NamedUtf8TempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "<Information> Application: Listening for http://[::1]:{}",
            EXPECTED_PORT
        )
        .unwrap();
        writeln!(file, "Another garbage line").unwrap();
        file.flush().unwrap();

        assert_eq!(
            discover_local_listening_port(file.path(), CLICKHOUSE_TIMEOUT)
                .await
                .unwrap(),
            EXPECTED_PORT
        );
    }

    #[tokio::test]
    async fn test_discover_clickhouse_ready() {
        // Write some data to a fake log file
        let mut file = NamedUtf8TempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "2023.07.31 20:12:38.936192 [ 82373 ] <Information> {}",
            CLICKHOUSE_READY,
        )
        .unwrap();
        writeln!(file, "Another garbage line").unwrap();
        file.flush().unwrap();

        assert!(matches!(
            discover_ready(file.path(), CLICKHOUSE_TIMEOUT, CLICKHOUSE_READY)
                .await,
            Ok(())
        ));
    }

    #[tokio::test]
    async fn test_discover_clickhouse_not_ready() {
        // Write some data to a fake log file
        let mut file = NamedUtf8TempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "2023.07.31 20:12:38.936192 [ 82373 ] <Information> Application: Not ready for connections.",
        )
        .unwrap();
        writeln!(file, "Another garbage line").unwrap();
        file.flush().unwrap();
        assert!(matches!(
            discover_ready(
                file.path(),
                Duration::from_secs(1),
                CLICKHOUSE_READY
            )
            .await,
            Err(ClickHouseError::Timeout {})
        ));
    }

    // A regression test for #131.
    //
    // The function `discover_local_listening_port` initially read from the log
    // file until EOF, but there's no guarantee that ClickHouse has written the
    // port we're searching for before the reader consumes the whole file. This
    // test confirms that the file is read until the line is found, ignoring
    // EOF, at least until the timeout is hit.
    #[tokio::test]
    async fn test_discover_local_listening_port_slow_write() {
        // In this case the writer is slightly "slower" than the reader.
        let writer_interval = Duration::from_millis(20);
        assert_eq!(
            read_log_file(CLICKHOUSE_TIMEOUT, writer_interval).await.unwrap(),
            EXPECTED_PORT
        );
    }

    // An extremely slow write test, to verify the timeout handling.
    #[tokio::test]
    async fn test_discover_local_listening_port_timeout() {
        // In this case, the writer is _much_ slower than the reader, so that the reader times out
        // entirely before finding the desired line.
        let reader_timeout = Duration::from_millis(1);
        let writer_interval = Duration::from_millis(100);
        assert!(read_log_file(reader_timeout, writer_interval).await.is_err());
    }

    // Implementation of the above tests, simulating simultaneous
    // reading/writing of the log file
    //
    // This uses Tokio's test utilities to manage time, rather than relying on
    // timeouts.
    async fn read_log_file(
        reader_timeout: Duration,
        writer_interval: Duration,
    ) -> Result<u16, ClickHouseError> {
        async fn write_and_wait(
            file: &mut NamedUtf8TempFile,
            line: String,
            interval: Duration,
        ) {
            println!(
                "Writing to log file: {:?}, contents: '{}'",
                file.path(),
                line
            );
            write!(file, "{}", line).unwrap();
            file.flush().unwrap();
            sleep(interval).await;
        }

        // Start a task that slowly writes lines to the log file.
        //
        // NOTE: This looks overly complicated, and it is. We have to wrap this
        // in a mutex because both this function, and the writer task we're
        // spawning, need access to the file. They may complete in any order,
        // and so it's not possible to give one of them ownership over the
        // `NamedTempFile`. If the owning task completes, that may delete the
        // file before the other task accesses it. So we need interior
        // mutability (because one of the references is mutable for writing),
        // and _this_ scope must own it.
        let file = Arc::new(Mutex::new(NamedUtf8TempFile::new()?));
        let path = file.lock().await.path().to_path_buf();
        let writer_file = file.clone();
        let writer_task = spawn(async move {
            let mut file = writer_file.lock().await;
            write_and_wait(
                &mut file,
                "A garbage line\n".to_string(),
                writer_interval,
            )
            .await;

            // Ensure we can still parse the line even if our buf reader hits
            // EOF in the middle of the line
            // (https://github.com/oxidecomputer/omicron/issues/3580).
            write_and_wait(
                &mut file,
                (&CLICKHOUSE_PORT[..30]).to_string(),
                writer_interval,
            )
            .await;
            write_and_wait(
                &mut file,
                format!("{}:{}\n", &CLICKHOUSE_PORT[30..], EXPECTED_PORT),
                writer_interval,
            )
            .await;

            write_and_wait(
                &mut file,
                "Another garbage line\n".to_string(),
                writer_interval,
            )
            .await;
        });
        println!("Starting reader task");
        let reader_task = discover_local_listening_port(&path, reader_timeout);

        // "Run" the test.
        //
        // Note that the futures for the reader/writer tasks must be pinned to
        // the stack, so that they may be polled on multiple passes through the
        // select loop without consuming them.
        tokio::pin!(writer_task);
        tokio::pin!(reader_task);
        let mut poll_writer = true;
        let reader_result = loop {
            tokio::select! {
                reader_result = &mut reader_task => {
                    println!("Reader finished");
                    break reader_result;
                },
                writer_result = &mut writer_task, if poll_writer => {
                    println!("Writer finished");
                    let _ = writer_result.unwrap();
                    poll_writer = false;
                },
            }
        };
        reader_result
    }
}
