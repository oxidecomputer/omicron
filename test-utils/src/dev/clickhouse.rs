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
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use std::net::{Ipv6Addr, SocketAddrV6};
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

/// A ClickHouse deployment, either single-node or a cluster.
#[derive(Debug)]
pub enum ClickHouseDeployment {
    /// A single-node deployment.
    ///
    /// This starts a single replica on one server. It is expected to work with
    /// the non-replicated version of the `oximeter` database.
    SingleNode(ClickHouseInstance),
    /// A replicated ClickHouse cluster.
    ///
    /// This starts several replica servers, and the required ClickHouse Keeper
    /// nodes that manage the cluster. It is expected to work with the
    /// replicated version of the `oximeter` database.
    Cluster(ClickHouseCluster),
}

impl ClickHouseDeployment {
    /// Create a single-node deployment.
    pub async fn new_single_node(
        logctx: &LogContext,
        http_port: u16,
    ) -> Result<Self, anyhow::Error> {
        ClickHouseInstance::new_single_node(logctx, http_port)
            .await
            .map(Self::SingleNode)
    }

    /// Create a replicated cluster deployment.
    pub async fn new_cluster(
        logctx: &LogContext,
        replica_config: PathBuf,
        keeper_config: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        ClickHouseCluster::new(logctx, replica_config, keeper_config)
            .await
            .map(Self::Cluster)
    }

    /// Return true if this is a cluster deployment.
    pub fn is_cluster(&self) -> bool {
        matches!(self, ClickHouseDeployment::Cluster(_))
    }

    /// Return an HTTP address for any instance in the deployment.
    ///
    /// If this is a single-node, there's only one of these. For a replicated
    /// cluster deployment, this returns an HTTP address to one of them, but
    /// it's not specified which.
    ///
    /// If one cares about a specific server, one can iterate over them and get
    /// their exact addresses.
    pub fn http_address(&self) -> SocketAddrV6 {
        match self {
            ClickHouseDeployment::SingleNode(instance) => instance.address,
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.replicas.first().unwrap().address
            }
        }
    }

    /// Cleanup all child processes and data files in the deployment.
    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                instance.cleanup().await
            }
            ClickHouseDeployment::Cluster(cluster) => cluster.cleanup().await,
        }
    }

    /// Return the path to the replica configuration files.
    ///
    /// This is only Some(_) in a cluster deployment. In a single-node, we use a
    /// configuration embedded in the server binary itself.
    pub fn replica_config_path(&self) -> Option<&Path> {
        match self {
            ClickHouseDeployment::SingleNode(_) => None,
            ClickHouseDeployment::Cluster(cluster) => {
                Some(cluster.replica_config_path())
            }
        }
    }

    /// Return the path to the keeper configuration files.
    ///
    /// This is only Some(_) in a cluster deployment. In a single-node, there
    /// are no Keepers, and so no config file for them.
    pub fn keeper_config_path(&self) -> Option<&Path> {
        match self {
            ClickHouseDeployment::SingleNode(_) => None,
            ClickHouseDeployment::Cluster(cluster) => {
                Some(cluster.keeper_config_path())
            }
        }
    }

    /// Wait for any node in the cluster to shutdown.
    pub async fn wait_for_shutdown(
        &mut self,
    ) -> Result<ClusterNode, anyhow::Error> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                instance.wait_for_shutdown().await
            }
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.wait_for_shutdown().await
            }
        }
    }

    /// Wait for _all_ nodes in the cluster to shutdown.
    pub async fn wait_for_all_shutdown(&mut self) -> Result<(), anyhow::Error> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                instance.wait_for_shutdown().await.map(|_| ())
            }
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.wait_for_all_shutdown().await
            }
        }
    }

    /// Return an iterator over all the replicas in the deployment.
    pub fn instances(
        &self,
    ) -> Box<dyn Iterator<Item = &ClickHouseInstance> + '_> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                Box::new(std::iter::once(instance))
            }
            ClickHouseDeployment::Cluster(cluster) => {
                Box::new(cluster.instances())
            }
        }
    }

    /// Return an iterator over all the ClickHouse Keepers in the deployment.
    pub fn keepers(&self) -> Box<dyn Iterator<Item = &ClickHouseKeeper> + '_> {
        match self {
            ClickHouseDeployment::SingleNode(_) => Box::new(std::iter::empty()),
            ClickHouseDeployment::Cluster(cluster) => {
                Box::new(cluster.keepers())
            }
        }
    }
}

/// A ClickHouse Keeper, which forms a quorum with others to manage replicas.
#[derive(Debug)]
pub struct ClickHouseKeeper(ClickHouseInstance);

impl std::ops::Deref for ClickHouseKeeper {
    type Target = ClickHouseInstance;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ClickHouseKeeper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A `ClickHouseInstance` is used to start and manage one ClickHouse server.
/// process.
///
/// Note that this only refers to ClickHouse _replicas_ or data servers. The
/// Keepers in a cluster are managed through `ClickHouseKeeper` objects.
#[derive(Debug)]
pub struct ClickHouseInstance {
    // Directory in which all data, logs, etc are stored.
    data_dir: Option<ClickHouseDataDir>,
    data_path: Utf8PathBuf,
    // The address the server is listening on
    pub address: SocketAddrV6,
    // Full list of command-line arguments
    args: Vec<String>,
    // Subprocess handle
    child: Option<tokio::process::Child>,
    // Environment variables for the child process.
    env: BTreeMap<String, String>,
}

/// An error starting a ClickHouse proceess.
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
    async fn new_single_node(
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

        // By default ClickHouse forks a child if it's been explicitly
        // requested via the following environment variable, _or_ if it's
        // not attached to a TTY. Avoid this behavior, so that we can
        // correctly deliver SIGINT. The "watchdog" masks SIGINT, meaning
        // we'd have to deliver that to the _child_, which is more
        // complicated.
        let mut env: BTreeMap<_, _> =
            [(String::from("CLICKHOUSE_WATCHDOG_ENABLE"), String::from("0"))]
                .into_iter()
                .collect();
        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            // ClickHouse internally tees its logs to a file, so we throw away
            // std{in,out,err}
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .current_dir(data_dir.cwd_path())
            .envs(&env)
            .spawn()
            .with_context(|| {
                format!("failed to spawn `clickhouse` (with args: {:?})", &args)
            })?;
        for (k, v) in std::env::vars_os() {
            env.insert(
                k.to_string_lossy().to_string(),
                v.to_string_lossy().to_string(),
            );
        }
        let data_path = data_dir.root_path().to_path_buf();
        let port = wait_for_port(data_dir.log_path()).await?;
        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);
        Ok(Self {
            data_dir: Some(data_dir),
            data_path,
            address,
            args,
            child: Some(child),
            env,
        })
    }

    /// Start a new replicated ClickHouse server on the given IPv6 port.
    async fn new_replicated(
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
        let mut env: BTreeMap<_, _> = [
            (String::from("CLICKHOUSE_WATCHDOG_ENABLE"), String::from("0")),
            (String::from("CH_LOG"), data_dir.log_path().to_string()),
            (String::from("CH_ERROR_LOG"), data_dir.err_log_path().to_string()),
            (String::from("CH_REPLICA_DISPLAY_NAME"), name.clone()),
            (String::from("CH_LISTEN_ADDR"), String::from("::")),
            (String::from("CH_LISTEN_PORT"), port.to_string()),
            (String::from("CH_TCP_PORT"), tcp_port.to_string()),
            (String::from("CH_INTERSERVER_PORT"), interserver_port.to_string()),
            (
                String::from("CH_DATASTORE"),
                data_dir.datastore_path().to_string(),
            ),
            (String::from("CH_TMP_PATH"), data_dir.tmp_path().to_string()),
            (
                String::from("CH_USER_FILES_PATH"),
                data_dir.user_files_path().to_string(),
            ),
            (
                String::from("CH_USER_LOCAL_DIR"),
                data_dir.access_path().to_string(),
            ),
            (
                String::from("CH_FORMAT_SCHEMA_PATH"),
                data_dir.format_schemas_path().to_string(),
            ),
            (String::from("CH_REPLICA_NUMBER"), r_number.clone()),
            (String::from("CH_REPLICA_HOST_01"), String::from("::1")),
            (String::from("CH_REPLICA_HOST_02"), String::from("::1")),
            // ClickHouse servers have a small quirk, where when setting the
            // keeper hosts as IPv6 localhost addresses in the replica
            // configuration file, they must be wrapped in square brackets
            // Otherwise, when running any query, a "Service not found" error
            // appears.
            (String::from("CH_KEEPER_HOST_01"), String::from("[::1]")),
            (String::from("CH_KEEPER_HOST_02"), String::from("[::1]")),
            (String::from("CH_KEEPER_HOST_03"), String::from("[::1]")),
        ]
        .into_iter()
        .collect();

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .current_dir(data_dir.cwd_path())
            .envs(&env)
            .spawn()
            .with_context(|| {
                format!("failed to spawn `clickhouse` (with args: {:?})", &args)
            })?;
        for (k, v) in std::env::vars_os() {
            env.insert(
                k.to_string_lossy().to_string(),
                v.to_string_lossy().to_string(),
            );
        }
        let data_path = data_dir.root_path().to_path_buf();
        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);
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
                address,
                args,
                child: Some(child),
                env,
            }),
            Err(e) => Err(e),
        }
    }

    /// Start a new ClickHouse keeper on the given IPv6 port.
    async fn new_keeper(
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
        let mut env: BTreeMap<_, _> = [
            (String::from("CLICKHOUSE_WATCHDOG_ENABLE"), String::from("0")),
            (String::from("CH_LOG"), data_dir.keeper_log_path().to_string()),
            (
                String::from("CH_ERROR_LOG"),
                data_dir.keeper_err_log_path().to_string(),
            ),
            (String::from("CH_LISTEN_ADDR"), String::from("::")),
            (String::from("CH_LISTEN_PORT"), port.to_string()),
            (String::from("CH_KEEPER_ID_CURRENT"), k_id.to_string()),
            (
                String::from("CH_DATASTORE"),
                data_dir.datastore_path().to_string(),
            ),
            (
                String::from("CH_LOG_STORAGE_PATH"),
                data_dir.keeper_log_storage_path().to_string(),
            ),
            (
                String::from("CH_SNAPSHOT_STORAGE_PATH"),
                data_dir.keeper_snapshot_storage_path().to_string(),
            ),
            (String::from("CH_KEEPER_ID_01"), String::from("1")),
            (String::from("CH_KEEPER_ID_02"), String::from("2")),
            (String::from("CH_KEEPER_ID_03"), String::from("3")),
            (String::from("CH_KEEPER_HOST_01"), String::from("::1")),
            (String::from("CH_KEEPER_HOST_02"), String::from("::1")),
            (String::from("CH_KEEPER_HOST_03"), String::from("::1")),
        ]
        .into_iter()
        .collect();

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .envs(&env)
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn `clickhouse keeper` (with args: {:?})",
                    &args
                )
            })?;
        for (k, v) in std::env::vars_os() {
            env.insert(
                k.to_string_lossy().to_string(),
                v.to_string_lossy().to_string(),
            );
        }
        let data_path = data_dir.root_path().to_path_buf();
        let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);
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
                address,
                args,
                child: Some(child),
                env,
            }),
            Err(e) => Err(e),
        }
    }

    /// Wait for the ClickHouse server process to shutdown, after it's been killed.
    pub async fn wait_for_shutdown(
        &mut self,
    ) -> Result<ClusterNode, anyhow::Error> {
        // NOTE: Await the child's shutdown, but do _not_ take it out of the
        // option yet. This allows us to await the shutdown of an entire
        // cluster by awaiting each child in parallel, exiting when the first
        // exits. By keeping these around, we can still deliver the signal to
        // the other, non-exited children.
        if let Some(child) = &mut self.child {
            child.wait().await?;
        }
        let _ = self.child.take();
        self.cleanup().await?;
        Ok(ClusterNode { kind: NodeKind::Replica, index: 1 })
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

    /// Return the environment used to start the ClickHouse process.
    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.env
    }

    /// Return the child PID, if any
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.id())
    }

    /// Return the HTTP port the server is listening on.
    pub fn port(&self) -> u16 {
        self.address.port()
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

/// Number of data replicas in our test cluster.
pub const N_REPLICAS: u8 = 2;

/// Number of ClickHouse Keepers in our test cluster.
pub const N_KEEPERS: u8 = 3;

/// A `ClickHouseCluster` is used to start and manage a 2 replica 3 keeper ClickHouse cluster.
#[derive(Debug)]
pub struct ClickHouseCluster {
    replicas: Vec<ClickHouseInstance>,
    keepers: Vec<ClickHouseKeeper>,
    replica_config_path: PathBuf,
    keeper_config_path: PathBuf,
}

impl ClickHouseCluster {
    /// Construct a new ClickHouse replicated cluster.
    pub async fn new(
        logctx: &LogContext,
        replica_config: PathBuf,
        keeper_config: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        let keepers =
            Self::new_keeper_set(logctx, N_KEEPERS, &keeper_config).await?;
        let replicas =
            Self::new_replica_set(logctx, N_REPLICAS, &replica_config).await?;
        Ok(Self {
            replicas,
            keepers,
            replica_config_path: replica_config,
            keeper_config_path: keeper_config,
        })
    }

    /// Create the set of ClickHouse Keepers for the cluster.
    async fn new_keeper_set(
        logctx: &LogContext,
        n_keepers: u8,
        config_path: &PathBuf,
    ) -> Result<Vec<ClickHouseKeeper>, anyhow::Error> {
        let mut keepers = Vec::with_capacity(usize::from(n_keepers));

        for i in 1..=u16::from(n_keepers) {
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
            keepers.push(ClickHouseKeeper(k));
        }

        Ok(keepers)
    }

    /// Create the set of ClickHouse replicas for the cluster.
    async fn new_replica_set(
        logctx: &LogContext,
        n_replicas: u8,
        config_path: &PathBuf,
    ) -> Result<Vec<ClickHouseInstance>, anyhow::Error> {
        let mut replicas = Vec::with_capacity(usize::from(n_replicas));

        for i in 1..=u16::from(n_replicas) {
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

    /// Clean up all nodes in the cluster.
    async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        for info in self.children_mut() {
            info.child.cleanup().await.with_context(|| {
                format!(
                    "cleaning up {:?} node {} (PID {})",
                    info.node.kind,
                    info.node.index,
                    info.child
                        .pid()
                        .map(|pid| pid.to_string())
                        .unwrap_or_else(|| String::from("?")),
                )
            })?;
        }
        Ok(())
    }

    /// Wait for any node in the cluster to exit, returning information about
    /// the node that exited.
    ///
    /// The error message contains information about which node in the cluster
    /// shutdown.
    async fn wait_for_shutdown(
        &mut self,
    ) -> Result<ClusterNode, anyhow::Error> {
        let mut futs = FuturesUnordered::new();
        for each in self.children_mut() {
            futs.push(async move {
                let ChildWaitInfo { child, node: ClusterNode { kind, index } } =
                    each;
                let pid = child
                    .pid()
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| String::from("?"));
                child.wait_for_shutdown().await.with_context(|| {
                    format!("ClickHouse {kind:?} {index} (PID {pid}) shutdown")
                })
            });
        }
        match futs.next().await {
            Some(res) => res,
            None => anyhow::bail!("Failed to await child future"),
        }
    }

    /// Wait for _all_ nodes in the cluster to exit.
    async fn wait_for_all_shutdown(&mut self) -> Result<(), anyhow::Error> {
        let mut res = Ok(());
        for each in self.children_mut() {
            let ChildWaitInfo { child, node: ClusterNode { kind, index } } =
                each;
            let pid = child
                .pid()
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| String::from("?"));
            res = child
                .wait_for_shutdown()
                .await
                .with_context(|| {
                    format!(
                        "ClickHouse {kind:?} {index} (PID {pid}) shutdown, \
                    data dir: {}",
                        child.data_path(),
                    )
                })
                .map(|_| ());
        }
        res
    }

    /// Return an iterator over all the ClickHouse instances in the cluster.
    fn instances(&self) -> impl Iterator<Item = &ClickHouseInstance> {
        self.replicas.iter()
    }

    /// Return an iterator over alll the ClickHouse Keepers in the cluster.
    fn keepers(&self) -> impl Iterator<Item = &ClickHouseKeeper> {
        self.keepers.iter()
    }

    // Return an iterator over the child processes as instances.
    //
    // This is pretty hacky, but used to wait _any_ child in
    // `wait_for_shutdown()`, along with child kind and its index among that
    // kind.
    fn children_mut(&mut self) -> impl Iterator<Item = ChildWaitInfo> {
        let keepers =
            self.keepers.iter_mut().enumerate().map(|(index, child)| {
                ChildWaitInfo {
                    child,
                    node: ClusterNode {
                        kind: NodeKind::Keeper,
                        index: index + 1,
                    },
                }
            });
        self.replicas
            .iter_mut()
            .enumerate()
            .map(|(index, child)| ChildWaitInfo {
                child,
                node: ClusterNode { kind: NodeKind::Keeper, index: index + 1 },
            })
            .chain(keepers)
    }
}

/// Helper type to wait for any child to exit.
///
/// See `ClickHouseCluster::children_mut`;
struct ChildWaitInfo<'a> {
    child: &'a mut ClickHouseInstance,
    node: ClusterNode,
}

/// Any node in the ClickHouse deployment.
#[derive(Clone, Copy, Debug)]
pub struct ClusterNode {
    /// The kind of node.
    pub kind: NodeKind,
    /// The node's index, among nodes of the same kind.
    pub index: usize,
}

/// The kind of a node in a ClickHouse deployment.
#[derive(Clone, Copy, Debug)]
pub enum NodeKind {
    /// This node is a data replica server.
    Replica,
    /// This node is a Keeper server.
    Keeper,
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
