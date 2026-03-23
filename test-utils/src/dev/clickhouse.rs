// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for managing ClickHouse during development

use std::collections::BTreeMap;
use std::num::NonZeroU8;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, anyhow};
use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::{Builder, Utf8TempDir};
use dropshot::test_util::{LogContext, log_prefix_for_test};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use omicron_common::address::{CLICKHOUSE_HTTP_PORT, CLICKHOUSE_TCP_PORT};
use std::net::{Ipv6Addr, SocketAddrV6};
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    time::{Instant, sleep},
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

// The string to look for in a ClickHouse log file when trying to determine the
// HTTP port number on which it is listening.
const CLICKHOUSE_HTTP_PORT_NEEDLE: &'static str =
    "Application: Listening for http://[::1]";

// The string to look for in a ClickHouse log file when trying to determine the
// native TCP protocol port number on which it is listening.
const CLICKHOUSE_TCP_PORT_NEEDLE: &'static str =
    "Application: Listening for native protocol (tcp): [::1]";

/// A ClickHouse deployment, either single-node or a cluster.
#[derive(Debug)]
pub enum ClickHouseDeployment {
    /// A single-node deployment.
    ///
    /// This starts a single replica on one server. It is expected to work with
    /// the non-replicated version of the `oximeter` database.
    SingleNode(Box<ClickHouseReplica>),
    /// A replicated ClickHouse cluster.
    ///
    /// This starts several replica servers, and the required ClickHouse Keeper
    /// nodes that manage the cluster. It is expected to work with the
    /// replicated version of the `oximeter` database.
    Cluster(Box<ClickHouseCluster>),
}

/// Port numbers that a ClickHouse replica listens on.
#[derive(Clone, Copy, Debug)]
pub struct ClickHousePorts {
    http: u16,
    native: u16,
}

impl Default for ClickHousePorts {
    fn default() -> Self {
        Self { http: CLICKHOUSE_HTTP_PORT, native: CLICKHOUSE_TCP_PORT }
    }
}

impl ClickHousePorts {
    /// Create ports for ClickHouse to listen on.
    ///
    /// This returns an error if both ports are the same and non-zero.
    pub fn new(http: u16, native: u16) -> Result<Self, ClickHouseError> {
        if http == native && http != 0 {
            return Err(ClickHouseError::ReplicaPortsMustBeDifferent);
        }
        Ok(Self { http, native })
    }

    /// Use ports of 0, to let the OS assign them for us.
    pub fn zero() -> Self {
        Self { http: 0, native: 0 }
    }

    // Return true if any of the port numbers are zero.
    //
    // This is used to determine when we've learned the ports from reading the
    // log files.
    fn any_zero(&self) -> bool {
        self.http == 0 || self.native == 0
    }

    // Assert that if the ports in self are non-zero, they match those in
    // `new_ports`. This is used to check that we recover the exact same ports
    // from a logfile that were specifically requested.
    #[track_caller]
    fn assert_consistent(&self, new_ports: &ClickHousePorts) {
        assert!(
            self.http == 0 || self.http == new_ports.http,
            "ClickHouse HTTP port was specified, but did not match the \
            value recovered from the log file. Specified {}, found {}.",
            self.http,
            new_ports.http,
        );
        assert!(
            self.native == 0 || self.native == new_ports.native,
            "ClickHouse native port was specified, but did not match the \
            value recovered from the log file. Specified {}, found {}.",
            self.native,
            new_ports.native,
        );
    }
}

impl ClickHouseDeployment {
    /// Create a single-node deployment.
    ///
    /// This spawns a single replica, listening on any available port. To choose
    /// the ports explicitly, use
    /// `ClickHouseDeployment::new_single_node_with_ports()`.
    pub async fn new_single_node(
        logctx: &LogContext,
    ) -> Result<Self, anyhow::Error> {
        Self::new_single_node_with_ports(logctx, ClickHousePorts::zero()).await
    }

    /// Create a single-node deployment, listening on specifc ports.
    pub async fn new_single_node_with_ports(
        logctx: &LogContext,
        ports: ClickHousePorts,
    ) -> Result<Self, anyhow::Error> {
        ClickHouseProcess::new_single_node(logctx, ports)
            .await
            .map(|replica| Self::SingleNode(Box::new(replica)))
    }

    /// Create a replicated cluster deployment.
    pub async fn new_cluster(
        logctx: &LogContext,
        replica_config: PathBuf,
        keeper_config: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        ClickHouseCluster::new(logctx, replica_config, keeper_config)
            .await
            .map(|cluster| Self::Cluster(Box::new(cluster)))
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
    /// If you'd like all of them, you can use `all_http_addresses()`.
    pub fn http_address(&self) -> SocketAddrV6 {
        match self {
            ClickHouseDeployment::SingleNode(instance) => instance.http_address,
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.replicas.first().unwrap().http_address
            }
        }
    }

    /// Return all HTTP addresses for a deployment's replicas.
    pub fn all_http_addresses(&self) -> Vec<SocketAddrV6> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                vec![instance.http_address]
            }
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.replicas.iter().map(|rep| rep.http_address).collect()
            }
        }
    }

    /// Return an address speaking the native TCP protocol for any instance in
    /// the deployment.
    ///
    /// If this is a single-node, there's only one of these. For a replicated
    /// cluster deployment, this returns an address to one of them, but it's not
    /// specified which.
    ///
    /// If you'd like all of them, you can use `all_native_addresses().`
    pub fn native_address(&self) -> SocketAddrV6 {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                instance.native_address
            }
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.replicas.first().unwrap().native_address
            }
        }
    }

    /// Return all native addresses for a deployment's replicas.
    pub fn all_native_addresses(&self) -> Vec<SocketAddrV6> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                vec![instance.native_address]
            }
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.replicas().map(|rep| rep.native_address).collect()
            }
        }
    }

    /// Cleanup all child processes and data files in the deployment.
    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                // NOTE: We need to access the `process` field directly,
                // because intentionally do not implement `DerefMut`. That is to
                // avoid letting users manipulate individual processes, except
                // through the `ClickHouseDeployment` type.
                instance.process.cleanup().await
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
                instance.process.wait_for_shutdown().await
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
                instance.process.wait_for_shutdown().await.map(|_| ())
            }
            ClickHouseDeployment::Cluster(cluster) => {
                cluster.wait_for_all_shutdown().await
            }
        }
    }

    /// Return an iterator over all the replicas in the deployment.
    pub fn replicas(
        &self,
    ) -> Box<dyn Iterator<Item = &ClickHouseReplica> + '_> {
        match self {
            ClickHouseDeployment::SingleNode(instance) => {
                Box::new(std::iter::once(&**instance))
            }
            ClickHouseDeployment::Cluster(cluster) => {
                Box::new(cluster.replicas())
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

/// A ClickHouse data replica, which manages the actual database and tables.
#[derive(Debug)]
pub struct ClickHouseReplica {
    pub http_address: SocketAddrV6,
    pub native_address: SocketAddrV6,
    process: ClickHouseProcess,
}

impl std::ops::Deref for ClickHouseReplica {
    type Target = ClickHouseProcess;

    fn deref(&self) -> &Self::Target {
        &self.process
    }
}

/// A ClickHouse Keeper, which forms a quorum with others to manage replicas.
#[derive(Debug)]
pub struct ClickHouseKeeper {
    pub address: SocketAddrV6,
    process: ClickHouseProcess,
}

impl std::ops::Deref for ClickHouseKeeper {
    type Target = ClickHouseProcess;

    fn deref(&self) -> &Self::Target {
        &self.process
    }
}

/// A `ClickHouseProcess` is used to start and manage one ClickHouse process.
///
/// This can be either a data replica or a ClickHouse Keeper. This object is
/// used to manage the child process and its data directories, and the objects
/// `ClickHouseInstance` and `ClickHouseKeeper` are used to access things like
/// network ports on those.
#[derive(Debug)]
pub struct ClickHouseProcess {
    // Directory in which all data, logs, etc are stored.
    data_dir: Option<ClickHouseDataDir>,
    data_path: Utf8PathBuf,
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

    #[error("ClickHouse replica HTTP and native TCP ports must be different")]
    ReplicaPortsMustBeDifferent,

    #[error("Invalid ClickHouse port number")]
    InvalidPort,

    #[error("Invalid ClickHouse listening address")]
    InvalidAddress,

    #[error("Invalid ClickHouse Keeper ID")]
    InvalidKeeperId,

    #[error("Failed to detect ClickHouse subprocess within timeout")]
    Timeout,
}

const SINGLE_NODE_CONFIG_FILE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../smf/clickhouse/config.xml");

impl ClickHouseProcess {
    /// Start a new single node ClickHouse server listening on the provided
    /// ports.
    async fn new_single_node(
        logctx: &LogContext,
        ports: ClickHousePorts,
    ) -> Result<ClickHouseReplica, anyhow::Error> {
        let data_dir = ClickHouseDataDir::new(logctx)?;

        // Copy the configuration file into the test directory, so we don't
        // leave the preprocessed config file hanging around.
        tokio::fs::copy(SINGLE_NODE_CONFIG_FILE, data_dir.config_file_path())
            .await
            .with_context(|| {
                format!(
                    "failed to copy config file from {} to test data path {}",
                    SINGLE_NODE_CONFIG_FILE,
                    data_dir.config_file_path(),
                )
            })?;
        let args = vec![
            "server".to_string(),
            "--config-file".to_string(),
            data_dir.config_file_path().to_string(),
            "--log-file".to_string(),
            data_dir.log_path().to_string(),
            "--errorlog-file".to_string(),
            data_dir.err_log_path().to_string(),
            "--".to_string(),
            "--tcp_port".to_string(),
            ports.native.to_string(),
            "--http_port".to_string(),
            ports.http.to_string(),
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
        // NOTE: Always extract the ports, even if they're specified, to ensure
        // that we don't return from this until the server is actually ready to
        // accept connections.
        let ports = {
            let new_ports = wait_for_ports(&data_dir.log_path()).await?;
            // If either port was specified, add an additional check that we
            // recovered exactly that port.
            ports.assert_consistent(&new_ports);
            new_ports
        };
        let http_address = ipv6_localhost_on(ports.http);
        let native_address = ipv6_localhost_on(ports.native);
        Ok(ClickHouseReplica {
            http_address,
            native_address,
            process: ClickHouseProcess {
                data_dir: Some(data_dir),
                data_path,
                args,
                child: Some(child),
                env,
            },
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
    ) -> Result<ClickHouseReplica, anyhow::Error> {
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
        wait_for_ready(
            &data_dir.log_path(),
            CLICKHOUSE_TIMEOUT,
            CLICKHOUSE_READY,
        )
        .await?;
        let process = ClickHouseProcess {
            data_dir: Some(data_dir),
            data_path,
            args,
            child: Some(child),
            env,
        };
        let http_address = ipv6_localhost_on(port);
        let native_address = ipv6_localhost_on(tcp_port);
        Ok(ClickHouseReplica { http_address, native_address, process })
    }

    /// Start a new ClickHouse keeper on the given IPv6 port.
    async fn new_keeper(
        logctx: &LogContext,
        port: u16,
        k_id: u16,
        config_path: PathBuf,
    ) -> Result<ClickHouseKeeper, anyhow::Error> {
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
        let address = ipv6_localhost_on(port);
        wait_for_ready(
            &data_dir.keeper_log_path(),
            CLICKHOUSE_KEEPER_TIMEOUT,
            KEEPER_READY,
        )
        .await?;
        let process = ClickHouseProcess {
            data_dir: Some(data_dir),
            data_path,
            args,
            child: Some(child),
            env,
        };
        Ok(ClickHouseKeeper { address, process })
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

    fn config_file_path(&self) -> Utf8PathBuf {
        self.root_path().join("config.xml")
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
        let dir = self.dir.keep();

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

impl Drop for ClickHouseProcess {
    fn drop(&mut self) {
        if self.child.is_some() || self.data_dir.is_some() {
            let maybe_pid = self
                .child
                .as_ref()
                .and_then(|child| child.id())
                .map(|id| format!("(PID {})", id))
                .unwrap_or_else(String::new);
            let maybe_dir = self
                .data_dir
                .as_ref()
                .map(|dir| format!(", {}", dir.root_path()))
                .unwrap_or_else(String::new);
            eprintln!(
                "WARN: dropped ClickHouse process without cleaning it up first \
                (there may still be a child process running {maybe_pid} and a \
                temporary directory leaked{maybe_dir})"
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
pub const N_REPLICAS: NonZeroU8 = NonZeroU8::new(2).unwrap();

/// Number of ClickHouse Keepers in our test cluster.
pub const N_KEEPERS: NonZeroU8 = NonZeroU8::new(3).unwrap();

/// A `ClickHouseCluster` is used to start and manage a 2 replica 3 keeper ClickHouse cluster.
#[derive(Debug)]
pub struct ClickHouseCluster {
    replicas: Vec<ClickHouseReplica>,
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
        n_keepers: NonZeroU8,
        config_path: &PathBuf,
    ) -> Result<Vec<ClickHouseKeeper>, anyhow::Error> {
        let mut keepers = Vec::with_capacity(usize::from(n_keepers.get()));

        for i in 1..=u16::from(n_keepers.get()) {
            let k_port = 9180 + i;
            let k_id = i;

            let keeper = ClickHouseProcess::new_keeper(
                logctx,
                k_port,
                k_id,
                config_path.clone(),
            )
            .await
            .map_err(|e| {
                anyhow!("Failed to start ClickHouse keeper {}: {}", i, e)
            })?;
            keepers.push(keeper);
        }

        Ok(keepers)
    }

    /// Create the set of ClickHouse replicas for the cluster.
    async fn new_replica_set(
        logctx: &LogContext,
        n_replicas: NonZeroU8,
        config_path: &PathBuf,
    ) -> Result<Vec<ClickHouseReplica>, anyhow::Error> {
        let mut replicas = Vec::with_capacity(usize::from(n_replicas.get()));

        for i in 1..=u16::from(n_replicas.get()) {
            let r_port = 8122 + i;
            let r_tcp_port = 9000 + i;
            let r_interserver_port = 9008 + i;
            let r_name = format!("oximeter_cluster node {}", i);
            let r_number = format!("0{}", i);
            let r = ClickHouseProcess::new_replicated(
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

    /// Return an iterator over all the ClickHouse replicas in the cluster.
    fn replicas(&self) -> impl Iterator<Item = &ClickHouseReplica> {
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
    fn children_mut(&mut self) -> impl Iterator<Item = ChildWaitInfo<'_>> {
        let keepers =
            self.keepers.iter_mut().enumerate().map(|(index, keeper)| {
                ChildWaitInfo {
                    child: &mut keeper.process,
                    node: ClusterNode {
                        kind: NodeKind::Keeper,
                        index: index + 1,
                    },
                }
            });
        self.replicas
            .iter_mut()
            .enumerate()
            .map(|(index, replica)| ChildWaitInfo {
                child: &mut replica.process,
                node: ClusterNode { kind: NodeKind::Keeper, index: index + 1 },
            })
            .chain(keepers)
    }
}

/// Return a socket address for localhost with the provided port.
fn ipv6_localhost_on(port: u16) -> SocketAddrV6 {
    SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0)
}

/// Helper type to wait for any child to exit.
///
/// See `ClickHouseCluster::children_mut`;
struct ChildWaitInfo<'a> {
    child: &'a mut ClickHouseProcess,
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
// HTTP and native protocol port numbers.
//
// We extract the port numbers from the log-file regardless of whether we
// know it already, as this is a more reliable check that the server is
// up and listening. Previously we only did this in the case we need to
// _learn_ the port, which introduces the possibility that we return
// from this function successfully, but the server itself is not yet
// ready to accept connections.
pub async fn wait_for_ports(
    log_path: &Utf8Path,
) -> Result<ClickHousePorts, anyhow::Error> {
    wait_for_ready(log_path, CLICKHOUSE_TIMEOUT, CLICKHOUSE_READY).await?;
    find_clickhouse_ports_in_log(log_path)
        .await
        .context("finding ClickHouse ports in log")
}

// Parse the ClickHouse log for the HTTP and native TCP port numbers.
//
// # Panics
//
// This function panics if it reaches EOF without discovering the ports. This
// means the function can only be called after `wait_for_ready()` or a similar
// method.
async fn find_clickhouse_ports_in_log(
    path: &Utf8Path,
) -> Result<ClickHousePorts, ClickHouseError> {
    let reader = BufReader::new(File::open(path).await?);
    let mut lines = reader.lines();
    let mut ports = ClickHousePorts::zero();
    loop {
        let line = lines.next_line().await?.expect(
            "Reached EOF in ClickHouse log file without discovering ports",
        );
        if let Some(http_port) =
            find_port_after_needle(&line, CLICKHOUSE_HTTP_PORT_NEEDLE)?
        {
            ports.http = http_port;
        } else if let Some(native_port) =
            find_port_after_needle(&line, CLICKHOUSE_TCP_PORT_NEEDLE)?
        {
            ports.native = native_port;
        } else {
            continue;
        }
        if !ports.any_zero() {
            return Ok(ports);
        }
    }
}

// Match the provided needle, if possible, and parse a port number immediately
// following it.
//
// If the needle is not found in the line, return `Ok(None)`. If the needle is
// found, but the port number cannot be parsed out of the line, return `Err(_)`.
// If the port is successfully parsed, return `Ok(Some(port))`.
fn find_port_after_needle(
    line: &str,
    needle: &str,
) -> Result<Option<u16>, ClickHouseError> {
    line.find(needle)
        .map(|needle_start| {
            // Our needle ends with something like `http://[::1]`;
            // we'll split on the colon we expect to follow it to find
            // the port.
            let address_start = needle_start + needle.len();
            return line[address_start..]
                .trim()
                .split(':')
                .next_back()
                .ok_or_else(|| ClickHouseError::InvalidAddress)?
                .parse()
                .map_err(|_| ClickHouseError::InvalidPort);
        })
        .transpose()
}

// Wait for the ClickHouse log file to report it is ready to receive connections
pub async fn wait_for_ready(
    log_path: &Utf8Path,
    timeout: Duration,
    needle: &str,
) -> Result<(), anyhow::Error> {
    let p = poll::wait_for_condition(
        || async {
            let result = discover_ready(log_path, timeout, needle).await;
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
        CLICKHOUSE_HTTP_PORT_NEEDLE, CLICKHOUSE_READY,
        CLICKHOUSE_TCP_PORT_NEEDLE, CLICKHOUSE_TIMEOUT, ClickHouseDeployment,
        ClickHouseError, ClickHousePorts, discover_ready, wait_for_ports,
    };
    use crate::dev::test_setup_log;
    use camino_tempfile::NamedUtf8TempFile;
    use std::process::Stdio;
    use std::{io::Write, sync::Arc, time::Duration};
    use tokio::{sync::Mutex, task::spawn, time::sleep};

    const EXPECTED_HTTP_PORT: u16 = 12345;
    const EXPECTED_TCP_PORT: u16 = 12346;

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
    async fn wait_for_ports_finds_actual_ports() {
        // Test the nominal case, where ClickHouse writes out the lines with
        // the ports, and then the sentinel indicating readiness.
        let mut file = NamedUtf8TempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "{}:{}",
            CLICKHOUSE_HTTP_PORT_NEEDLE, EXPECTED_HTTP_PORT,
        )
        .unwrap();
        writeln!(file, "Another garbage line").unwrap();
        writeln!(file, "{}:{}", CLICKHOUSE_TCP_PORT_NEEDLE, EXPECTED_TCP_PORT)
            .unwrap();
        writeln!(file, "{}", CLICKHOUSE_READY).unwrap();
        file.flush().unwrap();
        let ports = wait_for_ports(&file.path()).await.unwrap();
        assert_eq!(ports.http, EXPECTED_HTTP_PORT);
        assert_eq!(ports.native, EXPECTED_TCP_PORT);
    }

    #[should_panic]
    #[tokio::test]
    async fn wait_for_ports_panics_with_sentinel_but_no_ports() {
        let mut file = NamedUtf8TempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "{}:{}",
            CLICKHOUSE_HTTP_PORT_NEEDLE, EXPECTED_HTTP_PORT,
        )
        .unwrap();
        writeln!(file, "Another garbage line").unwrap();
        writeln!(file, "{}", CLICKHOUSE_READY).unwrap();
        file.flush().unwrap();
        wait_for_ports(&file.path()).await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_ports_waits_for_sentinel_line() {
        let file = Arc::new(Mutex::new(NamedUtf8TempFile::new().unwrap()));
        // Start a task that slowly writes lines into the file. This ensures
        // that we wait for a while until the sentinel line is written.
        let file_ = file.clone();
        spawn(async move {
            for line in [
                String::from("A garbage line"),
                format!(
                    "{}:{}",
                    CLICKHOUSE_HTTP_PORT_NEEDLE, EXPECTED_HTTP_PORT
                ),
                String::from("Another garbage line"),
                format!("{}:{}", CLICKHOUSE_TCP_PORT_NEEDLE, EXPECTED_TCP_PORT),
                String::from(CLICKHOUSE_READY),
            ] {
                {
                    let mut f = file_.lock().await;
                    writeln!(f, "{}", line).unwrap();
                    f.flush().unwrap();
                }
                sleep(Duration::from_millis(100)).await;
            }
        });
        let path = file.lock().await.path().to_owned();
        let ports = wait_for_ports(&path).await.unwrap();
        assert_eq!(ports.http, EXPECTED_HTTP_PORT);
        assert_eq!(ports.native, EXPECTED_TCP_PORT);
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

    #[test]
    fn test_clickhouse_ports_assert_consistent() {
        let second = ClickHousePorts { http: 1, native: 1 };
        ClickHousePorts { http: 0, native: 0 }.assert_consistent(&second);
        ClickHousePorts { http: 1, native: 0 }.assert_consistent(&second);
        ClickHousePorts { http: 0, native: 1 }.assert_consistent(&second);
        ClickHousePorts { http: 1, native: 1 }.assert_consistent(&second);
    }

    #[test]
    #[should_panic]
    fn test_clickhouse_ports_assert_consistent_panics_one_specified() {
        let second = ClickHousePorts { http: 1, native: 1 };
        ClickHousePorts { http: 0, native: 2 }.assert_consistent(&second);
    }

    #[test]
    #[should_panic]
    fn test_clickhouse_ports_assert_consistent_panics_both_specified() {
        let second = ClickHousePorts { http: 1, native: 1 };
        ClickHousePorts { http: 2, native: 2 }.assert_consistent(&second);
    }

    #[derive(Debug, serde::Deserialize)]
    struct Setting {
        name: String,
        value: String,
        changed: u8,
    }

    #[tokio::test]
    async fn sparse_serialization_is_disabled() {
        let logctx = test_setup_log("sparse_serialization_is_disabled");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = reqwest::Client::new();
        let url = format!("http://{}", db.http_address());
        let setting: Setting = client
            .post(url)
            .body(
                "SELECT name, value, changed \
                FROM system.merge_tree_settings \
                WHERE name == 'ratio_of_defaults_for_sparse_serialization' \
                FORMAT JSONEachRow",
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(setting.name, "ratio_of_defaults_for_sparse_serialization");
        assert_eq!(setting.value, "1");
        assert_eq!(setting.changed, 1);
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
