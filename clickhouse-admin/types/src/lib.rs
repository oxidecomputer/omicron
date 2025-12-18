// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Error, Result, bail};
use atomicwrites::AtomicFile;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use daft::Diffable;
use derive_more::{Add, AddAssign, Display, From};
use itertools::Itertools;
use omicron_common::api::external::Generation;
use schemars::{
    JsonSchema,
    r#gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
};
use serde::{Deserialize, Serialize};
use slog::{Logger, info};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::create_dir;
use std::io::{ErrorKind, Write};
use std::net::Ipv6Addr;
use std::str::FromStr;

mod config;
pub use config::{
    ClickhouseHost, GenerateConfigResult, KeeperConfig,
    KeeperConfigsForReplica, KeeperNodeConfig, LogConfig, LogLevel, Macros,
    NodeType, RaftServerConfig, RaftServerSettings, RaftServers, ReplicaConfig,
    ServerNodeConfig,
};

pub const CLICKHOUSE_SERVER_CONFIG_DIR: &str =
    "/opt/oxide/clickhouse_server/config.d";
pub const CLICKHOUSE_SERVER_CONFIG_FILE: &str = "replica-server-config.xml";
pub const CLICKHOUSE_KEEPER_CONFIG_DIR: &str = "/opt/oxide/clickhouse_keeper";
pub const CLICKHOUSE_KEEPER_CONFIG_FILE: &str = "keeper_config.xml";
pub const OXIMETER_CLUSTER: &str = "oximeter_cluster";

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
pub fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

/// A unique ID for a ClickHouse Keeper
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    From,
    Add,
    AddAssign,
    Display,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct KeeperId(pub u64);

/// A unique ID for a Clickhouse Server
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    From,
    Add,
    AddAssign,
    Display,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct ServerId(pub u64);

/// The top most type for configuring clickhouse-servers via
/// clickhouse-admin-server-api
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ServerConfigurableSettings {
    /// A unique identifier for the configuration generation.
    pub generation: Generation,
    /// Configurable settings for a ClickHouse replica server node.
    pub settings: ServerSettings,
}

impl ServerConfigurableSettings {
    /// Generate a configuration file for a replica server node
    pub fn generate_xml_file(&self) -> Result<ReplicaConfig> {
        let logger = LogConfig::new(
            self.settings.datastore_path.clone(),
            NodeType::Server,
        );
        let macros = Macros::new(self.settings.id);

        let keepers: Vec<KeeperNodeConfig> = self
            .settings
            .keepers
            .iter()
            .map(|host| KeeperNodeConfig::new(host.clone()))
            .collect();

        let servers: Vec<ServerNodeConfig> = self
            .settings
            .remote_servers
            .iter()
            .map(|host| ServerNodeConfig::new(host.clone()))
            .collect();

        let config = ReplicaConfig::new(
            logger,
            macros,
            self.listen_addr(),
            servers.clone(),
            keepers.clone(),
            self.datastore_path(),
            self.generation(),
        );

        match create_dir(self.settings.config_dir.clone()) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        };

        let path = self.settings.config_dir.join("replica-server-config.xml");
        AtomicFile::new(
            path.clone(),
            atomicwrites::OverwriteBehavior::AllowOverwrite,
        )
        .write(|f| f.write_all(config.to_xml().as_bytes()))
        .with_context(|| format!("failed to write to `{}`", path))?;

        Ok(config)
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    fn listen_addr(&self) -> Ipv6Addr {
        self.settings.listen_addr
    }

    fn datastore_path(&self) -> Utf8PathBuf {
        self.settings.datastore_path.clone()
    }
}

/// The top most type for configuring clickhouse-servers via
/// clickhouse-admin-keeper-api
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct KeeperConfigurableSettings {
    /// A unique identifier for the configuration generation.
    pub generation: Generation,
    /// Configurable settings for a ClickHouse keeper node.
    pub settings: KeeperSettings,
}

impl KeeperConfigurableSettings {
    /// Generate a configuration file for a keeper node
    pub fn generate_xml_file(&self) -> Result<KeeperConfig> {
        let logger = LogConfig::new(
            self.settings.datastore_path.clone(),
            NodeType::Keeper,
        );

        let raft_servers = self
            .settings
            .raft_servers
            .iter()
            .map(|settings| RaftServerConfig::new(settings.clone()))
            .collect();
        let raft_config = RaftServers::new(raft_servers);

        let config = KeeperConfig::new(
            logger,
            self.listen_addr(),
            self.id(),
            self.datastore_path(),
            raft_config,
            self.generation(),
        );

        match create_dir(self.settings.config_dir.clone()) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        };

        let path = self.settings.config_dir.join("keeper_config.xml");
        AtomicFile::new(
            path.clone(),
            atomicwrites::OverwriteBehavior::AllowOverwrite,
        )
        .write(|f| f.write_all(config.to_xml().as_bytes()))
        .with_context(|| format!("failed to write to `{}`", path))?;

        Ok(config)
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    fn listen_addr(&self) -> Ipv6Addr {
        self.settings.listen_addr
    }

    fn id(&self) -> KeeperId {
        self.settings.id
    }

    fn datastore_path(&self) -> Utf8PathBuf {
        self.settings.datastore_path.clone()
    }
}

/// Configurable settings for a ClickHouse replica server node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ServerSettings {
    /// Directory for the generated server configuration XML file
    #[schemars(schema_with = "path_schema")]
    pub config_dir: Utf8PathBuf,
    /// Unique ID of the server node
    pub id: ServerId,
    /// Directory for all files generated by ClickHouse itself
    #[schemars(schema_with = "path_schema")]
    pub datastore_path: Utf8PathBuf,
    /// Address the server is listening on
    pub listen_addr: Ipv6Addr,
    /// Addresses for each of the individual nodes in the Keeper cluster
    pub keepers: Vec<ClickhouseHost>,
    /// Addresses for each of the individual replica servers
    pub remote_servers: Vec<ClickhouseHost>,
}

impl ServerSettings {
    pub fn new(
        config_dir: Utf8PathBuf,
        id: ServerId,
        datastore_path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        keepers: Vec<ClickhouseHost>,
        remote_servers: Vec<ClickhouseHost>,
    ) -> Self {
        Self {
            config_dir,
            id,
            datastore_path,
            listen_addr,
            keepers,
            remote_servers,
        }
    }
}

/// Configurable settings for a ClickHouse keeper node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct KeeperSettings {
    /// Directory for the generated keeper configuration XML file
    #[schemars(schema_with = "path_schema")]
    pub config_dir: Utf8PathBuf,
    /// Unique ID of the keeper node
    pub id: KeeperId,
    /// ID and host of each server in the keeper cluster
    pub raft_servers: Vec<RaftServerSettings>,
    /// Directory for all files generated by ClickHouse itself
    #[schemars(schema_with = "path_schema")]
    pub datastore_path: Utf8PathBuf,
    /// Address the keeper is listening on
    pub listen_addr: Ipv6Addr,
}

impl KeeperSettings {
    pub fn new(
        config_dir: Utf8PathBuf,
        id: KeeperId,
        raft_servers: Vec<RaftServerSettings>,
        datastore_path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
    ) -> Self {
        Self { config_dir, id, raft_servers, datastore_path, listen_addr }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Logically grouped information file from a keeper node
pub struct Lgif {
    /// Index of the first log entry in the current log segment
    pub first_log_idx: u64,
    /// Term of the leader when the first log entry was created
    pub first_log_term: u64,
    /// Index of the last log entry in the current log segment
    pub last_log_idx: u64,
    /// Term of the leader when the last log entry was created
    pub last_log_term: u64,
    /// Index of the last committed log entry
    pub last_committed_log_idx: u64,
    /// Index of the last committed log entry from the leader's perspective
    pub leader_committed_log_idx: u64,
    /// Target index for log commitment during replication or recovery
    pub target_committed_log_idx: u64,
    /// Index of the most recent snapshot taken
    pub last_snapshot_idx: u64,
}

impl Lgif {
    pub fn parse(log: &Logger, data: &[u8]) -> Result<Self> {
        // The reponse we get from running `clickhouse keeper-client -h {HOST} --q lgif`
        // isn't in any known format (e.g. JSON), but rather a series of lines with key-value
        // pairs separated by a tab:
        //
        // ```console
        // $ clickhouse keeper-client -h localhost -p 20001 --q lgif
        // first_log_idx	1
        // first_log_term	1
        // last_log_idx	10889
        // last_log_term	20
        // last_committed_log_idx	10889
        // leader_committed_log_idx	10889
        // target_committed_log_idx	10889
        // last_snapshot_idx	9465
        // ```
        let s = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `clickhouse keeper-client --q lgif`";
            "output" => ?s
        );

        let expected = Lgif::expected_keys();

        // Verify the output contains the same amount of lines as the expected keys.
        // This will ensure we catch any new key-value pairs appended to the lgif output.
        let lines = s.trim().lines();
        if expected.len() != lines.count() {
            bail!(
                "Output from the Keeper differs to the expected output keys \
                Output: {s:?} \
                Expected output keys: {expected:?}"
            );
        }

        let mut vals: Vec<u64> = Vec::new();
        for (line, expected_key) in s.lines().zip(expected.clone()) {
            let mut split = line.split('\t');
            let Some(key) = split.next() else {
                bail!("Returned None while attempting to retrieve key");
            };
            if key != expected_key {
                bail!(
                    "Extracted key `{key:?}` from output differs from expected key `{expected_key}`"
                );
            }
            let Some(val) = split.next() else {
                bail!(
                    "Command output has a line that does not contain a key-value pair: {key:?}"
                );
            };
            let val = match u64::from_str(val) {
                Ok(v) => v,
                Err(e) => bail!(
                    "Unable to convert value {val:?} into u64 for key {key}: {e}"
                ),
            };
            vals.push(val);
        }

        let mut iter = vals.into_iter();
        Ok(Lgif {
            first_log_idx: iter.next().unwrap(),
            first_log_term: iter.next().unwrap(),
            last_log_idx: iter.next().unwrap(),
            last_log_term: iter.next().unwrap(),
            last_committed_log_idx: iter.next().unwrap(),
            leader_committed_log_idx: iter.next().unwrap(),
            target_committed_log_idx: iter.next().unwrap(),
            last_snapshot_idx: iter.next().unwrap(),
        })
    }

    fn expected_keys() -> Vec<&'static str> {
        vec![
            "first_log_idx",
            "first_log_term",
            "last_log_idx",
            "last_log_term",
            "last_committed_log_idx",
            "leader_committed_log_idx",
            "target_committed_log_idx",
            "last_snapshot_idx",
        ]
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Deserialize,
    Ord,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum KeeperServerType {
    Participant,
    Learner,
}

impl FromStr for KeeperServerType {
    type Err = Error;

    fn from_str(s: &str) -> Result<KeeperServerType, Self::Err> {
        match s {
            "participant" => Ok(KeeperServerType::Participant),
            "learner" => Ok(KeeperServerType::Learner),
            _ => bail!("{s} is not a valid keeper server type"),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Deserialize,
    PartialOrd,
    Ord,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct KeeperServerInfo {
    /// Unique, immutable ID of the keeper server
    pub server_id: KeeperId,
    /// Host of the keeper server
    pub host: ClickhouseHost,
    /// Keeper server raft port
    pub raft_port: u16,
    /// A keeper server either participant or learner
    /// (learner does not participate in leader elections).
    pub server_type: KeeperServerType,
    /// non-negative integer telling which nodes should be
    /// prioritised on leader elections.
    /// Priority of 0 means server will never be a leader.
    pub priority: u16,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Deserialize,
    PartialOrd,
    Ord,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
/// Keeper raft configuration information
pub struct RaftConfig {
    pub keeper_servers: BTreeSet<KeeperServerInfo>,
}

impl RaftConfig {
    pub fn parse(log: &Logger, data: &[u8]) -> Result<Self> {
        // The response we get from `$ clickhouse keeper-client -h {HOST} --q 'get /keeper/config'
        // is a format unique to ClickHouse, where the data for each server is separated by a colon
        //
        // ```console
        // $ clickhouse keeper-client -h localhost --q 'get /keeper/config'
        // server.1=::1:21001;participant;1
        // server.2=::1:21002;participant;1
        // server.3=::1:21003;participant;1
        //```
        let s = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `clickhouse keeper-client --q 'get /keeper/config'`";
            "output" => ?s
        );

        if s.is_empty() {
            bail!("Cannot parse an empty response");
        }

        let mut keeper_servers = BTreeSet::new();
        for line in s.lines() {
            let mut split = line.split('=');
            let Some(server) = split.next() else {
                bail!(
                    "Returned None while attempting to retrieve raft configuration"
                );
            };

            // Retrieve server ID
            let mut split_server = server.split(".");
            let Some(s) = split_server.next() else {
                bail!(
                    "Returned None while attempting to retrieve server identifier"
                )
            };
            if s != "server" {
                bail!(
                    "Output is not as expected. \
                Server identifier: '{server}' \
                Expected server identifier: 'server.{{SERVER_ID}}'"
                )
            };
            let Some(id) = split_server.next() else {
                bail!("Returned None while attempting to retrieve server ID");
            };
            let u64_id = match u64::from_str(id) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value {id:?} into u64: {e}"),
            };
            let server_id = KeeperId(u64_id);

            // Retrieve server information
            let Some(info) = split.next() else {
                bail!("Returned None while attempting to retrieve server info");
            };
            let mut split_info = info.split(";");

            // Retrieve port
            let Some(address) = split_info.next() else {
                bail!("Returned None while attempting to retrieve address")
            };
            let Some(port) = address.split(':').next_back() else {
                bail!("A port could not be extracted from {address}")
            };
            let raft_port = match u16::from_str(port) {
                Ok(v) => v,
                Err(e) => {
                    bail!("Unable to convert value {port:?} into u16: {e}")
                }
            };

            // Retrieve host
            let p = format!(":{}", port);
            let Some(h) = address.split(&p).next() else {
                bail!(
                    "A host could not be extracted from {address}. Missing port {port}"
                )
            };
            // The ouput we get from running the clickhouse keeper-client
            // command does not add square brackets to an IPv6 address
            // that cointains a port: server.1=::1:21001;participant;1
            // Because of this, we can parse `h` directly into an Ipv6Addr
            let host = ClickhouseHost::from_str(h)?;

            // Retrieve server_type
            let Some(s_type) = split_info.next() else {
                bail!("Returned None while attempting to retrieve server type")
            };
            let server_type = KeeperServerType::from_str(s_type)?;

            // Retrieve priority
            let Some(s_priority) = split_info.next() else {
                bail!("Returned None while attempting to retrieve priority")
            };
            let priority = match u16::from_str(s_priority) {
                Ok(v) => v,
                Err(e) => {
                    bail!(
                        "Unable to convert value {s_priority:?} into u16: {e}"
                    )
                }
            };

            keeper_servers.insert(KeeperServerInfo {
                server_id,
                host,
                raft_port,
                server_type,
                priority,
            });
        }

        Ok(RaftConfig { keeper_servers })
    }
}

// While we generally use "Config", in this case we use "Conf"
// as it is the four letter word command we are invoking:
// `clickhouse keeper-client --q conf`
/// Keeper configuration information
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct KeeperConf {
    /// Unique server id, each participant of the ClickHouse Keeper cluster must
    /// have a unique number (1, 2, 3, and so on).
    pub server_id: KeeperId,
    /// Whether Ipv6 is enabled.
    pub enable_ipv6: bool,
    /// Port for a client to connect.
    pub tcp_port: u16,
    /// Allow list of 4lw commands.
    pub four_letter_word_allow_list: String,
    /// Max size of batch in requests count before it will be sent to RAFT.
    pub max_requests_batch_size: u64,
    /// Min timeout for client session (ms).
    pub min_session_timeout_ms: u64,
    /// Max timeout for client session (ms).
    pub session_timeout_ms: u64,
    /// Timeout for a single client operation (ms).
    pub operation_timeout_ms: u64,
    /// How often ClickHouse Keeper checks for dead sessions and removes them (ms).
    pub dead_session_check_period_ms: u64,
    /// How often a ClickHouse Keeper leader will send heartbeats to followers (ms).
    pub heart_beat_interval_ms: u64,
    /// If the follower does not receive a heartbeat from the leader in this interval,
    /// then it can initiate leader election. Must be less than or equal to
    /// election_timeout_upper_bound_ms. Ideally they shouldn't be equal.
    pub election_timeout_lower_bound_ms: u64,
    /// If the follower does not receive a heartbeat from the leader in this interval,
    /// then it must initiate leader election.
    pub election_timeout_upper_bound_ms: u64,
    /// How many coordination log records to store before compaction.
    pub reserved_log_items: u64,
    /// How often ClickHouse Keeper will create new snapshots
    /// (in the number of records in logs).
    pub snapshot_distance: u64,
    /// Allow to forward write requests from followers to the leader.
    pub auto_forwarding: bool,
    /// Wait to finish internal connections and shutdown (ms).
    pub shutdown_timeout: u64,
    /// If the server doesn't connect to other quorum participants in the specified
    /// timeout it will terminate (ms).
    pub startup_timeout: u64,
    /// Text logging level about coordination (trace, debug, and so on).
    pub raft_logs_level: LogLevel,
    /// How many snapshots to keep.
    pub snapshots_to_keep: u64,
    /// How many log records to store in a single file.
    pub rotate_log_storage_interval: u64,
    /// Threshold when leader considers follower as stale and sends the snapshot
    /// to it instead of logs.
    pub stale_log_gap: u64,
    /// When the node became fresh.
    pub fresh_log_gap: u64,
    /// Max size in bytes of batch of requests that can be sent to RAFT.
    pub max_requests_batch_bytes_size: u64,
    /// Maximum number of requests that can be in queue for processing.
    pub max_request_queue_size: u64,
    /// Max size of batch of requests to try to get before proceeding with RAFT.
    /// Keeper will not wait for requests but take only requests that are already
    /// in the queue.
    pub max_requests_quick_batch_size: u64,
    /// Whether to execute read requests as writes through whole RAFT consesus with
    /// similar speed.
    pub quorum_reads: bool,
    /// Whether to call fsync on each change in RAFT changelog.
    pub force_sync: bool,
    /// Whether to write compressed coordination logs in ZSTD format.
    pub compress_logs: bool,
    /// Whether to write compressed snapshots in ZSTD format (instead of custom LZ4).
    pub compress_snapshots_with_zstd_format: bool,
    /// How many times we will try to apply configuration change (add/remove server)
    /// to the cluster.
    pub configuration_change_tries_count: u64,
    /// If connection to a peer is silent longer than this limit * (heartbeat interval),
    /// we re-establish the connection.
    pub raft_limits_reconnect_limit: u64,
    /// Path to coordination logs, just like ZooKeeper it is best to store logs
    /// on non-busy nodes.
    #[schemars(schema_with = "path_schema")]
    pub log_storage_path: Utf8PathBuf,
    /// Name of disk used for logs.
    pub log_storage_disk: String,
    /// Path to coordination snapshots.
    #[schemars(schema_with = "path_schema")]
    pub snapshot_storage_path: Utf8PathBuf,
    /// Name of disk used for storage.
    pub snapshot_storage_disk: String,
}

impl KeeperConf {
    pub fn parse(log: &Logger, data: &[u8]) -> Result<Self> {
        // Like Lgif, the reponse we get from running `clickhouse keeper-client -h {HOST} --q conf`
        // isn't in any known format (e.g. JSON), but rather a series of lines with key-value
        // pairs separated by a tab.
        let s = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `clickhouse keeper-client --q conf`";
            "output" => ?s
        );

        let expected = KeeperConf::expected_keys();

        // Verify the output contains the same amount of lines as the expected keys.
        // This will ensure we catch any new key-value pairs appended to the lgif output.
        let lines = s.trim().lines();
        if expected.len() != lines.count() {
            bail!(
                "Output from the Keeper differs to the expected output keys \
                Output: {s:?} \
                Expected output keys: {expected:?}"
            );
        }

        let mut vals: Vec<&str> = Vec::new();
        // The output from the `conf` command contains the `max_requests_batch_size` field
        // twice. We make sure to only read it once.
        for (line, expected_key) in s.lines().zip(expected.clone()).unique() {
            let mut split = line.split('=');
            let Some(key) = split.next() else {
                bail!("Returned None while attempting to retrieve key");
            };
            if key != expected_key {
                bail!(
                    "Extracted key `{key:?}` from output differs from expected key `{expected_key}`"
                );
            }
            let Some(val) = split.next() else {
                bail!(
                    "Command output has a line that does not contain a key-value pair: {key:?}"
                );
            };
            vals.push(val);
        }

        let mut iter = vals.into_iter();
        let server_id = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => KeeperId(v),
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let enable_ipv6 = match bool::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into bool: {e}"),
        };

        let tcp_port = match u16::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u16: {e}"),
        };

        let four_letter_word_allow_list = iter.next().unwrap().to_string();

        let max_requests_batch_size = match u64::from_str(iter.next().unwrap())
        {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let min_session_timeout_ms = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let session_timeout_ms = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let operation_timeout_ms = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let dead_session_check_period_ms =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64: {e}"),
            };

        let heart_beat_interval_ms = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let election_timeout_lower_bound_ms =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64: {e}"),
            };

        let election_timeout_upper_bound_ms =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64: {e}"),
            };

        let reserved_log_items = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let snapshot_distance = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let auto_forwarding = match bool::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into bool: {e}"),
        };

        let shutdown_timeout = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64 {e}"),
        };

        let startup_timeout = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let raft_logs_level = match LogLevel::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into LogLevel: {e}"),
        };

        let snapshots_to_keep = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let rotate_log_storage_interval =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64 {e}"),
            };

        let stale_log_gap = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let fresh_log_gap = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64: {e}"),
        };

        let max_requests_batch_bytes_size =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64 {e}"),
            };

        let max_request_queue_size = match u64::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into u64 {e}"),
        };

        let max_requests_quick_batch_size =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64 {e}"),
            };

        let quorum_reads = match bool::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into bool: {e}"),
        };

        let force_sync = match bool::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into bool: {e}"),
        };

        let compress_logs = match bool::from_str(iter.next().unwrap()) {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into bool: {e}"),
        };

        let compress_snapshots_with_zstd_format =
            match bool::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into bool: {e}"),
            };

        let configuration_change_tries_count =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64: {e}"),
            };

        let raft_limits_reconnect_limit =
            match u64::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => bail!("Unable to convert value into u64: {e}"),
            };

        let log_storage_path = match Utf8PathBuf::from_str(iter.next().unwrap())
        {
            Ok(v) => v,
            Err(e) => bail!("Unable to convert value into Utf8PathBuf: {e}"),
        };

        let log_storage_disk = iter.next().unwrap().to_string();

        let snapshot_storage_path =
            match Utf8PathBuf::from_str(iter.next().unwrap()) {
                Ok(v) => v,
                Err(e) => {
                    bail!("Unable to convert value into Utf8PathBuf: {e}")
                }
            };

        let snapshot_storage_disk = iter.next().unwrap().to_string();

        Ok(Self {
            server_id,
            enable_ipv6,
            tcp_port,
            four_letter_word_allow_list,
            max_requests_batch_size,
            min_session_timeout_ms,
            session_timeout_ms,
            operation_timeout_ms,
            dead_session_check_period_ms,
            heart_beat_interval_ms,
            election_timeout_lower_bound_ms,
            election_timeout_upper_bound_ms,
            reserved_log_items,
            snapshot_distance,
            auto_forwarding,
            shutdown_timeout,
            startup_timeout,
            raft_logs_level,
            snapshots_to_keep,
            rotate_log_storage_interval,
            stale_log_gap,
            fresh_log_gap,
            max_requests_batch_bytes_size,
            max_request_queue_size,
            max_requests_quick_batch_size,
            quorum_reads,
            force_sync,
            compress_logs,
            compress_snapshots_with_zstd_format,
            configuration_change_tries_count,
            raft_limits_reconnect_limit,
            log_storage_path,
            log_storage_disk,
            snapshot_storage_path,
            snapshot_storage_disk,
        })
    }

    fn expected_keys() -> Vec<&'static str> {
        vec![
            "server_id",
            "enable_ipv6",
            "tcp_port",
            "four_letter_word_allow_list",
            "max_requests_batch_size",
            "min_session_timeout_ms",
            "session_timeout_ms",
            "operation_timeout_ms",
            "dead_session_check_period_ms",
            "heart_beat_interval_ms",
            "election_timeout_lower_bound_ms",
            "election_timeout_upper_bound_ms",
            "reserved_log_items",
            "snapshot_distance",
            "auto_forwarding",
            "shutdown_timeout",
            "startup_timeout",
            "raft_logs_level",
            "snapshots_to_keep",
            "rotate_log_storage_interval",
            "stale_log_gap",
            "fresh_log_gap",
            "max_requests_batch_size",
            "max_requests_batch_bytes_size",
            "max_request_queue_size",
            "max_requests_quick_batch_size",
            "quorum_reads",
            "force_sync",
            "compress_logs",
            "compress_snapshots_with_zstd_format",
            "configuration_change_tries_count",
            "raft_limits_reconnect_limit",
            "log_storage_path",
            "log_storage_disk",
            "snapshot_storage_path",
            "snapshot_storage_disk",
        ]
    }
}

/// The configuration of the clickhouse keeper raft cluster returned from a
/// single keeper node
///
/// Each keeper is asked for its known raft configuration via `clickhouse-admin`
/// dropshot servers running in `ClickhouseKeeper` zones. state. We include the
/// leader committed log index known to the current keeper node (whether or not
/// it is the leader) to determine which configuration is newest.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct ClickhouseKeeperClusterMembership {
    /// Keeper ID of the keeper being queried
    pub queried_keeper: KeeperId,
    /// Index of the last committed log entry from the leader's perspective
    pub leader_committed_log_index: u64,
    /// Keeper IDs of all keepers in the cluster
    pub raft_config: BTreeSet<KeeperId>,
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
/// Contains information about distributed ddl queries (ON CLUSTER clause) that were
/// executed on a cluster.
pub struct DistributedDdlQueue {
    /// Query id
    pub entry: String,
    /// Version of the entry
    pub entry_version: u64,
    /// Host that initiated the DDL operation
    pub initiator_host: String,
    /// Port used by the initiator
    pub initiator_port: u16,
    /// Cluster name
    pub cluster: String,
    /// Query executed
    pub query: String,
    /// Settings used in the DDL operation
    pub settings: BTreeMap<String, String>,
    /// Query created time
    pub query_create_time: DateTime<Utc>,
    /// Hostname
    pub host: Ipv6Addr,
    /// Host Port
    pub port: u16,
    /// Status of the query
    pub status: String,
    /// Exception code
    pub exception_code: u64,
    /// Exception message
    pub exception_text: String,
    /// Query finish time
    pub query_finish_time: DateTime<Utc>,
    /// Duration of query execution (in milliseconds)
    pub query_duration_ms: u64,
}

impl DistributedDdlQueue {
    pub fn parse(log: &Logger, data: &[u8]) -> Result<Vec<Self>> {
        let s = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `system.distributed_ddl_queue`";
            "output" => ?s
        );

        let mut ddl = vec![];

        for line in s.lines() {
            let item: DistributedDdlQueue = serde_json::from_str(line)?;
            ddl.push(item);
        }

        Ok(ddl)
    }
}

#[inline]
fn default_interval() -> u64 {
    60
}

#[inline]
fn default_time_range() -> u64 {
    86400
}

#[inline]
fn default_timestamp_format() -> TimestampFormat {
    TimestampFormat::Utc
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Available metrics tables in the `system` database
pub enum SystemTable {
    AsynchronousMetricLog,
    MetricLog,
}

impl fmt::Display for SystemTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = match self {
            SystemTable::MetricLog => "metric_log",
            SystemTable::AsynchronousMetricLog => "asynchronous_metric_log",
        };
        write!(f, "{}", table)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Which format should the timestamp be in.
pub enum TimestampFormat {
    Utc,
    UnixEpoch,
}

impl fmt::Display for TimestampFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = match self {
            TimestampFormat::Utc => "iso",
            TimestampFormat::UnixEpoch => "unix_timestamp",
        };
        write!(f, "{}", table)
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct MetricInfoPath {
    /// Table to query in the `system` database
    pub table: SystemTable,
    /// Name of the metric to retrieve.
    pub metric: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TimeSeriesSettingsQuery {
    /// The interval to collect monitoring metrics in seconds.
    /// Default is 60 seconds.
    #[serde(default = "default_interval")]
    pub interval: u64,
    /// Range of time to collect monitoring metrics in seconds.
    /// Default is 86400 seconds (24 hrs).
    #[serde(default = "default_time_range")]
    pub time_range: u64,
    /// Format in which each timeseries timestamp will be in.
    /// Default is UTC
    #[serde(default = "default_timestamp_format")]
    pub timestamp_format: TimestampFormat,
}

/// Settings to specify which time series to retrieve.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SystemTimeSeriesSettings {
    /// Time series retrieval settings (time range and interval)
    pub retrieval_settings: TimeSeriesSettingsQuery,
    /// Database table and name of the metric to retrieve
    pub metric_info: MetricInfoPath,
}

impl SystemTimeSeriesSettings {
    fn interval(&self) -> u64 {
        self.retrieval_settings.interval
    }

    fn time_range(&self) -> u64 {
        self.retrieval_settings.time_range
    }

    fn timestamp_format(&self) -> TimestampFormat {
        self.retrieval_settings.timestamp_format
    }

    fn metric_name(&self) -> &str {
        &self.metric_info.metric
    }

    fn table(&self) -> SystemTable {
        self.metric_info.table
    }

    // TODO: Use more aggregate functions than just avg?
    pub fn query_avg(&self) -> String {
        let interval = self.interval();
        let time_range = self.time_range();
        let metric_name = self.metric_name();
        let table = self.table();
        let ts_fmt = self.timestamp_format();

        let avg_value = match table {
            SystemTable::MetricLog => metric_name,
            SystemTable::AsynchronousMetricLog => "value",
        };

        let mut query = format!(
            "SELECT toStartOfInterval(event_time, INTERVAL {interval} SECOND) AS time, avg({avg_value}) AS value
            FROM system.{table}
            WHERE event_date >= toDate(now() - {time_range}) AND event_time >= now() - {time_range}
            "
        );

        match table {
            SystemTable::MetricLog => (),
            SystemTable::AsynchronousMetricLog => query.push_str(
                format!(
                    "AND metric = '{metric_name}'
                "
                )
                .as_str(),
            ),
        };

        query.push_str(
            format!(
                "GROUP BY time
                ORDER BY time WITH FILL STEP {interval}
                FORMAT JSONEachRow
                SETTINGS date_time_output_format = '{ts_fmt}'"
            )
            .as_str(),
        );
        query
    }
}

// Our OpenAPI generator does not allow for enums to be of different
// primitive types. Because Utc is a "string" in json, Unix cannot be an int.
// This is why we set it as a `String`.
#[derive(Debug, Display, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum Timestamp {
    Utc(DateTime<Utc>),
    Unix(String),
}

/// Retrieved time series from the internal `system` database.
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SystemTimeSeries {
    pub time: String,
    pub value: f64,
    // TODO: Would be really nice to have an enum with possible units (s, ms, bytes)
    // Not sure if I can even add this, the system tables don't mention units at all.
}

impl SystemTimeSeries {
    pub fn parse(log: &Logger, data: &[u8]) -> Result<Vec<Self>> {
        let s = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `system` database";
            "output" => ?s
        );

        let mut m = vec![];

        for line in s.lines() {
            // serde_json deserialises f64 types with loss of precision at times.
            // For example, in our tests some of the values to serialize have a
            // fractional value of `.33333`, but once parsed, they become `.33331`.
            //
            // We do not require this level of precision, so we'll leave as is.
            // Just noting that we are aware of this slight inaccuracy.
            let item: SystemTimeSeries = serde_json::from_str(line)?;
            m.push(item);
        }

        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf;
    use camino_tempfile::Builder;
    use chrono::{DateTime, Utc};
    use omicron_common::api::external::Generation;
    use slog::{Drain, o};
    use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::str::FromStr;

    use crate::{
        ClickhouseHost, DistributedDdlQueue, KeeperConf,
        KeeperConfigurableSettings, KeeperId, KeeperServerInfo,
        KeeperServerType, KeeperSettings, Lgif, LogLevel, RaftConfig,
        RaftServerSettings, ServerConfigurableSettings, ServerId,
        ServerSettings, SystemTimeSeries,
    };

    fn log() -> slog::Logger {
        let decorator = PlainDecorator::new(TestStdoutWriter);
        let drain = FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[test]
    fn test_generate_keeper_config() {
        let config_dir = Builder::new()
            .tempdir_in(
                Utf8PathBuf::try_from(std::env::temp_dir()).unwrap())
                .expect("Could not create directory for ClickHouse configuration generation test"
            );

        let keepers = vec![
            RaftServerSettings {
                id: KeeperId(1),
                host: ClickhouseHost::Ipv6(
                    Ipv6Addr::from_str("ff::01").unwrap(),
                ),
            },
            RaftServerSettings {
                id: KeeperId(2),
                host: ClickhouseHost::Ipv4(
                    Ipv4Addr::from_str("127.0.0.1").unwrap(),
                ),
            },
            RaftServerSettings {
                id: KeeperId(3),
                host: ClickhouseHost::DomainName("ohai.com".to_string()),
            },
        ];

        let settings = KeeperSettings::new(
            Utf8PathBuf::from(config_dir.path()),
            KeeperId(1),
            keepers,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
        );

        let config = KeeperConfigurableSettings {
            generation: Generation::new(),
            settings,
        };

        config.generate_xml_file().unwrap();

        let expected_file = Utf8PathBuf::from_str("./testutils")
            .unwrap()
            .join("keeper_config.xml");
        let generated_file =
            Utf8PathBuf::from(config_dir.path()).join("keeper_config.xml");
        let generated_content = std::fs::read_to_string(generated_file)
            .expect("Failed to read from generated ClickHouse keeper file");

        expectorate::assert_contents(expected_file, &generated_content);
    }

    #[test]
    fn test_generate_replica_config() {
        let config_dir = Builder::new()
        .tempdir_in(
            Utf8PathBuf::try_from(std::env::temp_dir()).unwrap())
            .expect("Could not create directory for ClickHouse configuration generation test"
        );

        let keepers = vec![
            ClickhouseHost::Ipv6(Ipv6Addr::from_str("ff::01").unwrap()),
            ClickhouseHost::Ipv4(Ipv4Addr::from_str("127.0.0.1").unwrap()),
            ClickhouseHost::DomainName("we.dont.want.brackets.com".to_string()),
        ];

        let servers = vec![
            ClickhouseHost::Ipv6(Ipv6Addr::from_str("ff::09").unwrap()),
            ClickhouseHost::DomainName("ohai.com".to_string()),
        ];

        let settings = ServerSettings::new(
            Utf8PathBuf::from(config_dir.path()),
            ServerId(1),
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
            keepers,
            servers,
        );

        let config = ServerConfigurableSettings {
            settings,
            generation: Generation::new(),
        };
        config.generate_xml_file().unwrap();

        let expected_file = Utf8PathBuf::from_str("./testutils")
            .unwrap()
            .join("replica-server-config.xml");
        let generated_file = Utf8PathBuf::from(config_dir.path())
            .join("replica-server-config.xml");
        let generated_content = std::fs::read_to_string(generated_file).expect(
            "Failed to read from generated ClickHouse replica server file",
        );

        expectorate::assert_contents(expected_file, &generated_content);
    }

    #[test]
    fn test_full_lgif_parse_success() {
        let log = log();
        let data =
            "first_log_idx\t1\nfirst_log_term\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386\nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let lgif = Lgif::parse(&log, data).unwrap();

        assert!(lgif.first_log_idx == 1);
        assert!(lgif.first_log_term == 1);
        assert!(lgif.last_log_idx == 4386);
        assert!(lgif.last_log_term == 1);
        assert!(lgif.last_committed_log_idx == 4386);
        assert!(lgif.leader_committed_log_idx == 4386);
        assert!(lgif.target_committed_log_idx == 4386);
        assert!(lgif.last_snapshot_idx == 0);
    }

    #[test]
    fn test_missing_keys_lgif_parse_fail() {
        let log = log();
        let data =
            "first_log_idx\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386\nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Output from the Keeper differs to the expected output keys \
            Output: \"first_log_idx\\t1\\nlast_log_idx\\t4386\\nlast_log_term\\t1\\nlast_committed_log_idx\\t4386\\nleader_committed_log_idx\\t4386\\ntarget_committed_log_idx\\t4386\\nlast_snapshot_idx\\t0\\n\\n\" \
            Expected output keys: [\"first_log_idx\", \"first_log_term\", \"last_log_idx\", \"last_log_term\", \"last_committed_log_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\", \"last_snapshot_idx\"]"
        );
    }

    #[test]
    fn test_empty_value_lgif_parse_fail() {
        let log = log();
        let data =
            "first_log_idx\nfirst_log_term\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386\nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Command output has a line that does not contain a key-value pair: \"first_log_idx\""
        );
    }

    #[test]
    fn test_non_u64_value_lgif_parse_fail() {
        let log = log();
        let data =
            "first_log_idx\t1\nfirst_log_term\tBOB\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386\nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value \"BOB\" into u64 for key first_log_term: invalid digit found in string"
        );
    }

    #[test]
    fn test_non_existent_key_with_correct_value_lgif_parse_fail() {
        let log = log();
        let data =
            "first_log_idx\t1\nfirst_log\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386\nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Extracted key `\"first_log\"` from output differs from expected key `first_log_term`"
        );
    }

    #[test]
    fn test_additional_key_value_pairs_in_output_parse_fail() {
        let log = log();
        let data = "first_log_idx\t1\nfirst_log_term\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386\nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\nlast_snapshot_idx\t3\n\n"
            .as_bytes();

        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Output from the Keeper differs to the expected output keys \
            Output: \"first_log_idx\\t1\\nfirst_log_term\\t1\\nlast_log_idx\\t4386\\nlast_log_term\\t1\\nlast_committed_log_idx\\t4386\\nleader_committed_log_idx\\t4386\\ntarget_committed_log_idx\\t4386\\nlast_snapshot_idx\\t0\\nlast_snapshot_idx\\t3\\n\\n\" \
            Expected output keys: [\"first_log_idx\", \"first_log_term\", \"last_log_idx\", \"last_log_term\", \"last_committed_log_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\", \"last_snapshot_idx\"]",
        );
    }

    #[test]
    fn test_empty_output_parse_fail() {
        let log = log();
        let data = "".as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Output from the Keeper differs to the expected output keys \
           Output: \"\" \
           Expected output keys: [\"first_log_idx\", \"first_log_term\", \"last_log_idx\", \"last_log_term\", \"last_committed_log_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\", \"last_snapshot_idx\"]",
        );
    }

    #[test]
    fn test_full_raft_config_parse_success() {
        let log = log();
        let data =
            "server.1=::1:21001;participant;1\nserver.2=oxide.internal:21002;participant;1\nserver.3=127.0.0.1:21003;learner;0\n"
            .as_bytes();
        let raft_config = RaftConfig::parse(&log, data).unwrap();

        assert!(raft_config.keeper_servers.contains(&KeeperServerInfo {
            server_id: KeeperId(1),
            host: ClickhouseHost::Ipv6("::1".parse().unwrap()),
            raft_port: 21001,
            server_type: KeeperServerType::Participant,
            priority: 1,
        },));
        assert!(raft_config.keeper_servers.contains(&KeeperServerInfo {
            server_id: KeeperId(2),
            host: ClickhouseHost::DomainName("oxide.internal".to_string()),
            raft_port: 21002,
            server_type: KeeperServerType::Participant,
            priority: 1,
        },));
        assert!(raft_config.keeper_servers.contains(&KeeperServerInfo {
            server_id: KeeperId(3),
            host: ClickhouseHost::Ipv4("127.0.0.1".parse().unwrap()),
            raft_port: 21003,
            server_type: KeeperServerType::Learner,
            priority: 0,
        },));
    }

    #[test]
    fn test_misshapen_id_raft_config_parse_fail() {
        let log = log();
        let data = "serv.1=::1:21001;participant;1\n".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Output is not as expected. Server identifier: 'serv.1' Expected server identifier: 'server.{SERVER_ID}'",
        );
    }

    #[test]
    fn test_misshapen_port_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=::1:BOB;participant;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value \"BOB\" into u16: invalid digit found in string",
        );
    }

    #[test]
    fn test_empty_output_raft_config_parse_fail() {
        let log = log();
        let data = "".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(format!("{}", root_cause), "Cannot parse an empty response",);
    }

    #[test]
    fn test_missing_server_id_raft_config_parse_fail() {
        let log = log();
        let data = "server.=::1:21001;participant;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value \"\" into u64: cannot parse integer from empty string",
        );
    }

    #[test]
    fn test_missing_address_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=:21001;participant;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            " is not a valid address or domain name",
        );
    }

    #[test]
    fn test_invalid_address_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=oxide.com:21001;participant;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "oxide.com is not a valid address or domain name",
        );
    }

    #[test]
    fn test_missing_port_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=::1:;participant;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value \"\" into u16: cannot parse integer from empty string",
        );
    }

    #[test]
    fn test_missing_participant_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=::1:21001;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "1 is not a valid keeper server type",
        );

        let data = "server.1=::1:21001;;1".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            " is not a valid keeper server type",
        );
    }

    #[test]
    fn test_misshapen_participant_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=::1:21001;runner;1\n".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "runner is not a valid keeper server type",
        );
    }

    #[test]
    fn test_missing_priority_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=::1:21001;learner;\n".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value \"\" into u16: cannot parse integer from empty string",
        );

        let data = "server.1=::1:21001;learner\n".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Returned None while attempting to retrieve priority",
        );
    }

    #[test]
    fn test_misshapen_priority_raft_config_parse_fail() {
        let log = log();
        let data = "server.1=::1:21001;learner;BOB\n".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value \"BOB\" into u16: invalid digit found in string",
        );
    }

    #[test]
    fn test_misshapen_raft_config_parse_fail() {
        let log = log();
        let data = "=;;\n".as_bytes();
        let result = RaftConfig::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Output is not as expected. Server identifier: '' Expected server identifier: 'server.{SERVER_ID}'",
        );
    }

    #[test]
    fn test_full_keeper_conf_parse_success() {
        let log = log();
        // This data contains the duplicated "max_requests_batch_size" that occurs in the
        // real conf command output
        let data =
            "server_id=1
enable_ipv6=true
tcp_port=20001
four_letter_word_allow_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl
max_requests_batch_size=100
min_session_timeout_ms=10000
session_timeout_ms=30000
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=100000
snapshot_distance=100000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=180000
raft_logs_level=trace
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
max_requests_batch_bytes_size=102400
max_request_queue_size=100000
max_requests_quick_batch_size=100
quorum_reads=false
force_sync=true
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
raft_limits_reconnect_limit=50
log_storage_path=./deployment/keeper-1/coordination/log
log_storage_disk=LocalLogDisk
snapshot_storage_path=./deployment/keeper-1/coordination/snapshots
snapshot_storage_disk=LocalSnapshotDisk
\n"
            .as_bytes();
        let conf = KeeperConf::parse(&log, data).unwrap();

        assert!(conf.server_id == KeeperId(1));
        assert!(conf.enable_ipv6);
        assert!(conf.tcp_port == 20001);
        assert!(
            conf.four_letter_word_allow_list
                == *"conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl"
        );
        assert!(conf.max_requests_batch_size == 100);
        assert!(conf.min_session_timeout_ms == 10000);
        assert!(conf.session_timeout_ms == 30000);
        assert!(conf.operation_timeout_ms == 10000);
        assert!(conf.dead_session_check_period_ms == 500);
        assert!(conf.heart_beat_interval_ms == 500);
        assert!(conf.election_timeout_lower_bound_ms == 1000);
        assert!(conf.election_timeout_upper_bound_ms == 2000);
        assert!(conf.reserved_log_items == 100000);
        assert!(conf.snapshot_distance == 100000);
        assert!(conf.auto_forwarding);
        assert!(conf.shutdown_timeout == 5000);
        assert!(conf.startup_timeout == 180000);
        assert!(conf.raft_logs_level == LogLevel::Trace);
        assert!(conf.snapshots_to_keep == 3);
        assert!(conf.rotate_log_storage_interval == 100000);
        assert!(conf.stale_log_gap == 10000);
        assert!(conf.fresh_log_gap == 200);
        assert!(conf.max_requests_batch_bytes_size == 102400);
        assert!(conf.max_request_queue_size == 100000);
        assert!(conf.max_requests_quick_batch_size == 100);
        assert!(!conf.quorum_reads);
        assert!(conf.force_sync);
        assert!(conf.compress_logs);
        assert!(conf.compress_snapshots_with_zstd_format);
        assert!(conf.configuration_change_tries_count == 20);
        assert!(conf.raft_limits_reconnect_limit == 50);
        assert!(
            conf.log_storage_path
                == Utf8PathBuf::from_str(
                    "./deployment/keeper-1/coordination/log"
                )
                .unwrap()
        );
        assert!(conf.log_storage_disk == *"LocalLogDisk");
        assert!(
            conf.snapshot_storage_path
                == Utf8PathBuf::from_str(
                    "./deployment/keeper-1/coordination/snapshots"
                )
                .unwrap()
        );
        assert!(conf.snapshot_storage_disk == *"LocalSnapshotDisk")
    }

    #[test]
    fn test_missing_value_keeper_conf_parse_fail() {
        let log = log();
        // This data contains the duplicated "max_requests_batch_size" that occurs in the
        // real conf command output
        let data =
            "server_id=1
enable_ipv6=true
tcp_port=20001
four_letter_word_allow_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl
max_requests_batch_size=100
min_session_timeout_ms=10000
session_timeout_ms=
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=100000
snapshot_distance=100000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=180000
raft_logs_level=trace
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
max_requests_batch_bytes_size=102400
max_request_queue_size=100000
max_requests_quick_batch_size=100
quorum_reads=false
force_sync=true
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
raft_limits_reconnect_limit=50
log_storage_path=./deployment/keeper-1/coordination/log
log_storage_disk=LocalLogDisk
snapshot_storage_path=./deployment/keeper-1/coordination/snapshots
snapshot_storage_disk=LocalSnapshotDisk
\n"
            .as_bytes();
        let result = KeeperConf::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Unable to convert value into u64: cannot parse integer from empty string"
        );
    }

    #[test]
    fn test_malformed_output_keeper_conf_parse_fail() {
        let log = log();
        // This data contains the duplicated "max_requests_batch_size" that occurs in the
        // real conf command output
        let data =
            "server_id=1
enable_ipv6=true
tcp_port=20001
four_letter_word_allow_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl
max_requests_batch_size=100
min_session_timeout_ms=10000
session_timeout_ms
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=100000
snapshot_distance=100000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=180000
raft_logs_level=trace
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
max_requests_batch_bytes_size=102400
max_request_queue_size=100000
max_requests_quick_batch_size=100
quorum_reads=false
force_sync=true
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
raft_limits_reconnect_limit=50
log_storage_path=./deployment/keeper-1/coordination/log
log_storage_disk=LocalLogDisk
snapshot_storage_path=./deployment/keeper-1/coordination/snapshots
snapshot_storage_disk=LocalSnapshotDisk
\n"
            .as_bytes();
        let result = KeeperConf::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Command output has a line that does not contain a key-value pair: \"session_timeout_ms\""
        );
    }

    #[test]
    fn test_missing_field_keeper_conf_parse_fail() {
        let log = log();
        // This data contains the duplicated "max_requests_batch_size" that occurs in the
        // real conf command output
        let data =
            "server_id=1
enable_ipv6=true
tcp_port=20001
four_letter_word_allow_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl
max_requests_batch_size=100
min_session_timeout_ms=10000
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=100000
snapshot_distance=100000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=180000
raft_logs_level=trace
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
max_requests_batch_bytes_size=102400
max_request_queue_size=100000
max_requests_quick_batch_size=100
quorum_reads=false
force_sync=true
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
raft_limits_reconnect_limit=50
log_storage_path=./deployment/keeper-1/coordination/log
log_storage_disk=LocalLogDisk
snapshot_storage_path=./deployment/keeper-1/coordination/snapshots
snapshot_storage_disk=LocalSnapshotDisk
\n"
            .as_bytes();
        let result = KeeperConf::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Output from the Keeper differs to the expected output keys \
            Output: \"server_id=1\\nenable_ipv6=true\\ntcp_port=20001\\nfour_letter_word_allow_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl\\nmax_requests_batch_size=100\\nmin_session_timeout_ms=10000\\noperation_timeout_ms=10000\\ndead_session_check_period_ms=500\\nheart_beat_interval_ms=500\\nelection_timeout_lower_bound_ms=1000\\nelection_timeout_upper_bound_ms=2000\\nreserved_log_items=100000\\nsnapshot_distance=100000\\nauto_forwarding=true\\nshutdown_timeout=5000\\nstartup_timeout=180000\\nraft_logs_level=trace\\nsnapshots_to_keep=3\\nrotate_log_storage_interval=100000\\nstale_log_gap=10000\\nfresh_log_gap=200\\nmax_requests_batch_size=100\\nmax_requests_batch_bytes_size=102400\\nmax_request_queue_size=100000\\nmax_requests_quick_batch_size=100\\nquorum_reads=false\\nforce_sync=true\\ncompress_logs=true\\ncompress_snapshots_with_zstd_format=true\\nconfiguration_change_tries_count=20\\nraft_limits_reconnect_limit=50\\nlog_storage_path=./deployment/keeper-1/coordination/log\\nlog_storage_disk=LocalLogDisk\\nsnapshot_storage_path=./deployment/keeper-1/coordination/snapshots\\nsnapshot_storage_disk=LocalSnapshotDisk\\n\\n\" \
            Expected output keys: [\"server_id\", \"enable_ipv6\", \"tcp_port\", \"four_letter_word_allow_list\", \"max_requests_batch_size\", \"min_session_timeout_ms\", \"session_timeout_ms\", \"operation_timeout_ms\", \"dead_session_check_period_ms\", \"heart_beat_interval_ms\", \"election_timeout_lower_bound_ms\", \"election_timeout_upper_bound_ms\", \"reserved_log_items\", \"snapshot_distance\", \"auto_forwarding\", \"shutdown_timeout\", \"startup_timeout\", \"raft_logs_level\", \"snapshots_to_keep\", \"rotate_log_storage_interval\", \"stale_log_gap\", \"fresh_log_gap\", \"max_requests_batch_size\", \"max_requests_batch_bytes_size\", \"max_request_queue_size\", \"max_requests_quick_batch_size\", \"quorum_reads\", \"force_sync\", \"compress_logs\", \"compress_snapshots_with_zstd_format\", \"configuration_change_tries_count\", \"raft_limits_reconnect_limit\", \"log_storage_path\", \"log_storage_disk\", \"snapshot_storage_path\", \"snapshot_storage_disk\"]"
        );
    }

    #[test]
    fn test_non_existent_key_keeper_conf_parse_fail() {
        let log = log();
        // This data contains the duplicated "max_requests_batch_size" that occurs in the
        // real conf command output
        let data =
            "server_id=1
enable_ipv6=true
tcp_port=20001
four_letter_word_allow_list=conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl
max_requests_batch_size=100
min_session_timeout_ms=10000
session_timeout_fake=100
operation_timeout_ms=10000
dead_session_check_period_ms=500
heart_beat_interval_ms=500
election_timeout_lower_bound_ms=1000
election_timeout_upper_bound_ms=2000
reserved_log_items=100000
snapshot_distance=100000
auto_forwarding=true
shutdown_timeout=5000
startup_timeout=180000
raft_logs_level=trace
snapshots_to_keep=3
rotate_log_storage_interval=100000
stale_log_gap=10000
fresh_log_gap=200
max_requests_batch_size=100
max_requests_batch_bytes_size=102400
max_request_queue_size=100000
max_requests_quick_batch_size=100
quorum_reads=false
force_sync=true
compress_logs=true
compress_snapshots_with_zstd_format=true
configuration_change_tries_count=20
raft_limits_reconnect_limit=50
log_storage_path=./deployment/keeper-1/coordination/log
log_storage_disk=LocalLogDisk
snapshot_storage_path=./deployment/keeper-1/coordination/snapshots
snapshot_storage_disk=LocalSnapshotDisk
\n"
            .as_bytes();
        let result = KeeperConf::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Extracted key `\"session_timeout_fake\"` from output differs from expected key `session_timeout_ms`"
        );
    }

    #[test]
    fn test_distributed_ddl_queries_parse_success() {
        let log = log();
        let data =
            "{\"entry\":\"query-0000000000\",\"entry_version\":5,\"initiator_host\":\"ixchel\",\"initiator_port\":22001,\"cluster\":\"oximeter_cluster\",\"query\":\"CREATE DATABASE IF NOT EXISTS db1 UUID 'a49757e4-179e-42bd-866f-93ac43136e2d' ON CLUSTER oximeter_cluster\",\"settings\":{\"load_balancing\":\"random\"},\"query_create_time\":\"2024-11-01T16:16:45Z\",\"host\":\"::1\",\"port\":22001,\"status\":\"Finished\",\"exception_code\":0,\"exception_text\":\"\",\"query_finish_time\":\"2024-11-01T16:16:45Z\",\"query_duration_ms\":4}
{\"entry\":\"query-0000000000\",\"entry_version\":5,\"initiator_host\":\"ixchel\",\"initiator_port\":22001,\"cluster\":\"oximeter_cluster\",\"query\":\"CREATE DATABASE IF NOT EXISTS db1 UUID 'a49757e4-179e-42bd-866f-93ac43136e2d' ON CLUSTER oximeter_cluster\",\"settings\":{\"load_balancing\":\"random\"},\"query_create_time\":\"2024-11-01T16:16:45Z\",\"host\":\"::1\",\"port\":22002,\"status\":\"Finished\",\"exception_code\":0,\"exception_text\":\"\",\"query_finish_time\":\"2024-11-01T16:16:45Z\",\"query_duration_ms\":4}
"
            .as_bytes();
        let ddl = DistributedDdlQueue::parse(&log, data).unwrap();

        let expected_result = vec![
            DistributedDdlQueue{
                entry: "query-0000000000".to_string(),
                entry_version: 5,
                initiator_host: "ixchel".to_string(),
                initiator_port: 22001,
                cluster: "oximeter_cluster".to_string(),
                query: "CREATE DATABASE IF NOT EXISTS db1 UUID 'a49757e4-179e-42bd-866f-93ac43136e2d' ON CLUSTER oximeter_cluster".to_string(),
                settings: BTreeMap::from([
    ("load_balancing".to_string(), "random".to_string()),
]),
                query_create_time: "2024-11-01T16:16:45Z".parse::<DateTime::<Utc>>().unwrap(),
                host: Ipv6Addr::from_str("::1").unwrap(),
                port: 22001,
                exception_code: 0,
                exception_text: "".to_string(),
                status: "Finished".to_string(),
                query_finish_time: "2024-11-01T16:16:45Z".parse::<DateTime::<Utc>>().unwrap(),
                query_duration_ms: 4,
            },
            DistributedDdlQueue{
                entry: "query-0000000000".to_string(),
                entry_version: 5,
                initiator_host: "ixchel".to_string(),
                initiator_port: 22001,
                cluster: "oximeter_cluster".to_string(),
                query: "CREATE DATABASE IF NOT EXISTS db1 UUID 'a49757e4-179e-42bd-866f-93ac43136e2d' ON CLUSTER oximeter_cluster".to_string(),
                settings: BTreeMap::from([
    ("load_balancing".to_string(), "random".to_string()),
]),
                query_create_time: "2024-11-01T16:16:45Z".parse::<DateTime::<Utc>>().unwrap(),
                host: Ipv6Addr::from_str("::1").unwrap(),
                port: 22002,
                exception_code: 0,
                exception_text: "".to_string(),
                status: "Finished".to_string(),
                query_finish_time: "2024-11-01T16:16:45Z".parse::<DateTime::<Utc>>().unwrap(),
                query_duration_ms: 4,
            },
        ];
        assert!(ddl == expected_result);
    }

    #[test]
    fn test_empty_distributed_ddl_queries_parse_success() {
        let log = log();
        let data = "".as_bytes();
        let ddl = DistributedDdlQueue::parse(&log, data).unwrap();

        let expected_result = vec![];
        assert!(ddl == expected_result);
    }

    #[test]
    fn test_misshapen_distributed_ddl_queries_parse_fail() {
        let log = log();
        let data =
        "{\"entry\":\"query-0000000000\",\"initiator_host\":\"ixchel\",\"initiator_port\":22001,\"cluster\":\"oximeter_cluster\",\"query\":\"CREATE DATABASE IF NOT EXISTS db1 UUID 'a49757e4-179e-42bd-866f-93ac43136e2d' ON CLUSTER oximeter_cluster\",\"settings\":{\"load_balancing\":\"random\"},\"query_create_time\":\"2024-11-01T16:16:45Z\",\"host\":\"::1\",\"port\":22001,\"status\":\"Finished\",\"exception_code\":0,\"exception_text\":\"\",\"query_finish_time\":\"2024-11-01T16:16:45Z\",\"query_duration_ms\":4}
"
.as_bytes();
        let result = DistributedDdlQueue::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "missing field `entry_version` at line 1 column 454",
        );
    }

    #[test]
    fn test_unix_epoch_system_timeseries_parse_success() {
        let log = log();
        let data = "{\"time\":\"1732494720\",\"value\":110220450825.75238}
{\"time\":\"1732494840\",\"value\":110339992917.33333}
{\"time\":\"1732494960\",\"value\":110421854037.33333}\n"
            .as_bytes();
        let timeseries = SystemTimeSeries::parse(&log, data).unwrap();

        let expected = vec![
            SystemTimeSeries {
                time: "1732494720".to_string(),
                value: 110220450825.75238,
            },
            SystemTimeSeries {
                time: "1732494840".to_string(),
                value: 110339992917.33331,
            },
            SystemTimeSeries {
                time: "1732494960".to_string(),
                value: 110421854037.33331,
            },
        ];

        assert_eq!(timeseries, expected);
    }

    #[test]
    fn test_utc_system_timeseries_parse_success() {
        let log = log();
        let data =
            "{\"time\":\"2024-11-25T00:34:00Z\",\"value\":110220450825.75238}
{\"time\":\"2024-11-25T00:35:00Z\",\"value\":110339992917.33333}
{\"time\":\"2024-11-25T00:36:00Z\",\"value\":110421854037.33333}\n"
                .as_bytes();
        let timeseries = SystemTimeSeries::parse(&log, data).unwrap();

        let expected = vec![
            SystemTimeSeries {
                time: "2024-11-25T00:34:00Z".to_string(),
                value: 110220450825.75238,
            },
            SystemTimeSeries {
                time: "2024-11-25T00:35:00Z".to_string(),
                value: 110339992917.33331,
            },
            SystemTimeSeries {
                time: "2024-11-25T00:36:00Z".to_string(),
                value: 110421854037.33331,
            },
        ];

        assert_eq!(timeseries, expected);
    }

    #[test]
    fn test_misshapen_system_timeseries_parse_fail() {
        let log = log();
        let data = "{\"bob\":\"1732494720\",\"value\":110220450825.75238}\n"
            .as_bytes();
        let result = SystemTimeSeries::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "missing field `time` at line 1 column 47",
        );
    }

    #[test]
    fn test_time_format_system_timeseries_parse_fail() {
        let log = log();
        let data = "{\"time\":2024,\"value\":110220450825.75238}\n".as_bytes();
        let result = SystemTimeSeries::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "invalid type: integer `2024`, expected a string at line 1 column 12",
        );
    }
}
