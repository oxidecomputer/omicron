// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{KeeperId, ServerId, OXIMETER_CLUSTER};
use camino::Utf8PathBuf;
use omicron_common::address::{
    CLICKHOUSE_HTTP_PORT, CLICKHOUSE_INTERSERVER_PORT,
    CLICKHOUSE_KEEPER_RAFT_PORT, CLICKHOUSE_KEEPER_TCP_PORT,
    CLICKHOUSE_TCP_PORT,
};
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::{Deserialize, Serialize};
use std::{fmt::Display, net::Ipv6Addr};

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

/// Configuration for a ClickHouse replica server
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub logger: LogConfig,
    pub macros: Macros,
    pub listen_host: Ipv6Addr,
    pub http_port: u16,
    pub tcp_port: u16,
    pub interserver_http_port: u16,
    pub remote_servers: RemoteServers,
    pub keepers: KeeperConfigsForReplica,
    #[schemars(schema_with = "path_schema")]
    pub data_path: Utf8PathBuf,
}

impl ReplicaConfig {
    /// A new ClickHouse replica server configuration with default ports and directories
    pub fn new(
        logger: LogConfig,
        macros: Macros,
        listen_host: Ipv6Addr,
        remote_servers: Vec<ServerNodeConfig>,
        keepers: Vec<KeeperNodeConfig>,
        path: Utf8PathBuf,
    ) -> Self {
        let data_path = path.join("data");
        let remote_servers = RemoteServers::new(remote_servers);
        let keepers = KeeperConfigsForReplica::new(keepers);

        Self {
            logger,
            macros,
            listen_host,
            http_port: CLICKHOUSE_HTTP_PORT,
            tcp_port: CLICKHOUSE_TCP_PORT,
            interserver_http_port: CLICKHOUSE_INTERSERVER_PORT,
            remote_servers,
            keepers,
            data_path,
        }
    }

    pub fn to_xml(&self) -> String {
        let ReplicaConfig {
            logger,
            macros,
            listen_host,
            http_port,
            tcp_port,
            interserver_http_port,
            remote_servers,
            keepers,
            data_path,
        } = self;
        let logger = logger.to_xml();
        let cluster = macros.cluster.clone();
        let id = macros.replica;
        let macros = macros.to_xml();
        let keepers = keepers.to_xml();
        let remote_servers = remote_servers.to_xml();
        let user_files_path = data_path.clone().join("user_files");
        let format_schema_path = data_path.clone().join("format_schemas");
        format!(
            "
<clickhouse>
{logger}
    <path>{data_path}</path>

    <profiles>
        <default>
            <load_balancing>random</load_balancing>
        </default>

    </profiles>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>

    <user_files_path>{user_files_path}</user_files_path>
    <default_profile>default</default_profile>
    <format_schema_path>{format_schema_path}</format_schema_path>
    <display_name>{cluster}_{id}</display_name>
    <listen_host>{listen_host}</listen_host>
    <http_port>{http_port}</http_port>
    <tcp_port>{tcp_port}</tcp_port>
    <interserver_http_port>{interserver_http_port}</interserver_http_port>
    <interserver_http_host>{listen_host}</interserver_http_host>
    <distributed_ddl>
        <!-- Cleanup settings (active tasks will not be removed) -->

        <!-- Controls task TTL (default 1 week) -->
        <task_max_lifetime>604800</task_max_lifetime>

        <!-- Controls how often cleanup should be performed (in seconds) -->
        <cleanup_delay_period>60</cleanup_delay_period>

        <!-- Controls how many tasks could be in the queue -->
        <max_tasks_in_queue>1000</max_tasks_in_queue>
     </distributed_ddl>
{macros}
{remote_servers}
{keepers}

</clickhouse>
"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct Macros {
    pub shard: u64,
    pub replica: ServerId,
    pub cluster: String,
}

impl Macros {
    /// A new macros configuration block with default cluster
    pub fn new(replica: ServerId) -> Self {
        Self { shard: 1, replica, cluster: OXIMETER_CLUSTER.to_string() }
    }

    pub fn to_xml(&self) -> String {
        let Macros { shard, replica, cluster } = self;
        format!(
            "
    <macros>
        <shard>{shard}</shard>
        <replica>{replica}</replica>
        <cluster>{cluster}</cluster>
    </macros>"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct RemoteServers {
    pub cluster: String,
    pub secret: String,
    pub replicas: Vec<ServerNodeConfig>,
}

impl RemoteServers {
    /// A new remote_servers configuration block with default cluster
    pub fn new(replicas: Vec<ServerNodeConfig>) -> Self {
        Self {
            cluster: OXIMETER_CLUSTER.to_string(),
            // TODO(https://github.com/oxidecomputer/omicron/issues/3823): secret handling TBD
            secret: "some-unique-value".to_string(),
            replicas,
        }
    }

    pub fn to_xml(&self) -> String {
        let RemoteServers { cluster, secret, replicas } = self;

        let mut s = format!(
            "
    <remote_servers replace=\"true\">
        <{cluster}>
            <!-- TODO(https://github.com/oxidecomputer/omicron/issues/3823): secret handling TBD -->
            <secret>{secret}</secret>
            <shard>
                <internal_replication>true</internal_replication>"
        );

        for r in replicas {
            let ServerNodeConfig { host, port } = r;
            s.push_str(&format!(
                "
                <replica>
                    <host>{host}</host>
                    <port>{port}</port>
                </replica>"
            ));
        }

        s.push_str(&format!(
            "
            </shard>
        </{cluster}>
    </remote_servers>
        "
        ));

        s
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperConfigsForReplica {
    pub nodes: Vec<KeeperNodeConfig>,
}

impl KeeperConfigsForReplica {
    pub fn new(nodes: Vec<KeeperNodeConfig>) -> Self {
        Self { nodes }
    }

    pub fn to_xml(&self) -> String {
        let mut s = String::from("    <zookeeper>");
        for node in &self.nodes {
            let KeeperNodeConfig { host, port } = node;

            // ClickHouse servers have a small quirk, where when setting the
            // keeper hosts as IPv6 addresses in the replica configuration file,
            // they must be wrapped in square brackets.
            // Otherwise, when running any query, a "Service not found" error
            // appears.
            // https://github.com/ClickHouse/ClickHouse/blob/a011990fd75628c63c7995c4f15475f1d4125d10/src/Coordination/KeeperStateManager.cpp#L149
            let parsed_host = match host.parse::<Ipv6Addr>() {
                Ok(_) => format!("[{host}]"),
                Err(_) => host.to_string(),
            };

            s.push_str(&format!(
                "
        <node>
            <host>{parsed_host}</host>
            <port>{port}</port>
        </node>",
            ));
        }
        s.push_str("\n    </zookeeper>");
        s
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperNodeConfig {
    pub host: String,
    pub port: u16,
}

impl KeeperNodeConfig {
    /// A new ClickHouse keeper node configuration with default port
    pub fn new(host: String) -> Self {
        let port = CLICKHOUSE_KEEPER_TCP_PORT;
        Self { host, port }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ServerNodeConfig {
    pub host: String,
    pub port: u16,
}

impl ServerNodeConfig {
    /// A new ClickHouse replica node configuration with default port
    pub fn new(host: String) -> Self {
        let port = CLICKHOUSE_TCP_PORT;
        Self { host, port }
    }
}

pub enum NodeType {
    Server,
    Keeper,
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    #[schemars(schema_with = "path_schema")]
    pub log: Utf8PathBuf,
    #[schemars(schema_with = "path_schema")]
    pub errorlog: Utf8PathBuf,
    pub size: u16,
    pub count: usize,
}

impl LogConfig {
    /// A new logger configuration with default directories
    pub fn new(path: Utf8PathBuf, node_type: NodeType) -> Self {
        let prefix = match node_type {
            NodeType::Server => "clickhouse",
            NodeType::Keeper => "clickhouse-keeper",
        };

        let logs: Utf8PathBuf = path.join("log");
        let log = logs.join(format!("{prefix}.log"));
        let errorlog = logs.join(format!("{prefix}.err.log"));

        Self { level: LogLevel::default(), log, errorlog, size: 100, count: 1 }
    }

    pub fn to_xml(&self) -> String {
        let LogConfig { level, log, errorlog, size, count } = &self;
        format!(
            "
    <logger>
        <level>{level}</level>
        <log>{log}</log>
        <errorlog>{errorlog}</errorlog>
        <size>{size}M</size>
        <count>{count}</count>
    </logger>
"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperCoordinationSettings {
    pub operation_timeout_ms: u32,
    pub session_timeout_ms: u32,
    pub raft_logs_level: LogLevel,
}

impl KeeperCoordinationSettings {
    pub fn default() -> Self {
        Self {
            operation_timeout_ms: 10000,
            session_timeout_ms: 30000,
            raft_logs_level: LogLevel::Trace,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct RaftServers {
    pub servers: Vec<RaftServerConfig>,
}

impl RaftServers {
    pub fn new(servers: Vec<RaftServerConfig>) -> Self {
        Self { servers }
    }
    pub fn to_xml(&self) -> String {
        let mut s = String::new();
        for server in &self.servers {
            let RaftServerConfig { id, hostname, port } = server;
            s.push_str(&format!(
                "
            <server>
                <id>{id}</id>
                <hostname>{hostname}</hostname>
                <port>{port}</port>
            </server>
            "
            ));
        }

        s
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct RaftServerConfig {
    pub id: KeeperId,
    pub hostname: String,
    pub port: u16,
}

impl RaftServerConfig {
    pub fn new(id: KeeperId, hostname: String) -> Self {
        Self { id, hostname, port: CLICKHOUSE_KEEPER_RAFT_PORT }
    }
}

/// Configuration for a ClickHouse keeper
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct KeeperConfig {
    pub logger: LogConfig,
    pub listen_host: Ipv6Addr,
    pub tcp_port: u16,
    pub server_id: KeeperId,
    #[schemars(schema_with = "path_schema")]
    pub log_storage_path: Utf8PathBuf,
    #[schemars(schema_with = "path_schema")]
    pub snapshot_storage_path: Utf8PathBuf,
    pub coordination_settings: KeeperCoordinationSettings,
    pub raft_config: RaftServers,
}

impl KeeperConfig {
    /// A new ClickHouse keeper node configuration with default ports and directories
    pub fn new(
        logger: LogConfig,
        listen_host: Ipv6Addr,
        server_id: KeeperId,
        datastore_path: Utf8PathBuf,
        raft_config: RaftServers,
    ) -> Self {
        let coordination_path = datastore_path.join("coordination");
        let log_storage_path = coordination_path.join("log");
        let snapshot_storage_path = coordination_path.join("snapshots");
        let coordination_settings = KeeperCoordinationSettings::default();
        Self {
            logger,
            listen_host,
            tcp_port: CLICKHOUSE_KEEPER_TCP_PORT,
            server_id,
            log_storage_path,
            snapshot_storage_path,
            coordination_settings,
            raft_config,
        }
    }

    pub fn to_xml(&self) -> String {
        let KeeperConfig {
            logger,
            listen_host,
            tcp_port,
            server_id,
            log_storage_path,
            snapshot_storage_path,
            coordination_settings,
            raft_config,
        } = self;
        let logger = logger.to_xml();
        let KeeperCoordinationSettings {
            operation_timeout_ms,
            session_timeout_ms,
            raft_logs_level,
        } = coordination_settings;
        let raft_servers = raft_config.to_xml();
        format!(
            "
<clickhouse>
{logger}
    <listen_host>{listen_host}</listen_host>
    <keeper_server>
        <enable_reconfiguration>false</enable_reconfiguration>
        <tcp_port>{tcp_port}</tcp_port>
        <server_id>{server_id}</server_id>
        <log_storage_path>{log_storage_path}</log_storage_path>
        <snapshot_storage_path>{snapshot_storage_path}</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>{operation_timeout_ms}</operation_timeout_ms>
            <session_timeout_ms>{session_timeout_ms}</session_timeout_ms>
            <raft_logs_level>{raft_logs_level}</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
{raft_servers}
        </raft_configuration>
    </keeper_server>

</clickhouse>
"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
}

impl LogLevel {
    fn default() -> Self {
        LogLevel::Trace
    }
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
        };
        write!(f, "{s}")
    }
}
