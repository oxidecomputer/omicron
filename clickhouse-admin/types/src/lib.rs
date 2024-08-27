// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use camino::Utf8PathBuf;
use derive_more::{Add, AddAssign, Display, From};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::net::Ipv6Addr;

pub mod config;
use config::*;

pub const OXIMETER_CLUSTER: &str = "oximeter_cluster";

/// A unique ID for a clickhouse keeper
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
)]
pub struct KeeperId(pub u64);

/// A unique ID for a clickhouse server
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
)]
pub struct ServerId(pub u64);

#[derive(Debug, Clone)]
pub struct ClickhouseServerConfig {
    pub config_dir: Utf8PathBuf,
    pub id: ServerId,
    pub tcp_port: u16,
    pub http_port: u16,
    pub interserver_http_port: u16,
    pub path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub keepers: Vec<NodeConfig>,
    pub servers: Vec<NodeConfig>,
}

impl ClickhouseServerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config_dir: Utf8PathBuf,
        id: ServerId,
        tcp_port: u16,
        http_port: u16,
        interserver_http_port: u16,
        path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        keepers: Vec<NodeConfig>,
        servers: Vec<NodeConfig>,
    ) -> Self {
        Self {
            config_dir,
            id,
            tcp_port,
            http_port,
            interserver_http_port,
            path,
            listen_addr,
            keepers,
            servers,
        }
    }

    pub fn generate_xml_file(&self) -> Result<()> {
        let logs: Utf8PathBuf = self.path.join("logs");
        let log = logs.join("clickhouse.log");
        let errorlog = logs.join("clickhouse.err.log");
        let data_path = self.path.join("data");

        let config = ReplicaConfig {
            logger: LogConfig::new(log, errorlog),
            macros: Macros::new(self.id),
            listen_host: self.listen_addr.to_string(),
            http_port: self.http_port,
            tcp_port: self.tcp_port,
            interserver_http_port: self.interserver_http_port,
            remote_servers: RemoteServers::new(self.servers.clone()),
            keepers: KeeperConfigsForReplica::new(self.keepers.clone()),
            data_path,
        };
        let mut f =
            File::create(self.config_dir.join("replica-server-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClickhouseKeeperConfig {
    pub config_dir: Utf8PathBuf,
    pub raft_servers: Vec<RaftServerConfig>,
    pub path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub tcp_port: u16,
}

impl ClickhouseKeeperConfig {
    pub fn new(
        config_dir: Utf8PathBuf,
        raft_servers: Vec<RaftServerConfig>,
        path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        tcp_port: u16,
    ) -> Self {
        ClickhouseKeeperConfig {
            config_dir,
            raft_servers,
            path,
            listen_addr,
            tcp_port,
        }
    }
    /// Generate a config for `this_keeper` consisting of the keepers in `keeper_ids`
    pub fn generate_xml_file(&self, this_keeper: KeeperId) -> Result<()> {
        let log = self.path.join("clickhouse-keeper.log");
        let errorlog = self.path.join("clickhouse-keeper.err.log");
        let config = KeeperConfig {
            logger: LogConfig::new(log, errorlog),
            listen_host: self.listen_addr.to_string(),
            tcp_port: self.tcp_port,
            server_id: this_keeper,
            log_storage_path: self.path.join("coordination").join("log"),
            snapshot_storage_path: self
                .path
                .join("coordination")
                .join("snapshots"),
            coordination_settings: KeeperCoordinationSettings {
                operation_timeout_ms: 10000,
                session_timeout_ms: 30000,
                raft_logs_level: LogLevel::Trace,
            },
            raft_config: RaftServers { servers: self.raft_servers.clone() },
        };
        let mut f = File::create(self.config_dir.join("keeper-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv6Addr, str::FromStr};

    use camino::Utf8PathBuf;
    use camino_tempfile::Builder;
    use omicron_common::address::{
        CLICKHOUSE_HTTP_PORT, CLICKHOUSE_INTERSERVER_PORT,
        CLICKHOUSE_KEEPER_RAFT_PORT, CLICKHOUSE_KEEPER_TCP_PORT,
        CLICKHOUSE_TCP_PORT,
    };

    use crate::{
        ClickhouseKeeperConfig, ClickhouseServerConfig, KeeperId, NodeConfig,
        RaftServerConfig, ServerId,
    };

    #[test]
    fn test_generate_keeper_config() {
        let config_dir = Builder::new()
            .tempdir_in(
                Utf8PathBuf::try_from(std::env::temp_dir()).unwrap())
                .expect("Could not create directory for ClickHouse configuration generation test"
            );

        let keepers = vec![
            RaftServerConfig::new(
                KeeperId(1),
                "ff::01".to_string(),
                CLICKHOUSE_KEEPER_RAFT_PORT,
            ),
            RaftServerConfig::new(
                KeeperId(2),
                "ff::02".to_string(),
                CLICKHOUSE_KEEPER_RAFT_PORT,
            ),
            RaftServerConfig::new(
                KeeperId(3),
                "ff::03".to_string(),
                CLICKHOUSE_KEEPER_RAFT_PORT,
            ),
        ];

        let config = ClickhouseKeeperConfig::new(
            Utf8PathBuf::from(config_dir.path()),
            keepers,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
            CLICKHOUSE_KEEPER_TCP_PORT,
        );

        config.generate_xml_file(KeeperId(1)).unwrap();

        let expected_file = Utf8PathBuf::from_str("./testutils")
            .unwrap()
            .join("keeper-config.xml");
        let expected_content = std::fs::read_to_string(expected_file)
            .expect("Failed to read from expected ClickHouse keeper file");
        let generated_file =
            Utf8PathBuf::from(config_dir.path()).join("keeper-config.xml");
        let generated_content = std::fs::read_to_string(generated_file)
            .expect("Failed to read from generated ClickHouse keeper file");

        assert_eq!(expected_content, generated_content);
    }

    #[test]
    fn test_generate_replica_config() {
        let config_dir = Builder::new()
        .tempdir_in(
            Utf8PathBuf::try_from(std::env::temp_dir()).unwrap())
            .expect("Could not create directory for ClickHouse configuration generation test"
        );

        let keepers = vec![
            NodeConfig::new("ff::01".to_string(), CLICKHOUSE_KEEPER_TCP_PORT),
            NodeConfig::new("ff::02".to_string(), CLICKHOUSE_KEEPER_TCP_PORT),
            NodeConfig::new("ff::03".to_string(), CLICKHOUSE_KEEPER_TCP_PORT),
        ];

        let servers = vec![
            NodeConfig::new("ff::08".to_string(), CLICKHOUSE_TCP_PORT),
            NodeConfig::new("ff::09".to_string(), CLICKHOUSE_TCP_PORT),
        ];

        let config = ClickhouseServerConfig::new(
            Utf8PathBuf::from(config_dir.path()),
            ServerId(1),
            CLICKHOUSE_TCP_PORT,
            CLICKHOUSE_HTTP_PORT,
            CLICKHOUSE_INTERSERVER_PORT,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
            keepers,
            servers,
        );

        config.generate_xml_file().unwrap();

        let expected_file = Utf8PathBuf::from_str("./testutils")
            .unwrap()
            .join("replica-server-config.xml");
        let expected_content = std::fs::read_to_string(expected_file).expect(
            "Failed to read from expected ClickHouse replica server file",
        );
        let generated_file = Utf8PathBuf::from(config_dir.path())
            .join("replica-server-config.xml");
        let generated_content = std::fs::read_to_string(generated_file).expect(
            "Failed to read from generated ClickHouse replica server file",
        );

        assert_eq!(expected_content, generated_content);
    }
}
