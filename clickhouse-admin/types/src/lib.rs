// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use camino::Utf8PathBuf;
use camino_tempfile::NamedUtf8TempFile;
use derive_more::{Add, AddAssign, Display, From};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fs::rename;
use std::io::Write;
use std::net::Ipv6Addr;

pub mod config;
use config::*;

pub const OXIMETER_CLUSTER: &str = "oximeter_cluster";

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
)]
pub struct ServerId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ServerSettings {
    #[schemars(schema_with = "path_schema")]
    pub config_dir: Utf8PathBuf,
    pub node_id: ServerId,
    #[schemars(schema_with = "path_schema")]
    pub datastore_path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub keepers: Vec<ClickhouseHost>,
    pub remote_servers: Vec<ClickhouseHost>,
}

impl ServerSettings {
    pub fn new(
        config_dir: Utf8PathBuf,
        node_id: ServerId,
        datastore_path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        keepers: Vec<ClickhouseHost>,
        remote_servers: Vec<ClickhouseHost>,
    ) -> Self {
        Self {
            config_dir,
            node_id,
            datastore_path,
            listen_addr,
            keepers,
            remote_servers,
        }
    }

    /// Generate a configuration file for a replica server node
    pub fn generate_xml_file(&self) -> Result<ReplicaConfig> {
        let logger =
            LogConfig::new(self.datastore_path.clone(), NodeType::Server);
        let macros = Macros::new(self.node_id);

        let keepers: Vec<KeeperNodeConfig> = self
            .keepers
            .iter()
            .map(|host| KeeperNodeConfig::new(host.clone()))
            .collect();

        let servers: Vec<ServerNodeConfig> = self
            .remote_servers
            .iter()
            .map(|host| ServerNodeConfig::new(host.clone()))
            .collect();

        let config = ReplicaConfig::new(
            logger,
            macros,
            self.listen_addr,
            servers.clone(),
            keepers.clone(),
            self.datastore_path.clone(),
        );

        // Writing to a temporary file and then renaming it will ensure we
        // don't end up with a partially written file after a crash
        let mut f = NamedUtf8TempFile::new()?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
        rename(f.path(), self.config_dir.join("replica-server-config.xml"))?;
        Ok(config)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct KeeperSettings {
    #[schemars(schema_with = "path_schema")]
    pub config_dir: Utf8PathBuf,
    pub node_id: KeeperId,
    pub raft_servers: Vec<RaftServerSettings>,
    #[schemars(schema_with = "path_schema")]
    pub datastore_path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
}

impl KeeperSettings {
    pub fn new(
        config_dir: Utf8PathBuf,
        node_id: KeeperId,
        raft_servers: Vec<RaftServerSettings>,
        datastore_path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
    ) -> Self {
        Self { config_dir, node_id, raft_servers, datastore_path, listen_addr }
    }

    /// Generate a configuration file for a keeper node
    pub fn generate_xml_file(&self) -> Result<KeeperConfig> {
        let logger =
            LogConfig::new(self.datastore_path.clone(), NodeType::Keeper);

        let raft_servers = self
            .raft_servers
            .iter()
            .map(|settings| RaftServerConfig::new(settings.clone()))
            .collect();
        let raft_config = RaftServers::new(raft_servers);

        let config = KeeperConfig::new(
            logger,
            self.listen_addr,
            self.node_id,
            self.datastore_path.clone(),
            raft_config,
        );

        // Writing to a temporary file and then renaming it will ensure we
        // don't end up with a partially written file after a crash
        let mut f = NamedUtf8TempFile::new()?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
        rename(f.path(), self.config_dir.join("keeper-config.xml"))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, Ipv6Addr},
        str::FromStr,
    };

    use camino::Utf8PathBuf;
    use camino_tempfile::Builder;

    use crate::{
        ClickhouseHost, KeeperId, KeeperSettings, RaftServerSettings, ServerId,
        ServerSettings,
    };

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

        let config = KeeperSettings::new(
            Utf8PathBuf::from(config_dir.path()),
            KeeperId(1),
            keepers,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
        );

        config.generate_xml_file().unwrap();

        let expected_file = Utf8PathBuf::from_str("./testutils")
            .unwrap()
            .join("keeper-config.xml");
        let generated_file =
            Utf8PathBuf::from(config_dir.path()).join("keeper-config.xml");
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

        let config = ServerSettings::new(
            Utf8PathBuf::from(config_dir.path()),
            ServerId(1),
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
            keepers,
            servers,
        );

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
}
