// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use atomicwrites::AtomicFile;
use camino::Utf8PathBuf;
use derive_more::{Add, AddAssign, Display, From};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::create_dir;
use std::io::{ErrorKind, Write};
use std::net::Ipv6Addr;
use std::str::FromStr;

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

    /// Generate a configuration file for a replica server node
    pub fn generate_xml_file(&self) -> Result<ReplicaConfig> {
        let logger =
            LogConfig::new(self.datastore_path.clone(), NodeType::Server);
        let macros = Macros::new(self.id);

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

        match create_dir(self.config_dir.clone()) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        };

        let path = self.config_dir.join("replica-server-config.xml");
        AtomicFile::new(
            path.clone(),
            atomicwrites::OverwriteBehavior::AllowOverwrite,
        )
        .write(|f| f.write_all(config.to_xml().as_bytes()))
        .with_context(|| format!("failed to write to `{}`", path))?;

        Ok(config)
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
            self.id,
            self.datastore_path.clone(),
            raft_config,
        );

        match create_dir(self.config_dir.clone()) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        };

        let path = self.config_dir.join("keeper_config.xml");
        AtomicFile::new(
            path.clone(),
            atomicwrites::OverwriteBehavior::AllowOverwrite,
        )
        .write(|f| f.write_all(config.to_xml().as_bytes()))
        .with_context(|| format!("failed to write to `{}`", path))?;

        Ok(config)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Lgif {
    first_log_idx: u64,
    first_log_term: u64,
    last_log_idx: u64,
    last_log_term: u64,
    last_committed_log_idx: u64,
    leader_committed_log_idx: u64,
    target_committed_log_idx: u64,
    last_snapshot_idx: u64,
}

impl Lgif {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let binding = String::from_utf8_lossy(data);
        let output = binding.as_ref();
        let mut p: Vec<&str> = output.split(|c| c == '\t' || c == '\n').collect();

        let mut lgif: HashMap<&str, &str> = HashMap::new();

        // TODO: First I need to make sure it's an even number?
        let items_to_add = p.len()/2;
        // Create a hashmap with the vec
        for _ in 0..items_to_add {
            let key = p.remove(0);
            let value = p.remove(0);

            lgif.insert(key, value);
        }

        // TODO: Make this not suck
        Ok(Self {
            first_log_idx: u64::from_str(lgif.get("first_log_idx").unwrap()).unwrap(),
            first_log_term: u64::from_str(lgif.get("first_log_term").unwrap()).unwrap(),
            last_log_idx: u64::from_str(lgif.get("last_log_idx").unwrap()).unwrap(),
            last_log_term: u64::from_str(lgif.get("last_log_term").unwrap()).unwrap(),
            last_committed_log_idx: u64::from_str(lgif.get("last_committed_log_idx").unwrap()).unwrap(),
            leader_committed_log_idx: u64::from_str(lgif.get("leader_committed_log_idx").unwrap()).unwrap(),
            target_committed_log_idx: u64::from_str(lgif.get("target_committed_log_idx").unwrap()).unwrap(),
            last_snapshot_idx: u64::from_str(lgif.get("last_snapshot_idx").unwrap()).unwrap(),
        })
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
