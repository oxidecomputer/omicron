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
    pub datastore_path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub keepers: Vec<KeeperNodeConfig>,
    pub servers: Vec<ServerNodeConfig>,
}

impl ClickhouseServerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config_dir: Utf8PathBuf,
        id: ServerId,
        datastore_path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        keepers: Vec<KeeperNodeConfig>,
        servers: Vec<ServerNodeConfig>,
    ) -> Self {
        Self { config_dir, id, datastore_path, listen_addr, keepers, servers }
    }

    pub fn generate_xml_file(&self) -> Result<()> {
        let logger =
            LogConfig::new(self.datastore_path.clone(), NodeType::Server);
        let macros = Macros::new(self.id);

        let config = ReplicaConfig::new(
            logger,
            macros,
            self.listen_addr,
            self.servers.clone(),
            self.keepers.clone(),
            self.datastore_path.clone(),
        );
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
}

impl ClickhouseKeeperConfig {
    pub fn new(
        config_dir: Utf8PathBuf,
        raft_servers: Vec<RaftServerConfig>,
        path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
    ) -> Self {
        ClickhouseKeeperConfig { config_dir, raft_servers, path, listen_addr }
    }

    /// Generate a configuration file for `this_keeper` consisting of the keepers in `raft_servers`
    pub fn generate_xml_file(&self, this_keeper: KeeperId) -> Result<()> {
        let logger = LogConfig::new(self.path.clone(), NodeType::Keeper);
        let raft_config = RaftServers::new(self.raft_servers.clone());
        let config = KeeperConfig::new(
            logger,
            self.listen_addr,
            this_keeper,
            self.path.clone(),
            raft_config,
        );

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

    use crate::{
        ClickhouseKeeperConfig, ClickhouseServerConfig, KeeperId,
        KeeperNodeConfig, RaftServerConfig, ServerId, ServerNodeConfig,
    };

    #[test]
    fn test_generate_keeper_config() {
        let config_dir = Builder::new()
            .tempdir_in(
                Utf8PathBuf::try_from(std::env::temp_dir()).unwrap())
                .expect("Could not create directory for ClickHouse configuration generation test"
            );

        let keepers = vec![
            RaftServerConfig::new(KeeperId(1), "ff::01".to_string()),
            RaftServerConfig::new(KeeperId(2), "ff::02".to_string()),
            RaftServerConfig::new(KeeperId(3), "ff::03".to_string()),
        ];

        let config = ClickhouseKeeperConfig::new(
            Utf8PathBuf::from(config_dir.path()),
            keepers,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
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
            KeeperNodeConfig::new("ff::01".to_string()),
            KeeperNodeConfig::new("ff::02".to_string()),
            KeeperNodeConfig::new("ff::03".to_string()),
        ];

        let servers = vec![
            ServerNodeConfig::new("ff::08".to_string()),
            ServerNodeConfig::new("ff::09".to_string()),
        ];

        let config = ClickhouseServerConfig::new(
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
