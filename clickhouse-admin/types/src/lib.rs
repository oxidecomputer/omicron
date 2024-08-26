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
use thiserror::Error;

pub mod config;
use config::*;

#[derive(Error, Debug)]
pub enum ClickhouseConfigError {
    #[error("no keeper ID has been provided")]
    NoKeeperId,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

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

#[derive(Debug, Clone, Copy)]
pub struct KeeperNode {
    pub id: Option<KeeperId>,
    // TODO: We're writing DNS records here perhaps?
    pub host: Ipv6Addr,
    // TODO: Do I really need this port?
    pub http_port: u16,
}

impl KeeperNode {
    pub fn new(host: Ipv6Addr, http_port: u16) -> Self {
        KeeperNode { id: None, host, http_port }
    }

    pub fn with_id(mut self, id: KeeperId) -> Self {
        self.id = Some(id);
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ServerNode {
    pub id: ServerId,
    pub host: Ipv6Addr,
    //    pub http_port: u16,
    // TODO: Maybe add tcp_port?
    //    pub tcp_port: u16,
    //    pub interserver_http_port: u16,
}

impl ServerNode {
    pub fn new(id: ServerId, host: Ipv6Addr) -> Self {
        ServerNode { id, host }
    }
}

#[derive(Debug, Clone)]
pub struct ClickhouseServerConfig {
    pub cluster_name: String,
    pub id: ServerId,
    // TODO: We probably don't need this and can use the default port
    pub tcp_port: u16,
    pub http_port: u16,
    // TODO: We probably don't need this and can use the default port
    pub interserver_http_port: u16,
    pub path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub keepers: Vec<KeeperNode>,
    pub servers: Vec<ServerNode>,
}

impl ClickhouseServerConfig {
    pub fn new(
        cluster_name: String,
        id: ServerId,
        tcp_port: u16,
        http_port: u16,
        interserver_http_port: u16,
        path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        keepers: Vec<KeeperNode>,
        servers: Vec<ServerNode>,
    ) -> Self {
        Self {
            cluster_name,
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
        let servers = self
            .servers
            .iter()
            .map(|&s| ServerConfig {
                host: s.host.to_string(),
                port: self.tcp_port, // TODO: should tcp port be handled in the struct?
            })
            .collect();

        let remote_servers = RemoteServers {
            cluster: self.cluster_name.clone(),
            // TODO: Figure out what to do with secret
            secret: "some-unique-value".to_string(),
            replicas: servers,
        };

        let keepers = KeeperConfigsForReplica {
            nodes: self
                .keepers
                .iter()
                .map(|&k| ServerConfig {
                    host: k.host.to_string(),
                    port: k.http_port,
                })
                .collect(),
        };

        // TODO: See which is the best approach for log directory
        let logs: Utf8PathBuf = self.path.join("logs");
        // TODO: I probably shouldn't create a directory
        // because it's the data store
       // std::fs::create_dir_all(&logs)?;
        let log = logs.join("clickhouse.log");
        let errorlog = logs.join("clickhouse.err.log");
        let data_path = self.path.join("data");
        let config = ReplicaConfig {
            logger: LogConfig {
                level: LogLevel::Trace,
                log,
                errorlog,
                size: "100M".to_string(),
                count: 1,
            },
            macros: Macros {
                shard: 1,
                replica: self.id,
                cluster: self.cluster_name.clone(),
            },
            listen_host: self.listen_addr.to_string(),
            // These ports don't need to change
            http_port: self.http_port,
            tcp_port: self.tcp_port,
            interserver_http_port: self.interserver_http_port,
            remote_servers: remote_servers.clone(),
            keepers: keepers.clone(),
            data_path,
        };
        let mut f = File::create(self.path.join("replica-server-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ClickhouseKeeperConfig {
    pub keepers: Vec<KeeperNode>,
    pub path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub tcp_port: u16,
}

impl ClickhouseKeeperConfig {
    pub fn new(
        keepers: Vec<KeeperNode>,
        path: Utf8PathBuf,
        listen_addr: Ipv6Addr,
        tcp_port: u16,
    ) -> Self {
        ClickhouseKeeperConfig { keepers, path, listen_addr, tcp_port }
    }
    /// Generate a config for `this_keeper` consisting of the keepers in `keeper_ids`
    pub fn generate_xml_file(
        &self,
        this_keeper: KeeperId,
        //keeper_ids: BTreeSet<KeeperId>,
        // TODO: use keeper object instead
    ) -> Result<()> {
        //  let raft_servers: Vec<_> = keeper_ids
        //      .iter()
        //      .map(|id| RaftServerConfig {
        //          id: *id,
        //          hostname: "::1".to_string(),
        //          port: self.config.base_ports.raft + id.0 as u16,
        //      })
        //      .collect();

        let raft_servers: Vec<RaftServerConfig> = self
            .keepers
            .iter()
            .map(|&k| RaftServerConfig {
                // TODO: Implemement a "new()" method for RaftServerConfig
                id: k
                    .id
                    .ok_or(ClickhouseConfigError::NoKeeperId)
                    .expect("no keeper ID has been provided"),
                hostname: k.host.to_string(),
                port: k.http_port,
            })
            .collect();

        //        let dir: Utf8PathBuf =
        //            [self.path.as_str(), &format!("keeper-{this_keeper}")]
        //                .iter()
        //                .collect();
        //        let logs: Utf8PathBuf = self.path.join("logs");
        //        std::fs::create_dir_all(&logs)?;
        let log = self.path.join("clickhouse-keeper.log");
        let errorlog = self.path.join("clickhouse-keeper.err.log");
        let config = KeeperConfig {
            logger: LogConfig {
                level: LogLevel::Trace,
                log,
                errorlog,
                size: "100M".to_string(),
                count: 1,
            },
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
            raft_config: RaftServers { servers: raft_servers.clone() },
        };
        let mut f = File::create(self.path.join("keeper-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv6Addr, str::FromStr};

    use camino::Utf8PathBuf;

    use crate::{
        ClickhouseKeeperConfig, ClickhouseServerConfig, KeeperId,
        KeeperNode, ServerId, ServerNode,
    };

    #[test]
    fn test_generate_keeper_config() {
        let keepers = vec![
            KeeperNode::new(Ipv6Addr::from_str("ff::01").unwrap(), 1234)
                .with_id(KeeperId(1)),
            KeeperNode::new(Ipv6Addr::from_str("ff::02").unwrap(), 1234)
                .with_id(KeeperId(2)),
            KeeperNode::new(Ipv6Addr::from_str("ff::03").unwrap(), 1234)
                .with_id(KeeperId(3)),
        ];

        let config = ClickhouseKeeperConfig::new(
            keepers,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
            5678,
        );

        config.generate_xml_file(KeeperId(1)).unwrap();
    }

    #[test]
    fn test_generate_replica_config() {
        let keepers = vec![
            KeeperNode::new(Ipv6Addr::from_str("ff::01").unwrap(), 1234)
                .with_id(KeeperId(1)),
            KeeperNode::new(Ipv6Addr::from_str("ff::02").unwrap(), 1234)
                .with_id(KeeperId(2)),
            KeeperNode::new(Ipv6Addr::from_str("ff::03").unwrap(), 1234)
                .with_id(KeeperId(3)),
        ];

        let servers = vec![
            ServerNode::new(ServerId(1), Ipv6Addr::from_str("ff::08").unwrap()),
            ServerNode::new(ServerId(2), Ipv6Addr::from_str("ff::09").unwrap()),
        ];

        let config = ClickhouseServerConfig::new(
            "oximeter_cluster".to_string(),
            ServerId(1),
            1234,
            5678,
            3456,
            Utf8PathBuf::from_str("./").unwrap(),
            Ipv6Addr::from_str("ff::08").unwrap(),
            keepers,
            servers,
        );

        config.generate_xml_file().unwrap();
    }
}
