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

pub struct ClickhouseServerConfig {
    pub cluster_name: String,
    pub id: ServerId,
    // TODO: We probably don't need this and can use the default port
    pub tcp_port: u16,
    pub http_port: u16,
    // TODO: We probably don't need this and can use the default port
    pub interserver_http_port: u16,
    pub path: Utf8PathBuf,
    pub host: Ipv6Addr,
    pub keepers: Vec<KeeperNode>,
    pub servers: Vec<ServerNode>,
}

impl ClickhouseServerConfig {
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

        let logs: Utf8PathBuf = self.path.join("logs");
        // TODO: I probably shouldn't create a directory
        // because it's the data store
        std::fs::create_dir_all(&logs)?;
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
            listen_host: self.host.to_string(),
            // These ports don't need to change
            http_port: self.http_port,
            tcp_port: self.tcp_port,
            interserver_http_port: self.interserver_http_port,
            remote_servers: remote_servers.clone(),
            keepers: keepers.clone(),
            data_path,
        };
        let mut f = File::create(self.path.join("clickhouse-config.xml"))?;
        f.write_all(config.to_xml().as_bytes())?;
        f.flush()?;
        Ok(())
    }
}

//pub struct ClickhouseKeeperXml {}
//
//impl ClickhouseKeeperXml {
//    /// Generate a config for `this_keeper` consisting of the keepers in `keeper_ids`
//    fn generate_keeper_config(
//        &self,
//        this_keeper: KeeperId,
//        keeper_ids: BTreeSet<KeeperId>,
//        // TODO: use keeper object instead
//    ) -> Result<()> {
//        let raft_servers: Vec<_> = keeper_ids
//            .iter()
//            .map(|id| RaftServerConfig {
//                id: *id,
//                hostname: "::1".to_string(),
//                port: self.config.base_ports.raft + id.0 as u16,
//            })
//            .collect();
//        let dir: Utf8PathBuf =
//            [self.config.path.as_str(), &format!("keeper-{this_keeper}")]
//                .iter()
//                .collect();
//        let logs: Utf8PathBuf = dir.join("logs");
//        std::fs::create_dir_all(&logs)?;
//        let log = logs.join("clickhouse-keeper.log");
//        let errorlog = logs.join("clickhouse-keeper.err.log");
//        let config = KeeperConfig {
//            logger: LogConfig {
//                level: LogLevel::Trace,
//                log,
//                errorlog,
//                size: "100M".to_string(),
//                count: 1,
//            },
//            listen_host: "::1".to_string(),
//            tcp_port: self.config.base_ports.keeper + this_keeper.0 as u16,
//            server_id: this_keeper,
//            log_storage_path: dir.join("coordination").join("log"),
//            snapshot_storage_path: dir.join("coordination").join("snapshots"),
//            coordination_settings: KeeperCoordinationSettings {
//                operation_timeout_ms: 10000,
//                session_timeout_ms: 30000,
//                raft_logs_level: LogLevel::Trace,
//            },
//            raft_config: RaftServers { servers: raft_servers.clone() },
//        };
//        let mut f = File::create(dir.join("keeper-config.xml"))?;
//        f.write_all(config.to_xml().as_bytes())?;
//        f.flush()?;
//
//        Ok(())
//    }
//}
//
