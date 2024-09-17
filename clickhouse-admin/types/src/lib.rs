// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context, Result};
use atomicwrites::AtomicFile;
use camino::Utf8PathBuf;
use derive_more::{Add, AddAssign, Display, From};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{info, Logger};
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

macro_rules! define_struct_and_set_values {
    (
        #[$doc_struct:meta]
        struct $name:ident {
            $(
                #[$doc_field:meta]
                $field_name:ident: $field_type:ty
            ),* $(,)?
        }
    ) => {
        #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
        #[serde(rename_all = "snake_case")]
        #[$doc_struct]
        pub struct $name {
            $(
                #[$doc_field]
                $field_name: $field_type
            ),*
        }

        impl $name {
            // Check if a field name matches a given key and set its value.
            // Since the struct will be serialised into JSON for the reponse.
            // we want to make sure the keys are exactly the same.
            pub fn set_field_value(&mut self, key: &str, value: u64) -> Result<()> {
                match key {
                    $(
                        stringify!($field_name) => {
                            self.$field_name = value;
                            Ok(())
                        },
                    )*
                    _ => bail!("Key '{}' is not part of the expected keys.", key),
                }
            }
        }
    };
}

define_struct_and_set_values! {
    /// Logically grouped information file from a keeper node
    struct Lgif {
        /// Index of the first log entry in the current log segment
        first_log_idx: u64,
        /// Term of the leader when the first log entry was created
        first_log_term: u64,
        /// Index of the last log entry in the current log segment
        last_log_idx: u64,
        /// Term of the leader when the last log entry was created
        last_log_term: u64,
        /// Index of the last committed log entry
        last_committed_log_idx: u64,
        /// Index of the last committed log entry from the leader's perspective
        leader_committed_log_idx: u64,
        /// Target index for log commitment during replication or recovery
        target_committed_log_idx: u64,
        /// Index of the most recent snapshot taken
        last_snapshot_idx: u64,
    }
}

impl Lgif {
    pub fn new() -> Self {
        Self {
            first_log_idx: 0,
            first_log_term: 0,
            last_log_idx: 0,
            last_log_term: 0,
            last_committed_log_idx: 0,
            leader_committed_log_idx: 0,
            target_committed_log_idx: 0,
            last_snapshot_idx: 0,
        }
    }

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
        //
        // To parse the data we follow these steps:
        //
        // 1. Create an iterator over the lines of a string. These are split at newlines.
        // 2. Each line is split by the tab, and we make sure that the key and value are
        //    valid.
        // 3. Once we have a HashMap of valid key-value pairs, we set the fields of the
        //    Lgif struct with the retrieved data. To do this we make sure that the name
        //    of each HashMap key matches one of the field names and then we set the
        //    corresponding value.
        let binding = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `clickhouse keeper-config lgif`";
            "output" => ?binding
        );
        let lines = binding.lines();
        let mut lgif: HashMap<String, u64> = HashMap::new();

        for line in lines {
            let line = line.trim();
            if !line.is_empty() {
                let l: Vec<&str> = line.split('\t').collect();

                if l.len() != 2 {
                    bail!(
                        "Command output has a line that does not contain a key-value pair: {l:?}"
                    );
                }

                let key = l[0].to_string();
                let raw_value = l[1];
                let value = match u64::from_str(raw_value) {
                    Ok(v) => v,
                    Err(e) => {
                        bail!(
                            "Unable to convert value {raw_value:?} into u64 for key {key}: {e}"
                        );
                    }
                };

                lgif.insert(key, value);
            }
        }

        let mut keys: Vec<String> = lgif.keys().cloned().collect();
        let mut expected_keys = Lgif::expected_keys();
        keys.sort();
        expected_keys.sort();
        if !keys.eq(&expected_keys) {
            bail!(
                "Command output contains different keys to those expected. \
                Keys: {keys:?} Expected keys: {expected_keys:?}"
            );
        }

        let mut parsed_data = Lgif::new();
        for (key, value) in lgif {
            match parsed_data.set_field_value(&key, value) {
                Ok(()) => (),
                Err(e) => {
                    bail!(
                        "Unable to set Lgif struct field with key value pair: {e}"
                    );
                }
            };
        }

        Ok(parsed_data)
    }

    fn expected_keys() -> Vec<String> {
        vec![
            "first_log_idx".to_string(),
            "first_log_term".to_string(),
            "last_log_idx".to_string(),
            "last_log_term".to_string(),
            "last_committed_log_idx".to_string(),
            "leader_committed_log_idx".to_string(),
            "target_committed_log_idx".to_string(),
            "last_snapshot_idx".to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf;
    use camino_tempfile::Builder;
    use slog::{o, Drain};
    use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::str::FromStr;

    use crate::{
        ClickhouseHost, KeeperId, KeeperSettings, Lgif, RaftServerSettings,
        ServerId, ServerSettings,
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

    #[test]
    fn test_full_lgif_parse_success() {
        let log = log();
        let data =
            "first_log_idx\t1\nfirst_log_term\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386
            \nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
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
            "first_log_idx\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386
            \nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Command output contains different keys to those expected. \
            Keys: [\"first_log_idx\", \"last_committed_log_idx\", \"last_log_idx\", \"last_log_term\", \"last_snapshot_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\"] \
            Expected keys: [\"first_log_idx\", \"first_log_term\", \"last_committed_log_idx\", \"last_log_idx\", \"last_log_term\", \"last_snapshot_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\"]"
        );
    }

    #[test]
    fn test_empty_value_lgif_parse_fail() {
        let log = log();
        let data =
            "first_log_idx\nfirst_log_term\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386
            \nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Command output has a line that does not contain a key-value pair: [\"first_log_idx\"]"
        );
    }

    #[test]
    fn test_non_u64_value_lgif_parse_fail() {
        let log = log();
        let data =
            "first_log_idx\t1\nfirst_log_term\tBOB\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386
            \nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
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
            "first_log_idx\t1\nfirst_log\t1\nlast_log_idx\t4386\nlast_log_term\t1\nlast_committed_log_idx\t4386
            \nleader_committed_log_idx\t4386\ntarget_committed_log_idx\t4386\nlast_snapshot_idx\t0\n\n"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Command output contains different keys to those expected. \
            Keys: [\"first_log\", \"first_log_idx\", \"last_committed_log_idx\", \"last_log_idx\", \"last_log_term\", \"last_snapshot_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\"] \
            Expected keys: [\"first_log_idx\", \"first_log_term\", \"last_committed_log_idx\", \"last_log_idx\", \"last_log_term\", \"last_snapshot_idx\", \"leader_committed_log_idx\", \"target_committed_log_idx\"]"
        );
    }

    #[test]
    fn test_all_nonsense_lgif_parse_fail() {
        let log = log();
        let data = "Mmmbop, ba duba dop ba\nDu bop, ba du dop ba\nYeah, yeah"
            .as_bytes();
        let result = Lgif::parse(&log, data);
        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "Command output has a line that does not contain a key-value pair: [\"Mmmbop, ba duba dop ba\"]",
        );
    }
}
