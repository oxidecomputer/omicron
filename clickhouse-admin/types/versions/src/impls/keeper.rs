// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for keeper types.

use crate::latest::config::{ClickhouseHost, LogLevel};
use crate::latest::config::{
    KeeperConfig, LogConfig, NodeType, RaftServerConfig, RaftServerSettings,
    RaftServers,
};
use crate::latest::keeper::{
    KeeperConf, KeeperConfigurableSettings, KeeperId, KeeperServerInfo,
    KeeperServerType, KeeperSettings, Lgif, RaftConfig,
};
use anyhow::{Context, Result, bail};
use atomicwrites::AtomicFile;
use camino::Utf8PathBuf;
use itertools::Itertools;
use omicron_common::api::external::Generation;
use slog::{Logger, info};
use std::fs::create_dir;
use std::io::{ErrorKind, Write};
use std::net::Ipv6Addr;
use std::str::FromStr;

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

impl FromStr for KeeperServerType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<KeeperServerType, Self::Err> {
        match s {
            "participant" => Ok(KeeperServerType::Participant),
            "learner" => Ok(KeeperServerType::Learner),
            _ => bail!("{s} is not a valid keeper server type"),
        }
    }
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

        let mut keeper_servers = std::collections::BTreeSet::new();
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

#[cfg(test)]
mod tests {
    use crate::latest::config::{ClickhouseHost, LogLevel, RaftServerSettings};
    use crate::latest::keeper::{
        KeeperConf, KeeperConfigurableSettings, KeeperId, KeeperServerInfo,
        KeeperServerType, KeeperSettings, Lgif, RaftConfig,
    };
    use camino::Utf8PathBuf;
    use camino_tempfile::Builder;
    use omicron_common::api::external::Generation;
    use slog::{Drain, o};
    use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::str::FromStr;

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

        let expected_file =
            Utf8PathBuf::from("../testutils").join("keeper_config.xml");
        let generated_file =
            Utf8PathBuf::from(config_dir.path()).join("keeper_config.xml");
        let generated_content = std::fs::read_to_string(generated_file)
            .expect("Failed to read from generated ClickHouse keeper file");

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
}
