// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for server types.

use crate::latest::config::{
    ClickhouseHost, KeeperNodeConfig, LogConfig, Macros, NodeType,
    ReplicaConfig, ServerNodeConfig,
};
use crate::latest::server::{
    DistributedDdlQueue, ServerConfigurableSettings, ServerId, ServerSettings,
    SystemTable, SystemTimeSeries, SystemTimeSeriesSettings, TimestampFormat,
};
use anyhow::{Context, Result};
use atomicwrites::AtomicFile;
use camino::Utf8PathBuf;
use omicron_common::api::external::Generation;
use slog::{Logger, info};
use std::fs::create_dir;
use std::io::{ErrorKind, Write};
use std::net::Ipv6Addr;

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

impl std::fmt::Display for SystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let table = match self {
            SystemTable::MetricLog => "metric_log",
            SystemTable::AsynchronousMetricLog => "asynchronous_metric_log",
        };
        write!(f, "{}", table)
    }
}

impl std::fmt::Display for TimestampFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let table = match self {
            TimestampFormat::Utc => "iso",
            TimestampFormat::UnixEpoch => "unix_timestamp",
        };
        write!(f, "{}", table)
    }
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
    use crate::latest::config::ClickhouseHost;
    use crate::latest::server::{
        DistributedDdlQueue, ServerConfigurableSettings, ServerId,
        ServerSettings, SystemTimeSeries,
    };
    use camino::Utf8PathBuf;
    use camino_tempfile::Builder;
    use chrono::{DateTime, Utc};
    use omicron_common::api::external::Generation;
    use slog::{Drain, o};
    use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::str::FromStr;

    fn log() -> slog::Logger {
        let decorator = PlainDecorator::new(TestStdoutWriter);
        let drain = FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
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

        let expected_file =
            Utf8PathBuf::from("../testutils").join("replica-server-config.xml");
        let generated_file = Utf8PathBuf::from(config_dir.path())
            .join("replica-server-config.xml");
        let generated_content = std::fs::read_to_string(generated_file).expect(
            "Failed to read from generated ClickHouse replica server file",
        );

        expectorate::assert_contents(expected_file, &generated_content);
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
