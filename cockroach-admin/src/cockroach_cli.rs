// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use dropshot::HttpError;
use illumos_utils::output_to_exec_error;
use illumos_utils::ExecutionError;
use schemars::JsonSchema;
use serde::de;
use serde::Deserialize;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;
use slog_error_chain::SlogInlineError;
use std::io;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use tokio::process::Command;

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum CockroachCliError {
    #[error("failed to invoke `cockroach {subcommand}`")]
    InvokeCli {
        subcommand: &'static str,
        #[source]
        err: io::Error,
    },
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(
        "failed to parse `cockroach {subcommand}` output \
         (stdout: {stdout}, stderr: {stderr})"
    )]
    ParseOutput {
        subcommand: &'static str,
        stdout: String,
        stderr: String,
        #[source]
        err: csv::Error,
    },
}

impl From<CockroachCliError> for HttpError {
    fn from(err: CockroachCliError) -> Self {
        match err {
            CockroachCliError::InvokeCli { .. }
            | CockroachCliError::ExecutionError(_)
            | CockroachCliError::ParseOutput { .. } => {
                let message = InlineErrorChain::new(&err).to_string();
                HttpError {
                    status_code: http::StatusCode::INTERNAL_SERVER_ERROR,
                    error_code: Some(String::from("Internal")),
                    external_message: message.clone(),
                    internal_message: message,
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct CockroachCli {
    path_to_cockroach_binary: Utf8PathBuf,
    cockroach_address: SocketAddrV6,
}

impl CockroachCli {
    pub fn new(
        path_to_cockroach_binary: Utf8PathBuf,
        cockroach_address: SocketAddrV6,
    ) -> Self {
        Self { path_to_cockroach_binary, cockroach_address }
    }

    pub fn cockroach_address(&self) -> SocketAddrV6 {
        self.cockroach_address
    }

    pub async fn node_status(
        &self,
    ) -> Result<Vec<NodeStatus>, CockroachCliError> {
        let mut command = Command::new(&self.path_to_cockroach_binary);
        command
            .arg("node")
            .arg("status")
            .arg("--host")
            .arg(&format!("{}", self.cockroach_address))
            .arg("--insecure")
            .arg("--format")
            .arg("csv");
        let output = command.output().await.map_err(|err| {
            CockroachCliError::InvokeCli { subcommand: "node status", err }
        })?;
        if !output.status.success() {
            return Err(output_to_exec_error(command.as_std(), &output).into());
        }
        NodeStatus::parse_from_csv(io::Cursor::new(&output.stdout)).map_err(
            |err| CockroachCliError::ParseOutput {
                subcommand: "node status",
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                err,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeStatus {
    pub node_id: String,
    pub address: SocketAddr,
    pub sql_address: SocketAddr,
    pub build: String,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub locality: String,
    pub is_available: bool,
    pub is_live: bool,
}

// Slightly different `NodeStatus` that matches what we get from `cockroach`:
//
// * `id` column instead of `node_id`
// * timestamps are a fixed format with no timezone, so we have a custom
//   deserializer
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct CliNodeStatus {
    id: String,
    address: SocketAddr,
    sql_address: SocketAddr,
    build: String,
    #[serde(deserialize_with = "parse_cockroach_cli_timestamp")]
    started_at: DateTime<Utc>,
    #[serde(deserialize_with = "parse_cockroach_cli_timestamp")]
    updated_at: DateTime<Utc>,
    locality: String,
    is_available: bool,
    is_live: bool,
}

impl From<CliNodeStatus> for NodeStatus {
    fn from(cli: CliNodeStatus) -> Self {
        Self {
            node_id: cli.id,
            address: cli.address,
            sql_address: cli.sql_address,
            build: cli.build,
            started_at: cli.started_at,
            updated_at: cli.updated_at,
            locality: cli.locality,
            is_available: cli.is_available,
            is_live: cli.is_live,
        }
    }
}

fn parse_cockroach_cli_timestamp<'de, D>(
    d: D,
) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct CockroachTimestampVisitor;
    impl<'de> de::Visitor<'de> for CockroachTimestampVisitor {
        type Value = DateTime<Utc>;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("a Cockroach CLI timestamp")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let dt = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(E::custom)?;
            Ok(DateTime::from_naive_utc_and_offset(dt, Utc))
        }
    }

    d.deserialize_str(CockroachTimestampVisitor)
}

impl NodeStatus {
    pub fn parse_from_csv<R>(reader: R) -> Result<Vec<Self>, csv::Error>
    where
        R: io::Read,
    {
        let mut statuses = Vec::new();
        let mut reader = csv::Reader::from_reader(reader);
        for result in reader.deserialize() {
            let record: CliNodeStatus = result?;
            statuses.push(record.into());
        }
        Ok(statuses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use url::Url;

    #[test]
    fn test_node_status_parse_single_line_from_csv() {
        let input = r#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live
1,[::1]:42021,[::1]:42021,v22.1.9,2024-05-21 15:19:50.523796,2024-05-21 16:31:28.050069,,true,true"#;
        let expected = NodeStatus {
            node_id: "1".to_string(),
            address: "[::1]:42021".parse().unwrap(),
            sql_address: "[::1]:42021".parse().unwrap(),
            build: "v22.1.9".to_string(),
            started_at: DateTime::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(2024, 5, 21)
                    .unwrap()
                    .and_hms_micro_opt(15, 19, 50, 523796)
                    .unwrap(),
                Utc,
            ),
            updated_at: DateTime::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(2024, 5, 21)
                    .unwrap()
                    .and_hms_micro_opt(16, 31, 28, 50069)
                    .unwrap(),
                Utc,
            ),
            locality: String::new(),
            is_available: true,
            is_live: true,
        };

        let statuses = NodeStatus::parse_from_csv(io::Cursor::new(input))
            .expect("parsed input");
        assert_eq!(statuses, vec![expected]);
    }

    #[test]
    fn test_node_status_parse_multiple_lines_from_csv() {
        let input = r#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live
1,[fd00:1122:3344:109::3]:32221,[fd00:1122:3344:109::3]:32221,v22.1.9-dirty,2024-05-18 19:18:00.597145,2024-05-21 15:22:34.290434,,true,true
2,[fd00:1122:3344:105::3]:32221,[fd00:1122:3344:105::3]:32221,v22.1.9-dirty,2024-05-18 19:17:01.796714,2024-05-21 15:22:34.901268,,true,true
3,[fd00:1122:3344:10b::3]:32221,[fd00:1122:3344:10b::3]:32221,v22.1.9-dirty,2024-05-18 19:18:52.37564,2024-05-21 15:22:36.341146,,true,true
4,[fd00:1122:3344:107::3]:32221,[fd00:1122:3344:107::3]:32221,v22.1.9-dirty,2024-05-18 19:16:22.788276,2024-05-21 15:22:34.897047,,true,true
5,[fd00:1122:3344:108::3]:32221,[fd00:1122:3344:108::3]:32221,v22.1.9-dirty,2024-05-18 19:18:09.196634,2024-05-21 15:22:35.168738,,true,true"#;
        let expected = vec![
            NodeStatus {
                node_id: "1".to_string(),
                address: "[fd00:1122:3344:109::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:109::3]:32221".parse().unwrap(),
                build: "v22.1.9-dirty".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 18)
                        .unwrap()
                        .and_hms_micro_opt(19, 18, 0, 597145)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 21)
                        .unwrap()
                        .and_hms_micro_opt(15, 22, 34, 290434)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
            },
            NodeStatus {
                node_id: "2".to_string(),
                address: "[fd00:1122:3344:105::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:105::3]:32221".parse().unwrap(),
                build: "v22.1.9-dirty".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 18)
                        .unwrap()
                        .and_hms_micro_opt(19, 17, 1, 796714)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 21)
                        .unwrap()
                        .and_hms_micro_opt(15, 22, 34, 901268)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
            },
            NodeStatus {
                node_id: "3".to_string(),
                address: "[fd00:1122:3344:10b::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:10b::3]:32221".parse().unwrap(),
                build: "v22.1.9-dirty".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 18)
                        .unwrap()
                        .and_hms_micro_opt(19, 18, 52, 375640)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 21)
                        .unwrap()
                        .and_hms_micro_opt(15, 22, 36, 341146)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
            },
            NodeStatus {
                node_id: "4".to_string(),
                address: "[fd00:1122:3344:107::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:107::3]:32221".parse().unwrap(),
                build: "v22.1.9-dirty".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 18)
                        .unwrap()
                        .and_hms_micro_opt(19, 16, 22, 788276)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 21)
                        .unwrap()
                        .and_hms_micro_opt(15, 22, 34, 897047)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
            },
            NodeStatus {
                node_id: "5".to_string(),
                address: "[fd00:1122:3344:108::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:108::3]:32221".parse().unwrap(),
                build: "v22.1.9-dirty".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 18)
                        .unwrap()
                        .and_hms_micro_opt(19, 18, 9, 196634)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 5, 21)
                        .unwrap()
                        .and_hms_micro_opt(15, 22, 35, 168738)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
            },
        ];

        let statuses = NodeStatus::parse_from_csv(io::Cursor::new(input))
            .expect("parsed input");
        assert_eq!(statuses.len(), expected.len());
        for (status, expected) in statuses.iter().zip(&expected) {
            assert_eq!(status, expected);
        }
    }

    // Ensure that if `cockroach node status` changes in a future CRDB version
    // bump, we have a test that will fail to force us to check whether our
    // current parsing is still valid.
    #[tokio::test]
    async fn test_node_status_compatibility() {
        let logctx = dev::test_setup_log("test_node_status_compatibility");
        let mut db = test_setup_database(&logctx.log).await;
        let db_url = db.listen_url().to_string();

        let expected_headers = "id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live";

        // Manually run cockroach node status to grab just the CSV header line
        // (which the `csv` crate normally eats on our behalf) and check it's
        // exactly what we expect.
        let mut command = Command::new("cockroach");
        command
            .arg("node")
            .arg("status")
            .arg("--url")
            .arg(&db_url)
            .arg("--format")
            .arg("csv");
        let output =
            command.output().await.expect("ran `cockroach node status`");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut lines = stdout.lines();
        let headers = lines.next().expect("header line");
        assert_eq!(
            headers, expected_headers,
            "`cockroach node status --format csv` headers may have changed?"
        );

        // We should also be able to run our wrapper against this cockroach.
        let url: Url = db_url.parse().expect("valid url");
        let cockroach_address: SocketAddrV6 = format!(
            "{}:{}",
            url.host().expect("url has host"),
            url.port().expect("url has port")
        )
        .parse()
        .expect("valid SocketAddrV6");
        let cli = CockroachCli::new("cockroach".into(), cockroach_address);
        let status = cli.node_status().await.expect("got node status");

        // We can't check all the fields exactly, but some we know based on the
        // fact that our test database is a single node.
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].node_id, "1");
        assert_eq!(status[0].address, SocketAddr::V6(cockroach_address));
        assert_eq!(status[0].sql_address, SocketAddr::V6(cockroach_address));
        assert_eq!(status[0].is_available, true);
        assert_eq!(status[0].is_live, true);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
