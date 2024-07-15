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
        self.invoke_cli_with_format_csv(
            ["node", "status"].into_iter(),
            NodeStatus::parse_from_csv,
            "node status",
        )
        .await
    }

    pub async fn node_decommission(
        &self,
        node_id: &str,
    ) -> Result<NodeDecommission, CockroachCliError> {
        self.invoke_cli_with_format_csv(
            ["node", "decommission", node_id, "--wait", "none"].into_iter(),
            NodeDecommission::parse_from_csv,
            "node decommission",
        )
        .await
    }

    async fn invoke_cli_with_format_csv<'a, F, I, T>(
        &self,
        subcommand_args: I,
        parse_output: F,
        subcommand_description: &'static str,
    ) -> Result<T, CockroachCliError>
    where
        F: FnOnce(&[u8]) -> Result<T, csv::Error>,
        I: Iterator<Item = &'a str>,
    {
        let mut command = Command::new(&self.path_to_cockroach_binary);
        for arg in subcommand_args {
            command.arg(arg);
        }
        command
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
        parse_output(&output.stdout).map_err(|err| {
            CockroachCliError::ParseOutput {
                subcommand: subcommand_description,
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                err,
            }
        })
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
// timestamps are a fixed format with no timezone (but are actually UTC), so we
// have a custom deserializer, and the ID column is `id` instead of `node_id`.
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
    pub fn parse_from_csv(data: &[u8]) -> Result<Vec<Self>, csv::Error> {
        let mut statuses = Vec::new();
        let mut reader = csv::Reader::from_reader(io::Cursor::new(data));
        for result in reader.deserialize() {
            let record: CliNodeStatus = result?;
            statuses.push(record.into());
        }
        Ok(statuses)
    }
}

// The cockroach CLI and `crdb_internal.gossip_liveness` table use a string for
// node membership, but there are only three meaningful values per
// https://github.com/cockroachdb/cockroach/blob/0c92c710d2baadfdc5475be8d2238cf26cb152ca/pkg/kv/kvserver/liveness/livenesspb/liveness.go#L96,
// so we'll convert into a Rust enum and leave the "unknown" case for future
// changes that expand or reword these values.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum NodeMembership {
    Active,
    Decommissioning,
    Decommissioned,
    Unknown { value: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeDecommission {
    pub node_id: String,
    pub is_live: bool,
    pub replicas: i64,
    pub is_decommissioning: bool,
    pub membership: NodeMembership,
    pub is_draining: bool,
    pub notes: Vec<String>,
}

// Slightly different `NodeDecommission` that matches what we get from
// `cockroach`: this omites `notes`, which isn't really a CSV field at all, but
// is instead where we collect the non-CSV string output from the CLI, uses
// a custom deserializer for `membership` to handle unknown variants, and the ID
// column is `id` instead of `node_id`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct CliNodeDecommission {
    pub id: String,
    pub is_live: bool,
    pub replicas: i64,
    pub is_decommissioning: bool,
    #[serde(deserialize_with = "parse_node_membership")]
    pub membership: NodeMembership,
    pub is_draining: bool,
}

impl From<(CliNodeDecommission, Vec<String>)> for NodeDecommission {
    fn from((cli, notes): (CliNodeDecommission, Vec<String>)) -> Self {
        Self {
            node_id: cli.id,
            is_live: cli.is_live,
            replicas: cli.replicas,
            is_decommissioning: cli.is_decommissioning,
            membership: cli.membership,
            is_draining: cli.is_draining,
            notes,
        }
    }
}

fn parse_node_membership<'de, D>(d: D) -> Result<NodeMembership, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct CockroachNodeMembershipVisitor;

    impl<'de> de::Visitor<'de> for CockroachNodeMembershipVisitor {
        type Value = NodeMembership;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("a Cockroach node membership string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let membership = match v {
                "active" => NodeMembership::Active,
                "decommissioning" => NodeMembership::Decommissioning,
                "decommissioned" => NodeMembership::Decommissioned,
                _ => NodeMembership::Unknown { value: v.to_string() },
            };
            Ok(membership)
        }
    }

    d.deserialize_str(CockroachNodeMembershipVisitor)
}

impl NodeDecommission {
    pub fn parse_from_csv(data: &[u8]) -> Result<Self, csv::Error> {
        // Reading the node decommission output is awkward because it isn't
        // fully CSV. We expect a CSV header, then a row for each node being
        // decommissioned, then (maybe) a blank line followed by a note that is
        // just a string, not related to the initial CSV data. Even though the
        // CLI supports decommissioning more than one node in one invocation, we
        // only provide an API to decommission a single node, so we expect:
        //
        // 1. The CSV header line
        // 2. The one row of CSV data
        // 3. Trailing notes
        //
        // We'll collect the notes as a separate field and return them to our
        // caller.

        // First we'll run the data through a csv::Reader; this will pull out
        // the header row and the one row of data.
        let mut reader = csv::Reader::from_reader(io::Cursor::new(data));
        let record: CliNodeDecommission =
            reader.deserialize().next().ok_or_else(|| {
                io::Error::other("fewer than two lines of output")
            })??;

        // Get the position where the reader ended after that one row; we'll
        // collect any remaining nonempty lines as `notes`.
        let extra_data = &data[reader.position().byte() as usize..];
        let mut notes = Vec::new();
        for line in String::from_utf8_lossy(extra_data).lines() {
            let line = line.trim();
            if !line.is_empty() {
                notes.push(line.to_string());
            }
        }

        Ok(Self::from((record, notes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use test_strategy::proptest;
    use url::Url;

    #[test]
    fn test_node_status_parse_single_line_from_csv() {
        let input = br#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live
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

        let statuses = NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, vec![expected]);
    }

    #[test]
    fn test_node_status_parse_multiple_lines_from_csv() {
        let input = br#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live
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

        let statuses = NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses.len(), expected.len());
        for (status, expected) in statuses.iter().zip(&expected) {
            assert_eq!(status, expected);
        }
    }

    #[test]
    fn test_node_decommission_parse_with_no_trailing_notes() {
        let input =
            br#"id,is_live,replicas,is_decommissioning,membership,is_draining
6,true,24,true,decommissioning,false"#;
        let expected = NodeDecommission {
            node_id: "6".to_string(),
            is_live: true,
            replicas: 24,
            is_decommissioning: true,
            membership: NodeMembership::Decommissioning,
            is_draining: false,
            notes: vec![],
        };

        let statuses =
            NodeDecommission::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, expected);
    }

    #[test]
    fn test_node_decommission_parse_with_trailing_notes() {
        let input =
            br#"id,is_live,replicas,is_decommissioning,membership,is_draining
6,false,0,true,decommissioned,false

No more data reported on target nodes. Please verify cluster health before removing the nodes.
"#;
        let expected = NodeDecommission {
            node_id: "6".to_string(),
            is_live: false,
            replicas: 0,
            is_decommissioning: true,
            membership: NodeMembership::Decommissioned,
            is_draining: false,
            notes: vec!["No more data reported on target nodes. \
                Please verify cluster health before removing the nodes."
                .to_string()],
        };

        let statuses =
            NodeDecommission::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, expected);
    }

    #[test]
    fn test_node_decommission_parse_with_unexpected_membership_value() {
        let input =
            br#"id,is_live,replicas,is_decommissioning,membership,is_draining
6,false,0,true,foobar,false"#;
        let expected = NodeDecommission {
            node_id: "6".to_string(),
            is_live: false,
            replicas: 0,
            is_decommissioning: true,
            membership: NodeMembership::Unknown { value: "foobar".to_string() },
            is_draining: false,
            notes: vec![],
        };

        let statuses =
            NodeDecommission::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, expected);
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

    // Ensure that if `cockroach node decommission` changes in a future CRDB
    // version bump, we have a test that will fail to force us to check whether
    // our current parsing is still valid.
    #[tokio::test]
    async fn test_node_decommission_compatibility() {
        let logctx =
            dev::test_setup_log("test_node_decommission_compatibility");
        let mut db = test_setup_database(&logctx.log).await;
        let db_url = db.listen_url().to_string();

        let expected_headers =
            "id,is_live,replicas,is_decommissioning,membership,is_draining";

        // Manually run cockroach node decommission to grab just the CSV header
        // line (which the `csv` crate normally eats on our behalf) and check
        // it's exactly what we expect.
        let mut command = Command::new("cockroach");
        command
            .arg("node")
            .arg("decommission")
            .arg("1")
            .arg("--wait")
            .arg("none")
            .arg("--url")
            .arg(&db_url)
            .arg("--format")
            .arg("csv");
        let output =
            command.output().await.expect("ran `cockroach node decommission`");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut lines = stdout.lines();
        let headers = lines.next().expect("header line");
        assert_eq!(
            headers, expected_headers,
            "`cockroach node decommission --format csv` headers \
            may have changed?"
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
        let result = cli
            .node_decommission("1")
            .await
            .expect("got node decommission result");

        // We can't check all the fields exactly (e.g., replicas), but most we
        // know based on the fact that our test database is a single node, so
        // won't actually decommission itself.
        assert_eq!(result.node_id, "1");
        assert_eq!(result.is_live, true);
        assert_eq!(result.is_decommissioning, true);
        assert_eq!(result.membership, NodeMembership::Decommissioning);
        assert_eq!(result.is_draining, false);
        assert_eq!(result.notes, &[] as &[&str]);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[proptest]
    fn node_status_parse_doesnt_panic_on_arbitrary_input(input: Vec<u8>) {
        _ = NodeStatus::parse_from_csv(&input);
    }

    #[proptest]
    fn node_decommission_parse_doesnt_panic_on_arbitrary_input(input: Vec<u8>) {
        _ = NodeDecommission::parse_from_csv(&input);
    }
}
