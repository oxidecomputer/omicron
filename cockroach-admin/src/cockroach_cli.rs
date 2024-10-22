// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use cockroach_admin_types::NodeDecommission;
use cockroach_admin_types::NodeStatus;
use dropshot::HttpError;
use illumos_utils::output_to_exec_error;
use illumos_utils::ExecutionError;
use slog_error_chain::InlineErrorChain;
use slog_error_chain::SlogInlineError;
use std::io;
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

#[cfg(test)]
mod tests {
    use super::*;
    use cockroach_admin_types::NodeMembership;
    use nexus_test_utils::db::TestDatabase;
    use omicron_test_utils::dev;
    use std::net::SocketAddr;
    use url::Url;

    // Helper function to execute `command`, log its stdout/stderr/status, and
    // return its stdout.
    //
    // This is to help debug test flakes like
    // https://github.com/oxidecomputer/omicron/issues/6506.
    async fn exec_command_logging_output(command: &mut Command) -> String {
        let command_str = command
            .as_std()
            .get_args()
            .map(|s| s.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        let output = match command.output().await {
            Ok(output) => output,
            Err(err) => panic!("failed executing [{command_str}]: {err}"),
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Executed [{command_str}]");
        eprintln!("  Status: {}", output.status);
        eprintln!("  Stdout:");
        eprintln!("----------------");
        eprintln!("{stdout}");
        eprintln!("----------------");
        eprintln!("  Stderr:");
        eprintln!("----------------");
        eprintln!("{stderr}");
        eprintln!("----------------");
        stdout.to_string()
    }

    // Ensure that if `cockroach node status` changes in a future CRDB version
    // bump, we have a test that will fail to force us to check whether our
    // current parsing is still valid.
    #[tokio::test]
    async fn test_node_status_compatibility() {
        let logctx = dev::test_setup_log("test_node_status_compatibility");
        let db = TestDatabase::new_without_schema(&logctx.log).await;
        let db_url = db.crdb().listen_url().to_string();

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
        let stdout = exec_command_logging_output(&mut command).await;

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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Ensure that if `cockroach node decommission` changes in a future CRDB
    // version bump, we have a test that will fail to force us to check whether
    // our current parsing is still valid.
    #[tokio::test]
    async fn test_node_decommission_compatibility() {
        let logctx =
            dev::test_setup_log("test_node_decommission_compatibility");
        let db = TestDatabase::new_without_schema(&logctx.log).await;
        let db_url = db.crdb().listen_url().to_string();

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
        let stdout = exec_command_logging_output(&mut command).await;

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

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
