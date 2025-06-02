// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use cockroach_admin_types::NodeDecommission;
use cockroach_admin_types::NodeStatus;
use cockroach_admin_types::ParseError;
use dropshot::HttpError;
use illumos_utils::ExecutionError;
use illumos_utils::output_to_exec_error;
use slog_error_chain::InlineErrorChain;
use slog_error_chain::SlogInlineError;
use std::io;
use std::net::SocketAddrV6;
use std::process::Output;
use tokio::process::Command;

// Beginning of stderr when calling `cockroach init` on an already-initialized
// cluster. We treat this as successs to make the init function idempotent.
const CLUSTER_ALREADY_INIT: &str =
    "ERROR: cluster has already been initialized";

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum CockroachCliError {
    #[error("failed to invoke `cockroach {subcommand}`")]
    InvokeCli {
        subcommand: &'static str,
        #[source]
        err: io::Error,
    },
    #[error("node {0} not found")]
    NodeNotFound(String),
    #[error("multiple nodes found when asking for status of node {0}")]
    MultipleNodesFound(String),
    #[error("cannot decommission node {node_id}: {reason}")]
    NodeDecommissionNotAllowed { node_id: String, reason: String },
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error("failed to parse stdout {stdout:?}, stderr {stderr:?}")]
    ParseOutput {
        stdout: String,
        stderr: String,
        #[source]
        err: ParseError,
    },
}

impl From<CockroachCliError> for HttpError {
    fn from(err: CockroachCliError) -> Self {
        match err {
            CockroachCliError::NodeNotFound(_) => {
                let message = InlineErrorChain::new(&err).to_string();
                HttpError::for_bad_request(None, message)
            }
            CockroachCliError::NodeDecommissionNotAllowed { .. } => {
                let message = InlineErrorChain::new(&err).to_string();
                HttpError {
                    status_code: dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                    error_code: None,
                    external_message: message.clone(),
                    internal_message: message,
                    headers: None,
                }
            }
            CockroachCliError::InvokeCli { .. }
            | CockroachCliError::MultipleNodesFound(_)
            | CockroachCliError::ExecutionError(_)
            | CockroachCliError::ParseOutput { .. } => {
                let message = InlineErrorChain::new(&err).to_string();
                HttpError {
                    status_code:
                        dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
                    error_code: Some(String::from("Internal")),
                    external_message: message.clone(),
                    internal_message: message,
                    headers: None,
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

    pub async fn cluster_init(&self) -> Result<(), CockroachCliError> {
        self.invoke_cli_raw(
            ["init"],
            |command, output| {
                if output.status.success()
                    || output
                        .stderr
                        .starts_with(CLUSTER_ALREADY_INIT.as_bytes())
                {
                    Ok(())
                } else {
                    Err(output_to_exec_error(command, &output).into())
                }
            },
            "init",
        )
        .await
    }

    pub async fn schema_init(&self) -> Result<(), CockroachCliError> {
        const DBINIT_WITHIN_CRDB_ZONE: &str =
            "/opt/oxide/cockroachdb/sql/dbinit.sql";
        self.schema_init_impl(DBINIT_WITHIN_CRDB_ZONE).await
    }

    async fn schema_init_impl(
        &self,
        path_to_dbinit_sql: &str,
    ) -> Result<(), CockroachCliError> {
        self.invoke_cli_checking_status(
            ["sql", "--file", path_to_dbinit_sql],
            |_output| {
                // We don't check the output (it echos back all of the base SQL
                // statements we issue); we only need to check the status code,
                // which `invoke_cli_checking_status` does for us.
                Ok(())
            },
            "sql dbinit.sql",
        )
        .await
    }

    pub async fn node_status(
        &self,
    ) -> Result<Vec<NodeStatus>, CockroachCliError> {
        self.invoke_cli_with_format_csv(
            ["node", "status", "--all"],
            NodeStatus::parse_from_csv,
            "node status",
        )
        .await
    }

    async fn single_node_status(
        &self,
        node_id: &str,
    ) -> Result<NodeStatus, CockroachCliError> {
        let mut statuses = self
            .invoke_cli_with_format_csv(
                ["node", "status", node_id, "--all"],
                NodeStatus::parse_from_csv,
                "node status",
            )
            .await?;

        let Some(status) = statuses.pop() else {
            return Err(CockroachCliError::NodeNotFound(node_id.to_string()));
        };

        if !statuses.is_empty() {
            return Err(CockroachCliError::MultipleNodesFound(
                node_id.to_string(),
            ));
        }

        Ok(status)
    }

    pub async fn node_decommission(
        &self,
        node_id: &str,
    ) -> Result<NodeDecommission, CockroachCliError> {
        let status = self.single_node_status(node_id).await?;
        self.node_decommission_if_allowed(node_id, status).await
    }

    async fn node_decommission_if_allowed(
        &self,
        node_id: &str,
        status: NodeStatus,
    ) -> Result<NodeDecommission, CockroachCliError> {
        if status.ranges > 0 {
            return Err(CockroachCliError::NodeDecommissionNotAllowed {
                node_id: node_id.to_string(),
                reason: format!("{} ranges", status.ranges),
            });
        }
        self.invoke_cli_with_format_csv(
            ["node", "decommission", node_id, "--wait", "none"],
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
        F: FnOnce(&[u8]) -> Result<T, ParseError>,
        I: IntoIterator<Item = &'a str>,
    {
        self.invoke_cli_checking_status(
            subcommand_args.into_iter().chain(["--format", "csv"]),
            |output| {
                parse_output(&output.stdout).map_err(|err| {
                    CockroachCliError::ParseOutput {
                        stdout: String::from_utf8_lossy(&output.stdout)
                            .to_string(),
                        stderr: String::from_utf8_lossy(&output.stderr)
                            .to_string(),
                        err,
                    }
                })
            },
            subcommand_description,
        )
        .await
    }

    async fn invoke_cli_checking_status<'a, F, I, T>(
        &self,
        subcommand_args: I,
        parse_output: F,
        subcommand_description: &'static str,
    ) -> Result<T, CockroachCliError>
    where
        F: FnOnce(&Output) -> Result<T, CockroachCliError>,
        I: IntoIterator<Item = &'a str>,
    {
        self.invoke_cli_raw(
            subcommand_args,
            |command, output| {
                if !output.status.success() {
                    return Err(output_to_exec_error(command, output).into());
                }
                parse_output(&output)
            },
            subcommand_description,
        )
        .await
    }

    async fn invoke_cli_raw<'a, F, I, T>(
        &self,
        subcommand_args: I,
        parse_output: F,
        subcommand_description: &'static str,
    ) -> Result<T, CockroachCliError>
    where
        F: FnOnce(
            &std::process::Command,
            &Output,
        ) -> Result<T, CockroachCliError>,
        I: IntoIterator<Item = &'a str>,
    {
        let mut command = Command::new(&self.path_to_cockroach_binary);
        for arg in subcommand_args {
            command.arg(arg);
        }
        command
            .arg("--host")
            .arg(&format!("{}", self.cockroach_address))
            .arg("--insecure");
        let output = command.output().await.map_err(|err| {
            CockroachCliError::InvokeCli {
                subcommand: subcommand_description,
                err,
            }
        })?;
        parse_output(command.as_std(), &output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino_tempfile::Utf8TempDir;
    use chrono::Utc;
    use cockroach_admin_types::NodeMembership;
    use nexus_test_utils::db::TestDatabase;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::poll;
    use std::net::SocketAddr;
    use std::process::Child;
    use std::process::ExitStatus;
    use std::time::Duration;
    use url::Url;

    const DBINIT_RELATIVE_PATH: &str = "../schema/crdb/dbinit.sql";

    struct StringifiedOutput {
        stdout: String,
        stderr: String,
        status: ExitStatus,
    }

    // Helper function to execute `command`, log its stdout/stderr/status,
    // and return its output.
    //
    // This is to help debug test flakes like
    // https://github.com/oxidecomputer/omicron/issues/6506.
    async fn exec_command_logging_output(
        command: &mut Command,
    ) -> StringifiedOutput {
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
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
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
        StringifiedOutput { stdout, stderr, status: output.status }
    }

    // Ensure that if `cockroach node status` changes in a future CRDB version
    // bump, we have a test that will fail to force us to check whether our
    // current parsing is still valid.
    #[tokio::test]
    async fn test_node_status_compatibility() {
        let logctx = dev::test_setup_log("test_node_status_compatibility");
        let db = TestDatabase::new_populate_nothing(&logctx.log).await;
        let db_url = db.crdb().listen_url().to_string();

        let expected_headers = "id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live,replicas_leaders,replicas_leaseholders,ranges,ranges_unavailable,ranges_underreplicated,live_bytes,key_bytes,value_bytes,intent_bytes,system_bytes,gossiped_replicas,is_decommissioning,membership,is_draining";

        // Manually run cockroach node status to grab just the CSV header line
        // (which the `csv` crate normally eats on our behalf) and check it's
        // exactly what we expect.
        let mut command = Command::new("cockroach");
        command
            .arg("node")
            .arg("status")
            .arg("--all")
            .arg("--url")
            .arg(&db_url)
            .arg("--format")
            .arg("csv");
        let stdout = exec_command_logging_output(&mut command).await.stdout;

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

        let status =
            cli.single_node_status("1").await.expect("got node status");

        assert_eq!(status.node_id, "1");
        assert_eq!(status.address, SocketAddr::V6(cockroach_address));
        assert_eq!(status.sql_address, SocketAddr::V6(cockroach_address));
        assert_eq!(status.is_available, true);
        assert_eq!(status.is_live, true);

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
        let db = TestDatabase::new_populate_nothing(&logctx.log).await;
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
        let stdout = exec_command_logging_output(&mut command).await.stdout;

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
            .node_decommission_if_allowed(
                "1",
                // Only fields relevant to "is decommissioning okay" are
                // meaningful
                NodeStatus {
                    node_id: "1".to_string(),
                    address: SocketAddr::V6(cockroach_address),
                    sql_address: SocketAddr::V6(cockroach_address),
                    build: "test-build".to_string(),
                    started_at: Utc::now(),
                    updated_at: Utc::now(),
                    locality: "".to_string(),
                    is_available: true,
                    is_live: true,
                    replicas_leaders: 0,
                    replicas_leaseholders: 0,
                    ranges: 0,
                    ranges_unavailable: 0,
                    ranges_underreplicated: 0,
                    live_bytes: 0,
                    key_bytes: 0,
                    value_bytes: 0,
                    intent_bytes: 0,
                    system_bytes: 0,
                    gossiped_replicas: 0,
                    is_decommissioning: false,
                    membership: "active".to_string(),
                    is_draining: false,
                },
            )
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

    // Helper for starting CockroachDb in multinode mode (although still with
    // just one node!), which requires it to be explicitly initialized. We use
    // this to test our `cluster_init` implementation.
    #[derive(Debug)]
    struct UninitializedCockroach {
        _tempdir: Utf8TempDir,
        child: Child,
        listen_addr: SocketAddrV6,
        url: String,
    }

    impl Drop for UninitializedCockroach {
        fn drop(&mut self) {
            self.child.kill().expect("killed cockroach child process");
            self.child.wait().expect("waited for cockroach child process");
        }
    }

    impl UninitializedCockroach {
        // Clippy believes that the `child` process we spawn might be left as a
        // zombie inside the early returns in `poll::wait_for_condition()`.
        // However, this isn't true: we explicitly kill the child process in the
        // case where `wait_for_condition()` fails (and also when we're dropped,
        // in the success case!), so we shouldn't leave behind any zombies.
        //
        // Filed as https://github.com/rust-lang/rust-clippy/issues/14677
        #[allow(clippy::zombie_processes)]
        async fn start() -> Self {
            let tempdir = Utf8TempDir::new().expect("created temp dir");
            let mut command = std::process::Command::new("cockroach");
            command
                .current_dir(&tempdir)
                .arg("start")
                .arg("--insecure")
                .arg("--listen-addr")
                .arg("[::1]:0")
                .arg("--http-addr")
                .arg("[::1]:0")
                .arg("--join")
                .arg("[::1]:0");

            let mut child =
                command.spawn().expect("spawned cockroach child process");

            let listen_addr_path = tempdir
                .path()
                .join("cockroach-data")
                .join("cockroach.listen-addr");

            let retry_interval = Duration::from_secs(1);
            let give_up = Duration::from_secs(30);
            let listen_addr_fut = {
                let listen_addr_path = listen_addr_path.clone();
                poll::wait_for_condition(
                    move || {
                        let listen_addr_path = listen_addr_path.clone();
                        async move {
                            let Ok(contents) =
                                tokio::fs::read_to_string(&listen_addr_path)
                                    .await
                            else {
                                return Err(poll::CondCheckError::<()>::NotYet);
                            };
                            let addr: SocketAddrV6 = match contents.parse() {
                                Ok(addr) => addr,
                                Err(_) => {
                                    return Err(poll::CondCheckError::NotYet);
                                }
                            };
                            Ok(addr)
                        }
                    },
                    &retry_interval,
                    &give_up,
                )
            };

            let listen_addr = match listen_addr_fut.await {
                Ok(addr) => addr,
                Err(err) => {
                    child.kill().expect("killed child cockroach");
                    child.wait().expect("waited for child cockroach");
                    panic!(
                        "failed to wait for listen addr at \
                         {listen_addr_path}: {err:?}",
                    );
                }
            };

            Self {
                _tempdir: tempdir,
                child,
                listen_addr,
                url: format!("postgres://{listen_addr}"),
            }
        }
    }

    // Ensure that if `cockroach init` changes in a future CRDB version bump, we
    // have a test that will fail to force us to check whether our current
    // parsing is still valid.
    #[tokio::test]
    async fn test_cluster_init_compatibility() {
        let logctx = dev::test_setup_log("test_cluster_init_compatibility");

        {
            let db = UninitializedCockroach::start().await;

            // Manually run `cockroach init` to grab the output string and check
            // that it's exactly what we expect.
            let mut command = Command::new("cockroach");
            command.arg("init").arg("--insecure").arg("--url").arg(&db.url);
            let output = exec_command_logging_output(&mut command).await;
            assert!(output.status.success());
            assert_eq!(output.stdout, "Cluster successfully initialized\n");

            // Run it again to confirm the "already initialized" string.
            let stderr = exec_command_logging_output(&mut command).await.stderr;
            assert!(
                stderr.starts_with(CLUSTER_ALREADY_INIT),
                "unexpected stderr: {stderr:?}"
            );
        }

        // Repeat the above test but using our wrapper.
        {
            let db = UninitializedCockroach::start().await;
            let cli = CockroachCli::new("cockroach".into(), db.listen_addr);

            cli.cluster_init().await.expect("cluster initialized");
            cli.cluster_init().await.expect("cluster still initialized");
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_schema_init() {
        let logctx = dev::test_setup_log("test_schema_init");
        let db = TestDatabase::new_populate_nothing(&logctx.log).await;
        let db_url = db.crdb().listen_url().to_string();
        let url: Url = db_url.parse().expect("valid url");
        let cockroach_address: SocketAddrV6 = format!(
            "{}:{}",
            url.host().expect("url has host"),
            url.port().expect("url has port")
        )
        .parse()
        .expect("valid SocketAddrV6");

        let cli = CockroachCli::new("cockroach".into(), cockroach_address);
        cli.schema_init_impl(DBINIT_RELATIVE_PATH)
            .await
            .expect("initialized schema");
        cli.schema_init_impl(DBINIT_RELATIVE_PATH)
            .await
            .expect("initializing schema is idempotent");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_cluster_schema_init_interleaved() {
        let logctx =
            dev::test_setup_log("test_cluster_schema_init_interleaved");
        let db = UninitializedCockroach::start().await;
        let cli = CockroachCli::new("cockroach".into(), db.listen_addr);

        // We should be able to initialize the cluster, then install the schema,
        // then do both of those things again.
        cli.cluster_init().await.expect("cluster initialized");
        cli.schema_init_impl(DBINIT_RELATIVE_PATH)
            .await
            .expect("schema initialized");
        cli.cluster_init().await.expect("cluster still initialized");
        cli.schema_init_impl(DBINIT_RELATIVE_PATH)
            .await
            .expect("schema still initialized");

        logctx.cleanup_successful();
    }
}
