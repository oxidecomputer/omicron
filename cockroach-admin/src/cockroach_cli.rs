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
use std::net::{SocketAddr, SocketAddrV6};
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
    #[error("cannot decommission node {0}: node is still alive")]
    DecommissionLiveNode(String),
    #[error(
        "cannot decommission node {node_id}: \
         node has {gossiped_replicas} gossiped replicas"
    )]
    DecommissionGossipedReplicas { node_id: String, gossiped_replicas: i64 },
    #[error(
        "cannot decommission node {node_id}: {total_underreplicated_ranges} \
         range(s) underreplicated across node(s) {}",
         .nodes_with_underreplicated_ranges.join(",")
    )]
    DecommissionUnderreplicatedRanges {
        node_id: String,
        total_underreplicated_ranges: i64,
        nodes_with_underreplicated_ranges: Vec<String>,
    },
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
            CockroachCliError::DecommissionLiveNode(_)
            | CockroachCliError::DecommissionGossipedReplicas { .. }
            | CockroachCliError::DecommissionUnderreplicatedRanges { .. } => {
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
    cockroach_http_address: SocketAddr,
}

impl CockroachCli {
    pub fn new(
        path_to_cockroach_binary: Utf8PathBuf,
        cockroach_address: SocketAddrV6,
        cockroach_http_address: SocketAddr,
    ) -> Self {
        Self {
            path_to_cockroach_binary,
            cockroach_address,
            cockroach_http_address,
        }
    }

    pub fn cockroach_address(&self) -> SocketAddrV6 {
        self.cockroach_address
    }

    pub fn cockroach_http_address(&self) -> SocketAddr {
        self.cockroach_http_address
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

    pub async fn node_decommission(
        &self,
        node_id: &str,
    ) -> Result<NodeDecommission, CockroachCliError> {
        let statuses = self.node_status().await?;
        self.validate_node_decommissionable(node_id, statuses)?;
        self.invoke_node_decommission(node_id).await
    }

    // TODO-correctness These validation checks are NOT sufficient; see
    // https://github.com/oxidecomputer/omicron/issues/8445. Our request handler
    // for node decommissioning rejects requests, but we keep this method around
    // for documentation and because we hope to extend it to have correct checks
    // in the near future.
    fn validate_node_decommissionable(
        &self,
        node_id: &str,
        statuses: Vec<NodeStatus>,
    ) -> Result<(), CockroachCliError> {
        // We have three gates for decommissioning a node:
        //
        // 1. The node most not be `live` (the cockroach cluster must realize
        //    it's dead)
        // 2. The node must not have any `gossiped_replicas` (the cockroach
        //    cluster must not think this node is an active member of any ranges
        //    - in practice, we see this go to 0 about 60 seconds after a node
        //    goes offline). This attempts to guard against a window of time
        //    where a node has gone offline but cockroach has not yet reflected
        //    that fact in the counts of underreplicated ranges.
        // 3. The range descriptor for all ranges must not include the node we
        //    want to decommission. This gate shouldn't be necessary, but exists
        //    as a safeguard to avoid triggering what appears to be a CRDB race
        //    condition: https://github.com/oxidecomputer/omicron/issues/8239
        //
        // Cockroach doesn't expose an operation that would let us check gate 3.
        // As a proxy, instead check for a nonzero number of underreplicated
        // ranges _on any nodes_. If there is only 1 dead node, this proxy is
        // exactly equivalent: a range will be underreplicated IFF its range
        // descriptor references the dead node. If there are multiple dead
        // nodes, this proxy becomes fuzzier: we'll refuse to decommission every
        // dead node if any of those dead nodes is not decommissionable. This
        // seems fine in practice.

        // Find the status for the node we want to decommission. If this node
        // has _already_ been decommissioned, we'll get `None` here. This is
        // fine: decommissioning a decommissioned node is a no-op, so we'll skip
        // the first two gates that are specific to the node being
        // decommissioned.
        if let Some(this_node) =
            statuses.iter().find(|status| status.node_id == node_id)
        {
            // Gate 1 - node must not be live
            if this_node.is_live {
                return Err(CockroachCliError::DecommissionLiveNode(
                    node_id.to_string(),
                ));
            }

            // Gate 2 - node must not have gossiped_replicas
            if this_node.gossiped_replicas > 0 {
                return Err(CockroachCliError::DecommissionGossipedReplicas {
                    node_id: node_id.to_string(),
                    gossiped_replicas: this_node.gossiped_replicas,
                });
            }
        }

        // Gate 3 - there must be no underreplicated ranges
        let mut nodes_with_underreplicated_ranges = Vec::new();
        let mut total_underreplicated_ranges: i64 = 0;
        for status in statuses {
            if status.ranges_underreplicated > 0 {
                nodes_with_underreplicated_ranges.push(status.node_id);
                // `saturating_add` is almost certainly overly conservative:
                // we'll never have anywhere near i64::MAX ranges. But we're
                // accepting output from another utility, so it seems prudent to
                // guard against that utility misbehaving and reporting a bogus
                // count of underreplicated ranges.
                total_underreplicated_ranges = total_underreplicated_ranges
                    .saturating_add(status.ranges_underreplicated);
            }
        }
        if total_underreplicated_ranges > 0 {
            return Err(CockroachCliError::DecommissionUnderreplicatedRanges {
                node_id: node_id.to_string(),
                total_underreplicated_ranges,
                nodes_with_underreplicated_ranges,
            });
        }

        Ok(())
    }

    async fn invoke_node_decommission(
        &self,
        node_id: &str,
    ) -> Result<NodeDecommission, CockroachCliError> {
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
    use proptest::collection::btree_map;
    use proptest::prelude::*;
    use proptest::sample::Index;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::process::Child;
    use std::process::ExitStatus;
    use std::time::Duration;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;
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
        let cli = CockroachCli::new(
            "cockroach".into(),
            cockroach_address,
            SocketAddr::V6(cockroach_address),
        );
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
        let cli = CockroachCli::new(
            "cockroach".into(),
            cockroach_address,
            SocketAddr::V6(cockroach_address),
        );
        let result = cli
            .invoke_node_decommission("1")
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
            let cli = CockroachCli::new(
                "cockroach".into(),
                db.listen_addr,
                SocketAddr::V6(db.listen_addr),
            );

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

        let cli = CockroachCli::new(
            "cockroach".into(),
            cockroach_address,
            SocketAddr::V6(cockroach_address),
        );
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
        let cli = CockroachCli::new(
            "cockroach".into(),
            db.listen_addr,
            SocketAddr::V6(db.listen_addr),
        );

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

    // proptest strategy that produces 0 50% of the time and some positive
    // number 50% of the time. We want this because decommissioning gates care
    // specifically about 0, so we want those to show up pretty frequently.
    fn zero_or_positive_i64() -> impl Strategy<Value = i64> {
        prop_oneof![
            1 => Just(0),
            1 => 1i64..=i64::MAX,
        ]
    }

    #[derive(Debug, Arbitrary)]
    struct ValidateDecommissionableInput {
        is_live: bool,
        #[strategy(zero_or_positive_i64())]
        ranges_underreplicated: i64,
        #[strategy(zero_or_positive_i64())]
        gossiped_replicas: i64,
    }

    impl ValidateDecommissionableInput {
        fn into_node_status(self, node_id: String) -> NodeStatus {
            let Self { is_live, ranges_underreplicated, gossiped_replicas } =
                self;
            NodeStatus {
                node_id,
                address: "[::1]:0".parse().unwrap(),
                sql_address: "[::1]:0".parse().unwrap(),
                build: "test-build".to_string(),
                started_at: Utc::now(),
                updated_at: Utc::now(),
                locality: "".to_string(),
                is_available: true,
                is_live,
                replicas_leaders: 0,
                replicas_leaseholders: 0,
                ranges: 0,
                ranges_unavailable: 0,
                ranges_underreplicated,
                live_bytes: 0,
                key_bytes: 0,
                value_bytes: 0,
                intent_bytes: 0,
                system_bytes: 0,
                gossiped_replicas,
                is_decommissioning: false,
                membership: "active".to_string(),
                is_draining: false,
            }
        }
    }

    #[proptest]
    fn proptest_validate_decommissionable(
        #[strategy(btree_map(
              ".+",
              any::<ValidateDecommissionableInput>(),
              1..32,
        ))]
        input: BTreeMap<String, ValidateDecommissionableInput>,
        node_to_decommission: Index,
    ) {
        let statuses = input
            .into_iter()
            .map(|(node_id, input)| input.into_node_status(node_id))
            .collect::<Vec<_>>();
        let node_to_decommission = node_to_decommission.get(&statuses);

        let cli = CockroachCli::new(
            "never-called".into(),
            "[::1]:0".parse().unwrap(),
            SocketAddr::V6("[::1]:0".parse().unwrap()),
        );

        match cli.validate_node_decommissionable(
            &node_to_decommission.node_id,
            statuses.clone(),
        ) {
            Ok(()) => {
                // Check all 3 gates (see `validate_node_decommissionable`)
                assert!(!node_to_decommission.is_live);
                assert_eq!(node_to_decommission.gossiped_replicas, 0);
                for status in &statuses {
                    assert_eq!(status.ranges_underreplicated, 0);
                }
            }
            Err(CockroachCliError::DecommissionLiveNode(node_id)) => {
                assert_eq!(node_id, node_to_decommission.node_id);
                assert!(node_to_decommission.is_live);
            }
            Err(CockroachCliError::DecommissionGossipedReplicas {
                node_id,
                gossiped_replicas,
            }) => {
                assert_eq!(node_id, node_to_decommission.node_id);
                assert_ne!(gossiped_replicas, 0);
            }
            Err(CockroachCliError::DecommissionUnderreplicatedRanges {
                node_id,
                total_underreplicated_ranges,
                nodes_with_underreplicated_ranges: nodes,
            }) => {
                // If node is live, should've returned `DecommissionLiveNode`
                assert!(!node_to_decommission.is_live);

                // If node has gossiped replicas, should've returned
                // `DecommissionGossipedReplicas`
                assert_eq!(node_to_decommission.gossiped_replicas, 0);

                // Confirm error details
                assert_eq!(node_id, node_to_decommission.node_id);
                let mut expected_total_underreplicated: i64 = 0;
                for node in statuses {
                    if node.ranges_underreplicated > 0 {
                        expected_total_underreplicated =
                            expected_total_underreplicated
                                .saturating_add(node.ranges_underreplicated);
                        assert!(nodes.contains(&node.node_id));
                    } else {
                        assert!(!nodes.contains(&node.node_id));
                    }
                }
                assert!(expected_total_underreplicated > 0);
                assert_eq!(
                    total_underreplicated_ranges,
                    expected_total_underreplicated
                );
            }
            Err(err) => {
                panic!("unexpected error: {}", InlineErrorChain::new(&err));
            }
        }
    }
}
