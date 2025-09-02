// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke tests for the `omdb` tool
//!
//! Feel free to change the tool's output.  This test just makes it easy to make
//! sure you're only breaking what you intend.

use dropshot::Method;
use expectorate::assert_contents;
use http::StatusCode;
use nexus_test_utils::wait_for_producer;
use nexus_test_utils::{OXIMETER_UUID, PRODUCER_UUID};
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_common::api::external::SwitchLocation;
use omicron_test_utils::dev::test_cmds::Redactor;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use slog_error_chain::InlineErrorChain;
use std::fmt::Write;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;
use subprocess::Exec;
use uuid::Uuid;

/// name of the "omdb" executable
const CMD_OMDB: &str = env!("CARGO_BIN_EXE_omdb");

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// The `oximeter` list-producers command output is not easy to compare as a
/// string directly because the timing of registrations with both our test
/// producer and the one nexus registers. But, let's find our test producer
/// in the list.
fn assert_oximeter_list_producers_output(
    output: &str,
    ox_url: &str,
    test_producer: IpAddr,
) {
    assert!(
        output.contains(format!("Collector ID: {}", OXIMETER_UUID).as_str())
    );
    assert!(output.contains(ox_url));

    let found = output.lines().any(|line| {
        line.contains(PRODUCER_UUID)
            && line.contains(&test_producer.to_string())
    });

    assert!(
        found,
        "test producer {} and producer UUID {} not found on line together",
        test_producer, PRODUCER_UUID
    );
}

#[tokio::test]
async fn test_omdb_usage_errors() {
    clear_omdb_env();
    let cmd_path = path_to_executable(CMD_OMDB);
    let mut output = String::new();
    let invocations: &[&[&'static str]] = &[
        // No arguments
        &[],
        // Help output
        &["--help"],
        // Bogus command
        &["not-a-command"],
        // Bogus argument
        &["--not-a-command"],
        // Command help output
        &["db"],
        &["db", "--help"],
        &["db", "disks"],
        &["db", "dns"],
        &["db", "dns", "diff"],
        &["db", "dns", "names"],
        &["db", "inventory", "--help"],
        &["db", "inventory", "collections", "--help"],
        &["db", "inventory", "collections", "show"],
        &["db", "inventory", "collections", "show", "--help"],
        &["db", "inventory", "collections", "show", "all", "--help"],
        &["db", "inventory", "collections", "show", "sp", "--help"],
        &["db", "ereport"],
        &["db", "ereport", "list", "--help"],
        &["db", "ereport", "reporters", "--help"],
        &["db", "ereport", "info", "--help"],
        &["db", "sleds", "--help"],
        &["db", "saga"],
        &["db", "snapshots"],
        &["db", "network"],
        &["mgs"],
        &["nexus"],
        &["nexus", "background-tasks"],
        &["nexus", "background-tasks", "show", "--help"],
        &["nexus", "blueprints"],
        &["nexus", "sagas"],
        // Missing "--destructive" flag.  The URL is bogus but just ensures that
        // we get far enough to hit the error we care about.
        &[
            "nexus",
            "--nexus-internal-url",
            "http://[::1]:111",
            "sagas",
            "demo-create",
        ],
        &["nexus", "sleds"],
        &["sled-agent"],
        &["sled-agent", "zones"],
        &["oximeter", "--help"],
        &["oxql", "--help"],
        // Mispelled argument
        &["oxql", "--summarizes"],
        &["reconfigurator"],
        &["reconfigurator", "export"],
        &["reconfigurator", "archive"],
    ];

    for args in invocations {
        do_run(&mut output, |exec| exec, &cmd_path, args).await;
    }

    assert_contents("tests/usage_errors.out", &output);
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_omdb_success_cases(cptestctx: &ControlPlaneTestContext) {
    clear_omdb_env();

    let cmd_path = path_to_executable(CMD_OMDB);

    let postgres_url = cptestctx.database.listen_url();
    let nexus_internal_url =
        format!("http://{}/", cptestctx.internal_client.bind_address);
    let mgs_url = cptestctx
        .gateway
        .get(&SwitchLocation::Switch0)
        .expect("nexus_test always sets up MGS on switch 0")
        .client
        .baseurl();
    let ox_url = format!("http://{}/", cptestctx.oximeter.server_address());
    let ox_test_producer = cptestctx.producer.address().ip();
    let ch_url = format!("http://{}/", cptestctx.clickhouse.http_address());

    let tmpdir = camino_tempfile::tempdir()
        .expect("failed to create temporary directory");
    let tmppath = tmpdir.path().join("reconfigurator-export.out");
    let initial_blueprint_id = cptestctx.initial_blueprint_id.to_string();

    // Get the CockroachDB metadata from the blueprint so we can redact it
    let initial_blueprint: Blueprint = dropshot::test_util::read_json(
        &mut cptestctx
            .internal_client
            .make_request_no_body(
                Method::GET,
                &format!("/deployment/blueprints/all/{initial_blueprint_id}"),
                StatusCode::OK,
            )
            .await
            .unwrap(),
    )
    .await;

    // Wait for Nexus to have gathered at least one inventory collection. (We'll
    // check below that `reconfigurator export` contains at least one, so have
    // to wait until there's one to export.)
    cptestctx
        .wait_for_at_least_one_inventory_collection(Duration::from_secs(60))
        .await;

    let mut output = String::new();

    let invocations: &[&[&str]] = &[
        &["db", "disks", "list"],
        &["db", "dns", "show"],
        &["db", "dns", "diff", "external", "2"],
        &["db", "dns", "names", "external", "2"],
        &["db", "instances"],
        &["db", "sleds"],
        &["db", "sleds", "-F", "discretionary"],
        &["mgs", "inventory"],
        &["nexus", "background-tasks", "doc"],
        // Hide "currently executing" to avoid a test flake in case a task is
        // running while this command is run. But note that there are other
        // output lines (particularly "last completed activation") which can
        // potentially be flaky. We haven't seen "last completed activation"
        // actually being flaky yet, though.
        &["nexus", "background-tasks", "show", "--no-executing-info"],
        // background tasks: test picking out specific names
        &[
            "nexus",
            "background-tasks",
            "show",
            "saga_recovery",
            "--no-executing-info",
        ],
        &[
            "nexus",
            "background-tasks",
            "show",
            "blueprint_loader",
            "blueprint_executor",
            "--no-executing-info",
        ],
        // background tasks: test recognized group names
        &[
            "nexus",
            "background-tasks",
            "show",
            "dns_internal",
            "--no-executing-info",
        ],
        &[
            "nexus",
            "background-tasks",
            "show",
            "dns_external",
            "--no-executing-info",
        ],
        &["nexus", "background-tasks", "show", "all", "--no-executing-info"],
        &["nexus", "sagas", "list"],
        &["--destructive", "nexus", "sagas", "demo-create"],
        &["nexus", "sagas", "list"],
        &[
            "--destructive",
            "nexus",
            "background-tasks",
            "activate",
            "inventory_collection",
        ],
        &["nexus", "blueprints", "list"],
        &["nexus", "blueprints", "show", &initial_blueprint_id],
        &["nexus", "blueprints", "show", "current-target"],
        &[
            "nexus",
            "blueprints",
            "diff",
            &initial_blueprint_id,
            "current-target",
        ],
        // This one should fail because it has no parent.
        &["nexus", "blueprints", "diff", &initial_blueprint_id],
        // chicken switches: show and set
        &["nexus", "chicken-switches", "show", "current"],
        &[
            "-w",
            "nexus",
            "chicken-switches",
            "set",
            "--planner-enabled",
            "true",
        ],
        &[
            "-w",
            "nexus",
            "chicken-switches",
            "set",
            "--add-zones-with-mupdate-override",
            "false",
        ],
        // After the set commands above, we should see chicken switches
        // populated.
        &["nexus", "chicken-switches", "show", "current"],
        &["reconfigurator", "export", tmppath.as_str()],
        // We can't easily test the sled agent output because that's only
        // provided by a real sled agent, which is not available in the
        // ControlPlaneTestContext.
    ];

    let mut redactor = Redactor::default();
    redactor
        .extra_variable_length("tmp_path", tmppath.as_str())
        .extra_fixed_length("blueprint_id", &initial_blueprint_id)
        .extra_variable_length(
            "cockroachdb_fingerprint",
            &initial_blueprint.cockroachdb_fingerprint,
        );

    let crdb_version =
        initial_blueprint.cockroachdb_setting_preserve_downgrade.to_string();
    if initial_blueprint.cockroachdb_setting_preserve_downgrade.is_set() {
        redactor.extra_variable_length("cockroachdb_version", &crdb_version);
    }

    // The `tuf_artifact_replication` task's output depends on how
    // many sleds happened to register with Nexus before its first
    // execution. These redactions work around the issue described in
    // https://github.com/oxidecomputer/omicron/issues/7417.
    redactor
        .field("put config ok:", r"\d+")
        .field("list ok:", r"\d+")
        .field("triggered by", r"[\w ]+")
        .section(&["task: \"tuf_artifact_replication\"", "request ringbuf:"]);

    for args in invocations {
        println!("running commands with args: {:?}", args);
        let p = postgres_url.to_string();
        let u = nexus_internal_url.clone();
        let g = mgs_url.clone();
        let ox = ox_url.clone();
        let ch = ch_url.clone();
        do_run_extra(
            &mut output,
            move |exec| {
                exec.env("OMDB_DB_URL", &p)
                    .env("OMDB_NEXUS_URL", &u)
                    .env("OMDB_MGS_URL", &g)
                    .env("OMDB_OXIMETER_URL", &ox)
                    .env("OMDB_CLICKHOUSE_URL", &ch)
            },
            &cmd_path,
            args,
            &redactor,
        )
        .await;
    }

    assert_contents("tests/successes.out", &output);

    // The `reconfigurator-save` output is not easy to compare as a string.  But
    // let's make sure we can at least parse it and that it looks broadly like
    // what we'd expect.
    let generated = std::fs::read_to_string(&tmppath).unwrap_or_else(|error| {
        panic!(
            "failed to read temporary file containing reconfigurator-save \
            output: {:?}: {}",
            tmppath,
            InlineErrorChain::new(&error),
        )
    });
    let parsed: UnstableReconfiguratorState = serde_json::from_str(&generated)
        .unwrap_or_else(|error| {
            panic!(
                "failed to parse reconfigurator-save output (path {}): {}",
                tmppath,
                InlineErrorChain::new(&error),
            )
        });
    // Did we find at least one sled in the planning input, and at least one
    // collection?
    assert!(
        parsed
            .planning_input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .is_some()
    );
    assert!(!parsed.collections.is_empty());

    let ox_invocation = &["oximeter", "list-producers"];
    let mut ox_output = String::new();
    let ox = ox_url.clone();

    wait_for_producer(
        &cptestctx.oximeter,
        PRODUCER_UUID.parse::<Uuid>().unwrap(),
    )
    .await;
    do_run_no_redactions(
        &mut ox_output,
        move |exec| exec.env("OMDB_OXIMETER_URL", &ox),
        &cmd_path,
        ox_invocation,
    )
    .await;
    assert_oximeter_list_producers_output(
        &ox_output,
        &ox_url,
        ox_test_producer,
    );
}

/// Verify that we properly deal with cases where:
///
/// - a URL is specified on the command line
/// - a URL is specified in both places
///
/// for both of the URLs that we accept.  We don't need to check the cases where
/// (1) no URL is specified in either place because that's covered by the usage
/// test above, nor (2) the URL is specified only in the environment because
/// that's covered by the success tests above.
#[nexus_test(extra_sled_agents = 1)]
async fn test_omdb_env_settings(cptestctx: &ControlPlaneTestContext) {
    clear_omdb_env();

    let cmd_path = path_to_executable(CMD_OMDB);
    let postgres_url = cptestctx.database.listen_url().to_string();
    let nexus_internal_url =
        format!("http://{}", cptestctx.internal_client.bind_address);
    let ox_url = format!("http://{}/", cptestctx.oximeter.server_address());
    let ox_test_producer = cptestctx.producer.address().ip();
    let ch_url = format!("http://{}/", cptestctx.clickhouse.http_address());
    let dns_sockaddr = cptestctx.internal_dns.dns_server.local_address();
    let mut output = String::new();

    // Database URL
    // Case 1: specified on the command line
    let args = &["db", "--db-url", &postgres_url, "sleds"];
    do_run(&mut output, |exec| exec, &cmd_path, args).await;

    // Case 2: specified in multiple places (command-line argument wins)
    let args = &["db", "--db-url", "junk", "sleds"];
    let p = postgres_url.clone();
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DB_URL", &p),
        &cmd_path,
        args,
    )
    .await;

    // Nexus URL
    // Case 1: specified on the command line
    let args = &[
        "nexus",
        "--nexus-internal-url",
        &nexus_internal_url.clone(),
        "background-tasks",
        "doc",
    ];
    do_run(&mut output, |exec| exec, &cmd_path.clone(), args).await;

    // Case 2: specified in multiple places (command-line argument wins)
    let args =
        &["nexus", "--nexus-internal-url", "junk", "background-tasks", "doc"];
    let n = nexus_internal_url.clone();
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_NEXUS_URL", &n),
        &cmd_path,
        args,
    )
    .await;

    // Verify that if you provide a working internal DNS server, you can omit
    // the URLs.  That's true regardless of whether you pass it on the command
    // line or via an environment variable.
    let args = &["nexus", "background-tasks", "doc"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DNS_SERVER", dns_sockaddr.to_string()),
        &cmd_path,
        args,
    )
    .await;

    let args = &[
        "--dns-server",
        &dns_sockaddr.to_string(),
        "nexus",
        "background-tasks",
        "doc",
    ];
    do_run(&mut output, move |exec| exec, &cmd_path, args).await;

    let args = &["db", "sleds"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DNS_SERVER", dns_sockaddr.to_string()),
        &cmd_path,
        args,
    )
    .await;

    let args = &["--dns-server", &dns_sockaddr.to_string(), "db", "sleds"];
    do_run(&mut output, move |exec| exec, &cmd_path, args).await;

    // That said, the "sagas" command prints an extra warning in this case.
    let args = &["nexus", "sagas", "list"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DNS_SERVER", &dns_sockaddr.to_string()),
        &cmd_path,
        args,
    )
    .await;

    // Case: specified in multiple places (command-line argument wins)
    let args = &["oximeter", "--oximeter-url", "junk", "list-producers"];
    let ox = ox_url.clone();
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_OXIMETER_URL", &ox),
        &cmd_path,
        args,
    )
    .await;

    // Case: specified in multiple places (command-line argument wins)
    let args = &["oxql", "--clickhouse-url", "junk"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_CLICKHOUSE_URL", &ch_url),
        &cmd_path,
        args,
    )
    .await;

    assert_contents("tests/env.out", &output);

    // Oximeter URL
    // Case 1: specified on the command line.
    // Case 2: is covered by the success tests above.
    let ox_args1 = &["oximeter", "--oximeter-url", &ox_url, "list-producers"];
    let mut ox_output1 = String::new();
    wait_for_producer(
        &cptestctx.oximeter,
        PRODUCER_UUID.parse::<Uuid>().unwrap(),
    )
    .await;
    do_run_no_redactions(
        &mut ox_output1,
        move |exec| exec,
        &cmd_path,
        ox_args1,
    )
    .await;
    assert_oximeter_list_producers_output(
        &ox_output1,
        &ox_url,
        ox_test_producer,
    );
}

async fn do_run<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    do_run_extra(output, modexec, cmd_path, args, &Redactor::default()).await;
}

async fn do_run_no_redactions<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    do_run_extra(output, modexec, cmd_path, args, &Redactor::noop()).await;
}

async fn do_run_extra<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
    redactor: &Redactor<'_>,
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    write!(
        output,
        "EXECUTING COMMAND: {} {:?}\n",
        cmd_path.file_name().expect("missing command").to_string_lossy(),
        args.iter().map(|r| redactor.do_redact(r)).collect::<Vec<_>>()
    )
    .unwrap();

    // Using `subprocess`, the child process will be spawned synchronously.  In
    // some cases it then tries to make an HTTP request back into this process.
    // But if the executor is blocked on the child process, the HTTP server
    // acceptor won't run and we'll deadlock.  So we use spawn_blocking() to run
    // the child process from a different thread.  tokio requires that these be
    // 'static (it does not know that we're going to wait synchronously for the
    // task) so we need to create owned versions of these arguments.
    let cmd_path = cmd_path.to_owned();
    let owned_args: Vec<_> = args.into_iter().map(|s| s.to_string()).collect();
    let (exit_status, stdout_text, stderr_text) =
        tokio::task::spawn_blocking(move || {
            let exec = modexec(
                Exec::cmd(cmd_path)
                    // Set RUST_BACKTRACE explicitly for consistency between CI
                    // and developers' local runs.  We set it to 1 so that in
                    // the event of a panic, particularly in CI, we have more
                    // information about what went wrong.  But we set
                    // RUST_LIB_BACKTRACE=1 so that we don't get a dump to
                    // stderr for all the Errors that get created and handled
                    // gracefully.
                    .env("RUST_BACKTRACE", "1")
                    .env("RUST_LIB_BACKTRACE", "0")
                    .args(&owned_args),
            );
            run_command(exec)
        })
        .await
        .unwrap();

    write!(output, "termination: {:?}\n", exit_status).unwrap();
    write!(output, "---------------------------------------------\n").unwrap();
    write!(output, "stdout:\n").unwrap();
    output.push_str(&redactor.do_redact(&stdout_text));

    write!(output, "---------------------------------------------\n").unwrap();
    write!(output, "stderr:\n").unwrap();
    output.push_str(&redactor.do_redact(&stderr_text));

    write!(output, "=============================================\n").unwrap();
}

// We're testing behavior that can be affected by OMDB-related environment
// variables.  Clear all of them from the current process so that all child
// processes don't have them.  OMDB environment variables can affect even the
// help output provided by clap.  See clap-rs/clap#5673 for an example.
fn clear_omdb_env() {
    // Rust documents that it's not safe to manipulate the environment in a
    // multi-threaded process outside of Windows because it's possible that
    // other threads are reading or writing the environment and most systems do
    // not support this.  On illumos, the underlying interfaces are broadly
    // thread-safe.  Further, Omicron only supports running tests under `cargo
    // nextest`, in which case there are no threads running concurrently here
    // that may be reading or modifying the environment.
    for (env_var, _) in std::env::vars().filter(|(k, _)| k.starts_with("OMDB_"))
    {
        eprintln!("removing {:?} from environment", env_var);
        std::env::remove_var(env_var);
    }
}
