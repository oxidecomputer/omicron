// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke tests for the `omdb` tool
//!
//! Feel free to change the tool's output.  This test just makes it easy to make
//! sure you're only breaking what you intend.

use expectorate::assert_contents;
use nexus_test_utils_macros::nexus_test;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use std::fmt::Write;
use std::path::Path;
use subprocess::Exec;

/// name of the "omdb" executable
const CMD_OMDB: &str = env!("CARGO_BIN_EXE_omdb");

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[tokio::test]
async fn test_omdb_usage_errors() {
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
        &["db", "services"],
        &["db", "snapshots"],
        &["db", "network"],
        &["mgs"],
        &["nexus"],
        &["nexus", "background-tasks"],
        &["sled-agent"],
        &["sled-agent", "zones"],
        &["sled-agent", "zpools"],
    ];

    for args in invocations {
        do_run(&mut output, |exec| exec, &cmd_path, args).await;
    }

    assert_contents("tests/usage_errors.out", &output);
}

#[nexus_test]
async fn test_omdb_success_cases(cptestctx: &ControlPlaneTestContext) {
    let gwtestctx = gateway_test_utils::setup::test_setup(
        "test_omdb_success_case",
        gateway_messages::SpPort::One,
    )
    .await;
    let cmd_path = path_to_executable(CMD_OMDB);
    let postgres_url = cptestctx.database.listen_url();
    let nexus_internal_url =
        format!("http://{}/", cptestctx.internal_client.bind_address);
    let mgs_url = format!("http://{}/", gwtestctx.client.bind_address);
    let mut output = String::new();
    let invocations: &[&[&'static str]] = &[
        &["db", "disks", "list"],
        &["db", "dns", "show"],
        &["db", "dns", "diff", "external", "2"],
        &["db", "dns", "names", "external", "2"],
        &["db", "instances"],
        &["db", "services", "list-instances"],
        &["db", "services", "list-by-sled"],
        &["db", "sleds"],
        &["mgs", "inventory"],
        &["nexus", "background-tasks", "doc"],
        &["nexus", "background-tasks", "show"],
        // We can't easily test the sled agent output because that's only
        // provided by a real sled agent, which is not available in the
        // ControlPlaneTestContext.
    ];

    for args in invocations {
        println!("running commands with args: {:?}", args);
        let p = postgres_url.to_string();
        let u = nexus_internal_url.clone();
        let g = mgs_url.clone();
        do_run(
            &mut output,
            move |exec| {
                exec.env("OMDB_DB_URL", &p)
                    .env("OMDB_NEXUS_URL", &u)
                    .env("OMDB_MGS_URL", &g)
            },
            &cmd_path,
            args,
        )
        .await;
    }

    assert_contents("tests/successes.out", &output);
    gwtestctx.teardown().await;
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
#[nexus_test]
async fn test_omdb_env_settings(cptestctx: &ControlPlaneTestContext) {
    let cmd_path = path_to_executable(CMD_OMDB);
    let postgres_url = cptestctx.database.listen_url().to_string();
    let nexus_internal_url =
        format!("http://{}", cptestctx.internal_client.bind_address);
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

    assert_contents("tests/env.out", &output);
}

async fn do_run<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    println!("running command with args: {:?}", args);
    write!(
        output,
        "EXECUTING COMMAND: {} {:?}\n",
        cmd_path.file_name().expect("missing command").to_string_lossy(),
        args.iter().map(|r| redact_variable(r)).collect::<Vec<_>>(),
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
                    // Set RUST_BACKTRACE for consistency between CI and
                    // developers' local runs.  We set it to 0 only to match
                    // what someone would see who wasn't debugging it, but we
                    // could as well use 1 or "full" to store that instead.
                    .env("RUST_BACKTRACE", "0")
                    .args(&owned_args),
            );
            run_command(exec)
        })
        .await
        .unwrap();

    write!(output, "termination: {:?}\n", exit_status).unwrap();
    write!(output, "---------------------------------------------\n").unwrap();
    write!(output, "stdout:\n").unwrap();
    output.push_str(&redact_variable(&stdout_text));
    write!(output, "---------------------------------------------\n").unwrap();
    write!(output, "stderr:\n").unwrap();
    output.push_str(&redact_variable(&stderr_text));
    write!(output, "=============================================\n").unwrap();
}

/// Redacts text from stdout/stderr that may change from invocation to invocation
/// (e.g., assigned TCP port numbers, timestamps)
///
/// This allows use to use expectorate to verify the shape of the CLI output.
fn redact_variable(input: &str) -> String {
    // Replace TCP port numbers.  We include the localhost characters to avoid
    // catching any random sequence of numbers.
    let s = regex::Regex::new(r"\[::1\]:\d{4,5}")
        .unwrap()
        .replace_all(input, "[::1]:REDACTED_PORT")
        .to_string();
    let s = regex::Regex::new(r"\[::ffff:127.0.0.1\]:\d{4,5}")
        .unwrap()
        .replace_all(&s, "[::ffff:127.0.0.1]:REDACTED_PORT")
        .to_string();
    let s = regex::Regex::new(r"127\.0\.0\.1:\d{4,5}")
        .unwrap()
        .replace_all(&s, "127.0.0.1:REDACTED_PORT")
        .to_string();

    // Replace uuids.
    let s = regex::Regex::new(
        "[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-\
        [a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}",
    )
    .unwrap()
    .replace_all(&s, "REDACTED_UUID_REDACTED_UUID_REDACTED")
    .to_string();

    // Replace timestamps.
    let s = regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
        .unwrap()
        .replace_all(&s, "<REDACTED_TIMESTAMP>")
        .to_string();

    let s = regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
        .unwrap()
        .replace_all(&s, "<REDACTED     TIMESTAMP>")
        .to_string();

    // Replace formatted durations.  These are pretty specific to the background
    // task output.
    let s = regex::Regex::new(r"\d+s ago")
        .unwrap()
        .replace_all(&s, "<REDACTED DURATION>s ago")
        .to_string();

    let s = regex::Regex::new(r"\d+ms")
        .unwrap()
        .replace_all(&s, "<REDACTED DURATION>ms")
        .to_string();

    let s = regex::Regex::new(
        r"note: database schema version matches expected \(\d+\.\d+\.\d+\)",
    )
    .unwrap()
    .replace_all(
        &s,
        "note: database schema version matches expected \
        (<redacted database version>)",
    )
    .to_string();

    s
}
