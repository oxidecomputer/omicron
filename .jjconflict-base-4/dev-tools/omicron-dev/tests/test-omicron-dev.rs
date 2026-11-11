// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke tests for the omicron-dev command-line tool

use anyhow::Context;
use expectorate::assert_contents;
use omicron_dev_lib::test_utils::verify_graceful_exit;
use omicron_test_utils::dev::CRDB_SEED_TAR_ENV;
use omicron_test_utils::dev::db::has_omicron_schema;
use omicron_test_utils::dev::process_running;
use omicron_test_utils::dev::test_cmds::EXIT_USAGE;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use oxide_client::ClientConsoleAuthExt;
use std::io::BufRead;
use std::path::PathBuf;
use subprocess::Exec;
use subprocess::Redirection;

/// name of the "omicron-dev" executable
const CMD_OMICRON_DEV: &str = env!("CARGO_BIN_EXE_omicron-dev");

fn path_to_omicron_dev() -> PathBuf {
    path_to_executable(CMD_OMICRON_DEV)
}

/// Encapsulates the information we need from a running `omicron-dev run-all`
/// command.
#[derive(Debug)]
struct RunAll {
    subproc: subprocess::Popen,
    cmd_pid: u32,
    db_pid: u32,
    postgres_config: tokio_postgres::Config,
    temp_dir: PathBuf,
    external_url: String,
}

/// Like `run_db_run()`, but for the `run-all` command
fn run_run_all(exec: Exec) -> RunAll {
    let cmdline = exec.to_cmdline_lossy();
    eprintln!("will run: {}", cmdline);

    let subproc = exec
        .stdout(Redirection::Pipe)
        .popen()
        .expect("failed to start command");
    let mut subproc_out =
        std::io::BufReader::new(subproc.stdout.as_ref().unwrap());
    let cmd_pid = subproc.pid().unwrap();
    let (mut db_pid, mut external_url, mut postgres_url, mut temp_dir) =
        (None, None, None, None);

    eprintln!("waiting for stdout from child process");
    while db_pid.is_none()
        || external_url.is_none()
        || postgres_url.is_none()
        || temp_dir.is_none()
    {
        let mut buf = String::with_capacity(80);
        match subproc_out.read_line(&mut buf) {
            Ok(0) => {
                panic!("unexpected EOF from child process stdout");
            }
            Err(e) => {
                panic!("unexpected error reading child process stdout: {}", e);
            }
            Ok(_) => {
                print!("subproc stdout: {}", buf);
            }
        }

        if let Some(s) =
            buf.strip_prefix("omicron-dev: cockroachdb directory: ")
        {
            eprint!("found cockroachdb directory: {}", s);
            temp_dir = Some(PathBuf::from(s.trim().to_string()));
            continue;
        }

        if let Some(s) = buf.strip_prefix("omicron-dev: nexus external API: ") {
            eprint!("found Nexus external API: {}", s);
            external_url = Some(s.trim().to_string());
            continue;
        }

        if let Some(s) = buf.strip_prefix("omicron-dev: cockroachdb pid: ") {
            eprint!("found cockroachdb pid: {}", s);
            db_pid =
                Some(s.trim().to_string().parse().expect("pid was not a u32"));
            continue;
        }

        if let Some(s) = buf.strip_prefix("omicron-dev: cockroachdb URL: ") {
            eprint!("found postgres listen URL: {}", s);
            postgres_url = Some(s.trim().to_string());
            continue;
        }
    }

    assert!(process_running(cmd_pid));

    let postgres_config = postgres_url
        .as_ref()
        .unwrap()
        .parse::<tokio_postgres::Config>()
        .expect("invalid PostgreSQL URL");

    RunAll {
        subproc,
        cmd_pid,
        db_pid: db_pid.unwrap(),
        external_url: external_url.unwrap(),
        postgres_config,
        temp_dir: temp_dir.unwrap(),
    }
}

// Exercises the normal use case of `omicron-dev run-all`: everything starts up,
// we can connect to Nexus and CockroachDB and query them, then we simulate the
// user typing ^C at the shell, and then it cleans up its temporary directory.
//
// This mirrors the `test_db_run()` test.
#[tokio::test]
async fn test_run_all() {
    // Ensure that the CRDB_SEED_TAR environment variable is not set. We want to
    // simulate a user running omicron-dev without the test environment.
    // Check if CRDB_SEED_TAR_ENV is set and panic if it is
    if let Ok(val) = std::env::var(CRDB_SEED_TAR_ENV) {
        panic!(
            "CRDB_SEED_TAR_ENV should not be set here, but is set to {}",
            val
        );
    }

    let cmd_path = path_to_omicron_dev();

    let cmdstr = format!(
        "( set -o monitor; {} run-all --nexus-listen-port 0 && true )",
        cmd_path.display()
    );
    let exec =
        Exec::cmd("bash").arg("-c").arg(cmdstr).stderr(Redirection::Merge);
    let runall = run_run_all(exec);

    let test_task = async {
        // Make sure we can connect to CockroachDB.
        let (client, connection) = runall
            .postgres_config
            .connect(tokio_postgres::NoTls)
            .await
            .context("failed to connect to newly setup database")?;
        let conn_task = tokio::spawn(connection);
        anyhow::ensure!(has_omicron_schema(&client).await);
        drop(client);
        conn_task
            .await
            .context("failed to join on connection")?
            .context("connection failed with an error")?;
        eprintln!("cleaned up connection");

        // Make sure we can connect to Nexus.
        let client = oxide_client::Client::new(&format!(
            "http://{}",
            runall.external_url
        ));
        let _ =
            client.logout().send().await.context(
                "Unexpectedly failed to reach Nexus at logout endpoint",
            )?;
        Ok(())
    };
    let res = test_task.await;

    // Figure out what process group our child processes are in.  (That won't be
    // the child's pid because the immediate shell will be in our process group,
    // and it's the omicron-dev command that's the process group leader.)
    let pgid = unsafe { libc::getpgid(runall.db_pid as libc::pid_t) };
    assert_ne!(pgid, -1);

    // Send SIGINT to that process group.  This simulates an interactive session
    // where the user hits ^C.  Make sure everything is cleaned up gracefully.
    eprintln!("sending SIGINT to process group {}", pgid);
    assert_eq!(0, unsafe { libc::kill(-pgid, libc::SIGINT) });

    let wait = verify_graceful_exit(
        runall.subproc,
        runall.cmd_pid,
        runall.db_pid,
        &runall.temp_dir,
    );
    eprintln!("wait result: {:?}", wait);
    assert!(matches!(wait, subprocess::ExitStatus::Exited(0)));

    // Unwrap the caught errors we are actually trying to test.
    res.expect("failed to run test");
}

#[test]
fn test_omicron_dev_no_args() {
    let exec = Exec::cmd(path_to_omicron_dev());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents("tests/output/cmd-omicron-dev-noargs-stdout", &stdout_text);
    assert_contents("tests/output/cmd-omicron-dev-noargs-stderr", &stderr_text);
}

#[test]
fn test_omicron_dev_bad_cmd() {
    let exec = Exec::cmd(path_to_omicron_dev()).arg("bogus-command");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents(
        "tests/output/cmd-omicron-dev-bad-cmd-stdout",
        &stdout_text,
    );
    assert_contents(
        "tests/output/cmd-omicron-dev-bad-cmd-stderr",
        &stderr_text,
    );
}
