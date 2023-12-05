// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke tests for the omicron-dev command-line tool

use anyhow::Context;
use expectorate::assert_contents;
use omicron_test_utils::dev::db::has_omicron_schema;
use omicron_test_utils::dev::process_running;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::EXIT_USAGE;
use omicron_test_utils::dev::CRDB_SEED_TAR_ENV;
use oxide_client::ClientHiddenExt;
use std::io::BufRead;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use subprocess::Exec;
use subprocess::ExitStatus;
use subprocess::Redirection;

/// name of the "omicron-dev" executable
const CMD_OMICRON_DEV: &str = env!("CARGO_BIN_EXE_omicron-dev");

/// timeout used for various things that should be pretty quick
const TIMEOUT: Duration = Duration::from_secs(30);

fn path_to_omicron_dev() -> PathBuf {
    path_to_executable(CMD_OMICRON_DEV)
}

/// Encapsulates the information we need from a running `omicron-dev db-run`
/// command.
#[derive(Debug)]
struct DbRun {
    subproc: subprocess::Popen,
    cmd_pid: u32,
    db_pid: u32,
    listen_config_url: String,
    listen_config: tokio_postgres::Config,
    temp_dir: PathBuf,
}

/// Starts the "omicron-dev db-run" command and runs it for long enough to parse
/// the child pid, listen URL, and temporary directory.  Returns these, along
/// with a handle to the child process.
/// TODO-robustness It would be great to put a timeout on this.
fn run_db_run(exec: Exec, wait_for_populate: bool) -> DbRun {
    let cmdline = exec.to_cmdline_lossy();
    eprintln!("will run: {}", cmdline);

    let subproc = exec
        .stdout(Redirection::Pipe)
        .popen()
        .expect("failed to start command");
    let mut subproc_out =
        std::io::BufReader::new(subproc.stdout.as_ref().unwrap());
    let cmd_pid = subproc.pid().unwrap();
    let (mut db_pid, mut listen_config_url, mut temp_dir) = (None, None, None);
    let mut populated = false;

    eprintln!("waiting for stdout from child process");
    while db_pid.is_none()
        || listen_config_url.is_none()
        || temp_dir.is_none()
        || (wait_for_populate && !populated)
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

        if let Some(s) = buf.strip_prefix("omicron-dev: temporary directory: ")
        {
            eprint!("found temporary directory: {}", s);
            temp_dir = Some(PathBuf::from(s.trim_end().to_string()));
            continue;
        }

        if let Some(s) = buf.strip_prefix("omicron-dev: child process: pid ") {
            eprint!("found database pid: {}", s);
            db_pid = Some(
                s.trim_end().to_string().parse().expect("pid was not a u32"),
            );
            continue;
        }

        if let Some(s) =
            buf.strip_prefix("omicron-dev: CockroachDB listening at: ")
        {
            eprint!("found postgres listen URL: {}", s);
            listen_config_url = Some(s.trim_end().to_string());
            continue;
        }

        if buf.contains("omicron-dev: populated database") {
            eprintln!("found database populated");
            populated = true;
            continue;
        }
    }

    assert!(process_running(cmd_pid));
    assert!(process_running(db_pid.unwrap()));

    let listen_config = listen_config_url
        .as_ref()
        .unwrap()
        .parse::<tokio_postgres::Config>()
        .expect("invalid PostgreSQL URL");

    DbRun {
        subproc,
        cmd_pid,
        db_pid: db_pid.unwrap(),
        listen_config_url: listen_config_url.unwrap(),
        listen_config,
        temp_dir: temp_dir.unwrap(),
    }
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

/// Waits for the subprocess to exit and returns status information
///
/// This assumes the caller has arranged for the processes to terminate.  This
/// function verifies that both the omicron-dev and CockroachDB processes are
/// gone and that the temporary directory has been cleaned up.
fn verify_graceful_exit(
    mut subproc: subprocess::Popen,
    cmd_pid: u32,
    db_pid: u32,
    temp_dir: &Path,
) -> subprocess::ExitStatus {
    let wait_result = subproc
        .wait_timeout(TIMEOUT)
        .expect("failed to wait for process to exit")
        .unwrap_or_else(|| {
            panic!("timed out waiting {:?} for process to exit", &TIMEOUT)
        });

    assert!(!process_running(cmd_pid));
    assert!(!process_running(db_pid));
    assert_eq!(
        libc::ENOENT,
        std::fs::metadata(temp_dir)
            .expect_err("temporary directory still exists")
            .raw_os_error()
            .unwrap()
    );

    wait_result
}

// Exercises the normal use case of `omicron-dev db-run`: the database starts
// up, we can connect to it and query it, then we simulate the user typing ^C at
// the shell, and then it cleans up its temporary directory.
#[tokio::test]
async fn test_db_run() {
    let cmd_path = path_to_omicron_dev();

    // Rather than invoke the command directly, we'll use the shell to run the
    // command in a subshell with monitor mode active.  This puts the child
    // process into a separate process group, which allows us to send the whole
    // group SIGINT, which simulates what would happen if this were run
    // interactively from the shell (which is what we want to test).  Maybe
    // there's a better way to do this.  (Ideally, we would fork, use
    // setpgid(2) in the child, then exec our command.  The standard library
    // does not provide facilities to do this.  Maybe we could use the `libc`
    // crate to do this?)
    //
    // Note that it's not a good test to just send SIGINT to the CockroachDB
    // process.  In the real-world case we're trying to test, omicron-dev gets
    // SIGINT as well.  If it doesn't handle it explicitly, the process will be
    // terminated and temporary directories will be leaked.  However, the test
    // would pass because in the test case omicron-dev would never have gotten
    // the SIGINT.
    //
    // We also redirect stderr to stdout. Originally this was so that the output
    // doesn't get dumped to the user's terminal during regular `cargo test`
    // runs, though with nextest this is less of an issue.
    //
    // Finally, we set listen-port=0 to avoid conflicting with concurrent
    // invocations.
    //
    // The `&& true` looks redundant but it prevents recent versions of bash
    // from optimising away the fork() and causing cargo itself to receive
    // the ^C that we send during testing.
    let cmdstr = format!(
        "( set -o monitor; {} db-run --listen-port 0 && true )",
        cmd_path.display()
    );
    let exec =
        Exec::cmd("bash").arg("-c").arg(cmdstr).stderr(Redirection::Merge);
    let dbrun = run_db_run(exec, true);
    let test_task = async {
        let (client, connection) = dbrun
            .listen_config
            .connect(tokio_postgres::NoTls)
            .await
            .context("failed to connect to newly setup database")?;
        let conn_task = tokio::spawn(connection);

        anyhow::ensure!(has_omicron_schema(&client).await);

        // Now run db-populate.
        eprintln!("running db-populate");
        let populate_result = Exec::cmd(&cmd_path)
            .arg("db-populate")
            .arg("--database-url")
            .arg(&dbrun.listen_config_url)
            .stdout(Redirection::Pipe)
            .stderr(Redirection::Pipe)
            .capture()
            .context("failed to run db-populate")?;
        eprintln!("exit status: {:?}", populate_result.exit_status);
        eprintln!("stdout: {:?}", populate_result.stdout_str());
        eprintln!("stdout: {:?}", populate_result.stderr_str());
        anyhow::ensure!(has_omicron_schema(&client).await);

        // Try again, but with the --wipe flag.
        eprintln!("running db-populate --wipe");
        let populate_result = Exec::cmd(&cmd_path)
            .arg("db-populate")
            .arg("--wipe")
            .arg("--database-url")
            .arg(&dbrun.listen_config_url)
            .capture()
            .context("failed to run db-populate")?;
        anyhow::ensure!(matches!(
            populate_result.exit_status,
            ExitStatus::Exited(0)
        ));
        anyhow::ensure!(has_omicron_schema(&client).await);

        // Now run db-wipe.  This should work.
        eprintln!("running db-wipe");
        let wipe_result = Exec::cmd(&cmd_path)
            .arg("db-wipe")
            .arg("--database-url")
            .arg(&dbrun.listen_config_url)
            .capture()
            .context("failed to run db-wipe")?;
        anyhow::ensure!(matches!(
            wipe_result.exit_status,
            ExitStatus::Exited(0)
        ));
        anyhow::ensure!(!has_omicron_schema(&client).await);

        // The rest of the populate()/wipe() behavior is tested elsewhere.

        drop(client);
        conn_task
            .await
            .context("failed to join on connection")?
            .context("connection failed with an error")?;
        eprintln!("cleaned up connection");
        Ok(())
    };
    let res = test_task.await;

    // Figure out what process group our child processes are in.  (That won't be
    // the child's pid because the immediate shell will be in our process group,
    // and it's the omicron-dev command that's the process group leader.)
    let pgid = unsafe { libc::getpgid(dbrun.db_pid as libc::pid_t) };
    assert_ne!(pgid, -1);

    // Send SIGINT to that process group.  This simulates an interactive session
    // where the user hits ^C.  Make sure everything is cleaned up gracefully.
    eprintln!("sending SIGINT to process group {}", pgid);
    assert_eq!(0, unsafe { libc::kill(-pgid, libc::SIGINT) });

    let wait = verify_graceful_exit(
        dbrun.subproc,
        dbrun.cmd_pid,
        dbrun.db_pid,
        &dbrun.temp_dir,
    );
    eprintln!("wait result: {:?}", wait);
    assert!(matches!(wait, subprocess::ExitStatus::Exited(0)));
    res.expect("test task failed");
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

// Exercises the unusual case of `omicron-dev db-run` where the database shuts
// down unexpectedly.
#[tokio::test]
async fn test_db_killed() {
    // Redirect stderr to stdout just so that it doesn't get dumped to the
    // user's terminal during regular `cargo test` runs.
    let exec = Exec::cmd(&path_to_omicron_dev())
        .arg("db-run")
        .arg("--listen-port=0")
        .stderr(Redirection::Merge);
    // Although it doesn't seem necessary, we wait for "db-run" to finish
    // populating the database before we kill CockroachDB.  The main reason is
    // that we're trying to verify that if CockroachDB exits under normal
    // conditions, then db-run notices.  If we don't wait for populate() to
    // finish, then we might fail during populate(), and that's a different
    // failure path.  In particular, that path does _not_ necessarily wait for
    // CockroachDB to exit.  It arguably should, but this is considerably more
    // of an edge case than we're testing here.
    let dbrun = run_db_run(exec, true);
    assert_eq!(0, unsafe {
        libc::kill(dbrun.db_pid as libc::pid_t, libc::SIGKILL)
    });
    let wait = verify_graceful_exit(
        dbrun.subproc,
        dbrun.cmd_pid,
        dbrun.db_pid,
        &dbrun.temp_dir,
    );
    eprintln!("wait result: {:?}", wait);
    assert!(matches!(wait, subprocess::ExitStatus::Exited(1),));
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

#[test]
fn test_omicron_dev_db_populate_no_args() {
    let exec = Exec::cmd(path_to_omicron_dev()).arg("db-populate");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents(
        "tests/output/cmd-omicron-dev-db-populate-noargs-stdout",
        &stdout_text,
    );
    assert_contents(
        "tests/output/cmd-omicron-dev-db-populate-noargs-stderr",
        &stderr_text,
    );
}

#[test]
fn test_omicron_dev_db_wipe_no_args() {
    let exec = Exec::cmd(path_to_omicron_dev()).arg("db-wipe");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE, &stderr_text);
    assert_contents(
        "tests/output/cmd-omicron-dev-db-wipe-noargs-stdout",
        &stdout_text,
    );
    assert_contents(
        "tests/output/cmd-omicron-dev-db-wipe-noargs-stderr",
        &stderr_text,
    );
}

#[test]
fn test_cert_create() {
    let tmpdir = camino_tempfile::tempdir().unwrap();
    println!("tmpdir: {}", tmpdir.path());
    let output_base = format!("{}/test-", tmpdir.path());
    let exec = Exec::cmd(path_to_omicron_dev())
        .arg("cert-create")
        .arg(output_base)
        .arg("foo.example")
        .arg("bar.example");
    let (exit_status, _, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);
    let cert_path = tmpdir.path().join("test-cert.pem");
    let key_path = tmpdir.path().join("test-key.pem");
    let cert_contents = std::fs::read(&cert_path)
        .with_context(|| format!("reading certificate path {:?}", cert_path))
        .unwrap();
    let key_contents = std::fs::read(&key_path)
        .with_context(|| format!("reading private key path: {:?}", key_path))
        .unwrap();
    let certs_pem = openssl::x509::X509::stack_from_pem(&cert_contents)
        .context("parsing certificate")
        .unwrap();
    let private_key = openssl::pkey::PKey::private_key_from_pem(&key_contents)
        .context("parsing private key")
        .unwrap();
    assert!(certs_pem
        .iter()
        .last()
        .unwrap()
        .public_key()
        .unwrap()
        .public_eq(&private_key));
}
