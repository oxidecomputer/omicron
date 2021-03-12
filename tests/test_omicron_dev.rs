/*!
 * Smoke tests for the omicron_dev command-line tool
 */

use omicron::dev::process_running;
use std::io::BufRead;
use std::path::PathBuf;
use std::time::Duration;
use subprocess::Exec;
use subprocess::Redirection;

/** name of the "omicron_dev" executable */
const CMD_OMICRON_DEV: &str = env!("CARGO_BIN_EXE_omicron_dev");

/** timeout used for various things that should be pretty quick */
const TIMEOUT: Duration = Duration::from_secs(10);

/**
 * Encapsulates the information we need from a running `omicron_dev db-run`
 * command.
 */
#[derive(Debug)]
struct DbRun {
    subproc: subprocess::Popen,
    cmd_pid: u32,
    db_pid: u32,
    listen_config: tokio_postgres::Config,
    temp_dir: PathBuf,
}

/**
 * Starts the "omicron_dev db-run" command and runs it for long enough to parse
 * the child pid, listen URL, and temporary directory.  Returns these, along
 * with a handle to the child process.
 * TODO-robustness It would be great to put a timeout on this.
 */
fn run_db_run(exec: Exec) -> DbRun {
    let cmdline = exec.to_cmdline_lossy();
    eprintln!("will run: {}", cmdline);

    let subproc = exec
        .stdout(Redirection::Pipe)
        .popen()
        .expect("failed to start command");
    let mut subproc_out =
        std::io::BufReader::new(subproc.stdout.as_ref().unwrap());
    let cmd_pid = subproc.pid().unwrap();
    let (mut db_pid, mut listen_config, mut temp_dir) = (None, None, None);

    eprintln!("waiting for stdout from child process");
    while db_pid.is_none() || listen_config.is_none() || temp_dir.is_none() {
        let mut buf = String::with_capacity(80);
        match subproc_out.read_line(&mut buf) {
            Ok(0) => {
                panic!("unexpected EOF from child process stdout");
            }
            Err(e) => {
                panic!("unexpected error reading child process stdout: {}", e);
            }
            _ => (),
        }

        if let Some(s) = buf.strip_prefix("omicron_dev: temporary directory: ")
        {
            eprint!("found temporary directory: {}", s);
            temp_dir = Some(PathBuf::from(s.trim_end().to_string()));
            continue;
        }

        if let Some(s) = buf.strip_prefix("omicron_dev: child process: pid ") {
            eprint!("found database pid: {}", s);
            db_pid = Some(
                s.trim_end().to_string().parse().expect("pid was not a u32"),
            );
            continue;
        }

        if let Some(s) =
            buf.strip_prefix("omicron_dev: CockroachDB listening at: ")
        {
            eprint!("found postgres listen URL: {}", s);
            listen_config = Some(
                s.trim_end()
                    .parse::<tokio_postgres::Config>()
                    .expect("invalid PostgreSQL URL"),
            );
            continue;
        }
    }

    assert!(process_running(cmd_pid));
    assert!(process_running(db_pid.unwrap()));

    DbRun {
        subproc,
        cmd_pid,
        db_pid: db_pid.unwrap(),
        listen_config: listen_config.unwrap(),
        temp_dir: temp_dir.unwrap(),
    }
}

/**
 * Waits for the subprocess to exit and returns status information
 *
 * This assumes the caller has arranged for the processes to terminate.  This
 * function verifies that both the omicron_dev and CockroachDB processes are
 * gone and that the temporary directory has been cleaned up.
 */
fn verify_graceful_exit(mut dbrun: DbRun) -> subprocess::ExitStatus {
    let wait_result = dbrun
        .subproc
        .wait_timeout(TIMEOUT)
        .expect("failed to wait for process to exit")
        .unwrap_or_else(|| {
            panic!("timed out waiting {:?} for process to exit", &TIMEOUT)
        });

    assert!(!process_running(dbrun.cmd_pid));
    assert!(!process_running(dbrun.db_pid));
    assert_eq!(
        libc::ENOENT,
        std::fs::metadata(&dbrun.temp_dir)
            .expect_err("temporary directory still exists")
            .raw_os_error()
            .unwrap()
    );

    wait_result
}

/*
 * Exercises the normal use case of `omicron_dev db-run`: the database starts
 * up, we can connect to it and query it, then we simulate the user typing ^C at
 * the shell, and then it cleans up its temporary directory.
 */
#[tokio::test]
async fn test_db_run() {
    /*
     * Rather than invoke the command directly, we'll use the shell to run the
     * command in a subshell with monitor mode active.  This puts the child
     * process into a separate process group, which allows us to send the whole
     * group SIGINT, which simulates what would happen if this were run
     * interactively from the shell (which is what we want to test).  Maybe
     * there's a better way to do this.  (Ideally, we would fork, use
     * setpgid(2) in the child, then exec our command.  The standard library
     * does not provide facilities to do this.  Maybe we could use the `libc`
     * crate to do this?)
     *
     * Note that it's not a good test to just send SIGINT to the CockroachDB
     * process.  In the real-world case we're trying to test, omicron_dev gets
     * SIGINT as well.  If it doesn't handle it explicitly, the process will be
     * terminated and temporary directories will be leaked.  However, the test
     * would pass because in the test case omicron_dev would never have gotten
     * the SIGINT.
     *
     * We also redirect stderr to stdout just so that it doesn't get dumped to
     * the user's terminal during regular `cargo test` runs.
     */
    let cmdstr = format!("( set -o monitor; {} db-run )", CMD_OMICRON_DEV);
    let exec =
        Exec::cmd("bash").arg("-c").arg(cmdstr).stderr(Redirection::Merge);
    let dbrun = run_db_run(exec);
    let (client, connection) = dbrun
        .listen_config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("failed to connect to newly setup database");
    let conn_task = tokio::spawn(async { connection.await });

    let row = client
        .query_one("SELECT 54321", &[])
        .await
        .expect("failed to query database");
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<'_, _, i64>(0), 54321);
    eprintln!("successfully ran query");

    drop(client);
    conn_task
        .await
        .expect("failed to join on connection")
        .expect("connection failed with an error");
    eprintln!("cleaned up connection");

    /*
     * Figure out what process group our child processes are in.  (That won't be
     * the child's pid because the immediate shell will be in our process group,
     * and its the omicron_dev command that's the process group leader.)
     */
    let pgid = unsafe { libc::getpgid(dbrun.db_pid as libc::pid_t) };
    assert_ne!(pgid, -1);

    /*
     * Send SIGINT to that process group.  This simulates an interactive session
     * where the user hits ^C.  Make sure everything is cleaned up gracefully.
     */
    eprintln!("sending SIGINT to process group {}", pgid);
    assert_eq!(0, unsafe { libc::kill(-pgid, libc::SIGINT) });

    let wait = verify_graceful_exit(dbrun);
    eprintln!("wait result: {:?}", wait);
    assert!(matches!(wait, subprocess::ExitStatus::Exited(0),));
}

/*
 * Exercises the unusual case of `omicron_dev db-run` where the database shuts
 * down unexpectedly.
 */
#[tokio::test]
async fn test_db_killed() {
    /*
     * Redirect stderr to stdout just so that it doesn't get dumped to the
     * user's terminal during regular `cargo test` runs.
     */
    let exec =
        Exec::cmd(CMD_OMICRON_DEV).arg("db-run").stderr(Redirection::Merge);
    let dbrun = run_db_run(exec);
    assert_eq!(0, unsafe {
        libc::kill(dbrun.db_pid as libc::pid_t, libc::SIGKILL)
    });
    let wait = verify_graceful_exit(dbrun);
    eprintln!("wait result: {:?}", wait);
    assert!(matches!(wait, subprocess::ExitStatus::Exited(1),));
}
