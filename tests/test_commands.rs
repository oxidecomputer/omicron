/*!
 * Tests for the executable commands in this repo.  Most functionality is tested
 * elsewhere, so this really just sanity checks argument parsing, bad args, and
 * the --openapi mode.
 */

/*
 * TODO-coverage:
 * - test success cases of oxide_controller and sled_agent
 */

use openapiv3::OpenAPI;
use std::env::current_exe;
use std::env::temp_dir;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use subprocess::Exec;
use subprocess::ExitStatus;
use subprocess::NullFile;
use subprocess::Redirection;

/** name of the "oxide_controller" executable */
const CMD_CONTROLLER: &str = "oxide_controller";
/** name of the "sled_agent" executable */
const CMD_SLED_AGENT: &str = "sled_agent";
/**
 * maximum time to wait for any command
 *
 * This is important because a bug might actually cause this test to start one
 * of the servers and run it indefinitely.
 */
const TIMEOUT: Duration = Duration::from_millis(10000);

fn path_to_controller() -> PathBuf {
    path_to_executable(CMD_CONTROLLER)
}

fn path_to_sled_agent() -> PathBuf {
    path_to_executable(CMD_SLED_AGENT)
}

fn path_to_executable(cmd_name: &str) -> PathBuf {
    let mut rv = current_exe().expect("failed to find path to test program");
    rv.pop();
    assert_eq!(rv.file_name().unwrap(), "deps");
    rv.set_file_name(cmd_name);
    rv
}

/**
 * Run the given command to completion or up to a hardcoded timeout, whichever
 * is shorter.  The caller provides a `subprocess::Exec` object that's already
 * had its program, arguments, environment, etc. configured, but hasn't been
 * started.  Stdout and stderr will be buffered in a pipe and returned as
 * strings.  (It's possible that deadlock could result if the command tries to
 * write more data than the kernel's pipe buffer, since we will wait first for
 * the command to terminate and after that read the data from the pipe.)
 */
fn run_command(exec: Exec) -> (ExitStatus, String, String) {
    let cmdline = exec.to_cmdline_lossy();
    let timeout = TIMEOUT;

    let (stdout_path, stdout_file) = temp_file_create("stdout");
    let (stderr_path, stderr_file) = temp_file_create("stderr");

    let mut subproc = exec
        .stdin(NullFile)
        .stdout(Redirection::File(stdout_file))
        .stderr(Redirection::File(stderr_file))
        .detached()
        .popen()
        .expect(&format!("failed to start command: {}", cmdline));

    let exit_status = subproc
        .wait_timeout(TIMEOUT)
        .expect(&format!("failed to wait for command: {}", cmdline))
        .expect(&format!(
            "timed out waiting for command for {} ms: {}",
            timeout.as_millis(),
            cmdline
        ));

    let stdout_text =
        fs::read_to_string(&stdout_path).expect("failed to read stdout file");
    let stderr_text =
        fs::read_to_string(&stderr_path).expect("failed to read stdout file");
    fs::remove_file(&stdout_path).expect("failed to remove stdout file");
    fs::remove_file(&stderr_path).expect("failed to remove stderr file");

    (exit_status, stdout_text, stderr_text)
}

/**
 * Create a new temporary file.
 */
fn temp_file_create(label: &str) -> (PathBuf, fs::File) {
    let file_path = temp_file_path(label);
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file_path)
        .expect("failed to create temporary file");
    (file_path, file)
}

/**
 * Write the requested string to a temporary file and return the path to that
 * file.
 */
fn write_config(config: &str) -> PathBuf {
    let file_path = temp_file_path("test_commands_config");
    eprintln!("writing temp config: {}", file_path.display());
    fs::write(&file_path, config).expect("failed to write config file");
    file_path
}

static FILE_COUNTER: AtomicU32 = AtomicU32::new(0);

/**
 * Create a new temporary file name.
 */
fn temp_file_path(label: &str) -> PathBuf {
    let mut file_path = temp_dir();
    let file_name = format!(
        "{}.{}.{}",
        label,
        process::id(),
        FILE_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    file_path.push(file_name);
    file_path
}

fn assert_exit_code(exit_status: ExitStatus, code: u32) {
    if let ExitStatus::Exited(exit_code) = exit_status {
        assert_eq!(exit_code, code as u32);
    } else {
        panic!(
            "expected normal process exit with code {}, got {:?}",
            code, exit_status
        );
    }
}

/**
 * Returns the OS-specific error message for the case where a file was not
 * found.
 */
fn error_for_enoent() -> String {
    io::Error::from_raw_os_error(libc::ENOENT).to_string()
}

/*
 * Tests
 */

const EXIT_SUCCESS: u32 = libc::EXIT_SUCCESS as u32;
const EXIT_FAILURE: u32 = libc::EXIT_FAILURE as u32;
const EXIT_USAGE: u32 = 2;

#[test]
fn test_controller_no_args() {
    let exec = Exec::cmd(path_to_controller());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE);
    assert_eq!(&stdout_text, include_str!("test_controller_no_args-stdout"));
    assert_eq!(&stderr_text, include_str!("test_controller_no_args-stderr"));
}

#[test]
fn test_sled_agent_no_args() {
    let exec = Exec::cmd(path_to_sled_agent());
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_USAGE);
    assert_eq!(&stdout_text, include_str!("test_sled_agent_no_args-stdout"));
    assert_eq!(&stderr_text, include_str!("test_sled_agent_no_args-stderr"));
}

#[test]
fn test_controller_bad_config() {
    let exec = Exec::cmd(path_to_controller()).arg("nonexistent");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_FAILURE);
    assert_eq!(&stdout_text, include_str!("test_controller_bad_config-stdout"));
    assert_eq!(
        stderr_text,
        format!(
            "oxide_controller: read \"nonexistent\": {}\n",
            error_for_enoent()
        )
    );
}

#[test]
fn test_controller_openapi() {
    /*
     * This is a little goofy: we need a config file for the program.
     * (Arguably, --openapi shouldn't require a config file, but it's
     * conceivable that the exposed endpoints would depend on the
     * configuration.)  We ship a config file in "examples", and we may as well
     * use it here -- it would be a bug if that one didn't work for this
     * purpose.  However, it's not clear how to reliably locate it at runtime.
     * But we do know where it is at compile time, so we load it then.
     */
    let config = include_str!("../examples/config.toml");
    let config_path = write_config(config);
    let exec =
        Exec::cmd(path_to_controller()).arg(&config_path).arg("--openapi");
    let (exit_status, stdout_text, stderr_text) = run_command(exec);
    fs::remove_file(&config_path).expect("failed to remove temporary file");
    assert_exit_code(exit_status, EXIT_SUCCESS);
    assert_eq!(&stderr_text, include_str!("test_controller_openapi-stderr"));

    /*
     * Make sure the result parses as a valid OpenAPI spec and sanity-check a
     * few fields.
     */
    let spec: OpenAPI = serde_json::from_str(&stdout_text)
        .expect("stdout was not valid OpenAPI");
    assert_eq!(spec.openapi, "3.0.3");
    assert_eq!(spec.info.title, "Oxide Region API");
    assert_eq!(spec.info.version, "0.0.1");
    assert!(spec.paths.len() > 0);
    assert!(spec.paths.get("/projects").is_some());
}
