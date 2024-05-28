// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions used for automated testing of command-line programs

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

// Standard exit codes
pub const EXIT_SUCCESS: u32 = libc::EXIT_SUCCESS as u32;
pub const EXIT_FAILURE: u32 = libc::EXIT_FAILURE as u32;
pub const EXIT_USAGE: u32 = 2;

/// maximum time to wait for any command
///
/// This is important because a bug might actually cause this test to start one
/// of the servers and run it indefinitely.
const TIMEOUT: Duration = Duration::from_millis(60000);

pub fn path_to_executable(cmd_name: &str) -> PathBuf {
    let mut rv = PathBuf::from(cmd_name);
    // Drop the ".exe" extension on Windows.  Otherwise, this appears in stderr
    // output, which then differs across platforms.
    rv.set_extension("");
    rv
}

#[track_caller]
pub fn assert_exit_code(exit_status: ExitStatus, code: u32, stderr_text: &str) {
    if let ExitStatus::Exited(exit_code) = exit_status {
        assert_eq!(exit_code, code, "stderr:\n{}", stderr_text);
    } else {
        panic!(
            "expected normal process exit with code {}, got {:?}\n\nprocess stderr:{}",
            code, exit_status, stderr_text
        );
    }
}

/// Run the given command to completion or up to a hardcoded timeout, whichever
/// is shorter.  The caller provides a `subprocess::Exec` object that's already
/// had its program, arguments, environment, etc. configured, but hasn't been
/// started.  Stdin will be empty, and both stdout and stderr will be buffered to
/// disk and returned as strings.
pub fn run_command(exec: Exec) -> (ExitStatus, String, String) {
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
        .unwrap_or_else(|_| panic!("failed to start command: {}", cmdline));

    let exit_status = subproc
        .wait_timeout(timeout)
        .unwrap_or_else(|_| panic!("failed to wait for command: {}", cmdline))
        .unwrap_or_else(|| {
            panic!(
                "timed out waiting for command for {} ms: {}",
                timeout.as_millis(),
                cmdline
            )
        });

    let stdout_text =
        fs::read_to_string(&stdout_path).expect("failed to read stdout file");
    let stderr_text =
        fs::read_to_string(&stderr_path).expect("failed to read stdout file");
    fs::remove_file(&stdout_path).expect("failed to remove stdout file");
    fs::remove_file(&stderr_path).expect("failed to remove stderr file");

    (exit_status, stdout_text, stderr_text)
}

/// Create a new temporary file.
fn temp_file_create(label: &str) -> (PathBuf, fs::File) {
    let file_path = temp_file_path(label);
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file_path)
        .expect("failed to create temporary file");
    (file_path, file)
}

static FILE_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Create a new temporary file name.
pub fn temp_file_path(label: &str) -> PathBuf {
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

/// Returns the OS-specific error message for the case where a file was not
/// found.
pub fn error_for_enoent() -> String {
    io::Error::from_raw_os_error(libc::ENOENT).to_string()
}

/// Redacts text from a string (usually stdout/stderr) that may change from
/// invocation to invocation (e.g., assigned TCP port numbers, timestamps)
///
/// This allows use to use expectorate to verify the shape of the CLI output.
pub fn redact_variable(input: &str, extra_redactions: &[&str]) -> String {
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

    let mut s = regex::Regex::new(
        r"note: database schema version matches expected \(\d+\.\d+\.\d+\)",
    )
    .unwrap()
    .replace_all(
        &s,
        "note: database schema version matches expected \
        (<redacted database version>)",
    )
    .to_string();

    for r in extra_redactions {
        s = s.replace(r, "<REDACTED>");
    }

    s
}
