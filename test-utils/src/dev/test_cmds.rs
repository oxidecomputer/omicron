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
pub fn redact_variable(input: &str) -> String {
    // Replace TCP port numbers.  We include the localhost characters to avoid
    // catching any random sequence of numbers.
    let s = regex::Regex::new(r"\[::1\]:\d{4,5}")
        .unwrap()
        .replace_all(&input, "[::1]:REDACTED_PORT")
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
    //
    // The length of a UUID is 32 nibbles for the hex encoding of a u128 + 4
    // dashes = 36.
    const UUID_LEN: usize = 36;
    let s = regex::Regex::new(
        "[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-\
        [a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}",
    )
    .unwrap()
    .replace_all(&s, fill_redaction_text("uuid", UUID_LEN))
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

    let s = regex::Regex::new(r"iter \d+,")
        .unwrap()
        .replace_all(&s, "<REDACTED ITERATIONS>,")
        .to_string();

    s
}

/// Redact text from a string, allowing for extra redactions to be specified.
pub fn redact_extra(
    input: &str,
    extra_redactions: &ExtraRedactions<'_>,
) -> String {
    // Perform extra redactions at the beginning, not the end. This is because
    // some of the built-in redactions in redact_variable might match a
    // substring of something that should be handled by extra_redactions (e.g.
    // a temporary path).
    let mut s = input.to_owned();
    for (name, replacement) in &extra_redactions.redactions {
        s = s.replace(name, replacement);
    }
    redact_variable(&s)
}

/// Represents a list of extra redactions for [`redact_variable`].
///
/// Extra redactions are applied in-order, before any builtin redactions.
#[derive(Clone, Debug, Default)]
pub struct ExtraRedactions<'a> {
    // A pair of redaction and replacement strings.
    redactions: Vec<(&'a str, String)>,
}

impl<'a> ExtraRedactions<'a> {
    pub fn new() -> Self {
        Self { redactions: Vec::new() }
    }

    pub fn fixed_length(
        &mut self,
        name: &str,
        text_to_redact: &'a str,
    ) -> &mut Self {
        // Use the same number of chars as the number of bytes in
        // text_to_redact. We're almost entirely in ASCII-land so they're the
        // same, and getting the length right is nice but doesn't matter for
        // correctness.
        //
        // A technically more correct impl would use unicode-width, but ehhh.
        let replacement = fill_redaction_text(name, text_to_redact.len());
        self.redactions.push((text_to_redact, replacement));
        self
    }

    pub fn variable_length(
        &mut self,
        name: &str,
        text_to_redact: &'a str,
    ) -> &mut Self {
        let gen = format!("<{}_REDACTED>", name.to_uppercase());
        let replacement = gen.to_string();

        self.redactions.push((text_to_redact, replacement));
        self
    }
}

fn fill_redaction_text(name: &str, text_to_redact_len: usize) -> String {
    // The overall plan is to generate a string of the form
    // ---<REDACTED_NAME>---, depending on the length of the text to
    // redact.
    //
    // * Always include the < > signs for clarity, and either shorten the
    //   text or add dashes to compensate for the length.

    let base = format!("REDACTED_{}", name.to_uppercase());

    let text_len_minus_2 = text_to_redact_len.saturating_sub(2);

    let replacement = if text_len_minus_2 <= base.len() {
        // Shorten the base string to fit the text.
        format!("<{:.width$}>", base, width = text_len_minus_2)
    } else {
        // Add dashes on both sides to make up the difference.
        let dash_len = text_len_minus_2 - base.len();
        format!(
            "{}<{base}>{}",
            ".".repeat(dash_len / 2),
            ".".repeat(dash_len - dash_len / 2)
        )
    };
    replacement
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_extra() {
        let input = "time: 123ms, path: /var/tmp/tmp.456ms123s, \
            path2: /short, \
            path3: /variable-length/path";
        let actual = redact_extra(
            input,
            ExtraRedactions::new()
                .fixed_length("tp", "/var/tmp/tmp.456ms123s")
                .fixed_length("short_redact", "/short")
                .variable_length("variable", "/variable-length/path"),
        );
        assert_eq!(
            actual,
            "time: <REDACTED DURATION>ms, path: ....<REDACTED_TP>....., \
             path2: <REDA>, \
             path3: <VARIABLE_REDACTED>"
        );
    }
}
