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
#[derive(Clone, Debug)]
pub struct Redactor<'a> {
    basic: bool,
    uuids: bool,
    extra: Vec<(&'a str, String)>,
    extra_regex: Vec<(regex::Regex, String)>,
    sections: Vec<(&'a [&'a str], SectionMode)>,
}

/// How to handle a redacted section heading.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SectionMode {
    /// Redact the indented contents of the section, replacing them with
    /// `<REDACTED_SECTION>` at the current indentation level.
    ///
    /// This is intended for sections which are always present but whose
    /// contents may be variable.
    RedactContents,
    /// Totally remove the redacted section, including its heading, from the
    /// output.
    ///
    /// This is intended for sections which may or may not be *present* across
    /// invocations. Generally, this is a very blunt instrument that should
    /// normally not be necessary.
    TotallyAnnihilate,
}

impl Default for Redactor<'_> {
    fn default() -> Self {
        Self {
            basic: true,
            uuids: true,
            extra: Vec::new(),
            extra_regex: Vec::new(),
            sections: Vec::new(),
        }
    }
}

impl<'a> Redactor<'a> {
    /// Create a new redactor that does not do any redactions.
    pub fn noop() -> Self {
        Self {
            basic: false,
            uuids: false,
            extra: Vec::new(),
            extra_regex: Vec::new(),
            sections: Vec::new(),
        }
    }

    pub fn basic(&mut self, basic: bool) -> &mut Self {
        self.basic = basic;
        self
    }

    pub fn uuids(&mut self, uuids: bool) -> &mut Self {
        self.uuids = uuids;
        self
    }

    pub fn extra_fixed_length(
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
        self.extra.push((text_to_redact, replacement));
        self
    }

    pub fn extra_variable_length(
        &mut self,
        name: &str,
        text_to_redact: &'a str,
    ) -> &mut Self {
        let replacement = format!("<{}_REDACTED>", name.to_uppercase());
        self.extra.push((text_to_redact, replacement));
        self
    }

    /// Redact the value of a named field with a known value shape.
    ///
    /// This can be used for redacting a common value (such as a small integer)
    /// that changes from run to run but otherwise doesn't have any context
    /// that helps it be redacted using other methods. The value will only
    /// be redacted if it matches `name` concatenated with one or more spaces
    /// concatenated with a string matching `value_regex`. An example:
    ///
    /// ```
    /// # use omicron_test_utils::dev::test_cmds::Redactor;
    /// # let mut redactor = Redactor::default();
    /// redactor.field("list ok:", r"\d+");
    /// ```
    ///
    /// will replace `list ok:   1` with `list ok:   <LIST_OK_REDACTED>`.
    pub fn field(&mut self, name: &str, value_regex: &str) -> &mut Self {
        let re = regex::Regex::new(&format!(
            r"\b(?<prefix>{} +){}\b",
            regex::escape(name),
            value_regex
        ))
        .unwrap();
        let replacement = format!(
            "$prefix<{}_REDACTED>",
            name.replace(|c: char| c.is_ascii_punctuation(), "")
                .replace(" ", "_")
                .to_uppercase()
        );
        self.extra_regex.push((re, replacement));
        self
    }

    /// Redact an entire indented section.
    ///
    /// This can be used if the shape of a section might change from run to run.
    ///
    /// `headings` is the path of heading names indicating the section to
    /// redact. For example, to redact only the first "ringbuf:" section from
    /// this output:
    ///
    /// ```ignore
    /// section A:
    ///   nested:
    ///     ringbuf:
    ///       this should be redacted
    /// section B:
    ///   ringbuf:
    ///     this should not be redacted
    /// ```
    ///
    /// we can use:
    ///
    /// ```
    /// # use omicron_test_utils::dev::test_cmds::Redactor;
    /// # let mut redactor = Redactor::default();
    /// redactor.section(&["section A:", "ringbuf:"]);
    /// ```
    ///
    /// Note that not all section headings need to be listed in `headings` in
    /// order for the section to be redacted.
    pub fn section(&mut self, headings: &'a [&'a str]) -> &mut Self {
        assert!(!headings.is_empty(), "headings should not be empty");
        self.sections.push((headings, SectionMode::RedactContents));
        self
    }

    /// Completely and utterly destroy an entire indented section.
    ///
    /// While [`Redactor::section`] will redact the _contents_ of an indented
    /// section, this method will completely remove the section, including its
    /// heading, from the output. This is a very blunt instrument, and is
    /// intended for situations in which a section's *presence* may vary across
    /// invocations.
    ///
    /// If only the section's *content* may vary, prefer to use
    /// [`Redactor::section`].
    pub fn totally_annihilate_section(
        &mut self,
        headings: &'a [&'a str],
    ) -> &mut Self {
        assert!(!headings.is_empty(), "headings should not be empty");
        self.sections.push((headings, SectionMode::TotallyAnnihilate));
        self
    }

    pub fn do_redact(&self, input: &str) -> String {
        // Perform extra redactions at the beginning, not the end. This is because
        // some of the built-in redactions in redact_variable might match a
        // substring of something that should be handled by extra_redactions (e.g.
        // a temporary path).
        let mut s = input.to_owned();
        for (name, replacement) in &self.extra {
            s = s.replace(name, replacement);
        }
        for (regex, replacement) in &self.extra_regex {
            s = regex.replace_all(&s, replacement).into_owned();
        }
        for &(ref headings, mode) in &self.sections {
            s = redact_section(&s, headings, mode);
        }

        if self.basic {
            s = redact_basic(&s);
        }
        if self.uuids {
            s = redact_uuids(&s);
        }

        s
    }
}

fn redact_basic(input: &str) -> String {
    // Replace TCP port numbers. We include the localhost
    // characters to avoid catching any random sequence of numbers.
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

    // Replace timestamps.
    //
    // Format: RFC 3339 (ISO 8601)
    // Examples:
    //  1970-01-01T00:00:00Z
    //  1970-01-01T00:00:00.00001Z
    //
    // Note that depending on the amount of trailing zeros,
    // this value can have different widths. However, "<REDACTED_TIMESTAMP>"
    // has a deterministic width, so that's used instead.
    let s = regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z")
        .unwrap()
        .replace_all(&s, "<REDACTED_TIMESTAMP>")
        .to_string();

    // Replace formatted durations.  These are pretty specific to the background
    // task output.
    let s = regex::Regex::new(r"\d+s ago")
        .unwrap()
        .replace_all(&s, "<REDACTED DURATION>s ago")
        .to_string();

    // Replace interval (s).
    let s = regex::Regex::new(r"\d+s")
        .unwrap()
        .replace_all(&s, "<REDACTED_DURATION>s")
        .to_string();

    // Replace interval (ms).
    let s = regex::Regex::new(r"\d+ms")
        .unwrap()
        .replace_all(&s, "<REDACTED DURATION>ms")
        .to_string();

    // Replace interval (m).
    let s = regex::Regex::new(r"\d+m")
        .unwrap()
        .replace_all(&s, "<REDACTED_DURATION>m")
        .to_string();

    // Replace interval (h).
    let s = regex::Regex::new(r"\d+h")
        .unwrap()
        .replace_all(&s, "<REDACTED_DURATION>h")
        .to_string();

    // Replace interval (days).
    let s = regex::Regex::new(r"\d+days")
        .unwrap()
        .replace_all(&s, "<REDACTED_DURATION>days")
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

fn redact_uuids(input: &str) -> String {
    // The length of a UUID is 32 nibbles for the hex encoding of a u128 + 4
    // dashes = 36.
    const UUID_LEN: usize = 36;
    regex::Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
        .unwrap()
        .replace_all(&input, fill_redaction_text("uuid", UUID_LEN))
        .to_string()
}

fn redact_section(input: &str, headings: &[&str], mode: SectionMode) -> String {
    let mut output = String::new();
    let mut indent_stack = Vec::new();
    let mut print_redacted = false;
    for line in input.split_inclusive('\n') {
        let indent = line.len() - line.trim_start().len();
        if !line.trim().is_empty() {
            while let Some(last) = indent_stack.pop() {
                if indent > last {
                    indent_stack.push(last);
                    break;
                }
            }
        }
        if indent_stack.len() == headings.len() {
            if print_redacted && mode == SectionMode::RedactContents {
                print_redacted = false;
                output.push_str(&line[..indent]);
                output.push_str("<REDACTED_SECTION>\n");
            }
            continue;
        }
        if line[indent..].trim_end() == headings[indent_stack.len()] {
            indent_stack.push(indent);
            if indent_stack.len() == headings.len() {
                // If we just found the section's heading line, and we have been
                // instructed to totally annihilate the section, don't push the
                // heading line to the output.
                if mode == SectionMode::TotallyAnnihilate {
                    continue;
                }
                print_redacted = true;
            }
        }

        output.push_str(line);
    }
    output
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
    use chrono::{DateTime, Utc};

    #[test]
    fn test_redact_extra() {
        let input = "time: 123ms, path: /var/tmp/tmp.456ms123s, \
            path2: /short, \
            path3: /variable-length/path";
        let actual = Redactor::default()
            .extra_fixed_length("tp", "/var/tmp/tmp.456ms123s")
            .extra_fixed_length("short_redact", "/short")
            .extra_variable_length("variable", "/variable-length/path")
            .do_redact(input);
        assert_eq!(
            actual,
            "time: <REDACTED DURATION>ms, path: ....<REDACTED_TP>....., \
             path2: <REDA>, \
             path3: <VARIABLE_REDACTED>"
        );
    }

    #[test]
    fn test_redact_timestamps() {
        let times = [
            DateTime::<Utc>::from_timestamp_nanos(0),
            DateTime::<Utc>::from_timestamp_nanos(1),
            DateTime::<Utc>::from_timestamp_nanos(10),
            DateTime::<Utc>::from_timestamp_nanos(100000),
            DateTime::<Utc>::from_timestamp_nanos(123456789),
            // This doesn't impact the test at all, but as a fun fact, this
            // happened on March 18th, 2005.
            DateTime::<Utc>::from_timestamp_nanos(1111111111100000000),
            DateTime::<Utc>::from_timestamp_nanos(1111111111111100000),
            DateTime::<Utc>::from_timestamp_nanos(1111111111111111110),
            DateTime::<Utc>::from_timestamp_nanos(1111111111111111111),
            // ... and this one happens on June 6th, 2040.
            DateTime::<Utc>::from_timestamp_nanos(2222222222000000000),
            DateTime::<Utc>::from_timestamp_nanos(2222222222222200000),
            DateTime::<Utc>::from_timestamp_nanos(2222222222222222220),
            DateTime::<Utc>::from_timestamp_nanos(2222222222222222222),
        ];
        for time in times {
            let input = format!("{:?}", time);
            assert_eq!(
                Redactor::default().do_redact(&input),
                "<REDACTED_TIMESTAMP>",
                "Failed to redact {:?}",
                time
            );
        }
    }

    #[test]
    fn test_redact_section() {
        const INPUT: &str = "\
section A:
  nested:
    ringbuf:
      this should be redacted
      a second line to be redacted

      a line followed by an empty line
section B:
  ringbuf:
    this should not be redacted

    a line followed by an empty line";
        const OUTPUT: &str = "\
section A:
  nested:
    ringbuf:
      <REDACTED_SECTION>
section B:
  ringbuf:
    this should not be redacted

    a line followed by an empty line";

        let mut redactor = Redactor::default();
        redactor.section(&["section A:", "ringbuf:"]);
        assert_eq!(redactor.do_redact(INPUT), OUTPUT);
    }

    #[test]
    fn test_totally_annihilate_section() {
        const INPUT: &str = "\
section A:
  nested:
    ringbuf:
      this should be redacted
      a second line to be redacted

      a line followed by an empty line
    this line should not be redacted
section B:
  ringbuf:
    this should not be redacted

    a line followed by an empty line";
        const OUTPUT: &str = "\
section A:
  nested:
    this line should not be redacted
section B:
  ringbuf:
    this should not be redacted

    a line followed by an empty line";

        let mut redactor = Redactor::default();
        redactor.totally_annihilate_section(&["section A:", "ringbuf:"]);
        assert_eq!(redactor.do_redact(INPUT), OUTPUT);
    }
}
