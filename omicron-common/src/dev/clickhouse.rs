//! Tools for managing ClickHouse during development

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, ensure, Context};
use tempfile::TempDir;

use crate::dev::poll;

/// A `ClickHouseInstance` is used to start and manage a ClickHouse server process.
#[derive(Debug)]
pub struct ClickHouseInstance {
    // Directory in which all data, logs, etc are stored.
    data_dir: Option<TempDir>,
    data_path: PathBuf,
    // The HTTP port the server is listening on
    port: u16,
    // Full list of command-line arguments
    args: Vec<String>,
    // Subprocess handle
    child: Option<tokio::process::Child>,
}

impl ClickHouseInstance {
    /// Start a new ClickHouse server
    pub async fn new(port: u16) -> Result<Self, anyhow::Error> {
        let data_dir = TempDir::new()?;
        let log_path = data_dir.path().join("clickhouse-server.log");
        let err_log_path = data_dir.path().join("clickhouse-server.errlog");
        let args = vec![
            "server".to_string(),
            "--log-file".to_string(),
            log_path.display().to_string(),
            "--errorlog-file".to_string(),
            err_log_path.display().to_string(),
            "--".to_string(),
            "--http_port".to_string(),
            format!("{}", port),
            "--path".to_string(),
            data_dir.path().display().to_string(),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            // ClickHouse internall tees its logs to a file, so we throw away std{in,out,err}
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            // By default ClickHouse forks a child if it's been explicitly requested via the
            // following environment variable, _or_ if it's not attached to a TTY. Avoid this
            // behavior, so that we can correctly deliver SIGINT. The "watchdog" masks SIGINT,
            // meaning we'd have to deliver that to the _child_, which is more complicated.
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .spawn()?;

        // Wait for the "status" file to become available, at least with a newline.
        let data_path = data_dir.path().to_path_buf();
        let status_file = data_path.join("status");
        poll::wait_for_condition(
            || async {
                let contents =
                    match tokio::fs::read_to_string(&status_file).await {
                        Ok(contents) => contents,
                        Err(e) => match e.kind() {
                            std::io::ErrorKind::NotFound => {
                                return Err(poll::CondCheckError::NotYet)
                            }
                            _ => return Err(poll::CondCheckError::from(e)),
                        },
                    };
                if !contents.is_empty() && contents.contains('\n') {
                    Ok(())
                } else {
                    Err(poll::CondCheckError::NotYet)
                }
            },
            &Duration::from_millis(500),
            &Duration::from_secs(10),
        )
        .await?;

        // Discover the HTTP port on which we're listening, if it's not provided explicitly by the
        // caller.
        //
        // Note: This is annoying. For tests, or any situation in which we'd run multiple servers,
        // we'd like to let the OS choose a port for us, by listening on port 0. ClickHouse
        // supports this, but doesn't do anything to discover the port on which it's actually
        // listening (i.e., the log file just says "listening on port 0"). In contrast, CockroachDB
        // dumps the full URL on which it's listening to a file for users to discover.
        //
        // This is a workaround which shells out to `lsof` or `pfiles` to discover the port.
        let port = if port != 0 {
            port
        } else {
            discover_local_listening_port(child.id().unwrap()).await?
        };

        Ok(Self {
            data_dir: Some(data_dir),
            data_path,
            port,
            args,
            child: Some(child),
        })
    }

    /// Wait for the ClickHouse server process to shutdown, after it's been killed.
    pub async fn wait_for_shutdown(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.wait().await?;
        }
        self.cleanup().await
    }

    /// Kill the ClickHouse server process and cleanup the data directory.
    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.start_kill().context("Sending SIGKILL to child")?;
            child.wait().await.context("waiting for child")?;
        }
        if let Some(dir) = self.data_dir.take() {
            dir.close().context("Cleaning up temporary directory")?;

            // ClickHouse doesn't fully respect the `--path` flag, and still seems
            // to put the `preprocessed_configs` directory in $CWD.
            let _ = std::fs::remove_dir_all("./preprocessed_configs");
        }
        Ok(())
    }

    /// Return the full path to the directory used for the server's data.
    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    /// Return the command-line used to start the ClickHouse server process
    pub fn cmdline(&self) -> &Vec<String> {
        &self.args
    }

    /// Return the child PID, if any
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.id())
    }

    /// Return the HTTP port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for ClickHouseInstance {
    fn drop(&mut self) {
        if self.child.is_some() || self.data_dir.is_some() {
            eprintln!(
                "WARN: dropped ClickHouseInstance without cleaning it up first \
                (there may still be a child process running and a \
                temporary directory leaked)"
            );
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
            if let Some(dir) = self.data_dir.take() {
                let _ = dir.close();
            }
        }
    }
}

// Parse the output of the command used to find the HTTP port ClickHouse listens on, in the event
// we start it with a port of 0.
#[cfg(not(target_os = "illumos"))]
async fn discover_local_listening_port(pid: u32) -> Result<u16, anyhow::Error> {
    // `lsof` doesn't do the right thing with the PID. It seems to list _all_ files for the
    // process, on both macOS and Linux, rather than the intersection of the PID we care about and
    // the other filters. So we ignore it.
    let output = tokio::process::Command::new("lsof")
        .arg("-i")
        .arg("6TCP@localhost") // TCP over IPv6, on the local address
        .arg("-F")
        .arg("n") // Only print the file name, \n-terminated
        .output() // Spawn and collect the output
        .await
        .context("Could not determine ClickHouse port number: Failed to spawn process.")?;
    ensure!(
        output.status.success(),
        "Could not determine ClickHouse port number: Process failed"
    );
    extract_port_from_lsof_output(pid, &output.stdout)
}

// Parse the output of the command used to find the HTTP port ClickHouse listens on, in the event
// we start it with a port of 0.
#[cfg(target_os = "illumos")]
async fn discover_local_listening_port(pid: u32) -> Result<u16, anyhow::Error> {
    let output = tokio::process::Command::new("pfiles")
        .arg(format!("{}", pid))
        .output() // Spawn and collect the output
        .await
        .context("Could not determine ClickHouse port number: Failed to spawn pfiles process.")?;
    ensure!(
        output.status.success(),
        "Could not determine ClickHouse port number: Pfiles process failed"
    );
    extract_port_from_pfiles_output(&output.stdout)
}

// Ports that ClickHouse opens, but we'd like to ignore when discovering.
const IGNORED_CLICKHOUSE_PORTS: &[u16] = &[9000, 9004];

// Extract the port from `pfiles` output.
//
// This output is much simpler, since it already restricts things to the PID we're interested int.
// Just look for AF_INET lines and pull out the port.
#[cfg_attr(target_os = "illumos", allow(dead_code))]
fn extract_port_from_pfiles_output(
    output: &[u8],
) -> Result<u16, anyhow::Error> {
    let text = std::str::from_utf8(output)
            .context("Could not determine ClickHouse port number: Non-UTF8 output from command")?;
    for port_str in text.lines().filter_map(|line| {
        if line.trim().starts_with("sockname: AF_INET") {
            line.split_whitespace().last()
        } else {
            None
        }
    }) {
        let port = port_str
            .parse()
            .context("Could not determine ClickHouse port number: Invalid port found in output")?;
        if !IGNORED_CLICKHOUSE_PORTS.contains(&port) {
            return Ok(port);
        }
    }
    bail!("Could not determine ClickHouse port number: No valid ports found in output");
}

// Parse the output from the `lsof` command on non-illumos systems
//
// The exact command run is: `lsof -i 6TCP@localhost -F n`.
//
// The output has groups of files like this:
// p<PID>
// f<FD>
// n<NAME>
// f<FD>
// n<NAME>
// ...
// p<PID>
// ...
//
// Parsing proceeds by:
// - Splitting into lines
// - Ignoring output until a PID line `p<PID>` is found, with the expected PID
// - Ignores `n<FD>` lines
// - Parses lines that look like `flocalhost:<NUMERIC_PORT>`
// - Returns the first match, that's _not_ one of the other ports ClickHouse opens.
//
// If any of these conditions fails, an error is returned.
#[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
fn extract_port_from_lsof_output(
    expected_pid: u32,
    output: &[u8],
) -> Result<u16, anyhow::Error> {
    ensure!(
        !output.is_empty(),
        "Could not determine ClickHouse port number: Process output empty"
    );

    // Break into newline-terminated chunks.
    let mut chunks = output.split(|&x| x == b'\n');

    // Small helpers to parse chunks.
    let is_process_start = |chunk: &[u8]| matches!(chunk.first(), Some(b'p'));
    let is_file_descriptor = |chunk: &[u8]| matches!(chunk.first(), Some(b'f'));
    let is_file_name = |chunk: &[u8]| matches!(chunk.first(), Some(b'n'));

    while let Some(chunk) = chunks.next() {
        if is_process_start(&chunk) {
            // Start of a process group.
            //
            // Parse the PID, check if it matches our expected PID.
            let pid: u32 = match String::from_utf8(chunk[1..].to_vec())
                .context("Could not determine ClickHouse port number: Non-UTF8 output")?
                .parse() {
                    Ok(pid) => pid,
                    _ => continue,
            };
            if pid == expected_pid {
                // PID matches
                //
                // The first chunk should be the numeric file descriptor
                if let Some(should_be_fd) = chunks.next() {
                    ensure!(
                        is_file_descriptor(should_be_fd),
                        "Could not determine ClickHouse port number: Expected numeric file descriptor in output"
                    );
                } else {
                    bail!("Could not determine ClickHouse port number: Expected numeric file descriptor in output");
                }

                // Process chunks until we find one that has a valid port, or we get one that's
                // _not_ a filename.
                while let Some(chunk) = chunks.next() {
                    ensure!(
                        is_file_name(chunk),
                        "Could not determine ClickHouse port number: Expected file name in output"
                    );

                    // Ignore leading `n`, which is part of the formatting from lsof
                    let chunk = &chunk[1..];

                    // Check if this looks like `localhost:<PORT>`
                    const LOCALHOST: &[u8] = b"localhost:";
                    if chunk.starts_with(LOCALHOST) {
                        let port: u16 = std::str::from_utf8(&chunk[LOCALHOST.len()..])
                            .context("Could not determine ClickHouse port number: Invalid PID in output")?
                            .parse()
                            .context("Could not determine ClickHouse port number: Invalid PID in output")?;

                        // Check that it's not one of the default other TCP ports ClickHouse opens
                        // by default
                        if !IGNORED_CLICKHOUSE_PORTS.contains(&port) {
                            return Ok(port);
                        }
                    }
                }

                // Early exit, the PID matched, but we couldn't find a valid port
                break;
            }
        }
    }
    bail!("Could not determine ClickHouse port number: No valid ports found in output");
}

#[cfg(test)]
mod pfiles_tests {
    use super::extract_port_from_pfiles_output;

    // A known-good test output.
    const GOOD_INPUT: &[u8] = br#"
        25: S_IFSOCK mode:0666 dev:547,0 ino:24056 uid:0 gid:0 rdev:0,0
          O_RDWR FD_CLOEXEC
            SOCK_STREAM
            SO_REUSEADDR,SO_SNDBUF(49152),SO_RCVBUF(128000)
            sockname: AF_INET6 ::1  port: 9004
        26: S_IFSOCK mode:0666 dev:547,0 ino:24056 uid:0 gid:0 rdev:0,0
          O_RDWR FD_CLOEXEC
            SOCK_STREAM
            SO_REUSEADDR,SO_SNDBUF(49152),SO_RCVBUF(128000)
            sockname: AF_INET 127.0.0.1  port: 8123
        27: S_IFSOCK mode:0666 dev:547,0 ino:42019 uid:0 gid:0 rdev:0,0
          O_RDWR FD_CLOEXEC
            SOCK_STREAM
            SO_REUSEADDR,SO_SNDBUF(49152),SO_RCVBUF(128000)
            sockname: AF_INET 127.0.0.1  port: 9000
        "#;

    // Only contains the ignored ClickHouse ports
    const ONLY_IGNORED_CLICKHOUSE_PORTS: &[u8] = br#"
        25: S_IFSOCK mode:0666 dev:547,0 ino:24056 uid:0 gid:0 rdev:0,0
          O_RDWR FD_CLOEXEC
            SOCK_STREAM
            SO_REUSEADDR,SO_SNDBUF(49152),SO_RCVBUF(128000)
            sockname: AF_INET6 ::1  port: 9004
        "#;

    #[test]
    fn test_extract_port_from_pfiles_output() {
        assert_eq!(extract_port_from_pfiles_output(&GOOD_INPUT).unwrap(), 8123);
    }

    #[test]
    fn test_extract_port_from_lsof_output_no_valid_port() {
        assert!(extract_port_from_pfiles_output(
            &ONLY_IGNORED_CLICKHOUSE_PORTS
        )
        .is_err());
    }
}

#[cfg(test)]
mod lsof_tests {
    use super::extract_port_from_lsof_output;

    // A known-good test output. This was generated by running the actual command while a
    // ClickHouse process is running.
    const GOOD_INPUT: &[u8] = b"p462\n\
        f4\n\
        nlocalhost:19536\n\
        p7741\n\
        f8\n\
        nlocalhost:53091\n\
        f9\n\
        nlocalhost:cslistener\n\
        f12\n\
        nlocalhost:9004\n";

    // This command has some valid `localhost:PORT` lines, but those ports are known to be other
    // ports that ClickHouse opens that aren't HTTP. These are the native client and the mySQL
    // client ports.
    const ONLY_IGNORED_CLICKHOUSE_PORTS: &[u8] = b"p462\n\
        f4\n\
        nlocalhost:19536\n\
        p7741\n\
        f8\n\
        nlocalhost:9000\n\
        f9\n\
        nlocalhost:cslistener\n\
        f12\n\
        nlocalhost:9004\n";

    // A bad output that has no lines like `flocalhost:<PORT>\n` at all
    const NO_FILE_NAMES: &[u8] = b"p462\n\
        f4\n\
        nlocalhost:19536\n\
        p7741\n\
        f8\n\
        f9\n\
        f12\n";

    #[test]
    fn test_extract_port_from_lsof_output() {
        assert_eq!(
            extract_port_from_lsof_output(7741, &GOOD_INPUT).unwrap(),
            53091
        );
    }

    #[test]
    fn test_extract_port_from_lsof_output_no_valid_port() {
        assert!(extract_port_from_lsof_output(
            7741,
            &ONLY_IGNORED_CLICKHOUSE_PORTS
        )
        .is_err());
    }

    // A test that uses the good input, but assumes we're looking for another PID.
    #[test]
    fn test_extract_port_from_lsof_output_incorrect_pid() {
        assert!(extract_port_from_lsof_output(0, &GOOD_INPUT).is_err());
    }

    #[test]
    fn test_extract_port_from_lsof_output_no_file_names() {
        assert!(extract_port_from_lsof_output(0, &NO_FILE_NAMES).is_err());
    }
}
