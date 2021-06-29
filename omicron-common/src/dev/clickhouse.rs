//! Tools for managing ClickHouse during development

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use tempfile::TempDir;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};

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
        let port = if port != 0 {
            port
        } else {
            discover_local_listening_port(&log_path).await?
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

// Parse the ClickHouse log file at the given path, looking for a line reporting the port number of
// the HTTP server. This is only used if the port is chosen by the OS, not the caller.
async fn discover_local_listening_port(
    path: &Path,
) -> Result<u16, anyhow::Error> {
    let reader = BufReader::new(
        File::open(path).await.context("Failed to open ClickHouse log file")?,
    );
    const NEEDLE: &str = "<Information> Application: Listening for http://";
    let mut lines = reader.lines();
    while let Some(line) = lines
        .next_line()
        .await
        .context("Failed to read line from ClickHouse log file")?
    {
        if let Some(needle_start) = line.find(&NEEDLE) {
            let address_start = needle_start + NEEDLE.len();
            let address: SocketAddr =
                line[address_start..].trim().parse().context(
                    "Failed to parse ClickHouse socket address from log",
                )?;
            return Ok(address.port());
        }
    }
    bail!("Failed to discover port from ClickHouse log file");
}
