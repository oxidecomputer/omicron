// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for managing Dendrite during development

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    time::{sleep, Instant},
};

/// Specifies the amount of time we will wait for `dpd` to launch,
/// which is currently confirmed by watching `dpd`'s log output
/// for a message specifying the address and port `dpd` is listening on.
pub const DENDRITE_TIMEOUT: Duration = Duration::new(30, 0);

/// Represents a running instance of the Dendrite dataplane daemon (dpd).
pub struct DendriteInstance {
    /// Port number the dpd instance is listening on. This can be provided
    /// manually, or dynamically determined if a value of 0 is provided.
    pub port: u16,
    /// Arguments provided to the `dpd` cli command.
    pub args: Vec<String>,
    /// Child process spawned by running `dpd`
    pub child: Option<tokio::process::Child>,
    /// Temporary directory where logging output and other files generated by
    /// `dpd` are stored.
    pub data_dir: Option<PathBuf>,
}

impl DendriteInstance {
    pub async fn start(port: u16) -> Result<Self, anyhow::Error> {
        let mut port = port;
        let temp_dir = TempDir::new()?;
        let address_one = format!("[::1]:{port}");

        let args = vec![
            "run".to_string(),
            "--listen-addresses".to_string(),
            address_one,
        ];

        let child = tokio::process::Command::new("dpd")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(redirect_file(
                temp_dir.path(),
                "dendrite_stdout",
            )?))
            .stderr(Stdio::from(redirect_file(
                temp_dir.path(),
                "dendrite_stderr",
            )?))
            .spawn()
            .with_context(|| {
                format!("failed to spawn `dpd` (with args: {:?})", &args)
            })?;

        let child = Some(child);

        let temp_dir = temp_dir.into_path();
        if port == 0 {
            port = discover_port(
                temp_dir.join("dendrite_stdout").display().to_string(),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to discover dendrite port from files in {}",
                    temp_dir.display()
                )
            })?;
        }

        Ok(Self { port, args, child, data_dir: Some(temp_dir) })
    }

    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.start_kill().context("Sending SIGKILL to child")?;
            child.wait().await.context("waiting for child")?;
        }
        if let Some(dir) = self.data_dir.take() {
            std::fs::remove_dir_all(&dir).with_context(|| {
                format!("cleaning up temporary directory {}", dir.display())
            })?;
        }
        Ok(())
    }
}

impl Drop for DendriteInstance {
    fn drop(&mut self) {
        if self.child.is_some() || self.data_dir.is_some() {
            eprintln!(
                "WARN: dropped DendriteInstance without cleaning it up first \
                (there may still be a child process running and a \
                temporary directory leaked)"
            );
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
            if let Some(path) = self.data_dir.take() {
                eprintln!(
                    "WARN: dendrite temporary directory leaked: {}",
                    path.display()
                );
            }
        }
    }
}

fn redirect_file(
    temp_dir_path: &Path,
    label: &str,
) -> Result<std::fs::File, anyhow::Error> {
    let out_path = temp_dir_path.join(label);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&out_path)
        .with_context(|| format!("open \"{}\"", out_path.display()))
}

async fn discover_port(logfile: String) -> Result<u16, anyhow::Error> {
    let timeout = Instant::now() + DENDRITE_TIMEOUT;
    tokio::time::timeout_at(timeout, find_dendrite_port_in_log(logfile))
        .await
        .context("time out while discovering dendrite port number")?
}

async fn find_dendrite_port_in_log(
    logfile: String,
) -> Result<u16, anyhow::Error> {
    let re = regex::Regex::new(r#""local_addr":"\[::1\]:([0-9]+)""#).unwrap();
    let mut reader = BufReader::new(File::open(&logfile).await?);
    let mut lines = reader.lines();
    loop {
        match lines.next_line().await? {
            Some(line) => {
                if let Some(cap) = re.captures(&line) {
                    // unwrap on get(1) should be ok, since captures() returns `None`
                    // if there are no matches found
                    let port = cap.get(1).unwrap();
                    let result = port.as_str().parse::<u16>()?;
                    return Ok(result);
                }
            }
            None => {
                sleep(Duration::from_millis(10)).await;

                // We might have gotten a partial line; close the file, reopen
                // it, and start reading again from the beginning.
                reader = BufReader::new(File::open(&logfile).await?);
                lines = reader.lines();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::find_dendrite_port_in_log;
    use std::io::Write;
    use std::process::Stdio;
    use tempfile::NamedTempFile;

    const EXPECTED_PORT: u16 = 12345;

    #[tokio::test]
    async fn test_dpd_in_path() {
        // With no arguments, we expect to see the default help message.
        tokio::process::Command::new("dpd")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Cannot find 'dpd' on PATH. Refer to README.md for installation instructions");
    }

    #[tokio::test]
    async fn test_discover_local_listening_port() {
        // Write some data to a fake log file
        // This line is representative of the kind of output that dpd currently logs
        let line = r#"{"msg":"registered endpoint","v":0,"name":"dpd","level":20,"time":"2023-03-31T21:29:27.715517984Z","hostname":"workstation-1","pid":7480,"local_addr":"[::1]:12345","server_id":"0","unit":"api-server","path":"/all-settings","method":"DELETE"}"#;
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(file, "{}", line).unwrap();
        writeln!(file, "Another garbage line").unwrap();
        file.flush().unwrap();

        assert_eq!(
            find_dendrite_port_in_log(file.path().display().to_string())
                .await
                .unwrap(),
            EXPECTED_PORT
        );
    }
}
