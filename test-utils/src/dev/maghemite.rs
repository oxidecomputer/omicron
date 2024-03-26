// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for managing Maghemite during development

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

/// Specifies the amount of time we will wait for `mgd` to launch,
/// which is currently confirmed by watching `mgd`'s log output
/// for a message specifying the address and port `mgd` is listening on.
pub const MGD_TIMEOUT: Duration = Duration::new(5, 0);

pub struct MgdInstance {
    /// Port number the mgd instance is listening on. This can be provided
    /// manually, or dynamically determined if a value of 0 is provided.
    pub port: u16,
    /// Arguments provided to the `mgd` cli command.
    pub args: Vec<String>,
    /// Child process spawned by running `mgd`
    pub child: Option<tokio::process::Child>,
    /// Temporary directory where logging output and other files generated by
    /// `mgd` are stored.
    pub data_dir: Option<PathBuf>,
}

impl MgdInstance {
    pub async fn start(mut port: u16) -> Result<Self, anyhow::Error> {
        let temp_dir = TempDir::new()?;

        let args = vec![
            "run".to_string(),
            "--admin-addr".into(),
            "::1".into(),
            "--admin-port".into(),
            port.to_string(),
            "--no-bgp-dispatcher".into(),
            "--data-dir".into(),
            temp_dir.path().display().to_string(),
            "--rack-uuid".into(),
            uuid::Uuid::new_v4().to_string(),
            "--sled-uuid".into(),
            uuid::Uuid::new_v4().to_string(),
        ];

        let child = tokio::process::Command::new("mgd")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(redirect_file(temp_dir.path(), "mgd_stdout")?))
            .stderr(Stdio::from(redirect_file(temp_dir.path(), "mgd_stderr")?))
            .spawn()
            .with_context(|| {
                format!("failed to spawn `mgd` (with args: {:?})", &args)
            })?;

        let child = Some(child);

        let temp_dir = temp_dir.into_path();
        if port == 0 {
            port = discover_port(
                temp_dir.join("mgd_stdout").display().to_string(),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to discover mgd port from files in {}",
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

impl Drop for MgdInstance {
    fn drop(&mut self) {
        if self.child.is_some() || self.data_dir.is_some() {
            eprintln!(
                "WARN: dropped MgdInstance without cleaning it up first \
                (there may still be a child process running and a \
                temporary directory leaked)"
            );
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
            if let Some(path) = self.data_dir.take() {
                eprintln!(
                    "WARN: mgd temporary directory leaked: {}",
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
    let timeout = Instant::now() + MGD_TIMEOUT;
    tokio::time::timeout_at(timeout, find_mgd_port_in_log(logfile))
        .await
        .context("time out while discovering mgd port number")?
}

async fn find_mgd_port_in_log(logfile: String) -> Result<u16, anyhow::Error> {
    let re = regex::Regex::new(r#""local_addr":"\[::\]:?([0-9]+)""#).unwrap();
    let reader = BufReader::new(File::open(logfile).await?);
    let mut lines = reader.lines();
    loop {
        match lines.next_line().await? {
            Some(line) => {
                if let Some(cap) = re.captures(&line) {
                    // unwrap on get(1) should be ok, since captures() returns
                    // `None` if there are no matches found
                    let port = cap.get(1).unwrap();
                    let result = port.as_str().parse::<u16>()?;
                    return Ok(result);
                }
            }
            None => {
                sleep(Duration::from_millis(10)).await;
            }
        }
    }
}
