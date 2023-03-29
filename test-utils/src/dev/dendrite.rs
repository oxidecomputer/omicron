// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for managing Dendrite during development

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    time::{sleep, Instant},
};

pub struct DendriteInstance {
    pub port: u16,
    pub args: Vec<String>,
    pub child: Option<tokio::process::Child>,
    pub data_dir: Option<TempDir>,
}

impl DendriteInstance {
    pub async fn start(port: u16) -> Result<Self, anyhow::Error> {
        let mut port = port;
        let temp_dir = TempDir::new()?;
        let address_one = format!("[::1]:{port}");
        let current_dir = std::env::current_dir()?;
        let p4_dir = current_dir
            .join("..")
            .join("out")
            .join("dendrite-stub")
            .join("bf_sde");

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
            .env("P4_DIR", p4_dir)
            .spawn()
            .with_context(|| {
                format!("failed to spawn `dpd` (with args: {:?})", &args)
            })?;

        let child = Some(child);

        if port == 0 {
            port = discover_port(
                temp_dir.path().join("dendrite_stdout").display().to_string(),
            )
            .await?;
        }

        Ok(Self { port, args, child, data_dir: Some(temp_dir) })
    }

    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.start_kill().context("Sending SIGKILL to child")?;
            child.wait().await.context("waiting for child")?;
        }
        if let Some(dir) = self.data_dir.take() {
            dir.close().context("Cleaning up temporary directory")?;
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
            if let Some(dir) = self.data_dir.take() {
                let path = dir.into_path();
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
    let timeout = Instant::now() + Duration::new(5, 0);
    tokio::time::timeout_at(timeout, find_dendrite_port_in_log(logfile))
        .await
        .context("time out while discovering dendrite port number")?
}

async fn find_dendrite_port_in_log(
    logfile: String,
) -> Result<u16, anyhow::Error> {
    let re = regex::Regex::new(r#""local_addr":"\[::1\]:?([0-9]+)""#).unwrap();
    let reader = BufReader::new(File::open(logfile).await?);
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
            }
        }
    }
}
