// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use omicron_common::cmd::{fatal, CmdError};
use sp_sim::config::{Config, SpType};
use sp_sim::{Gimlet, Sidecar};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;
use tokio::io::AsyncReadExt;
use tokio::select;

#[derive(Debug, StructOpt)]
#[structopt(name = "sp-sim", about = "See README.adoc for more information")]
struct Args {
    #[structopt(name = "CONFIG_FILE_PATH", parse(from_os_str))]
    config_file_path: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::from_args_safe().map_err(|err| {
        CmdError::Usage(format!("parsing arguments: {}", err.message))
    })?;
    let config = Config::from_file(args.config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    match config.sp_type {
        SpType::Sidecar => run_sidecar(&config).await,
        SpType::Gimlet => run_gimlet(&config).await,
    }
}

async fn run_sidecar(config: &Config) -> Result<(), CmdError> {
    let _sidecar = Sidecar::spawn(config)
        .await
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    // for now, do nothing except respond to incoming messages. in the future,
    // maybe we respond to external input (signals?) to change ignition state,
    // or maybe that's limited to library use.
    tokio::time::sleep(Duration::MAX).await;
    Ok(())
}

async fn run_gimlet(config: &Config) -> Result<(), CmdError> {
    let mut gimlet = Gimlet::spawn(config)
        .await
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    // tokio docs warn against using its stdin handle for user-interactive
    // input; we'll live dangerously in this simulator
    let mut stdin = tokio::io::stdin();
    let mut stdout = std::io::stdout();
    let mut buf = [0; 512];

    loop {
        select! {
            res = stdin.read(&mut buf) => {
                let n = res.map_err(|e| {
                    CmdError::Failure(format!("failed to read stdin: {}", e))
                })?;
                gimlet
                    .send_serial_console(&buf[..n])
                    .await
                    .map_err(|e| CmdError::Failure(e.to_string()))?;
            }
            incoming = gimlet.incoming_serial_console() => {
                write!(stdout, "{}", String::from_utf8_lossy(&incoming)).unwrap();
                stdout.flush().unwrap();
            }
        }
    }
}
