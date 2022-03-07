// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use omicron_common::cmd::{fatal, CmdError};
use slog::Logger;
use sp_sim::config::{Config, GimletConfig, SidecarConfig};
use sp_sim::{Gimlet, Sidecar};
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

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

    let mut sidecar_handles = Vec::new();
    let mut gimlet_handles = Vec::new();

    for (i, sidecar) in config.simulated_sps.sidecar.iter().enumerate() {
        sidecar_handles.push(
            spawn_sidecar(
                &config,
                sidecar,
                sp_sim::logger(&config, format!("sidecar {}", i)).unwrap(),
            )
            .await?,
        );
    }
    for (i, gimlet) in config.simulated_sps.gimlet.iter().enumerate() {
        gimlet_handles.push(
            spawn_gimlet(
                &config,
                gimlet,
                sp_sim::logger(&config, format!("gimlet {}", i)).unwrap(),
            )
            .await?,
        );
    }

    // for now, do nothing except let the spawned tasks run. in the future
    // (or when used as a library), the expectation is that a caller can
    // poke the simulated SPs via the handles to inject state changes, etc.
    loop {
        tokio::time::sleep(Duration::MAX).await;
    }
}

async fn spawn_sidecar(
    config: &Config,
    sidecar_config: &SidecarConfig,
    log: Logger,
) -> Result<Sidecar, CmdError> {
    Sidecar::spawn(config, sidecar_config, log)
        .await
        .map_err(|e| CmdError::Failure(e.to_string()))
}

async fn spawn_gimlet(
    config: &Config,
    gimlet_config: &GimletConfig,
    log: Logger,
) -> Result<Gimlet, CmdError> {
    Gimlet::spawn(config, gimlet_config, log)
        .await
        .map_err(|e| CmdError::Failure(e.to_string()))
}
