// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use omicron_common::cmd::{fatal, CmdError};
use sp_sim::config::Config;
use sp_sim::SimRack;
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

    let log = sp_sim::logger(&config)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    let _rack = SimRack::start(&config, &log)
        .await
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    // for now, do nothing except let the spawned tasks run. in the future
    // (or when used as a library), the expectation is that a caller can
    // poke the simulated SPs via the handles to inject state changes, etc.
    loop {
        tokio::time::sleep(Duration::MAX).await;
    }
}
