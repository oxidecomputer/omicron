// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Result, anyhow};
use clap::Parser;
use omicron_common::cmd::{CmdError, fatal};
use sp_sim::SimRack;
use sp_sim::config::Config;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Parser)]
#[clap(name = "sp-sim", about = "See README.adoc for more information")]
struct Args {
    #[clap(name = "CONFIG_FILE_PATH", action)]
    config_file_path: PathBuf,
}

fn main() {
    if let Err(cmd_error) = oxide_tokio_rt::run(do_run()) {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();
    let config = Config::from_file(args.config_file_path)
        .map_err(|e| CmdError::Failure(anyhow!(e)))?;

    let log = sp_sim::logger(&config).map_err(CmdError::Failure)?;

    let _rack =
        SimRack::start(&config, &log).await.map_err(CmdError::Failure)?;

    // for now, do nothing except let the spawned tasks run. in the future
    // (or when used as a library), the expectation is that a caller can
    // poke the simulated SPs via the handles to inject state changes, etc.
    loop {
        tokio::time::sleep(Duration::MAX).await;
    }
}
