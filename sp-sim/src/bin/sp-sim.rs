// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use omicron_common::cmd::{fatal, CmdError};
use sp_sim::{Config, Sidecar};
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

    let _sidecar = Sidecar::spawn(&config)
        .await
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    // gross; real use case is as a lib, where we wait for incoming requests to
    // poke at `sidecar` to change its state for tests. for now just wait to be
    // killed.
    tokio::time::sleep(Duration::MAX).await;
    Ok(())
}
