// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run Nexus, the heart of the control plane

// TODO
// - Server hostname
// - Disable signals?
// - General networking and runtime tuning for availability and security: see
//   omicron#2184, omicron#2414.

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use nexus_config::NexusConfig;
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
use omicron_nexus::run_server;

#[derive(Debug, Parser)]
#[clap(name = "nexus", about = "See README.adoc for more information")]
struct Args {
    #[clap(name = "CONFIG_FILE_PATH", action)]
    config_file_path: Option<Utf8PathBuf>,
}

fn main() {
    if let Err(cmd_error) = oxide_tokio_rt::run(do_run()) {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    let config_path = match args.config_file_path {
        Some(path) => path,
        None => {
            use clap::CommandFactory;

            eprintln!("{}", Args::command().render_help());
            return Err(CmdError::Usage(
                "CONFIG_FILE_PATH is required".to_string(),
            ));
        }
    };
    let config = NexusConfig::from_file(config_path)
        .map_err(|e| CmdError::Failure(anyhow!(e)))?;

    run_server(&config).await.map_err(|err| CmdError::Failure(anyhow!(err)))
}
