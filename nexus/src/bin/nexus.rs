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
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_nexus::run_openapi_external;
use omicron_nexus::run_openapi_internal;
use omicron_nexus::run_server;

#[derive(Debug, Parser)]
#[clap(name = "nexus", about = "See README.adoc for more information")]
struct Args {
    #[clap(
        short = 'O',
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit",
        conflicts_with = "openapi_internal",
        action
    )]
    openapi: bool,

    #[clap(
        short = 'I',
        long = "openapi-internal",
        help = "Print the internal OpenAPI Spec document and exit",
        action
    )]
    openapi_internal: bool,

    #[clap(name = "CONFIG_FILE_PATH", action)]
    config_file_path: Option<Utf8PathBuf>,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    if args.openapi {
        run_openapi_external().map_err(|err| CmdError::Failure(anyhow!(err)))
    } else if args.openapi_internal {
        run_openapi_internal().map_err(|err| CmdError::Failure(anyhow!(err)))
    } else {
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
}
