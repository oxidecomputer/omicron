// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run Nexus, the heart of the control plane

// TODO
// - TCP and HTTP KeepAlive parameters
// - Server hostname
// - Disable signals?
// - Analogs for actix client_timeout (request timeout), client_shutdown (client
//   shutdown timeout), server backlog, number of workers, max connections per
//   worker, max connect-in-progress sockets, shutdown_timeout (server shutdown
//   timeout)

use clap::Parser;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_nexus::run_openapi_external;
use omicron_nexus::run_openapi_internal;
use omicron_nexus::run_server;
use omicron_nexus::Config;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(name = "nexus", about = "See README.adoc for more information")]
struct Args {
    #[clap(
        short = 'O',
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit",
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
    config_file_path: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    let config = Config::from_file(args.config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    if args.openapi {
        run_openapi_external().map_err(CmdError::Failure)
    } else if args.openapi_internal {
        run_openapi_internal().map_err(CmdError::Failure)
    } else {
        run_server(&config).await.map_err(CmdError::Failure)
    }
}
