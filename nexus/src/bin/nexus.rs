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

use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_common::nexus_config::RuntimeConfig;
use omicron_nexus::run_openapi_external;
use omicron_nexus::run_openapi_internal;
use omicron_nexus::run_server;
use omicron_nexus::{Config, PackageConfig};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "nexus", about = "See README.adoc for more information")]
struct Args {
    #[structopt(
        short = "O",
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit"
    )]
    openapi: bool,

    #[structopt(
        short = "I",
        long = "openapi-internal",
        help = "Print the internal OpenAPI Spec document and exit"
    )]
    openapi_internal: bool,

    #[structopt(name = "PACKAGE_CONFIG_FILE_PATH", parse(from_os_str))]
    pkg_config_file_path: PathBuf,

    #[structopt(name = "RUNTIME_CONFIG_FILE_PATH", parse(from_os_str))]
    rt_config_file_path: PathBuf,
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

    let rt_config = RuntimeConfig::from_file(args.rt_config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    let pkg_config = PackageConfig::from_file(args.pkg_config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    let config = Config {
        runtime: rt_config,
        pkg: pkg_config,
    };

    if args.openapi {
        run_openapi_external().map_err(CmdError::Failure)
    } else if args.openapi_internal {
        run_openapi_internal().map_err(CmdError::Failure)
    } else {
        run_server(&config).await.map_err(CmdError::Failure)
    }
}
