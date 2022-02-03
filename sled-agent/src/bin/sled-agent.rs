// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the sled agent

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::external::Error;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::bootstrap::{
    config::Config as BootstrapConfig, server as bootstrap_server,
};
use omicron_sled_agent::{
    config::Config as SledConfig, server as sled_server,
};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug)]
enum ApiRequest {
    Bootstrap,
    Sled,
}

impl std::str::FromStr for ApiRequest {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bootstrap" => Ok(ApiRequest::Bootstrap),
            "sled" => Ok(ApiRequest::Sled),
            _ => Err(Error::InvalidValue {
                label: s.to_string(),
                message: "Invalid value: try one of {bootstrap, sled}"
                    .to_string(),
            }),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled_agent",
    about = "See README.adoc for more information"
)]
enum Args {
    /// Generates the OpenAPI specification.
    Openapi {
        #[structopt(name = "api_type", parse(try_from_str))]
        api_requested: ApiRequest,
    },
    /// Runs the Sled Agent server.
    Run {
        #[structopt(name = "CONFIG_FILE_PATH", parse(from_os_str))]
        config_path: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(message);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::from_args_safe().map_err(|err| {
        CmdError::Usage(format!("parsing arguments: {}", err.message))
    })?;

    match args {
        Args::Openapi { api_requested } => match api_requested {
            ApiRequest::Bootstrap => {
                bootstrap_server::run_openapi().map_err(CmdError::Failure)
            }
            ApiRequest::Sled => {
                sled_server::run_openapi().map_err(CmdError::Failure)
            }
        },
        Args::Run {
            config_path,
        } => {
            let config = SledConfig::from_file(config_path).map_err(|e| CmdError::Failure(e.to_string()))?;

            // Configure and run the Bootstrap server.
            let bootstrap_config = BootstrapConfig {
                id: config.id,
                dropshot: ConfigDropshot {
                    bind_address: config.bootstrap_address,
                    ..Default::default()
                },
                log: ConfigLogging::StderrTerminal {
                    level: ConfigLoggingLevel::Info,
                },
            };
            let boot_server = bootstrap_server::Server::start(&bootstrap_config)
                .await
                .map_err(CmdError::Failure)?;

            let sled_server = sled_server::Server::start(&config)
                .await
                .map_err(CmdError::Failure)?;

            tokio::select! {
                _ = boot_server.wait_for_finish() => {
                    eprintln!("Boot server exited unexpectedly");
                },
                _ = sled_server.wait_for_finish() => {
                    eprintln!("Sled server exited unexpectedly");
                },
            }
            Ok(())
        }
    }
}
