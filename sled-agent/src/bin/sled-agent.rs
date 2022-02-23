// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the sled agent

#![feature(async_closure)]

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::external::Error;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::bootstrap::{
    config::Config as BootstrapConfig, config::SetupServiceConfig as RssConfig,
    server as bootstrap_server,
};
use omicron_sled_agent::{config::Config as SledConfig, server as sled_server};
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
        Args::Run { config_path } => {
            let config = SledConfig::from_file(&config_path)
                .map_err(|e| CmdError::Failure(e.to_string()))?;

            // TODO: I don't love this, but...
            //
            // - Sled agent starts with the normal config file - typically
            // called "config.toml".
            // - Thing-flinger likes allowing "sled-specific" configs to arrive
            // by overlaying files in the package...
            // - ... so we need a way to *possibly* supply this extra config,
            // without otherwise changing the package.
            //
            // This means we gotta (maybe) ingest a config file, without
            // *explicitly* being told about it.
            //
            // Hence, this approach: look around in the same directory as the
            // expected config file.
            //
            // Better ideas are welcome.
            let rss_config_path = {
                let mut rss_config_path = config_path.clone();
                rss_config_path.pop();
                rss_config_path.push("config-rss.toml");
                rss_config_path
            };
            let rss_config = if rss_config_path.exists() {
                Some(
                    RssConfig::from_file(rss_config_path)
                        .map_err(|e| CmdError::Failure(e.to_string()))?,
                )
            } else {
                None
            };

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
                rss_config,
            };
            let run_bootstrap = async move || -> Result<(), CmdError> {
                bootstrap_server::Server::start(&bootstrap_config)
                    .await
                    .map_err(CmdError::Failure)?
                    .wait_for_finish()
                    .await
                    .map_err(CmdError::Failure)
            };

            let run_sled_server = async move || -> Result<(), CmdError> {
                sled_server::Server::start(&config)
                    .await
                    .map_err(CmdError::Failure)?
                    .wait_for_finish()
                    .await
                    .map_err(CmdError::Failure)
            };

            tokio::select! {
                Err(e) = run_bootstrap() => {
                    eprintln!("Boot server exited unexpectedly: {:?}", e);
                },
                Err(e) = run_sled_server() => {
                    eprintln!("Sled server exited unexpectedly: {:?}", e);
                },
            }
            Ok(())
        }
    }
}
