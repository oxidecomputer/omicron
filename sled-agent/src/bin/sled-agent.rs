// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the sled agent

use dropshot::ConfigDropshot;
use omicron_common::api::external::Error;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::bootstrap::{
    agent::bootstrap_address, config::Config as BootstrapConfig,
    server as bootstrap_server,
};
use omicron_sled_agent::rack_setup::config::SetupServiceConfig as RssConfig;
use omicron_sled_agent::{config::Config as SledConfig, server as sled_server};
use sp_sim::config::GimletConfig;
use std::net::SocketAddr;
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

            // - Sled agent starts with the normal config file - typically
            // called "config.toml".
            // - Thing-flinger likes allowing "sled-specific" configs to arrive
            // by overlaying files in the package...
            // - ... so we need a way to *possibly* supply this extra config,
            // without otherwise changing the package.
            //
            // This means we must possibly ingest a config file, without
            // *explicitly* being told about it.
            //
            // Hence, this approach: look around in the same directory as the
            // expected config file.
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
            let sp_config_path = {
                let mut sp_config_path = config_path.clone();
                sp_config_path.pop();
                sp_config_path.push("config-sp.toml");
                sp_config_path
            };
            let sp_config = if sp_config_path.exists() {
                Some(
                    GimletConfig::from_file(sp_config_path)
                        .map_err(|e| CmdError::Failure(e.to_string()))?,
                )
            } else {
                None
            };

            // Derive the bootstrap address from the data link's MAC address.
            let link = config
                .get_link()
                .map_err(|e| CmdError::Failure(e.to_string()))?;
            let bootstrap_address = bootstrap_address(link)
                .map_err(|e| CmdError::Failure(e.to_string()))?;

            // Are we going to simulate a local SP? If so:
            //
            // 1. The bootstrap dropshot server listens on localhost
            // 2. A sprockets proxy listens on `bootstrap_address` (and relays
            //    incoming connections to the localhost dropshot server)
            //
            // If we're not simulating a local SP, we can't establish sprockets
            // sessions, so we'll have the bootstrap dropshot server listen on
            // `bootstrap_address` (and no sprockets proxy).
            //
            // TODO-security: With this configuration, dropshot itself is
            // running plain HTTP and blindly trusting all connections from
            // localhost. We have a similar sprockets proxy on the client side,
            // where the proxy blindly trusts all connections from localhost
            // (although the client-side proxy only runs while is being made,
            // while our dropshot server is always listening). Can we secure
            // these connections sufficiently? Other options include expanding
            // dropshot/progenitor to allow a custom connection layer (supported
            // by hyper, but not reqwest), keeping the sprockets proxy but using
            // something other than TCP that we can lock down, or abandoning
            // dropshot and using a bespoke protocol over a raw
            // sprockets-encrypted TCP connection.
            let (bootstrap_dropshot_addr, sprockets_proxy_bind_addr) =
                if sp_config.is_some() {
                    ("[::1]:0".parse().unwrap(), Some(bootstrap_address))
                } else {
                    (SocketAddr::V6(bootstrap_address), None)
                };

            // Configure and run the Bootstrap server.
            let bootstrap_config = BootstrapConfig {
                id: config.id,
                dropshot: ConfigDropshot {
                    bind_address: bootstrap_dropshot_addr,
                    request_body_max_bytes: 1024 * 1024,
                    ..Default::default()
                },
                log: config.log.clone(),
                rss_config,
                sprockets_proxy_bind_addr,
                sp_config,
            };

            // TODO: It's a little silly to pass the config this way - namely,
            // that we construct the bootstrap config from `config`, but then
            // pass it separately just so the sled agent can ingest it later on.
            bootstrap_server::Server::start(
                *bootstrap_address.ip(),
                bootstrap_config,
                config,
            )
            .await
            .map_err(CmdError::Failure)?
            .wait_for_finish()
            .await
            .map_err(CmdError::Failure)?;

            Ok(())
        }
    }
}
