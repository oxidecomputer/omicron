// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for rack setup configuration via wicketd.

use crate::wicketd::create_wicketd_client;
use anyhow::Context;
use anyhow::Result;
use clap::Subcommand;
use std::io;
use std::io::Read;
use std::net::SocketAddrV6;
use std::time::Duration;
use wicketd_client::types::PutRssUserConfigInsensitive;

mod config_toml;

use config_toml::TomlTemplate;

const WICKETD_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Subcommand)]
pub(crate) enum SetupArgs {
    /// Get the current rack configuration as a TOML template
    ///
    /// Save this template to a file, edit it, then upload it via `set-config`.
    GetConfig,

    /// Set the current rack configuration from a filled-in TOML template
    SetConfig,
}

impl SetupArgs {
    pub(crate) fn exec(
        self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        let runtime =
            tokio::runtime::Runtime::new().context("creating tokio runtime")?;

        runtime.block_on(self.exec_impl(log, wicketd_addr))
    }

    async fn exec_impl(
        self,
        log: slog::Logger,
        wicketd_addr: SocketAddrV6,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        match self {
            SetupArgs::GetConfig => {
                let config = client
                    .get_rss_config()
                    .await
                    .context("error fetching current config from wicketd")?
                    .into_inner();

                let template = TomlTemplate::populate(&config.insensitive);

                // This is intentionally not `println`; our template already
                // includes the final newline.
                print!("{template}");
            }
            SetupArgs::SetConfig => {
                let mut config = String::new();
                slog::info!(log, "reading config from stdin...");
                io::stdin()
                    .read_to_string(&mut config)
                    .context("failed to read config from stdin")?;

                slog::info!(log, "parsing config...");
                let config: PutRssUserConfigInsensitive =
                    toml::de::from_str(&config)
                        .context("failed to parse config TOML")?;

                slog::info!(log, "uploading config to wicketd...");
                client
                    .put_rss_config(&config)
                    .await
                    .context("error uploading config to wicketd")?;

                slog::info!(log, "config upload complete");
            }
        }

        Ok(())
    }
}
