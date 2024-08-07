// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for inventory checks via wicketd.

use crate::cli::CommandOutput;
use crate::wicketd::create_wicketd_client;
use anyhow::Context;
use anyhow::Result;
use clap::Subcommand;
use owo_colors::OwoColorize;
use sled_hardware_types::Baseboard;
use slog::Logger;
use std::net::SocketAddrV6;
use std::time::Duration;
use wicket_common::rack_setup::BootstrapSledDescription;

const WICKETD_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Subcommand)]
pub(crate) enum InventoryArgs {
    /// List state of all bootstrap sleds, as configured with rack-setup
    ConfiguredBootstrapSleds {
        /// Print output as json
        #[clap(long)]
        json: bool,
    },
}

impl InventoryArgs {
    pub(crate) async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        mut output: CommandOutput<'_>,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        match self {
            InventoryArgs::ConfiguredBootstrapSleds { json } => {
                // We don't use the /bootstrap-sleds endpoint, because that
                // gets all sleds visible on the bootstrap network. We want
                // something subtly different here.
                // - We want the status of only sleds we've configured wicket
                //   to use for setup. /bootstrap-sleds will give us sleds
                //   we don't want
                // - We want the status even if they aren't visible on the
                //   bootstrap network yet.
                //
                // In other words, we want the sled information displayed at the
                // bottom of the rack setup screen in the TUI, and we get it the
                // same way it does.
                let conf = client
                    .get_rss_config()
                    .await
                    .context("failed to get rss config")?;

                let bootstrap_sleds = &conf.insensitive.bootstrap_sleds;
                if json {
                    let json_str = serde_json::to_string(bootstrap_sleds)
                        .context("serializing sled data failed")?;
                    writeln!(output.stdout, "{}", json_str)
                        .expect("writing to stdout failed");
                } else {
                    for sled in bootstrap_sleds {
                        print_bootstrap_sled_data(sled, &mut output);
                    }
                }

                Ok(())
            }
        }
    }
}

fn print_bootstrap_sled_data(
    desc: &BootstrapSledDescription,
    output: &mut CommandOutput<'_>,
) {
    let slot = desc.id.slot;

    let identifier = match &desc.baseboard {
        Baseboard::Gimlet { identifier, .. } => identifier.clone(),
        Baseboard::Pc { identifier, .. } => identifier.clone(),
        Baseboard::Unknown => "unknown".to_string(),
    };

    let address = desc.bootstrap_ip;

    // Create status indicators
    let status = match address {
        None => format!("{}", '⚠'.red()),
        Some(_) => format!("{}", '✔'.green()),
    };

    let addr_fmt = match address {
        None => "(not available)".to_string(),
        Some(addr) => format!("\t{}", addr),
    };

    // Print out this entry. We say "Cubby" rather than "Slot" here purely
    // because the TUI also says "Cubby".
    writeln!(
        output.stdout,
        "{status} Cubby {:02}\t{identifier}{addr_fmt}",
        slot
    )
    .expect("writing to stdout failed");
}
