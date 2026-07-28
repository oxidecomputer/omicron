// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for inventory checks via wicketd.

use crate::cli::CommandOutput;
use crate::wicketd::create_wicketd_client;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use clap::{Subcommand, ValueEnum};
use owo_colors::OwoColorize;
use slog::Logger;
use std::fmt;
use std::net::SocketAddrV6;
use std::time::Duration;
use wicket_common::inventory::Transceiver;
use wicket_common::rack_setup::BootstrapSledDescription;
use wicketd_client::types::{GetInventoryParams, GetInventoryResponse};

const WICKETD_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Subcommand)]
pub(crate) enum InventoryArgs {
    /// List state of all bootstrap sleds, as configured with rack-setup
    ConfiguredBootstrapSleds {
        /// Select output format
        #[clap(long, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// List the front-panel transceivers on each switch.
    ///
    /// This is the same data the TUI shows when a switch is selected,
    /// including vendor identity, power state, and environmental monitors
    /// (temperature, optical Rx/Tx power). It comes from the SP via wicketd
    /// and so remains available when Nexus is not.
    Transceivers {
        /// Select output format
        #[clap(long, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
}

#[derive(Debug, ValueEnum, Clone)]
pub enum OutputFormat {
    /// Print output as operator-readable table
    Table,

    /// Print output as json
    Json,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
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
            InventoryArgs::ConfiguredBootstrapSleds { format } => {
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
                match format {
                    OutputFormat::Json => {
                        let json_str =
                            serde_json::to_string_pretty(bootstrap_sleds)
                                .context("serializing sled data failed")?;
                        writeln!(output.stdout, "{}", json_str)
                            .expect("writing to stdout failed");
                    }
                    OutputFormat::Table => {
                        for sled in bootstrap_sleds {
                            print_bootstrap_sled_data(sled, &mut output);
                        }
                    }
                }

                Ok(())
            }
            InventoryArgs::Transceivers { format } => {
                let params = GetInventoryParams { force_refresh: Vec::new() };
                let response = client
                    .get_inventory(&params)
                    .await
                    .context("failed to get inventory from wicketd")?
                    .into_inner();
                let inventory = match response {
                    GetInventoryResponse::Response { inventory } => inventory,
                    GetInventoryResponse::Unavailable => {
                        bail!("wicketd reports inventory as unavailable")
                    }
                };
                let Some(snapshot) = inventory.transceivers else {
                    bail!(
                        "wicketd has no transceiver inventory yet \
                         (still discovering switch slot?)"
                    );
                };

                match format {
                    OutputFormat::Json => {
                        let json_str = serde_json::to_string_pretty(&snapshot)
                            .context("serializing transceiver data failed")?;
                        writeln!(output.stdout, "{}", json_str)
                            .expect("writing to stdout failed");
                    }
                    OutputFormat::Table => {
                        let mut slots: Vec<_> =
                            snapshot.inventory.iter().collect();
                        slots.sort_by_key(|(slot, _)| *slot);
                        for (slot, transceivers) in slots {
                            writeln!(output.stdout, "{slot:?}:")
                                .expect("writing to stdout failed");
                            let mut transceivers: Vec<_> =
                                transceivers.iter().collect();
                            transceivers.sort_by(|a, b| a.port.cmp(&b.port));
                            for tr in transceivers {
                                print_transceiver_row(tr, &mut output);
                            }
                        }
                        writeln!(
                            output.stdout,
                            "(last updated {:?} ago; use --format json for \
                             full monitor data)",
                            snapshot.last_seen
                        )
                        .expect("writing to stdout failed");
                    }
                }

                Ok(())
            }
        }
    }
}

fn print_transceiver_row(tr: &Transceiver, output: &mut CommandOutput<'_>) {
    let vendor = match &tr.vendor {
        Ok(v) => format!("{} {}", v.vendor.name.trim(), v.vendor.part.trim()),
        Err(e) => format!("(vendor: {e})"),
    };
    let power = match &tr.power {
        Ok(p) => format!("{:?}", p.state),
        Err(e) => format!("(power: {e})"),
    };
    let monitors = match &tr.monitors {
        Ok(_) => format!("{}", '✔'.green()),
        Err(e) => format!("{} {e}", '⚠'.yellow()),
    };
    writeln!(
        output.stdout,
        "  {:<8} {:<32} {:<10} monitors={monitors}",
        tr.port, vendor, power,
    )
    .expect("writing to stdout failed");
}

fn print_bootstrap_sled_data(
    desc: &BootstrapSledDescription,
    output: &mut CommandOutput<'_>,
) {
    let slot = desc.id.slot;

    let identifier = desc.baseboard.identifier();
    let address = desc.bootstrap_ip;

    // Create status indicators
    let status = match address {
        None => format!("{}", '⚠'.red()),
        Some(_) => format!("{}", '✔'.green()),
    };

    let addr_fmt = match address {
        None => "(not available)".to_string(),
        Some(addr) => format!("{}", addr),
    };

    // Print out this entry. We say "Cubby" rather than "Slot" here purely
    // because the TUI also says "Cubby".
    writeln!(
        output.stdout,
        "{status} Cubby {:02}\t{identifier}\t{addr_fmt}",
        slot
    )
    .expect("writing to stdout failed");
}
