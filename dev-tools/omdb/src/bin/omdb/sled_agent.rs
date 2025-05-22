// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Sleds

use crate::Omdb;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use anyhow::Context;
use anyhow::bail;
use clap::Args;
use clap::Subcommand;

/// Arguments to the "omdb sled-agent" subcommand
#[derive(Debug, Args)]
pub struct SledAgentArgs {
    /// URL of the Sled internal API
    #[clap(
        long,
        env = "OMDB_SLED_AGENT_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    sled_agent_url: Option<String>,

    #[command(subcommand)]
    command: SledAgentCommands,
}

/// Subcommands for the "omdb sled-agent" subcommand
#[derive(Debug, Subcommand)]
enum SledAgentCommands {
    /// print information about zones
    #[clap(subcommand)]
    Zones(ZoneCommands),

    /// print information about the local bootstore node
    #[clap(subcommand)]
    Bootstore(BootstoreCommands),
}

#[derive(Debug, Subcommand)]
enum ZoneCommands {
    /// Print list of all running control plane zones
    List,
}

#[derive(Debug, Subcommand)]
enum ZpoolCommands {
    /// Print list of all zpools managed by the sled agent
    List,
}

#[derive(Debug, Subcommand)]
enum DatasetCommands {
    /// Print list of all datasets the sled agent is configured to manage
    ///
    /// Note that the set of actual datasets on the sled may be distinct,
    /// use the `omdb db inventory collections show` command to see the latest
    /// set of datasets collected from sleds.
    List,
}

#[derive(Debug, Subcommand)]
enum BootstoreCommands {
    /// Show the internal state of the local bootstore node
    Status,
}

impl SledAgentArgs {
    /// Run a `omdb sled-agent` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        _omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        // This is a little goofy. The sled URL is required, but can come
        // from the environment, in which case it won't be on the command line.
        let Some(sled_agent_url) = &self.sled_agent_url else {
            bail!(
                "sled URL must be specified with --sled-agent-url or \
                OMDB_SLED_AGENT_URL"
            );
        };
        let client =
            sled_agent_client::Client::new(sled_agent_url, log.clone());

        match &self.command {
            SledAgentCommands::Zones(ZoneCommands::List) => {
                cmd_zones_list(&client).await
            }
            SledAgentCommands::Bootstore(BootstoreCommands::Status) => {
                cmd_bootstore_status(&client).await
            }
        }
    }
}

/// Runs `omdb sled-agent zones list`
async fn cmd_zones_list(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let response = client.zones_list().await.context("listing zones")?;
    let zones = response.into_inner();
    let zones: Vec<_> = zones.into_iter().collect();

    println!("zones:");
    if zones.is_empty() {
        println!("    <none>");
    }
    for zone in &zones {
        println!("    {:?}", zone);
    }

    Ok(())
}

/// Runs `omdb sled-agent bootstore status`
async fn cmd_bootstore_status(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let status = client.bootstore_status().await.context("bootstore status")?;
    println!("fsm ledger generation: {}", status.fsm_ledger_generation);
    println!(
        "network config ledger generation: {:?}",
        status.network_config_ledger_generation
    );
    println!("fsm state: {}", status.fsm_state);
    println!("peers (found by ddmd):");
    if status.peers.is_empty() {
        println!("    <none>");
    }
    for peer in status.peers.iter() {
        println!("    {peer}");
    }
    println!("established connections:");
    if status.established_connections.is_empty() {
        println!("    <none>");
    }
    for c in status.established_connections.iter() {
        println!("     {:?} : {}", c.baseboard, c.addr);
    }
    println!("accepted connections:");
    if status.accepted_connections.is_empty() {
        println!("    <none>");
    }
    for addr in status.accepted_connections.iter() {
        println!("    {addr}");
    }
    println!("negotiating connections:");
    if status.negotiating_connections.is_empty() {
        println!("    <none>");
    }
    for addr in status.negotiating_connections.iter() {
        println!("    {addr}");
    }

    Ok(())
}
