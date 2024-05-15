// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query a crucible-agent

use anyhow::bail;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use crucible_agent_client::types::RegionId;
use crucible_agent_client::Client;
use tabled::Tabled;

use crate::helpers::CONNECTION_OPTIONS_HEADING;
use crate::Omdb;

/// Arguments to the "omdb crucible-agent" subcommand
#[derive(Debug, Args)]
pub struct CrucibleAgentArgs {
    /// URL of the crucible agent internal API
    #[clap(
        long,
        env = "OMDB_CRUCIBLE_AGENT_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    crucible_agent_url: Option<String>,

    #[command(subcommand)]
    command: CrucibleAgentCommands,
}

/// Subcommands for the "omdb crucible-agent" subcommand
#[derive(Debug, Subcommand)]
enum CrucibleAgentCommands {
    /// print information about regions
    #[clap(subcommand)]
    Regions(RegionCommands),
    /// print information about snapshots
    #[clap(subcommand)]
    Snapshots(SnapshotCommands),
}

#[derive(Debug, Subcommand)]
enum RegionCommands {
    /// Print list of all running control plane regions
    List,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommands {
    /// Print list of all running control plane snapshots
    List,
}

impl CrucibleAgentArgs {
    /// Run a `omdb crucible-agent` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        _omdb: &Omdb,
    ) -> Result<(), anyhow::Error> {
        // The crucible agent URL is required, but can come
        // from the environment, in which case it won't be on the command line.
        let Some(crucible_agent_url) = &self.crucible_agent_url else {
            bail!(
                "crucible agent URL must be specified with \
                --crucible-agent-url or by setting the environment variable \
                OMDB_CRUCIBLE_AGENT_URL"
            );
        };
        let client = Client::new(crucible_agent_url);

        match &self.command {
            CrucibleAgentCommands::Regions(RegionCommands::List) => {
                cmd_region_list(&client).await
            }
            CrucibleAgentCommands::Snapshots(SnapshotCommands::List) => {
                cmd_snapshot_list(&client).await
            }
        }
    }
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct Region {
    region_id: String,
    state: String,
    block_size: String,
    extent_size: String,
    extent_count: String,
    port: String,
}

/// Runs `omdb crucible-agent regions list`
async fn cmd_region_list(
    client: &crucible_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let regions = client.region_list().await.context("listing regions")?;

    let mut rows = Vec::new();
    for region in regions.iter() {
        rows.push(Region {
            region_id: region.id.clone().to_string(),
            state: region.state.to_string(),
            block_size: region.block_size.to_string(),
            extent_size: region.extent_size.to_string(),
            extent_count: region.extent_count.to_string(),
            port: region.port_number.to_string(),
        });
    }
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);
    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct Snapshot {
    region_id: String,
    snapshot_id: String,
    state: String,
    port: String,
}
/// Runs `omdb crucible-agent snapshot list`
async fn cmd_snapshot_list(
    client: &crucible_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let regions = client.region_list().await.context("listing regions")?;

    let mut rows = Vec::new();
    for region in regions.iter() {
        let snapshots = match client
            .region_get_snapshots(&RegionId(region.id.to_string()))
            .await
        {
            Ok(snapshots) => snapshots,
            Err(e) => {
                println!(
                    "Error {} looking at region {} for snapshots",
                    e,
                    region.id.to_string()
                );
                continue;
            }
        };
        if snapshots.snapshots.is_empty() {
            continue;
        }
        for snap in snapshots.snapshots.iter() {
            match snapshots.running_snapshots.get(&snap.name) {
                Some(rs) => {
                    rows.push(Snapshot {
                        region_id: region.id.clone().to_string(),
                        snapshot_id: snap.name.to_string(),
                        state: rs.state.to_string(),
                        port: rs.port_number.to_string(),
                    });
                }
                None => {
                    rows.push(Snapshot {
                        region_id: region.id.clone().to_string(),
                        snapshot_id: snap.name.to_string(),
                        state: "---".to_string(),
                        port: "---".to_string(),
                    });
                }
            }
        }
    }
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}
