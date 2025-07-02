// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query a crucible-pantry

use anyhow::Context;
use anyhow::bail;
use clap::Args;
use clap::Subcommand;
use crucible_pantry_client::Client;
use tabled::Tabled;
use uuid::Uuid;

use crate::Omdb;
use crate::helpers::CONNECTION_OPTIONS_HEADING;

/// Arguments to the "omdb crucible-pantry" subcommand
#[derive(Debug, Args)]
pub struct CruciblePantryArgs {
    /// URL of the crucible pantry internal API
    #[clap(
        long,
        env = "OMDB_CRUCIBLE_PANTRY_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    crucible_pantry_url: Option<String>,

    #[command(subcommand)]
    command: CruciblePantryCommands,
}

/// Subcommands for the "omdb crucible-pantry" subcommand
#[derive(Debug, Subcommand)]
enum CruciblePantryCommands {
    /// Print information about a pantry jobs finished status
    IsFinished(FinishedArgs),
    /// Print information about the pantry
    Status,
    /// Print information about a specific volume
    Volume(VolumeArgs),
}

#[derive(Debug, Args, Clone)]
struct FinishedArgs {
    /// The Job ID
    id: String,
}

#[derive(Debug, Args, Clone)]
struct VolumeArgs {
    /// The UUID of the volume
    uuid: Uuid,
}

impl CruciblePantryArgs {
    /// Run a `omdb crucible-pantry` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        _omdb: &Omdb,
    ) -> Result<(), anyhow::Error> {
        // The crucible pantry URL is required, but can come from the
        // environment, in which case it won't be on the command line.
        let Some(crucible_pantry_url) = &self.crucible_pantry_url else {
            bail!(
                "crucible pantry URL must be specified with \
                --crucible-pantry-url or by setting the environment variable \
                OMDB_CRUCIBLE_PANTRY_URL"
            );
        };
        let client = Client::new(crucible_pantry_url);

        match &self.command {
            CruciblePantryCommands::IsFinished(id) => {
                cmd_is_finished(&client, id).await
            }
            CruciblePantryCommands::Status => cmd_pantry_status(&client).await,
            CruciblePantryCommands::Volume(uuid) => {
                cmd_volume_info(&client, uuid).await
            }
        }
    }
}

/// Runs `omdb crucible-agent volume`
async fn cmd_is_finished(
    client: &crucible_pantry_client::Client,
    args: &FinishedArgs,
) -> Result<(), anyhow::Error> {
    let is_finished =
        client.is_job_finished(&args.id).await.context("checking jobs")?;
    println!("Job: {} reports is_finished: {:?}", args.id, is_finished);
    Ok(())
}

/// Runs `omdb crucible-agent volume`
async fn cmd_volume_info(
    client: &crucible_pantry_client::Client,
    args: &VolumeArgs,
) -> Result<(), anyhow::Error> {
    let volume = args.uuid.to_string();
    let vs = client.volume_status(&volume).await.context("listing volumes")?;
    println!("          active: {}", vs.active);
    println!(" num_job_handles: {}", vs.num_job_handles);
    println!("     seen_active: {}", vs.seen_active);
    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct VolumeStatus {
    volume_id: String,
    active: String,
    num_job_handles: String,
    seen_active: String,
}

/// Runs `omdb crucible-pantry status`
async fn cmd_pantry_status(
    client: &crucible_pantry_client::Client,
) -> Result<(), anyhow::Error> {
    let status = client.pantry_status().await.context("listing volumes")?;

    println!("num_job_handles {:?}", status.num_job_handles);
    println!("Volumes found: {}", status.volumes.len());

    let mut rows = Vec::new();
    for v in &status.volumes {
        // let vs = client.volume_status(&v).await.context("listing volumes")?;
        match client.volume_status(&v).await.context("listing volumes") {
            Ok(vs) => {
                rows.push(VolumeStatus {
                    volume_id: v.clone().to_string(),
                    active: vs.active.clone().to_string(),
                    num_job_handles: vs.num_job_handles.clone().to_string(),
                    seen_active: vs.seen_active.clone().to_string(),
                });
            }
            Err(e) => {
                println!("Failed to get info for volume {v}: {e}");
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
