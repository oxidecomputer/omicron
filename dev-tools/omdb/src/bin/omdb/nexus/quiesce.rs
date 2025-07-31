// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands for managing Nexus quiesce state

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use anyhow::Context;
use chrono::TimeDelta;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use nexus_client::types::QuiesceState;
use std::time::Duration;

#[derive(Debug, Args)]
pub struct QuiesceArgs {
    #[command(subcommand)]
    command: QuiesceCommands,
}

#[derive(Debug, Subcommand)]
pub enum QuiesceCommands {
    /// Show the current Nexus quiesce status
    Show(QuiesceShowArgs),

    /// Start quiescing Nexus
    Start,
}

#[derive(Debug, Args)]
struct QuiesceShowArgs {
    /// Show details about held database connections
    #[clap(short, long, default_value_t = false)]
    verbose: bool,
}

pub async fn cmd_nexus_quiesce(
    omdb: &Omdb,
    client: &nexus_client::Client,
    args: &QuiesceArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        QuiesceCommands::Show(args) => quiesce_show(&client, args).await,
        QuiesceCommands::Start => {
            let token = omdb.check_allow_destructive()?;
            quiesce_start(&client, token).await
        }
    }
}

async fn quiesce_show(
    client: &nexus_client::Client,
    args: &QuiesceShowArgs,
) -> Result<(), anyhow::Error> {
    let now = Utc::now();
    let quiesce = client
        .quiesce_get()
        .await
        .context("fetching quiesce state")?
        .into_inner();
    match quiesce.state {
        QuiesceState::Running => {
            println!("running normally (not quiesced, not quiescing)");
        }
        QuiesceState::WaitingForSagas { time_requested } => {
            println!(
                "quiescing since {} ({} ago)",
                humantime::format_rfc3339_millis(time_requested.into()),
                format_time_delta(now - time_requested),
            );
            println!("details: waiting for running sagas to finish");
        }
        QuiesceState::WaitingForDb {
            time_requested,
            duration_waiting_for_sagas,
            ..
        } => {
            println!(
                "quiescing since {} ({} ago)",
                humantime::format_rfc3339_millis(time_requested.into()),
                format_time_delta(now - time_requested),
            );
            println!(
                "details: waiting for database connections to be released"
            );
            println!(
                "    previously: waiting for sagas took {}",
                format_duration_ms(duration_waiting_for_sagas.into()),
            );
        }
        QuiesceState::Quiesced {
            time_quiesced,
            duration_waiting_for_sagas,
            duration_waiting_for_db,
            duration_total,
            ..
        } => {
            println!(
                "quiesced since {} ({} ago)",
                humantime::format_rfc3339_millis(time_quiesced.into()),
                format_time_delta(now - time_quiesced),
            );
            println!(
                "    waiting for sagas took {}",
                format_duration_ms(duration_waiting_for_sagas.into()),
            );
            println!(
                "    waiting for db quiesce took {}",
                format_duration_ms(duration_waiting_for_db.into()),
            );
            println!(
                "    total quiesce time: {}",
                format_duration_ms(duration_total.into()),
            );
        }
    }

    println!("sagas running: {}", quiesce.sagas_running.len());
    for saga in &quiesce.sagas_running {
        println!(
            "    saga {} started at {} ({})",
            saga.saga_id,
            humantime::format_rfc3339_millis(saga.time_started.into()),
            saga.saga_name
        );
    }

    println!("database connections held: {}", quiesce.db_claims.len());
    for claim in &quiesce.db_claims {
        println!(
            "    claim {} held since {} ({} ago)",
            claim.id,
            claim.held_since,
            format_time_delta(Utc::now() - claim.held_since),
        );
        if args.verbose {
            println!("    acquired by:");
            println!("{}", textwrap::indent(&claim.debug, "        "));
        }
    }

    Ok(())
}

async fn quiesce_start(
    client: &nexus_client::Client,
    _token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    client.quiesce_start().await.context("quiescing Nexus")?;
    quiesce_show(client, &QuiesceShowArgs { verbose: false }).await
}

fn format_duration_ms(duration: Duration) -> String {
    // Ignore units smaller than a millisecond.
    let elapsed = Duration::from_millis(
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
    );
    humantime::format_duration(elapsed).to_string()
}

fn format_time_delta(time_delta: TimeDelta) -> String {
    match time_delta.to_std() {
        Ok(d) => format_duration_ms(d),
        Err(_) => String::from("<time delta out of range>"),
    }
}
