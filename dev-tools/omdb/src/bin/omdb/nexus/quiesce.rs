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
use nexus_client::types::QuiesceStatus;
use nexus_client::types::SagaQuiesceStatus;
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
pub struct QuiesceShowArgs {
    /// Show stack traces for held database connections
    #[clap(short, long, default_value_t = false)]
    stacks: bool,
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

    let QuiesceStatus { db_claims, sagas, state } = quiesce;

    match state {
        QuiesceState::Undetermined => {
            println!("has not yet determined if it is quiescing");
        }
        QuiesceState::Running => {
            println!("running normally (not quiesced, not quiescing)");
        }
        QuiesceState::DrainingSagas { time_requested } => {
            println!(
                "quiescing since {} ({} ago)",
                humantime::format_rfc3339_millis(time_requested.into()),
                format_time_delta(now - time_requested),
            );
            println!("details: waiting for running sagas to finish");
        }
        QuiesceState::DrainingDb {
            time_requested,
            duration_draining_sagas,
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
                format_duration_ms(duration_draining_sagas.into()),
            );
        }
        QuiesceState::RecordingQuiesce {
            time_requested,
            duration_draining_sagas,
            duration_draining_db,
            ..
        } => {
            println!(
                "quiescing since {} ({} ago)",
                humantime::format_rfc3339_millis(time_requested.into()),
                format_time_delta(now - time_requested),
            );
            println!(
                "    waiting for sagas took {}",
                format_duration_ms(duration_draining_sagas.into()),
            );
            println!(
                "    waiting for db quiesce took {}",
                format_duration_ms(duration_draining_db.into()),
            );
        }
        QuiesceState::Quiesced {
            time_quiesced,
            duration_draining_sagas,
            duration_draining_db,
            duration_recording_quiesce,
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
                format_duration_ms(duration_draining_sagas.into()),
            );
            println!(
                "    waiting for db quiesce took {}",
                format_duration_ms(duration_draining_db.into()),
            );
            println!(
                "    recording quiesce took {}",
                format_duration_ms(duration_recording_quiesce.into()),
            );
            println!(
                "    total quiesce time: {}",
                format_duration_ms(duration_total.into()),
            );
        }
    }

    let SagaQuiesceStatus {
        sagas_pending,
        drained_blueprint_id,
        first_recovery_complete,
        new_sagas_allowed,
        reassignment_blueprint_id,
        reassignment_generation,
        reassignment_pending,
        recovered_blueprint_id,
        recovered_reassignment_generation,
    } = sagas;

    println!("saga quiesce:");
    println!("    new sagas: {:?}", new_sagas_allowed);
    println!(
        "    drained as of blueprint: {}",
        drained_blueprint_id
            .map(|s| s.to_string())
            .as_deref()
            .unwrap_or("none")
    );
    println!(
        "    blueprint for last recovery pass: {}",
        recovered_blueprint_id
            .map(|s| s.to_string())
            .as_deref()
            .unwrap_or("none")
    );
    println!(
        "    blueprint for last reassignment pass: {}",
        reassignment_blueprint_id
            .map(|s| s.to_string())
            .as_deref()
            .unwrap_or("none")
    );
    println!(
        "    reassignment generation: {} (pass running: {})",
        reassignment_generation,
        if reassignment_pending { "yes" } else { "no" }
    );
    println!("    recovered generation: {}", recovered_reassignment_generation);
    println!(
        "    recovered at least once successfully: {}",
        if first_recovery_complete { "yes" } else { "no" },
    );

    println!("    sagas running: {}", sagas_pending.len());
    for saga in &sagas_pending {
        println!(
            "        saga {} pending since {} ({})",
            saga.saga_id,
            humantime::format_rfc3339_millis(saga.time_pending.into()),
            saga.saga_name
        );
    }

    println!("database connections held: {}", db_claims.len());
    for claim in &db_claims {
        println!(
            "    claim {} held since {} ({} ago)",
            claim.id,
            claim.held_since,
            format_time_delta(Utc::now() - claim.held_since),
        );
        if args.stacks {
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
    quiesce_show(client, &QuiesceShowArgs { stacks: false }).await
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
