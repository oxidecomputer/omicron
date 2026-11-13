// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that repair bad database state (as best as possible!)
//!
//! Bugs can cause corrupted database state. One option as part of writing the
//! fix into Nexus is to also write routines that automatically detect and
//! repair that corrupted state, but sometimes this isn't possible: an example
//! of this is when the fix requires the system to remain up to preserve some
//! transient state, where an update may bounce hosts or zones and lose that
//! state.
//!
//! It may also not make sense to teach Nexus about how a certain state can get
//! corrupted due to that state only being produced as a result of an error.
//! Building the repair into Nexus means it would need to indefinitely check for
//! the corruption even though that corruption may never exist in the same shape
//! again after the fix.
//!
//! These commands attempt to detact and / or repair the bad database state as
//! best as possible. In cases where it's not possible to execute this repair,
//! all we can do is print as much information about the consequences as
//! possible - manual intervention is required.

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use anyhow::Result;
use anyhow::bail;
use clap::Args;
use clap::Subcommand;
use nexus_db_queries::db::DataStore;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

mod common;
mod cs1184;
mod cs1321;

/// Arguments to the "omdb repair-db" subcommand
#[derive(Clone, Debug, Args)]
pub struct RepairDbArgs {
    #[command(subcommand)]
    command: RepairDbCommands,
}

/// Subcommands for the "omdb repair-db" subcommand
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Subcommand)]
enum RepairDbCommands {
    /// Detect the corruption caused by each known issue
    DetectAll(DetectAllArgs),

    /// Detect or repair duplicate local storage allocations
    Cs1184(Cs1184SubCommand),

    /// Detect or repair zombie local storage allocations
    Cs1321(Cs1321SubCommand),
}

#[derive(Clone, Debug, Args)]
pub struct DetectAllArgs {
    /// Only show the detected issues, not the detail messages
    #[arg(long, short, default_value_t = false)]
    issue_only: bool,
}

#[derive(Clone, Debug, Args)]
pub struct Cs1184SubCommand {
    #[command(subcommand)]
    command: Cs1184Args,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Cs1184Args {
    Detect,

    /// Automatically garbage collect all orphaned allocations that were
    /// definitely never used with instances
    GarbageCollectNeverUsed,

    /// Automatically garbage collect all orphaned allocations that can easily
    /// be dealt with.
    GarbageCollectAuto,

    /// Garbage collect an orphaned allocation, which may require manual steps
    GarbageCollect {
        #[arg(long, short)]
        allocation_id: Uuid,
    },
}

#[derive(Clone, Debug, Args)]
pub struct Cs1321SubCommand {
    #[command(subcommand)]
    command: Cs1321Args,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Cs1321Args {
    Detect,

    GarbageCollectAuto,
}

impl RepairDbArgs {
    /// Run a `omdb repair-db` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        datastore: &Arc<DataStore>,
        log: &slog::Logger,
    ) -> Result<()> {
        match &self.command {
            RepairDbCommands::DetectAll(args) => {
                cmd_repair_db_detect_all(datastore, log, args).await
            }

            RepairDbCommands::Cs1184(Cs1184SubCommand {
                command: Cs1184Args::Detect,
            }) => cmd_repair_db_cs1184_detect(datastore, log).await,

            RepairDbCommands::Cs1184(Cs1184SubCommand {
                command: Cs1184Args::GarbageCollectNeverUsed,
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_repair_db_cs1184_gc_never_used_allocations(
                    datastore, log, token,
                )
                .await
            }

            RepairDbCommands::Cs1184(Cs1184SubCommand {
                command: Cs1184Args::GarbageCollectAuto,
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_repair_db_cs1184_gc_auto(datastore, log, token).await
            }

            RepairDbCommands::Cs1184(Cs1184SubCommand {
                command: Cs1184Args::GarbageCollect { allocation_id },
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_repair_db_cs1184_gc_allocation(
                    datastore,
                    log,
                    token,
                    allocation_id,
                )
                .await
            }

            RepairDbCommands::Cs1321(Cs1321SubCommand {
                command: Cs1321Args::Detect,
            }) => cmd_repair_db_cs1321_detect(datastore, log).await,

            RepairDbCommands::Cs1321(Cs1321SubCommand {
                command: Cs1321Args::GarbageCollectAuto,
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_repair_db_cs1321_gc_auto(datastore, log, token).await
            }
        }
    }
}

pub struct CorruptedStateInfo {
    pub message: String,
}

#[async_trait::async_trait]
trait DatabaseCorruption {
    async fn new(datastore: Arc<DataStore>, log: &Logger) -> Self;

    async fn detect(&self) -> Result<Vec<CorruptedStateInfo>>;
}

/// Print any information related to corrupted database state. If nothing is
/// detected, then print nothing. Returns true if corrupted database state is
/// found, false otherwise.
async fn show_detected_issues(
    show_issue_only: bool,
    name: &'static str,
    dc: impl DatabaseCorruption,
) -> Result<bool> {
    let issues = dc.detect().await?;

    if issues.is_empty() {
        return Ok(false);
    }

    if show_issue_only {
        println!("{name}");
    } else {
        println!("{name}: ");

        for issue in issues {
            println!("\t{}", issue.message);
        }
    }

    Ok(true)
}

/// Detect any known database corruption, and print details if some is found
async fn cmd_repair_db_detect_all(
    datastore: &Arc<DataStore>,
    log: &Logger,
    args: &DetectAllArgs,
) -> Result<()> {
    let mut corruption_found = false;

    // The name argument here should equal the associated sub command

    corruption_found |= show_detected_issues(
        args.issue_only,
        "cs1184",
        cs1184::OrphanedLocalStorageAllocations::new(datastore.clone(), log)
            .await,
    )
    .await?;

    corruption_found |= show_detected_issues(
        args.issue_only,
        "cs1321",
        cs1321::ZombieLocalStorageAllocations::new(datastore.clone(), log)
            .await,
    )
    .await?;

    // If any corruption is found, return a non-zero exit code so scripts can
    // trigger off this.

    if corruption_found {
        anyhow::bail!("database has corrupted state");
    } else {
        Ok(())
    }
}

async fn cmd_repair_db_cs1184_detect(
    datastore: &Arc<DataStore>,
    log: &Logger,
) -> Result<()> {
    let issue =
        cs1184::OrphanedLocalStorageAllocations::new(datastore.clone(), log)
            .await;

    let issues = issue.detect().await?;

    if issues.is_empty() {
        return Ok(());
    }

    for issue in issues {
        println!("{}", issue.message);
    }

    bail!("orphaned local storage allocations found")
}

async fn cmd_repair_db_cs1184_gc_never_used_allocations(
    datastore: &Arc<DataStore>,
    log: &Logger,
    token: DestructiveOperationToken,
) -> Result<()> {
    let issue =
        cs1184::OrphanedLocalStorageAllocations::new(datastore.clone(), log)
            .await;

    issue.garbage_collect_never_used_allocations(token).await
}

async fn cmd_repair_db_cs1184_gc_auto(
    datastore: &Arc<DataStore>,
    log: &Logger,
    token: DestructiveOperationToken,
) -> Result<()> {
    let issue =
        cs1184::OrphanedLocalStorageAllocations::new(datastore.clone(), log)
            .await;

    let orphaned_record_ids =
        issue.find_orphaned_local_storage_allocations().await?;

    for orphaned_record_id in orphaned_record_ids {
        if let Err(e) = issue
            .garbage_collect_allocation(&token, orphaned_record_id, false)
            .await
        {
            println!(
                r#"
garbage collecting allocation {orphaned_record_id} may need an interactive
session: {e}"#
            );
        }
    }

    Ok(())
}

async fn cmd_repair_db_cs1184_gc_allocation(
    datastore: &Arc<DataStore>,
    log: &Logger,
    token: DestructiveOperationToken,
    allocation_id: &Uuid,
) -> Result<()> {
    let issue =
        cs1184::OrphanedLocalStorageAllocations::new(datastore.clone(), log)
            .await;

    issue.garbage_collect_allocation(&token, *allocation_id, true).await
}

async fn cmd_repair_db_cs1321_detect(
    datastore: &Arc<DataStore>,
    log: &Logger,
) -> Result<()> {
    let issue =
        cs1321::ZombieLocalStorageAllocations::new(datastore.clone(), log)
            .await;

    let issues = issue.detect().await?;

    if issues.is_empty() {
        return Ok(());
    }

    for issue in issues {
        println!("{}", issue.message);
    }

    bail!("zombie local storage allocations found")
}

async fn cmd_repair_db_cs1321_gc_auto(
    datastore: &Arc<DataStore>,
    log: &Logger,
    token: DestructiveOperationToken,
) -> Result<()> {
    let issue =
        cs1321::ZombieLocalStorageAllocations::new(datastore.clone(), log)
            .await;

    issue.garbage_collect_auto(token).await?;

    Ok(())
}
