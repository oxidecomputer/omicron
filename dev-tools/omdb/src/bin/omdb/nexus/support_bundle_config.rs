// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands for support bundle auto-deletion configuration

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::DbUrlOptions;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use std::sync::Arc;

#[derive(Debug, Args)]
pub struct SupportBundleConfigArgs {
    #[clap(flatten)]
    db_url_opts: DbUrlOptions,

    #[command(subcommand)]
    command: SupportBundleConfigCommands,
}

#[derive(Debug, Subcommand)]
pub enum SupportBundleConfigCommands {
    /// Show current support bundle auto-deletion config
    Show,

    /// Set support bundle auto-deletion config
    Set(SupportBundleConfigSetArgs),
}

#[derive(Debug, Clone, Args)]
pub struct SupportBundleConfigSetArgs {
    /// Target percentage of datasets to keep free (0-100)
    #[clap(long)]
    target_free_percent: Option<u8>,

    /// Minimum percentage of datasets to keep as bundles (0-100)
    #[clap(long)]
    min_keep_percent: Option<u8>,
}

pub async fn cmd_nexus_support_bundle_config(
    omdb: &Omdb,
    log: &slog::Logger,
    args: &SupportBundleConfigArgs,
) -> Result<(), anyhow::Error> {
    let datastore = args.db_url_opts.connect(omdb, log).await?;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    let result = match &args.command {
        SupportBundleConfigCommands::Show => {
            support_bundle_config_show(&opctx, &datastore).await
        }
        SupportBundleConfigCommands::Set(set_args) => {
            let token = omdb.check_allow_destructive()?;
            support_bundle_config_set(&opctx, &datastore, set_args, token).await
        }
    };

    datastore.terminate().await;
    result
}

async fn support_bundle_config_show(
    opctx: &OpContext,
    datastore: &Arc<DataStore>,
) -> Result<(), anyhow::Error> {
    let config = datastore
        .support_bundle_config_get(opctx)
        .await
        .context("failed to get support bundle config")?;

    println!("Support Bundle Auto-Deletion Config:");
    println!(
        "  Target free datasets: {}% (CEIL calculation)",
        config.target_free_percent
    );
    println!(
        "  Minimum bundles to keep: {}% (CEIL calculation)",
        config.min_keep_percent
    );
    println!("  Last modified: {}", config.time_modified);

    Ok(())
}

async fn support_bundle_config_set(
    opctx: &OpContext,
    datastore: &Arc<DataStore>,
    args: &SupportBundleConfigSetArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    // Get current config
    let current = datastore
        .support_bundle_config_get(opctx)
        .await
        .context("failed to get current support bundle config")?;

    // Apply changes, using current values as defaults
    let new_target_free =
        args.target_free_percent.unwrap_or(current.target_free_percent as u8);
    let new_min_keep =
        args.min_keep_percent.unwrap_or(current.min_keep_percent as u8);

    // Check if anything changed
    if i64::from(new_target_free) == current.target_free_percent
        && i64::from(new_min_keep) == current.min_keep_percent
    {
        println!("No changes to current config:");
        println!(
            "  Target free datasets: {}% (CEIL calculation)",
            current.target_free_percent
        );
        println!(
            "  Minimum bundles to keep: {}% (CEIL calculation)",
            current.min_keep_percent
        );
        return Ok(());
    }

    // Apply the update
    datastore
        .support_bundle_config_set(opctx, new_target_free, new_min_keep)
        .await
        .context("failed to set support bundle config")?;

    println!("Support bundle config updated:");
    if i64::from(new_target_free) != current.target_free_percent {
        println!(
            "  Target free datasets: {}% -> {}%",
            current.target_free_percent, new_target_free
        );
    } else {
        println!("  Target free datasets: {}% (unchanged)", new_target_free);
    }
    if i64::from(new_min_keep) != current.min_keep_percent {
        println!(
            "  Minimum bundles to keep: {}% -> {}%",
            current.min_keep_percent, new_min_keep
        );
    } else {
        println!("  Minimum bundles to keep: {}% (unchanged)", new_min_keep);
    }

    Ok(())
}
