// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update the database

use anyhow::anyhow;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use omicron_common::postgres_config::PostgresConfigWithUrl;
use std::num::NonZeroU32;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

#[derive(Debug, Args)]
pub struct DbArgs {
    /// URL of the database SQL interface
    db_url: PostgresConfigWithUrl,

    #[command(subcommand)]
    command: DbCommands,
}

/// Subcommands that query or update the database
#[derive(Debug, Subcommand)]
enum DbCommands {
    /// Print information about control plane services
    Services,

    /// Print information about sleds
    Sleds,
}

impl DbArgs {
    pub async fn run_cmd(
        &self,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let db_config = db::Config { url: self.db_url.clone() };
        // XXX-dap want a pool that fails faster
        let pool = Arc::new(db::Pool::new(&log.clone(), &db_config));

        // Being a dev tool, we want this to try this operation regardless of
        // whether the schema matches what we expect.
        let datastore = Arc::new(
            DataStore::new_unchecked(pool)
                .map_err(|e| anyhow!(e).context("creating datastore"))?,
        );

        let opctx = OpContext::for_tests(log.clone(), datastore.clone());

        check_schema_version(&datastore).await;

        match &self.command {
            DbCommands::Services => cmd_db_services(&opctx, &datastore).await,
            DbCommands::Sleds => cmd_db_sleds(&opctx, &datastore).await,
        }
    }
}

/// Check the version of the schema in the database and report whether it
/// appears to be compatible with this tool.
///
/// This is just advisory.  We will not abort if the version appears
/// incompatible because in practice it may well not matter and it's very
/// valuable for this tool to work if it possibly can.
async fn check_schema_version(datastore: &DataStore) {
    let expected_version = nexus_db_model::schema::SCHEMA_VERSION;
    let version_check = datastore.database_schema_version().await;

    match version_check {
        Ok(found_version) => {
            if found_version == expected_version {
                eprintln!(
                    "note: schema version matches expected ({})",
                    expected_version
                );
                return;
            }

            eprintln!(
                "WARN: found schema version {}, expected {}",
                found_version, expected_version
            );
        }
        Err(error) => {
            eprintln!("WARN: failed to query schema version: {:#}", error);
        }
    };

    eprintln!(
        "{}",
        textwrap::fill(
            "It's possible the database is running a version that's different \
            from what this tool understands.  This may result in errors or \
            incorrect output.",
            80
        )
    );
}

async fn cmd_db_services(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // XXX-dap check that we haven't hit the hardcoded limit here
    // XXX-dap join with sled information for a by-sled view

    for service_kind in ServiceKind::iter() {
        print!("SERVICE: {:?}", service_kind);

        let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };

        let instances = datastore
            .services_list_kind(&opctx, service_kind, &pagparams)
            .await
            .with_context(|| {
                format!("listing instances of kind {:?}", service_kind)
            })?;
        print!(" (instances: {})\n", instances.len());

        for i in instances {
            println!(
                "  IP {} id {} sled {} zone {}",
                *i.ip,
                i.id(),
                i.sled_id,
                i.zone_id
                    .map(|z| z.to_string())
                    .unwrap_or_else(|| String::from("(none)"))
            );
        }

        println!("");
    }

    Ok(())
}

// XXX-dap commonize
async fn cmd_db_sleds(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // XXX-dap check schema version to report a warning if no good
    // XXX-dap check that we haven't hit the hardcoded limit here

    let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit: NonZeroU32::new(100).unwrap(),
    };

    let sleds = datastore
        .sled_list(&opctx, &pagparams)
        .await
        .context("listing sleds")?;

    for s in sleds {
        print!("sled {} IP {}", s.id(), s.ip());
        if s.is_scrimlet() {
            println!(" (scrimlet)");
        } else {
            println!("");
        }
    }

    println!("");

    Ok(())
}
