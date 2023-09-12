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
    #[command(subcommand)]
    command: DbCommands,
}

/// Subcommands that query or update the database
#[derive(Debug, Subcommand)]
enum DbCommands {
    /// Print information about control plane services
    Services {
        /// URL of the database SQL interface
        db_url: PostgresConfigWithUrl,
    },

    /// Print information about sleds
    Sleds {
        // XXX-dap how to commonize this?
        /// URL of the database SQL interface
        db_url: PostgresConfigWithUrl,
    },
}

impl DbArgs {
    pub async fn run_cmd(
        &self,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        match &self.command {
            DbCommands::Services { db_url } => {
                cmd_db_services(log, db_url.clone()).await
            }
            DbCommands::Sleds { db_url } => {
                cmd_db_sleds(log, db_url.clone()).await
            }
        }
    }
}

async fn cmd_db_services(
    log: &slog::Logger,
    db_url: PostgresConfigWithUrl,
) -> Result<(), anyhow::Error> {
    let db_config = db::Config { url: db_url };
    let pool = Arc::new(db::Pool::new(&log.clone(), &db_config));

    // Being a dev tool, we want this to try this operation regardless of
    // whether the schema matches what we expect.
    let datastore = Arc::new(
        DataStore::new_unchecked(pool)
            .map_err(|e| anyhow!(e).context("creating datastore"))?,
    );

    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    // XXX-dap check schema version to report a warning if no good
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
    log: &slog::Logger,
    db_url: PostgresConfigWithUrl,
) -> Result<(), anyhow::Error> {
    let db_config = db::Config { url: db_url };
    let pool = Arc::new(db::Pool::new(&log.clone(), &db_config));

    // Being a dev tool, we want this to try this operation regardless of
    // whether the schema matches what we expect.
    let datastore = Arc::new(
        DataStore::new_unchecked(pool)
            .map_err(|e| anyhow!(e).context("creating datastore"))?,
    );

    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

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
