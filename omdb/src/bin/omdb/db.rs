// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update the database

use anyhow::anyhow;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use nexus_db_model::Sled;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use omicron_common::postgres_config::PostgresConfigWithUrl;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use strum::IntoEnumIterator;
use tabled::Tabled;
use uuid::Uuid;

#[derive(Debug, Args)]
pub struct DbArgs {
    /// URL of the database SQL interface
    db_url: PostgresConfigWithUrl,

    /// limit to apply to queries that fetch rows
    #[clap(
        long = "fetch-limit",
        default_value_t = NonZeroU32::new(100).unwrap()
    )]
    fetch_limit: NonZeroU32,

    #[command(subcommand)]
    command: DbCommands,
}

/// Subcommands that query or update the database
#[derive(Debug, Subcommand)]
enum DbCommands {
    /// Print information about control plane services
    Services(ServicesArgs),
    /// Print information about sleds
    Sleds,
}

#[derive(Debug, Args)]
struct ServicesArgs {
    #[command(subcommand)]
    command: ServicesCommands,
}

#[derive(Debug, Subcommand)]
enum ServicesCommands {
    /// List service instances
    ListInstances,
    /// List service instances, grouped by sled
    ListBySled,
}

impl DbArgs {
    /// Run a `omdb db` subcommand.
    pub async fn run_cmd(
        &self,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let db_config = db::Config { url: self.db_url.clone() };
        let pool = Arc::new(db::Pool::new(&log.clone(), &db_config));

        // Being a dev tool, we want to try this operation even if the schema
        // doesn't match what we expect.  So we use `DataStore::new_unchecked()`
        // here.  We will then check the schema version explicitly and warn the
        // user if it doesn't match.
        let datastore = Arc::new(
            DataStore::new_unchecked(pool)
                .map_err(|e| anyhow!(e).context("creating datastore"))?,
        );
        check_schema_version(&datastore).await;

        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        match &self.command {
            DbCommands::Services(ServicesArgs {
                command: ServicesCommands::ListInstances,
            }) => {
                cmd_db_services_list_instances(
                    &opctx,
                    &datastore,
                    self.fetch_limit,
                )
                .await
            }
            DbCommands::Services(ServicesArgs {
                command: ServicesCommands::ListBySled,
            }) => {
                cmd_db_services_list_by_sled(
                    &opctx,
                    &datastore,
                    self.fetch_limit,
                )
                .await
            }
            DbCommands::Sleds => {
                cmd_db_sleds(&opctx, &datastore, self.fetch_limit).await
            }
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

/// Check the result of a query to see if it hit the given limit.  If so, warn
/// the user that our output may be incomplete and that they might try a larger
/// one.  (We don't want to bail out, though.  Incomplete data is better than no
/// data.)
fn check_limit<I, F>(items: &[I], limit: NonZeroU32, context: F)
where
    F: FnOnce() -> String,
{
    if items.len() == usize::try_from(limit.get()).unwrap() {
        eprintln!(
            "WARN: {}: found {} items (the limit).  There may be more items \
            that were ignored.  Consider overriding with --fetch-limit.",
            context(),
            items.len(),
        );
    }
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct ServiceInstanceRow {
    #[tabled(rename = "SERVICE")]
    kind: String,
    instance_id: Uuid,
    addr: String,
    sled_serial: String,
}

/// Run `omdb db services list-instances`.
async fn cmd_db_services_list_instances(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit,
    };
    let sled_list = datastore
        .sled_list(&opctx, &pagparams)
        .await
        .context("listing sleds")?;
    check_limit(&sled_list, limit, || String::from("listing sleds"));

    let sleds: BTreeMap<Uuid, Sled> =
        sled_list.into_iter().map(|s| (s.id(), s)).collect();

    let mut rows = vec![];

    for service_kind in ServiceKind::iter() {
        let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit,
        };

        let context =
            || format!("listing instances of kind {:?}", service_kind);
        let instances = datastore
            .services_list_kind(&opctx, service_kind, &pagparams)
            .await
            .with_context(&context)?;
        check_limit(&instances, limit, &context);

        rows.extend(instances.into_iter().map(|instance| {
            let addr =
                std::net::SocketAddrV6::new(*instance.ip, *instance.port, 0, 0)
                    .to_string();

            ServiceInstanceRow {
                kind: format!("{:?}", service_kind),
                instance_id: instance.id(),
                addr,
                sled_serial: sleds
                    .get(&instance.sled_id)
                    .map(|s| s.serial_number())
                    .unwrap_or("unknown")
                    .to_string(),
            }
        }));
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
struct ServiceInstanceSledRow {
    #[tabled(rename = "SERVICE")]
    kind: String,
    instance_id: Uuid,
    addr: String,
}

/// Run `omdb db services list-by-sled`.
async fn cmd_db_services_list_by_sled(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit,
    };
    let sled_list = datastore
        .sled_list(&opctx, &pagparams)
        .await
        .context("listing sleds")?;
    check_limit(&sled_list, limit, || String::from("listing sleds"));

    let sleds: BTreeMap<Uuid, Sled> =
        sled_list.into_iter().map(|s| (s.id(), s)).collect();
    let mut services_by_sled: BTreeMap<Uuid, Vec<ServiceInstanceSledRow>> =
        BTreeMap::new();

    for service_kind in ServiceKind::iter() {
        let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit,
        };

        let context =
            || format!("listing instances of kind {:?}", service_kind);
        let instances = datastore
            .services_list_kind(&opctx, service_kind, &pagparams)
            .await
            .with_context(&context)?;
        check_limit(&instances, limit, &context);

        for i in instances {
            let addr =
                std::net::SocketAddrV6::new(*i.ip, *i.port, 0, 0).to_string();
            let sled_instances =
                services_by_sled.entry(i.sled_id).or_insert_with(Vec::new);
            sled_instances.push(ServiceInstanceSledRow {
                kind: format!("{:?}", service_kind),
                instance_id: i.id(),
                addr,
            })
        }
    }

    for (sled_id, instances) in services_by_sled {
        println!(
            "sled: {} (id {})\n",
            sleds.get(&sled_id).map(|s| s.serial_number()).unwrap_or("unknown"),
            sled_id,
        );
        let table = tabled::Table::new(instances)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!("{}", textwrap::indent(&table.to_string(), "  "));
        println!("");
    }

    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct SledRow {
    serial: String,
    ip: String,
    role: &'static str,
    id: Uuid,
}

impl From<Sled> for SledRow {
    fn from(s: Sled) -> Self {
        SledRow {
            id: s.id(),
            serial: s.serial_number().to_string(),
            ip: s.address().to_string(),
            role: if s.is_scrimlet() { "scrimlet" } else { "-" },
        }
    }
}

/// Run `omdb db sleds`.
async fn cmd_db_sleds(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let pagparams: DataPageParams<'_, Uuid> = DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit,
    };

    let sleds = datastore
        .sled_list(&opctx, &pagparams)
        .await
        .context("listing sleds")?;
    check_limit(&sleds, limit, || String::from("listing sleds"));

    let rows = sleds.into_iter().map(|s| SledRow::from(s));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}
