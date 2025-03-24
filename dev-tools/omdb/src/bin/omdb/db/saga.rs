// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db saga` subcommands

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::datetime_rfc3339_concise;
use anyhow::Context;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use internal_dns_types::names::ServiceName;
use nexus_db_model::Saga;
use nexus_db_model::SagaNodeEvent;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::DataStoreConnection;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::pagination::paginated;
use std::collections::HashSet;
use std::sync::Arc;
use tabled::Tabled;
use uuid::Uuid;

use steno::ActionError;
use steno::SagaCachedState;
use steno::SagaNodeEventType;

/// OMDB's SEC id, used when inserting errors into running sagas. There should
/// be no way that regular V4 UUID creation collides with this, as the first
/// hexidecimal digit in the third group always starts with a 4 in for that
/// format.
const OMDB_SEC_UUID: Uuid =
    Uuid::from_u128(0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAu128);

/// `omdb db saga` subcommand
#[derive(Debug, Args, Clone)]
pub struct SagaArgs {
    #[command(subcommand)]
    command: SagaCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum SagaCommands {
    /// List running sagas
    Running,

    /// Inject an error into a saga's currently running node(s)
    InjectError(SagaInjectErrorArgs),
}

#[derive(Clone, Debug, Args)]
struct SagaInjectErrorArgs {
    saga_id: Uuid,

    /// Skip checking if the SEC is up
    #[clap(long, default_value_t = false)]
    bypass_sec_check: bool,
}

impl SagaArgs {
    pub async fn exec(
        &self,
        omdb: &Omdb,
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Result<(), anyhow::Error> {
        match &self.command {
            SagaCommands::Running => cmd_sagas_running(opctx, datastore).await,

            SagaCommands::InjectError(args) => {
                let token = omdb.check_allow_destructive()?;
                cmd_sagas_inject_error(omdb, opctx, datastore, args, token)
                    .await
            }
        }
    }
}

async fn cmd_sagas_running(
    _opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    let sagas = get_all_sagas_in_state(&conn, SagaCachedState::Running).await?;

    #[derive(Tabled)]
    struct SagaRow {
        id: Uuid,
        current_sec: String,
        time_created: String,
        name: String,
        state: String,
    }

    let rows: Vec<_> = sagas
        .into_iter()
        .map(|saga: Saga| SagaRow {
            id: saga.id.0.into(),
            current_sec: if let Some(current_sec) = saga.current_sec {
                current_sec.0.to_string()
            } else {
                String::from("-")
            },
            time_created: datetime_rfc3339_concise(&saga.time_created),
            name: saga.name,
            state: format!("{:?}", saga.saga_state),
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_sagas_inject_error(
    omdb: &Omdb,
    opctx: &OpContext,
    datastore: &DataStore,
    args: &SagaInjectErrorArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    // Before doing anything: find the current SEC for the saga, and ping it to
    // ensure that the Nexus is down.
    if !args.bypass_sec_check {
        let saga: Saga = {
            use db::schema::saga::dsl;
            dsl::saga
                .filter(dsl::id.eq(args.saga_id))
                .first_async(&*conn)
                .await?
        };

        match saga.current_sec {
            None => {
                // If there's no current SEC, then we don't need to check if
                // it's up. Would we see this if the saga was Requested but not
                // started?
            }

            Some(current_sec) => {
                let resolver = omdb.dns_resolver(opctx.log.clone()).await?;
                let srv = resolver.lookup_srv(ServiceName::Nexus).await?;

                let Some((target, port)) = srv
                    .iter()
                    .find(|(name, _)| name.contains(&current_sec.to_string()))
                else {
                    bail!("dns lookup for {current_sec} found nothing");
                };

                let Some(addr) = resolver.ipv6_lookup(&target).await? else {
                    bail!("dns lookup for {target} found nothing");
                };

                let client = nexus_client::Client::new(
                    &format!("http://[{addr}]:{port}/"),
                    opctx.log.clone(),
                );

                match client.ping().await {
                    Ok(_) => {
                        bail!("{current_sec} answered a ping");
                    }

                    Err(e) => match e {
                        nexus_client::Error::InvalidRequest(_)
                        | nexus_client::Error::InvalidUpgrade(_)
                        | nexus_client::Error::ErrorResponse(_)
                        | nexus_client::Error::ResponseBodyError(_)
                        | nexus_client::Error::InvalidResponsePayload(_, _)
                        | nexus_client::Error::UnexpectedResponse(_)
                        | nexus_client::Error::PreHookError(_)
                        | nexus_client::Error::PostHookError(_) => {
                            bail!("{current_sec} failed a ping with {e}");
                        }

                        nexus_client::Error::CommunicationError(_) => {
                            // Assume communication error means that it could
                            // not be contacted.
                            //
                            // Note: this could be seen if Nexus is up but
                            // unreachable from where omdb is run!
                        }
                    },
                }
            }
        }
    }

    // Find all the nodes where there is a started record but not a done record

    let started_nodes: Vec<SagaNodeEvent> = {
        use db::schema::saga_node_event::dsl;

        dsl::saga_node_event
            .filter(dsl::saga_id.eq(args.saga_id))
            .filter(dsl::event_type.eq(SagaNodeEventType::Started.label()))
            .load_async(&*conn)
            .await?
    };

    let complete_nodes: Vec<SagaNodeEvent> = {
        use db::schema::saga_node_event::dsl;

        // Note the actual enum contents don't matter in both these cases, it
        // won't affect the label string
        let succeeded_label =
            SagaNodeEventType::Succeeded(Arc::new(serde_json::Value::Null))
                .label();

        let failed_label =
            SagaNodeEventType::Failed(ActionError::InjectedError).label();

        dsl::saga_node_event
            .filter(dsl::saga_id.eq(args.saga_id))
            .filter(
                dsl::event_type
                    .eq(succeeded_label)
                    .or(dsl::event_type.eq(failed_label)),
            )
            .load_async(&*conn)
            .await?
    };

    let incomplete_nodes: HashSet<u32> = {
        let started_node_ids: HashSet<u32> =
            started_nodes.iter().map(|node| node.node_id.0.into()).collect();
        let complete_node_ids: HashSet<u32> =
            complete_nodes.iter().map(|node| node.node_id.0.into()).collect();

        started_node_ids.difference(&complete_node_ids).cloned().collect()
    };

    let incomplete_nodes: Vec<&SagaNodeEvent> = {
        let mut result = vec![];

        for node_id in incomplete_nodes {
            let Some(node) = started_nodes
                .iter()
                .find(|node| node.node_id.0 == node_id.into())
            else {
                bail!("could not find node?");
            };

            result.push(node);
        }

        result
    };

    // Inject an error for those nodes, which will cause the saga to unwind
    for node in incomplete_nodes {
        let action_error = ActionError::action_failed(String::from(
            "error injected with omdb",
        ));

        let fault = SagaNodeEvent {
            saga_id: node.saga_id,
            node_id: node.node_id,
            event_type: SagaNodeEventType::Failed(action_error.clone())
                .label()
                .to_string(),
            data: Some(serde_json::to_value(action_error)?),
            event_time: chrono::Utc::now(),
            creator: OMDB_SEC_UUID.into(),
        };

        eprintln!(
            "injecting error for saga {:?} node {:?}",
            node.saga_id, node.node_id,
        );

        {
            use db::schema::saga_node_event::dsl;

            diesel::insert_into(dsl::saga_node_event)
                .values(fault.clone())
                .execute_async(&*conn)
                .await?;
        }
    }

    Ok(())
}

// helper functions

async fn get_all_sagas_in_state(
    conn: &DataStoreConnection,
    state: SagaCachedState,
) -> Result<Vec<Saga>, anyhow::Error> {
    let mut sagas = Vec::new();
    let mut paginator = Paginator::new(SQL_BATCH_SIZE);
    while let Some(p) = paginator.next() {
        use db::schema::saga::dsl;
        let records_batch =
            paginated(dsl::saga, dsl::id, &p.current_pagparams())
                .filter(
                    dsl::saga_state.eq(nexus_db_model::SagaCachedState(state)),
                )
                .select(Saga::as_select())
                .load_async(&**conn)
                .await
                .context("fetching sagas")?;

        paginator = p.found_batch(&records_batch, &|s: &Saga| s.id);

        sagas.extend(records_batch);
    }

    // Sort them by creation time (equivalently: how long they've been running)
    sagas.sort_by_key(|s| s.time_created);
    sagas.reverse();

    Ok(sagas)
}
