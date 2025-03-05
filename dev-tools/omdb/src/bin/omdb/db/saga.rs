// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db saga` subcommands

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::datetime_rfc3339_concise;
use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use nexus_db_model::Saga;
use nexus_db_model::SagaNodeEvent;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::DataStoreConnection;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::pagination::paginated;
use tabled::Tabled;
use uuid::Uuid;

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

    /// Inject an error into a saga's currently running node
    Fault(SagaFaultArgs),
}

#[derive(Clone, Debug, Args)]
struct SagaFaultArgs {
    saga_id: Uuid,
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

            SagaCommands::Fault(args) => {
                let token = omdb.check_allow_destructive()?;
                cmd_sagas_fault(opctx, datastore, args, token).await
            }
        }
    }
}

async fn cmd_sagas_running(
    _opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    let sagas =
        get_all_sagas_in_state(&conn, steno::SagaCachedState::Running).await?;

    #[derive(Tabled)]
    struct SagaRow {
        id: Uuid,
        creator_id: Uuid,
        time_created: String,
        name: String,
        state: String,
    }

    let rows: Vec<_> = sagas
        .into_iter()
        .map(|saga: Saga| SagaRow {
            id: saga.id.0.into(),
            creator_id: saga.creator.0,
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

async fn cmd_sagas_fault(
    _opctx: &OpContext,
    datastore: &DataStore,
    args: &SagaFaultArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    // Find the most recent node for a given saga
    let most_recent_node: SagaNodeEvent = {
        use db::schema::saga_node_event::dsl;

        dsl::saga_node_event
            .filter(dsl::saga_id.eq(args.saga_id))
            .order(dsl::event_time.desc())
            .limit(1)
            .first_async(&*conn)
            .await?
    };

    // Inject a fault for that node, which will cause the saga to unwind
    let action_error = steno::ActionError::action_failed(String::from(
        "error injected with omdb",
    ));

    let fault = SagaNodeEvent {
        saga_id: most_recent_node.saga_id,
        node_id: most_recent_node.node_id,
        event_type: steno::SagaNodeEventType::Failed(action_error.clone())
            .label()
            .to_string(),
        data: Some(serde_json::to_value(action_error)?),
        event_time: chrono::Utc::now(),
        creator: most_recent_node.creator,
    };

    {
        use db::schema::saga_node_event::dsl;

        diesel::insert_into(dsl::saga_node_event)
            .values(fault.clone())
            .execute_async(&*conn)
            .await?;
    }

    Ok(())
}

// helper functions

async fn get_all_sagas_in_state(
    conn: &DataStoreConnection,
    state: steno::SagaCachedState,
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
