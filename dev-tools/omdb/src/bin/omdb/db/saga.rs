// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db saga` subcommands

use crate::db::datetime_rfc3339_concise;
use anyhow::{bail, Context};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::paginated;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use std::collections::BTreeSet;
use tabled::Tabled;
use uuid::Uuid;

/// `omdb db saga` subcommand
#[derive(Debug, Args, Clone)]
pub struct SagaArgs {
    #[command(subcommand)]
    command: SagaCommands,
}

impl SagaArgs {
    pub async fn exec(
        &self,
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Result<(), anyhow::Error> {
        match &self.command {
            SagaCommands::Details(details_args) => {
                cmd_saga_details(opctx, datastore, details_args).await
            }
            SagaCommands::Running => cmd_sagas_running(opctx, datastore).await,
        }
    }
}

#[derive(Debug, Subcommand, Clone)]
enum SagaCommands {
    /// Print more information about a particular saga
    Details(SagaDetailsArgs),
    /// List running sagas
    Running,
}

/// arguments for `omdb db saga details`
#[derive(Clone, Debug, Args)]
struct SagaDetailsArgs {
    saga_id: Uuid,
}

/// `omdb db saga details`
async fn cmd_saga_details(
    opctx: &OpContext,
    datastore: &DataStore,
    details_args: &SagaDetailsArgs,
) -> Result<(), anyhow::Error> {
    let saga: nexus_db_model::Saga = {
        use db::schema::saga::dsl;
        let conn = datastore.pool_connection_for_tests().await?;
        dsl::saga
            .filter(dsl::id.eq(details_args.saga_id))
            .select(nexus_db_model::Saga::as_select())
            .first_async(&*conn)
            .await
            .context("fetching saga")?
    };

    println!("saga id: {}", saga.id);
    let details = match SagaDetails::load(opctx, datastore, &saga).await {
        Ok(details) => details,
        Err(error) => {
            eprintln!("raw record: {:?}", saga);
            return Err(error);
        }
    };

    println!("saga name: {}", details.saga_name());
    if details.stuck() {
        println!("saga state: STUCK (cached: {})", saga.saga_state.0);
    } else {
        println!("saga state: {}", saga.saga_state.0);
    }
    println!("unwinding: {}", if details.unwinding() { "yes" } else { "no " });
    println!("created: {} ({} ago)", saga.time_created, details.elapsed());
    println!("creator SEC: {}", saga.creator);
    println!(
        "current SEC: {}",
        saga.current_sec.map(|s| s.to_string()).as_deref().unwrap_or("<none>")
    );
    println!("adopt generation: {}", saga.adopt_generation);
    println!("last adopted: {}", datetime_rfc3339_concise(&saga.adopt_time));

    println!("\nACTION EXECUTION STATE");
    println!(
        "actions completed (includes failures): {}",
        details.nactions_done
    );
    println!("estimated total nodes: {}", details.estimated_total_nodes());
    println!("nodes completed: {}", details.all_nodes_done());
    println!(
        "           actions: {} (includes failures)",
        details.nactions_done
    );
    println!("      undo actions: {} (includes failures)", details.nundo_done);
    println!("nodes running: {}", details.nodes_running());
    println!("           actions: {}", details.nodes_running.len());
    for n in &details.nodes_running {
        println!("               node {} started but not finished", n);
    }
    println!("      undo actions: {}", details.undo_running.len());
    for n in &details.undo_running {
        println!("          undo node {} started but not finished", n);
    }

    println!("\nDAG INFORMATION");
    for node in details.dag.get_nodes() {
        println!(
            "node {:>4}: {} ({})",
            node.index().index(),
            node.name().as_ref(),
            node.label()
        );
    }
    println!(
        "note: start node, end node, and subsaga start nodes are not printed."
    );

    Ok(())
}

/// `omdb db saga running`
async fn cmd_sagas_running(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    // Fetch all running sagas.
    let mut sagas = Vec::new();
    let mut paginator = Paginator::new(SQL_BATCH_SIZE);
    while let Some(p) = paginator.next() {
        use db::schema::saga::dsl;
        let records_batch =
            paginated(dsl::saga, dsl::id, &p.current_pagparams())
                .filter(dsl::saga_state.eq(nexus_db_model::SagaCachedState(
                    steno::SagaCachedState::Running,
                )))
                .select(nexus_db_model::Saga::as_select())
                .load_async(&*conn)
                .await
                .context("fetching sagas")?;
        paginator =
            p.found_batch(&records_batch, &|s: &nexus_db_model::Saga| s.id);
        sagas.extend(records_batch);
    }

    // Sort them by creation time (equivalently: how long they've been running)
    sagas.sort_by_key(|s| s.time_created);
    sagas.reverse();

    // Print them out.
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SagaRow {
        id: Uuid,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        created: DateTime<Utc>,
        elap: String,
        name: String,
        state: String,
        ndone: String,
        nrun: usize,
    }

    let mut rows = Vec::with_capacity(sagas.len());
    for s in &sagas {
        let details = match SagaDetails::load(opctx, datastore, s).await {
            Ok(details) => details,
            Err(error) => {
                eprintln!(
                    "warning: failed to load details about saga {}: {:#}",
                    s.id, error,
                );
                continue;
            }
        };

        let state = if details.stuck() {
            String::from("STUCK")
        } else {
            s.saga_state.0.to_string()
        };

        rows.push(SagaRow {
            id: details.saga_id().into(),
            created: s.time_created,
            elap: details.elapsed().to_string(),
            name: details.saga_name(),
            state,
            ndone: format!(
                "{}/{}?",
                details.all_nodes_done(),
                details.estimated_total_nodes()
            ),
            nrun: details.nodes_running(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);
    Ok(())
}

/// Summarizes detailed information about a saga that requires the associated
/// saga log
struct SagaDetails<'a> {
    saga: &'a nexus_db_model::Saga,
    saga_log: steno::SagaLog,
    dag: steno::SagaDag,
    nodes_running: BTreeSet<steno::SagaNodeId>,
    undo_running: BTreeSet<steno::SagaNodeId>,
    /// formatted time since the saga started running
    elapsed: String,
    /// count of actions that have completed (successfully or not)
    nactions_done: usize,
    /// count of undo actions that have completed (successfully or not)
    nundo_done: usize,
    /// indicates that an undo action has failed
    stuck: bool,
}

impl<'a> SagaDetails<'a> {
    /// Load details about the given saga
    pub async fn load(
        opctx: &OpContext,
        datastore: &DataStore,
        s: &'a nexus_db_model::Saga,
    ) -> Result<SagaDetails<'a>, anyhow::Error> {
        let dag: steno::SagaDag = serde_json::from_value(s.saga_dag.clone())
            .context("deserializing DAG")?;

        let log_events = datastore
            .saga_fetch_log_batched(opctx, s.id)
            .await
            .context("fetching saga log")?;
        let saga_log = steno::SagaLog::new_recover(s.id.0, log_events)
            .context("loading saga log")?;

        // Walk through the saga log to compute a summary of what's running,
        // what still needs to be run, etc.
        //
        // It would be better if Steno provided more of this information for us.
        let mut stuck = false;
        let mut nodes_running = BTreeSet::new();
        let mut undo_running = BTreeSet::new();
        let mut nactions_done = 0;
        let mut nundo_done = 0;
        for event in saga_log.events() {
            match event.event_type {
                steno::SagaNodeEventType::Started => {
                    if !nodes_running.insert(event.node_id) {
                        bail!(
                            "unexpected event {:?} for node which was already
                             running",
                            event
                        );
                    }
                }
                steno::SagaNodeEventType::Succeeded(_)
                | steno::SagaNodeEventType::Failed(_) => {
                    if !nodes_running.remove(&event.node_id) {
                        bail!(
                            "unexpected event {:?} for node which did not \
                             seem to be running",
                            event
                        );
                    }
                    nactions_done += 1;
                }
                steno::SagaNodeEventType::UndoStarted => {
                    if !undo_running.insert(event.node_id) {
                        bail!(
                            "unexpected event {:?} for node which was already
                             being undone",
                            event
                        );
                    }
                }
                steno::SagaNodeEventType::UndoFinished
                | steno::SagaNodeEventType::UndoFailed(_) => {
                    if !undo_running.remove(&event.node_id) {
                        bail!(
                            "unexpected event {:?} for node which did not
                             seem to be running",
                            event
                        );
                    }
                    nundo_done += 1;
                    if matches!(
                        event.event_type,
                        steno::SagaNodeEventType::UndoFailed(_)
                    ) {
                        stuck = true;
                    }
                }
            }
        }

        // Calculate and format the elapsed time once so that it doesn't change
        // at various points while the program is running.
        let elapsed = {
            let elapsed = Utc::now() - s.time_created;
            // Drop subsecond parts of the elapsed time -- we don't need that
            // level of precision.
            let elapsed = TimeDelta::seconds(elapsed.num_seconds());
            elapsed
                .to_std()
                .map(|t| humantime::format_duration(t).to_string())
                .unwrap_or_else(|e| format!("<error: {e}>"))
        };

        Ok(SagaDetails {
            saga: s,
            saga_log,
            dag,
            nodes_running,
            undo_running,
            elapsed,
            nactions_done,
            nundo_done,
            stuck,
        })
    }

    pub fn saga_id(&self) -> steno::SagaId {
        self.saga.id.0
    }

    pub fn saga_name(&self) -> String {
        self.dag.saga_name().to_string()
    }

    pub fn all_nodes_done(&self) -> usize {
        self.nactions_done + self.nundo_done
    }

    pub fn nodes_running(&self) -> usize {
        self.nodes_running.len() + self.undo_running.len()
    }

    /// Returns an estimated count of the number of nodes that will have to
    /// finish before this saga comes to rest.
    ///
    /// This is not exact because Steno does not report some nodes (like
    /// SubsagaStart) here.
    pub fn estimated_total_nodes(&self) -> usize {
        if self.saga_log.unwinding() {
            // If the saga is unwinding, it is much harder to calculate how many
            // nodes there will be in the end.
            self.nodes_running() + self.nactions_done - self.nundo_done
        } else {
            // Add two to account for the start and end nodes, which are not
            // returned by this iterator.
            self.dag.get_nodes().count() + 2
        }
    }

    pub fn elapsed(&self) -> &str {
        &self.elapsed
    }

    pub fn stuck(&self) -> bool {
        self.stuck
    }

    pub fn unwinding(&self) -> bool {
        self.saga_log.unwinding()
    }
}
