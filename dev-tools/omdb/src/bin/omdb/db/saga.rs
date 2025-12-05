// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db saga` subcommands

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::datetime_rfc3339_concise;
use crate::helpers::ConfirmationPrompt;
use crate::helpers::should_colorize;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use internal_dns_resolver::ResolveError;
use internal_dns_types::names::ServiceName;
use nexus_db_lookup::DataStoreConnection;
use nexus_db_model::Saga;
use nexus_db_model::SagaNodeEvent;
use nexus_db_model::SagaState;
use nexus_db_model::SecId;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::pagination::paginated;
use owo_colors::OwoColorize;
use petgraph::Graph;
use petgraph::graph::NodeIndex;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use tabled::Tabled;
use uuid::Uuid;

use steno::ActionError;
use steno::SagaNodeEventType;

/// OMDB's SEC id, used when inserting errors into running sagas. There should
/// be no way that regular V4 UUID creation collides with this, because in a
/// valid V4 UUID the first hex digit in the third group always starts with a 4.
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

    /// Prevent new Nexus processes from resuming execution of a saga.
    ///
    /// On startup, and periodically thereafter, each Nexus checks the database
    /// for sagas that are assigned to that Nexus that it is not currently
    /// executing so that it can begin to execute them. Abandoning a saga causes
    /// it never to appear in a recovery candidate set: it will be ignored by
    /// any Nexus that asks for a list of sagas to resume executing.
    ///
    /// WARNING: It is best to use the `running` command to identify the saga's
    /// current executor and verify that it is offline before proceeding. If the
    /// saga's assigned Nexus is running, it can continue executing the saga and
    /// may clobber the Abandoned state. By default, this subcommand verifies
    /// that the saga's assigned Nexus is unreachable before proceeding, but
    /// while all inactive Nexuses are unreachable, an unreachable Nexus is not
    /// necessarily inactive.
    Abandon(SagaAbandonArgs),

    /// Show the execution of a saga
    Show(SagaShowArgs),
}

#[derive(Clone, Debug, Args)]
struct SagaInjectErrorArgs {
    saga_id: Uuid,

    /// Skip checking if the SEC is up
    #[clap(long, default_value_t = false)]
    bypass_sec_check: bool,
}

#[derive(Clone, Copy, Debug, Args)]
struct SagaAbandonArgs {
    saga_id: Uuid,

    /// Skip checking if the SEC is up
    #[clap(long, default_value_t = false)]
    bypass_sec_check: bool,
}

#[derive(Clone, Copy, Debug, Args)]
struct SagaShowArgs {
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

            SagaCommands::InjectError(args) => {
                let token = omdb.check_allow_destructive()?;
                cmd_sagas_inject_error(omdb, opctx, datastore, args, token)
                    .await
            }

            SagaCommands::Abandon(args) => {
                let token = omdb.check_allow_destructive()?;
                cmd_sagas_abandon(omdb, opctx, datastore, *args, token).await
            }

            SagaCommands::Show(args) => {
                cmd_sagas_show(omdb, opctx, datastore, *args).await
            }
        }
    }
}

async fn cmd_sagas_running(
    _opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    let sagas = get_all_sagas_in_state(&conn, SagaState::Running).await?;

    let rows: Vec<_> = sagas.into_iter().map(SagaRow::from).collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .to_string();

    println!("{}", table);

    Ok(())
}

#[derive(Tabled)]
struct SagaRow {
    id: Uuid,
    current_sec: String,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    time_created: DateTime<Utc>,
    name: String,
    state: String,
}

impl From<Saga> for SagaRow {
    fn from(saga: Saga) -> Self {
        let Saga {
            id,
            creator: _,
            time_created,
            name,
            saga_dag: _,
            saga_state,
            current_sec,
            adopt_generation: _,
            adopt_time: _,
        } = saga;
        Self {
            id: id.0.into(),
            current_sec: if let Some(current_sec) = current_sec {
                current_sec.0.to_string()
            } else {
                String::from("-")
            },
            time_created,
            name,
            state: format!("{saga_state:?}"),
        }
    }
}

async fn cmd_sagas_inject_error(
    omdb: &Omdb,
    opctx: &OpContext,
    datastore: &DataStore,
    args: &SagaInjectErrorArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let should_print_color =
        should_colorize(omdb.output.color, supports_color::Stream::Stdout);
    let conn = datastore.pool_connection_for_tests().await?;

    // Add a confirmation prompt reminding the caller of the risks of this
    // injection

    let text = r#"
WARNING: Injecting an error into a saga will (hopefully) cause it to be
unwound, but if the actions into which errors are injected have taken effect,
those effects will not be undone. This can result in corruption of control
plane state, even if the Nexus assigned to this saga is not currently running.
You should only do this if:

- you've stopped Nexus and then verified that the currently-running nodes
  either have no side effects, have not made any changes to the system, or
  you've already undone them by hand

- this is a development system whose state can be wiped
"#;

    if should_print_color {
        println!("{}", text.red().bold());
    } else {
        println!("{text}");
    }

    // Before doing anything: find the current SEC for the saga, and ping it to
    // ensure that the Nexus is down.
    if !args.bypass_sec_check {
        let saga: Saga = {
            use nexus_db_schema::schema::saga::dsl;
            dsl::saga
                .filter(dsl::id.eq(args.saga_id))
                .first_async(&*conn)
                .await?
        };

        let status = get_saga_sec_status(omdb, opctx, &saga).await;
        status.display_message(should_print_color);
        status.into_result()?;
    } else {
        let text = "Skipping check of whether the Nexus assigned to this saga \
        is running. If this Nexus is running, the control plane state managed \
        by this saga may become corrupted!";

        if should_print_color {
            println!("{}", text.red().bold());
        } else {
            println!("{text}");
        }
    }

    // Before making any changes, ask for confirmation

    let mut prompt = ConfirmationPrompt::new();
    prompt.read_and_validate("y/N", "y")?;
    drop(prompt);

    // Find all the nodes where there is a started record but not a done record

    let started_nodes: Vec<SagaNodeEvent> = {
        use nexus_db_schema::schema::saga_node_event::dsl;

        dsl::saga_node_event
            .filter(dsl::saga_id.eq(args.saga_id))
            .filter(dsl::event_type.eq(SagaNodeEventType::Started.label()))
            .load_async(&*conn)
            .await?
    };

    let complete_nodes: Vec<SagaNodeEvent> = {
        use nexus_db_schema::schema::saga_node_event::dsl;

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
            // SAFETY: this unwrap is ok because incomplete_nodes will always
            // contain a subset of entries from started_nodes.
            let node = started_nodes
                .iter()
                .find(|node| node.node_id.0 == node_id.into())
                .unwrap();

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

        println!(
            "injecting error for saga {:?} node {:?}",
            node.saga_id, node.node_id,
        );

        {
            use nexus_db_schema::schema::saga_node_event::dsl;

            diesel::insert_into(dsl::saga_node_event)
                .values(fault.clone())
                .execute_async(&*conn)
                .await?;
        }
    }

    Ok(())
}

async fn cmd_sagas_abandon(
    omdb: &Omdb,
    opctx: &OpContext,
    datastore: &DataStore,
    SagaAbandonArgs { saga_id, bypass_sec_check }: SagaAbandonArgs,
    _destruction_token: DestructiveOperationToken,
) -> anyhow::Result<()> {
    use nexus_db_schema::schema::saga::dsl;

    let should_print_color =
        should_colorize(omdb.output.color, supports_color::Stream::Stdout);
    let conn = datastore.pool_connection_for_tests().await?;
    let saga: Saga =
        { dsl::saga.filter(dsl::id.eq(saga_id)).first_async(&*conn).await? };

    match saga.saga_state {
        SagaState::Done => {
            bail!("saga {saga_id} is already done executing");
        }
        SagaState::Abandoned => {
            bail!("saga {saga_id} is already abandoned");
        }
        SagaState::Running | SagaState::Unwinding => {}
    }

    let text = r#"
WARNING: Marking a saga as abandoned prevents it from running in the following
circumstances:

- If the saga is assigned to a Nexus that is not running, and that Nexus starts,
  it will not resume executing the saga.

- Other Nexuses will not adopt and resume the saga, even if its current assigned
  Nexus is expunged.

If the saga's current Nexus is actively driving it, the saga will continue to
execute even if it is abandoned. You should only proceed if:

- you've stopped the saga's assigned Nexus AND are prepared to undo any changes
  the saga may already have made to the system, or

- this is a development system whose state can be wiped.
    "#;

    if should_print_color {
        println!("{}", text.red().bold());
    } else {
        println!("{text}");
    }

    // Before doing anything: find the current SEC for the saga, and ping it to
    // ensure that the Nexus is down.
    if !bypass_sec_check {
        let saga: Saga = {
            dsl::saga.filter(dsl::id.eq(saga_id)).first_async(&*conn).await?
        };

        let status = get_saga_sec_status(omdb, opctx, &saga).await;
        status.display_message(should_print_color);
        status.into_result()?;
    } else {
        let text = "Skipping check of whether the Nexus assigned to this saga \
        is running. If this Nexus is running, the saga may continue executing!";

        if should_print_color {
            println!("{}", text.red().bold());
        } else {
            println!("{text}");
        }
    }

    let mut prompt = ConfirmationPrompt::new();
    prompt.read_and_validate("y/N", "y")?;
    drop(prompt);

    diesel::update(dsl::saga)
        .filter(dsl::id.eq(saga_id))
        .set(dsl::saga_state.eq(SagaState::Abandoned))
        .execute_async(&*conn)
        .await?;

    Ok(())
}

// helper functions

async fn get_all_sagas_in_state(
    conn: &DataStoreConnection,
    state: SagaState,
) -> Result<Vec<Saga>, anyhow::Error> {
    let mut sagas = Vec::new();
    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        use nexus_db_schema::schema::saga::dsl;
        let records_batch =
            paginated(dsl::saga, dsl::id, &p.current_pagparams())
                .filter(dsl::saga_state.eq(state))
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

/// The outcome of an attempt to ascertain the status of a saga's execution
/// coordinator.
enum SagaSecStatus {
    NoSecAssigned,
    DnsResolverUnavailable(anyhow::Error),
    NexusResolutionFailed(ResolveError),
    NoDnsServiceRecord(SecId),
    DnsIpv6LookupFailed { target: String, error: ResolveError },
    NoAddressFromDns(String),
    SecAnsweredPing(SecId),
    SecPingError { sec_id: SecId, observed_error: String },
    SecAppearsInactive { sec_id: SecId, observed_error: String },
}

impl SagaSecStatus {
    /// Prints to stdout a formatted (possibly even colorized!) message
    /// describing this status.
    fn display_message(&self, should_print_color: bool) {
        enum Severity {
            Info,
            Warning,
            Error,
        }

        let (msg, severity) = match self {
            Self::NoSecAssigned => (
                "warning: saga has no assigned SEC, so cannot verify that the \
                saga is not still running! Proceed?"
                    .to_string(),
                Severity::Warning,
            ),
            Self::DnsResolverUnavailable(error) => (
                format!(
                    "Cannot proceed: failed to obtain DNS resolver: {error}"
                ),
                Severity::Error,
            ),
            Self::NexusResolutionFailed(error) => (
                format!(
                    "Cannot proceed: failed to resolve Nexus addresses via \
                    DNS: {error}"
                ),
                Severity::Error,
            ),
            Self::NoDnsServiceRecord(id) => (
                format!(
                    "Cannot proceed: no SRV record for Nexus with id {id}, \
                    so cannot verify that it is not still running!"
                ),
                Severity::Error,
            ),
            Self::DnsIpv6LookupFailed { target, error } => (
                format!(
                    "Cannot proceed: failed to obtain Nexus IPv6 address for \
                    {target}: {error}"
                ),
                Severity::Error,
            ),
            Self::NoAddressFromDns(target) => (
                format!(
                    "Cannot proceed: no AAAA record for Nexus with id {target},
                    so cannot verify that it is not still running!"
                ),
                Severity::Error,
            ),
            Self::SecAnsweredPing(id) => (
                format!(
                    "Cannot proceed: Nexus with id matching current SEC \
                    responded ok to a ping, meaning it is still running. \
                    Abandoning or injecting errors into a running saga is not \
                    safe. Please ensure the Nexus with id {id} is stopped \
                    before proceeding."
                ),
                Severity::Error,
            ),
            Self::SecPingError { sec_id: id, .. } => (
                format!(
                    "Cannot proceed: Nexus with id matching current SEC \
                    responded with an error to a ping, meaning it is still \
                    running. Abandoning or injecting errors into a running \
                    saga is not safe. Please ensure the Nexus with id {id} is \
                    stopped before proceeding."
                ),
                Severity::Error,
            ),
            Self::SecAppearsInactive { sec_id, observed_error } => (
                format!(
                    "saw {observed_error} when trying to ping Nexus with id \
                    {sec_id}. Proceed?"
                ),
                Severity::Info,
            ),
        };

        match severity {
            Severity::Info => println!("{msg}"),
            Severity::Warning => {
                if should_print_color {
                    println!("{}", msg.yellow().bold());
                } else {
                    println!("{msg}");
                }
            }
            Severity::Error => {
                if should_print_color {
                    println!("{}", msg.red().bold());
                } else {
                    println!("{msg}");
                }
            }
        }
    }

    /// Returns `Ok` if this status indicates that there's some reason to
    /// believe that the relevant SEC is actually offline and `Err` otherwise.
    fn into_result(self) -> anyhow::Result<()> {
        match self {
            Self::DnsResolverUnavailable(error) => Err(error),
            Self::NexusResolutionFailed(error) => Err(error.into()),
            Self::DnsIpv6LookupFailed { error, .. } => Err(error.into()),
            Self::NoDnsServiceRecord(id) => {
                Err(anyhow!("dns lookup for {id} found nothing"))
            }
            Self::NoAddressFromDns(target) => {
                Err(anyhow!("dns lookup for {target} found nothing"))
            }
            Self::SecAnsweredPing(id) => Err(anyhow!("{id} answered a ping")),
            Self::SecPingError { sec_id, observed_error } => {
                Err(anyhow!("{sec_id} failed a ping with {observed_error}"))
            }
            Self::NoSecAssigned | Self::SecAppearsInactive { .. } => Ok(()),
        }
    }
}

/// Attempts to determine whether the supplied `Saga` is being managed by an
/// active saga execution coordinator.
async fn get_saga_sec_status(
    omdb: &Omdb,
    opctx: &OpContext,
    saga: &Saga,
) -> SagaSecStatus {
    let Some(current_sec) = saga.current_sec else {
        return SagaSecStatus::NoSecAssigned;
    };

    let resolver = match omdb.dns_resolver(opctx.log.clone()).await {
        Ok(resolver) => resolver,
        Err(e) => return SagaSecStatus::DnsResolverUnavailable(e),
    };
    let srv = match resolver.lookup_srv(ServiceName::Nexus).await {
        Ok(srv) => srv,
        Err(e) => return SagaSecStatus::NexusResolutionFailed(e),
    };
    let Some((target, port)) =
        srv.iter().find(|(name, _)| name.contains(&current_sec.to_string()))
    else {
        return SagaSecStatus::NoDnsServiceRecord(current_sec);
    };

    let addr = match resolver.ipv6_lookup(&target).await {
        Ok(Some(addr)) => addr,
        Ok(None) => return SagaSecStatus::NoAddressFromDns(target.clone()),
        Err(e) => {
            return SagaSecStatus::DnsIpv6LookupFailed {
                target: target.clone(),
                error: e,
            };
        }
    };

    let client = nexus_lockstep_client::Client::new(
        &format!("http://[{addr}]:{port}/"),
        opctx.log.clone(),
    );

    match client.ping().await {
        Ok(_) => {
            return SagaSecStatus::SecAnsweredPing(current_sec);
        }

        Err(e) => match e {
            nexus_lockstep_client::Error::InvalidRequest(_)
            | nexus_lockstep_client::Error::InvalidUpgrade(_)
            | nexus_lockstep_client::Error::ErrorResponse(_)
            | nexus_lockstep_client::Error::ResponseBodyError(_)
            | nexus_lockstep_client::Error::InvalidResponsePayload(_, _)
            | nexus_lockstep_client::Error::UnexpectedResponse(_)
            | nexus_lockstep_client::Error::PreHookError(_)
            | nexus_lockstep_client::Error::PostHookError(_) => {
                return SagaSecStatus::SecPingError {
                    sec_id: current_sec,
                    observed_error: e.to_string(),
                };
            }

            nexus_lockstep_client::Error::CommunicationError(_) => {
                // Assume communication error means that it could not be
                // contacted.
                //
                // Note: this could be seen if Nexus is up but
                // unreachable from where omdb is run!
                return SagaSecStatus::SecAppearsInactive {
                    sec_id: current_sec,
                    observed_error: e.to_string(),
                };
            }
        },
    }
}

// Copy some types from Steno, because steno uses pub(crate) everywhere. We
// don't want to change Steno to make the internals public, but these types
// should be fairly stable.
#[derive(Serialize, Deserialize)]
struct StenoDag {
    pub saga_name: String,
    pub graph: Graph<StenoNode, ()>,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
}

impl StenoDag {
    pub fn get(&self, node_index: NodeIndex) -> Option<&StenoNode> {
        self.graph.node_weight(node_index)
    }

    pub fn get_from_saga_node_id(
        &self,
        saga_node_id: &nexus_db_model::saga_types::SagaNodeId,
    ) -> Option<&StenoNode> {
        self.get(u32::from(saga_node_id.0).into())
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum StenoNode {
    Start { params: Arc<serde_json::Value> },
    End,
    Action { name: String, label: String, action_name: String },
    Constant { name: String, value: Arc<serde_json::Value> },
    SubsagaStart { saga_name: String, params_node_name: String },
    SubsagaEnd { name: String },
}

async fn cmd_sagas_show(
    omdb: &Omdb,
    opctx: &OpContext,
    datastore: &DataStore,
    SagaShowArgs { saga_id }: SagaShowArgs,
) -> anyhow::Result<()> {
    let conn = datastore.pool_connection_for_tests().await?;
    let mut nodes = Vec::new();
    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Ascending);

    while let Some(p) = paginator.next() {
        use nexus_db_schema::schema::saga_node_event::dsl;

        let batch = paginated(
            dsl::saga_node_event,
            dsl::event_time,
            &p.current_pagparams(),
        )
        .filter(dsl::saga_id.eq(saga_id))
        .order_by(dsl::event_time)
        .select(SagaNodeEvent::as_select())
        .load_async(&*conn)
        .await
        .with_context(|| format!("error fetching saga nodes for {saga_id}"))?;
        paginator =
            p.found_batch(&batch, &|node: &SagaNodeEvent| node.event_time);
        nodes.extend(batch);
    }

    let saga = {
        use nexus_db_schema::schema::saga::dsl;
        dsl::saga
            .filter(dsl::id.eq(saga_id))
            .select(Saga::as_select())
            .first_async(&*conn)
            .await
            .with_context(|| format!("error fetching saga {saga_id}"))?
    };

    print_saga_nodes(Some(saga), nodes);

    Ok(())
}

/// Print a table showing saga nodes. If a Saga object is supplied as the first
/// argument, then look up the saga node's name and use that for output instead
/// of a node id.
fn print_saga_nodes(saga: Option<Saga>, saga_nodes: Vec<SagaNodeEvent>) {
    let dag: Option<StenoDag> = saga.as_ref().map(|saga: &Saga| {
        serde_json::from_value(saga.saga_dag.clone()).unwrap()
    });

    if let Some(saga) = saga {
        let dag = saga.saga_dag.clone();

        let table = tabled::Table::new(Some(SagaRow::from(saga)))
            .with(tabled::settings::Style::psql())
            .to_string();

        println!("{table}\n");
        println!("DAG: {}", dag);
        println!();
    }

    struct SagaNodeRow {
        saga_id: Uuid,
        event_time: String,
        sub_saga_id: Option<u32>,
        node_id: String,
        event_type: String,
        data: String,
    }

    // Keep track of which saga nodes are subsaga start nodes.
    let mut sub_saga_starts: BTreeMap<
        nexus_db_model::saga_types::SagaNodeId,
        u32,
    > = BTreeMap::default();
    let mut sub_saga_counter = 0;

    // Keep track of which nodes belong to which sub sagas
    let mut sub_saga_map: BTreeMap<
        nexus_db_model::saga_types::SagaNodeId,
        u32,
    > = BTreeMap::default();

    let mut rows: Vec<_> = Vec::with_capacity(saga_nodes.len());

    for saga_node in saga_nodes {
        let (node_id, sub_saga_id, data) = if let Some(dag) = &dag {
            let dag_node =
                dag.get_from_saga_node_id(&saga_node.node_id).unwrap();

            let sub_saga_id: Option<u32> = match sub_saga_map
                .get(&saga_node.node_id)
            {
                Some(id) => {
                    // We already determined this node was part of a sub
                    // saga.
                    Some(*id)
                }

                None => {
                    // Figure out if we're in an existing sub saga
                    let mut this_sub_saga_start_node_id = None;
                    let mut this_sub_saga_id = None;

                    for (sub_saga_start_node_id, sub_saga_id) in
                        &sub_saga_starts
                    {
                        // Is there a path from the start of the sub saga to
                        // this node? If so, then this node is in the sub saga.
                        let from: u32 = sub_saga_start_node_id.0.into();
                        let to: u32 = saga_node.node_id.0.into();

                        if petgraph::algo::has_path_connecting(
                            &dag.graph,
                            from.into(),
                            to.into(),
                            None,
                        ) {
                            this_sub_saga_start_node_id =
                                Some(*sub_saga_start_node_id);
                            this_sub_saga_id = Some(*sub_saga_id);
                            break;
                        }
                    }

                    if let Some(this_sub_saga_start_node_id) =
                        this_sub_saga_start_node_id
                    {
                        // Is the sub saga over?
                        if matches!(dag_node, StenoNode::SubsagaEnd { .. }) {
                            sub_saga_starts
                                .remove(&this_sub_saga_start_node_id);
                        }
                    } else {
                        // Did a new sub saga start?
                        if matches!(dag_node, StenoNode::SubsagaStart { .. })
                            && saga_node.event_type == *"started"
                        {
                            sub_saga_starts
                                .insert(saga_node.node_id, sub_saga_counter);
                            sub_saga_counter += 1;

                            this_sub_saga_id = Some(sub_saga_counter - 1);
                        } else {
                            // Not in a sub saga
                        }
                    }

                    if let Some(this_sub_saga_id) = this_sub_saga_id {
                        sub_saga_map
                            .insert(saga_node.node_id, this_sub_saga_id);
                    }

                    this_sub_saga_id
                }
            };

            let node_id = format!(
                "{:3}: {}",
                saga_node.node_id.0,
                match dag_node {
                    StenoNode::Start { .. } => String::from("start"),
                    StenoNode::End => String::from("end"),
                    StenoNode::Action { action_name, .. } =>
                        action_name.clone(),
                    StenoNode::Constant { name, .. } => name.clone(),
                    StenoNode::SubsagaStart { saga_name, params_node_name } =>
                        format!(
                            "subsaga start {} ({})",
                            saga_name, params_node_name
                        ),
                    StenoNode::SubsagaEnd { name } =>
                        format!("subsaga end {}", name),
                },
            );

            // If the saga node produced data, label it with the saga name, not the action name
            let data = match dag_node {
                StenoNode::Action { name, .. } => {
                    if let Some(saga_node_data) = saga_node.data {
                        match saga_node_data {
                            serde_json::Value::Null => String::from(""),

                            x => {
                                format!(
                                    "\"{}\" => {}",
                                    name,
                                    serde_json::to_string(&x).unwrap(),
                                )
                            }
                        }
                    } else {
                        String::from("")
                    }
                }

                _ => String::from(""),
            };

            (node_id, sub_saga_id, data)
        } else {
            let node_id = format!("{}", saga_node.node_id.0);

            let data = if let Some(saga_node_data) = saga_node.data {
                match saga_node_data {
                    serde_json::Value::Null => String::from(""),
                    x => serde_json::to_string(&x).unwrap(),
                }
            } else {
                String::from("")
            };

            (node_id, None, data)
        };

        rows.push(SagaNodeRow {
            saga_id: saga_node.saga_id.0.into(),
            event_time: datetime_rfc3339_concise(&saga_node.event_time),
            sub_saga_id,
            node_id,
            event_type: saga_node.event_type,
            data,
        })
    }

    if rows.is_empty() {
        return;
    }

    // We avoid `Tabled` here because some columns could be very wide, and its
    // auto-sizing would cause all rows to be that wide, resulting in unreadable
    // output.
    let row_char_counts: Vec<_> = rows
        .iter()
        .map(|x| {
            (
                format!("{}", x.saga_id).chars().count(),
                x.event_time.chars().count(),
                if let Some(sub_saga_id) = x.sub_saga_id {
                    format!("{}", sub_saga_id).chars().count()
                } else {
                    0
                },
                x.node_id.chars().count(),
                x.event_type.chars().count(),
                x.data.chars().count(),
            )
        })
        .collect();

    let (width0, width1, width2, width3, width4): (
        usize,
        usize,
        usize,
        usize,
        usize,
    ) = (
        row_char_counts.iter().map(|x| x.0).max().unwrap(),
        row_char_counts.iter().map(|x| x.1).max().unwrap(),
        std::cmp::max(
            row_char_counts.iter().map(|x| x.2).max().unwrap(),
            "sub saga".len(),
        ),
        std::cmp::max(
            row_char_counts.iter().map(|x| x.3).max().unwrap(),
            "node id".len(),
        ),
        std::cmp::max(
            row_char_counts.iter().map(|x| x.4).max().unwrap(),
            "event type".len(),
        ),
    );

    println!(
        "{:>width0$} | {:width1$} | {:width2$} | {:width3$} | {:width4$} | {}",
        String::from("saga id"),
        String::from("event time"),
        String::from("sub saga"),
        String::from("node id"),
        String::from("event type"),
        String::from("data"),
    );

    println!(
        "{:>width0$} | {:width1$} | {:width2$} | {:width3$} | {:width4$} | {}",
        (0..width0).map(|_| "-").collect::<String>(),
        (0..width1).map(|_| "-").collect::<String>(),
        (0..width2).map(|_| "-").collect::<String>(),
        (0..width3).map(|_| "-").collect::<String>(),
        (0..width4).map(|_| "-").collect::<String>(),
        String::from("---"),
    );

    for row in rows {
        println!(
            "{:>width0$} | {:width1$} | {:>width2$} | {:width3$} | {:width4$} | {}",
            row.saga_id,
            row.event_time,
            if let Some(sub_saga_id) = row.sub_saga_id {
                format!("{}", sub_saga_id)
            } else {
                String::from("")
            },
            row.node_id,
            row.event_type,
            row.data,
        );
    }
}
