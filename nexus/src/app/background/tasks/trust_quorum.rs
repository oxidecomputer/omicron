// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for preparing and committing trust quorum configurations

use crate::app::background::BackgroundTask;
use anyhow::{Context, Error, anyhow, bail};
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_networking::sled_client_by_baseboard_id_and_rack_id_if_commissioned;
use nexus_types::internal_api::background::TrustQuorumManagerStatus;
use nexus_types::trust_quorum::{
    TrustQuorumConfig as NexusTrustQuorumConfig, TrustQuorumMemberState,
};
use omicron_uuid_kinds::RackUuid;
use parallel_task_set::ParallelTaskSet;
use rand::seq::SliceRandom;
use serde_json::json;
use sled_agent_client::types::{
    ProxyCommitRequest, ProxyPrepareAndCommitRequest,
};
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info, o};
use slog_error_chain::InlineErrorChain;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use swrite::{SWrite, swrite, swriteln};
use tokio::task::JoinSet;
use trust_quorum_types::messages::{CommitRequest, PrepareAndCommitRequest};
use trust_quorum_types::status::CommitStatus;
use trust_quorum_types::types::Epoch;

pub struct TrustQuorumManager {
    datastore: Arc<DataStore>,
}

impl TrustQuorumManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    async fn activate_impl(
        &mut self,
        opctx: &OpContext,
    ) -> TrustQuorumManagerStatus {
        let opctx = opctx.child(BTreeMap::from([(
            "bgtask".into(),
            "TrustQuorumManager".into(),
        )]));
        let log = opctx.log.new(o!("bgtask" => "TrustQuorumManager"));
        // First, see if nexus needs to do any work.
        let epochs_by_rack_id = match self
            .datastore
            .tq_get_all_active_rack_id_and_latest_epoch(&opctx)
            .await
        {
            Ok(epochs_by_rack_id) => epochs_by_rack_id,
            Err(err) => {
                let msg = format!(
                    "Failed to load active trust quorum configs: {}",
                    InlineErrorChain::new(&err)
                );
                error!(log, "{msg}");
                return TrustQuorumManagerStatus::Error(msg);
            }
        };

        info!(
            log,
            "Loaded {} active trust quorum configurations from database",
            epochs_by_rack_id.len()
        );

        // For each rack, do the work
        let mut workers = ParallelTaskSet::new();
        for (rack_id, epoch) in epochs_by_rack_id {
            let log = log.clone();
            let opctx = opctx.child(BTreeMap::from([
                ("rack_id".into(), rack_id.to_string()),
                ("epoch".into(), epoch.to_string()),
            ]));
            let datastore = self.datastore.clone();
            workers
                .spawn(async move {
                    let res = drive_reconfiguration(
                        log, opctx, datastore, rack_id, epoch,
                    )
                    .await;
                    (rack_id, epoch, res)
                })
                .await;
        }

        let mut statuses = vec![];
        let mut errors = vec![];
        while let Some((rack_id, epoch, res)) = workers.join_next().await {
            // Propagate panics: we don't cancel the worker tasks, so this
            // can only fail if the task itself already panicked.
            match res {
                Ok(status) => {
                    let status_string = status.to_display(rack_id, epoch);
                    info!(log, "{status_string}");
                    statuses.push(status_string);
                }
                Err(err) => {
                    let msg = format!(
                        "Reconfiguration worker error for \
                                rack_id {rack_id}, epoch {epoch}: {}",
                        InlineErrorChain::new(&*err)
                    );
                    error!(log, "{msg}");
                    errors.push(msg);
                }
            }
        }

        TrustQuorumManagerStatus::PerRackStatus { statuses, errors }
    }
}

impl BackgroundTask for TrustQuorumManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let status = self.activate_impl(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => json!({
                    "error": format!(
                        "could not serialize task status: {}",
                         InlineErrorChain::new(&err)
                     ),
                }),
            }
        })
    }
}

// Status of an activation for a single rack's active configuration
enum Status {
    ConfigInactive,
    CoordinatorNoLongerCommissioned(BaseboardId),
    Preparing,
    Commit(Vec<CommitOpsResults>),
}

impl Status {
    pub fn to_display(&self, rack_id: RackUuid, epoch: Epoch) -> String {
        let mut s = String::new();
        swriteln!(
            &mut s,
            "Trust Quorum status for rack_id: {rack_id}, epoch: {epoch}:"
        );
        match self {
            Status::ConfigInactive => {
                swriteln!(
                    &mut s,
                    "    Configuration became inactive: fully committed or \
                    aborted."
                );
            }
            Status::CoordinatorNoLongerCommissioned(coordinator_id) => {
                swriteln!(
                    &mut s,
                    "    Coordinator ({coordinator_id}) was decommissioned."
                );
            }
            Status::Preparing => {
                swriteln!(&mut s, "    Prepare phase in progress.");
            }
            Status::Commit(op_results) => {
                for r in op_results {
                    swriteln!(
                        &mut s,
                        "    Commit Status from client {}:",
                        r.client_destination
                    );
                    swrite!(&mut s, "      acked: ");
                    swriteln!(&mut s, "{}", itertools::join(&r.acked, ", "));
                    swrite!(&mut s, "      pending: ");
                    swriteln!(&mut s, "{}", itertools::join(&r.pending, ", "));
                    swriteln!(&mut s, "      errors:");
                    for e in &r.errors {
                        swriteln!(&mut s, "        {e}");
                    }
                }
            }
        }
        s
    }
}

async fn drive_reconfiguration(
    log: Logger,
    opctx: OpContext,
    datastore: Arc<DataStore>,
    rack_id: RackUuid,
    epoch: Epoch,
) -> Result<Status, Error> {
    let Some(config) = datastore
        .tq_get_config(&opctx, rack_id, epoch)
        .await
        .context("Failed to get tq configuration")?
    else {
        bail!("failed to retrieve configuration: missing");
    };

    // If we are preparing, then collect from coordinator, otherwise
    // attempt to commit at unacked nodes.
    if config.state.is_active() {
        info!(
            log,
            "Loaded active trust quorum config from database";
            "rack_id" => %rack_id,
            "epoch" => %epoch,
            "state" => ?config.state
        );
    } else {
        info!(
            log,
            "Loaded inactive trust quorum config from database. Skipping";
            "rack_id" => %rack_id,
            "epoch" => %epoch,
            "state" => ?config.state
        );
        return Ok(Status::ConfigInactive);
    }

    // Poll the coordinator for commit acks
    if config.state.is_preparing() {
        return prepare(log, opctx, datastore, config).await;
    }

    // For each unacked node, need to send a `Commit` or `PrepareAndCommit'
    //
    // At this point we know that the configuration is active and we are not
    // preparing. Therefore we must be committing.
    commit(log, opctx, datastore, config).await
}

async fn prepare(
    log: Logger,
    opctx: OpContext,
    datastore: Arc<DataStore>,
    config: NexusTrustQuorumConfig,
) -> Result<Status, Error> {
    // Get a sled agent for the coordinator
    let Some(coordinator_sa) =
        sled_client_by_baseboard_id_and_rack_id_if_commissioned(
            &datastore,
            &opctx,
            &config.coordinator,
            config.rack_id,
            &log,
        )
        .await
        .with_context(|| {
            format!(
                "failed to retrieve commissioned sled \
                    by baseboard_id {} and rack_id {}.",
                &config.coordinator, &config.rack_id
            )
        })?
    else {
        return Ok(Status::CoordinatorNoLongerCommissioned(
            config.coordinator.clone(),
        ));
    };

    let status = coordinator_sa
        .trust_quorum_coordinator_status()
        .await
        .with_context(|| {
            format!(
                "Failed to get coordinator status from {}.",
                &config.coordinator
            )
        })?
        .into_inner();

    // Write state back to DB
    let state = datastore
        .tq_update_prepare_status(&opctx, status.config, status.acked_prepares)
        .await?;

    // If we have started committing then we need to commit at each member.
    // We assume that other Nexuses have not yet committed, but there is
    // an inherent race between nexuses anyway, and the database ensures
    // safety.
    //
    // Otherwise, we haven't finished preparing, so just return.
    if state.is_committing() {
        return commit(log, opctx, datastore, config).await;
    } else {
        return Ok(Status::Preparing);
    }
}

#[derive(Debug)]
enum CommitOp {
    PrepareAndCommit(BaseboardId),
    Commit(BaseboardId),
}

// For each unacked member, issue a `Commit` or `PrepareAndCommit` request.
//
// Return a unique status for each client that issued operations.
async fn commit(
    log: Logger,
    opctx: OpContext,
    datastore: Arc<DataStore>,
    nexus_config: NexusTrustQuorumConfig,
) -> Result<Status, Error> {
    // All `Commit` or `PrepareAndCommit` requests sent to sled-agents
    let mut ops = vec![];
    for (baseboard_id, member_data) in &nexus_config.members {
        match member_data.state {
            TrustQuorumMemberState::Unacked => {
                ops.push(CommitOp::PrepareAndCommit(baseboard_id.clone()));
            }
            TrustQuorumMemberState::Prepared => {
                ops.push(CommitOp::Commit(baseboard_id.clone()));
            }
            TrustQuorumMemberState::Committed => {
                // Member is already committed. Nothing to do.
            }
        }
    }

    if ops.is_empty() {
        bail!(
            "Unexpected error. No members to commit, \
            even though configuration was active."
        );
    }

    //
    // Get a set of random clients. We can proxy requests if these clients are
    // different from the members that need committing.
    //
    const MAX_CONCURRENCY: usize = 5;
    let num_clients = usize::min(MAX_CONCURRENCY, ops.len());

    // First shuffle the possible members
    let mut randomized: Vec<_> = nexus_config.members.keys().collect();
    randomized.shuffle(&mut rand::rng());
    let mut clients = Vec::with_capacity(num_clients);

    // Now try to get a client for `num_clients`
    for id in randomized {
        let Some(client) =
            sled_client_by_baseboard_id_and_rack_id_if_commissioned(
                &datastore,
                &opctx,
                &id,
                nexus_config.rack_id,
                &log,
            )
            .await?
        else {
            continue;
        };
        clients.push((id.clone(), client));
        if clients.len() == num_clients {
            break;
        }
    }

    if clients.is_empty() {
        bail!(format!(
            "No sled-agent clients could be retrieved for trust quorum \
            configuration with rack_id {} and epoch {}",
            nexus_config.rack_id, nexus_config.epoch
        ));
    }

    let mut workers = JoinSet::new();
    let config = trust_quorum_types::configuration::Configuration::try_from(
        &nexus_config,
    )?;

    let ops_per_client = ops.len() / clients.len();
    let num_clients = clients.len();
    for (i, (client_dest, client)) in clients.into_iter().enumerate() {
        let config = config.clone();
        let client_ops = if i + 1 == num_clients {
            // Take all remaining ops for the last client.
            std::mem::take(&mut ops)
        } else {
            // Draining removes elements and moves the elements on the back to
            // the front to fill the hole. Therefore the size shrinks and we can
            // always drain from the front.
            ops.drain(..ops_per_client).collect()
        };
        let client_dest2 = client_dest.clone();
        workers.spawn(async move {
            run_commit_ops(client_dest2, client, client_ops, config).await
        });
    }

    let mut acked = BTreeSet::new();
    let mut client_results = Vec::with_capacity(num_clients);
    while let Some(res) = workers.join_next().await {
        let r = res.expect("worker task didn't panic");
        // Collect all acks so we can update the DB
        acked.extend(r.acked.clone());
        client_results.push(r);
    }

    if !acked.is_empty() {
        // Write state back to DB
        datastore
            .tq_update_commit_status(
                &opctx,
                nexus_config.rack_id,
                nexus_config.epoch,
                acked.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to update commit status in database for rack_id {}, \
                     epoch {}",
                    nexus_config.rack_id, nexus_config.epoch,
                )
            })?;
    }

    Ok(Status::Commit(client_results))
}

/// Results from a worker task attempting to send client requests to a
/// sled-agent to `PrepareAndCommit` or `Commit` reconfigurations.
struct CommitOpsResults {
    pub client_destination: BaseboardId,
    pub acked: Vec<BaseboardId>,
    pub pending: Vec<BaseboardId>,
    pub errors: Vec<Error>,
}

impl CommitOpsResults {
    pub fn new(client_destination: BaseboardId) -> Self {
        Self {
            client_destination,
            acked: vec![],
            pending: vec![],
            errors: vec![],
        }
    }
}

/// Send `PrepareAndCommit` and/or `Commit` requests via `client`.
///
/// Return which nodes acked successfully, which reported errors, and which were
/// pending commit.
async fn run_commit_ops(
    client_destination: BaseboardId,
    client: sled_agent_client::Client,
    ops: Vec<CommitOp>,
    config: trust_quorum_types::configuration::Configuration,
) -> CommitOpsResults {
    let mut res = CommitOpsResults::new(client_destination.clone());
    for op in ops {
        match op {
            CommitOp::PrepareAndCommit(id) => {
                let request =
                    PrepareAndCommitRequest { config: config.clone() };
                if id == client_destination {
                    match client.trust_quorum_prepare_and_commit(&request).await
                    {
                        Ok(status) => match status.into_inner() {
                            CommitStatus::Committed => res.acked.push(id),
                            CommitStatus::Pending => res.pending.push(id),
                        },
                        Err(err) => {
                            res.errors.push(anyhow!(
                                "Failed to send PrepareAndCommit to {id}: {err}"
                            ));
                        }
                    }
                } else {
                    match client
                        .trust_quorum_proxy_prepare_and_commit(
                            &ProxyPrepareAndCommitRequest {
                                destination: id.clone(),
                                request,
                            },
                        )
                        .await
                    {
                        Ok(status) => match status.into_inner() {
                            CommitStatus::Committed => res.acked.push(id),
                            CommitStatus::Pending => res.pending.push(id),
                        },
                        Err(err) => {
                            res.errors.push(anyhow!(
                                "Failed to send PrepareAndCommit to {id} via \
                                {client_destination}: {err}"
                            ));
                        }
                    }
                }
            }
            CommitOp::Commit(id) => {
                let request = CommitRequest {
                    rack_id: config.rack_id,
                    epoch: config.epoch,
                };
                if id == client_destination {
                    match client.trust_quorum_commit(&request).await {
                        Ok(_) => {
                            // If nexus is sending a `Commit` then it knows that
                            // this node has already prepared, and therefore
                            // there is no status to return.  Commit should
                            // happen immediately.
                            res.acked.push(id);
                        }
                        Err(err) => {
                            res.errors.push(anyhow!(
                                "Failed to send Commit to {id}: {err}"
                            ));
                        }
                    }
                } else {
                    match client
                        .trust_quorum_proxy_commit(&ProxyCommitRequest {
                            destination: id.clone(),
                            request,
                        })
                        .await
                    {
                        Ok(_) => res.acked.push(id),
                        Err(err) => {
                            res.errors.push(anyhow!(
                                "Failed to send Commit to {id} via \
                                {client_destination}: {err}"
                            ));
                        }
                    }
                }
            }
        }
    }
    res
}
