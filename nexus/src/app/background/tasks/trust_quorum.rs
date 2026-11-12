// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for preparing and committing trust quorum configurations

use crate::app::background::Activator;
use crate::app::background::BackgroundTask;
use crate::app::rack::rack_subnet;
use anyhow::{Context, Error, anyhow, bail};
use futures::future::BoxFuture;
use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_db_model::SledUnderlaySubnetAllocation;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledUnderlayAllocationResult;
use nexus_db_queries::db::datastore::TqPrepareStatusOutcome;
use nexus_networking::{
    InServiceSledClientError,
    sled_client_by_baseboard_id_and_rack_id_if_in_service,
    sled_client_by_baseboard_id_and_rack_id_if_in_service_ext,
};
use nexus_types::internal_api::background::TrustQuorumManagerStatus;
use nexus_types::trust_quorum::{
    TrustQuorumConfig as NexusTrustQuorumConfig, TrustQuorumMemberState,
};
use omicron_common::address::{Ipv6Subnet, RACK_PREFIX_LENGTH, get_64_subnet};
use omicron_common::http_client::internal_http_client_builder;
use omicron_uuid_kinds::{RackUuid, SledUuid};
use parallel_task_set::ParallelTaskSet;
use rand::seq::SliceRandom;
use serde_json::json;
use sled_agent_client::types::AddSledRequest;
use sled_agent_client::types::StartSledAgentRequest;
use sled_agent_client::types::StartSledAgentRequestBody;
use sled_agent_client::types::{
    ProxyCommitRequest, ProxyPrepareAndCommitRequest,
};
use sled_agent_types::trust_quorum::TrustQuorumCoordinatorStatusResponse;
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use swrite::{SWrite, swrite, swriteln};
use tokio::task::JoinSet;
use trust_quorum_types::messages::{CommitRequest, PrepareAndCommitRequest};
use trust_quorum_types::status::CommitStatus;
use trust_quorum_types::types::Epoch;

const MAX_SLED_AGENT_REQUEST_CONCURRENCY: usize = 5;
pub struct TrustQuorumManager {
    datastore: Arc<DataStore>,
    sled_expunge: Activator,
}

impl TrustQuorumManager {
    pub fn new(datastore: Arc<DataStore>, sled_expunge: Activator) -> Self {
        Self { datastore, sled_expunge }
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
        let mut outputs = Vec::new();
        for (rack_id, epoch) in epochs_by_rack_id {
            let log = log.clone();
            let opctx = opctx.child(BTreeMap::from([
                ("rack_id".into(), rack_id.to_string()),
                ("epoch".into(), epoch.to_string()),
            ]));
            let datastore = self.datastore.clone();
            let sled_expunge = self.sled_expunge.clone();
            if let Some(output) = workers
                .spawn(async move {
                    let res = drive_reconfiguration(
                        log,
                        opctx,
                        datastore,
                        &sled_expunge,
                        rack_id,
                        epoch,
                    )
                    .await;
                    (rack_id, epoch, res)
                })
                .await
            {
                outputs.push(output);
            }
        }

        let mut statuses = vec![];
        let mut errors = vec![];
        outputs.extend(workers.join_remaining().await);
        for (rack_id, epoch, res) in outputs {
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
    CoordinatorNotInService(BaseboardId),
    Preparing,
    Commit(Vec<CommitOpsResults>),
    CommitAndStartSledAgents(Vec<CommitOpsResults>, Vec<StartSledAgentResults>),
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
            Status::CoordinatorNotInService(coordinator_id) => {
                swriteln!(
                    &mut s,
                    "    Coordinator ({coordinator_id}) is not in service: \
                     configuration is aborted."
                );
            }
            Status::Preparing => {
                swriteln!(&mut s, "    Prepare phase in progress.");
            }
            Status::Commit(op_results) => {
                for r in op_results {
                    r.write_to_string(&mut s);
                }
            }
            Status::CommitAndStartSledAgents(op_results, start_results) => {
                for r in op_results {
                    r.write_to_string(&mut s);
                }
                for r in start_results {
                    r.write_to_string(&mut s);
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
    sled_expunge: &Activator,
    rack_id: RackUuid,
    epoch: Epoch,
) -> Result<Status, Error> {
    let authz_tq = authz::TrustQuorumConfig::for_rack_id(rack_id);
    let Some(config) = datastore
        .tq_get_config(&opctx, authz_tq.clone(), epoch)
        .await
        .context("Failed to get tq configuration")?
    else {
        bail!("failed to retrieve configuration: missing");
    };

    // If we are preparing, then collect from coordinator, otherwise
    // attempt to commit at unacked nodes.
    let status = if config.state.is_active() {
        info!(
            log,
            "Loaded active trust quorum config from database.";
            "rack_id" => %rack_id,
            "epoch" => %epoch,
            "state" => ?config.state
        );
        if config.state.is_preparing() {
            prepare(&log, &opctx, &datastore, sled_expunge, config.clone())
                .await?
        } else {
            commit(&log, &opctx, &datastore, config.clone()).await?
        }
    } else {
        info!(
            log,
            "Loaded inactive trust quorum config from database.";
            "rack_id" => %rack_id,
            "epoch" => %epoch,
            "state" => ?config.state
        );
        Status::ConfigInactive
    };

    // We may have committed all members, but some of them may not have started
    // sled-agents yet.
    //
    // We continue to retry sending them a `StartSledAgentRequest` until it
    // starts.
    let added_sleds = datastore
        .tq_get_committed_members_without_active_sled(&opctx, authz_tq.clone())
        .await?;

    if added_sleds.is_empty() {
        return Ok(status);
    }

    // At this point, We know we've committed at least some members in this or
    // prior attempts and need to start their sled-agents. Get the results for
    // this attempt, if they exist.
    let commit_results = match status {
        Status::Commit(commit_results) => commit_results,
        _ => vec![],
    };

    // Allocate a subnet and start a sled agent for each sled that needs it
    let rack_id = config.rack_id;
    let epoch = config.epoch;

    let existing_sleds: BTreeSet<_> = config
        .members
        .iter()
        .filter_map(|m| {
            if added_sleds.contains(&m.0) { None } else { Some(m.0.clone()) }
        })
        .collect();

    let started_sleds = allocate_subnets_and_start_sled_agents(
        log,
        opctx,
        datastore,
        rack_id,
        epoch,
        added_sleds,
        existing_sleds,
    )
    .await
    .with_context(|| {
        format!(
            "Failed to start sled agents for added sleds for \
                    rack {rack_id}, epoch {epoch}"
        )
    })?;

    return Ok(Status::CommitAndStartSledAgents(commit_results, started_sleds));
}

async fn prepare(
    log: &Logger,
    opctx: &OpContext,
    datastore: &DataStore,
    sled_expunge: &Activator,
    config: NexusTrustQuorumConfig,
) -> Result<Status, Error> {
    // Get a sled agent for the coordinator
    let coordinator_sa =
        match sled_client_by_baseboard_id_and_rack_id_if_in_service(
            &datastore,
            &opctx,
            &config.coordinator,
            config.rack_id,
            &log,
        )
        .await
        {
            Ok(client) => client,
            Err(InServiceSledClientError::NotInService { .. }) => {
                abort_config_with_coordinator_not_in_service(
                    log, opctx, datastore, &config,
                )
                .await?;
                // TODO-RAINCLAUDE: an aborted removal config also left preparing, which fails its sled expunge request; as on the Committing path below, the wake only saves sled_expunge a period.
                info!(
                    log,
                    "Trust quorum config aborted; activating sled_expunge.";
                    "rack_id" => %config.rack_id,
                    "epoch" => %config.epoch,
                );
                sled_expunge.activate();
                return Ok(Status::CoordinatorNotInService(config.coordinator));
            }
            Err(error @ InServiceSledClientError::LookupFailed { .. }) => {
                return Err(error.into());
            }
        };

    let response = coordinator_sa
        .trust_quorum_coordinator_status()
        .await
        .with_context(|| {
            format!(
                "Failed to get coordinator status from {}.",
                config.coordinator
            )
        })?
        .into_inner();
    let status = match response {
        TrustQuorumCoordinatorStatusResponse::Coordinating { status } => status,
        TrustQuorumCoordinatorStatusResponse::NotCoordinating {
            latest_config_epoch,
        } => {
            let latest_config_epoch = match latest_config_epoch {
                Some(epoch) => epoch.to_string(),
                None => "none".to_string(),
            };
            bail!(
                "Coordinator {} reported no coordinator status: it is not \
                 currently coordinating a reconfiguration (latest persisted \
                 configuration epoch: {latest_config_epoch}).",
                config.coordinator,
            );
        }
    };

    // Write state back to DB
    let outcome = record_prepare_status(
        opctx,
        datastore,
        status.config,
        status.acked_prepares,
    )
    .await?;

    // If we have started committing then we need to commit at each member.
    // We assume that other Nexuses have not yet committed, but there is
    // an inherent race between nexuses anyway, and the database ensures
    // safety.
    //
    // Otherwise, we haven't finished preparing, so just return.
    match outcome {
        TqPrepareStatusOutcome::Committing { config } => {
            // TODO-RAINCLAUDE: a sled expunge request waits for its removal config to leave preparing (and a re-add config leaving preparing fails it); waking sled_expunge here only saves it a period, and it happens before commit so a commit error can't lose it.
            info!(
                log,
                "Trust quorum config left preparing; activating sled_expunge.";
                "rack_id" => %config.rack_id,
                "epoch" => %config.epoch,
            );
            sled_expunge.activate();
            commit(log, opctx, datastore, *config).await
        }
        TqPrepareStatusOutcome::StillPreparing => Ok(Status::Preparing),
    }
}

async fn record_prepare_status(
    opctx: &OpContext,
    datastore: &DataStore,
    coordinator_config: trust_quorum_types::configuration::Configuration,
    acked_prepares: BTreeSet<BaseboardId>,
) -> Result<TqPrepareStatusOutcome, Error> {
    let outcome = datastore
        .tq_update_prepare_status(opctx, coordinator_config, acked_prepares)
        .await?;
    Ok(outcome)
}

/// Abort a trust quorum reconfiguration in the database because the coordinator
/// is no longer in service.
///
/// Returns `Ok(())` if the abort succeeded, including when the configuration
/// was already aborted.
///
/// Returns an error if the abort failed, including the case where the abort
/// cannot happen because this configuration has already moved past the
/// preparing stage. The next time the background task is activated, it'll
/// reload the configuration and act on its current state. (Why not do it in
/// this activation? Because the race is quite narrow -- this Nexus has to
/// discover that the coordinator is out of service at the same time that
/// another Nexus moves this config past the preparing stage. We'll let the
/// Nexus that moved the config past the preparing stage take charge.)
async fn abort_config_with_coordinator_not_in_service(
    log: &Logger,
    opctx: &OpContext,
    datastore: &DataStore,
    config: &NexusTrustQuorumConfig,
) -> Result<(), Error> {
    let reason = format!(
        "Coordinator {} is no longer an in-service sled in rack {}; aborted \
         by the trust quorum background task.",
        config.coordinator, config.rack_id
    );
    datastore
        .tq_abort_config(
            opctx,
            authz::TrustQuorumConfig::for_rack_id(config.rack_id),
            config.epoch,
            reason,
        )
        .await
        .with_context(|| {
            format!(
                "failed to abort the configuration whose coordinator {} is \
                 not in service",
                config.coordinator
            )
        })?;
    warn!(
        log,
        "Aborted trust quorum configuration: coordinator is not in service";
        "rack_id" => %config.rack_id,
        "epoch" => %config.epoch,
        "coordinator" => %config.coordinator,
    );
    Ok(())
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
    log: &Logger,
    opctx: &OpContext,
    datastore: &DataStore,
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

    // Get a set of random clients. We can proxy requests if these clients are
    // different from the members that need committing.
    let num_clients = usize::min(MAX_SLED_AGENT_REQUEST_CONCURRENCY, ops.len());
    let clients = get_sled_agent_clients(
        &log,
        &opctx,
        &datastore,
        nexus_config.rack_id,
        nexus_config.epoch,
        &nexus_config.members.keys().cloned().collect(),
        num_clients,
        Duration::from_secs(15),
    )
    .await?;

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
        let authz_tq =
            authz::TrustQuorumConfig::for_rack_id(nexus_config.rack_id);
        let _ = datastore
            .tq_update_commit_status(
                &opctx,
                authz_tq,
                nexus_config.epoch,
                acked.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to update commit status in database for rack {}, \
                     epoch {}",
                    nexus_config.rack_id, nexus_config.epoch,
                )
            })?;

        return Ok(Status::Commit(client_results));
    }

    Ok(Status::Commit(client_results))
}

async fn allocate_subnets_and_start_sled_agents(
    log: Logger,
    opctx: OpContext,
    datastore: Arc<DataStore>,
    rack_id: RackUuid,
    epoch: Epoch,
    added_sleds: BTreeSet<BaseboardId>,
    existing_sleds: BTreeSet<BaseboardId>,
) -> Result<Vec<StartSledAgentResults>, Error> {
    info!(
        log,
        "Looking up hw_baseboard_id for newly added sleds: {added_sleds:?}"
    );
    let mut added_sleds_and_hw_ids = vec![];
    for baseboard_id in &added_sleds {
        added_sleds_and_hw_ids.push((
            baseboard_id.clone(),
            datastore.find_hw_baseboard_id(&opctx, &baseboard_id).await?,
        ));
    }

    info!(
        log,
        "Allocating underlay network for newly added sleds: {added_sleds:?}"
    );

    let mut allocations_by_baseboard_id = BTreeMap::new();
    for (baseboard_id, hw_baseboard_id) in added_sleds_and_hw_ids {
        let allocation = match datastore
            .allocate_sled_underlay_subnet_octets(
                &opctx,
                rack_id,
                hw_baseboard_id,
            )
            .await?
        {
            SledUnderlayAllocationResult::New(allocation) => allocation,
            SledUnderlayAllocationResult::CommissionedSled(allocation) => {
                bail!(format!(
                    "Sled network allocation already exists for rack {rack_id} \
                    BaseboardId {baseboard_id}, sled: {}, subnet octet: {}",
                    allocation.sled_id, allocation.subnet_octet
                ));
            }
        };
        allocations_by_baseboard_id.insert(baseboard_id, allocation);
    }

    start_sled_agents(
        log,
        opctx,
        datastore,
        rack_id,
        epoch,
        existing_sleds,
        allocations_by_baseboard_id,
    )
    .await
}

async fn start_sled_agents(
    log: Logger,
    opctx: OpContext,
    datastore: Arc<DataStore>,
    rack_id: RackUuid,
    epoch: Epoch,
    existing_sleds: BTreeSet<BaseboardId>,
    allocations_by_baseboard_id: BTreeMap<
        BaseboardId,
        SledUnderlaySubnetAllocation,
    >,
) -> Result<Vec<StartSledAgentResults>, Error> {
    info!(log, "Looking up subnet for rack: {}", rack_id);
    let subnet = datastore.rack_subnet(&opctx, rack_id).await?;
    let rack_subnet =
        Ipv6Subnet::<RACK_PREFIX_LENGTH>::from(rack_subnet(Some(subnet))?);

    // Get a set of random sled agent clients for concurrent calls to
    // `start_sled_agent`.
    let num_clients = usize::min(
        MAX_SLED_AGENT_REQUEST_CONCURRENCY,
        allocations_by_baseboard_id.len(),
    );
    let clients = get_sled_agent_clients(
        &log,
        &opctx,
        &datastore,
        rack_id,
        epoch,
        &existing_sleds,
        num_clients,
        Duration::from_mins(5),
    )
    .await?;

    let mut workers = JoinSet::new();
    let allocations_per_client =
        allocations_by_baseboard_id.len() / clients.len();
    let num_clients = clients.len();
    let mut iter = allocations_by_baseboard_id.into_iter();
    for (i, (client_dest, client)) in clients.into_iter().enumerate() {
        let allocations = if i + 1 == num_clients {
            // Take all remaining allocations for the last client.
            iter.by_ref().collect()
        } else {
            iter.by_ref().take(allocations_per_client).collect()
        };
        let client_dest2 = client_dest.clone();
        let log = log.clone();
        workers.spawn(async move {
            send_start_sled_agent_requests(
                &log,
                client_dest2,
                client,
                rack_subnet,
                allocations,
            )
            .await
        });
    }

    Ok(workers.join_all().await)
}

async fn send_start_sled_agent_requests(
    log: &Logger,
    client_dest: BaseboardId,
    client: sled_agent_client::Client,
    rack_subnet: Ipv6Subnet<RACK_PREFIX_LENGTH>,
    allocations_by_baseboard_id: BTreeMap<
        BaseboardId,
        SledUnderlaySubnetAllocation,
    >,
) -> StartSledAgentResults {
    let mut results = StartSledAgentResults::new(client_dest.clone());

    for (baseboard_id, allocation) in allocations_by_baseboard_id {
        let req = AddSledRequest {
            sled_id: baseboard_id.clone(),
            start_request: StartSledAgentRequest {
                generation: 0,
                schema_version: 1,
                body: StartSledAgentRequestBody {
                    id: allocation.sled_id.into(),
                    rack_id: allocation.rack_id.into(),
                    use_trust_quorum: true,
                    is_lrtq_learner: false,
                    subnet: sled_agent_client::types::Ipv6Subnet {
                        net: get_64_subnet(
                            rack_subnet,
                            allocation.subnet_octet.try_into().unwrap(),
                        )
                        .net(),
                    },
                },
            },
        };

        let log =
            log.new(o!("sled_agent_baseboard_id" => client_dest.to_string()));

        match client.sled_add(&req).await {
            Ok(_) => {
                info!(
                    log,
                    "Successfully started sled agent";
                    "baseboard_id" => %baseboard_id,
                    "sled_id" => allocation.sled_id.to_string()
                );
                results.started.push((baseboard_id, allocation.sled_id.into()));
            }
            Err(err) => {
                let error = anyhow!(
                    "Failed to start sled agent: {}",
                    InlineErrorChain::new(&err)
                );
                error!(log, "{error}"; "baseboard_id" => %baseboard_id);
                results.errors.insert(baseboard_id, error);
            }
        }
    }

    results
}

/// Retrieve up to `num_clients` clients for in-service sled agents.
///
/// Returns an error if no clients are available.
#[allow(clippy::too_many_arguments)]
async fn get_sled_agent_clients(
    log: &Logger,
    opctx: &OpContext,
    datastore: &DataStore,
    rack_id: RackUuid,
    epoch: Epoch,
    existing_sleds: &BTreeSet<BaseboardId>,
    num_clients: usize,
    timeout: Duration,
) -> Result<Vec<(BaseboardId, sled_agent_client::Client)>, Error> {
    // First shuffle the possible members
    let mut randomized: Vec<_> = existing_sleds.iter().cloned().collect();
    randomized.shuffle(&mut rand::rng());
    let mut clients = Vec::with_capacity(num_clients);

    let reqwest_client = internal_http_client_builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .map_err(|e| {
            anyhow!(
                "failed to create reqwest client for sled agent: {}",
                InlineErrorChain::new(&e)
            )
        })?;

    // Now try to get a client for `num_clients`
    for id in randomized {
        let client =
            match sled_client_by_baseboard_id_and_rack_id_if_in_service_ext(
                &datastore,
                opctx,
                &id,
                rack_id,
                log,
                reqwest_client.clone(),
            )
            .await
            {
                Ok(client) => client,
                Err(InServiceSledClientError::NotInService { .. }) => continue,
                Err(error @ InServiceSledClientError::LookupFailed { .. }) => {
                    return Err(error.into());
                }
            };
        clients.push((id.clone(), client));
        if clients.len() == num_clients {
            break;
        }
    }

    if clients.is_empty() {
        bail!(format!(
            "No sled-agent clients could be retrieved for trust quorum \
            configuration with rack_id {rack_id} and epoch {epoch}"
        ));
    }

    Ok(clients)
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
    pub fn write_to_string(&self, s: &mut String) {
        swriteln!(
            s,
            "    Commit Status from client {}:",
            self.client_destination
        );
        swrite!(s, "      acked: ");
        swriteln!(s, "{}", itertools::join(&self.acked, ", "));
        swrite!(s, "      pending: ");
        swriteln!(s, "{}", itertools::join(&self.pending, ", "));
        swriteln!(s, "      errors:");
        for e in &self.errors {
            swriteln!(s, "        {e}");
        }
    }
}

struct StartSledAgentResults {
    client_dest: BaseboardId,
    started: Vec<(BaseboardId, SledUuid)>,
    errors: BTreeMap<BaseboardId, Error>,
}

impl StartSledAgentResults {
    pub fn new(client_dest: BaseboardId) -> Self {
        Self { client_dest, started: vec![], errors: BTreeMap::new() }
    }

    pub fn write_to_string(&self, s: &mut String) {
        swriteln!(
            s,
            "    StartSledAgent Status from client {}:",
            self.client_dest
        );

        for (baseboard_id, sled_id) in &self.started {
            swriteln!(s, "      started:");
            swriteln!(
                s,
                "        baseboard_id: {baseboard_id}, sled_id: {sled_id}"
            );
        }
        for (baseboard_id, err) in &self.errors {
            swriteln!(s, "      error:");
            swriteln!(s, "        {baseboard_id}: {err}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
    use nexus_types::trust_quorum::{
        IsLrtqUpgrade, ProposedTrustQuorumConfig, TrustQuorumConfigState,
    };
    use omicron_test_utils::dev::test_setup_log;
    use trust_quorum_types::configuration::Configuration;
    use trust_quorum_types::crypto::{
        EncryptedRackSecrets, Salt, Sha3_256Digest,
    };
    use trust_quorum_types::membership::TrustQuorumMembers;
    use uuid::Uuid;

    async fn load_config(
        opctx: &OpContext,
        datastore: &DataStore,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> NexusTrustQuorumConfig {
        datastore
            .tq_get_config(
                opctx,
                authz::TrustQuorumConfig::for_rack_id(rack_id),
                epoch,
            )
            .await
            .expect("loaded trust quorum config")
            .unwrap_or_else(|| {
                panic!("trust quorum config at epoch {epoch} exists")
            })
    }

    #[tokio::test]
    async fn test_record_prepare_status_secrets_after_digests() {
        let logctx =
            test_setup_log("test_record_prepare_status_secrets_after_digests");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids: Vec<_> = (0..5)
            .map(|_| HwBaseboardId {
                id: Uuid::new_v4(),
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            })
            .collect();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        {
            use nexus_db_schema::schema::hw_baseboard_id::dsl;
            diesel::insert_into(dsl::hw_baseboard_id)
                .values(hw_ids.clone())
                .execute_async(&*conn)
                .await
                .expect("inserted hw baseboard ids");
        }
        let members: BTreeSet<BaseboardId> =
            hw_ids.into_iter().map(BaseboardId::from).collect();

        let rack_id = RackUuid::new_v4();
        for baseboard_id in &members {
            sled_upsert_for_baseboard(datastore, rack_id, baseboard_id).await;
        }
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            authz::TrustQuorumConfig::for_rack_id(rack_id),
            TrustQuorumMembers::new(members.clone())
                .expect("test membership is within the size bounds"),
            members.first().expect("at least one member").clone(),
        )
        .await
        .expect("inserted epoch 1");
        datastore
            .tq_insert_latest_config(
                opctx,
                ProposedTrustQuorumConfig {
                    rack_id,
                    epoch: Epoch(2),
                    is_lrtq_upgrade: IsLrtqUpgrade::No {
                        last_committed_epoch: Epoch(1),
                    },
                    members: TrustQuorumMembers::new(members.clone())
                        .expect("test members are within the size bounds"),
                },
            )
            .await
            .expect("inserted epoch 2");

        // In this first activation, while the coordinator is still collecting
        // old shares:
        //
        // * digests are known
        // * secrets are not
        // * nobody has acked
        let loaded_config =
            load_config(opctx, datastore, rack_id, Epoch(2)).await;
        let collecting_shares = Configuration {
            rack_id,
            epoch: Epoch(2),
            coordinator: loaded_config.coordinator.clone(),
            members: members
                .iter()
                .enumerate()
                .map(|(i, id)| {
                    let byte = u8::try_from(i + 1).expect("few members");
                    (id.clone(), Sha3_256Digest([byte; 32]))
                })
                .collect(),
            threshold: loaded_config.threshold,
            encrypted_rack_secrets: None,
        };
        let outcome = record_prepare_status(
            opctx,
            datastore,
            collecting_shares.clone(),
            BTreeSet::new(),
        )
        .await
        .expect("recorded prepare status");
        match outcome {
            TqPrepareStatusOutcome::StillPreparing => {}
            TqPrepareStatusOutcome::Committing { config: _ } => {
                panic!("expected StillPreparing, got {outcome:?}")
            }
        }

        // In this next activation, after the coordinator collected old shares
        // and sent prepares:
        //
        // * secrets are known
        // * enough members have acked
        let secrets =
            EncryptedRackSecrets::new(Salt([7; 32]), vec![1, 2, 3, 4].into());
        let preparing = Configuration {
            encrypted_rack_secrets: Some(secrets.clone()),
            ..collecting_shares.clone()
        };
        let min_acks = loaded_config.threshold.0 as usize
            + loaded_config.commit_crash_tolerance as usize;
        let acked: BTreeSet<_> =
            members.iter().take(min_acks).cloned().collect();
        let outcome = record_prepare_status(
            opctx,
            datastore,
            preparing.clone(),
            acked.clone(),
        )
        .await
        .expect("recorded prepare status");
        let commit_config = match outcome {
            TqPrepareStatusOutcome::Committing { config } => config,
            TqPrepareStatusOutcome::StillPreparing => {
                panic!("expected Committing, got {outcome:?}")
            }
        };

        // commit_config is the one written by the update, so it has the
        // expected secrets, and the acked members are prepared.
        assert_eq!(
            *commit_config,
            load_config(opctx, datastore, rack_id, Epoch(2)).await
        );
        assert_eq!(commit_config.state, TrustQuorumConfigState::Committing);
        let commit_wire_config = Configuration::try_from(&*commit_config)
            .expect("commit config has share digests");
        assert_eq!(commit_wire_config.encrypted_rack_secrets, Some(secrets));
        assert_eq!(commit_wire_config, preparing);
        for (id, member) in &commit_config.members {
            let expected = if acked.contains(id) {
                TrustQuorumMemberState::Prepared
            } else {
                TrustQuorumMemberState::Unacked
            };
            assert_eq!(member.state, expected, "member {id}");
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
