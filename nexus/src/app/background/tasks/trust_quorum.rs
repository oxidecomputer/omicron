// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for preparing and committing trust quorum configurations

use crate::app::background::BackgroundTask;
use crate::app::rack::rack_subnet;
use anyhow::{Context, Error, anyhow, bail};
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_model::SledUnderlaySubnetAllocation;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledUnderlayAllocationResult;
use nexus_networking::{
    sled_client_by_baseboard_id_and_rack_id_if_commissioned,
    sled_client_by_baseboard_id_and_rack_id_if_commissioned_ext,
};
use nexus_types::internal_api::background::TrustQuorumManagerStatus;
use nexus_types::trust_quorum::{
    TrustQuorumConfig as NexusTrustQuorumConfig, TrustQuorumConfigState,
    TrustQuorumMemberState,
};
use omicron_common::address::{Ipv6Subnet, RACK_PREFIX, get_64_subnet};
use omicron_uuid_kinds::{GenericUuid, RackUuid, SledUuid};
use parallel_task_set::ParallelTaskSet;
use rand::seq::SliceRandom;
use serde_json::json;
use sled_agent_client::types::AddSledRequest;
use sled_agent_client::types::StartSledAgentRequest;
use sled_agent_client::types::StartSledAgentRequestBody;
use sled_agent_client::types::{
    ProxyCommitRequest, ProxyPrepareAndCommitRequest,
};
use sled_hardware_types::BaseboardId;
use slog::{Logger, error, info, o};
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
    Committed(Vec<CommitOpsResults>, Vec<StartSledAgentResults>),
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
                    r.write_to_string(&mut s);
                }
            }
            Status::Committed(op_results, start_results) => {
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
        let state = datastore
            .tq_update_commit_status(
                &opctx,
                nexus_config.rack_id,
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

        // All sleds have acked commit. Let's see if any are newly added and
        // therefore require their sled agents to be started.
        //
        // TODO: Currently we can only attempt to start sled agents from the
        // Nexus that committed the configuration. This ensures a single Nexus
        // issues the attempts, which is necessary because the call to start
        // sled agents is neither idempotent nor concurrency safe.
        //
        // Unfortunately, this means that if the request fails here, it will
        // never be retried. We'll likely have to debug this by either catching
        // the error in the current call to the bg task status from omdb
        // (unlikely), or reading logs.
        //
        // We are forced to do this because of urgency. Fixing how
        // sled-agents get started has been a long standing issue:
        // https://github.com/oxidecomputer/omicron/issues/4494. We'd like
        // a reconciler pattern so that we could continuously retry from
        // nexus in a background task similar to what is described here:
        // https://github.com/oxidecomputer/omicron/issues/5132. However, this
        // is a substantial project and will have to come after the initial
        // trust quorum release.
        if state == TrustQuorumConfigState::Committed {
            let rack_id = nexus_config.rack_id;
            let epoch = nexus_config.epoch;
            let started_sleds = allocate_subnets_and_start_sled_agents(
                log,
                opctx,
                datastore,
                nexus_config,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to start sled agents for added sleds for \
                            rack {rack_id}, epoch {epoch}"
                )
            })?;
            return Ok(Status::Committed(client_results, started_sleds));
        }
    }

    Ok(Status::Commit(client_results))
}

async fn allocate_subnets_and_start_sled_agents(
    log: Logger,
    opctx: OpContext,
    datastore: Arc<DataStore>,
    committed_config: NexusTrustQuorumConfig,
) -> Result<Vec<StartSledAgentResults>, Error> {
    // No sleds could have been added to the trust quorum if there is no prior
    // committed configuration.
    let Some(last_committed_epoch) = committed_config.last_committed_epoch
    else {
        return Ok(vec![]);
    };

    let rack_id = committed_config.rack_id;
    let epoch = committed_config.epoch;

    // Retrieve the last committed configuration so we can diff members and see
    // who was added.
    let Some(last_committed_config) =
        datastore.tq_get_config(&opctx, rack_id, last_committed_epoch).await?
    else {
        bail!(
            "Failed to retrieve config from DB for rack {rack_id}, \
            last_committed_epoch {epoch}",
        );
    };

    let added_sleds: BTreeSet<_> = committed_config
        .members
        .keys()
        .filter(|&id| !last_committed_config.members.contains_key(id))
        .cloned()
        .collect();

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
                rack_id.into_untyped_uuid(),
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
        last_committed_config.members.keys().cloned().collect(),
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
    all_members: BTreeSet<BaseboardId>,
    allocations_by_baseboard_id: BTreeMap<
        BaseboardId,
        SledUnderlaySubnetAllocation,
    >,
) -> Result<Vec<StartSledAgentResults>, Error> {
    info!(log, "Looking up subnet for rack: {}", rack_id);
    let subnet =
        datastore.rack_subnet(&opctx, rack_id.into_untyped_uuid()).await?;
    let rack_subnet =
        Ipv6Subnet::<RACK_PREFIX>::from(rack_subnet(Some(subnet))?);

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
        &all_members,
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
    rack_subnet: Ipv6Subnet<RACK_PREFIX>,
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
                    rack_id: allocation.rack_id,
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

/// Retrieve up to `num_clients` clients for commissioned sled agents.
///
/// Returns an error if no clients are available.
#[allow(clippy::too_many_arguments)]
async fn get_sled_agent_clients(
    log: &Logger,
    opctx: &OpContext,
    datastore: &DataStore,
    rack_id: RackUuid,
    epoch: Epoch,
    all_members: &BTreeSet<BaseboardId>,
    num_clients: usize,
    timeout: Duration,
) -> Result<Vec<(BaseboardId, sled_agent_client::Client)>, Error> {
    // First shuffle the possible members
    let mut randomized: Vec<_> = all_members.iter().cloned().collect();
    randomized.shuffle(&mut rand::rng());
    let mut clients = Vec::with_capacity(num_clients);

    let reqwest_client = reqwest::ClientBuilder::new()
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
        let Some(client) =
            sled_client_by_baseboard_id_and_rack_id_if_commissioned_ext(
                &datastore,
                opctx,
                &id,
                rack_id,
                log,
                reqwest_client.clone(),
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
