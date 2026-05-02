// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use crate::app::background::BackgroundTask;
use crate::app::instance::SledAgentInstanceError;
use crate::app::saga::SagaCompletionFuture;
use crate::app::saga::StartSaga;
use futures::{FutureExt, future::BoxFuture};
use gateway_client::types::PowerState;
use nexus_db_model::Instance;
use nexus_db_model::Project;
use nexus_db_model::Sled;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_networking::GatewayClient;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use nexus_types::inventory;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use oximeter::types::ProducerRegistry;
use parallel_task_set::ParallelTaskSet;
use sled_agent_client::Client as SledAgentClient;
use sled_agent_types::instance;
use sled_agent_types::instance::SledVmmState;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use uuid::Uuid;

oximeter::use_timeseries!("vm-health-check.toml");
use virtual_machine::VirtualMachine;

/// Background task that periodically checks instance states.
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
    resolver: internal_dns_resolver::Resolver,
    metrics: Arc<Mutex<metrics::Metrics>>,
    inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
    id: WatcherIdentity,
}

/// Determines how many instance checks and their subsequent update sagas (if
/// the instance's state has changed) can execute concurrently.  If this
/// number is too high, there's risk that this task could starve other Nexus
/// activities or overload the database or overload one or more sled agents.
///
/// The only consequence of the number being too low is that it may take longer
/// for the system to notice an instance's VMM state requires an instance state
/// transition, which translates to increased latency between when events (like
/// a VMM crash or a completed migration) occur, and when the necessary actions
/// (like releasing unused resource allocations or allowing an instance to
/// be restarted or deleted) are performed.  However, in the happy path, instance
/// update sagas execute immediately upon receipt of a VMM state update pushed
/// by a sled-agent in `cpapi_instances_put`.  Therefore, discovering a needed
/// state transition a health check only occurs if the pushed state update was
/// not handled correctly, or if the sled-agent itself has restarted and
/// forgotten about the instances it was supposed to know about.  For now, we
/// tune this pretty low, choosing safety over low recovery latency for these
/// relatively rare events.
const MAX_CONCURRENT_CHECKS: usize = 16;

impl InstanceWatcher {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        sagas: Arc<dyn StartSaga>,
        producer_registry: &ProducerRegistry,
        resolver: internal_dns_resolver::Resolver,
        inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
        id: WatcherIdentity,
    ) -> Self {
        let metrics = Arc::new(Mutex::new(metrics::Metrics::default()));
        producer_registry
            .register_producer(metrics::Producer(metrics.clone()))
            .unwrap();
        Self { datastore, sagas, metrics, id, resolver, inv_rx }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        gateways: &Arc<[GatewayClient]>,
        client: SledAgentClient,
        target: VirtualMachine,
        vmm: Vmm,
        sled: Sled,
    ) -> impl Future<Output = Check> + Send + 'static + use<> {
        let datastore = self.datastore.clone();
        let sagas = self.sagas.clone();
        let gateways = gateways.clone();
        let inv_rx = self.inv_rx.clone();

        let vmm_id = PropolisUuid::from_untyped_uuid(target.vmm_id);
        let opctx = {
            let mut meta = std::collections::BTreeMap::new();
            meta.insert(
                "instance_id".to_string(),
                target.instance_id.to_string(),
            );
            meta.insert("vmm_id".to_string(), vmm_id.to_string());
            meta.insert("sled_id".to_string(), sled.id().to_string());
            meta.insert(
                "sled_serial".to_string(),
                sled.serial_number().to_string(),
            );
            opctx.child(meta)
        };

        async move {
            slog::trace!(
                opctx.log, "checking on VMM"; "propolis_id" => %vmm.id
            );

            let check_result = async {
                let outcome =
                    run_check(&opctx, inv_rx, &sled, &vmm, &gateways, &client)
                        .await?;

                let state = match check.outcome {
                    CheckOutcome::Success(ref state) => state,
                    CheckOutcome::Failure(_) => {
                        // TODO(eliza): it would be nicer if this used the same
                        // code path as `mark_instance_failed`...
                        &SledVmmState {
                            vmm_state: instance::VmmRuntimeState {
                                generation: vmm.generation.0.next(),
                                state: instance::VmmState::Failed,
                                time_updated: chrono::Utc::now(),
                            },
                            // It's fine to synthesize `None`s here because a `None`
                            // just means "don't update the migration state", not
                            // "there is no migration".
                            migration_in: None,
                            migration_out: None,
                        }
                    }
                };

                let mut update_saga_started = false;
                let update_result = match update_vmm_state(&datastore, &opctx, &sagas, vmm_id, &state).await {
                    Ok(None) => Ok(()),
                    Ok(Some((saga_id, saga_completed))) => {
                        update_saga_started = true;
                         saga_completed.await.map_err(|error| {
                             warn!(
                                 opctx.log,
                                 "update saga failed";
                                 "saga_id" => %saga_id,
                                 "error" => &error,
                             );
                             Err(UpdateError::SagaFailed { saga_id, error })
                         })
                    }
                    Err(error) => {
                        error!(opctx.log, "error updating VMM record"; &InlineErrorChain::new(&error));
                        Err(error)
                    },
                };


                Ok(CompletedCheck {
                    outcome, update_result, update_saga_started
                })
            }.await;
            Check { target, check_result }
        }
    }
}

async fn run_check(
    opctx: &OpContext,
    inv_rx: watch::Receiver<Option<Arc<inventory::Collection>>>,
    sled: &Sled,
    vmm: &Vmm,
    gateways: &[GatewayClient],
    client: &SledAgentClient,
) -> Result<CheckOutcome, Incomplete> {
    if sled.policy() == SledPolicy::Expunged {
        // If the sled has been expunged, any VMMs still on that sled
        // should be marked as `Failed`.
        slog::info!(
            opctx.log,
            "instance is assigned to a VMM on an Expunged sled; \
             marking it as Failed";
        );
        return Ok(CheckOutcome::Failure(Failure::SledExpunged));
    }

    // Ask the sled-agent what it has to say for itself.
    let rsp = client
        .vmm_get_state(&PropolisUuid::from_untyped_uuid(vmm.id))
        .await
        .map_err(SledAgentInstanceError);
    match rsp {
        // We received a response from the sled-agent. We shall update the
        // instance's state to match.
        //
        // Note that this does not always mean that the *VMM* is healthy,
        // but only that we successfully got its state from the sled-agent.
        Ok(rsp) => {
            let state = rsp.into_inner();
            Ok(CheckOutcome::Success(state))
        }
        // Oh, this error indicates that the VMM should transition to
        // `Failed`. Let's synthesize a `SledInstanceState` that does
        // that.
        Err(e) if e.vmm_gone() => {
            let error = InlineErrorChain::new(&e);
            slog::info!(
                opctx.log,
                "sled-agent error indicates that this instance's \
                 VMM has failed!";
                error,
            );
            Ok(CheckOutcome::Failure(Failure::NoSuchInstance))
        }
        // We were able to contact the sled-agent, but it responded with an
        // error which does *not* tell us that the VMM has failed. Either
        // the sled-agent is unhealthy, or we sent an invalid request for
        // some reason. In either case, the check is inconclusive and the
        // instance's state will not change.
        Err(SledAgentInstanceError(ClientError::ErrorResponse(rsp))) => {
            // This is a bit goofy: we destructure the error because
            // `ResponseValue` has a `status()` which is not optional (so we
            // don't have to unwrap it), but then we re-construct the
            // `progenitor_client::Error` because we would like to format it
            // with `InlineErrorChain`. This looks silly but there's nothing
            // actually *wrong* with it...
            let status = rsp.status();
            let error = ClientError::ErrorResponse(rsp);
            let error = InlineErrorChain::new(&error);
            if status.is_client_error() {
                slog::warn!(
                    opctx.log,
                    "check incomplete due to client error";
                    "status" => ?status,
                    "error" => error,
                );
            } else {
                slog::info!(
                    opctx.log,
                    "check incomplete due to server error";
                    "status" => ?status,
                    "error" => error,
                );
            }

            Err(Incomplete::SledAgentHttpError(status.as_u16()))
        }
        // We were unable to communicate with the sled-agent. This could be
        // due to a network partition between us and the sled-agent, or
        // because the sled is powered off or not present. We will use the
        // management network to check whether the sled is present and
        // powered on. If we determine conclusively that it is not present
        // and in A0, the instance is moved to `Failed`. Otherwise, the
        // check is inconclusive and the instance's state will not change.
        Err(SledAgentInstanceError(ClientError::CommunicationError(e))) => {
            slog::info!(
                opctx.log,
                "sled-agent is unreachable";
                InlineErrorChain::new(&e),
            );

            // Is your computer running?
            match is_computer_on(&opctx, &inv_rx, &gateways, &sled).await {
                // ...impossible to tell! Results are inconclusive.
                Err(error) => {
                    let error = InlineErrorChain::new(&*error);
                    warn!(
                        opctx.log,
                        "sled-agent is unreachable, but we cannot \
                         determine if your computer is running";
                        error
                    );
                }

                // ...better go catch it!
                Ok(PowerState::A0) => {}

                // It is not running, the instance can't possibly be there.
                Ok(state) => {
                    slog::info!(
                        opctx.log,
                        "instance is assigned to a VMM on a sled that is not \
                         in A0, marking it as Failed";
                        "sled_power_state" => ?state,
                    );
                    return Ok(CheckOutcome::Failure(Failure::SledOff));
                }
            }

            Err(Incomplete::SledAgentUnreachable)
        }
        // Any other errors mean that the check is inconclusive.
        Err(SledAgentInstanceError(e)) => {
            slog::warn!(
                opctx.log,
                "error checking up on instance";
                "error" => InlineErrorChain::new(&e),
                "status" => ?e.status(),
            );
            Err(Incomplete::ClientError)
        }
    }
}

async fn update_vmm_state(
    datastore: &DataStore,
    opctx: &OpContext,
    sagas: &Arc<dyn StartSaga>,
    vmm_id: PropolisUuid,
    state: &SledVmmState,
) -> Result<Option<(steno::SagaId, SagaCompletionFuture)>, UpdateError> {
    debug!(
        opctx.log,
        "updating instance state";
        "state" => ?state,
    );
    let update = crate::app::instance::process_vmm_update(
        &datastore, &opctx, vmm_id, &state,
    )
    .await
    .map_err(|e| match e {
        Error::ObjectNotFound { .. } => UpdateError::InstanceNotFound,
        e => UpdateError::UpdateVmm(e),
    })?;
    let Some((_, saga)) = update else { return Ok(None) };
    sagas.saga_run(saga).await.map_err(UpdateError::StartSaga).map(Some)
}

/// An implementation of the `is_computer_on()` function originally defined
/// in the BeOS C library][1].
///
/// [1]: https://web.archive.org/web/20260309055003/https://www.haiku-os.org/legacy-docs/bebook/TheKernelKit_SystemInfo.html
async fn is_computer_on(
    opctx: &OpContext,
    inv_rx: &watch::Receiver<Option<Arc<inventory::Collection>>>,
    gateways: &[GatewayClient],
    sled: &Sled,
) -> anyhow::Result<PowerState> {
    let Some(inv) = inv_rx.borrow().clone() else {
        anyhow::bail!(
            "cannot find sled to ask if it's turned on, since there is no \
             inventory collection yet"
        );
    };

    let part_number = sled.part_number();
    let rev = sled.revision();
    let serial_number = sled.serial_number();

    // TODO(eliza): this sucks, i would rather not have to memcpy the strings
    // here...this would be nicer if we iterated over sled-agents from the
    // inventory in the main loop. change that!
    let bb = BaseboardId {
        part_number: part_number.to_owned(),
        serial_number: serial_number.to_owned(),
    };
    let Some(sp) = inv.sps.get(&bb) else {
        anyhow::bail!(
            "SP for sled {part_number}:{rev}:{serial_number} is not in the \
             inventory"
        );
    };

    // Ask MGS whether the computer is on. If trying to talk to one of the
    // gateways fails for whatever reason, ask any others we were able to
    // resolve as well.
    for GatewayClient { client, addr } in gateways {
        let state = match client.sp_get(&sp.sp_type, sp.sp_slot).await {
            Ok(response) => response.into_inner(),
            Err(error) => {
                let error = InlineErrorChain::new(&error);
                slog::warn!(
                    &opctx.log,
                    "requesting sled SP status from MGS failed";
                    "gateway_addr" => %addr,
                    "error" => &error,
                );
                continue;
            }
        };

        if state.serial_number != sled.serial_number() {
            // Well, this is surprising; is our inventory stale? In any case, if
            // we can't find the actual location of the sled, we can't determine
            // whether or not it's turned on...
            anyhow::bail!(
                "expected sled {part_number}:{rev}:{serial_number} to be in \
                 slot {}, but found sled {}:{}:{} instead",
                sp.sp_slot,
                state.model,
                state.revision,
                state.serial_number,
            );
        }

        return Ok(state.power_state);
    }

    Err(anyhow::anyhow!(
        "no MGS could determine if the sled is on (asked {})",
        gateways.len()
    ))
}

/// The identity of the process performing the health check, for distinguishing
/// health check metrics emitted by different Nexus instances.
///
/// This is a struct just to ensure that the two UUIDs are named arguments
/// (rather than positional arguments) and can't be swapped accidentally.
#[derive(Copy, Clone)]
pub struct WatcherIdentity {
    pub nexus_id: OmicronZoneUuid,
    pub rack_id: Uuid,
}

impl VirtualMachine {
    fn new(
        WatcherIdentity { rack_id, nexus_id }: WatcherIdentity,
        sled: &Sled,
        instance: &Instance,
        vmm: &Vmm,
        project: &Project,
    ) -> Self {
        let addr = sled.address();
        Self {
            rack_id,
            nexus_id: nexus_id.into_untyped_uuid(),
            instance_id: instance.id(),
            silo_id: project.silo_id,
            project_id: project.id(),
            vmm_id: vmm.id,
            // XXX oximeter cannot do typed uuids?
            sled_agent_id: sled.id().into_untyped_uuid(),
            sled_agent_ip: (*addr.ip()).into(),
            sled_agent_port: addr.port(),
        }
    }
}

struct Check {
    target: VirtualMachine,

    check_result: Result<CompletedCheck, Incomplete>,
}

struct CompletedCheck {
    outcome: CheckOutcome,
    update_result: Result<(), UpdateError>,
    update_saga_started: bool,
}

#[derive(Debug, Clone)]
enum CheckOutcome {
    Success(SledVmmState),
    Failure(Failure),
}

impl Check {
    fn state_str(&self) -> Cow<'static, str> {
        match self.check_result {
            Ok(CompletedCheck {
                outcome: CheckOutcome::Success(ref state),
                ..
            }) => InstanceState::from(state.vmm_state.state).label().into(),
            Ok(CompletedCheck {
                outcome: CheckOutcome::Failure(_), ..
            }) => InstanceState::Failed.label().into(),
            Err(_) => "unknown".into(),
        }
    }

    fn reason_str(&self) -> Cow<'static, str> {
        match self.check_result {
            Ok(CompletedCheck {
                outcome: CheckOutcome::Success(_), ..
            }) => "success".into(),
            Ok(CompletedCheck {
                outcome: CheckOutcome::Failure(reason),
                ..
            }) => reason.as_str(),
            Err(e) => e.as_str(),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize,
)]
enum Failure {
    /// The sled-agent indicated that it doesn't know about an instance ID that
    /// we believe it *should* know about. This probably means the sled-agent,
    /// and potentially the whole sled, has been restarted.
    NoSuchInstance,
    /// The instance was assigned to a sled that was expunged. Its VMM has been
    /// marked as `Failed`, since the sled is no longer present.
    SledExpunged,
    /// The instance was assigned to a sled that is not currently in A0.
    /// Its VMM has been marked as `Failed`, since the computer it's on is not,
    /// you know...turned on.
    SledOff,
}

impl Failure {
    fn as_str(&self) -> Cow<'static, str> {
        match self {
            Self::NoSuchInstance => "no_such_instance".into(),
            Self::SledExpunged => "sled_expunged".into(),
            Self::SledOff => "sled_off".into(),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize,
)]
enum Incomplete {
    /// Something else went wrong while making an HTTP request.
    ClientError,
    /// The sled-agent for the sled on which the instance is running was
    /// unreachable.
    ///
    /// This may indicate a network partition between us and that sled, or that
    /// the sled-agent process has crashed.
    SledAgentUnreachable,
    /// The sled-agent responded with an unexpected HTTP error.
    SledAgentHttpError(u16),
    /// We attempted to update the instance state in the database, but no
    /// instance with that UUID existed.
    ///
    /// Because the instance UUIDs that we perform checks on come from querying
    /// the instances table, this would probably indicate that the instance was
    /// removed from the database between when we listed instances and when the
    /// check completed.
    InstanceNotFound,
    /// Something went wrong while updating the state of the instance in the
    /// database.
    UpdateFailed,
}

impl Incomplete {
    fn as_str(&self) -> Cow<'static, str> {
        match self {
            Self::ClientError => "client_error".into(),
            Self::SledAgentHttpError(status) => status.to_string().into(),
            Self::SledAgentUnreachable => "unreachable".into(),
            Self::InstanceNotFound => "instance_not_found".into(),
            Self::UpdateFailed => "update_failed".into(),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
enum UpdateError {
    #[error("instance not found")]
    InstanceNotFound,
    #[error("failed to update VMM record")]
    UpdateVmm(#[source] Error),
    #[error("failed to start instance_update saga")]
    StartSaga(#[source] Error),
    #[error("instance_update saga {saga_id} failed")]
    SagaFailed {
        saga_id: steno::SagaId,
        #[source]
        error: Error,
    },
}

type ClientError = sled_agent_client::Error<sled_agent_client::types::Error>;

impl BackgroundTask for InstanceWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut paginator = Some(Paginator::new(
                nexus_db_queries::db::datastore::SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending
            ));
            let mut tasks = ParallelTaskSet::new_with_parallelism(
                MAX_CONCURRENT_CHECKS,
            );

            let gateways = match GatewayClient::resolve_all_gateways(&opctx.log, &self.resolver).await {
                Ok(gateways) => gateways.collect::<Arc<[_]>>(),
                Err(err) => {
                    // This is a bit sad, but talking to MGS is only necessary
                    // to check if a sled is powered on in the event that the
                    // sled-agent is unreachable. If we don't see communication
                    // errors talking to sled-agents, we can confirm that VMMs
                    // are, or are not, healthy, even if we are unable to talk
                    // to MGS.
                    slog::warn!(
                        opctx.log,
                        "failed to resolve any management gateways; we will \
                         not be checking if sleds are powered on, should \
                         sled-agents be unreachable";
                        "error" => InlineErrorChain::new(&*err),
                    );
                    Arc::new([])
                },
            };

            let mut update_sagas_queued: usize = 0;
            let mut instance_states: BTreeMap<String, usize> =
                BTreeMap::new();
            let mut check_failures: BTreeMap<String, usize> =
                BTreeMap::new();
            let mut check_errors: BTreeMap<String, usize> = BTreeMap::new();

            // A `reqwest` client is a reference-counted handle to a connection
            // pool that can be reused by multiple requests. Making a new client
            // is fairly expensive, but cloning one is cheap, and cloning it
            // allows reusing pooled TCP connections. Therefore, we will order
            // the database query by sled ID, and reuse the same sled-agent
            // client as long as we are talking to the same sled.
            let mut curr_client: Option<(SledUuid, SledAgentClient)> = None;
            let mut instances = VecDeque::new();
            let mut total: usize = 0;
            loop {
                if instances.is_empty() {
                   if let Some(p) = paginator.take().and_then(Paginator::next) {
                        let maybe_batch = self
                            .datastore
                            .instance_and_vmm_list_by_sled_agent(
                                opctx,
                                &p.current_pagparams(),
                            )
                            .await;
                        let batch = match maybe_batch {
                            Ok(batch) => batch,
                            Err(e) => {
                                slog::error!(
                                    opctx.log,
                                    "sled instances by sled agent query failed: {e}"
                                );
                                return serde_json::json!({ "error": e.to_string() });
                            }
                        };
                        paginator = Some(p.found_batch(&batch, &|(sled, _, vmm, _)| (sled.id().into_untyped_uuid(), vmm.id)));
                        instances = batch.into();
                    }
                }

                let check = if let Some((sled, instance, vmm, project)) = instances.pop_front() {
                    let client = match curr_client {
                        // If we are still talking to the same sled, reuse the
                        // existing client and its connection pool.
                        Some((sled_id, ref client)) if sled_id == sled.id() => client.clone(),
                        // Otherwise, if we've moved on to a new sled, refresh
                        // the client.
                        ref mut curr => {
                            let client = nexus_networking::sled_client_from_address(
                                sled.id(),
                                sled.address(),
                                &opctx.log,
                            );
                            *curr = Some((sled.id(), client.clone()));
                            client
                        },
                    };

                    let target = VirtualMachine::new(self.id, &sled, &instance, &vmm, &project);
                    tasks.spawn(self.check_instance(opctx, &gateways, client, target, vmm, sled)).await
                } else {
                    // If there are no remaining instances to check, wait for
                    // all previously spawned check to complete.
                    match tasks.join_next().await {
                        // The ParallelTaskSet` is empty, and there are no new
                        // instances to check, so we're done here.
                        None => break,
                        Some(check) => Some(check),
                    }
                };

                if let Some(check) = check {
                    total += 1;
                    // match check.check_result {
                    //     Ok(CheckOutcome::Success(state)) => {
                    //         *instance_states
                    //             .entry(state.to_string())
                    //             .or_default() += 1;
                    //     }
                    //     CheckOutcome::Failure(reason) => {
                    //         *check_failures.entry(reason.as_str().into_owned()).or_default() += 1;
                    //     }
                    //     CheckOutcome::Unknown => {
                    //         if let Err(reason) = check.result {
                    //             *check_errors.entry(reason.as_str().into_owned()).or_default() += 1;
                    //         }
                    //     }
                    // };
                    // if check.update_saga_queued {
                    //     update_sagas_queued += 1;
                    // }
                    self.metrics.lock().unwrap().record_check(check);
                }
            }

            // All requests completed! Prune any old instance metrics for
            // instances that we didn't check --- if we didn't spawn a check for
            // something, that means it wasn't present in the most recent
            // database query.
            let pruned = self.metrics.lock().unwrap().prune();

            slog::info!(opctx.log, "all instance checks complete";
                "total_instances" => total,
                "total_completed" => instance_states.len() + check_failures.len(),
                "total_failed" => check_failures.len(),
                "total_incomplete" => check_errors.len(),
                "update_sagas_queued" => update_sagas_queued,
                "pruned_instances" => pruned,
            );
            serde_json::json!({
                "total_instances": total,
                "instance_states": instance_states,
                "failed_checks": check_failures,
                "incomplete_checks": check_errors,
                "update_sagas_queued": update_sagas_queued,
                "pruned_instances": pruned,
            })
        }
        .boxed()
    }
}

mod metrics {
    use super::virtual_machine::Check;
    use super::virtual_machine::IncompleteCheck;
    use super::{CheckOutcome, Incomplete, VirtualMachine};
    use oximeter::MetricsError;
    use oximeter::Sample;
    use oximeter::types::Cumulative;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    pub(super) struct Metrics {
        instances: BTreeMap<VirtualMachine, Instance>,
    }

    #[derive(Debug)]
    pub(super) struct Producer(pub(super) Arc<Mutex<Metrics>>);

    #[derive(Debug, Default)]
    struct Instance {
        checks: BTreeMap<CheckOutcome, Check>,
        check_errors: BTreeMap<Incomplete, IncompleteCheck>,
        touched: bool,
    }

    impl Metrics {
        pub(crate) fn record_check(&mut self, check: super::Check) {
            let instance = self.instances.entry(check.target).or_default();
            instance
                .checks
                .entry(check.outcome)
                .or_insert_with(|| Check {
                    state: check.state_str(),
                    reason: check.reason_str(),
                    datum: Cumulative::default(),
                })
                .datum += 1;
            if let Err(error) = check.result {
                instance
                    .check_errors
                    .entry(error)
                    .or_insert_with(|| IncompleteCheck {
                        failure_reason: error.as_str(),
                        datum: Cumulative::default(),
                    })
                    .datum += 1;
            }
            instance.touched = true;
        }

        pub(super) fn prune(&mut self) -> usize {
            let len = self.instances.len();
            self.instances.retain(|_, instance| {
                std::mem::replace(&mut instance.touched, false)
            });
            len - self.instances.len()
        }

        fn len(&self) -> usize {
            self.instances.values().map(Instance::len).sum()
        }
    }

    impl oximeter::Producer for Producer {
        fn produce(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
            let metrics = self.0.lock().unwrap();
            let mut v = Vec::with_capacity(metrics.len());
            for (target, instance) in &metrics.instances {
                instance.sample_into(target, &mut v)?;
            }
            Ok(Box::new(v.into_iter()))
        }
    }

    impl Instance {
        fn len(&self) -> usize {
            self.checks.len() + self.check_errors.len()
        }

        fn sample_into(
            &self,
            target: &VirtualMachine,
            dest: &mut Vec<Sample>,
        ) -> Result<(), MetricsError> {
            for metric in self.checks.values() {
                dest.push(Sample::new(target, metric)?);
            }
            for metric in self.check_errors.values() {
                dest.push(Sample::new(target, metric)?);
            }
            Ok(())
        }
    }
}
