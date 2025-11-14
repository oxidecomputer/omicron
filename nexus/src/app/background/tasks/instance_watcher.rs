// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use crate::app::background::BackgroundTask;
use crate::app::instance::SledAgentInstanceError;
use crate::app::saga::StartSaga;
use futures::{FutureExt, future::BoxFuture};
use nexus_db_model::Instance;
use nexus_db_model::Project;
use nexus_db_model::Sled;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::SledVmmState;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use oximeter::types::ProducerRegistry;
use parallel_task_set::ParallelTaskSet;
use sled_agent_client::Client as SledAgentClient;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

oximeter::use_timeseries!("vm-health-check.toml");
use virtual_machine::VirtualMachine;

/// Background task that periodically checks instance states.
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
    metrics: Arc<Mutex<metrics::Metrics>>,
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
        id: WatcherIdentity,
    ) -> Self {
        let metrics = Arc::new(Mutex::new(metrics::Metrics::default()));
        producer_registry
            .register_producer(metrics::Producer(metrics.clone()))
            .unwrap();
        Self { datastore, sagas, metrics, id }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        client: SledAgentClient,
        target: VirtualMachine,
        vmm: Vmm,
        sled: Sled,
    ) -> impl Future<Output = Check> + Send + 'static + use<> {
        let datastore = self.datastore.clone();
        let sagas = self.sagas.clone();

        let vmm_id = PropolisUuid::from_untyped_uuid(target.vmm_id);
        let opctx = {
            let mut meta = std::collections::BTreeMap::new();
            meta.insert(
                "instance_id".to_string(),
                target.instance_id.to_string(),
            );
            meta.insert("vmm_id".to_string(), vmm_id.to_string());
            meta.insert("sled_id".to_string(), sled.id().to_string());
            opctx.child(meta)
        };

        async move {
            slog::trace!(
                opctx.log, "checking on VMM"; "propolis_id" => %vmm_id
            );

            let mut check = Check {
                target,
                outcome: Default::default(),
                result: Ok(()),
                update_saga_queued: false,
            };

            let state = if sled.policy() == SledPolicy::Expunged {
                // If the sled has been expunged, any VMMs still on that sled
                // should be marked as `Failed`.
                slog::info!(
                    opctx.log,
                    "instance is assigned to a VMM on an Expunged sled; \
                     marking it as Failed";
                );
                check.outcome = CheckOutcome::Failure(Failure::SledExpunged);
                // TODO(eliza): it would be nicer if this used the same
                // code path as `mark_instance_failed`...
                SledVmmState {
                    vmm_state: nexus::VmmRuntimeState {
                        generation: vmm.runtime.generation.0.next(),
                        state: nexus::VmmState::Failed,
                        time_updated: chrono::Utc::now(),
                    },
                    // It's fine to synthesize `None`s here because a `None`
                    // just means "don't update the migration state", not
                    // "there is no migration".
                    migration_in: None,
                    migration_out: None,
                }
            } else {
                // Otherwise, ask the sled-agent what it has to say for itself.
                let rsp = client
                    .vmm_get_state(&vmm_id)
                    .await
                    .map_err(SledAgentInstanceError);
                match rsp {
                    Ok(rsp) => {
                        let state: SledVmmState = rsp.into_inner().into();
                        check.outcome =
                            CheckOutcome::Success(state.vmm_state.state.into());
                        state
                    }
                    // Oh, this error indicates that the VMM should transition to
                    // `Failed`. Let's synthesize a `SledInstanceState` that does
                    // that.
                    Err(e) if e.vmm_gone() => {
                        slog::info!(
                            opctx.log,
                            "sled-agent error indicates that this instance's \
                             VMM has failed!";
                            "error" => %e,
                        );
                        check.outcome =
                            CheckOutcome::Failure(Failure::NoSuchInstance);
                        // TODO(eliza): it would be nicer if this used the same
                        // code path as `mark_instance_failed`...
                        SledVmmState {
                            vmm_state: nexus::VmmRuntimeState {
                                generation: vmm.runtime.generation.0.next(),
                                state: nexus::VmmState::Failed,
                                time_updated: chrono::Utc::now(),
                            },
                            // It's fine to synthesize `None`s here because a `None`
                            // just means "don't update the migration state", not
                            // "there is no migration".
                            migration_in: None,
                            migration_out: None,
                        }
                    }
                    Err(SledAgentInstanceError(
                        ClientError::ErrorResponse(rsp),
                    )) => {
                        let status = rsp.status();
                        if status.is_client_error() {
                            slog::warn!(opctx.log, "check incomplete due to client error";
                            "status" => ?status, "error" => ?rsp.into_inner());
                        } else {
                            slog::info!(opctx.log, "check incomplete due to server error";
                        "status" => ?status, "error" => ?rsp.into_inner());
                        }

                        check.result = Err(Incomplete::SledAgentHttpError(
                            status.as_u16(),
                        ));
                        return check;
                    }
                    Err(SledAgentInstanceError(
                        ClientError::CommunicationError(e),
                    )) => {
                        // TODO(eliza): eventually, we may want to transition the
                        // instance to the `Failed` state if the sled-agent has been
                        // unreachable for a while. We may also want to take other
                        // corrective actions or alert an operator in this case.
                        //
                        // TODO(eliza): because we have the purported IP address
                        // of the instance's VMM from our database query, we could
                        // also ask the VMM directly when the sled-agent is
                        // unreachable. We should start doing that here at some
                        // point.
                        slog::info!(opctx.log, "sled agent is unreachable"; "error" => ?e);
                        check.result = Err(Incomplete::SledAgentUnreachable);
                        return check;
                    }
                    Err(SledAgentInstanceError(e)) => {
                        slog::warn!(
                            opctx.log,
                            "error checking up on instance";
                            "error" => ?e,
                            "status" => ?e.status(),
                        );
                        check.result = Err(Incomplete::ClientError);
                        return check;
                    }
                }
            };

            debug!(
                opctx.log,
                "updating instance state";
                "state" => ?state,
            );
            match crate::app::instance::process_vmm_update(
                &datastore,
                &opctx,
                PropolisUuid::from_untyped_uuid(target.vmm_id),
                &state,
            )
            .await
            {
                Err(e) => {
                    warn!(opctx.log, "error updating instance"; "error" => %e);
                    check.result = match e {
                        Error::ObjectNotFound { .. } => {
                            Err(Incomplete::InstanceNotFound)
                        }
                        _ => Err(Incomplete::UpdateFailed),
                    };
                }
                Ok(Some((_, saga))) => match sagas.saga_run(saga).await {
                    Ok((saga_id, completed)) => {
                        check.update_saga_queued = true;
                        if let Err(e) = completed.await {
                            warn!(
                                opctx.log,
                                "update saga failed";
                                "saga_id" => %saga_id,
                                "error" => e,
                            );
                            check.result = Err(Incomplete::UpdateFailed);
                        }
                    }
                    Err(e) => {
                        warn!(
                            opctx.log,
                            "update saga could not be started";
                            "error" => e,
                        );
                        check.result = Err(Incomplete::UpdateFailed);
                    }
                },
                Ok(None) => {}
            };

            check
        }
    }
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

    /// The outcome of performing this check. Either we were able to reach the
    /// sled-agent that owns this instance and it told us the instance's state
    /// and VMM, or we the health check failed in a way that suggests a
    /// potential issue with the sled-agent or instance.
    ///
    /// If we were not able to perform the request at all due to an error on
    /// *our* end, this will be `None`.
    outcome: CheckOutcome,

    /// `Some` if the instance check was unsuccessful.
    ///
    /// This indicates that something went wrong *while performing the check* that
    /// does not necessarily indicate that the instance itself is in a bad
    /// state. For example, the sled-agent client may have constructed an
    /// invalid request, or an error may have occurred while updating the
    /// instance in the database.
    ///
    /// Depending on when the error occurred, the `outcome` field may also
    /// be populated.
    result: Result<(), Incomplete>,

    update_saga_queued: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
enum CheckOutcome {
    Success(InstanceState),
    Failure(Failure),
    #[default]
    Unknown,
}

impl Check {
    fn state_str(&self) -> Cow<'static, str> {
        match self.outcome {
            CheckOutcome::Success(state) => state.label().into(),
            CheckOutcome::Failure(_) => InstanceState::Failed.label().into(),
            CheckOutcome::Unknown => "unknown".into(),
        }
    }

    fn reason_str(&self) -> Cow<'static, str> {
        match self.outcome {
            CheckOutcome::Success(_) => "success".into(),
            CheckOutcome::Failure(reason) => reason.as_str(),
            CheckOutcome::Unknown => match self.result {
                Ok(()) => "unknown".into(), // this shouldn't happen, but there's no way to prevent it from happening,
                Err(e) => e.as_str(),
            },
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
}

impl Failure {
    fn as_str(&self) -> Cow<'static, str> {
        match self {
            Self::NoSuchInstance => "no_such_instance".into(),
            Self::SledExpunged => "sled_expunged".into(),
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
    /// This may indicate a network partition between us and that sled, that
    /// the sled-agent process has crashed, or that the sled is down.
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

                let completed_check = if let Some((sled, instance, vmm, project)) = instances.pop_front() {
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
                    tasks.spawn(self.check_instance(opctx, client, target ,vmm ,sled)).await
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

                if let Some(check) = completed_check {
                    total += 1;
                    match check.outcome {
                        CheckOutcome::Success(state) => {
                            *instance_states
                                .entry(state.to_string())
                                .or_default() += 1;
                        }
                        CheckOutcome::Failure(reason) => {
                            *check_failures.entry(reason.as_str().into_owned()).or_default() += 1;
                        }
                        CheckOutcome::Unknown => {
                            if let Err(reason) = check.result {
                                *check_errors.entry(reason.as_str().into_owned()).or_default() += 1;
                            }
                        }
                    }
                    if check.update_saga_queued {
                        update_sagas_queued += 1;
                    }
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
