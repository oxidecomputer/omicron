// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use crate::app::background::BackgroundTask;
use crate::app::instance::SledAgentInstanceError;
use crate::app::saga::StartSaga;
use futures::{future::BoxFuture, FutureExt};
use nexus_db_model::Instance;
use nexus_db_model::Project;
use nexus_db_model::Sled;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::SledVmmState;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use oximeter::types::ProducerRegistry;
use sled_agent_client::Client as SledAgentClient;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU32;
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

const MAX_SLED_AGENTS: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

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
        client: &SledAgentClient,
        target: VirtualMachine,
        vmm: Vmm,
    ) -> impl Future<Output = Check> + Send + 'static {
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
            opctx.child(meta)
        };
        let client = client.clone();

        async move {
            slog::trace!(
                opctx.log, "checking on VMM"; "propolis_id" => %vmm_id
            );

            slog::trace!(opctx.log, "checking on instance...");
            let rsp = client
                .vmm_get_state(&vmm_id)
                .await
                .map_err(SledAgentInstanceError);
            let mut check = Check {
                target,
                outcome: Default::default(),
                result: Ok(()),
                update_saga_queued: false,
            };
            let state: SledVmmState = match rsp {
                Ok(rsp) => rsp.into_inner().into(),
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
                            r#gen: vmm.runtime.r#gen.0.next(),
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
                Err(SledAgentInstanceError(ClientError::ErrorResponse(
                    rsp,
                ))) => {
                    let status = rsp.status();
                    if status.is_client_error() {
                        slog::warn!(opctx.log, "check failed due to client error";
                            "status" => ?status, "error" => ?rsp.into_inner());
                        check.result =
                            Err(Incomplete::ClientHttpError(status.as_u16()));
                    } else {
                        slog::info!(opctx.log, "check failed due to server error";
                        "status" => ?status, "error" => ?rsp.into_inner());
                    }

                    check.outcome = CheckOutcome::Failure(
                        Failure::SledAgentResponse(status.as_u16()),
                    );
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
                    // TODO(eliza): because we have the preported IP address
                    // of the instance's VMM from our databse query, we could
                    // also ask the VMM directly when the sled-agent is
                    // unreachable. We should start doing that here at some
                    // point.
                    slog::info!(opctx.log, "sled agent is unreachable"; "error" => ?e);
                    check.outcome =
                        CheckOutcome::Failure(Failure::SledAgentUnreachable);
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
            };

            check.outcome = CheckOutcome::Success(state.vmm_state.state.into());
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
                Ok(Some((_, saga))) => {
                    check.update_saga_queued = true;
                    if let Err(e) = sagas.saga_start(saga).await {
                        warn!(opctx.log, "update saga failed"; "error" => ?e);
                        check.result = Err(Incomplete::UpdateFailed);
                    }
                }
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
    pub nexus_id: Uuid,
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
            nexus_id,
            instance_id: instance.id(),
            silo_id: project.silo_id,
            project_id: project.id(),
            vmm_id: vmm.id,
            sled_agent_id: sled.id(),
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
    /// The sled-agent for the sled on which the instance is running was
    /// unreachable.
    ///
    /// This may indicate a network partition between us and that sled, that
    /// the sled-agent process has crashed, or that the sled is down.
    SledAgentUnreachable,
    /// The sled-agent responded with an unexpected HTTP error.
    SledAgentResponse(u16),
    /// The sled-agent indicated that it doesn't know about an instance ID that
    /// we believe it *should* know about. This probably means the sled-agent,
    /// and potentially the whole sled, has been restarted.
    NoSuchInstance,
}

impl Failure {
    fn as_str(&self) -> Cow<'static, str> {
        match self {
            Self::SledAgentUnreachable => "unreachable".into(),
            Self::SledAgentResponse(status) => status.to_string().into(),
            Self::NoSuchInstance => "no_such_instance".into(),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize,
)]
enum Incomplete {
    /// The sled-agent responded with an HTTP client error, indicating that our
    /// request as somehow malformed.
    ClientHttpError(u16),
    /// Something else went wrong while making an HTTP request.
    ClientError,
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
            Self::ClientHttpError(status) => status.to_string().into(),
            Self::ClientError => "client_error".into(),
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
            let mut tasks = tokio::task::JoinSet::new();
            let mut paginator = Paginator::new(MAX_SLED_AGENTS);
            let mk_client = |sled: &Sled| {
                nexus_networking::sled_client_from_address(
                    sled.id(),
                    sled.address(),
                    &opctx.log,
                )
            };

            while let Some(p) = paginator.next() {
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
                paginator = p.found_batch(&batch, &|(sled, _, vmm, _)| (sled.id(), vmm.id));

                // When we iterate over the batch of sled instances, we pop the
                // first sled from the batch before looping over the rest, to
                // insure that the initial sled-agent client is created first,
                // as we need the address of the first sled to construct it.
                // We could, alternatively, make the sled-agent client an
                // `Option`, but then every subsequent iteration would have to
                // handle the case where it's `None`, and I thought this was a
                // bit neater...
                let mut batch = batch.into_iter();
                if let Some((mut curr_sled, instance, vmm, project)) = batch.next() {
                    let mut client = mk_client(&curr_sled);
                    let target = VirtualMachine::new(self.id, &curr_sled, &instance, &vmm, &project);
                    tasks.spawn(self.check_instance(opctx, &client, target, vmm));

                    for (sled, instance, vmm, project) in batch {
                        // We're now talking to a new sled agent; update the client.
                        if sled.id() != curr_sled.id() {
                            client = mk_client(&sled);
                            curr_sled = sled;
                        }

                        let target = VirtualMachine::new(self.id, &curr_sled, &instance, &vmm, &project);
                        tasks.spawn(self.check_instance(opctx, &client, target, vmm));
                    }
                }
            }

            // Now, wait for the check results to come back.
            let mut total: usize = 0;
            let mut update_sagas_queued: usize = 0;
            let mut instance_states: BTreeMap<String, usize> =
                BTreeMap::new();
            let mut check_failures: BTreeMap<String, usize> =
                BTreeMap::new();
            let mut check_errors: BTreeMap<String, usize> = BTreeMap::new();
            while let Some(result) = tasks.join_next().await {
                total += 1;
                let check = result.expect(
                    "a `JoinError` is returned if a spawned task \
                    panics, or if the task is aborted. we never abort \
                    tasks on this `JoinSet`, and nexus is compiled with \
                    `panic=\"abort\"`, so neither of these cases should \
                    ever occur",
                );
                match check.outcome {
                    CheckOutcome::Success(state) => {
                        *instance_states
                            .entry(state.to_string())
                            .or_default() += 1;
                    }
                    CheckOutcome::Failure(reason) => {
                        *check_failures.entry(reason.as_str().into_owned()).or_default() += 1;
                    }
                    CheckOutcome::Unknown => {}
                }
                if let Err(ref reason) = check.result {
                    *check_errors.entry(reason.as_str().into_owned()).or_default() += 1;
                }
                if check.update_saga_queued {
                    update_sagas_queued += 1;
                }
                self.metrics.lock().unwrap().record_check(check);

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
    use oximeter::types::Cumulative;
    use oximeter::MetricsError;
    use oximeter::Sample;
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
