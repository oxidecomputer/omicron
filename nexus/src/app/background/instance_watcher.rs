// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use futures::{future::BoxFuture, FutureExt};
use http::StatusCode;
use nexus_db_model::Instance;
use nexus_db_model::Project;
use nexus_db_model::Sled;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::SledInstanceState;
use oximeter::types::ProducerRegistry;
use sled_agent_client::Client as SledAgentClient;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

/// Background task that periodically checks instance states.
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
    metrics: Arc<Mutex<metrics::Metrics>>,
    id: WatcherIdentity,
    v2p_notification_tx: tokio::sync::watch::Sender<()>,
}

const MAX_SLED_AGENTS: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl InstanceWatcher {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns::resolver::Resolver,
        producer_registry: &ProducerRegistry,
        id: WatcherIdentity,
        v2p_notification_tx: tokio::sync::watch::Sender<()>,
    ) -> Self {
        let metrics = Arc::new(Mutex::new(metrics::Metrics::default()));
        producer_registry
            .register_producer(metrics::Producer(metrics.clone()))
            .unwrap();
        Self { datastore, resolver, metrics, id, v2p_notification_tx }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        client: &SledAgentClient,
        target: VirtualMachine,
    ) -> impl Future<Output = Check> + Send + 'static {
        let datastore = self.datastore.clone();
        let resolver = self.resolver.clone();

        let opctx = opctx.child(
            std::iter::once((
                "instance_id".to_string(),
                target.instance_id.to_string(),
            ))
            .collect(),
        );
        let client = client.clone();
        let v2p_notification_tx = self.v2p_notification_tx.clone();

        async move {
            slog::trace!(opctx.log, "checking on instance...");
            let rsp = client.instance_get_state(&target.instance_id).await;
            let mut check =
                Check { target, outcome: Default::default(), result: Ok(()) };
            let state = match rsp {
                Ok(rsp) => rsp.into_inner(),
                Err(ClientError::ErrorResponse(rsp)) => {
                    let status = rsp.status();
                    if status == StatusCode::NOT_FOUND
                        && rsp.as_ref().error_code.as_deref()
                            == Some("NO_SUCH_INSTANCE")
                    {
                        slog::info!(opctx.log, "instance is wayyyyy gone");
                        // TODO(eliza): eventually, we should attempt to put the
                        // instance in the `Failed` state here.
                        check.outcome =
                            CheckOutcome::Failure(Failure::NoSuchInstance);
                        return check;
                    }
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
                Err(ClientError::CommunicationError(e)) => {
                    // TODO(eliza): eventually, we may want to transition the
                    // instance to the `Failed` state if the sled-agent has been
                    // unreachable for a while. We may also want to take other
                    // corrective actions or alert an operator in this case.
                    //
                    // TODO(eliza):  because we have the preported IP address
                    // of the instance's VMM from our databse query, we could
                    // also ask the VMM directly when the sled-agent is
                    // unreachable. We should start doing that here at some
                    // point.
                    slog::info!(opctx.log, "sled agent is unreachable"; "error" => ?e);
                    check.outcome =
                        CheckOutcome::Failure(Failure::SledAgentUnreachable);
                    return check;
                }
                Err(e) => {
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

            let new_runtime_state: SledInstanceState = state.into();
            check.outcome =
                CheckOutcome::Success(new_runtime_state.vmm_state.state);
            slog::debug!(
                opctx.log,
                "updating instance state";
                "state" => ?new_runtime_state.vmm_state.state,
            );
            check.result = crate::app::instance::notify_instance_updated(
                &datastore,
                &resolver,
                &opctx,
                &opctx,
                &opctx.log,
                &target.instance_id,
                &new_runtime_state,
                v2p_notification_tx,
            )
            .await
            .map_err(|e| {
                slog::warn!(
                    opctx.log,
                    "error updating instance";
                    "error" => ?e,
                    "state" => ?new_runtime_state.vmm_state.state,
                );
                Incomplete::UpdateFailed
            })
            .and_then(|updated| {
                updated.ok_or_else(|| {
                    slog::warn!(
                        opctx.log,
                        "error updating instance: not found in database";
                        "state" => ?new_runtime_state.vmm_state.state,
                    );
                    Incomplete::InstanceNotFound
                })
            })
            .map(|updated| {
                slog::debug!(
                    opctx.log,
                    "update successful";
                    "instance_updated" => updated.instance_updated,
                    "vmm_updated" => updated.vmm_updated,
                    "state" => ?new_runtime_state.vmm_state.state,
                );
            });

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

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, oximeter::Target,
)]
struct VirtualMachine {
    /// The rack ID of the Nexus process which performed the health check.
    rack_id: Uuid,
    /// The ID of the Nexus process which performed the health check.
    nexus_id: Uuid,
    /// The instance's ID.
    instance_id: Uuid,
    /// The silo ID of the instance's silo.
    silo_id: Uuid,
    /// The project ID of the instance.
    project_id: Uuid,
    /// The VMM ID of the instance's virtual machine manager.
    vmm_id: Uuid,
    /// The sled-agent's ID.
    sled_agent_id: Uuid,
    /// The sled agent's IP address.
    sled_agent_ip: IpAddr,
    /// The sled agent's port.
    sled_agent_port: u16,
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
                paginator = p.found_batch(&batch, &|(sled, _, _, _)| sled.id());

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
                    tasks.spawn(self.check_instance(opctx, &client, target));

                    for (sled, instance, vmm, project) in batch {
                        // We're now talking to a new sled agent; update the client.
                        if sled.id() != curr_sled.id() {
                            client = mk_client(&sled);
                            curr_sled = sled;
                        }

                        let target = VirtualMachine::new(self.id, &curr_sled, &instance, &vmm, &project);
                        tasks.spawn(self.check_instance(opctx, &client, target));
                    }
                }
            }

            // Now, wait for the check results to come back.
            let mut total: usize = 0;
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
                "pruned_instances" => pruned,
            );
            serde_json::json!({
                "total_instances": total,
                "instance_states": instance_states,
                "failed_checks": check_failures,
                "incomplete_checks": check_errors,
                "pruned_instances": pruned,
            })
        }
        .boxed()
    }
}

mod metrics {
    use super::{CheckOutcome, Incomplete, VirtualMachine};
    use oximeter::types::Cumulative;
    use oximeter::Metric;
    use oximeter::MetricsError;
    use oximeter::Sample;
    use std::borrow::Cow;
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
                        reason: error.as_str(),
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

    /// The number of successful checks for a single instance, VMM, and sled agent.
    #[derive(Clone, Debug, Metric)]
    struct Check {
        /// The string representation of the instance's state as understood by
        /// the VMM. If the check failed, this will generally be "failed".
        state: Cow<'static, str>,
        /// `Why the instance was marked as being in this state.
        ///
        /// If an instance was marked as "failed" due to a check failure, this
        /// will be a string representation of the failure reason. Otherwise, if
        /// the check was successful, this will be "success". Note that this may
        /// be "success" even if the instance's state is "failed", which
        /// indicates that we successfully queried the instance's state from the
        /// sled-agent, and the *sled-agent* reported that the instance has
        /// failed --- which is distinct from the instance watcher marking an
        /// instance as failed due to a failed check.
        reason: Cow<'static, str>,
        /// The number of checks for this instance and sled agent which recorded
        /// this state for this reason.
        datum: Cumulative<u64>,
    }

    /// The number of unsuccessful checks for an instance and sled agent pair.
    #[derive(Clone, Debug, Metric)]
    struct IncompleteCheck {
        /// The reason why the check was unsuccessful.
        ///
        /// This is generated from the [`Incomplete`] enum's `Display` implementation.
        reason: Cow<'static, str>,
        /// The number of failed checks for this instance and sled agent.
        datum: Cumulative<u64>,
    }
}
