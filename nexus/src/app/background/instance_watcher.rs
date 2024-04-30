// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pulling instance state from sled-agents.

use super::common::BackgroundTask;
use crate::app::instance::InstanceUpdated;
use futures::{future::BoxFuture, FutureExt};
use http::StatusCode;
use nexus_db_model::{Sled, SledInstance};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use oximeter::types::ProducerRegistry;
use sled_agent_client::Client as SledAgentClient;
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

/// Background task that periodically checks instance states.
#[derive(Clone)]
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
    metrics: Arc<Mutex<metrics::Metrics>>,
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
    ) -> Self {
        let metrics = Arc::new(Mutex::new(metrics::Metrics::default()));
        producer_registry
            .register_producer(metrics::Producer(metrics.clone()))
            .unwrap();
        Self { datastore, resolver, metrics }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        sled: &Sled,
        client: &SledAgentClient,
        instance: SledInstance,
    ) -> impl Future<Output = CheckResult> + Send + 'static {
        let instance_id = instance.instance_id();
        let watcher = self.clone();
        let target = InstanceTarget {
            instance_id,
            sled_agent_id: sled.id(),
            sled_agent_ip: std::net::Ipv6Addr::from(sled.ip).into(),
            sled_agent_port: sled.port.into(),
        };
        let opctx = opctx.child(
            std::iter::once((
                "instance_id".to_string(),
                instance_id.to_string(),
            ))
            .collect(),
        );
        let client = client.clone();

        async move {
            let InstanceWatcher { datastore, resolver, .. } = watcher;
            slog::trace!(opctx.log, "checking on instance...");
            let rsp = client.instance_get_state(&instance_id).await;
            let state = match rsp {
                Ok(rsp) => rsp.into_inner(),
                Err(ClientError::ErrorResponse(rsp)) => {
                    let status = rsp.status();
                    if status == StatusCode::NOT_FOUND
                        && rsp.as_ref().error_code.as_deref()
                            == Some("NO_SUCH_INSTANCE")
                    {
                        slog::info!(opctx.log, "instance is wayyyyy gone");
                        return CheckResult {
                            target,
                            check_failure: Some(CheckFailure::NoSuchInstance),
                            update_failure: None,
                            instance_updated: None,
                        };
                    }
                    if status.is_client_error() {
                        slog::warn!(opctx.log, "check failed due to client error";
                            "status" => ?status, "error" => ?rsp.into_inner());
                        return CheckResult {
                            target,
                            check_failure: None,
                            update_failure: Some(
                                UpdateFailure::ClientHttpError(status),
                            ),
                            instance_updated: None,
                        };
                    }

                    slog::info!(opctx.log, "check failed due to server error";
                        "status" => ?status, "error" => ?rsp.into_inner());

                    return CheckResult {
                        target,
                        check_failure: Some(CheckFailure::SledAgentResponse(
                            status,
                        )),
                        update_failure: None,
                        instance_updated: None,
                    };
                }
                Err(ClientError::CommunicationError(e)) => {
                    slog::info!(opctx.log, "sled agent is unreachable"; "error" => ?e);
                    return CheckResult {
                        target,
                        check_failure: Some(CheckFailure::SledAgentUnreachable),
                        update_failure: None,
                        instance_updated: None,
                    };
                }
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "error checking up on instance";
                        "error" => ?e,
                        "status" => ?e.status(),
                    );
                    return CheckResult {
                        target,
                        check_failure: None,
                        update_failure: Some(UpdateFailure::ClientError),
                        instance_updated: None,
                    };
                }
            };

            slog::debug!(opctx.log, "updating instance state: {state:?}");
            let update_result = crate::app::instance::notify_instance_updated(
                &datastore,
                &resolver,
                &opctx,
                &opctx,
                &opctx.log,
                &instance_id,
                &state.into(),
            )
            .await
            .map_err(|_| UpdateFailure::UpdateFailed)
            .and_then(|updated| updated.ok_or(UpdateFailure::InstanceNotFound));
            match update_result {
                Ok(updated) => {
                    slog::debug!(opctx.log, "update successful"; "instance_updated" => updated.instance_updated, "vmm_updated" => updated.vmm_updated);
                    CheckResult {
                        target,
                        instance_updated: Some(updated),
                        check_failure: None,
                        update_failure: None,
                    }
                }
                Err(e) => {
                    slog::warn!(opctx.log, "error updating instance"; "error" => ?e);
                    CheckResult {
                        target,
                        instance_updated: None,
                        check_failure: None,
                        update_failure: Some(e),
                    }
                }
            }
        }
    }
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, oximeter::Target,
)]
struct InstanceTarget {
    /// The instance's ID.
    instance_id: Uuid,
    /// The sled-agent's ID.
    sled_agent_id: Uuid,
    /// The sled agent's IP address.
    sled_agent_ip: IpAddr,
    /// The sled agent's port.
    sled_agent_port: u16,
}

struct CheckResult {
    target: InstanceTarget,
    instance_updated: Option<crate::app::instance::InstanceUpdated>,
    check_failure: Option<CheckFailure>,
    update_failure: Option<UpdateFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CheckFailure {
    SledAgentUnreachable,
    SledAgentResponse(StatusCode),
    NoSuchInstance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum UpdateFailure {
    ClientHttpError(StatusCode),
    ClientError,
    InstanceNotFound,
    UpdateFailed,
}

impl fmt::Display for CheckFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SledAgentUnreachable => f.write_str("unreachable"),
            Self::SledAgentResponse(status) => {
                write!(f, "{status}")
            }
            Self::NoSuchInstance => f.write_str("no_such_instance"),
        }
    }
}

impl fmt::Display for UpdateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientHttpError(status) => write!(f, "{status}"),
            Self::ClientError => f.write_str("client_error"),
            Self::InstanceNotFound => f.write_str("instance_not_found"),
            Self::UpdateFailed => f.write_str("update_failed"),
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
            while let Some(p) = paginator.next() {
                let maybe_batch = self
                    .datastore
                    .sled_instance_list_by_sled_agent(
                        opctx,
                        &p.current_pagparams(),
                    )
                    .await;
                let batch = match maybe_batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        slog::warn!(
                            opctx.log,
                            "sled instances by sled agent query failed: {e}"
                        );
                        break;
                    }
                };
                paginator = p.found_batch(&batch, &|(sled, _)| sled.id());
                let mut batch = batch.into_iter();

                if let Some((mut curr_sled, sled_instance)) = batch.next() {
                    let mk_client = |sled: &Sled| {
                        nexus_networking::sled_client_from_address(
                            sled.id(),
                            sled.address(),
                            &opctx.log,
                        )
                    };

                    let mut client = mk_client(&curr_sled);
                    tasks.spawn(self.check_instance(
                        opctx,
                        &curr_sled,
                        &client,
                        sled_instance,
                    ));

                    for (sled, sled_instance) in batch {
                        // We're now talking to a new sled agent; update the client.
                        if sled.id() != curr_sled.id() {
                            client = mk_client(&sled);
                            curr_sled = sled;
                        }
                        tasks.spawn(self.check_instance(
                            opctx,
                            &curr_sled,
                            &client,
                            sled_instance,
                        ));
                    }
                }
            }

            // All requests fired off! While we wait for them to come back,
            // let's prune old instances.
            let pruned = self.metrics.lock().unwrap().prune();

            // Now, wait for the check results to come back.
            let mut total: usize = 0;
            let mut instances_updated: usize = 0;
            let mut vmms_updated: usize = 0;
            let mut no_change: usize = 0;
            let mut not_found: usize = 0;
            let mut sled_agent_errors: usize = 0;
            let mut update_errors: usize = 0;
            while let Some(result) = tasks.join_next().await {
                total += 1;
                let CheckResult {
                    target,
                    instance_updated,
                    check_failure,
                    update_failure,
                    ..
                } = result.expect(
                    "a `JoinError` is returned if a spawned task \
                    panics, or if the task is aborted. we never abort \
                    tasks on this `JoinSet`, and nexus is compiled with \
                    `panic=\"abort\"`, so neither of these cases should \
                    ever occur",
                );
                let mut metrics = self.metrics.lock().unwrap();
                let metric = metrics.instance(target);
                if let Some(up) = instance_updated {
                    if up.instance_updated {
                        instances_updated += 1;
                    }

                    if up.vmm_updated {
                        vmms_updated += 1;
                    }

                    if !(up.vmm_updated || up.instance_updated) {
                        no_change += 1;
                    }
                    metric.success(up);
                }
                if let Some(reason) = check_failure {
                    match reason {
                        CheckFailure::NoSuchInstance => not_found += 1,
                        _ => sled_agent_errors += 1,
                    }

                    metric.check_failure(reason);
                }
                if let Some(reason) = update_failure {
                    metric.update_failure(reason);
                    update_errors += 1;
                }
            }

            slog::info!(opctx.log, "all instance checks complete";
                "total_instances" => total,
                "instances_updated" => instances_updated,
                "vmms_updated" => vmms_updated,
                "no_change" => no_change,
                "not_found" => not_found,
                "sled_agent_errors" => sled_agent_errors,
                "update_errors" => update_errors,
                "pruned_instances" => pruned,
            );
            serde_json::json!({
                "total_instances": total,
                "instances_updated": instances_updated,
                "vmms_updated": vmms_updated,
                "no_change": no_change,
                "not_found": not_found,
                "sled_agent_errors": sled_agent_errors,
                "update_errors": update_errors,
                "pruned_instances": pruned,
            })
        }
        .boxed()
    }
}

mod metrics {
    use super::{CheckFailure, InstanceTarget, InstanceUpdated, UpdateFailure};
    use oximeter::types::Cumulative;
    use oximeter::Metric;
    use oximeter::MetricsError;
    use oximeter::Sample;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    pub(super) struct Metrics {
        instances: BTreeMap<InstanceTarget, Instance>,
    }

    #[derive(Debug)]
    pub(super) struct Producer(pub(super) Arc<Mutex<Metrics>>);

    #[derive(Debug)]
    pub(super) struct Instance {
        no_update: InstanceChecks,
        instance_updated: InstanceChecks,
        vmm_updated: InstanceChecks,
        both_updated: InstanceChecks,
        check_failures: BTreeMap<CheckFailure, InstanceCheckFailures>,
        update_failures: BTreeMap<UpdateFailure, InstanceUpdateFailures>,
        touched: bool,
    }

    impl Metrics {
        pub(crate) fn instance(
            &mut self,
            instance: InstanceTarget,
        ) -> &mut Instance {
            self.instances.entry(instance).or_insert_with(|| Instance {
                no_update: InstanceChecks {
                    instance_updated: false,
                    vmm_updated: false,
                    datum: Cumulative::default(),
                },
                instance_updated: InstanceChecks {
                    instance_updated: true,
                    vmm_updated: false,
                    datum: Cumulative::default(),
                },
                vmm_updated: InstanceChecks {
                    instance_updated: false,
                    vmm_updated: true,
                    datum: Cumulative::default(),
                },
                both_updated: InstanceChecks {
                    instance_updated: true,
                    vmm_updated: true,
                    datum: Cumulative::default(),
                },
                check_failures: BTreeMap::new(),
                update_failures: BTreeMap::new(),
                touched: false,
            })
        }

        pub(super) fn prune(&mut self) -> usize {
            let mut pruned = 0;
            self.instances.retain(|_, instance| {
                let touched = std::mem::replace(&mut instance.touched, false);
                if !touched {
                    pruned += 1;
                }
                touched
            });
            pruned
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
        pub(super) fn success(&mut self, updated: InstanceUpdated) {
            match updated {
                InstanceUpdated {
                    instance_updated: true,
                    vmm_updated: true,
                } => self.both_updated.datum += 1,
                InstanceUpdated {
                    instance_updated: true,
                    vmm_updated: false,
                } => self.instance_updated.datum += 1,
                InstanceUpdated {
                    instance_updated: false,
                    vmm_updated: true,
                } => self.vmm_updated.datum += 1,
                InstanceUpdated {
                    instance_updated: false,
                    vmm_updated: false,
                } => self.no_update.datum += 1,
            }
            self.touched = true;
        }

        pub(super) fn check_failure(&mut self, reason: CheckFailure) {
            self.check_failures
                .entry(reason)
                .or_insert_with(|| InstanceCheckFailures {
                    reason: reason.to_string(),
                    datum: Cumulative::default(),
                })
                .datum += 1;
            self.touched = true;
        }

        pub(super) fn update_failure(&mut self, reason: UpdateFailure) {
            self.update_failures
                .entry(reason)
                .or_insert_with(|| InstanceUpdateFailures {
                    reason: reason.to_string(),
                    datum: Cumulative::default(),
                })
                .datum += 1;
            self.touched = true;
        }

        fn len(&self) -> usize {
            4 + self.check_failures.len() + self.update_failures.len()
        }

        fn sample_into(
            &self,
            target: &InstanceTarget,
            dest: &mut Vec<Sample>,
        ) -> Result<(), MetricsError> {
            dest.push(Sample::new(target, &self.no_update)?);
            dest.push(Sample::new(target, &self.instance_updated)?);
            dest.push(Sample::new(target, &self.vmm_updated)?);
            dest.push(Sample::new(target, &self.both_updated)?);
            for metric in self.check_failures.values() {
                dest.push(Sample::new(target, metric)?);
            }
            for metric in self.update_failures.values() {
                dest.push(Sample::new(target, metric)?);
            }
            Ok(())
        }
    }

    /// The number of successful checks for a single instance and sled agent
    /// pair.
    #[derive(Clone, Debug, Metric)]
    struct InstanceChecks {
        /// `true` if the instance state changed as a result of this check.
        instance_updated: bool,
        /// `true` if the VMM state changed as a result of this check.
        vmm_updated: bool,
        /// The number of successful checks for this instance and sled agent.
        datum: Cumulative<u64>,
    }

    /// The number of failed checks for an instance and sled agent pair.
    #[derive(Clone, Debug, Metric)]
    struct InstanceCheckFailures {
        /// The reason why the check failed.
        ///
        /// # Note
        /// This must always be generated from a `CheckFailure` enum.
        reason: String,
        /// The number of failed checks for this instance and sled agent.
        datum: Cumulative<u64>,
    }

    /// The number of failed instance updates for an instance and sled agent pair.
    #[derive(Clone, Debug, Metric)]
    struct InstanceUpdateFailures {
        /// The reason why the check failed.
        ///
        /// # Note
        /// This must always be generated from a `CheckFailure` enum.
        // TODO(eliza): it would be nice if this was a `oximeter::FieldType`:
        // From<&str>` impl, so that this could be a `&'static str`.
        reason: String,
        /// The number of failed checks for this instance and sled agent.
        datum: Cumulative<u64>,
    }
}
