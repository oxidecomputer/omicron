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
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that periodically checks instance states.
#[derive(Clone)]
pub(crate) struct InstanceWatcher {
    datastore: Arc<DataStore>,
    resolver: internal_dns::resolver::Resolver,
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
        Self { datastore, resolver }
    }

    fn check_instance(
        &self,
        opctx: &OpContext,
        client: &SledAgentClient,
        instance: SledInstance,
    ) -> impl Future<
        Output = CheckResult,
    > + Send
           + 'static {
        let instance_id = instance.instance_id();
        let watcher = self.clone();
        let opctx = opctx.child(
            std::iter::once((
                "instance_id".to_string(),
                instance_id.to_string(),
            ))
            .collect(),
        );
        let client = client.clone();

        async move {
            let InstanceWatcher { datastore, resolver } = watcher; 
            let target = CheckTarget {
                sled_agent_ip: client.address().ip(),
                sled_agent_port: client.address().port(),
                instance,
            };
            slog::trace!(opctx.log, "checking on instance...");
            let rsp = client.instance_get_state(&instance_id).await;
            let state = match rsp {
                Ok(rsp) => rsp.into_inner(),
                Err(ClientError::ErrorResponse(rsp))
                    if rsp.status() == http::StatusCode::NOT_FOUND
                        && rsp.as_ref().error_code.as_deref()
                            == Some("NO_SUCH_INSTANCE") =>
                {
                    slog::debug!(opctx.log, "instance is wayyyyy gone");
                    return CheckResult { target, check_failure: Some(CheckFailure::NoSuchInstance), update_failure: None };
                }
                Err(e) => {
                    let status = e.status();
                    slog::warn!(
                        opctx.log,
                        "error checking up on instance";
                        "error" => ?e,
                        "status" => ?status,
                    );
                    if let Some(status) = status {
                        let check_failure = Some(CheckFailure::SledAgentResponse(status));
                        return CheckResult { check_failure, update_failure: None };
                    } else {
                        match e.
                    }
                }
            };

            slog::debug!(opctx.log, "updating instance state: {state:?}");
            crate::app::instance::notify_instance_updated(
                &datastore,
                &resolver,
                &opctx,
                &opctx,
                &opctx.log,
                &instance_id,
                &state.into(),
            )
            .await
            .map_err(|_| CheckError::UpdateFailed)?
            .ok_or(CheckError::UnknownInstance)
        }
    }
}

struct CheckTarget {
    sled_agent_ip: IpAddr,
    sled_agent_port: u16,
    instance: SledInstance,
}

struct CheckResult {
    target: CheckTarget,
    instance_updated: Option<crate::instances::InstanceUpdated>,
    check_failure: Option<CheckFailure>,
    update_failure: Option<UpdateFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CheckFailure {
    SledAgentUnreachable,
    SledAgentResponse(StatusCode),
    NoSuchInstance,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum UpdateFailure {
    ClientError,
    InstanceNotFound,
    UpdateFailed,
    Other,
}

// impl

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
                            &client,
                            sled_instance,
                        ));
                    }
                }
            }

            // All requests fired off, let's wait for them to come back.
            let mut total = 0;
            let mut instances_updated = 0;
            let mut vmms_updated = 0;
            let mut no_change = 0;
            let mut not_found = 0;
            let mut sled_agent_errors = 0;
            let mut update_errors = 0;
            while let Some(result) = tasks.join_next().await {
                total += 1;
                match result {
                    Ok(Ok(InstanceUpdated {
                        vmm_updated,
                        instance_updated,
                    })) => {
                        if instance_updated {
                            instances_updated += 1;
                        }

                        if vmm_updated {
                            vmms_updated += 1;
                        }

                        if !(vmm_updated || instance_updated) {
                            no_change += 1;
                        }
                    }
                    Ok(Err(CheckError::NotFound)) => not_found += 1,
                    Ok(Err(CheckError::SledAgent)) => sled_agent_errors += 1,
                    Ok(Err(CheckError::Update)) => update_errors += 1,
                    Err(e) => unreachable!(
                        "a `JoinError` is returned if a spawned task \
                        panics, or if the task is aborted. we never abort \
                        tasks on this `JoinSet`, and nexus is compiled with \
                        `panic=\"abort\"`, so neither of these cases should \
                        ever occur: {e}",
                    ),
                }
            }

            slog::info!(opctx.log, "all instance checks complete";
                "total_instances" => ?total,
                "instances_updated" => ?instances_updated,
                "vmms_updated" => ?vmms_updated,
                "no_change" => ?no_change,
                "not_found" => ?not_found,
                "sled_agent_errors" => ?sled_agent_errors,
                "update_errors" => ?update_errors,
            );
            serde_json::json!({
                "total_instances": total,
                "instances_updated": instances_updated,
                "vmms_updated": vmms_updated,
                "no_change": no_change,
                "not_found": not_found,
                "sled_agent_errors": sled_agent_errors,
                "update_errors": update_errors,
            })
        }
        .boxed()
    }
}

mod metrics {
    use super::{CheckFailure, InstanceUpdated, UpdateFailure};
    use oximeter::types::Cumulative;
    use oximeter::Metric;
    use std::collections::BTreeMap;
    use std::net::IpAddr;
    use uuid::Uuid;

    pub(super) struct Metrics {
        sled_agents: BTreeMap<Uuid, SledAgent>,
    }

    type SledAgent = BTreeMap<Uuid, Instance>;

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
        pub fn instance(
            &mut self,
            sled_id: Uuid,
            sled_ip: IpAddr,
            sled_port: u16,
            instance_id: Uuid,
        ) -> &mut Instance {
            self.sled_agents
                .entry(sled_id)
                .or_default()
                .entry(instance_id)
                .or_insert_with(|| Instance {
                    no_update: InstanceChecks {
                        instance_id,
                        sled_agent_id: sled_id,
                        sled_agent_ip: sled_ip,
                        sled_agent_port: sled_port,
                        instance_updated: false,
                        vmm_updated: false,
                        datum: Cumulative::default(),
                    },
                    instance_updated: InstanceChecks {
                        instance_id,
                        sled_agent_id: sled_id,
                        sled_agent_ip: sled_ip,
                        sled_agent_port: sled_port,
                        instance_updated: true,
                        vmm_updated: false,
                        datum: Cumulative::default(),
                    },
                    vmm_updated: InstanceChecks {
                        instance_id,
                        sled_agent_id: sled_id,
                        sled_agent_ip: sled_ip,
                        sled_agent_port: sled_port,
                        instance_updated: false,
                        vmm_updated: true,
                        datum: Cumulative::default(),
                    },
                    both_updated: InstanceChecks {
                        instance_id,
                        sled_agent_id: sled_id,
                        sled_agent_ip: sled_ip,
                        sled_agent_port: sled_port,
                        instance_updated: true,
                        vmm_updated: true,
                        datum: Cumulative::default(),
                    },
                    check_failures: BTreeMap::new(),
                    update_failures: BTreeMap::new(),
                    touched: false,
                })
        }
    }

    impl Instance {
        pub fn success(&mut self, updated: InstanceUpdated) {
            match updated {
                InstanceUpdated { instance_updated: true, vmm_updated: true } => self.both_updated.datum += 1,
                InstanceUpdated { instance_updated: true, vmm_updated: false } => self.instance_updated.datum += 1,
                InstanceUpdated { instance_updated: false, vmm_updated: true } => self.vmm_updated.datum += 1,
                InstanceUpdated { instance_updated: false, vmm_updated: false } => self.no_update.datum += 1,
            }
            self.touched = true;
        }

        pub fn check_failure(&mut self, reason: CheckFailure) {
            self.check_failures
                .entry(reason)
                .or_insert_with(|| InstanceCheckFailures {
                    instance_id: self.no_update.instance_id,
                    sled_agent_id: self.no_update.sled_agent_id,
                    sled_agent_ip: self.no_update.sled_agent_ip,
                    sled_agent_port: self.no_update.sled_agent_port,
                    reason: reason.to_string(),
                    datum: Cumulative::default(),
                })
                .datum += 1;
            self.touched = true;
        }

        pub fn update_failure(&mut self, reason: UpdateFailure) {
            self.update_failures
                .entry(reason)
                .or_insert_with(|| InstanceUpdateFailures {
                    instance_id: self.no_update.instance_id,
                    sled_agent_id: self.no_update.sled_agent_id,
                    sled_agent_ip: self.no_update.sled_agent_ip,
                    sled_agent_port: self.no_update.sled_agent_port,
                    reason: reason.as_str(),
                    datum: Cumulative::default(),
                })
                .datum += 1;
            self.touched = true;
        }
    }

    /// The number of successful checks for a single instance and sled agent
    /// pair.
    #[derive(Clone, Debug, Metric)]
    struct InstanceChecks {
        /// The instance's ID.
        instance_id: Uuid,
        /// The sled-agent's ID.
        sled_agent_id: Uuid,
        /// The sled agent's IP address.
        sled_agent_ip: IpAddr,
        /// The sled agent's port.
        sled_agent_port: u16,
        instance_updated: bool,
        vmm_updated: bool,
        /// The number of successful checks for this instance and sled agent.
        datum: Cumulative<u64>,
    }

    /// The number of failed checks for an instance and sled agent pair.
    #[derive(Clone, Debug, Metric)]
    struct InstanceCheckFailures {
        /// The instance's ID.
        instance_id: Uuid,
        /// The sled-agent's ID.
        sled_agent_id: Uuid,
        /// The sled agent's IP address.
        sled_agent_ip: IpAddr,
        /// The sled agent's port.
        sled_agent_port: u16,
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
        /// The instance's ID.
        instance_id: Uuid,
        /// The sled-agent's ID.
        sled_agent_id: Uuid,
        /// The sled agent's IP address.
        sled_agent_ip: IpAddr,
        /// The sled agent's port.
        sled_agent_port: u16,
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
