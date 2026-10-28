// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that stops VMMs that have been marked as needing to be
//! stopped for a sled update.

use crate::app::background::BackgroundTask;
use crate::app::instance;
use crate::app::instance::SledAgentInstanceError;
use crate::app::saga::StartSaga;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::instance::SledVmmState;
use nexus_types::instance::VmmRuntimeState;
use nexus_types::instance::VmmState;
use nexus_types::internal_api::background::VmmStopForUpdateStatus;
use nexus_types::internal_api::background::VmmsBySled;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use parallel_task_set::ParallelTaskSet;
use reqwest::Client as ReqwestClient;
use serde_json::json;
use sled_agent_client::types::VmmPutStateBody;
use sled_agent_client::types::VmmStateRequested;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Determines the number of attempts to stop VMMs (sled agent PUT request and
/// subsequent update saga per VMM) that can execute concurrently.
///
/// Like the instance-watcher task, we keep this number low to make sure we
/// don't starve the system of resources.
const MAX_CONCURRENT_STOPS: usize = 16;

struct VmmStopForUpdateResult {
    vmms_stopped_by_sled: IdOrdMap<VmmsBySled>,
    vmms_failed_to_stop_by_sled: IdOrdMap<VmmsBySled>,
    errors: Vec<String>,
}

impl VmmStopForUpdateResult {
    fn new() -> Self {
        Self {
            vmms_stopped_by_sled: IdOrdMap::new(),
            vmms_failed_to_stop_by_sled: IdOrdMap::new(),
            errors: Vec::new(),
        }
    }

    /// Merges the values of `self` with a given [`VmmStopForUpdateResult`]
    fn merge(&mut self, other: VmmStopForUpdateResult) {
        let VmmStopForUpdateResult {
            vmms_stopped_by_sled,
            vmms_failed_to_stop_by_sled,
            errors,
        } = other;

        for VmmsBySled { sled_id, vmm_ids } in vmms_stopped_by_sled {
            self.vmms_stopped_by_sled
                .entry(sled_id)
                .or_insert_with(|| VmmsBySled {
                    sled_id,
                    vmm_ids: BTreeSet::new(),
                })
                .vmm_ids
                .extend(vmm_ids);
        }
        for VmmsBySled { sled_id, vmm_ids } in vmms_failed_to_stop_by_sled {
            self.vmms_failed_to_stop_by_sled
                .entry(sled_id)
                .or_insert_with(|| VmmsBySled {
                    sled_id,
                    vmm_ids: BTreeSet::new(),
                })
                .vmm_ids
                .extend(vmm_ids);
        }
        self.errors.extend(errors);
    }
}

pub struct VmmStopForUpdate {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
    reqwest_client: Option<ReqwestClient>,
}

impl VmmStopForUpdate {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        sagas: Arc<dyn StartSaga>,
    ) -> Self {
        Self { datastore, sagas, reqwest_client: None }
    }

    /// Retrieves a list of VMMs marked to be stopped for update by sled, and
    /// stops them in batches.
    async fn stop_all(&mut self, opctx: &OpContext) -> VmmStopForUpdateResult {
        let mut result = VmmStopForUpdateResult::new();

        // Build the reqwest client beforehand to avoid building a new one per
        // page or sled below.
        let reqwest_client = match self.reqwest_client.clone() {
            Some(client) => client,
            None => {
                match nexus_networking::default_reqwest_client_builder().build()
                {
                    Ok(client) => {
                        self.reqwest_client = Some(client.clone());
                        client
                    }
                    Err(e) => {
                        let err = InlineErrorChain::new(&e);

                        slog::error!(
                            opctx.log,
                            "Failed to build the HTTP client";
                            &err,
                        );

                        result.errors.push(format!(
                            "failed to build the HTTP client: {err}",
                        ));

                        return result;
                    }
                }
            }
        };

        let mut tasks =
            ParallelTaskSet::new_with_parallelism(MAX_CONCURRENT_STOPS);
        let task_opctx = Arc::new(opctx.child(BTreeMap::new()));

        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let vmms = match self
                .datastore
                .vmm_list_stoppable_for_update_by_sled_id(
                    opctx,
                    &p.current_pagparams(),
                )
                .await
            {
                Ok(vmms) => vmms,
                Err(err) => {
                    result.errors.push(InlineErrorChain::new(&err).to_string());
                    break;
                }
            };
            paginator = p.found_batch(&vmms, &|vmm| {
                (vmm.sled_id().into_untyped_uuid(), vmm.id)
            });

            self.stop_batch(
                &vmms,
                &reqwest_client,
                &task_opctx,
                &mut tasks,
                &mut result,
            )
            .await;
        }

        while let Some(vmm_result) = tasks.join_next().await {
            result.merge(vmm_result);
        }

        result
    }

    /// Stops a batch of VMMs
    async fn stop_batch(
        &self,
        vmms: &[Vmm],
        reqwest_client: &ReqwestClient,
        opctx: &Arc<OpContext>,
        tasks: &mut ParallelTaskSet<VmmStopForUpdateResult>,
        result: &mut VmmStopForUpdateResult,
    ) {
        for vmms_per_sled in vmms.chunk_by(|a, b| a.sled_id() == b.sled_id()) {
            // We only retrieve the first VMM in order to get the sled id.
            // The slice is constructed by VMMs with the same sled id, so it's
            // fine to just check the first one
            let Some(first_vmm) = vmms_per_sled.first() else {
                // `chunk_by` never returns an empty slice, so this should never
                // be `None`. We handle the case regardless
                continue;
            };
            let sled_id = first_vmm.sled_id();

            let vmm_ids: Vec<PropolisUuid> = vmms_per_sled
                .iter()
                .map(|vmm| PropolisUuid::from_untyped_uuid(vmm.id))
                .collect();

            slog::info!(
                opctx.log,
                "Stopping VMMs for update";
                "sled_id" => %sled_id,
                "vmms" => ?vmm_ids,
            );

            // Since we're chunking by sled id, each will only appear once per
            // batch of VMMs. Because of this, it's ok to start a new sled agent
            // client per sled id.
            let sled_client = match nexus_networking::sled_client_ext(
                &self.datastore,
                &opctx,
                sled_id,
                &opctx.log,
                reqwest_client.clone(),
            )
            .await
            {
                Ok(client) => client,
                Err(e) => {
                    slog::error!(
                        opctx.log,
                        "Failed to create the sled agent client";
                        "sled_id" => %sled_id,
                        InlineErrorChain::new(&e),
                    );

                    result.errors.push(InlineErrorChain::new(&e).to_string());

                    continue;
                }
            };

            for vmm in vmms_per_sled {
                let stop =
                    self.stop_vmm(opctx, sled_client.clone(), vmm.clone());
                if let Some(vmm_result) = tasks.spawn(stop).await {
                    result.merge(vmm_result);
                }
            }
        }
    }

    /// Stops a single VMM.
    fn stop_vmm(
        &self,
        opctx: &Arc<OpContext>,
        sled_client: sled_agent_client::Client,
        vmm: Vmm,
    ) -> impl Future<Output = VmmStopForUpdateResult> + Send + 'static + use<>
    {
        let datastore = self.datastore.clone();
        let sagas = self.sagas.clone();
        let opctx = Arc::clone(opctx);
        let vmm_id = PropolisUuid::from_untyped_uuid(vmm.id);
        let instance_id = InstanceUuid::from_untyped_uuid(vmm.instance_id);
        let sled_id = vmm.sled_id();

        async move {
            let mut result = VmmStopForUpdateResult::new();
            let response = match sled_client
                .vmm_put_state(
                    &vmm_id,
                    &VmmPutStateBody { state: VmmStateRequested::Stopped },
                )
                .await
            {
                Ok(res) => res.into_inner().updated_runtime.map(Into::into),
                Err(e) => {
                    let err = SledAgentInstanceError::new(
                        &opctx, &datastore, sled_id, e,
                    )
                    .await;

                    // First we check if the VMM should transition to `Failed`
                    // based on this error. If it does, we mark it as such so we
                    // don't have to wait on the instance watcher to find and
                    // restart this instance.
                    if let Some(reason) = err.vmm_failure_reason() {
                        slog::info!(
                            opctx.log,
                            "Attempted to stop a VMM, but discovered that it \
                            should transition to Failed.";
                            "sled_id" => %sled_id,
                            "vmm_id" => %vmm_id,
                            "instance_id" => %instance_id,
                            "failure_reason" => %reason,
                        );

                        result
                            .errors
                            .push(InlineErrorChain::new(&err).to_string());
                        Some(SledVmmState {
                            vmm_state: vmm
                                .runtime()
                                .transition(VmmState::Failed(reason)),
                            migration_in: None,
                            migration_out: None,
                        })
                    } else {
                        let error = InlineErrorChain::new(&err);
                        slog::info!(
                            opctx.log,
                            "Failed to mark VMM as Stopped";
                            "sled_id" => %sled_id,
                            "vmm_id" => %vmm_id,
                            "instance_id" => %instance_id,
                            &error,
                        );

                        result.errors.push(error.to_string());
                        result
                            .vmms_failed_to_stop_by_sled
                            .entry(sled_id)
                            .or_insert_with(|| VmmsBySled {
                                sled_id,
                                vmm_ids: BTreeSet::new(),
                            })
                            .vmm_ids
                            .insert(vmm_id);

                        return result;
                    }
                }
            };

            match response {
                None => {
                    slog::debug!(
                        opctx.log,
                        "VMM already stopped, no changes to state";
                        "sled_id" => %sled_id,
                        "vmm_id" => %vmm_id,
                        "instance_id" => %instance_id,
                    );
                }
                Some(sled_vmm_state) => {
                    // Updating the VMM as we have done above is, in practice,
                    // enough to stop the VMM itself. The instance on the other
                    // hand, is not updated instantaneously to report the new
                    // state the VMM is in.
                    //
                    // There are a few ways in which the instance will know the
                    // state of the VMM has changed, and therefore its own state
                    // should change to reflect the VMM's new state:
                    //
                    //   - The instance watcher background task continuously
                    //     makes sure the instances' states are correct in
                    //     relation to their VMMs.
                    //   - Sled agent will attempt to publish the new state to
                    //     Nexus when the VMM process exits.
                    //
                    // Both of these methods are not immediate, and it will take
                    // some time for the instance to report itself as `Stopped`.
                    // To reduce the latency from the time we change the `Vmm`'s
                    // state to the time this is reflected in the instance, we
                    // do two things:
                    //
                    //  - Attempt to write the new VMM state to the db with
                    //    `instance::process_vmm_update`.
                    //  - Execute the instance update saga that
                    //    `instance::process_vmm_update` returns to reconcile
                    //    the instance to the new VMM state.
                    //
                    // We don't care if either of the previous two steps fails.
                    // We are running these steps as an _optimisation_ only. The
                    // instance state will be updated eventually by either the
                    // instance watcher task, or sled agent.
                    try_instance_state_update(
                        &mut result,
                        datastore,
                        sagas,
                        &opctx,
                        AssociatedIds { vmm_id, instance_id, sled_id },
                        sled_vmm_state.clone(),
                    )
                    .await;

                    let SledVmmState {
                        vmm_state,
                        migration_in: _,
                        migration_out: _,
                    } = sled_vmm_state;

                    let VmmRuntimeState { state, generation, time_updated } =
                        vmm_state;

                    match state {
                        // If we succeeded, one of three things happened:
                        //
                        //  - The VMM has not stopped yet, but propolis has
                        //    accepted the `put_state` request and reports
                        //    the VMM's state as `Stopping`.
                        //  - A propolis zone does not exist; the VMM is
                        //    reported as `Destroyed`.
                        //  - A VMM should technically never be reported as
                        //    `Stopped` since
                        //    propolis_client::types::InstanceState::Stopped
                        //    is always reported as `VmmState::Stopping`.
                        //    But, if for some reason we get this state,
                        //    it's what we want, so we consider it a
                        //    success.
                        VmmState::Stopped
                        | VmmState::Stopping
                        | VmmState::Destroyed => {
                            result
                                .vmms_stopped_by_sled
                                .entry(sled_id)
                                .or_insert_with(|| VmmsBySled {
                                    sled_id,
                                    vmm_ids: BTreeSet::new(),
                                })
                                .vmm_ids
                                .insert(vmm_id);

                            slog::debug!(
                                opctx.log,
                                "Stopped VMM for sled evacuation during \
                                an update";
                                "sled_id" => %sled_id,
                                "vmm_id" => %vmm_id,
                                "vmm_generation" => ?generation,
                                "instance_id" => %instance_id,
                                "time_updated" => ?time_updated,
                            );
                        }
                        // `vmm_put_state` only reports the `VmmState::Failed`
                        // state when Propolis is alive but does not find a VMM.
                        // This implies that it restarted and lost the
                        // previously created VMM.
                        //
                        // Additionally, when `vmm_put_state` returns a
                        // `NO_SUCH_INSTANCE` error, we transition the runtime
                        // state to `VmmState::Failed` above.
                        //
                        // Because of this, it's not necessary to stop it
                        // anymore, so we don't add it to the
                        // `vmms_failed_to_stop_by_sled` list. The instance
                        // reincarnation task will automatically attempt to
                        // restart any instance that is in a failed state.
                        VmmState::Failed(reason) => {
                            let description = reason.description();
                            slog::info!(
                                opctx.log,
                                "Unable to find VMM. No need to stop it";
                                "sled_id" => %sled_id,
                                "vmm_id" => %vmm_id,
                                "instance_id" => %instance_id,
                                "reason" => description,
                                "state" => ?state,
                                "vmm_generation" => ?generation,
                                "time_updated" => ?time_updated,
                            );
                        }
                        VmmState::Migrating
                        | VmmState::Rebooting
                        | VmmState::Starting
                        | VmmState::Running
                        | VmmState::Creating
                        | VmmState::SagaUnwound => {
                            slog::warn!(
                                opctx.log,
                                "Failed to mark VMM as Stopped, \
                                unexpected state reported";
                                "sled_id" => %sled_id,
                                "vmm_id" => %vmm_id,
                                "state" => ?state,
                                "vmm_generation" => ?generation,
                                "instance_id" => %instance_id,
                                "time_updated" => ?time_updated,
                            );
                            result.errors.push(format!(
                                "failed to mark VMM {} on sled {} as \
                                 Stopped; unexpected state reported {}",
                                vmm_id, sled_id, state
                            ));
                        }
                    }
                }
            }

            result
        }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> VmmStopForUpdateStatus {
        let VmmStopForUpdateResult {
            vmms_stopped_by_sled,
            vmms_failed_to_stop_by_sled,
            errors: error_messages,
        } = self.stop_all(opctx).await;

        if vmms_stopped_by_sled.is_empty() {
            slog::info!(
                &opctx.log,
                "No VMMs were stopped for a sled update";
            );
        } else {
            for sled in &vmms_stopped_by_sled {
                let VmmsBySled { sled_id, vmm_ids } = sled;
                slog::info!(
                    &opctx.log,
                    "Stopped {} VMMs marked to stop for a sled update",
                    vmm_ids.len();
                    "sled_id" => %sled_id,
                    "vmms" => ?vmm_ids,
                );
            }
        }

        if !vmms_failed_to_stop_by_sled.is_empty() {
            for sled in &vmms_failed_to_stop_by_sled {
                let VmmsBySled { sled_id, vmm_ids } = sled;
                slog::info!(
                    &opctx.log,
                    "Failed to stop {} VMMs marked to stop for a sled update",
                    vmm_ids.len();
                    "sled_id" => %sled_id,
                    "vmms" => ?vmm_ids,
                );
            }
        }

        if !error_messages.is_empty() {
            slog::info!(
                &opctx.log,
                "Attempting to stop VMMs marked to stop for a sled update \
                 returned errors";
                "errors" => ?error_messages,
            );
        }

        VmmStopForUpdateStatus {
            vmms_stopped_by_sled,
            vmms_failed_to_stop_by_sled,
            error_messages,
        }
    }
}

impl BackgroundTask for VmmStopForUpdate {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    json!({
                        "error": format!("failed to serialize status: {err}")
                    })
                }
            }
        })
    }
}

// This struct exists solely to avoid a clippy "too_many_arguments" error
struct AssociatedIds {
    vmm_id: PropolisUuid,
    instance_id: InstanceUuid,
    sled_id: SledUuid,
}

// Attempt to update the instance state.
//
// This function is intended to be run by a caller that does not care whether
// the instance state is actually successfully updated or not. It does not
// Errors are not returned, but rather logged and saved in `VmmStopForUpdateResult`.
async fn try_instance_state_update(
    result: &mut VmmStopForUpdateResult,
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
    opctx: &Arc<OpContext>,
    ids: AssociatedIds,
    sled_vmm_state: SledVmmState,
) {
    let AssociatedIds { vmm_id, instance_id, sled_id } = ids;

    // Attempt to write the new VMM state to the db.
    let saga = match instance::process_vmm_update(
        &datastore,
        opctx,
        vmm_id,
        &sled_vmm_state,
    )
    .await
    {
        // As mentioned earlier, it's not a huge deal if we fail here. The
        // instance state will be updated eventually by the instance watcher
        // task or sled agent.
        Err(err) => {
            let error = InlineErrorChain::new(&err);
            slog::warn!(
                opctx.log,
                "Failed to record new VMM state";
                "sled_id" => %sled_id,
                "vmm_id" => %vmm_id,
                "state" => ?sled_vmm_state,
                "instance_id" => %instance_id,
                &error,
            );
            result.errors.push(error.to_string());
            return;
        }
        // The new state would like an instance-update saga, so we'll try to
        // run it.
        Ok(Some((_, saga))) => saga,
        // The new state does not require an update saga, nothing left to do
        // here.
        Ok(None) => return,
    };
    let (saga_id, completed) = match sagas.saga_run(saga).await {
        Ok(s) => s,
        Err(err) => {
            let error = InlineErrorChain::new(&err);
            slog::warn!(
                opctx.log,
                "Instance update saga could not be started";
                "sled_id" => %sled_id,
                "vmm_id" => %vmm_id,
                "instance_id" => %instance_id,
                &error,
            );
            result.errors.push(error.to_string());
            return;
        }
    };
    if let Err(err) = completed.await {
        let error = InlineErrorChain::new(&err);
        slog::warn!(
            opctx.log,
            "Instance update saga failed";
            "sled_id" => %sled_id,
            "vmm_id" => %vmm_id,
            "saga_id" => %saga_id,
            "instance_id" => %instance_id,
            &error,
        );
        result.errors.push(error.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::sagas::test_helpers::instance_fetch;
    use crate::app::sagas::test_helpers::instance_simulate;
    use crate::app::sagas::test_helpers::instance_wait_for_state;
    use crate::app::sagas::test_helpers::test_opctx;
    use crate::app::sagas::test_helpers::wait_for_running_sagas;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
    use nexus_db_lookup::AsyncConnection;
    use nexus_db_model::Generation;
    use nexus_db_model::InstanceState;
    use nexus_db_model::Vmm;
    use nexus_db_model::VmmCpuPlatform;
    use nexus_db_model::VmmFailureReason;
    use nexus_db_model::VmmState as DbVmmState;
    use nexus_db_model::to_db_typed_generation;
    use nexus_test_utils::ControlPlaneBuilder;
    use nexus_test_utils::resource_helpers::create_anti_affinity_group;
    use nexus_test_utils::resource_helpers::create_default_ip_pools;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::object_create;
    use nexus_types::external_api::instance::InstanceCreate;
    use nexus_types_versions::latest::instance::Instance;
    use nexus_types_versions::latest::instance::InstanceNetworkInterfaceAttachment;
    use nexus_types_versions::latest::instance::InstanceSelector;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::NameOrId;
    use omicron_generation_kinds::UpdateDispositionGeneration;
    use omicron_uuid_kinds::InstanceUuid;
    use omicron_uuid_kinds::SledUuid;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::time::Duration;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "vmm-stop-for-update";

    // In order to stop a VMM, it needs to be registered with the simulated
    // sled agent. We keep a record of the states of the real VMMs we will be
    // stopping.
    //
    // The label is used to name instances and groups.
    const REAL_VMM_STATES: [(&str, DbVmmState); 3] = [
        ("starting", DbVmmState::Starting),
        ("running", DbVmmState::Running),
        ("rebooting", DbVmmState::Rebooting),
    ];

    async fn create_started_instance(
        cptestctx: &ControlPlaneTestContext,
        name: &str,
        anti_affinity_group: &str,
    ) -> InstanceUuid {
        let created = object_create::<_, Instance>(
            &cptestctx.external_client,
            &format!("/v1/instances?project={PROJECT_NAME}"),
            &InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: format!("instance {name}"),
                },
                ncpus: 1i64.try_into().unwrap(),
                memory: ByteCount::from_gibibytes_u32(1),
                hostname: "myhostname".try_into().unwrap(),
                user_data: Vec::new(),
                network_interfaces: InstanceNetworkInterfaceAttachment::None,
                external_ips: Vec::new(),
                disks: Vec::new(),
                boot_disk: None,
                cpu_platform: None,
                ssh_public_keys: None,
                start: true,
                auto_restart_policy: None,
                anti_affinity_groups: vec![NameOrId::Name(
                    anti_affinity_group.parse().unwrap(),
                )],
                multicast_groups: Vec::new(),
                enable_jumbo_frames: false,
            },
        )
        .await;
        InstanceUuid::from_untyped_uuid(created.identity.id)
    }

    async fn reboot_instance(
        cptestctx: &ControlPlaneTestContext,
        instance_id: InstanceUuid,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(cptestctx);
        let lookup = nexus
            .instance_lookup(
                &opctx,
                InstanceSelector {
                    project: None,
                    instance: NameOrId::from(instance_id.into_untyped_uuid()),
                },
            )
            .expect("instance lookup should succeed");
        nexus
            .instance_reboot(&opctx, &lookup)
            .await
            .expect("instance should reboot");
    }

    async fn active_vmm(
        cptestctx: &ControlPlaneTestContext,
        instance_id: InstanceUuid,
    ) -> Vmm {
        instance_fetch(cptestctx, instance_id)
            .await
            .vmm()
            .clone()
            .expect("instance should have an active VMM")
    }

    async fn mark_vmm_stopped_for_update(
        conn: &AsyncConnection,
        vmm_id: Uuid,
        generation: UpdateDispositionGeneration,
    ) {
        use nexus_db_schema::schema::vmm::dsl;

        let updated = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(vmm_id))
            .set(
                dsl::stop_for_update_disposition_generation
                    .eq(Some(to_db_typed_generation(generation))),
            )
            .execute_async(conn)
            .await
            .expect("VMM should be marked");
        assert_eq!(updated, 1, "exactly one VMM should be marked");
    }

    // Inserts a VMM row without sled agent knowing about it. This is used for
    // VMMs in states that we don't expect to stop.
    async fn insert_vmm_in_state(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: SledUuid,
        state: DbVmmState,
        marker: Option<UpdateDispositionGeneration>,
    ) -> Vmm {
        let failure_reason = (state == DbVmmState::Failed)
            .then_some(VmmFailureReason::FromSledAgent);
        datastore
            .vmm_insert(
                opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: Uuid::new_v4(),
                    sled_id: sled_id.into(),
                    propolis_ip: "10.1.9.32".parse().unwrap(),
                    propolis_port: 420.into(),
                    cpu_platform: VmmCpuPlatform::SledDefault,
                    time_state_updated: Utc::now(),
                    generation: Generation::new(),
                    state,
                    failure_reason,
                    stop_for_update_disposition_generation: marker
                        .map(Into::into),
                },
            )
            .await
            .expect("VMM should be inserted")
    }

    // `vmm_fetch` does not retrieve rows with a non-null `time_deleted`, so we
    // need a little helper that does.
    async fn fetch_vmms(
        conn: &AsyncConnection,
        ids: &[Uuid],
    ) -> BTreeMap<Uuid, Vmm> {
        use nexus_db_schema::schema::vmm::dsl;

        dsl::vmm
            .filter(dsl::id.eq_any(ids.to_vec()))
            .select(Vmm::as_select())
            .load_async::<Vmm>(conn)
            .await
            .expect("VMMs should be fetched")
            .into_iter()
            .map(|vmm| (vmm.id, vmm))
            .collect()
    }

    fn vmm_ids_in_all_sleds(
        vmms_by_sled: &IdOrdMap<VmmsBySled>,
    ) -> BTreeSet<Uuid> {
        vmms_by_sled
            .iter()
            .flat_map(|sled| {
                sled.vmm_ids.iter().map(|id| id.into_untyped_uuid())
            })
            .collect()
    }

    #[tokio::test]
    async fn test_vmm_stop_for_update_activation() {
        // Set up the test environment.
        //
        // We want two sleds to test that the task stops VMMs across multiple
        // sleds. Additionally, to avoid flakiness, we increase the intervals
        // for background tasks that could potentially modify VMM rows while
        // this test is running.
        let cptestctx =
            ControlPlaneBuilder::new("test_vmm_stop_for_update_activation")
                .with_extra_sled_agents(1)
                .customize_nexus_config(&|config| {
                    let one_day = Duration::from_secs(86_400);
                    let tasks = &mut config.pkg.background_tasks;
                    tasks.instance_watcher.period_secs = one_day;
                    tasks.abandoned_vmm_reaper.period_secs = one_day;
                    tasks.vmm_mark_stop_for_update.period_secs = one_day;
                    tasks.vmm_stop_for_update.period_secs = one_day;
                })
                .start::<crate::Server>()
                .await;

        let client = &cptestctx.external_client;
        let datastore =
            cptestctx.server.server_context().nexus.datastore().clone();
        let nexus = cptestctx.server.server_context().nexus.clone();
        let opctx = test_opctx(&cptestctx);
        let sled_a = cptestctx.first_sled_id();
        let sled_b = cptestctx.second_sled_id();
        let generation = UpdateDispositionGeneration::from(1);

        create_default_ip_pools(client).await;
        create_project(client, PROJECT_NAME).await;

        // The simulated agent can only stop VMMs that are registered with it.
        // So, for each VMM that we intend to stop, we create a real instance
        // via the external API.
        //
        // For each state in `REAL_VMM_STATES`, two instances are created per
        // anti-affinity group. This ensures that both sleds have the same
        // amount of instances in each state.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let mut instance_ids = Vec::new();
        let mut vmm_ids = Vec::new();
        for (label, state) in REAL_VMM_STATES {
            let group = format!("group-{label}");
            create_anti_affinity_group(client, PROJECT_NAME, &group).await;
            for i in 0..2 {
                let id = create_started_instance(
                    &cptestctx,
                    &format!("{label}-{i}"),
                    &group,
                )
                .await;
                match state {
                    // A new VMM in a simulated environment stays in `Starting`
                    // until the "poke" API is called, so we can confidently
                    // test a VMM in this state.
                    DbVmmState::Starting => {}
                    DbVmmState::Running => {
                        instance_simulate(&cptestctx, &id).await;
                    }
                    DbVmmState::Rebooting => {
                        instance_simulate(&cptestctx, &id).await;
                        // Like above, we can keep a VMM in `Rebooting` if we
                        // don't call the "poke" API
                        reboot_instance(&cptestctx, id).await;
                    }
                    _ => unreachable!("{state:?} is not in REAL_VMM_STATES"),
                }
                let vmm = active_vmm(&cptestctx, id).await;
                assert_eq!(vmm.state, state, "VMM {} is not {state:?}", vmm.id);
                // Mark them to be stopped by the task
                mark_vmm_stopped_for_update(&conn, vmm.id, generation).await;
                instance_ids.push(id);
                vmm_ids.push(vmm.id);
            }
        }

        // For all other states that shouldn't be stopped by the task, we insert
        // rows directly in the DB. The task should skip them entirely, so they
        // don't need to be registered with sled agent.
        //
        // We insert marked non-stoppable VMMs. This ensures that even if the
        // task to mark VMMs marks a VMM that is in a state that it shouldn't be
        // stopped in, this task skips it entirely.
        for sled_id in [sled_a, sled_b] {
            for &state in DbVmmState::ALL_STATES {
                let unmarked = insert_vmm_in_state(
                    &datastore, &opctx, sled_id, state, None,
                )
                .await;
                vmm_ids.push(unmarked.id);

                if !REAL_VMM_STATES.iter().any(|(_, real)| *real == state) {
                    let marked = insert_vmm_in_state(
                        &datastore,
                        &opctx,
                        sled_id,
                        state,
                        Some(generation),
                    )
                    .await;
                    vmm_ids.push(marked.id);
                }
            }
        }

        let vmms = fetch_vmms(&conn, &vmm_ids).await;

        // There should be 40 VMM rows:
        //  - 6 VMMs registered with sled agent (one in each of the 3
        //    `REAL_VMM_STATES` on each of the 2 sleds)
        //  - 20 unmarked VMMs (one in every state on each sled)
        //  - 14 marked VMMs (one in each of the states *not* in
        //    `REAL_VMM_STATES` on each sled)
        assert_eq!(vmms.len(), 40);

        // Collect which VMMs we expect the task will stop and which it won't.
        let should_be_stopped = |vmm: &Vmm| {
            vmm.stop_for_update_disposition_generation.is_some()
                && DbVmmState::SHOULD_STOP_FOR_EVACUATION.contains(&vmm.state)
        };
        let (expected_rows_to_be_stopped, expected_rows_to_not_be_stopped): (
            Vec<Vmm>,
            Vec<Vmm>,
        ) = vmms.values().cloned().partition(should_be_stopped);

        assert_eq!(expected_rows_to_be_stopped.len(), 6);

        // Confirm there is one of each stoppable state per sled in
        // `expected_rows_to_be_stopped`
        for sled_id in [sled_a, sled_b] {
            for (_, state) in REAL_VMM_STATES {
                let count = expected_rows_to_be_stopped
                    .iter()
                    .filter(|vmm| {
                        vmm.sled_id() == sled_id && vmm.state == state
                    })
                    .count();
                assert_eq!(
                    count, 1,
                    "sled {sled_id} should have one marked VMM in {state:?}"
                );
            }
        }

        // Run the task.
        let mut task =
            VmmStopForUpdate::new(datastore.clone(), nexus.sagas.clone());
        let status = task.actually_activate(&opctx).await;

        // No errors are expected
        assert_eq!(
            status.error_messages.len(),
            0,
            "unexpected errors: {:?}",
            status.error_messages
        );

        // We stopped the VMMs we expected to stop
        let stopped_count: usize = status
            .vmms_stopped_by_sled
            .iter()
            .map(|sled| sled.vmm_ids.len())
            .sum();
        assert_eq!(stopped_count, expected_rows_to_be_stopped.len());

        let expected_stopped_ids: BTreeSet<Uuid> =
            expected_rows_to_be_stopped.iter().map(|vmm| vmm.id).collect();
        assert_eq!(
            vmm_ids_in_all_sleds(&status.vmms_stopped_by_sled),
            expected_stopped_ids
        );

        // No VMMs are expected to fail to stop
        let failed_to_stop_count: usize = status
            .vmms_failed_to_stop_by_sled
            .iter()
            .map(|sled| sled.vmm_ids.len())
            .sum();
        assert_eq!(failed_to_stop_count, 0);

        // No errors are expected
        assert_eq!(
            status.error_messages.len(),
            0,
            "unexpected errors: {:?}",
            status.error_messages
        );

        // Poke each simulated instance so each sled agent reports back to Nexus
        // and wait until each one reports `SledEvacuating`
        for id in &instance_ids {
            instance_simulate(&cptestctx, id).await;
            instance_wait_for_state(
                &cptestctx,
                *id,
                InstanceState::SledEvacuating,
            )
            .await;
        }
        // Before fetching the VMMs again, make sure all instance update sagas
        // are finished
        wait_for_running_sagas(&cptestctx).await;
        let vmms_after = fetch_vmms(&conn, &vmm_ids).await;
        assert_eq!(vmms_after.len(), 40);

        // Every VMM we expected to stop should be destroyed at this point
        for expected in &expected_rows_to_be_stopped {
            let actual = &vmms_after[&expected.id];
            assert_eq!(
                actual.state,
                DbVmmState::Destroyed,
                "VMM {} on sled {} was not stopped",
                expected.id,
                expected.sled_id(),
            );
            assert!(
                actual.time_deleted.is_some(),
                "VMM {} was not deleted",
                expected.id,
            );
            assert_eq!(
                actual.stop_for_update_disposition_generation,
                expected.stop_for_update_disposition_generation,
            );
        }

        // Make sure all other rows are unchanged
        for expected in &expected_rows_to_not_be_stopped {
            assert_eq!(
                &vmms_after[&expected.id],
                expected,
                "VMM {} on sled {} in state {:?} changed unexpectedly",
                expected.id,
                expected.sled_id(),
                expected.state,
            );
        }

        // Run the task again.
        let status_second_run = task.actually_activate(&opctx).await;

        // No new VMMs were marked to be stopped so nothing is stopped
        assert!(status_second_run.vmms_stopped_by_sled.is_empty());

        // Nothing changed, so we expect no errors or failures
        assert!(status_second_run.vmms_failed_to_stop_by_sled.is_empty());
        assert!(status_second_run.error_messages.is_empty());

        // The second run makes no changes to the VMMs
        let vmms_after_second_run = fetch_vmms(&conn, &vmm_ids).await;
        assert_eq!(vmms_after_second_run, vmms_after);

        cptestctx.teardown().await;
    }
}
