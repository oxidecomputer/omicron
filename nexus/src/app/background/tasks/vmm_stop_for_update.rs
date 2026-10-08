// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that stops VMMs that have been marked as needing to be
//! stopped for a sled update.

use crate::app::background::BackgroundTask;
use crate::app::instance::SledAgentInstanceError;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::VmmStopForUpdateStatus;
use nexus_types::internal_api::background::VmmsBySled;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use serde_json::json;
use sled_agent_client::types::VmmPutStateBody;
use sled_agent_client::types::VmmStateRequested;
use sled_agent_types::instance::SledVmmState;
use sled_agent_types::instance::VmmRuntimeState;
use sled_agent_types::instance::VmmState;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

struct VmmStopForUpdateResult {
    vmms_stopped_by_sled: IdOrdMap<VmmsBySled>,
    vmms_failed_by_sled: IdOrdMap<VmmsBySled>,
    errors: Vec<Error>,
}

impl VmmStopForUpdateResult {
    fn new() -> Self {
        Self {
            vmms_stopped_by_sled: IdOrdMap::new(),
            vmms_failed_by_sled: IdOrdMap::new(),
            errors: Vec::new(),
        }
    }

    /// Merges the values of `self` with a given [`VmmStopForUpdateResult`]
    fn merge(&mut self, other: VmmStopForUpdateResult) {
        let VmmStopForUpdateResult {
            vmms_stopped_by_sled,
            vmms_failed_by_sled,
            errors,
        } = other;

        for VmmsBySled { sled_id, vmm_ids } in vmms_stopped_by_sled {
            self.vmms_stopped_by_sled
                .entry(sled_id)
                .or_insert_with(|| VmmsBySled { sled_id, vmm_ids: Vec::new() })
                .vmm_ids
                .extend(vmm_ids);
        }
        for VmmsBySled { sled_id, vmm_ids } in vmms_failed_by_sled {
            self.vmms_failed_by_sled
                .entry(sled_id)
                .or_insert_with(|| VmmsBySled { sled_id, vmm_ids: Vec::new() })
                .vmm_ids
                .extend(vmm_ids);
        }
        self.errors.extend(errors);
    }
}

pub struct VmmStopForUpdate {
    datastore: Arc<DataStore>,
}

impl VmmStopForUpdate {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    /// Retrieves a list of VMMs marked to be stopped for update by sled, and
    /// stops them in batches.
    async fn stop_all(&self, opctx: &OpContext) -> VmmStopForUpdateResult {
        let mut result = VmmStopForUpdateResult::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let vmms = match self
                .datastore
                .vmm_list_marked_stop_for_update(opctx, &p.current_pagparams())
                .await
            {
                Ok(vmms) => vmms,
                Err(err) => {
                    result.errors.push(err);
                    break;
                }
            };
            paginator = p.found_batch(&vmms, &|vmm| vmm.id);

            let mut vmms_by_sled = IdOrdMap::new();
            for vmm in vmms {
                let sled_id = vmm.sled_id();
                vmms_by_sled
                    .entry(sled_id)
                    .or_insert_with(|| VmmsBySled {
                        sled_id,
                        vmm_ids: Vec::new(),
                    })
                    .vmm_ids
                    .push(PropolisUuid::from_untyped_uuid(vmm.id));
            }
            let batch_result = self.stop_batch(vmms_by_sled, opctx).await;
            result.merge(batch_result);
        }

        result
    }

    /// Stops a batch of VMMs
    async fn stop_batch(
        &self,
        vmms_by_sled: IdOrdMap<VmmsBySled>,
        opctx: &OpContext,
    ) -> VmmStopForUpdateResult {
        let mut result = VmmStopForUpdateResult::new();

        for VmmsBySled { sled_id, vmm_ids } in &vmms_by_sled {
            slog::info!(
                opctx.log,
                "Stopping VMMs for update";
                "sled_id" => %sled_id,
                "vmms" => ?vmm_ids,
            );

            // Since we're using IdOrdMap, each sled id will only appear once.
            // Because of this it's not a big deal to start a new sled agent
            // client per sled id.
            let sled_client = match nexus_networking::sled_client(
                &self.datastore,
                &opctx,
                *sled_id,
                &opctx.log,
            )
            .await
            {
                Ok(client) => client,
                Err(e) => {
                    slog::error!(
                        opctx.log,
                        "Failed to create sled agent client";
                        "sled_id" => %sled_id,
                        InlineErrorChain::new(&e),
                    );

                    result.errors.push(e.clone());

                    continue;
                }
            };

            for id in vmm_ids {
                let response = match sled_client
                    .vmm_put_state(
                        id,
                        &VmmPutStateBody { state: VmmStateRequested::Stopped },
                    )
                    .await
                {
                    Ok(res) => res.into_inner().updated_runtime,
                    Err(e) => {
                        let err = SledAgentInstanceError(e);

                        slog::error!(
                            opctx.log,
                            "Failed to mark VMM as Stopped";
                            "sled_id" => %sled_id,
                            "vmm_id" => %id,
                            InlineErrorChain::new(&err),
                        );

                        result.errors.push(err.into());
                        result
                            .vmms_failed_by_sled
                            .entry(*sled_id)
                            .or_insert_with(|| VmmsBySled {
                                sled_id: *sled_id,
                                vmm_ids: Vec::new(),
                            })
                            .vmm_ids
                            .push(*id);

                        continue;
                    }
                };

                match response {
                    None => {
                        slog::debug!(
                            opctx.log,
                            "VMM already stopped, no changes to state";
                            "sled_id" => %sled_id,
                            "vmm_id" => %id
                        );
                    }
                    Some(vmm) => {
                        let SledVmmState {
                            vmm_state,
                            migration_in: _,
                            migration_out: _,
                        } = vmm;

                        let VmmRuntimeState { state, generation, time_updated } =
                            vmm_state;

                        match state {
                            // If we succeeded, one of three things happened:
                            //
                            //  - The VMM has not stopped yet, but propolis has
                            //    accepted the `put_state` request and reports
                            //    the VMM's state as `Stopping`.
                            //  - A propolis zone does not exist yet. In this
                            //    scenario a VMM could have been in the
                            //    `Creating` state but had not reached the point
                            //    where the propolis zone was created. In this
                            //    case the request succeeded, but the VMM is
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
                                    .entry(*sled_id)
                                    .or_insert_with(|| VmmsBySled {
                                        sled_id: *sled_id,
                                        vmm_ids: Vec::new(),
                                    })
                                    .vmm_ids
                                    .push(*id);

                                slog::debug!(
                                    opctx.log,
                                    "Stopped VMM for sled evacuation during \
                                    an update";
                                    "sled_id" => %sled_id,
                                    "vmm_id" => %id,
                                    "vmm_generation" => ?generation,
                                    "time_updated" => ?time_updated,
                                );
                            }
                            // Propolis is alive but does not find a VMM. This
                            // implies that it restarted and lost the previously
                            // created VMM. The VMM is reported as Failed. It's
                            // not necessary to stop it anymore, so we don't add
                            // it to the `vmms_failed_by_sled` list.
                            VmmState::Failed => {
                                slog::info!(
                                    opctx.log,
                                    "Unable to find VMM, possibly due to a \
                                    restart. No need to stop it";
                                    "sled_id" => %sled_id,
                                    "vmm_id" => %id,
                                    "state" => ?state,
                                    "vmm_generation" => ?generation,
                                    "time_updated" => ?time_updated,
                                );
                            }
                            VmmState::Migrating
                            | VmmState::Rebooting
                            | VmmState::Starting
                            | VmmState::Running => {
                                slog::error!(
                                    opctx.log,
                                    "Failed to mark VMM as Stopped, \
                                    unexpected state reported";
                                    "sled_id" => %sled_id,
                                    "vmm_id" => %id,
                                    "state" => ?state,
                                    "vmm_generation" => ?generation,
                                    "time_updated" => ?time_updated,
                                );
                            }
                        }
                    }
                }
            }
        }

        result
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> VmmStopForUpdateStatus {
        let VmmStopForUpdateResult {
            vmms_stopped_by_sled,
            vmms_failed_by_sled,
            errors,
        } = self.stop_all(opctx).await;

        let error_messages: Vec<String> = errors
            .iter()
            .map(|err| InlineErrorChain::new(err).to_string())
            .collect();

        if vmms_stopped_by_sled.is_empty() {
            slog::info!(
                &opctx.log,
                "no VMMs were stopped for a sled update";
            );
        } else {
            for sled in &vmms_stopped_by_sled {
                let VmmsBySled { sled_id, vmm_ids } = sled;
                slog::info!(
                    &opctx.log,
                    "stopped {} VMMs marked to stop for a sled update",
                    vmm_ids.len();
                    "sled_id" => %sled_id,
                    "vmms" => ?vmm_ids,
                );
            }
        }

        if !vmms_failed_by_sled.is_empty() {
            for sled in &vmms_failed_by_sled {
                let VmmsBySled { sled_id, vmm_ids } = sled;
                slog::info!(
                    &opctx.log,
                    "failed to stop {} VMMs marked to stop for a sled update",
                    vmm_ids.len();
                    "sled_id" => %sled_id,
                    "vmms" => ?vmm_ids,
                );
            }
        }

        if !error_messages.is_empty() {
            slog::error!(
                &opctx.log,
                "failed to stop VMMs marked to stop for a sled update";
                "errors" => ?error_messages,
            );
        }

        VmmStopForUpdateStatus {
            vmms_stopped_by_sled,
            vmms_failed_by_sled,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::sagas::test_helpers;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
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
    use nexus_types::external_api::instance;
    use nexus_types_versions::latest;
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

    // TODO-K: Fix comment The states of the real (simulated) VMMs that the
    // task should stop. The label is used to name instances and groups.
    //
    // TODO-K: Explain why we can't have a `Creating` VMM
    const REAL_VMM_STATES: [(&str, DbVmmState); 3] = [
        ("starting", DbVmmState::Starting),
        ("running", DbVmmState::Running),
        ("rebooting", DbVmmState::Rebooting),
    ];

    // TODO-K: Fix comment Creates and starts an instance that belongs to
    // `anti_affinity_group`, and returns its ID.
    async fn create_started_instance(
        cptestctx: &ControlPlaneTestContext,
        name: &str,
        anti_affinity_group: &str,
    ) -> InstanceUuid {
        let created = object_create::<_, latest::instance::Instance>(
            &cptestctx.external_client,
            &format!("/v1/instances?project={PROJECT_NAME}"),
            &instance::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: format!("instance {name}"),
                },
                ncpus: 1i64.try_into().unwrap(),
                memory: ByteCount::from_gibibytes_u32(1),
                hostname: "myhostname".try_into().unwrap(),
                user_data: Vec::new(),
                network_interfaces:
                    instance::InstanceNetworkInterfaceAttachment::None,
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

    // TODO-K: Fix comment Asks Nexus to reboot a running instance. The
    // simulated sled agent leaves the VMM in `Rebooting` until it is poked.
    async fn reboot_instance(
        cptestctx: &ControlPlaneTestContext,
        instance_id: InstanceUuid,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_helpers::test_opctx(cptestctx);
        let lookup = nexus
            .instance_lookup(
                &opctx,
                instance::InstanceSelector {
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
        test_helpers::instance_fetch(cptestctx, instance_id)
            .await
            .vmm()
            .clone()
            .expect("instance should have an active VMM")
    }

    // TODO-K: Fix comment Marks a single VMM to be stopped for an update.
    async fn mark_vmm(
        conn: &nexus_db_lookup::AsyncConnection,
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

    // TODO-K: Fix comment Inserts a VMM row that no sled agent knows about.
    async fn insert_vmm(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: SledUuid,
        state: DbVmmState,
        marker: Option<UpdateDispositionGeneration>,
    ) -> Vmm {
        // TODO-K: Fix comment The `failure_reason_iff_failed` constraint
        // requires a failure reason for `Failed` VMMs, and only for them.
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

    // TODO-K: Fix comment Fetches the given VMM rows, including deleted rows
    // (unlike `vmm_fetch`, which skips them).
    async fn fetch_vmms(
        conn: &nexus_db_lookup::AsyncConnection,
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

    fn vmm_ids_in(sleds: &IdOrdMap<VmmsBySled>) -> BTreeSet<Uuid> {
        sleds
            .iter()
            .flat_map(|sled| {
                sled.vmm_ids.iter().map(|id| id.into_untyped_uuid())
            })
            .collect()
    }

    #[tokio::test]
    async fn test_vmm_stop_for_update_activation() {
        // TODO-K: Fix comment Use two simulated sleds. Lengthen the periods
        // of the background tasks that could change the VMM rows while this
        // test runs. Each of them still runs once when Nexus starts, before
        // any of the rows below exist.
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
        let opctx = test_helpers::test_opctx(&cptestctx);
        let sled_a = cptestctx.first_sled_id();
        let sled_b = cptestctx.second_sled_id();
        let generation = UpdateDispositionGeneration::from(1);

        create_default_ip_pools(client).await;
        create_project(client, PROJECT_NAME).await;

        // TODO-K: Fix comment Step 1: create the VMM rows.
        //
        // The simulated sled agents only know about VMMs that an instance
        // start registered with them, so the marked VMMs that the task should
        // stop are real VMMs. For each state in `REAL_VMM_STATES`, start two
        // instances in one anti-affinity group, so that one VMM lands on each
        // sled, put the VMMs in that state, and mark them.
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
                    // A new VMM in a simulatated environemnt stays in
                    // `Starting` until the `poke` API is called.
                    DbVmmState::Starting => {}
                    DbVmmState::Running => {
                        test_helpers::instance_simulate(&cptestctx, &id).await;
                    }
                    DbVmmState::Rebooting => {
                        test_helpers::instance_simulate(&cptestctx, &id).await;
                        reboot_instance(&cptestctx, id).await;
                    }
                    _ => unreachable!("{state:?} is not in REAL_VMM_STATES"),
                }
                let vmm = active_vmm(&cptestctx, id).await;
                assert_eq!(vmm.state, state, "VMM {} is not {state:?}", vmm.id);
                mark_vmm(&conn, vmm.id, generation).await;
                instance_ids.push(id);
                vmm_ids.push(vmm.id);
            }
        }

        // TODO-K: Fix comment The task never contacts a sled agent about the
        // other rows, so they are inserted directly: one unmarked VMM in every
        // state, and one marked VMM in every state that no real VMM covers.
        // That includes a marked `Creating` VMM, which no sled agent knows.
        for sled_id in [sled_a, sled_b] {
            for &state in DbVmmState::ALL_STATES {
                let unmarked =
                    insert_vmm(&datastore, &opctx, sled_id, state, None).await;
                vmm_ids.push(unmarked.id);
                if !REAL_VMM_STATES.iter().any(|(_, real)| *real == state) {
                    let marked = insert_vmm(
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

        // TODO-K: Fix comment Step 2: retrieve every VMM row created above.
        let vmms = fetch_vmms(&conn, &vmm_ids).await;
        // TODO-K: Fix comment There should be 40 rows: 6 real VMMs (one in
        // each of the 3 `REAL_VMM_STATES` on each of the 2 sleds), 20 unmarked
        // VMMs (one in each of the 10 states on each sled), and 14 marked VMMs
        // (one in each of the 7 states without a real VMM on each sled).
        assert_eq!(vmms.len(), 40);

        // TODO-K: Fix comment Step 3: split the rows into the ones the task
        // should stop and the ones it should leave unchanged. A marked
        // `Creating` VMM is stoppable, but no sled agent knows about it, so
        // the stop request fails (404) and the row stays unchanged.
        let should_be_stopped = |vmm: &Vmm| {
            vmm.stop_for_update_disposition_generation.is_some()
                && DbVmmState::SHOULD_STOP_FOR_EVACUATION.contains(&vmm.state)
                && vmm.state != DbVmmState::Creating
        };
        let (expected_rows_to_be_stopped, expected_rows_to_be_unchanged): (
            Vec<Vmm>,
            Vec<Vmm>,
        ) = vmms.values().cloned().partition(should_be_stopped);
        let expected_failed_ids: BTreeSet<Uuid> = expected_rows_to_be_unchanged
            .iter()
            .filter(|vmm| {
                vmm.stop_for_update_disposition_generation.is_some()
                    && vmm.state == DbVmmState::Creating
            })
            .map(|vmm| vmm.id)
            .collect();
        assert_eq!(expected_rows_to_be_stopped.len(), 6);
        assert_eq!(expected_failed_ids.len(), 2);
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

        // TODO-K: Fix comment Step 4: run the task.
        let mut task = VmmStopForUpdate::new(datastore.clone());
        let status = task.actually_activate(&opctx).await;

        // TODO-K: Fix comment Step 5: check the task's status.
        let stopped_count: usize = status
            .vmms_stopped_by_sled
            .iter()
            .map(|sled| sled.vmm_ids.len())
            .sum();
        assert_eq!(stopped_count, expected_rows_to_be_stopped.len());
        let expected_stopped_ids: BTreeSet<Uuid> =
            expected_rows_to_be_stopped.iter().map(|vmm| vmm.id).collect();
        assert_eq!(
            vmm_ids_in(&status.vmms_stopped_by_sled),
            expected_stopped_ids
        );
        let failed_count: usize = status
            .vmms_failed_by_sled
            .iter()
            .map(|sled| sled.vmm_ids.len())
            .sum();
        assert_eq!(failed_count, expected_failed_ids.len());
        assert_eq!(
            vmm_ids_in(&status.vmms_failed_by_sled),
            expected_failed_ids
        );
        assert_eq!(status.error_messages.len(), expected_failed_ids.len());

        // TODO-K: Fix comment Step 6: the task does not write VMM states to
        // the database. The simulated sled agents report the rest of each
        // stop to Nexus only when they are poked. Poke each stopped VMM, wait
        // for its instance to reach `NoVmm`, wait for the instance-update
        // sagas to finish, and then retrieve every row again.
        for id in &instance_ids {
            test_helpers::instance_simulate(&cptestctx, id).await;
            test_helpers::instance_wait_for_state(
                &cptestctx,
                *id,
                InstanceState::NoVmm,
            )
            .await;
        }
        test_helpers::wait_for_running_sagas(&cptestctx).await;
        let vmms_after = fetch_vmms(&conn, &vmm_ids).await;
        assert_eq!(vmms_after.len(), 40);

        // TODO-K: Fix comment Step 7: every VMM the task stopped is now
        // `Destroyed`, the instance-update saga marked it deleted, and it is
        // still marked to be stopped for an update.
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

        // TODO-K: Fix comment Step 8: every other row is exactly as it was.
        for expected in &expected_rows_to_be_unchanged {
            assert_eq!(
                &vmms_after[&expected.id],
                expected,
                "VMM {} on sled {} in state {:?} changed unexpectedly",
                expected.id,
                expected.sled_id(),
                expected.state,
            );
        }

        cptestctx.teardown().await;
    }
}
