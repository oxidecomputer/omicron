// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures abandoned VMMs are fully destroyed.
//!
//! A VMM is considered "abandoned" if (and only if):
//!
//! - It is in the `Destroyed` state.
//! - It has previously been asigned to an instance.
//! - It is not currently running the instance, and it is also not the
//!   migration target of that instance (i.e. it is no longer pointed to by
//!   the instance record's `active_propolis_id` and `target_propolis_id`
//!   fields).
//! - It has not been deleted yet.

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that searches for abandoned VMM recordss and deletes them.
pub struct AbandonedVmmReaper {
    datastore: Arc<DataStore>,
}

#[derive(Debug, Default)]
struct ActivationResults {
    found: usize,
    sled_resources_deleted: usize,
    sled_resources_already_deleted: usize,
    vmms_deleted: usize,
    vmms_already_deleted: usize,
    error_count: usize,
}

const MAX_BATCH: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl AbandonedVmmReaper {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    async fn reap(
        &mut self,
        results: &mut ActivationResults,
        opctx: &OpContext,
    ) -> Result<(), anyhow::Error> {
        slog::info!(opctx.log, "Abandoned VMM reaper running");

        let mut paginator = Paginator::new(MAX_BATCH);
        let mut last_err = Ok(());
        while let Some(p) = paginator.next() {
            let vmms = self
                .datastore
                .vmm_list_abandoned(opctx, &p.current_pagparams())
                .await
                .context("failed to list abandoned VMMs")?;
            paginator = p.found_batch(&vmms, &|vmm| vmm.id);
            results.found += vmms.len();
            slog::debug!(opctx.log, "Found abandoned VMMs"; "count" => vmms.len());

            for vmm in vmms {
                let vmm_id = vmm.id;
                slog::trace!(opctx.log, "Deleting abandoned VMM"; "vmm" => %vmm_id);
                // Attempt to remove the abandoned VMM from the virtual
                // provisioning collection.
                match self
                    .datastore
                    .sled_reservation_delete(opctx, vmm_id)
                    .await
                {
                    Ok(_) => results.sled_resources_deleted += 1,
                    Err(Error::ObjectNotFound { .. }) => {
                        results.sled_resources_already_deleted += 1;
                    }
                    Err(e) => {
                        slog::warn!(opctx.log, "Failed to delete sled reservation for abandoned VMM";
                            "vmm" => %vmm_id,
                            "error" => %e,
                        );
                        results.error_count += 1;
                        last_err = Err(e).with_context(|| {
                            format!("failed to delete sled reservation for {vmm_id}")
                        });
                    }
                }

                // Now, attempt to mark the VMM record as deleted.
                match self.datastore.vmm_mark_deleted(opctx, &vmm_id).await {
                    Ok(_) => results.vmms_deleted += 1,
                    Err(Error::ObjectNotFound { .. }) => {
                        results.vmms_already_deleted += 1;
                    }
                    Err(e) => {
                        slog::warn!(opctx.log, "Failed to mark abandoned VMM as deleted";
                            "vmm" => %vmm_id,
                            "error" => %e,
                        );
                        results.error_count += 1;
                        last_err = Err(e).with_context(|| {
                            format!("failed to mark {vmm_id} as deleted")
                        });
                    }
                }
            }
        }

        last_err
    }
}

impl BackgroundTask for AbandonedVmmReaper {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let mut results = ActivationResults::default();
            let error = match self.reap(&mut results, opctx).await {
                Ok(_) => {
                    slog::info!(opctx.log, "Abandoned VMMs reaped";
                        "found" => results.found,
                        "sled_resources_deleted" => results.sled_resources_deleted,
                        "sled_resources_already_deleted" => results.sled_resources_already_deleted,
                        "vmms_deleted" => results.vmms_deleted,
                    );
                    None
                }
                Err(err) => {
                    slog::error!(opctx.log, "Abandoned VMM reaper activation failed";
                        "error" => %err,
                        "found" => results.found,
                        "sled_resources_deleted" => results.sled_resources_deleted,
                        "sled_resources_already_deleted" => results.sled_resources_already_deleted,
                        "vmms_deleted" => results.vmms_deleted,
                    );
                    Some(err.to_string())
                }
            };
            serde_json::json!({
                "found": results.found,
                "vmms_deleted": results.vmms_deleted,
                "vmms_already_deleted": results.vmms_already_deleted,
                "sled_resources_deleted": results.sled_resources_deleted,
                "sled_resources_already_deleted": results.sled_resources_already_deleted,
                "error_count": results.error_count,
                "error": error,
            })
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use chrono::Utc;
    use nexus_db_model::ByteCount;
    use nexus_db_model::Generation;
    use nexus_db_model::InstanceState;
    use nexus_db_model::Resources;
    use nexus_db_model::SledResourceKind;
    use nexus_db_model::Vmm;
    use nexus_db_model::VmmRuntimeState;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::InstanceState as ApiInstanceState;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "carcosa";

    #[nexus_test(server = crate::Server)]
    async fn test_abandoned_vmms_are_reaped(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let client = &cptestctx.external_client;
        resource_helpers::create_default_ip_pool(&client).await;

        let _project =
            dbg!(resource_helpers::create_project(client, PROJECT_NAME).await);
        let instance = dbg!(
            resource_helpers::create_instance(client, PROJECT_NAME, "cassilda")
                .await
        );

        let destroyed_vmm_id = Uuid::new_v4();
        datastore
            .vmm_insert(
                &opctx,
                dbg!(Vmm {
                    id: destroyed_vmm_id,
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: instance.identity.id,
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "::1".parse().unwrap(),
                    propolis_port: 12345.into(),
                    runtime: VmmRuntimeState {
                        state: InstanceState::new(ApiInstanceState::Destroyed),
                        time_state_updated: Utc::now(),
                        gen: Generation::new(),
                    }
                }),
            )
            .await
            .expect("it should work");
        let resources = Resources::new(
            1,
            // Just require the bare non-zero amount of RAM.
            ByteCount::try_from(1024).unwrap(),
            ByteCount::try_from(1024).unwrap(),
        );
        let constraints = nexus_db_model::SledReservationConstraints::none();
        let resource = dbg!(datastore
            .sled_reservation_create(
                &opctx,
                destroyed_vmm_id,
                SledResourceKind::Instance,
                resources.clone(),
                constraints,
            )
            .await
            .expect("should work"));

        let mut task = AbandonedVmmReaper::new(datastore.clone());

        let mut results = ActivationResults::default();
        dbg!(task.reap(&mut results, &opctx,).await)
            .expect("activation completes successfully");
        dbg!(&results);

        assert_eq!(results.vmms_deleted, 1);
        assert_eq!(results.sled_resources_deleted, 1);
        assert_eq!(results.vmms_already_deleted, 0);
        assert_eq!(results.sled_resources_already_deleted, 0);
        assert_eq!(results.error_count, 0);
    }
}
