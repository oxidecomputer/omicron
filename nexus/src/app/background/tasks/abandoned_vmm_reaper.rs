// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures abandoned VMMs are fully destroyed.
//!
//! A VMM is considered "abandoned" if (and only if):
//!
//! - It is in the `Destroyed` state.
//! - It is not currently running an instance, and it is also not the
//!   migration target of any instance (i.e. it is not pointed to by
//!   any instance record's `active_propolis_id` and `target_propolis_id`
//!   fields).
//! - It has not been deleted yet.
//!
//! VMMs are abandoned when the instance they are responsible for migrates.
//! Should the migration succeed, the previously occupied VMM process is now
//! abandoned. If a migration is attempted but fails, the *target* VMM is now
//! abandoned, as the instance remains on the source VMM.
//!
//! Such VMMs may be deleted fairly simply: any sled resources reserved for the
//! VMM process can be deallocated, and the VMM record in the database is then
//! marked as deleted. Note that reaping abandoned VMMs does not require
//! deallocating virtual provisioning resources, NAT entries, and other such
//! resources which are owned by the *instance*, rather than the VMM process;
//! this task is only responsible for cleaning up VMMs left behind by an
//! instance that has moved to *another* VMM process. The instance itself
//! remains alive and continues to own its virtual provisioning resources.
//!
//! Cleanup of instance resources when an instance's *active* VMM is destroyed
//! is handled elsewhere, by `notify_instance_updated` and (eventually) the
//! `instance-update` saga.

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use omicron_uuid_kinds::{GenericUuid, PropolisUuid};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that searches for abandoned VMM records and deletes them.
pub struct AbandonedVmmReaper {
    datastore: Arc<DataStore>,
}

#[derive(Debug, Default)]
struct ActivationResults {
    found: usize,
    sled_reservations_deleted: usize,
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

    /// List abandoned VMMs and clean up all of their database records.
    async fn reap_all(
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
            self.reap_batch(results, &mut last_err, opctx, &vmms).await;
        }

        last_err
    }

    /// Clean up a batch of abandoned VMMs.
    ///
    /// This is separated out from `reap_all` to facilitate testing situations
    /// where we race with another Nexus instance to delete an abandoned VMM. In
    /// order to deterministically simulate such cases, we have to perform the
    /// query to list abandoned VMMs, ensure that the VMM record is deleted, and
    /// *then* perform the cleanup with the stale list of abandoned VMMs, rather
    /// than doing it all in one go. Thus, this is factored out.
    async fn reap_batch(
        &mut self,
        results: &mut ActivationResults,
        last_err: &mut Result<(), anyhow::Error>,
        opctx: &OpContext,
        vmms: &[Vmm],
    ) {
        results.found += vmms.len();
        slog::debug!(opctx.log, "Found abandoned VMMs"; "count" => vmms.len());

        for vmm in vmms {
            let vmm_id = PropolisUuid::from_untyped_uuid(vmm.id);
            slog::trace!(opctx.log, "Deleting abandoned VMM"; "vmm" => %vmm_id);
            // Attempt to remove the abandoned VMM's sled resource reservation.
            match self
                .datastore
                .sled_reservation_delete(opctx, vmm_id.into_untyped_uuid())
                .await
            {
                Ok(_) => {
                    slog::trace!(
                        opctx.log,
                        "Deleted abandoned VMM's sled reservation";
                        "vmm" => %vmm_id,
                    );
                    results.sled_reservations_deleted += 1;
                }
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "Failed to delete sled reservation for abandoned VMM";
                        "vmm" => %vmm_id,
                        "error" => %e,
                    );
                    results.error_count += 1;
                    *last_err = Err(e).with_context(|| {
                        format!(
                            "failed to delete sled reservation for VMM {vmm_id}"
                        )
                    });
                }
            }

            // Now, attempt to mark the VMM record as deleted.
            match self.datastore.vmm_mark_deleted(opctx, &vmm_id).await {
                Ok(true) => {
                    slog::trace!(
                        opctx.log,
                        "Deleted abandoned VMM";
                        "vmm" => %vmm_id,
                    );
                    results.vmms_deleted += 1;
                }
                Ok(false) => {
                    slog::trace!(
                        opctx.log,
                        "Abandoned VMM was already deleted";
                        "vmm" => %vmm_id,
                    );
                    results.vmms_already_deleted += 1;
                }
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "Failed to mark abandoned VMM as deleted";
                        "vmm" => %vmm_id,
                        "error" => %e,
                    );
                    results.error_count += 1;
                    *last_err = Err(e).with_context(|| {
                        format!("failed to mark VMM {vmm_id} as deleted")
                    });
                }
            }
        }
    }
}

impl BackgroundTask for AbandonedVmmReaper {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let mut results = ActivationResults::default();
            let error = match self.reap_all(&mut results, opctx).await {
                Ok(_) => {
                    slog::info!(opctx.log, "Abandoned VMMs reaped";
                        "found" => results.found,
                        "sled_reservations_deleted" => results.sled_reservations_deleted,
                        "vmms_deleted" => results.vmms_deleted,
                        "vmms_already_deleted" => results.vmms_already_deleted,
                    );
                    None
                }
                Err(err) => {
                    slog::error!(opctx.log, "Abandoned VMM reaper activation failed";
                        "error" => %err,
                        "found" => results.found,
                        "sled_reservations_deleted" => results.sled_reservations_deleted,
                        "vmms_deleted" => results.vmms_deleted,
                        "vmms_already_deleted" => results.vmms_already_deleted,
                    );
                    Some(err.to_string())
                }
            };
            serde_json::json!({
                "found": results.found,
                "vmms_deleted": results.vmms_deleted,
                "vmms_already_deleted": results.vmms_already_deleted,
                "sled_reservations_deleted": results.sled_reservations_deleted,
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
    use nexus_db_model::Resources;
    use nexus_db_model::SledResource;
    use nexus_db_model::SledResourceKind;
    use nexus_db_model::Vmm;
    use nexus_db_model::VmmRuntimeState;
    use nexus_db_model::VmmState;
    use nexus_test_utils::resource_helpers;
    use nexus_test_utils_macros::nexus_test;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "carcosa";

    struct TestFixture {
        destroyed_vmm_id: PropolisUuid,
    }

    impl TestFixture {
        async fn setup(
            client: &dropshot::test_util::ClientTestContext,
            datastore: &Arc<DataStore>,
            opctx: &OpContext,
        ) -> Self {
            resource_helpers::create_default_ip_pool(&client).await;

            let _project =
                resource_helpers::create_project(client, PROJECT_NAME).await;
            let instance = resource_helpers::create_instance(
                client,
                PROJECT_NAME,
                "cassilda",
            )
            .await;

            let destroyed_vmm_id = PropolisUuid::new_v4();
            datastore
                .vmm_insert(
                    &opctx,
                    dbg!(Vmm {
                        id: destroyed_vmm_id.into_untyped_uuid(),
                        time_created: Utc::now(),
                        time_deleted: None,
                        instance_id: instance.identity.id,
                        sled_id: Uuid::new_v4(),
                        propolis_ip: "::1".parse().unwrap(),
                        propolis_port: 12345.into(),
                        runtime: VmmRuntimeState {
                            state: VmmState::Destroyed,
                            time_state_updated: Utc::now(),
                            gen: Generation::new(),
                        }
                    }),
                )
                .await
                .expect("destroyed vmm record should be created successfully");
            let resources = Resources::new(
                1,
                // Just require the bare non-zero amount of RAM.
                ByteCount::try_from(1024).unwrap(),
                ByteCount::try_from(1024).unwrap(),
            );
            let constraints =
                nexus_db_model::SledReservationConstraints::none();
            dbg!(datastore
                .sled_reservation_create(
                    &opctx,
                    destroyed_vmm_id.into_untyped_uuid(),
                    SledResourceKind::Instance,
                    resources.clone(),
                    constraints,
                )
                .await
                .expect("sled reservation should be created successfully"));
            Self { destroyed_vmm_id }
        }

        async fn assert_reaped(&self, datastore: &DataStore) {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::{
                ExpressionMethods, OptionalExtension, QueryDsl,
                SelectableHelper,
            };
            use nexus_db_queries::db::schema::sled_resource::dsl as sled_resource_dsl;
            use nexus_db_queries::db::schema::vmm::dsl as vmm_dsl;

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let fetched_vmm = vmm_dsl::vmm
                .filter(
                    vmm_dsl::id.eq(self.destroyed_vmm_id.into_untyped_uuid()),
                )
                .filter(vmm_dsl::time_deleted.is_null())
                .select(Vmm::as_select())
                .first_async::<Vmm>(&*conn)
                .await
                .optional()
                .expect("VMM query should succeed");
            assert!(
                dbg!(fetched_vmm).is_none(),
                "VMM record should have been deleted"
            );

            let fetched_sled_resource = sled_resource_dsl::sled_resource
                .filter(
                    sled_resource_dsl::id
                        .eq(self.destroyed_vmm_id.into_untyped_uuid()),
                )
                .select(SledResource::as_select())
                .first_async::<SledResource>(&*conn)
                .await
                .optional()
                .expect("sled resource query should succeed");
            assert!(
                dbg!(fetched_sled_resource).is_none(),
                "sled resource record should have been deleted"
            );
        }
    }

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
        let fixture =
            TestFixture::setup(&cptestctx.external_client, datastore, &opctx)
                .await;

        let mut task = AbandonedVmmReaper::new(datastore.clone());

        let mut results = ActivationResults::default();
        dbg!(task.reap_all(&mut results, &opctx,).await)
            .expect("activation completes successfully");
        dbg!(&results);

        assert_eq!(results.vmms_deleted, 1);
        assert_eq!(results.sled_reservations_deleted, 1);
        assert_eq!(results.vmms_already_deleted, 0);
        assert_eq!(results.error_count, 0);
        fixture.assert_reaped(datastore).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn vmm_already_deleted(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let fixture =
            TestFixture::setup(&cptestctx.external_client, datastore, &opctx)
                .await;

        // For this test, we separate the database query run by the background
        // task to list abandoned VMMs from the actual cleanup of those VMMs, in
        // order to simulate a condition where the VMM record was deleted
        // between when the listing query was run and when the bg task attempted
        // to delete the VMM record.
        let paginator = Paginator::new(MAX_BATCH);
        let p = paginator.next().unwrap();
        let abandoned_vmms = datastore
            .vmm_list_abandoned(&opctx, &p.current_pagparams())
            .await
            .expect("must list abandoned vmms");

        assert!(!abandoned_vmms.is_empty());

        datastore
            .vmm_mark_deleted(&opctx, &fixture.destroyed_vmm_id)
            .await
            .expect("simulate another nexus marking the VMM deleted");

        let mut results = ActivationResults::default();
        let mut last_err = Ok(());
        let mut task = AbandonedVmmReaper::new(datastore.clone());
        task.reap_batch(&mut results, &mut last_err, &opctx, &abandoned_vmms)
            .await;
        dbg!(last_err).expect("should not have errored");
        dbg!(&results);

        assert_eq!(results.found, 1);
        assert_eq!(results.vmms_deleted, 0);
        assert_eq!(results.sled_reservations_deleted, 1);
        assert_eq!(results.vmms_already_deleted, 1);
        assert_eq!(results.error_count, 0);

        fixture.assert_reaped(datastore).await
    }

    #[nexus_test(server = crate::Server)]
    async fn sled_resource_already_deleted(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let fixture =
            TestFixture::setup(&cptestctx.external_client, datastore, &opctx)
                .await;

        // For this test, we separate the database query run by the background
        // task to list abandoned VMMs from the actual cleanup of those VMMs, in
        // order to simulate a condition where the sled reservation record was
        // deleted between when the listing query was run and when the bg task
        // attempted to delete the sled reservation..
        let paginator = Paginator::new(MAX_BATCH);
        let p = paginator.next().unwrap();
        let abandoned_vmms = datastore
            .vmm_list_abandoned(&opctx, &p.current_pagparams())
            .await
            .expect("must list abandoned vmms");

        assert!(!abandoned_vmms.is_empty());

        datastore
            .sled_reservation_delete(
                &opctx,
                fixture.destroyed_vmm_id.into_untyped_uuid(),
            )
            .await
            .expect(
                "simulate another nexus marking the sled reservation deleted",
            );

        let mut results = ActivationResults::default();
        let mut last_err = Ok(());
        let mut task = AbandonedVmmReaper::new(datastore.clone());
        task.reap_batch(&mut results, &mut last_err, &opctx, &abandoned_vmms)
            .await;
        dbg!(last_err).expect("should not have errored");
        dbg!(&results);

        assert_eq!(results.found, 1);
        assert_eq!(results.vmms_deleted, 1);
        assert_eq!(results.sled_reservations_deleted, 1);
        assert_eq!(results.vmms_already_deleted, 0);
        assert_eq!(results.error_count, 0);

        fixture.assert_reaped(datastore).await
    }
}
