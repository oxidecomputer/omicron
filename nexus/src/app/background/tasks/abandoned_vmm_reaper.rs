// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures abandoned VMMs are fully destroyed.
//!
//! A VMM is considered "abandoned" if (and only if):
//!
//! - It is in the `Destroyed`, `Failed`, or `SagaUnwound` state.
//! - It is not currently running an instance, and it is also not the
//!   migration target of an instance (i.e. it is not pointed to by its
//!   owning instance's `active_propolis_id` or `target_propolis_id`
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
//! is handled elsewhere, by `process_vmm_update` and the `instance-update`
//! saga.

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_model::Vmm;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::AbandonedVmmReaperStatus;
use nexus_types::internal_api::background::abandoned_vmm_reaper as status;
use omicron_uuid_kinds::{GenericUuid, PropolisUuid};
use slog_error_chain::InlineErrorChain;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that searches for abandoned VMM records and deletes them.
pub struct AbandonedVmmReaper {
    datastore: Arc<DataStore>,
}

impl AbandonedVmmReaper {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    async fn reap_all(
        &mut self,
        opctx: &OpContext,
        batch_size: NonZeroU32,
    ) -> AbandonedVmmReaperStatus {
        let vmms = self.reap_all_vmms(opctx, batch_size).await;
        if !vmms.errors.is_empty() {
            slog::error!(
                opctx.log,
                "Reaping abandoned VMMs failed";
                "errors" => vmms.errors.len(),
                "vmms_found" => vmms.found,
                "sled_reservations_deleted" => vmms.sled_reservations_deleted,
                "vmms_deleted" => vmms.deleted,
                "vmms_already_deleted" => vmms.already_deleted,
            );
        } else if vmms.found > 0 {
            slog::info!(
                opctx.log,
                "Abandoned VMMs reaped";
                "vmms_found" => vmms.found,
                "sled_reservations_deleted" => vmms.sled_reservations_deleted,
                "vmms_deleted" => vmms.deleted,
                "vmms_already_deleted" => vmms.already_deleted,
            );
        } else {
            slog::debug!(opctx.log, "No abandoned VMM records found");
        }

        // Now, reap any abandoned sled resource allocation records. This
        // includes all tombstoned records that have no VMM record.
        let reservations = self.reap_sled_reservations(opctx, batch_size).await;
        if !reservations.errors.is_empty() {
            slog::error!(
                opctx.log,
                "Reaping abandoned sled resource reservations failed";
                "errors" => reservations.errors.len(),
                "sled_reservations_found" => reservations.found,
                "sled_reservations_deleted" => reservations.deleted,
            );
        } else if reservations.found > 0 {
            slog::info!(
                opctx.log,
                "Abandoned sled resource reservations reaped";
                "sled_reservations_found" => reservations.found,
                "sled_reservations_deleted" => reservations.deleted,
            );
        } else {
            slog::debug!(opctx.log, "No abandoned sled resource records found");
        }

        AbandonedVmmReaperStatus { batch_size, vmms, reservations }
    }

    /// List abandoned VMMs and clean up all of their database records.
    async fn reap_all_vmms(
        &mut self,
        opctx: &OpContext,
        batch_size: NonZeroU32,
    ) -> status::AbandonedVmms {
        let mut status = status::AbandonedVmms {
            batches: 0,
            found: 0,
            deleted: 0,
            already_deleted: 0,
            sled_reservations_deleted: 0,
            errors: Vec::new(),
        };

        let mut paginator =
            Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
        while let Some(p) = paginator.next() {
            let vmms = match self
                .datastore
                .vmm_list_abandoned(opctx, &p.current_pagparams())
                .await
            {
                Ok(vmms) => vmms,
                Err(e) => {
                    const ERR_MSG: &'static str =
                        "Failed to list abandoned VMM records";
                    let error = InlineErrorChain::new(&e);
                    slog::error!(opctx.log,"{ERR_MSG}"; &error);
                    status.errors.push(format!("{ERR_MSG}: {error}"));
                    break;
                }
            };
            paginator = p.found_batch(&vmms, &|vmm| vmm.id);
            self.reap_vmm_batch(&mut status, opctx, &vmms).await;
        }

        status
    }

    /// Clean up a batch of abandoned VMMs.
    ///
    /// This is separated out from `reap_all` to facilitate testing situations
    /// where we race with another Nexus instance to delete an abandoned VMM. In
    /// order to deterministically simulate such cases, we have to perform the
    /// query to list abandoned VMMs, ensure that the VMM record is deleted, and
    /// *then* perform the cleanup with the stale list of abandoned VMMs, rather
    /// than doing it all in one go. Thus, this is factored out.
    async fn reap_vmm_batch(
        &mut self,
        status: &mut status::AbandonedVmms,
        opctx: &OpContext,
        vmms: &[Vmm],
    ) {
        status.found += vmms.len();
        slog::debug!(
            opctx.log,
            "Found abandoned VMMs";
            "count" => vmms.len(),
            "total" => status.found,
        );

        for vmm in vmms {
            let vmm_id = PropolisUuid::from_untyped_uuid(vmm.id);
            slog::trace!(opctx.log, "Deleting abandoned VMM"; "vmm" => %vmm_id);

            self.delete_sled_reservation(
                opctx,
                vmm_id,
                &mut status.sled_reservations_deleted,
                &mut status.errors,
            )
            .await;

            // Now, attempt to mark the VMM record as deleted.
            match self.datastore.vmm_mark_deleted(opctx, &vmm_id).await {
                Ok(true) => {
                    slog::trace!(
                        opctx.log,
                        "Deleted abandoned VMM";
                        "vmm" => %vmm_id,
                    );
                    status.deleted += 1;
                }
                Ok(false) => {
                    slog::trace!(
                        opctx.log,
                        "Abandoned VMM was already deleted";
                        "vmm" => %vmm_id,
                    );
                    status.already_deleted += 1;
                }
                Err(e) => {
                    const ERR_MSG: &'static str = "Failed to delete";
                    slog::warn!(
                        opctx.log,
                        "{ERR_MSG} abandoned VMM";
                        "vmm" => %vmm_id,
                        "error" => %e,
                    );
                    status.errors.push(format!("{ERR_MSG} {vmm_id}: {e}"))
                }
            }
        }
    }

    async fn reap_sled_reservations(
        &mut self,
        opctx: &OpContext,
        batch_size: NonZeroU32,
    ) -> status::AbandonedReservations {
        let mut status = status::AbandonedReservations {
            batches: 0,
            found: 0,
            deleted: 0,
            errors: Vec::new(),
        };

        let mut paginator =
            Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
        while let Some(p) = paginator.next() {
            let batch = match self
                .datastore
                .sled_reservation_list_abandoned(opctx, &p.current_pagparams())
                .await
            {
                Ok(batch) => batch,
                Err(e) => {
                    const ERR_MSG: &'static str =
                        "Failed to list abandoned sled reservations";
                    let error = InlineErrorChain::new(&e);
                    slog::error!(opctx.log,"{ERR_MSG}"; &error);
                    status.errors.push(format!("{ERR_MSG}: {error}"));
                    break;
                }
            };

            status.batches += 1;
            status.found += batch.len();
            paginator = p.found_batch(&batch, &|v| *v);

            slog::debug!(
                opctx.log,
                "Found abandoned sled resource reservations";
                "count" => batch.len(),
                "total" => status.found,
            );

            for id in batch {
                self.delete_sled_reservation(
                    opctx,
                    id.into(),
                    &mut status.deleted,
                    &mut status.errors,
                )
                .await;
            }
        }

        status
    }

    async fn delete_sled_reservation(
        &mut self,
        opctx: &OpContext,
        vmm_id: PropolisUuid,
        deleted_count: &mut usize,
        errors: &mut Vec<String>,
    ) {
        // Attempt to remove the abandoned VMM's sled resource reservation.
        match self.datastore.sled_reservation_delete(opctx, vmm_id).await {
            Ok(_) => {
                slog::trace!(
                    opctx.log,
                    "Deleted abandoned VMM's sled reservation";
                    "vmm" => %vmm_id,
                );
                *deleted_count += 1;
            }
            Err(e) => {
                const ERR_MSG: &'static str =
                    "Failed to delete sled reservation";
                let error = InlineErrorChain::new(&e);
                slog::warn!(
                    opctx.log,
                    "{ERR_MSG} for abandoned VMM";
                    "vmm" => %vmm_id,
                    &error,
                );
                errors.push(format!("{ERR_MSG} for {vmm_id}: {error}"));
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
            let status = self.reap_all(opctx, SQL_BATCH_SIZE).await;
            serde_json::json!(status)
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
    use nexus_db_model::SledResourceVmm;
    use nexus_db_model::SledResourceVmmState;
    use nexus_db_model::Vmm;
    use nexus_db_model::VmmCpuPlatform;
    use nexus_db_model::VmmFailureReason;
    use nexus_db_model::VmmState;
    use nexus_test_utils_macros::nexus_test;
    use omicron_uuid_kinds::InstanceUuid;
    use omicron_uuid_kinds::SledUuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    /// Describes the `vmm` record (if any) that accompanies a sled resource
    /// reservation created by a [`TestFixture`].
    ///
    /// The variants correspond to the three ways in which
    /// [`DataStore::sled_reservation_list_abandoned`] can determine that no
    /// live VMM is using a reservation's resources.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum VmmRecord {
        /// No `vmm` record exists for the reservation's ID at all, simulating a
        /// VMM that has somehow been hard-deleted from the `vmm` table.
        None,
        /// A `vmm` record exists in the given state and has *not* been
        /// soft-deleted.
        Present(VmmState),
        /// A `vmm` record exists in the given state but has been soft-deleted
        /// (its `time_deleted` is set).
        Deleted(VmmState),
    }

    impl VmmRecord {
        /// Whether this VMM record satisfies one of the VMM conditions in
        /// [`DataStore::sled_reservation_list_abandoned`] --- i.e. no live VMM
        /// is using the reserved resources.
        fn no_live_vmm(self) -> bool {
            match self {
                // `vmm.state IS NULL` in the left join.
                VmmRecord::None => true,
                // `vmm.time_deleted IS NOT NULL`.
                VmmRecord::Deleted(_) => true,
                // `vmm.state = ANY(DESTROYABLE_STATES)`.
                VmmRecord::Present(state) => {
                    VmmState::DESTROYABLE_STATES.contains(&state)
                }
            }
        }
    }

    /// A single `vmm` + `sled_resource_vmm` pair for a [`TestFixture`] to set
    /// up, described in terms of the state each record should be created in.
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct ReservationSpec {
        /// The `vmm` record (if any) to create alongside the reservation.
        vmm: VmmRecord,
        /// The state to create the `sled_resource_vmm` reservation in.
        reservation: SledResourceVmmState,
    }

    impl ReservationSpec {
        /// Whether a reservation built from this spec is "abandoned" per
        /// [`DataStore::sled_reservation_list_abandoned`], and thus must be
        /// reaped by [`AbandonedVmmReaper::reap_sled_reservations`].
        fn is_abandoned(self) -> bool {
            self.reservation == SledResourceVmmState::Tombstoned
                && self.vmm.no_live_vmm()
        }
    }

    /// A [`ReservationSpec`] paired with the ID of the records created for it,
    /// so that assertions can refer to both the expected state and the concrete
    /// records in the database.
    struct FixtureReservation {
        id: PropolisUuid,
        spec: ReservationSpec,
    }

    /// A set of `vmm`/`sled_resource_vmm` records created directly in the
    /// database from a list of [`ReservationSpec`]s.
    ///
    /// The records are inserted directly rather than through
    /// `sled_reservation_create`, because that path only ever produces `Active`
    /// or `Target` reservations and requires a real sled with free resources;
    /// these tests need reservations in arbitrary states (in particular
    /// `Tombstoned`) paired with arbitrary VMM records.
    struct TestFixture {
        reservations: Vec<FixtureReservation>,
    }

    impl TestFixture {
        async fn setup(
            datastore: &DataStore,
            opctx: &OpContext,
            specs: impl IntoIterator<Item = ReservationSpec>,
        ) -> Self {
            let mut reservations = Vec::new();
            for spec in specs {
                let id = Self::create_records(datastore, opctx, spec).await;
                reservations.push(FixtureReservation { id, spec });
            }
            Self { reservations }
        }

        /// Inserts the `sled_resource_vmm` record (and, if the spec calls for
        /// it, the accompanying `vmm` record) described by `spec`, returning
        /// the ID shared by both records.
        async fn create_records(
            datastore: &DataStore,
            opctx: &OpContext,
            spec: ReservationSpec,
        ) -> PropolisUuid {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use nexus_db_schema::schema::sled_resource_vmm::dsl as resource_dsl;

            let id = PropolisUuid::new_v4();
            // Each record gets a distinct instance ID so that non-`Tombstoned`
            // reservations don't collide on the
            // `single_vmm_reservation_per_state` unique index.
            let instance_id = InstanceUuid::new_v4();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::insert_into(resource_dsl::sled_resource_vmm)
                .values(SledResourceVmm::new(
                    id,
                    instance_id,
                    SledUuid::new_v4(),
                    Resources::new(
                        1,
                        // Just require the bare non-zero amount of RAM.
                        ByteCount::try_from(1024).unwrap(),
                        ByteCount::try_from(1024).unwrap(),
                    ),
                    spec.reservation,
                ))
                .execute_async(&*conn)
                .await
                .expect("sled resource reservation should be inserted");

            let (state, time_deleted) = match spec.vmm {
                VmmRecord::None => return id,
                VmmRecord::Present(state) => (state, None),
                VmmRecord::Deleted(state) => (state, Some(Utc::now())),
            };

            // The `vmm` table's `failure_reason_iff_failed` constraint requires
            // a failure reason if and only if the VMM is `Failed`.
            let failure_reason = (state == VmmState::Failed)
                .then_some(VmmFailureReason::Prehistoric);

            datastore
                .vmm_insert(
                    opctx,
                    dbg!(Vmm {
                        id: id.into_untyped_uuid(),
                        time_created: Utc::now(),
                        time_deleted,
                        instance_id: instance_id.into_untyped_uuid(),
                        sled_id: SledUuid::new_v4().into(),
                        propolis_ip: "::1".parse().unwrap(),
                        propolis_port: 12345.into(),
                        cpu_platform: VmmCpuPlatform::SledDefault,
                        state,
                        time_state_updated: Utc::now(),
                        generation: Generation::new(),
                        failure_reason,
                    }),
                )
                .await
                .expect("vmm record should be inserted");

            id
        }

        /// Returns the ID of the single reservation in a one-spec fixture,
        /// panicking if the fixture holds any other number of reservations.
        fn only_id(&self) -> PropolisUuid {
            match self.reservations.as_slice() {
                [only] => only.id,
                other => panic!(
                    "expected exactly one reservation in the fixture, \
                     but found {}",
                    other.len(),
                ),
            }
        }

        /// Fetches the current `sled_resource_vmm` record with the given ID, if
        /// any still exists.
        async fn fetch_reservation(
            datastore: &DataStore,
            id: PropolisUuid,
        ) -> Option<SledResourceVmm> {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::{
                ExpressionMethods, OptionalExtension, QueryDsl,
                SelectableHelper,
            };
            use nexus_db_schema::schema::sled_resource_vmm::dsl;

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            dsl::sled_resource_vmm
                .filter(dsl::id.eq(id.into_untyped_uuid()))
                .select(SledResourceVmm::as_select())
                .first_async::<SledResourceVmm>(&*conn)
                .await
                .optional()
                .expect("sled resource query should succeed")
        }

        /// Fetches the current non-deleted `vmm` record with the given ID, if
        /// any still exists.
        async fn fetch_active_vmm(
            datastore: &DataStore,
            id: PropolisUuid,
        ) -> Option<Vmm> {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::{
                ExpressionMethods, OptionalExtension, QueryDsl,
                SelectableHelper,
            };
            use nexus_db_schema::schema::vmm::dsl;

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            dsl::vmm
                .filter(dsl::id.eq(id.into_untyped_uuid()))
                .filter(dsl::time_deleted.is_null())
                .select(Vmm::as_select())
                .first_async::<Vmm>(&*conn)
                .await
                .optional()
                .expect("VMM query should succeed")
        }

        /// Asserts that every VMM and reservation tracked by this fixture has
        /// been fully reaped: no live `vmm` record and no `sled_resource_vmm`
        /// record remain for any tracked ID.
        ///
        /// This is the appropriate assertion for the VMM-reaping path, which
        /// deletes a VMM's reservation regardless of its state.
        async fn assert_reaped(&self, datastore: &DataStore) {
            for FixtureReservation { id, spec } in &self.reservations {
                let id = *id;
                assert!(
                    dbg!(Self::fetch_active_vmm(datastore, id).await).is_none(),
                    "VMM {id} ({:?}) should have been deleted",
                    spec.vmm,
                );
                assert!(
                    dbg!(Self::fetch_reservation(datastore, id).await)
                        .is_none(),
                    "sled resource reservation {id} ({:?}) should have been \
                     deleted",
                    spec.reservation,
                );
            }
        }

        /// Asserts that reservation reaping deleted exactly the *abandoned*
        /// reservations tracked by this fixture: abandoned reservations no
        /// longer exist, while every other reservation still exists in its
        /// original state.
        ///
        /// Reservation reaping must never touch `vmm` records, so this also
        /// asserts that each tracked VMM record is left exactly as it was
        /// created.
        async fn assert_only_abandoned_reservations_reaped(
            &self,
            datastore: &DataStore,
        ) {
            for FixtureReservation { id, spec } in &self.reservations {
                let id = *id;
                let reservation =
                    dbg!(Self::fetch_reservation(datastore, id).await);
                if spec.is_abandoned() {
                    assert!(
                        reservation.is_none(),
                        "abandoned reservation {id} (vmm: {:?}, \
                         reservation: {:?}) should have been reaped",
                        spec.vmm,
                        spec.reservation,
                    );
                } else {
                    let reservation = reservation.unwrap_or_else(|| {
                        panic!(
                            "reservation {id} (vmm: {:?}, reservation: {:?}) \
                             is not abandoned and should NOT have been reaped",
                            spec.vmm, spec.reservation,
                        )
                    });
                    assert_eq!(
                        reservation.state, spec.reservation,
                        "reservation {id} should retain its original state",
                    );
                }

                // In all cases, the `vmm` record must be left untouched.
                let vmm = dbg!(Self::fetch_active_vmm(datastore, id).await);
                match spec.vmm {
                    VmmRecord::None | VmmRecord::Deleted(_) => assert!(
                        vmm.is_none(),
                        "no live VMM record should exist for {id} ({:?})",
                        spec.vmm,
                    ),
                    VmmRecord::Present(state) => {
                        let vmm = vmm.unwrap_or_else(|| {
                            panic!("VMM record for {id} should still exist")
                        });
                        assert_eq!(
                            vmm.state, state,
                            "VMM {id} state should be unchanged",
                        );
                    }
                }
            }
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
        // A destroyed VMM (the target of a migration that failed) whose
        // `Target` reservation was never cleaned up. The reservation is
        // deleted by the VMM-reaping path, not by reservation reaping, since a
        // `Target` reservation is not "abandoned".
        let fixture = TestFixture::setup(
            datastore,
            &opctx,
            [ReservationSpec {
                vmm: VmmRecord::Present(VmmState::Destroyed),
                reservation: SledResourceVmmState::Target,
            }],
        )
        .await;

        let mut task = AbandonedVmmReaper::new(datastore.clone());

        let status = dbg!(task.reap_all(&opctx, SQL_BATCH_SIZE).await);

        assert_eq!(status.vmms.found, 1);
        assert_eq!(status.vmms.deleted, 1);
        assert_eq!(status.vmms.sled_reservations_deleted, 1);
        assert_eq!(status.vmms.already_deleted, 0);
        assert_eq!(status.vmms.errors, Vec::<String>::new());
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
        let fixture = TestFixture::setup(
            datastore,
            &opctx,
            [ReservationSpec {
                vmm: VmmRecord::Present(VmmState::Destroyed),
                reservation: SledResourceVmmState::Target,
            }],
        )
        .await;

        // For this test, we separate the database query run by the background
        // task to list abandoned VMMs from the actual cleanup of those VMMs, in
        // order to simulate a condition where the VMM record was deleted
        // between when the listing query was run and when the bg task attempted
        // to delete the VMM record.
        let paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let p = paginator.next().unwrap();
        let abandoned_vmms = datastore
            .vmm_list_abandoned(&opctx, &p.current_pagparams())
            .await
            .expect("must list abandoned vmms");

        assert!(!abandoned_vmms.is_empty());

        datastore
            .vmm_mark_deleted(&opctx, &fixture.only_id())
            .await
            .expect("simulate another nexus marking the VMM deleted");

        let mut status = status::AbandonedVmms::default();
        let mut task = AbandonedVmmReaper::new(datastore.clone());
        task.reap_vmm_batch(&mut status, &opctx, &abandoned_vmms).await;
        dbg!(&status);

        assert_eq!(status.found, 1);
        assert_eq!(status.deleted, 0);
        assert_eq!(status.sled_reservations_deleted, 1);
        assert_eq!(status.already_deleted, 1);
        assert_eq!(status.errors, Vec::<String>::new());

        fixture.assert_reaped(datastore).await
    }

    #[nexus_test(server = crate::Server)]
    async fn sled_resource_vmm_already_deleted(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let fixture = TestFixture::setup(
            datastore,
            &opctx,
            [ReservationSpec {
                vmm: VmmRecord::Present(VmmState::Destroyed),
                reservation: SledResourceVmmState::Target,
            }],
        )
        .await;

        // For this test, we separate the database query run by the background
        // task to list abandoned VMMs from the actual cleanup of those VMMs, in
        // order to simulate a condition where the sled reservation record was
        // deleted between when the listing query was run and when the bg task
        // attempted to delete the sled reservation..
        let paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let p = paginator.next().unwrap();
        let abandoned_vmms = datastore
            .vmm_list_abandoned(&opctx, &p.current_pagparams())
            .await
            .expect("must list abandoned vmms");

        assert!(!abandoned_vmms.is_empty());

        datastore
            .sled_reservation_delete(&opctx, fixture.only_id())
            .await
            .expect(
                "simulate another nexus marking the sled reservation deleted",
            );

        let mut status = status::AbandonedVmms::default();
        let mut task = AbandonedVmmReaper::new(datastore.clone());
        task.reap_vmm_batch(&mut status, &opctx, &abandoned_vmms).await;
        dbg!(&status);

        assert_eq!(status.found, 1);
        assert_eq!(status.deleted, 1);
        assert_eq!(status.sled_reservations_deleted, 1);
        assert_eq!(status.already_deleted, 0);
        assert_eq!(status.errors, Vec::<String>::new());

        fixture.assert_reaped(datastore).await
    }

    /// Exercises every branch of the `sled_reservation_list_abandoned` query:
    /// each reservation that the query considers abandoned must be reaped, and
    /// every reservation it does *not* consider abandoned must be left alone,
    /// including reservations that still have a live VMM and reservations that
    /// are not `Tombstoned`.
    #[nexus_test(server = crate::Server)]
    async fn test_only_abandoned_reservations_are_reaped(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        use SledResourceVmmState::{Active, Target, Tombstoned};

        let fixture = TestFixture::setup(
            datastore,
            &opctx,
            [
                // --- Abandoned: `Tombstoned`, and one of the query's three
                // VMM conditions holds. ---
                //
                // The VMM is in a `DESTROYABLE` state...
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Destroyed),
                    reservation: Tombstoned,
                },
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Failed),
                    reservation: Tombstoned,
                },
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::SagaUnwound),
                    reservation: Tombstoned,
                },
                // ...or the VMM has been soft-deleted (regardless of its
                // state, which is why we also cover a soft-deleted `Running`
                // VMM)...
                ReservationSpec {
                    vmm: VmmRecord::Deleted(VmmState::Destroyed),
                    reservation: Tombstoned,
                },
                ReservationSpec {
                    vmm: VmmRecord::Deleted(VmmState::Running),
                    reservation: Tombstoned,
                },
                // ...or the VMM record is gone entirely.
                ReservationSpec {
                    vmm: VmmRecord::None,
                    reservation: Tombstoned,
                },
                // --- Not abandoned, and so must survive reaping. ---
                //
                // `Tombstoned`, but the VMM is still alive: not abandoned,
                // because none of the query's VMM conditions hold.
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Running),
                    reservation: Tombstoned,
                },
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Starting),
                    reservation: Tombstoned,
                },
                // `Active` reservations are never abandoned, even when the VMM
                // condition would otherwise match (destroyed, or absent).
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Running),
                    reservation: Active,
                },
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Destroyed),
                    reservation: Active,
                },
                ReservationSpec { vmm: VmmRecord::None, reservation: Active },
                // `Target` reservations are likewise never abandoned. In
                // particular, a failed migration target leaves a `Target`
                // reservation behind a `Destroyed` VMM; this is *not* reaped by
                // reservation reaping, and is instead cleaned up via the VMM
                // record (see `delete_sled_reservation`).
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Running),
                    reservation: Target,
                },
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Destroyed),
                    reservation: Target,
                },
            ],
        )
        .await;

        let expected_reaped = fixture
            .reservations
            .iter()
            .filter(|res| res.spec.is_abandoned())
            .count();

        let mut task = AbandonedVmmReaper::new(datastore.clone());
        let status =
            dbg!(task.reap_sled_reservations(&opctx, SQL_BATCH_SIZE).await);

        assert_eq!(status.errors, Vec::<String>::new());
        assert_eq!(status.found, expected_reaped);
        assert_eq!(status.deleted, expected_reaped);

        fixture.assert_only_abandoned_reservations_reaped(datastore).await;
    }

    /// Verifies that abandoned reservations are reaped by
    /// [`AbandonedVmmReaper::reap_sled_reservations`] *without* relying on the
    /// `delete_sled_reservation` call that
    /// [`AbandonedVmmReaper::reap_vmm_batch`] performs while deleting VMM
    /// records.
    ///
    /// This documents that the reservation-deletion step in the VMM-reaping
    /// path is redundant for *abandoned* (i.e. `Tombstoned`) reservations: were
    /// that step removed, the reservation-reaping pass would still delete these
    /// records, because a `Tombstoned` reservation is abandoned whether its VMM
    /// is still present in a destroyable state (condition 3) or has already
    /// been soft-deleted (condition 1).
    #[nexus_test(server = crate::Server)]
    async fn test_reservations_reaped_without_vmm_reaper_step(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Both reservations are abandoned, and model what the VMM-reaping
        // path's `delete_sled_reservation` step would otherwise clean up:
        let fixture = TestFixture::setup(
            datastore,
            &opctx,
            [
                // A successful migration leaves the source VMM `Destroyed`
                // with a `Tombstoned` reservation. If the VMM-reaping step were
                // removed, this reservation would remain until reservation
                // reaping runs and catches it via condition 3.
                ReservationSpec {
                    vmm: VmmRecord::Present(VmmState::Destroyed),
                    reservation: SledResourceVmmState::Tombstoned,
                },
                // If the VMM record has already been soft-deleted (e.g. by the
                // instance-update saga) without its reservation being cleaned
                // up, the reservation is still abandoned via condition 1.
                ReservationSpec {
                    vmm: VmmRecord::Deleted(VmmState::Destroyed),
                    reservation: SledResourceVmmState::Tombstoned,
                },
            ],
        )
        .await;

        // Deliberately run *only* the reservation-reaping pass, simulating the
        // removal of `reap_vmm_batch`'s `delete_sled_reservation` call.
        let mut task = AbandonedVmmReaper::new(datastore.clone());
        let status =
            dbg!(task.reap_sled_reservations(&opctx, SQL_BATCH_SIZE).await);

        assert_eq!(status.errors, Vec::<String>::new());
        assert_eq!(status.found, 2);
        assert_eq!(status.deleted, 2);

        fixture.assert_only_abandoned_reservations_reaped(datastore).await;
    }
}
