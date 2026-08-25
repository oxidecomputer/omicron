// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::upsert::excluded;
use iddqd::IdOrdMap;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::DbSledBpAvailability;
use nexus_db_model::RendezvousSledBpAvailability;
use nexus_db_model::RendezvousSledBpAvailabilityDecommission;
use nexus_db_model::RendezvousSledBpAvailabilityUpdate;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;

impl DataStore {
    /// List one page of sleds in the rendezvous table.
    async fn rendezvous_sled_bp_availability_list_all_page(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SledUuid>,
    ) -> ListResultVec<RendezvousSledBpAvailability> {
        use nexus_db_schema::schema::rendezvous_sled_bp_availability::dsl;

        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        paginated(
            dsl::rendezvous_sled_bp_availability,
            dsl::sled_id,
            &pagparams.map_name(|id| id.as_untyped_uuid()),
        )
        .select(RendezvousSledBpAvailability::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all sleds in the rendezvous table, making as many queries as needed
    /// to get them all, keyed by sled ID.
    ///
    /// This returns rows in every state, including `decommissioned`. Callers
    /// that only care about provisionable sleds must filter on
    /// [`RendezvousSledBpAvailability::bp_availability`].
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn rendezvous_sled_bp_availability_list_all_batched(
        &self,
        opctx: &OpContext,
    ) -> Result<IdOrdMap<RendezvousSledBpAvailability>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;

        let mut all_sleds = IdOrdMap::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            // `Paginator::found_batch` needs an ordered slice to compute the
            // next page marker, so the per-page helper yields a `Vec`; we fold
            // each page into the keyed map as we go.
            let batch = self
                .rendezvous_sled_bp_availability_list_all_page(
                    opctx,
                    &p.current_pagparams(),
                )
                .await?;
            paginator = p
                .found_batch(&batch, &|s: &RendezvousSledBpAvailability| {
                    s.sled_id()
                });
            all_sleds.extend(batch);
        }

        Ok(all_sleds)
    }

    /// Record a sled's availability in the `rendezvous_sled_bp_availability`
    /// table, guarded by its `update_disposition` generation.
    ///
    /// The write applies only if both of the following conditions are met:
    ///
    /// 1. The existing row is not `decommissioned` (a terminal state).
    /// 2. The incoming `update_disposition_generation` is greater than the
    ///    stored one.
    ///
    /// Together, these prevent duelling Nexuses from trampling over each
    /// other's state.
    ///
    /// Returns `true` if a row was written, and `false` if the write was
    /// rejected (stale generation, or the sled is already decommissioned).
    pub async fn rendezvous_sled_bp_availability_upsert(
        &self,
        opctx: &OpContext,
        update: RendezvousSledBpAvailabilityUpdate,
    ) -> Result<bool, Error> {
        // `FilterDsl` brings `.filter()` (the `WHERE` on the `ON CONFLICT DO
        // UPDATE` below) into scope; the prelude's `QueryDsl` only offers it for
        // table-like queries, not upsert statements.
        use diesel::query_dsl::methods::FilterDsl;
        use nexus_db_schema::schema::rendezvous_sled_bp_availability::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // Compared against the stored generation by the staleness guard.
        let incoming_generation = nexus_db_model::Generation::from(
            update.update_disposition_generation(),
        );
        let sled = update.into_insertable();

        diesel::insert_into(dsl::rendezvous_sled_bp_availability)
            .values(sled)
            .on_conflict(dsl::sled_id)
            .do_update()
            .set((
                dsl::bp_availability.eq(excluded(dsl::bp_availability)),
                dsl::update_disposition_generation
                    .eq(excluded(dsl::update_disposition_generation)),
                dsl::blueprint_id.eq(excluded(dsl::blueprint_id)),
                dsl::time_modified.eq(excluded(dsl::time_modified)),
            ))
            // Never resurrect a decommissioned sled.
            .filter(
                dsl::bp_availability.ne(DbSledBpAvailability::Decommissioned),
            )
            // Overwrite only if our generation is newer than what's stored.
            .filter(dsl::update_disposition_generation.lt(incoming_generation))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            // The conflict target is the primary key, so at most one row is
            // inserted or updated.
            .map(|rows_modified| match rows_modified {
                0 => false,
                1 => true,
                n => unreachable!(
                    "upsert by primary key sled_id modified {n} rows"
                ),
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Record that a sled was decommissioned in the
    /// `rendezvous_sled_bp_availability` table.
    ///
    /// This is a terminal state.
    ///
    /// Returns `true` if the sled transitioned to decommissioned, and `false`
    /// if it was already decommissioned.
    pub async fn rendezvous_sled_bp_availability_decommission(
        &self,
        opctx: &OpContext,
        decommission: RendezvousSledBpAvailabilityDecommission,
    ) -> Result<bool, Error> {
        // `FilterDsl` brings `.filter()` (the `WHERE` on the `ON CONFLICT DO
        // UPDATE` below) into scope; the prelude's `QueryDsl` only offers it for
        // table-like queries, not upsert statements.
        use diesel::query_dsl::methods::FilterDsl;
        use nexus_db_schema::schema::rendezvous_sled_bp_availability::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let tombstone = decommission.into_insertable();

        diesel::insert_into(dsl::rendezvous_sled_bp_availability)
            .values(tombstone)
            .on_conflict(dsl::sled_id)
            .do_update()
            .set((
                dsl::bp_availability.eq(excluded(dsl::bp_availability)),
                // Set the generation to NULL. This is akin to setting a
                // generation of infinity -- decommissioned is a terminal state,
                // and both this upsert and the active one above filter out
                // already-decommissioned rows.
                dsl::update_disposition_generation
                    .eq(excluded(dsl::update_disposition_generation)),
                dsl::blueprint_id.eq(excluded(dsl::blueprint_id)),
                dsl::time_modified.eq(excluded(dsl::time_modified)),
            ))
            // An already-decommissioned row is left alone, making a repeated
            // decommission a no-op.
            .filter(
                dsl::bp_availability.ne(DbSledBpAvailability::Decommissioned),
            )
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            // The conflict target is the primary key, so at most one row is
            // inserted or updated.
            .map(|rows_modified| match rows_modified {
                0 => false,
                1 => true,
                n => unreachable!(
                    "upsert by primary key sled_id modified {n} rows"
                ),
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::ActiveSledBpAvailability;
    use nexus_db_model::SledBpAvailabilityState;
    use omicron_generation_kinds::Generation;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;

    // Convenience for building an availability update at a given
    // availability/generation.
    fn row(
        sled_id: SledUuid,
        availability: ActiveSledBpAvailability,
        generation: u32,
        blueprint_id: BlueprintUuid,
    ) -> RendezvousSledBpAvailabilityUpdate {
        RendezvousSledBpAvailabilityUpdate::new(
            sled_id,
            availability,
            Generation::from(generation),
            blueprint_id,
        )
    }

    // Fetch the single row for a sled, if present.
    async fn get(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: SledUuid,
    ) -> Option<RendezvousSledBpAvailability> {
        datastore
            .rendezvous_sled_bp_availability_list_all_batched(opctx)
            .await
            .unwrap()
            .get(&sled_id)
            .cloned()
    }

    #[tokio::test]
    async fn upsert_rejects_stale_generation() {
        let logctx = dev::test_setup_log("upsert_rejects_stale_generation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_id = SledUuid::new_v4();
        let bp1 = BlueprintUuid::new_v4();
        let bp2 = BlueprintUuid::new_v4();

        // Initial insert at generation 1, available.
        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 1, bp1),
            )
            .await
            .expect("query succeeded");
        assert!(wrote, "first insert should write");
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation: Generation::from(1),
            }
        );

        // A newer generation (2) flipping the sled to unavailable should win.
        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Unavailable, 2, bp2),
            )
            .await
            .expect("query succeeded");
        assert!(wrote, "newer generation should write");
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation: Generation::from(2),
            }
        );
        assert_eq!(got.blueprint_id(), bp2);

        // A stale write at the same generation (2) trying to flip back to
        // available must be rejected.
        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 2, bp1),
            )
            .await
            .expect("query succeeded");
        assert!(!wrote, "equal-generation write should be rejected as stale");
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation: Generation::from(2),
            },
            "row must not have rolled back"
        );

        // A stale write at an older generation (1) must also be rejected.
        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 1, bp1),
            )
            .await
            .expect("query succeeded");
        assert!(!wrote, "older-generation write should be rejected as stale");
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation: Generation::from(2),
            },
            "row must not have rolled back"
        );

        // A newer generation (3) flipping back to available wins.
        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 3, bp2),
            )
            .await
            .expect("query succeeded");
        assert!(wrote, "newest generation should write");
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation: Generation::from(3),
            }
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn decommission_is_terminal() {
        let logctx = dev::test_setup_log("decommission_is_terminal");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_id = SledUuid::new_v4();
        let bp = BlueprintUuid::new_v4();

        // Insert the sled (available, generation 1) and confirm we see it.
        datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 1, bp),
            )
            .await
            .expect("query succeeded");
        assert_eq!(
            get(opctx, datastore, sled_id)
                .await
                .unwrap()
                .state()
                .expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation: Generation::from(1u32),
            },
        );

        // Decommissioning it should change the sled's state to
        // `decommissioned`.
        let decommissioned = datastore
            .rendezvous_sled_bp_availability_decommission(
                opctx,
                RendezvousSledBpAvailabilityDecommission::new(sled_id, bp),
            )
            .await
            .expect("query succeeded");
        assert!(decommissioned);
        let got = get(opctx, datastore, sled_id).await.expect("row remains");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Decommissioned
        );

        // A second decommission is a no-op.
        let decommissioned = datastore
            .rendezvous_sled_bp_availability_decommission(
                opctx,
                RendezvousSledBpAvailabilityDecommission::new(sled_id, bp),
            )
            .await
            .expect("query succeeded");
        assert!(!decommissioned);

        // A stale Nexus must not be able to resurrect a decommissioned sled,
        // even at a newer generation.
        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 5, bp),
            )
            .await
            .expect("query succeeded");
        assert!(!wrote, "decommissioned sled must not be resurrected");
        assert_eq!(
            get(opctx, datastore, sled_id)
                .await
                .unwrap()
                .state()
                .expect("reassembled row state"),
            SledBpAvailabilityState::Decommissioned,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn decommission_without_row_inserts_tombstone() {
        let logctx =
            dev::test_setup_log("decommission_without_row_inserts_tombstone");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_id = SledUuid::new_v4();
        let bp = BlueprintUuid::new_v4();
        let bp_stale = BlueprintUuid::new_v4();

        let decommissioned = datastore
            .rendezvous_sled_bp_availability_decommission(
                opctx,
                RendezvousSledBpAvailabilityDecommission::new(sled_id, bp),
            )
            .await
            .expect("query succeeded");
        assert!(decommissioned, "fresh tombstone insert should report true");
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Decommissioned,
        );
        assert_eq!(got.blueprint_id(), bp);

        let wrote = datastore
            .rendezvous_sled_bp_availability_upsert(
                opctx,
                row(sled_id, ActiveSledBpAvailability::Available, 5, bp_stale),
            )
            .await
            .expect("query succeeded");
        assert!(!wrote, "decommissioned sled must not be resurrected");
        assert_eq!(
            get(opctx, datastore, sled_id).await.unwrap().blueprint_id(),
            bp,
            "stale write must not touch the tombstone's blueprint_id",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn check_constraint_rejects_inconsistent_columns() {
        let logctx = dev::test_setup_log(
            "check_constraint_rejects_inconsistent_columns",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        for (case, sql) in [
            (
                "active availability with NULL generation",
                "INSERT INTO omicron.public.rendezvous_sled_bp_availability \
                 (sled_id, bp_availability, update_disposition_generation, \
                  blueprint_id, time_created, time_modified) \
                 VALUES (gen_random_uuid(), 'available', NULL, \
                  gen_random_uuid(), now(), now())",
            ),
            (
                "decommissioned with a generation",
                "INSERT INTO omicron.public.rendezvous_sled_bp_availability \
                 (sled_id, bp_availability, update_disposition_generation, \
                  blueprint_id, time_created, time_modified) \
                 VALUES (gen_random_uuid(), 'decommissioned', 3, \
                  gen_random_uuid(), now(), now())",
            ),
        ] {
            let err = diesel::sql_query(sql)
                .execute_async(&*conn)
                .await
                .expect_err("insert of an inconsistent row was rejected");
            match err {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::CheckViolation,
                    info,
                ) => {
                    assert_eq!(
                        info.constraint_name(),
                        Some("decommissioned_has_no_generation"),
                        "case {case:?} must be rejected by the \
                         decommissioned_has_no_generation constraint",
                    );
                }
                other => panic!(
                    "case {case:?}: expected a check-constraint violation, \
                     got {other:?}"
                ),
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
