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
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::ActiveSledBpAvailability;
use nexus_db_model::DbSledBpAvailability;
use nexus_db_model::RendezvousSledBpAvailability;
use nexus_db_model::RendezvousSledBpAvailabilityDecommission;
use nexus_db_model::RendezvousSledBpAvailabilityUpdate;
use nexus_db_model::SledBlueprintAvailabilityInput;
use nexus_db_model::SledBpAvailabilityState;
use nexus_types::internal_api::background::SledBlueprintAvailabilityRendezvousStats;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_generation_kinds::UpdateDispositionGeneration;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::debug;
use slog::info;

/// The result of a generation-guarded availability upsert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SledBpAvailabilityUpsertOutcome {
    /// The row was inserted or updated.
    Written,
    /// The guard rejected the write.
    ///
    /// This can happen because:
    ///
    /// - the stored row is at an equal or newer `update_disposition` generation;
    /// - or, the sled is decommissioned.
    Rejected,
}

/// The result of recording a sled as decommissioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SledBpAvailabilityDecommissionOutcome {
    /// The sled transitioned to decommissioned (fresh tombstone or an active
    /// row overwritten).
    Decommissioned,
    /// The sled was already decommissioned; the row was left alone.
    AlreadyDecommissioned,
}

/// The result of writing one blueprint sled's availability to the
/// `rendezvous_sled_bp_availability` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SledBpAvailabilityWrite {
    pub sled_id: SledUuid,
    pub outcome: SledBpAvailabilityWriteOutcome,
}

impl IdOrdItem for SledBpAvailabilityWrite {
    type Key<'a> = SledUuid;

    fn key(&self) -> Self::Key<'_> {
        self.sled_id
    }

    id_upcast!();
}

impl SledBpAvailabilityWrite {
    /// Log this write to the given logger, and increment the corresponding
    /// scount in `stats`.
    pub fn log_to_and_count(
        &self,
        log: &slog::Logger,
        stats: &mut SledBlueprintAvailabilityRendezvousStats,
    ) {
        let sled_id = self.sled_id;
        match self.outcome {
            SledBpAvailabilityWriteOutcome::Active {
                availability,
                update_disposition_generation,
                outcome: SledBpAvailabilityUpsertOutcome::Written,
            } => match availability {
                ActiveSledBpAvailability::Available => {
                    stats.num_marked_available += 1;
                    info!(
                        log,
                        "marked sled available for provisioning";
                        "sled_id" => %sled_id,
                        "update_disposition_generation" =>
                            %update_disposition_generation,
                    );
                }
                ActiveSledBpAvailability::Unavailable => {
                    stats.num_marked_unavailable += 1;
                    info!(
                        log,
                        "marked sled unavailable for provisioning";
                        "sled_id" => %sled_id,
                        "update_disposition_generation" =>
                            %update_disposition_generation,
                    );
                }
            },
            SledBpAvailabilityWriteOutcome::Active {
                availability,
                update_disposition_generation,
                outcome: SledBpAvailabilityUpsertOutcome::Rejected,
            } => {
                // We decided to perform a write, but a duelling Nexus
                // recorded an equal-or-newer generation (or decommissioned
                // the sled) first, so the row already reflects
                // current-or-newer state. From our perspective, this is an
                // unchanged sled.
                stats.num_unchanged += 1;
                debug!(
                    log,
                    "left sled availability row as-is: stored row is at an \
                     equal or newer generation, or the sled is decommissioned";
                    "sled_id" => %sled_id,
                    "availability" =>
                        DbSledBpAvailability::from(availability).label(),
                    "update_disposition_generation" =>
                        %update_disposition_generation,
                );
            }
            SledBpAvailabilityWriteOutcome::Decommission(
                SledBpAvailabilityDecommissionOutcome::Decommissioned,
            ) => {
                stats.num_decommissioned += 1;
                info!(
                    log,
                    "decommissioned sled in rendezvous table";
                    "sled_id" => %sled_id,
                );
            }
            SledBpAvailabilityWriteOutcome::Decommission(
                SledBpAvailabilityDecommissionOutcome::AlreadyDecommissioned,
            ) => {
                // Another Nexus decommissioned it first.
                stats.num_already_decommissioned += 1;
                debug!(
                    log,
                    "left sled availability row as-is: already decommissioned";
                    "sled_id" => %sled_id,
                );
            }
        }
    }
}

/// The result of writing one blueprint sled's availability to the
/// `rendezvous_sled_bp_availability` table.
///
/// Part of [`SledBpAvailabilityWrite`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SledBpAvailabilityWriteOutcome {
    /// The sled is active in the blueprint.
    Active {
        /// The availability written to the database.
        availability: ActiveSledBpAvailability,

        /// The generation of the update disposition that was written.
        update_disposition_generation: UpdateDispositionGeneration,

        /// Whether the upsert operation was successful.
        outcome: SledBpAvailabilityUpsertOutcome,
    },

    /// The sled is decommissioned in the blueprint.
    Decommission(SledBpAvailabilityDecommissionOutcome),
}

/// An error produced while writing a batch of sleds to
/// `rendezvous_sled_bp_availability`.
#[derive(Debug, thiserror::Error)]
pub enum SledBpAvailabilityWriteError {
    /// The write was not started due to an error (e.g. authz/connection
    /// failed).
    #[error("failed to start writing sled availability")]
    NotStarted(#[source] Error),

    /// The write failed in the middle of the operation.
    #[error(
        "failed to write availability for sled {failed_sled_id} after \
         {} write(s) completed ({num_not_attempted} not attempted)",
        completed.len()
    )]
    Failed {
        /// Information about the sleds that were successfully written before
        /// the failure.
        completed: IdOrdMap<SledBpAvailabilityWrite>,
        /// The UUID of the sled that failed to be written.
        failed_sled_id: SledUuid,
        /// The number of sleds for which writes were not attempted, not
        /// including the failed sled.
        num_not_attempted: usize,
        /// The underlying error.
        #[source]
        error: Error,
    },
}

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

    /// Write the availability of each sled in `sleds` to the database.
    pub async fn rendezvous_sled_bp_availability_write(
        &self,
        opctx: &OpContext,
        blueprint_id: BlueprintUuid,
        sleds: IdOrdMap<SledBlueprintAvailabilityInput>,
    ) -> Result<IdOrdMap<SledBpAvailabilityWrite>, SledBpAvailabilityWriteError>
    {
        opctx
            .authorize(authz::Action::Modify, &authz::FLEET)
            .await
            .map_err(SledBpAvailabilityWriteError::NotStarted)?;
        let conn = self
            .pool_connection_authorized(opctx)
            .await
            .map_err(SledBpAvailabilityWriteError::NotStarted)?;
        Self::rendezvous_sled_bp_availability_write_on_connection(
            &conn,
            blueprint_id,
            sleds,
        )
        .await
    }

    /// on_connection variant of `rendezvous_sled_bp_availability_write`.
    ///
    /// Writes are issued one sled at a time, and are stopped at the first
    /// error. In case of a failure after a partial write, the returned
    /// [`SledBpAvailabilityWriteError`] contains information about the sleds
    /// that succeeded.
    ///
    /// The caller is responsible for authorizing the operation.
    pub(crate) async fn rendezvous_sled_bp_availability_write_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        blueprint_id: BlueprintUuid,
        sleds: IdOrdMap<SledBlueprintAvailabilityInput>,
    ) -> Result<IdOrdMap<SledBpAvailabilityWrite>, SledBpAvailabilityWriteError>
    {
        let num_sleds = sleds.len();
        let mut writes = IdOrdMap::new();
        for SledBlueprintAvailabilityInput { sled_id, state } in sleds {
            let result = match state {
                SledBpAvailabilityState::Active {
                    availability,
                    update_disposition_generation,
                } => {
                    Self::rendezvous_sled_bp_availability_upsert_on_connection(
                        conn,
                        RendezvousSledBpAvailabilityUpdate::new(
                            sled_id,
                            availability,
                            update_disposition_generation,
                            blueprint_id,
                        ),
                    )
                    .await
                    .map(|outcome| SledBpAvailabilityWriteOutcome::Active {
                        availability,
                        update_disposition_generation,
                        outcome,
                    })
                    .map_err(|e| {
                        e.internal_context(format!(
                            "failed to upsert availability for sled {sled_id}"
                        ))
                    })
                }
                SledBpAvailabilityState::Decommissioned => {
                    Self::rendezvous_sled_bp_availability_decommission_on_connection(
                        conn,
                        RendezvousSledBpAvailabilityDecommission::new(
                            sled_id,
                            blueprint_id,
                        ),
                    )
                    .await
                    .map(SledBpAvailabilityWriteOutcome::Decommission)
                    .map_err(|e| {
                        e.internal_context(format!(
                            "failed to decommission sled {sled_id}"
                        ))
                    })
                }
            };
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(error) => {
                    let num_not_attempted = num_sleds - writes.len() - 1;
                    return Err(SledBpAvailabilityWriteError::Failed {
                        completed: writes,
                        failed_sled_id: sled_id,
                        num_not_attempted,
                        error,
                    });
                }
            };
            writes
                .insert_unique(SledBpAvailabilityWrite { sled_id, outcome })
                .expect(
                    "input map is keyed by sled ID, so each sled appears once",
                );
        }
        Ok(writes)
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
    /// The caller is responsible for authorizing the operation.
    pub(crate) async fn rendezvous_sled_bp_availability_upsert_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        update: RendezvousSledBpAvailabilityUpdate,
    ) -> Result<SledBpAvailabilityUpsertOutcome, Error> {
        // `FilterDsl` brings `.filter()` (the `WHERE` on the `ON CONFLICT DO
        // UPDATE` below) into scope; the prelude's `QueryDsl` only offers it for
        // table-like queries, not upsert statements.
        use diesel::query_dsl::methods::FilterDsl;
        use nexus_db_schema::schema::rendezvous_sled_bp_availability::dsl;

        // Compared against the stored generation by the staleness guard.
        let incoming_generation = nexus_db_model::to_db_typed_generation(
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
            .execute_async(conn)
            .await
            // The conflict target is the primary key, so at most one row is
            // inserted or updated.
            .map(|rows_modified| match rows_modified {
                0 => SledBpAvailabilityUpsertOutcome::Rejected,
                1 => SledBpAvailabilityUpsertOutcome::Written,
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
    /// The caller is responsible for authorizing the operation.
    pub(crate) async fn rendezvous_sled_bp_availability_decommission_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        decommission: RendezvousSledBpAvailabilityDecommission,
    ) -> Result<SledBpAvailabilityDecommissionOutcome, Error> {
        // `FilterDsl` brings `.filter()` (the `WHERE` on the `ON CONFLICT DO
        // UPDATE` below) into scope; the prelude's `QueryDsl` only offers it for
        // table-like queries, not upsert statements.
        use diesel::query_dsl::methods::FilterDsl;
        use nexus_db_schema::schema::rendezvous_sled_bp_availability::dsl;

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
            .execute_async(conn)
            .await
            // The conflict target is the primary key, so at most one row is
            // inserted or updated.
            .map(|rows_modified| match rows_modified {
                0 => {
                    SledBpAvailabilityDecommissionOutcome::AlreadyDecommissioned
                }
                1 => SledBpAvailabilityDecommissionOutcome::Decommissioned,
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
    use async_bb8_diesel::AsyncSimpleConnection;
    use iddqd::id_ord_map;
    use nexus_db_model::ActiveSledBpAvailability;
    use nexus_db_model::SledBpAvailabilityState;
    use omicron_generation_kinds::UpdateDispositionGeneration;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;

    async fn upsert_one(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: SledUuid,
        availability: ActiveSledBpAvailability,
        generation: u32,
        blueprint_id: BlueprintUuid,
    ) -> SledBpAvailabilityUpsertOutcome {
        let update_disposition_generation =
            UpdateDispositionGeneration::from(generation);
        let writes = datastore
            .rendezvous_sled_bp_availability_write(
                opctx,
                blueprint_id,
                id_ord_map! {
                    SledBlueprintAvailabilityInput {
                        sled_id,
                        state: SledBpAvailabilityState::Active {
                            availability,
                            update_disposition_generation,
                        },
                    },
                },
            )
            .await
            .expect("query succeeded");
        match writes.get(&sled_id).expect("write for the input sled").outcome {
            SledBpAvailabilityWriteOutcome::Active {
                availability: written_availability,
                update_disposition_generation: written_generation,
                outcome,
            } => {
                assert_eq!(written_availability, availability);
                assert_eq!(written_generation, update_disposition_generation);
                outcome
            }
            SledBpAvailabilityWriteOutcome::Decommission(outcome) => panic!(
                "an active input must report an active outcome, got \
                 {outcome:?}"
            ),
        }
    }

    async fn decommission_one(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: SledUuid,
        blueprint_id: BlueprintUuid,
    ) -> SledBpAvailabilityDecommissionOutcome {
        let writes = datastore
            .rendezvous_sled_bp_availability_write(
                opctx,
                blueprint_id,
                id_ord_map! {
                    SledBlueprintAvailabilityInput {
                        sled_id,
                        state: SledBpAvailabilityState::Decommissioned,
                    },
                },
            )
            .await
            .expect("query succeeded");
        match writes.get(&sled_id).expect("write for the input sled").outcome {
            SledBpAvailabilityWriteOutcome::Decommission(outcome) => outcome,
            SledBpAvailabilityWriteOutcome::Active { .. } => panic!(
                "a decommissioned input must report a decommission outcome"
            ),
        }
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
        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            1,
            bp1,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Written,
            "first insert should write"
        );
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(1),
            }
        );

        // A newer generation (2) flipping the sled to unavailable should win.
        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Unavailable,
            2,
            bp2,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Written,
            "newer generation should write"
        );
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(2),
            }
        );
        assert_eq!(got.blueprint_id(), bp2);

        // A stale write at the same generation (2) trying to flip back to
        // available must be rejected.
        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            2,
            bp1,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Rejected,
            "equal-generation write should be rejected as stale"
        );
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(2),
            },
            "row must not have rolled back"
        );

        // A stale write at an older generation (1) must also be rejected.
        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            1,
            bp1,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Rejected,
            "older-generation write should be rejected as stale"
        );
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(2),
            },
            "row must not have rolled back"
        );

        // A newer generation (3) flipping back to available wins.
        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            3,
            bp2,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Written,
            "newest generation should write"
        );
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(3),
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
        upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            1,
            bp,
        )
        .await;
        assert_eq!(
            get(opctx, datastore, sled_id)
                .await
                .unwrap()
                .state()
                .expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(1u32),
            },
        );

        // Decommissioning it should change the sled's state to
        // `decommissioned`.
        let outcome = decommission_one(opctx, datastore, sled_id, bp).await;
        assert_eq!(
            outcome,
            SledBpAvailabilityDecommissionOutcome::Decommissioned
        );
        let got = get(opctx, datastore, sled_id).await.expect("row remains");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Decommissioned
        );

        // A second decommission is a no-op.
        let outcome = decommission_one(opctx, datastore, sled_id, bp).await;
        assert_eq!(
            outcome,
            SledBpAvailabilityDecommissionOutcome::AlreadyDecommissioned
        );

        // A stale Nexus must not be able to resurrect a decommissioned sled,
        // even at a newer generation.
        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            5,
            bp,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Rejected,
            "decommissioned sled must not be resurrected"
        );
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

        let outcome = decommission_one(opctx, datastore, sled_id, bp).await;
        assert_eq!(
            outcome,
            SledBpAvailabilityDecommissionOutcome::Decommissioned,
            "fresh tombstone insert should report true"
        );
        let got = get(opctx, datastore, sled_id).await.expect("row present");
        assert_eq!(
            got.state().expect("reassembled row state"),
            SledBpAvailabilityState::Decommissioned,
        );
        assert_eq!(got.blueprint_id(), bp);

        let outcome = upsert_one(
            opctx,
            datastore,
            sled_id,
            ActiveSledBpAvailability::Available,
            5,
            bp_stale,
        )
        .await;
        assert_eq!(
            outcome,
            SledBpAvailabilityUpsertOutcome::Rejected,
            "decommissioned sled must not be resurrected"
        );
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

    #[tokio::test]
    async fn bulk_write_reports_each_sled_outcome() {
        let logctx =
            dev::test_setup_log("bulk_write_reports_each_sled_outcome");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let bp1 = BlueprintUuid::new_v4();
        let bp2 = BlueprintUuid::new_v4();

        let fresh = SledUuid::new_v4();
        let stale_gen = SledUuid::new_v4();
        let tombstone = SledUuid::new_v4();

        upsert_one(
            opctx,
            datastore,
            stale_gen,
            ActiveSledBpAvailability::Available,
            2,
            bp1,
        )
        .await;

        let active =
            |availability, generation: u32| SledBpAvailabilityState::Active {
                availability,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(generation),
            };
        let writes = datastore
            .rendezvous_sled_bp_availability_write(
                opctx,
                bp2,
                id_ord_map! {
                    SledBlueprintAvailabilityInput {
                        sled_id: fresh,
                        state: active(ActiveSledBpAvailability::Available, 1),
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: stale_gen,
                        state: active(ActiveSledBpAvailability::Unavailable, 1),
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: tombstone,
                        state: SledBpAvailabilityState::Decommissioned,
                    },
                },
            )
            .await
            .expect("bulk write succeeded");

        let expected = id_ord_map! {
            SledBpAvailabilityWrite {
                sled_id: fresh,
                outcome: SledBpAvailabilityWriteOutcome::Active {
                    availability: ActiveSledBpAvailability::Available,
                    update_disposition_generation:
                        UpdateDispositionGeneration::from(1u32),
                    outcome: SledBpAvailabilityUpsertOutcome::Written,
                },
            },
            SledBpAvailabilityWrite {
                sled_id: stale_gen,
                outcome: SledBpAvailabilityWriteOutcome::Active {
                    availability: ActiveSledBpAvailability::Unavailable,
                    update_disposition_generation:
                        UpdateDispositionGeneration::from(1u32),
                    outcome: SledBpAvailabilityUpsertOutcome::Rejected,
                },
            },
            SledBpAvailabilityWrite {
                sled_id: tombstone,
                outcome: SledBpAvailabilityWriteOutcome::Decommission(
                    SledBpAvailabilityDecommissionOutcome::Decommissioned,
                ),
            },
        };
        assert_eq!(writes, expected);

        let rows = datastore
            .rendezvous_sled_bp_availability_list_all_batched(opctx)
            .await
            .expect("listed rows");
        let blueprint_id_of = |sled_id: SledUuid| {
            rows.get(&sled_id)
                .unwrap_or_else(|| panic!("row for sled {sled_id}"))
                .blueprint_id()
        };
        assert_eq!(blueprint_id_of(fresh), bp2);
        assert_eq!(
            blueprint_id_of(stale_gen),
            bp1,
            "rejected write must not touch the row"
        );
        assert_eq!(blueprint_id_of(tombstone), bp2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn bulk_write_stops_at_first_error() {
        let logctx = dev::test_setup_log("bulk_write_stops_at_first_error");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Use fixed sled IDs so we can cause a deterministic failure.
        let sled = |n: u128| SledUuid::from_u128(n);
        let rejected_upsert = sled(3);
        let rejected_decommission = sled(13);

        // Inject a deterministic failure via a test-only CHECK constraint.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        conn.batch_execute_async(&format!(
            "ALTER TABLE omicron.public.rendezvous_sled_bp_availability \
             ADD CONSTRAINT test_reject_sleds \
             CHECK (sled_id NOT IN ('{rejected_upsert}', \
             '{rejected_decommission}'))"
        ))
        .await
        .expect("added the test constraint");

        let bp = BlueprintUuid::new_v4();
        let available = SledBpAvailabilityState::Active {
            availability: ActiveSledBpAvailability::Available,
            update_disposition_generation: UpdateDispositionGeneration::from(
                1u32,
            ),
        };
        let available_written = SledBpAvailabilityWriteOutcome::Active {
            availability: ActiveSledBpAvailability::Available,
            update_disposition_generation: UpdateDispositionGeneration::from(
                1u32,
            ),
            outcome: SledBpAvailabilityUpsertOutcome::Written,
        };
        let decommissioned = SledBpAvailabilityWriteOutcome::Decommission(
            SledBpAvailabilityDecommissionOutcome::Decommissioned,
        );

        struct Case {
            name: &'static str,
            inputs: IdOrdMap<SledBlueprintAvailabilityInput>,
            expected_completed: IdOrdMap<SledBpAvailabilityWrite>,
            expected_failed_sled_id: SledUuid,
            expected_num_not_attempted: usize,
            expected_context: String,
            expected_absent: Vec<SledUuid>,
        }

        let cases = [
            Case {
                name: "upsert fails on the third of five sleds",
                inputs: id_ord_map! {
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(1),
                        state: available,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(2),
                        state: SledBpAvailabilityState::Decommissioned,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: rejected_upsert,
                        state: available,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(4),
                        state: SledBpAvailabilityState::Decommissioned,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(5),
                        state: available,
                    },
                },
                expected_completed: id_ord_map! {
                    SledBpAvailabilityWrite {
                        sled_id: sled(1),
                        outcome: available_written,
                    },
                    SledBpAvailabilityWrite {
                        sled_id: sled(2),
                        outcome: decommissioned,
                    },
                },
                expected_failed_sled_id: rejected_upsert,
                expected_num_not_attempted: 2,
                expected_context: format!(
                    "failed to upsert availability for sled {rejected_upsert}"
                ),
                expected_absent: vec![rejected_upsert, sled(4), sled(5)],
            },
            Case {
                name: "decommission fails on the third of four sleds",
                inputs: id_ord_map! {
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(11),
                        state: available,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(12),
                        state: available,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: rejected_decommission,
                        state: SledBpAvailabilityState::Decommissioned,
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: sled(14),
                        state: available,
                    },
                },
                expected_completed: id_ord_map! {
                    SledBpAvailabilityWrite {
                        sled_id: sled(11),
                        outcome: available_written,
                    },
                    SledBpAvailabilityWrite {
                        sled_id: sled(12),
                        outcome: available_written,
                    },
                },
                expected_failed_sled_id: rejected_decommission,
                expected_num_not_attempted: 1,
                expected_context: format!(
                    "failed to decommission sled {rejected_decommission}"
                ),
                expected_absent: vec![rejected_decommission, sled(14)],
            },
        ];

        for case in cases {
            let name = case.name;
            let err = datastore
                .rendezvous_sled_bp_availability_write(opctx, bp, case.inputs)
                .await
                .expect_err("the injected constraint fails the batch");
            match err {
                SledBpAvailabilityWriteError::Failed {
                    completed,
                    failed_sled_id,
                    num_not_attempted,
                    error,
                } => {
                    assert_eq!(completed, case.expected_completed, "{name}");
                    assert_eq!(
                        failed_sled_id, case.expected_failed_sled_id,
                        "{name}"
                    );
                    assert_eq!(
                        num_not_attempted, case.expected_num_not_attempted,
                        "{name}"
                    );
                    match error {
                        Error::InternalError { internal_message } => {
                            assert!(
                                internal_message
                                    .contains(&case.expected_context),
                                "{name}: internal message {internal_message:?} \
                                 must contain {:?}",
                                case.expected_context,
                            );
                            assert!(
                                internal_message.contains("CHECK constraint"),
                                "{name}: internal message {internal_message:?} \
                                 must name the CHECK constraint violation",
                            );
                        }
                        other => panic!(
                            "{name}: expected an internal error, got {other:?}"
                        ),
                    }
                }
                SledBpAvailabilityWriteError::NotStarted(error) => {
                    panic!("{name}: the write must have started, got {error:?}")
                }
            }

            let rows = datastore
                .rendezvous_sled_bp_availability_list_all_batched(opctx)
                .await
                .expect("listed rows");
            for write in &case.expected_completed {
                let row = rows.get(&write.sled_id).unwrap_or_else(|| {
                    panic!(
                        "{name}: completed write for sled {} is durable",
                        write.sled_id
                    )
                });
                assert_eq!(row.blueprint_id(), bp, "{name}");
            }
            for sled_id in case.expected_absent {
                assert!(
                    rows.get(&sled_id).is_none(),
                    "{name}: no row must exist for sled {sled_id}, which \
                     failed or was not attempted",
                );
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
