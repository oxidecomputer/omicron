// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of the `rendezvous_sled_bp_availability` table (part of RFD 666).
//!
//! See the table comment in `dbinit.sql`.

use anyhow::Context;
use iddqd::IdOrdMap;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SledBpAvailabilityWriteError;
use nexus_db_queries::db::model::DbSledBpAvailability;
use nexus_db_queries::db::model::SledBlueprintAvailabilityInput;
use nexus_db_queries::db::model::SledBpAvailabilityState;
use nexus_types::internal_api::background::SledBlueprintAvailabilityRendezvousStats;
use omicron_uuid_kinds::BlueprintUuid;
use slog::error;
use slog::info;

/// Reconcile the `rendezvous_sled_bp_availability` table against the target
/// blueprint.
///
/// `blueprint_sleds` should contain a [`SledBlueprintAvailabilityInput`] for
/// every sled in the blueprint, including decommissioned sleds.
pub(crate) async fn reconcile_sled_blueprint_availability(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint_id: BlueprintUuid,
    blueprint_sleds: IdOrdMap<SledBlueprintAvailabilityInput>,
) -> anyhow::Result<SledBlueprintAvailabilityRendezvousStats> {
    // Fetch the current contents of the table up front, so we can skip no-op
    // writes when nothing has changed since the last activation. Similar to the
    // dataset reconcilers, the write-skipping is purely an optimization: the
    // code below is correct without it, just chattier. The snapshot also
    // drives the invariant check and the not-in-blueprint accounting below.
    let mut existing_db_sleds = datastore
        .rendezvous_sled_bp_availability_list_all_batched(opctx)
        .await
        .context("failed to list sleds in rendezvous_sled_bp_availability")?;

    let mut stats = SledBlueprintAvailabilityRendezvousStats::default();

    // Use the snapshot to decide which sleds need a write at all. (This will
    // always identify a superset of writes that will be successful.)
    let mut to_write = IdOrdMap::new();
    for input in blueprint_sleds {
        let SledBlueprintAvailabilityInput { sled_id, state } = input;
        // Account for this sled; rows left in `existing_db_sleds` after the loop
        // are ones the blueprint doesn't mention (handled below).
        let existing = existing_db_sleds.remove(&sled_id);
        let existing_state = match &existing {
            None => None,
            Some(row) => Some(row.state()?),
        };

        match state {
            SledBpAvailabilityState::Active {
                availability,
                update_disposition_generation,
            } => {
                if let (
                    Some(row),
                    Some(SledBpAvailabilityState::Active {
                        availability: stored_availability,
                        update_disposition_generation: stored_generation,
                    }),
                ) = (&existing, existing_state)
                    && stored_generation == update_disposition_generation
                    && stored_availability != availability
                {
                    // It shouldn't be possible to have an equal stored
                    // generation with different availability. If we see
                    // this, there's a planner bug that led to db
                    // corruption. Log an error, count it in the stats,
                    // and otherwise leave this row alone.
                    error!(
                        opctx.log,
                        "sled availability invariant violated: equal \
                         update_disposition generation with different \
                         availability";
                        "sled_id" => %sled_id,
                        "update_disposition_generation" =>
                            %update_disposition_generation,
                        "stored_availability" =>
                            DbSledBpAvailability::from(stored_availability)
                                .label(),
                        "blueprint_availability" =>
                            DbSledBpAvailability::from(availability).label(),
                        "stored_blueprint_id" => %row.blueprint_id(),
                        "blueprint_id" => %blueprint_id,
                    );
                    stats.num_invariant_violations += 1;
                    continue;
                }

                // Skip no-op writes.
                //
                // * For active sleds, we write only when the blueprint's
                //   generation is newer than what's recorded. Note that the
                //   upsert checks this atomically as well -- this is just an
                //   optimization.
                // * Decommissioned sleds are terminal so there isn't anything
                //   to do here.
                let needs_write = match existing_state {
                    None => true,
                    Some(SledBpAvailabilityState::Decommissioned) => false,
                    Some(SledBpAvailabilityState::Active {
                        update_disposition_generation: stored_generation,
                        ..
                    }) => stored_generation < update_disposition_generation,
                };
                if !needs_write {
                    stats.num_unchanged += 1;
                    continue;
                }
            }
            SledBpAvailabilityState::Decommissioned => {
                // This is monotonic, so unlike the available/unavailable flip
                // it isn't guarded by a generation number.
                match existing_state {
                    Some(SledBpAvailabilityState::Decommissioned) => {
                        stats.num_already_decommissioned += 1;
                        continue;
                    }
                    // The None case here means that:
                    //
                    // * Our snapshot of the existing contents didn't have a row for
                    //   the sled.
                    // * But the sled is in the blueprint and marked decommissioned.
                    //
                    // This can legitimately happen in two ways:
                    //
                    // 1. The sled was added and decommissioned without a
                    //    rendezvous pass observing it as active. (e.g., this
                    //    task was disabled across that window).
                    // 2. A stale Nexus, acting on an older blueprint in which
                    //    the sled was still active, inserted a row after the
                    //    snapshot was taken.
                    //
                    // So in the None case we still write the decommission,
                    // which does an upsert just like the active-sled write.
                    //
                    // * With case 1 we'll insert a fresh row.
                    // * With case 2 we'll tombstone the racing row.
                    //
                    // Either way, the sled ends up with a durable
                    // decommissioned tombstone.
                    None | Some(SledBpAvailabilityState::Active { .. }) => {}
                }
            }
        }

        to_write.insert_unique(input).expect(
            "blueprint_sleds is keyed by sled ID, so each sled appears once",
        );
    }

    let writes = match datastore
        .rendezvous_sled_bp_availability_write(opctx, blueprint_id, to_write)
        .await
    {
        Ok(writes) => writes,
        Err(SledBpAvailabilityWriteError::Failed {
            completed,
            failed_sled_id,
            num_not_attempted,
            error,
        }) => {
            for write in &completed {
                write.log_to_and_count(&opctx.log, &mut stats);
            }
            error!(
                opctx.log,
                "sled availability write failed partway; writes completed \
                 before the failure are counted here";
                "blueprint_id" => %blueprint_id,
                "failed_sled_id" => %failed_sled_id,
                "num_not_attempted" => num_not_attempted,
                "error" => %error,
                &stats,
            );
            return Err(anyhow::Error::from(error).context(format!(
                "failed to write availability for sled {failed_sled_id} \
                 ({num_not_attempted} sled(s) not attempted)",
            )));
        }
        Err(err @ SledBpAvailabilityWriteError::NotStarted(_)) => {
            return Err(anyhow::Error::from(err)
                .context("failed to write sled availability"));
        }
    };
    for write in &writes {
        write.log_to_and_count(&opctx.log, &mut stats);
    }

    // Rows still here are for sleds the blueprint doesn't mention. We leave
    // them untouched either way, but account for them separately:
    //
    // * An active row can only be left over on a stale blueprint predating a
    //   sled another Nexus already recorded, so it's worth calling out.
    // * A decommissioned row is a terminal tombstone. The blueprint doesn't
    //   prune decommissioned sleds today, but once it does this will be the
    //   steady state for every pruned sled, so it isn't noteworthy.
    let mut active_not_in_blueprint = Vec::new();
    for row in &existing_db_sleds {
        match row.state()? {
            SledBpAvailabilityState::Active { .. } => {
                active_not_in_blueprint.push(row.sled_id());
            }
            SledBpAvailabilityState::Decommissioned => {
                stats.num_decommissioned_not_in_blueprint += 1;
            }
        }
    }
    stats.num_not_in_blueprint = active_not_in_blueprint.len();
    if !active_not_in_blueprint.is_empty() {
        info!(
            opctx.log,
            "left active rows for sleds absent from the target blueprint \
             untouched; this Nexus may be acting on a stale blueprint";
            "num_not_in_blueprint" => stats.num_not_in_blueprint,
            "sled_ids" => ?active_not_in_blueprint,
        );
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::usize_to_id;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use iddqd::id_ord_map;
    use nexus_db_queries::db::model::ActiveSledBpAvailability;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
    use omicron_generation_kinds::UpdateDispositionGeneration;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SledUuid;
    use proptest::prelude::*;
    use proptest::proptest;
    use test_strategy::Arbitrary;

    #[derive(Debug, Clone, Copy, Arbitrary)]
    struct PrepState {
        #[strategy(prop_oneof![
            Just(ActiveSledBpAvailability::Available),
            Just(ActiveSledBpAvailability::Unavailable),
        ])]
        availability: ActiveSledBpAvailability,
        #[strategy(1u32..=4)]
        generation: u32,
    }

    impl PrepState {
        fn state(self) -> SledBpAvailabilityState {
            SledBpAvailabilityState::Active {
                availability: self.availability,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(self.generation),
            }
        }
    }

    /// A pre-existing table row.
    #[derive(Debug, Clone, Copy, Arbitrary)]
    enum DbPrep {
        /// An `available`/`unavailable` row at some generation.
        Active(PrepState),
        /// A terminal `decommissioned` row.
        Decommissioned,
    }

    impl DbPrep {
        fn state(self) -> SledBpAvailabilityState {
            match self {
                DbPrep::Active(ps) => ps.state(),
                DbPrep::Decommissioned => {
                    SledBpAvailabilityState::Decommissioned
                }
            }
        }

        async fn insert(
            self,
            opctx: &OpContext,
            datastore: &DataStore,
            sled_id: SledUuid,
            blueprint_id: BlueprintUuid,
        ) {
            datastore
                .rendezvous_sled_bp_availability_write(
                    opctx,
                    blueprint_id,
                    id_ord_map! {
                        SledBlueprintAvailabilityInput {
                            sled_id,
                            state: self.state(),
                        },
                    },
                )
                .await
                .expect("query succeeded");
        }
    }

    /// A sled's disposition in the blueprint being reconciled.
    #[derive(Debug, Clone, Copy, Arbitrary)]
    enum BlueprintPrep {
        /// An active sled with an availability disposition at some generation.
        Active(PrepState),
        /// A sled the blueprint explicitly marks decommissioned.
        Decommissioned,
    }

    impl BlueprintPrep {
        fn state(self) -> SledBpAvailabilityState {
            match self {
                BlueprintPrep::Active(ps) => ps.state(),
                BlueprintPrep::Decommissioned => {
                    SledBpAvailabilityState::Decommissioned
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, Arbitrary)]
    struct SledPrep {
        /// The sled's existing table row, or `None` if no row exists yet.
        in_database: Option<DbPrep>,
        /// The sled's disposition in the blueprint, or `None` if the blueprint
        /// being reconciled does not mention this sled at all (e.g. a stale
        /// blueprint that predates the sled's addition).
        in_blueprint: Option<BlueprintPrep>,
    }

    impl SledPrep {
        // Compute what the row should look like after reconciliation, or return
        // `None` if the row should not be altered.
        fn expected_row(self) -> Option<SledBpAvailabilityState> {
            match (self.in_database, self.in_blueprint) {
                // A decommissioned row is terminal.
                (Some(DbPrep::Decommissioned), _) => {
                    Some(SledBpAvailabilityState::Decommissioned)
                }
                // The blueprint does not mention this sled at all. Any
                // existing row must be left exactly as it is.
                (None, None) => None,
                (Some(DbPrep::Active(pre)), None) => Some(pre.state()),
                // The sled is decommissioned in the blueprint.
                (
                    Some(DbPrep::Active(_)) | None,
                    Some(BlueprintPrep::Decommissioned),
                ) => Some(SledBpAvailabilityState::Decommissioned),
                // This sled is active in the blueprint. The result depends on
                // which generation is later.
                //
                // Note that a different value with the same generation is
                // logged as an error (and counted in num_invariant_violations)
                // by the reconciler, so this proptest will cause a bunch of
                // those lines to be emitted. That is expected and harmless.
                (None, Some(BlueprintPrep::Active(bp))) => Some(bp.state()),
                (
                    Some(DbPrep::Active(pre)),
                    Some(BlueprintPrep::Active(bp)),
                ) => {
                    if bp.generation > pre.generation {
                        Some(bp.state())
                    } else {
                        Some(pre.state())
                    }
                }
            }
        }

        fn update_stats(
            self,
            stats: &mut SledBlueprintAvailabilityRendezvousStats,
        ) {
            match (self.in_database, self.in_blueprint) {
                // Not mentioned by the blueprint.
                (None, None) => {}
                (Some(DbPrep::Active(_)), None) => {
                    stats.num_not_in_blueprint += 1;
                }
                (Some(DbPrep::Decommissioned), None) => {
                    stats.num_decommissioned_not_in_blueprint += 1;
                }
                // Fresh insert driven by an active blueprint sled.
                (None, Some(BlueprintPrep::Active(bp))) => {
                    if bp.availability == ActiveSledBpAvailability::Available {
                        stats.num_marked_available += 1;
                    } else {
                        stats.num_marked_unavailable += 1;
                    }
                }
                // Active row also listed active in the blueprint: a newer
                // generation rewrites it, an equal generation with different
                // availability is an invariant violation, and any other
                // equal-or-older one is a no-op.
                (
                    Some(DbPrep::Active(pre)),
                    Some(BlueprintPrep::Active(bp)),
                ) => {
                    if bp.generation > pre.generation {
                        if bp.availability
                            == ActiveSledBpAvailability::Available
                        {
                            stats.num_marked_available += 1;
                        } else {
                            stats.num_marked_unavailable += 1;
                        }
                    } else if bp.generation == pre.generation
                        && bp.availability != pre.availability
                    {
                        stats.num_invariant_violations += 1;
                    } else {
                        stats.num_unchanged += 1;
                    }
                }
                // Terminal row that the blueprint still lists as active:
                // left untouched, counted as unchanged.
                (
                    Some(DbPrep::Decommissioned),
                    Some(BlueprintPrep::Active(_)),
                ) => {
                    stats.num_unchanged += 1;
                }
                // Blueprint marks the sled decommissioned, and within the db
                // the sled is either active or not present.
                (
                    Some(DbPrep::Active(_)) | None,
                    Some(BlueprintPrep::Decommissioned),
                ) => {
                    stats.num_decommissioned += 1;
                }
                // Blueprint marks the sled decommissioned, but there is
                // nothing to do (an already-terminal row).
                (
                    Some(DbPrep::Decommissioned),
                    Some(BlueprintPrep::Decommissioned),
                ) => {
                    stats.num_already_decommissioned += 1;
                }
            }
        }
    }

    // Clean up from any previous proptest cases.
    async fn clear_table(datastore: &DataStore) {
        use nexus_db_schema::schema::rendezvous_sled_bp_availability::dsl;
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        datastore
            .transaction_non_retry_wrapper("proptest_prep_cleanup")
            .transaction(&conn, |conn| async move {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                diesel::delete(dsl::rendezvous_sled_bp_availability)
                    .execute_async(&conn)
                    .await?;
                Ok::<_, diesel::result::Error>(())
            })
            .await
            .unwrap();
    }

    async fn proptest_do_prep(
        opctx: &OpContext,
        datastore: &DataStore,
        blueprint_id: BlueprintUuid,
        prep: &[SledPrep],
    ) -> IdOrdMap<SledBlueprintAvailabilityInput> {
        clear_table(datastore).await;

        let mut blueprint_sleds = IdOrdMap::new();
        for (id, prep) in prep.iter().enumerate() {
            let sled_id: SledUuid = usize_to_id(id);
            if let Some(db) = prep.in_database {
                db.insert(opctx, datastore, sled_id, blueprint_id).await;
            }
            if let Some(bp) = prep.in_blueprint {
                blueprint_sleds
                    .insert_unique(SledBlueprintAvailabilityInput {
                        sled_id,
                        state: bp.state(),
                    })
                    .expect("usize_to_id makes each sled ID unique");
            }
        }
        blueprint_sleds
    }

    #[test]
    fn proptest_reconciliation() {
        // Create our own runtime so we can interleave expensive async setup
        // (building a datastore) with a proptest that itself runs async code.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .enable_io()
            .build()
            .expect("tokio Runtime built successfully");

        let logctx = dev::test_setup_log("proptest_reconciliation");
        let db =
            runtime.block_on(TestDatabase::new_with_datastore(&logctx.log));
        let (opctx, datastore) = (db.opctx(), db.datastore());

        proptest!(ProptestConfig::with_cases(64),
        |(prep in proptest::collection::vec(
            any::<SledPrep>(),
            0..20,
        ))| {
            let blueprint_id = BlueprintUuid::new_v4();

            let (result_stats, db_rows) = runtime.block_on(async {
                let blueprint_sleds = proptest_do_prep(
                    opctx,
                    datastore,
                    blueprint_id,
                    &prep,
                ).await;

                let result_stats = reconcile_sled_blueprint_availability(
                    opctx,
                    datastore,
                    blueprint_id,
                    blueprint_sleds,
                )
                .await
                .expect("reconciled sled availability");

                let db_rows = datastore
                    .rendezvous_sled_bp_availability_list_all_batched(opctx)
                    .await
                    .unwrap();

                (result_stats, db_rows)
            });

            // The stats the reconcile pass should have reported, accumulated
            // per sled below and asserted once at the end.
            let mut expected_stats = SledBlueprintAvailabilityRendezvousStats::default();

            for (id, prep) in prep.iter().enumerate() {
                let sled_id: SledUuid = usize_to_id(id);
                let row = db_rows.get(&sled_id).map(|s| {
                    s.state().expect("reassembled row state")
                });

                prop_assert_eq!(
                    row, prep.expected_row(),
                    "unexpected row for {}: prep {:?}", sled_id, prep,
                );
                prep.update_stats(&mut expected_stats);
            }

            prop_assert_eq!(result_stats, expected_stats);
        });

        runtime.block_on(db.terminate());
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn partial_failure_then_retry_converges() {
        let logctx =
            dev::test_setup_log("partial_failure_then_retry_converges");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Use fixed sled IDs and inject a deterministic failure.
        let rejected: SledUuid = usize_to_id(3);
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        conn.batch_execute_async(&format!(
            "ALTER TABLE omicron.public.rendezvous_sled_bp_availability \
             ADD CONSTRAINT test_reject_sled CHECK (sled_id != '{rejected}')"
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
        let inputs = || {
            IdOrdMap::from_iter_unique((1..=5).map(|n| {
                SledBlueprintAvailabilityInput {
                    sled_id: usize_to_id(n),
                    state: if n == 2 {
                        SledBpAvailabilityState::Decommissioned
                    } else {
                        available
                    },
                }
            }))
            .expect("distinct sled IDs")
        };

        let err = reconcile_sled_blueprint_availability(
            opctx,
            datastore,
            bp,
            inputs(),
        )
        .await
        .expect_err("the injected constraint fails the pass");
        let message = format!("{err:#}");
        for needle in [
            format!(
                "failed to write availability for sled {rejected} \
                 (2 sled(s) not attempted)"
            ),
            format!("failed to upsert availability for sled {rejected}"),
        ] {
            assert!(
                message.contains(&needle),
                "error {message:?} must contain {needle:?}"
            );
        }

        let rows = datastore
            .rendezvous_sled_bp_availability_list_all_batched(opctx)
            .await
            .expect("listed rows");
        let states: Vec<(SledUuid, Option<SledBpAvailabilityState>)> = (1..=5)
            .map(|n| {
                let sled_id: SledUuid = usize_to_id(n);
                let state = rows
                    .get(&sled_id)
                    .map(|row| row.state().expect("reassembled row state"));
                (sled_id, state)
            })
            .collect();
        assert_eq!(
            states,
            vec![
                (usize_to_id(1), Some(available)),
                (usize_to_id(2), Some(SledBpAvailabilityState::Decommissioned)),
                (usize_to_id(3), None),
                (usize_to_id(4), None),
                (usize_to_id(5), None),
            ],
            "writes before the failure are durable; the failed and \
             unattempted sleds have no row",
        );

        conn.batch_execute_async(
            "ALTER TABLE omicron.public.rendezvous_sled_bp_availability \
             DROP CONSTRAINT test_reject_sled",
        )
        .await
        .expect("dropped the test constraint");

        let stats = reconcile_sled_blueprint_availability(
            opctx,
            datastore,
            bp,
            inputs(),
        )
        .await
        .expect("the retry succeeds once the constraint is gone");
        assert_eq!(
            stats,
            SledBlueprintAvailabilityRendezvousStats {
                num_marked_available: 3,
                num_unchanged: 1,
                num_already_decommissioned: 1,
                ..Default::default()
            },
            "the retry writes only the sleds the failed pass did not reach",
        );

        let rows = datastore
            .rendezvous_sled_bp_availability_list_all_batched(opctx)
            .await
            .expect("listed rows");
        for input in inputs() {
            let row = rows.get(&input.sled_id).unwrap_or_else(|| {
                panic!("row present for sled {}", input.sled_id)
            });
            assert_eq!(
                row.state().expect("reassembled row state"),
                input.state,
                "sled {} matches the blueprint after the retry",
                input.sled_id,
            );
            assert_eq!(row.blueprint_id(), bp);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn equal_generation_different_availability_left_untouched() {
        let logctx = dev::test_setup_log(
            "equal_generation_different_availability_left_untouched",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_id: SledUuid = usize_to_id(1);
        let bp_stored = BlueprintUuid::new_v4();
        let bp_reconciled = BlueprintUuid::new_v4();

        datastore
            .rendezvous_sled_bp_availability_write(
                opctx,
                bp_stored,
                id_ord_map! {
                    SledBlueprintAvailabilityInput {
                        sled_id,
                        state: SledBpAvailabilityState::Active {
                            availability: ActiveSledBpAvailability::Available,
                            update_disposition_generation:
                                UpdateDispositionGeneration::from(2u32),
                        },
                    },
                },
            )
            .await
            .expect("seeded the stored row");

        let stats = reconcile_sled_blueprint_availability(
            opctx,
            datastore,
            bp_reconciled,
            id_ord_map! {
                SledBlueprintAvailabilityInput {
                    sled_id,
                    state: SledBpAvailabilityState::Active {
                        availability: ActiveSledBpAvailability::Unavailable,
                        update_disposition_generation:
                            UpdateDispositionGeneration::from(2u32),
                    },
                },
            },
        )
        .await
        .expect("reconciled sled availability");

        assert_eq!(
            stats,
            SledBlueprintAvailabilityRendezvousStats {
                num_invariant_violations: 1,
                ..Default::default()
            },
            "the corrupt-looking sled must be counted as an invariant \
             violation and nothing else",
        );

        let row = datastore
            .rendezvous_sled_bp_availability_list_all_batched(opctx)
            .await
            .unwrap()
            .get(&sled_id)
            .cloned()
            .expect("row present");
        assert_eq!(
            row.state().expect("reassembled row state"),
            SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation:
                    UpdateDispositionGeneration::from(2u32),
            },
            "the stored row must be left untouched",
        );
        assert_eq!(
            row.blueprint_id(),
            bp_stored,
            "blueprint_id must not be rewritten",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
