// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`SupportBundle`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Dataset;
use crate::db::model::DatasetKind;
use crate::db::model::SupportBundle;
use crate::db::model::SupportBundleState;
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use futures::FutureExt;
use nexus_types::identity::Asset;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use uuid::Uuid;

const CANNOT_ALLOCATE_ERR_MSG: &'static str =
"Current policy limits support bundle creation to 'one per external disk', and \
 no disks are available. Either delete old support bundles or add additional \
 disks";

/// Provides a report on how many bundle were expunged, and why.
#[derive(Debug, Clone)]
pub struct SupportBundleExpungementReport {
    pub bundles_failed_missing_datasets: usize,
    pub bundles_cancelled_missing_nexus: usize,
    pub bundles_failed_missing_nexus: usize,
}

impl DataStore {
    /// Creates a new support bundle.
    ///
    /// Requires that the UUID of the calling Nexus be supplied as input -
    /// this particular Zone is responsible for the collection process.
    ///
    /// Note that really any functioning Nexus would work as the "assignee",
    /// but it's clear that our instance will work, because we're currently
    /// running.
    pub async fn support_bundle_create(
        &self,
        opctx: &OpContext,
        reason_for_creation: &'static str,
        this_nexus_id: OmicronZoneUuid,
    ) -> CreateResult<SupportBundle> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        #[derive(Debug)]
        enum SupportBundleError {
            TooManyBundles,
        }

        let err = OptionalError::new();
        self.transaction_retry_wrapper("support_bundle_create")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    use db::schema::dataset::dsl as dataset_dsl;
                    use db::schema::support_bundle::dsl as support_bundle_dsl;

                    // Observe all "non-deleted, debug datasets".
                    //
                    // Return the first one we find that doesn't already
                    // have a support bundle allocated to it.
                    let free_dataset = dataset_dsl::dataset
                        .filter(dataset_dsl::time_deleted.is_null())
                        .filter(dataset_dsl::kind.eq(DatasetKind::Debug))
                        .left_join(support_bundle_dsl::support_bundle.on(
                            dataset_dsl::id.eq(support_bundle_dsl::dataset_id),
                        ))
                        .filter(support_bundle_dsl::dataset_id.is_null())
                        .select(Dataset::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?;

                    let Some(dataset) = free_dataset else {
                        return Err(
                            err.bail(SupportBundleError::TooManyBundles)
                        );
                    };

                    // We could check that "this_nexus_id" is not expunged, but
                    // we have some evidence that it is valid: this Nexus is
                    // currently running!
                    //
                    // Besides, we COULD be expunged immediately after inserting
                    // the SupportBundle. In this case, we'd fall back to the
                    // case of "clean up a bundle which is managed by an
                    // expunged Nexus" anyway.

                    let bundle = SupportBundle::new(
                        reason_for_creation,
                        ZpoolUuid::from_untyped_uuid(dataset.pool_id),
                        DatasetUuid::from_untyped_uuid(dataset.id()),
                        this_nexus_id,
                    );

                    diesel::insert_into(support_bundle_dsl::support_bundle)
                        .values(bundle.clone())
                        .execute_async(&conn)
                        .await?;

                    Ok(bundle)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        SupportBundleError::TooManyBundles => {
                            return external::Error::insufficient_capacity(
                                CANNOT_ALLOCATE_ERR_MSG,
                                "Support Bundle storage exhausted",
                            );
                        }
                    }
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Looks up a single support bundle
    pub async fn support_bundle_get(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
    ) -> LookupResult<SupportBundle> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::support_bundle::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        dsl::support_bundle
            .filter(dsl::id.eq(id.into_untyped_uuid()))
            .select(SupportBundle::as_select())
            .first_async::<SupportBundle>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Lists one page of support bundles
    pub async fn support_bundle_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SupportBundle> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::support_bundle::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        paginated(dsl::support_bundle, dsl::id, pagparams)
            .select(SupportBundle::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Marks support bundles as failed if their assigned Nexus or backing
    /// dataset has been destroyed.
    pub async fn support_bundle_fail_expunged(
        &self,
        opctx: &OpContext,
        blueprint: &nexus_types::deployment::Blueprint,
    ) -> Result<SupportBundleExpungementReport, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        // For this blueprint: The set of all expunged Nexus zones
        let invalid_nexus_zones = blueprint
            .all_omicron_zones(
                nexus_types::deployment::BlueprintZoneFilter::Expunged,
            )
            .filter_map(|(_sled, zone)| {
                if matches!(
                    zone.zone_type,
                    nexus_types::deployment::BlueprintZoneType::Nexus(_)
                ) {
                    Some(zone.id.into_untyped_uuid())
                } else {
                    None
                }
            })
            .collect::<Vec<Uuid>>();
        let valid_nexus_zones = blueprint
            .all_omicron_zones(
                nexus_types::deployment::BlueprintZoneFilter::ShouldBeRunning,
            )
            .filter_map(|(_sled, zone)| {
                if matches!(
                    zone.zone_type,
                    nexus_types::deployment::BlueprintZoneType::Nexus(_)
                ) {
                    Some(zone.id.into_untyped_uuid())
                } else {
                    None
                }
            })
            .collect::<Vec<Uuid>>();

        // For this blueprint: The set of expunged debug datasets
        let invalid_datasets = blueprint
            .all_omicron_datasets(
                nexus_types::deployment::BlueprintDatasetFilter::Expunged,
            )
            .filter_map(|dataset_config| {
                if matches!(
                    dataset_config.kind,
                    omicron_common::api::internal::shared::DatasetKind::Debug
                ) {
                    Some(dataset_config.id.into_untyped_uuid())
                } else {
                    None
                }
            })
            .collect::<Vec<Uuid>>();

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_if_current_blueprint_is(
            &conn,
            "support_bundle_fail_expunged",
            opctx,
            BlueprintUuid::from_untyped_uuid(blueprint.id),
            |conn| {
                let invalid_nexus_zones = invalid_nexus_zones.clone();
                let valid_nexus_zones = valid_nexus_zones.clone();
                let invalid_datasets = invalid_datasets.clone();
                async move {
                    use db::schema::support_bundle::dsl;

                    // Find all bundles on datasets that no longer exist, and
                    // mark them "failed".
                    let bundles_with_bad_datasets = dsl::support_bundle
                        .filter(dsl::dataset_id.eq_any(invalid_datasets))
                        .select(SupportBundle::as_select())
                        .load_async(&*conn)
                        .await?;

                    let bundles_failed_missing_datasets =
                        diesel::update(dsl::support_bundle)
                            .filter(dsl::state.ne(SupportBundleState::Failed))
                            .filter(
                                dsl::id.eq_any(
                                    bundles_with_bad_datasets
                                        .iter()
                                        .map(|b| b.id)
                                        .collect::<Vec<_>>(),
                                ),
                            )
                            .set((
                                dsl::state.eq(SupportBundleState::Failed),
                                dsl::reason_for_failure
                                    .eq("Allocated dataset no longer exists"),
                            ))
                            .execute_async(&*conn)
                            .await?;

                    let Some(arbitrary_valid_nexus) =
                        valid_nexus_zones.get(0).cloned()
                    else {
                        return Err(external::Error::internal_error(
                            "No valid Nexuses",
                        )
                        .into());
                    };

                    // Find all bundles on nexuses that no longer exist.
                    //
                    // If the bundle might have storage:
                    // - Mark it cancelled and re-assign the managing Nexus
                    // - Otherwise: mark it failed
                    let bundles_with_bad_nexuses = dsl::support_bundle
                        .filter(dsl::assigned_nexus.eq_any(invalid_nexus_zones))
                        .select(SupportBundle::as_select())
                        .load_async(&*conn)
                        .await?;
                    let (needs_cancellation, needs_failure): (Vec<_>, Vec<_>) =
                        bundles_with_bad_nexuses.into_iter().partition(
                            |bundle| bundle.state.might_have_dataset_storage(),
                        );

                    // Mark these support bundles as cancelled, and assign then
                    // to a nexus that should still exist.
                    //
                    // This should lead to their storage being freed, if it
                    // exists.
                    let bundles_cancelled_missing_nexus = diesel::update(
                        dsl::support_bundle,
                    )
                    .filter(dsl::state.ne(SupportBundleState::Cancelling))
                    .filter(dsl::state.ne(SupportBundleState::Failed))
                    .filter(
                        dsl::id.eq_any(
                            needs_cancellation
                                .iter()
                                .map(|b| b.id)
                                .collect::<Vec<_>>(),
                        ),
                    )
                    .set((
                        dsl::assigned_nexus.eq(arbitrary_valid_nexus),
                        dsl::state.eq(SupportBundleState::Cancelling),
                        dsl::reason_for_failure
                            .eq("Nexus managing this bundle no longer exists"),
                    ))
                    .execute_async(&*conn)
                    .await?;

                    // Mark these support bundles as failed.
                    //
                    // If they don't have storage (e.g., they never got that
                    // far, or their underlying dataset was expunged) there
                    // isn't anything left to free. This means we can skip the
                    // "Cancelling" state and jump to "Failed".
                    let bundles_failed_missing_nexus = diesel::update(
                        dsl::support_bundle,
                    )
                    .filter(dsl::state.ne(SupportBundleState::Cancelling))
                    .filter(dsl::state.ne(SupportBundleState::Failed))
                    .filter(dsl::id.eq_any(
                        needs_failure.iter().map(|b| b.id).collect::<Vec<_>>(),
                    ))
                    .set((
                        dsl::state.eq(SupportBundleState::Failed),
                        dsl::reason_for_failure
                            .eq("Nexus managing this bundle no longer exists"),
                    ))
                    .execute_async(&*conn)
                    .await?;

                    Ok(SupportBundleExpungementReport {
                        bundles_failed_missing_datasets,
                        bundles_cancelled_missing_nexus,
                        bundles_failed_missing_nexus,
                    })
                }
                .boxed()
            },
        )
        .await
    }

    /// Cancels a single support bundle.
    ///
    /// Note that this does not delete the support bundle record, but starts the
    /// process by which it may later be deleted.
    pub async fn support_bundle_cancel(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::support_bundle::dsl as support_bundle_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::update(support_bundle_dsl::support_bundle)
            .filter(support_bundle_dsl::id.eq(id.into_untyped_uuid()))
            .set(support_bundle_dsl::state.eq(SupportBundleState::Cancelling))
            .execute_async(&*conn)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Deletes a support bundle.
    ///
    /// This should only be invoked after all storage for the support bundle has
    /// been cleared.
    pub async fn support_bundle_delete(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::support_bundle::dsl as support_bundle_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::delete(support_bundle_dsl::support_bundle)
            .filter(support_bundle_dsl::id.eq(id.into_untyped_uuid()))
            .execute_async(&*conn)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::test::bp_insert_and_make_target;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_model::Zpool;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::example::SimRngState;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::BlueprintDatasetFilter;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::BlueprintZoneType;
    use omicron_common::api::internal::shared::DatasetKind::Debug as DebugDatasetKind;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SledUuid;
    use rand::Rng;

    // Pool/Dataset pairs, for debug datasets only.
    struct TestPool {
        pool: ZpoolUuid,
        dataset: DatasetUuid,
    }

    // Sleds and their pools, with a focus on debug datasets only.
    struct TestSled {
        sled: SledUuid,
        pools: Vec<TestPool>,
    }

    impl TestSled {
        fn new_with_pool_count(pool_count: usize) -> Self {
            Self {
                sled: SledUuid::new_v4(),
                pools: (0..pool_count)
                    .map(|_| TestPool {
                        pool: ZpoolUuid::new_v4(),
                        dataset: DatasetUuid::new_v4(),
                    })
                    .collect(),
            }
        }

        fn new_from_blueprint(blueprint: &Blueprint) -> Vec<Self> {
            let mut sleds = vec![];
            for (sled, datasets) in &blueprint.blueprint_datasets {
                let pools = datasets
                    .datasets
                    .values()
                    .filter_map(|dataset| {
                        if !matches!(dataset.kind, DebugDatasetKind)
                            || !dataset
                                .disposition
                                .matches(BlueprintDatasetFilter::InService)
                        {
                            return None;
                        };

                        Some(TestPool {
                            pool: dataset.pool.id(),
                            dataset: dataset.id,
                        })
                    })
                    .collect();

                sleds.push(TestSled { sled: *sled, pools });
            }
            sleds
        }

        async fn create_database_records(
            &self,
            datastore: &DataStore,
            opctx: &OpContext,
        ) {
            let rack_id = Uuid::new_v4();
            let sled = SledUpdate::new(
                *self.sled.as_untyped_uuid(),
                "[::1]:0".parse().unwrap(),
                SledBaseboard {
                    serial_number: format!(
                        "test-{}",
                        rand::thread_rng().gen::<u64>()
                    ),
                    part_number: "test-pn".to_string(),
                    revision: 0,
                },
                SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 128,
                    usable_physical_ram: (64 << 30).try_into().unwrap(),
                    reservoir_size: (16 << 30).try_into().unwrap(),
                },
                rack_id,
                Generation::new(),
            );
            datastore.sled_upsert(sled).await.expect("failed to upsert sled");

            // Create fake zpools that back our fake datasets.
            for pool in &self.pools {
                let zpool = Zpool::new(
                    *pool.pool.as_untyped_uuid(),
                    *self.sled.as_untyped_uuid(),
                    Uuid::new_v4(),
                );
                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("failed to upsert zpool");

                let dataset = Dataset::new(
                    pool.dataset.into_untyped_uuid(),
                    pool.pool.into_untyped_uuid(),
                    None,
                    DebugDatasetKind,
                );
                datastore
                    .dataset_upsert(dataset)
                    .await
                    .expect("failed to upsert dataset");
            }
        }
    }

    // Creates a fake sled with `pool_count` zpools, and a debug dataset on each
    // zpool.
    async fn create_sled_and_zpools(
        datastore: &DataStore,
        opctx: &OpContext,
        pool_count: usize,
    ) -> TestSled {
        let sled = TestSled::new_with_pool_count(pool_count);
        sled.create_database_records(&datastore, &opctx).await;
        sled
    }

    async fn support_bundle_create_expect_no_capacity(
        datastore: &DataStore,
        opctx: &OpContext,
        this_nexus_id: OmicronZoneUuid,
    ) {
        let err = datastore
            .support_bundle_create(&opctx, "for tests", this_nexus_id)
            .await
            .expect_err("Shouldn't provision bundle without datasets");
        let Error::InsufficientCapacity { message } = err else {
            panic!("Unexpected error: {err:?} - we expected 'InsufficientCapacity'");
        };
        assert_eq!(
            CANNOT_ALLOCATE_ERR_MSG,
            message.external_message(),
            "Unexpected error: {message:?}"
        );
    }

    #[tokio::test]
    async fn test_bundle_create_capacity_limits() {
        let logctx = dev::test_setup_log("test_bundle_create_capacity_limits");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let this_nexus_id = OmicronZoneUuid::new_v4();

        // No sleds, no datasets. Allocation should fail.

        support_bundle_create_expect_no_capacity(
            &datastore,
            &opctx,
            this_nexus_id,
        )
        .await;

        // Create a sled with a couple pools. Allocation should succeed.

        const POOL_COUNT: usize = 2;
        let _test_sled =
            create_sled_and_zpools(&datastore, &opctx, POOL_COUNT).await;
        let mut bundles = vec![];
        for _ in 0..POOL_COUNT {
            bundles.push(
                datastore
                    .support_bundle_create(
                        &opctx,
                        "for the test",
                        this_nexus_id,
                    )
                    .await
                    .expect("Should be able to create bundle"),
            );
        }

        // If we try to allocate any more bundles, we'll run out of capacity.

        support_bundle_create_expect_no_capacity(
            &datastore,
            &opctx,
            this_nexus_id,
        )
        .await;

        // If we cancel a bundle, it isn't deleted (yet).
        // This operation should signify that we can start to free up
        // storage on the dataset, but that needs to happen outside the
        // database.
        //
        // We should still expect to hit capacity limits.

        datastore
            .support_bundle_cancel(&opctx, bundles[0].id.into())
            .await
            .expect("Should be able to cancel this bundle");
        support_bundle_create_expect_no_capacity(
            &datastore,
            &opctx,
            this_nexus_id,
        )
        .await;

        // If we delete a bundle, it should be gone. This means we can
        // re-allocate from that dataset which was just freed up.

        datastore
            .support_bundle_delete(&opctx, bundles[0].id.into())
            .await
            .expect("Should be able to cancel this bundle");
        datastore
            .support_bundle_create(&opctx, "for the test", this_nexus_id)
            .await
            .expect("Should be able to create bundle");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_crud_operations() {
        let logctx = dev::test_setup_log("test_crud_operations");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_sled = create_sled_and_zpools(&datastore, &opctx, 1).await;
        let reason = "Bundle for test";
        let this_nexus_id = OmicronZoneUuid::new_v4();

        // Create the bundle, then observe it through the "getter" APIs

        let mut bundle = datastore
            .support_bundle_create(&opctx, reason, this_nexus_id)
            .await
            .expect("Should be able to create bundle");
        assert_eq!(bundle.reason_for_creation, reason);
        assert_eq!(bundle.reason_for_failure, None);
        assert_eq!(bundle.assigned_nexus, Some(this_nexus_id.into()));
        assert_eq!(bundle.state, SupportBundleState::Collecting);
        assert_eq!(bundle.zpool_id, test_sled.pools[0].pool.into());
        assert_eq!(bundle.dataset_id, test_sled.pools[0].dataset.into());

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just created");
        // Overwrite this column; it is modified slightly upon database insertion.
        bundle.time_created = observed_bundle.time_created;
        assert_eq!(bundle, observed_bundle);

        let pagparams = DataPageParams::max_page();
        let observed_bundles = datastore
            .support_bundle_list(&opctx, &pagparams)
            .await
            .expect("Should be able to get bundle we just created");
        assert_eq!(1, observed_bundles.len());
        assert_eq!(bundle, observed_bundles[0]);

        // Cancel the bundle, observe the new state

        datastore
            .support_bundle_cancel(&opctx, bundle.id.into())
            .await
            .expect("Should be able to cancel our bundle");
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just created");
        assert_eq!(SupportBundleState::Cancelling, observed_bundle.state);

        // Delete the bundle, observe that it's gone

        datastore
            .support_bundle_delete(&opctx, bundle.id.into())
            .await
            .expect("Should be able to cancel our bundle");
        let observed_bundles = datastore
            .support_bundle_list(&opctx, &pagparams)
            .await
            .expect("Should be able to get bundle we just created");
        assert!(observed_bundles.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn get_nexuses_from_blueprint(
        bp: &Blueprint,
        filter: BlueprintZoneFilter,
    ) -> Vec<OmicronZoneUuid> {
        bp.blueprint_zones
            .values()
            .map(|zones_config| {
                let mut nexus_zones = vec![];
                for zone in &zones_config.zones {
                    if matches!(zone.zone_type, BlueprintZoneType::Nexus(_))
                        && zone.disposition.matches(filter)
                    {
                        nexus_zones.push(zone.id);
                    }
                }
                nexus_zones
            })
            .flatten()
            .collect()
    }

    fn get_debug_datasets_from_blueprint(
        bp: &Blueprint,
        filter: BlueprintDatasetFilter,
    ) -> Vec<DatasetUuid> {
        bp.blueprint_datasets
            .values()
            .map(|datasets_config| {
                let mut debug_datasets = vec![];
                for dataset in datasets_config.datasets.values() {
                    if matches!(dataset.kind, DebugDatasetKind)
                        && dataset.disposition.matches(filter)
                    {
                        debug_datasets.push(dataset.id);
                    }
                }
                debug_datasets
            })
            .flatten()
            .collect()
    }

    #[tokio::test]
    async fn test_bundle_expungement() {
        static TEST_NAME: &str = "test_bundle_expungement";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (_example, mut bp1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();

        // Weirdly, the "ExampleSystemBuilder" blueprint has a parent blueprint,
        // but which isn't exposed through the API. Since we're only able to see
        // the blueprint it emits, that means we can't actually make it the
        // target because "the parent blueprint is not the current target".
        //
        // Instead of dealing with that, we lie: claim this is the primordial
        // blueprint, with no parent.
        //
        // Regardless, make this starter blueprint our target.
        bp1.parent_blueprint_id = None;
        bp_insert_and_make_target(&opctx, &datastore, &bp1).await;

        // Manually perform the equivalent of blueprint execution to populate
        // database records.
        let sleds = TestSled::new_from_blueprint(&bp1);
        for sled in &sleds {
            sled.create_database_records(&datastore, &opctx).await;
        }

        // Extract Nexus and Dataset information from the generated blueprint.
        let this_nexus_id = get_nexuses_from_blueprint(
            &bp1,
            BlueprintZoneFilter::ShouldBeRunning,
        )
        .get(0)
        .map(|id| *id)
        .expect("There should be a Nexus in the example blueprint");
        let debug_datasets = get_debug_datasets_from_blueprint(
            &bp1,
            BlueprintDatasetFilter::InService,
        );
        assert!(!debug_datasets.is_empty());

        // When we create a bundle, it should exist on a dataset provisioned by
        // the blueprint.
        let bundle = datastore
            .support_bundle_create(&opctx, "for the test", this_nexus_id)
            .await
            .expect("Should be able to create bundle");
        assert_eq!(bundle.assigned_nexus, Some(this_nexus_id.into()));

        assert!(
            debug_datasets.contains(&DatasetUuid::from(bundle.dataset_id)),
            "Bundle should have been allocated from a blueprint dataset"
        );

        // Expunge the bundle's dataset.
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = Uuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);

            for datasets in bp2.blueprint_datasets.values_mut() {
                for dataset in datasets.datasets.values_mut() {
                    if dataset.id == bundle.dataset_id.into() {
                        dataset.disposition =
                            BlueprintDatasetDisposition::Expunged;
                    }
                }
            }
            bp2
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp2).await;

        // TODO: Call this on bp1, observe no changes?
        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp2)
            .await
            .expect("Should have been able to mark bundle state as failed");

        assert_eq!(1, report.bundles_failed_missing_datasets);
        assert_eq!(0, report.bundles_cancelled_missing_nexus);
        assert_eq!(0, report.bundles_failed_missing_nexus);

        // TODO: Another test, maybe?
        // TODO: Find the nexus where we allocated the bundle
        // TODO: expunge it

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
