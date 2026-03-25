// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for executing requested state changes in the fault
//! management blueprint.

use crate::app::background::BackgroundTask;
use crate::app::background::tasks::fm_sitrep_load::CurrentSitrep;
use futures::future::BoxFuture;
use nexus_background_task_interface::Activator;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_types::fm::Sitrep;
use nexus_types::fm::SitrepVersion;
use nexus_types::fm::case::AlertRequest;
use nexus_types::internal_api::background::FmAlertStats as AlertStats;
use nexus_types::internal_api::background::FmRendezvousStatus as Status;
use nexus_types::internal_api::background::FmSupportBundleStats as SupportBundleStats;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

pub struct FmRendezvous {
    datastore: Arc<DataStore>,
    sitrep_watcher: watch::Receiver<CurrentSitrep>,
    alert_dispatcher: Activator,
    support_bundle_collector: Activator,
    nexus_id: OmicronZoneUuid,
}

impl BackgroundTask for FmRendezvous {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    let err = format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&err)
                    );
                    json!({ "error": err })
                }
            }
        })
    }
}

impl FmRendezvous {
    pub fn new(
        datastore: Arc<DataStore>,
        rx: watch::Receiver<CurrentSitrep>,
        alert_dispatcher: Activator,
        support_bundle_collector: Activator,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        Self {
            datastore,
            sitrep_watcher: rx,
            alert_dispatcher,
            support_bundle_collector,
            nexus_id,
        }
    }

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        let Some(sitrep) = self.sitrep_watcher.borrow_and_update().clone()
        else {
            return Status::NoSitrep;
        };

        // TODO(eliza): as we start doing other things (i.e. updating
        // problems), consider spawning these in their own tasks...
        let alerts = self.create_requested_alerts(&sitrep, opctx).await;
        let support_bundles =
            self.create_requested_support_bundles(&sitrep, opctx).await;

        Status::Executed { sitrep_id: sitrep.1.id(), alerts, support_bundles }
    }

    async fn create_requested_alerts(
        &self,
        sitrep: &Arc<(SitrepVersion, Sitrep)>,
        opctx: &OpContext,
    ) -> AlertStats {
        let (_, ref sitrep) = **sitrep;
        let mut status = AlertStats::default();

        // XXX(eliza): is it better to allocate all of these into a big array
        // and do a single `INSERT INTO` query, or iterate over them one by one
        // (not allocating) but insert one at a time? Note that a batched insert
        // would need to use `ON CONFLICT DO NOTHING` rather than checking for
        // `Conflict` errors from individual inserts, since multiple Nexus
        // instances may run this task concurrently.
        //
        // TODO(#9592) Currently, these `alert_create` calls have no guard
        // against a stale Nexus inserting alerts from an outdated sitrep. This
        // is fine for now because alert requests are carried forward into newer
        // sitreps, so a stale insert is redundant rather than incorrect.
        // However, if alerts are ever hard-deleted (e.g. when a case is
        // closed), a lagging Nexus could re-create "zombie" alert records after
        // deletion. At that point, the INSERT should be guarded by a CTE that
        // checks the sitrep generation matches the current one.
        for (case_id, req) in sitrep.alerts_requested() {
            let &AlertRequest { id, class, requested_sitrep_id, .. } = req;
            status.total_alerts_requested += 1;
            if requested_sitrep_id == sitrep.id() {
                status.current_sitrep_alerts_requested += 1;
            }
            match self
                .datastore
                .alert_create(
                    &opctx,
                    db::model::Alert::for_fm_alert_request(req, case_id),
                )
                .await
            {
                // Alert already exists --- this is expected, since multiple
                // Nexus instances may run this task concurrently for the same
                // sitrep, or a previous activation may have partially completed.
                Err(Error::Conflict { .. }) => {}
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "failed to create requested alert";
                        "case_id" => %case_id,
                        "alert_id" => %id,
                        "alert_class" => %class,
                        "error" => %e,
                    );
                    status
                        .errors
                        .push(format!("alert {id} (class: {class}): {e}"));
                }
                Ok(_) => status.alerts_created += 1,
            }
        }

        let n_errors = status.errors.len();
        if n_errors > 0 {
            slog::warn!(
                opctx.log,
                "created {} alerts requested by the current sitrep, but \
                 {n_errors} alerts could not be created!",
                status.alerts_created;
                "sitrep_id" => %sitrep.id(),
                "total_alerts_requested" => status.total_alerts_requested,
                "alerts_created" => status.alerts_created,
                "errors" => n_errors,
            );
        } else if status.alerts_created > 0 {
            slog::info!(
                opctx.log,
                "created {} alerts requested by the current sitrep",
                status.alerts_created;
                "sitrep_id" => %sitrep.id(),
                "total_alerts_requested" => status.total_alerts_requested,
                "alerts_created" => status.alerts_created,
            );
        } else if status.total_alerts_requested > 0 {
            slog::debug!(
                opctx.log,
                "all alerts requested by the current sitrep already exist";
                "sitrep_id" => %sitrep.id(),
                "total_alerts_requested" => status.total_alerts_requested,
                "alerts_created" => status.alerts_created,
            );
        } else {
            slog::debug!(
                opctx.log,
                "current sitrep requests no alerts";
                "sitrep_id" => %sitrep.id(),
                "total_alerts_requested" => status.total_alerts_requested,
                "alerts_created" => status.alerts_created,
            );
        }

        // We created some alerts, so let the alert dispatcher know.
        if status.alerts_created > 0 {
            self.alert_dispatcher.activate();
        }

        status
    }

    async fn create_requested_support_bundles(
        &self,
        sitrep: &Arc<(SitrepVersion, Sitrep)>,
        opctx: &OpContext,
    ) -> SupportBundleStats {
        let (_, ref sitrep) = **sitrep;
        let mut status = SupportBundleStats::default();

        // Like alert creation (see `create_requested_alerts`), we iterate
        // ALL bundle requests in the sitrep, not just new ones. Multiple Nexus
        // instances may run this concurrently; `Conflict` on duplicate insert
        // is treated as a no-op.
        //
        // TODO(#9592) Same stale-Nexus concern as alerts: if support bundle
        // requests are ever removed from sitreps (e.g. when a case is closed),
        // a lagging Nexus could re-create "zombie" bundles from an outdated
        // sitrep. Currently safe because requests are carried forward.
        for (case, req) in sitrep.support_bundles_requested() {
            let bundle_id = req.id;
            status.total_bundles_requested += 1;
            if req.requested_sitrep_id == sitrep.id() {
                status.current_sitrep_bundles_requested += 1;
            }

            let reason = format!(
                "Requested by {:?} diagnosis engine for case {}",
                case.de, case.id,
            );
            match self
                .datastore
                .support_bundle_create(
                    opctx,
                    nexus_types::support_bundle::SupportBundleCreateParams {
                        id: bundle_id,
                        reason,
                        nexus_id: self.nexus_id,
                        user_comment: None,
                        fm_case_id: Some(case.id),
                    },
                )
                .await
            {
                // Bundle already exists (idempotent retry), that's fine.
                Err(Error::Conflict { .. }) => {}
                Err(Error::InsufficientCapacity { .. }) => {
                    slog::warn!(
                        opctx.log,
                        "insufficient capacity for support bundle";
                        "case_id" => %case.id,
                        "bundle_id" => %bundle_id,
                    );
                    status.capacity_errors += 1;
                }
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "failed to create requested support bundle";
                        "case_id" => %case.id,
                        "bundle_id" => %bundle_id,
                        "error" => %e,
                    );
                    status
                        .errors
                        .push(format!("support bundle {bundle_id}: {e}"));
                }
                Ok(_) => status.bundles_created += 1,
            }
        }

        let n_errors = status.errors.len();
        if n_errors > 0 || status.capacity_errors > 0 {
            slog::warn!(
                opctx.log,
                "created {} support bundles requested by the current \
                 sitrep, but {} requests could not be fulfilled!",
                status.bundles_created,
                n_errors + status.capacity_errors;
                "sitrep_id" => %sitrep.id(),
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
                "capacity_errors" => status.capacity_errors,
                "errors" => n_errors,
            );
        } else if status.bundles_created > 0 {
            slog::info!(
                opctx.log,
                "created {} support bundles requested by the current sitrep",
                status.bundles_created;
                "sitrep_id" => %sitrep.id(),
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
            );
        } else if status.total_bundles_requested > 0 {
            slog::debug!(
                opctx.log,
                "all support bundles requested by the current sitrep \
                 already exist";
                "sitrep_id" => %sitrep.id(),
                "total_bundles_requested" => status.total_bundles_requested,
            );
        } else {
            slog::debug!(
                opctx.log,
                "current sitrep requests no support bundles";
                "sitrep_id" => %sitrep.id(),
            );
        }

        // We created some bundles, so let the collector know.
        if status.bundles_created > 0 {
            self.support_bundle_collector.activate();
        }

        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::prelude::*;
    use nexus_db_queries::db;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_schema::schema::alert::dsl as alert_dsl;
    use nexus_types::alert::AlertClass;
    use nexus_types::fm;
    use nexus_types::support_bundle::BundleDataSelection;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use omicron_uuid_kinds::SupportBundleUuid;

    async fn fetch_alert(
        datastore: &DataStore,
        alert: AlertUuid,
    ) -> Result<db::model::Alert, diesel::result::Error> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let result = alert_dsl::alert
            .filter(alert_dsl::id.eq(alert.into_untyped_uuid()))
            .select(db::model::Alert::as_select())
            .first_async(&*conn)
            .await;
        dbg!(result)
    }

    #[tokio::test]
    async fn test_alert_requests() {
        let logctx = dev::test_setup_log("test_alert_requests");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);

        let alert_dispatcher_activator = Activator::new();
        // We are going to check that the alert dispatcher task is activated, so
        // we must actually wire it up first.
        alert_dispatcher_activator.mark_wired_up().unwrap();

        let support_bundle_collector_activator = Activator::new();
        support_bundle_collector_activator.mark_wired_up().unwrap();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        // Initial activation should do nothing.
        let status = dbg!(task.actually_activate(opctx).await);
        assert_eq!(status, Status::NoSitrep);

        // Now, create a new sitrep with alert requests.
        let sitrep1_id = SitrepUuid::new_v4();
        let alert1_id = AlertUuid::new_v4();
        let case1_id = CaseUuid::new_v4();
        let mut case1 = fm::Case {
            id: case1_id,
            created_sitrep_id: sitrep1_id,
            closed_sitrep_id: None,
            de: fm::DiagnosisEngineKind::PowerShelf,
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
            comment: "my great case".to_string(),
        };
        case1
            .alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert1_id,
                class: AlertClass::TestFoo,
                requested_sitrep_id: sitrep1_id,
                payload: serde_json::json!({}),
            })
            .unwrap();
        let sitrep1 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case1.clone()).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep1_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep 1".to_string(),
                    time_created: Utc::now(),
                },
                cases,
            }
        };

        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep1_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                sitrep1,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        let Status::Executed { sitrep_id, alerts, .. } = status else {
            panic!("rendezvous should have executed, as there is a sitrep");
        };
        assert_eq!(sitrep_id, sitrep1_id);
        assert_eq!(
            alerts,
            AlertStats {
                total_alerts_requested: 1,
                current_sitrep_alerts_requested: 1,
                alerts_created: 1,
                errors: Vec::new()
            }
        );
        let db_alert1 = fetch_alert(&datastore, alert1_id)
            .await
            .expect("alert1 must have been created");
        assert_eq!(db_alert1.class, db::model::AlertClass::TestFoo);
        assert_eq!(
            db_alert1.case_id.map(|id| id.into()),
            Some(case1_id),
            "alert1 should have case_id set to case1"
        );

        // Now, create a second sitrep sitrep with more alert requests.
        let sitrep2_id = SitrepUuid::new_v4();
        let alert2_id = AlertUuid::new_v4();
        let alert3_id = AlertUuid::new_v4();
        // Make a new case with its own alert request.
        let case2_id = CaseUuid::new_v4();
        let mut case2 = fm::Case {
            id: case2_id,
            created_sitrep_id: sitrep1_id,
            closed_sitrep_id: None,
            de: fm::DiagnosisEngineKind::PowerShelf,
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
            comment: "my other great case".to_string(),
        };
        case2
            .alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert2_id,
                class: AlertClass::TestFooBar,
                requested_sitrep_id: sitrep2_id,
                payload: serde_json::json!({}),
            })
            .unwrap();
        // Also, add a second alert request to the existing case.
        case1
            .alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert3_id,
                class: AlertClass::TestFooBaz,
                requested_sitrep_id: sitrep2_id,
                payload: serde_json::json!({}),
            })
            .unwrap();
        let sitrep2 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case1.clone()).unwrap();
            cases.insert_unique(case2.clone()).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep2_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: Some(sitrep1_id),
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep 2".to_string(),
                    time_created: Utc::now(),
                },
                cases,
            }
        };

        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep2_id,
                    version: 2,
                    time_made_current: Utc::now(),
                },
                sitrep2,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        let Status::Executed { sitrep_id, alerts, .. } = status else {
            panic!("rendezvous should have executed, as there is a sitrep");
        };
        assert_eq!(sitrep_id, sitrep2_id);
        assert_eq!(
            alerts,
            AlertStats {
                total_alerts_requested: 3,
                current_sitrep_alerts_requested: 2,
                alerts_created: 2,
                errors: Vec::new()
            }
        );

        let db_alert1 = fetch_alert(&datastore, alert1_id)
            .await
            .expect("alert1 must have been created");
        assert_eq!(db_alert1.class, db::model::AlertClass::TestFoo);
        assert_eq!(
            db_alert1.case_id.map(|id| id.into()),
            Some(case1_id),
            "alert1 should have case_id set to case1"
        );
        let db_alert2 = fetch_alert(&datastore, alert2_id)
            .await
            .expect("alert2 must have been created");
        assert_eq!(db_alert2.class, db::model::AlertClass::TestFooBar);
        assert_eq!(
            db_alert2.case_id.map(|id| id.into()),
            Some(case2_id),
            "alert2 should have case_id set to case2"
        );
        let db_alert3 = fetch_alert(&datastore, alert3_id)
            .await
            .expect("alert3 must have been created");
        assert_eq!(db_alert3.class, db::model::AlertClass::TestFooBaz);
        assert_eq!(
            db_alert3.case_id.map(|id| id.into()),
            Some(case1_id),
            "alert3 should have case_id set to case1"
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Create a sled with zpools and debug datasets so support bundle
    /// allocation can succeed.
    async fn create_sled_with_zpools(
        datastore: &DataStore,
        opctx: &OpContext,
        pool_count: usize,
    ) {
        use nexus_db_model::Generation;
        use nexus_db_model::RendezvousDebugDataset;
        use nexus_db_model::SledBaseboard;
        use nexus_db_model::SledCpuFamily;
        use nexus_db_model::SledSystemHardware;
        use nexus_db_model::SledUpdate;
        use nexus_db_model::Zpool;
        use omicron_common::api::external::ByteCount;
        use omicron_uuid_kinds::BlueprintUuid;
        use omicron_uuid_kinds::DatasetUuid;
        use omicron_uuid_kinds::PhysicalDiskUuid;
        use omicron_uuid_kinds::SledUuid;
        use omicron_uuid_kinds::ZpoolUuid;

        let sled_id = SledUuid::new_v4();
        let sled = SledUpdate::new(
            sled_id,
            "[::1]:0".parse().unwrap(),
            0,
            SledBaseboard {
                serial_number: format!("test-sled-{sled_id}"),
                part_number: "test-part".to_string(),
                revision: 0,
            },
            SledSystemHardware {
                is_scrimlet: false,
                usable_hardware_threads: 4,
                usable_physical_ram: ByteCount::from(1024 * 1024).into(),
                reservoir_size: ByteCount::from(0).into(),
                cpu_family: SledCpuFamily::AmdMilan,
            },
            uuid::Uuid::new_v4(),
            Generation::new(),
        );
        datastore.sled_upsert(sled).await.unwrap();

        let blueprint_id = BlueprintUuid::new_v4();
        for _ in 0..pool_count {
            let zpool_id = ZpoolUuid::new_v4();
            let zpool = Zpool::new(
                zpool_id,
                sled_id,
                PhysicalDiskUuid::new_v4(),
                ByteCount::from(0).into(),
            );
            datastore.zpool_insert(opctx, zpool).await.unwrap();

            let dataset = RendezvousDebugDataset::new(
                DatasetUuid::new_v4(),
                zpool_id,
                blueprint_id,
            );
            datastore
                .debug_dataset_insert_if_not_exists(opctx, dataset)
                .await
                .unwrap();
        }
    }

    async fn fetch_support_bundle(
        datastore: &DataStore,
        bundle_id: SupportBundleUuid,
    ) -> Result<db::model::SupportBundle, diesel::result::Error> {
        use nexus_db_schema::schema::support_bundle::dsl as sb_dsl;
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        sb_dsl::support_bundle
            .filter(sb_dsl::id.eq(bundle_id.into_untyped_uuid()))
            .select(db::model::SupportBundle::as_select())
            .first_async(&*conn)
            .await
    }

    #[tokio::test]
    async fn test_support_bundle_requests() {
        let logctx = dev::test_setup_log("test_support_bundle_requests");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);

        let alert_dispatcher_activator = Activator::new();
        alert_dispatcher_activator.mark_wired_up().unwrap();
        let support_bundle_collector_activator = Activator::new();
        support_bundle_collector_activator.mark_wired_up().unwrap();

        let nexus_id = OmicronZoneUuid::new_v4();
        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            nexus_id,
        );

        // Create zpools/datasets so support bundle allocation can succeed.
        create_sled_with_zpools(&datastore, &opctx, 3).await;

        // Build a sitrep with a case containing a support bundle request.
        let sitrep1_id = SitrepUuid::new_v4();
        let case1_id = CaseUuid::new_v4();
        let bundle1_id = SupportBundleUuid::new_v4();

        let mut case1 = fm::Case {
            id: case1_id,
            created_sitrep_id: sitrep1_id,
            closed_sitrep_id: None,
            de: fm::DiagnosisEngineKind::PowerShelf,
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
            comment: "case with support bundle request".to_string(),
        };
        case1
            .support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle1_id,
                requested_sitrep_id: sitrep1_id,

                data_selection: BundleDataSelection::all(),
            })
            .unwrap();

        let sitrep1 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case1.clone()).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep1_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep 1".to_string(),
                    time_created: Utc::now(),
                },
                cases,
            }
        };

        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep1_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                sitrep1,
            ))))
            .unwrap();

        // First activation: should create the support bundle.
        let status = dbg!(task.actually_activate(opctx).await);
        let Status::Executed { sitrep_id, support_bundles, .. } = status else {
            panic!("rendezvous should have executed, as there is a sitrep");
        };
        assert_eq!(sitrep_id, sitrep1_id);
        assert_eq!(support_bundles.total_bundles_requested, 1);
        assert_eq!(support_bundles.current_sitrep_bundles_requested, 1);
        assert_eq!(support_bundles.bundles_created, 1);
        assert!(support_bundles.errors.is_empty());
        assert_eq!(support_bundles.capacity_errors, 0);

        // The bundle should exist in the database in Collecting state.
        let db_bundle = fetch_support_bundle(&datastore, bundle1_id)
            .await
            .expect("bundle1 must have been created");
        assert_eq!(db_bundle.state, db::model::SupportBundleState::Collecting,);
        assert_eq!(db_bundle.fm_case_id.map(|id| id.into()), Some(case1_id),);

        // The collector should have been activated.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                support_bundle_collector_activator.activated(),
            )
            .await
            .is_ok(),
            "collector should be activated when bundles are created"
        );

        // Second activation: same sitrep, bundle already exists (conflict
        // is no-op).
        let status = dbg!(task.actually_activate(opctx).await);
        let Status::Executed { support_bundles, .. } = status else {
            panic!("rendezvous should have executed");
        };
        assert_eq!(support_bundles.bundles_created, 0);
        assert_eq!(support_bundles.total_bundles_requested, 1);

        // Build a second sitrep adding another bundle request.
        let sitrep2_id = SitrepUuid::new_v4();
        let bundle2_id = SupportBundleUuid::new_v4();
        case1
            .support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle2_id,
                requested_sitrep_id: sitrep2_id,

                data_selection: BundleDataSelection::all(),
            })
            .unwrap();

        let sitrep2 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case1.clone()).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep2_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: Some(sitrep1_id),
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep 2".to_string(),
                    time_created: Utc::now(),
                },
                cases,
            }
        };

        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep2_id,
                    version: 2,
                    time_made_current: Utc::now(),
                },
                sitrep2,
            ))))
            .unwrap();

        // Third activation: should create only the new bundle.
        let status = dbg!(task.actually_activate(opctx).await);
        let Status::Executed { support_bundles, .. } = status else {
            panic!("rendezvous should have executed");
        };
        assert_eq!(support_bundles.total_bundles_requested, 2);
        assert_eq!(support_bundles.current_sitrep_bundles_requested, 1);
        assert_eq!(support_bundles.bundles_created, 1);
        assert!(support_bundles.errors.is_empty());

        // Verify the second bundle exists.
        let db_bundle2 = fetch_support_bundle(&datastore, bundle2_id)
            .await
            .expect("bundle2 must have been created");
        assert_eq!(db_bundle2.state, db::model::SupportBundleState::Collecting,);

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_support_bundle_requests_capacity_error() {
        let logctx =
            dev::test_setup_log("test_support_bundle_requests_capacity_error");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);

        let alert_dispatcher_activator = Activator::new();
        alert_dispatcher_activator.mark_wired_up().unwrap();
        let support_bundle_collector_activator = Activator::new();
        support_bundle_collector_activator.mark_wired_up().unwrap();

        let nexus_id = OmicronZoneUuid::new_v4();
        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            nexus_id,
        );

        // Do NOT create any zpools/datasets, so bundle allocation will fail
        // due to insufficient capacity.

        let sitrep_id = SitrepUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let bundle_id = SupportBundleUuid::new_v4();

        let mut case = fm::Case {
            id: case_id,
            created_sitrep_id: sitrep_id,
            closed_sitrep_id: None,
            de: fm::DiagnosisEngineKind::PowerShelf,
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
            comment: "case with no capacity".to_string(),
        };
        case.support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle_id,
                requested_sitrep_id: sitrep_id,

                data_selection: BundleDataSelection::all(),
            })
            .unwrap();

        let sitrep = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep no capacity".to_string(),
                    time_created: Utc::now(),
                },
                cases,
            }
        };

        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                sitrep,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        let Status::Executed { support_bundles, .. } = status else {
            panic!("rendezvous should have executed");
        };
        assert_eq!(support_bundles.capacity_errors, 1);
        assert_eq!(support_bundles.bundles_created, 0);
        assert_eq!(support_bundles.total_bundles_requested, 1);

        // The bundle should NOT exist in the database.
        let result = fetch_support_bundle(&datastore, bundle_id).await;
        assert!(
            result.is_err(),
            "bundle should not exist when capacity is exhausted"
        );

        // The collector should NOT have been activated (no bundles created).
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                support_bundle_collector_activator.activated(),
            )
            .await
            .is_err(),
            "collector should not be activated when no bundles were created"
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
