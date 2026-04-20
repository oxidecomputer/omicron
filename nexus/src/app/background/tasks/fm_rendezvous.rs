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
use nexus_db_queries::db::datastore::SupportBundleCreateParams;
use nexus_db_queries::db::datastore::SupportBundleProvenance;
use nexus_types::fm;
use nexus_types::fm::case::AlertRequest;
use nexus_types::internal_api::background::FmRendezvousStatus as Status;
use nexus_types::internal_api::background::fm_rendezvous::*;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct FmRendezvous {
    datastore: Arc<DataStore>,
    sitrep_watcher: watch::Receiver<Option<CurrentSitrep>>,
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
        rx: watch::Receiver<Option<CurrentSitrep>>,
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
            return Status::default();
        };

        let alerts = self.spawn_op(
            &sitrep,
            opctx,
            "creating requested alerts",
            Self::create_requested_alerts,
        );
        let support_bundles = self.spawn_op(
            &sitrep,
            opctx,
            "creating requested support bundles",
            Self::create_requested_support_bundles,
        );
        let marking = self.spawn_op(
            &sitrep,
            opctx,
            "marking ereports as seen",
            Self::mark_ereports_seen,
        );

        const TASKS_SHOULDNT_FAIL: &str = "\
            rendezvous op tasks should never return a `JoinError`. Nexus is \
            compiled with `panic = \"abort\"`, so if the spawned task has \
            panicked the whole process should already have panicked. and, \
            we never abort the tasks here, so we will never see a `JoinError` \
            indicating that they were aborted.
        ";

        Status {
            sitrep_id: Some(sitrep.1.id()),
            alerts: alerts.await.expect(TASKS_SHOULDNT_FAIL),
            support_bundles: support_bundles.await.expect(TASKS_SHOULDNT_FAIL),
            ereport_marking: marking.await.expect(TASKS_SHOULDNT_FAIL),
        }
    }

    fn spawn_op<F>(
        &self,
        sitrep: &CurrentSitrep,
        opctx: &OpContext,
        opname: impl ToString,
        op: impl Fn(Self, CurrentSitrep, OpContext) -> F,
    ) -> tokio::task::JoinHandle<OpStatus<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let this = self.clone();
        let sitrep = sitrep.clone();
        let opctx = opctx.child(
            [
                ("sitrep_id".to_string(), sitrep.1.id().to_string()),
                ("rendezvous_op".to_string(), opname.to_string()),
            ]
            .into_iter()
            .collect(),
        );
        let op = op(this, sitrep, opctx);
        tokio::task::spawn(async move {
            let start = chrono::Utc::now();
            let details = op.await;
            let end = chrono::Utc::now();
            OpStatus { result: OpResult::Executed { start, end }, details }
        })
    }

    async fn create_requested_alerts(
        self,
        sitrep: CurrentSitrep,
        opctx: OpContext,
    ) -> AlertCreationStatus {
        let (_, ref sitrep) = *sitrep;
        let mut status = AlertCreationStatus::default();

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

    async fn mark_ereports_seen(
        self,
        sitrep: CurrentSitrep,
        opctx: OpContext,
    ) -> EreportMarkingStatus {
        const BATCH_SIZE: usize = 1000;

        let (_, ref sitrep) = *sitrep;
        let mut status = EreportMarkingStatus {
            batch_size: BATCH_SIZE,
            total_ereports_in_sitrep: sitrep.ereports_by_id.len(),
            ..EreportMarkingStatus::default()
        };

        let mut ereport_ids = sitrep
            .ereports_by_id
            .iter()
            // it is unfortunately necessary to clone the arc here, for if we do
            // not, the async block in `activate` is not valid for the requisite
            // lifetime, as the iterator crosses an await point in the loop
            // below, and this is sadly all too much for the compiler's little
            // brain to handle.
            //
            // this is just a reference-count bump, so it doesn't really matter,
            // especially as the alternative would be to `collect()` all the
            // ereports into a giant `Vec`, which would make me much sadder.
            .cloned()
            .filter_map(|ereport| {
                if ereport.marked_seen_in.is_none() {
                    Some(*ereport.id())
                } else {
                    None
                }
            })
            .peekable();

        while ereport_ids.peek().is_some() {
            let mut n_unmarked = 0;
            status.batches += 1;
            match self
                .datastore
                .ereports_mark_seen(
                    &opctx,
                    sitrep.id(),
                    ereport_ids
                        .by_ref()
                        .take(BATCH_SIZE)
                        .inspect(|_| n_unmarked += 1),
                )
                .await
            {
                Ok(n_marked) => {
                    status.ereports_marked_seen += n_marked;
                    if n_marked != 0 {
                        slog::debug!(
                            opctx.log,
                            "marked {n_marked} of {n_unmarked} ereports as seen"
                        );
                    }
                }
                Err(err) => {
                    slog::error!(
                        opctx.log,
                        "failed to mark {n_unmarked} ereports as seen";
                        &err
                    );
                    status.errors.push(InlineErrorChain::new(&err).to_string());
                }
            }
            status.ereports_not_marked_in_sitrep += n_unmarked;
        }

        status
    }

    async fn create_requested_support_bundles(
        self,
        sitrep: CurrentSitrep,
        opctx: OpContext,
    ) -> SupportBundleCreationStatus {
        let (_, ref sitrep) = *sitrep;
        let mut status = SupportBundleCreationStatus::default();

        // Like alert creation (see `create_requested_alerts`), we iterate all
        // bundle requests in the sitrep, not just new ones. Multiple Nexus
        // instances may run this concurrently. Bundle creation is idempotent,
        // so racing instances won't inadvertently create duplicates.
        //
        // TODO(#9592) Same stale-Nexus concern as alerts: if support bundle
        // requests are ever removed from sitreps (e.g. when a case is closed),
        // a lagging Nexus could re-create "zombie" bundles from an outdated
        // sitrep. Currently safe because requests are carried forward.
        for (case, req) in sitrep.support_bundles_requested() {
            let case_id = case.id;
            let de = case.metadata.de;
            let fm::case::SupportBundleRequest {
                id: bundle_id,
                requested_sitrep_id,
                data_selection,
                comment,
            } = req;
            let bundle_id = *bundle_id;

            status.total_bundles_requested += 1;
            if *requested_sitrep_id == sitrep.id() {
                status.current_sitrep_bundles_requested += 1;
            }

            // Add a generic reason if the diagnosis engine didn't provide one.
            // TODO: should the reason string _always_ include the DE's name
            // and/or the case ID?
            let reason = if comment.is_empty() {
                format!(
                    "Requested by {de:?} diagnosis engine for case {case_id}"
                )
            } else {
                comment.clone()
            };
            match self
                .datastore
                .support_bundle_create(
                    &opctx,
                    SupportBundleCreateParams {
                        provenance: SupportBundleProvenance::Fm {
                            id: bundle_id,
                            case_id,
                        },
                        reason: &reason,
                        nexus_id: self.nexus_id,
                        user_comment: None,
                        data_selection: data_selection.clone(),
                    },
                )
                .await
            {
                // Bundle already exists from a previous activation, idempotent.
                Err(Error::ObjectAlreadyExists { .. }) => {
                    slog::debug!(
                        opctx.log,
                        "support bundle already exists";
                        "case_id" => %case_id,
                        "bundle_id" => %bundle_id,
                    );
                }
                // Other errors, unexpected.
                Err(e) => {
                    slog::warn!(
                        opctx.log,
                        "failed to create requested support bundle";
                        "case_id" => %case_id,
                        "bundle_id" => %bundle_id,
                        "error" => %e,
                    );
                    status.errors.push(format!(
                        "support bundle {bundle_id} for case {case_id}: {e}",
                    ));
                }
                Ok(_) => status.bundles_created += 1,
            }
        }

        let n_errors = status.errors.len();
        if n_errors > 0 {
            slog::warn!(
                opctx.log,
                "created {} support bundles requested by the current \
                 sitrep, but {} requests could not be fulfilled!",
                status.bundles_created,
                n_errors;
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
                "errors" => n_errors,
            );
        } else if status.bundles_created > 0 {
            slog::info!(
                opctx.log,
                "created {} support bundles requested by the current sitrep",
                status.bundles_created;
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
            );
        } else if status.total_bundles_requested > 0 {
            slog::debug!(
                opctx.log,
                "all support bundles requested by the current sitrep \
                 already exist";
                "total_bundles_requested" => status.total_bundles_requested,
            );
        } else {
            slog::debug!(
                opctx.log,
                "current sitrep requests no support bundles"
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
    use nexus_types::fm::ereport::EreportData;
    use nexus_types::fm::ereport::Reporter;
    use nexus_types::support_bundle::BundleDataSelection;
    use omicron_common::api::external::DataPageParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::CaseEreportUuid;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::EreporterRestartUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use omicron_uuid_kinds::SupportBundleUuid;
    use std::num::NonZeroU32;

    /// Activators needed by `FmRendezvous`, pre-wired for testing.
    struct TestActivators {
        alert_dispatcher_activator: Activator,
        support_bundle_collector_activator: Activator,
    }

    fn make_activators() -> TestActivators {
        let activators = TestActivators {
            alert_dispatcher_activator: Activator::new(),
            support_bundle_collector_activator: Activator::new(),
        };
        activators.alert_dispatcher_activator.mark_wired_up().unwrap();
        activators.support_bundle_collector_activator.mark_wired_up().unwrap();
        activators
    }

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

        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        // Initial activation should do nothing.
        let status = dbg!(task.actually_activate(opctx).await);
        assert_eq!(status.sitrep_id, None);

        // Now, create a new sitrep with alert requests.
        let sitrep1_id = SitrepUuid::new_v4();
        let alert1_id = AlertUuid::new_v4();
        let case1_id = CaseUuid::new_v4();
        let mut case1 = fm::Case {
            id: case1_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: sitrep1_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: "my great case".to_string(),
            },
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
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
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id: Default::default(),
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

        let Status { sitrep_id, alerts, .. } =
            dbg!(task.actually_activate(opctx).await);
        assert_eq!(sitrep_id, Some(sitrep1_id));
        assert_eq!(
            alerts.details,
            AlertCreationStatus {
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
            metadata: fm::case::Metadata {
                created_sitrep_id: sitrep1_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: "my other great case".to_string(),
            },
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
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
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id: Default::default(),
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
        let Status { sitrep_id, alerts, .. } = status;
        assert_eq!(sitrep_id, Some(sitrep2_id));
        assert_eq!(
            alerts.details,
            AlertCreationStatus {
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

    /// Paginate through all unseen ereports, collecting every row.
    async fn list_all_unseen_ereports(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> Vec<db::model::Ereport> {
        let batch_size = NonZeroU32::new(100).unwrap();
        let mut all = Vec::new();
        let mut marker = None;
        loop {
            let page = datastore
                .ereports_list_unmarked(
                    opctx,
                    &DataPageParams {
                        marker: marker.as_ref(),
                        direction: dropshot::PaginationOrder::Ascending,
                        limit: batch_size,
                    },
                )
                .await
                .expect("failed to list unseen ereports");
            if page.is_empty() {
                break;
            }
            let last = page.last().unwrap();
            marker = Some((last.restart_id.into_untyped_uuid(), last.ena));
            all.extend(page);
        }
        all
    }

    /// Asserts that each of the provided ereport IDs has been marked as
    /// seen in the provided sitrep in the database, and that none of them
    /// appear in the `ereports_list_unseen` query results.
    async fn assert_ereports_seen_in(
        datastore: &DataStore,
        opctx: &OpContext,
        ids: &[ereport_types::EreportId],
        sitrep_id: SitrepUuid,
    ) {
        let unseen = list_all_unseen_ereports(datastore, opctx).await;
        for &id in ids {
            let ereport =
                datastore.ereport_fetch(opctx, id).await.unwrap_or_else(|e| {
                    panic!("ereport {id} must exist in db: {e}")
                });
            assert_eq!(
                ereport.marked_seen_in.map(SitrepUuid::from),
                Some(sitrep_id),
                "ereport {id} should be marked seen in {sitrep_id}"
            );
            assert!(
                !unseen.iter().any(|e| e.id() == id),
                "ereport {id} is marked seen but still appears in \
                 the unseen list"
            );

            eprintln!("- ereport {id} is seen in {sitrep_id}");
        }
    }

    /// Asserts that each of the provided ereport IDs has NOT been marked
    /// as seen in the database (i.e. `marked_seen_in` is `None`), and
    /// that all of them appear in the `ereports_list_unseen` query
    /// results.
    async fn assert_ereports_unseen(
        datastore: &DataStore,
        opctx: &OpContext,
        ids: &[ereport_types::EreportId],
    ) {
        let unseen = list_all_unseen_ereports(datastore, opctx).await;
        for &id in ids {
            let ereport =
                datastore.ereport_fetch(opctx, id).await.unwrap_or_else(|e| {
                    panic!("ereport {id} must exist in db: {e}")
                });
            assert!(
                ereport.marked_seen_in.is_none(),
                "ereport {id} should not be marked seen, but has \
                 marked_seen_in = {:?}",
                ereport.marked_seen_in,
            );
            assert!(
                unseen.iter().any(|e| e.id() == id),
                "ereport {id} is not marked seen but is missing from \
                 the unseen list"
            );
            eprintln!("- ereport {id} is unseen");
        }
    }

    #[tokio::test]
    async fn test_mark_ereports_seen() {
        let logctx = dev::test_setup_log("test_mark_ereports_seen");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);

        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        let restart_id = EreporterRestartUuid::new_v4();
        let collector_id = OmicronZoneUuid::new_v4();
        let reporter = Reporter::Sp {
            sp_type: nexus_types::inventory::SpType::Sled,
            slot: 0,
        };

        let ereport1_data = EreportData {
            id: ereport_types::EreportId {
                restart_id,
                ena: ereport_types::Ena(2),
            },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.one".to_string()),
            report: serde_json::json!({"info": "first ereport"}),
        };
        let ereport2_data = EreportData {
            id: ereport_types::EreportId {
                restart_id,
                ena: ereport_types::Ena(3),
            },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.two".to_string()),
            report: serde_json::json!({"info": "second ereport"}),
        };
        let ereport3_data = EreportData {
            id: ereport_types::EreportId {
                restart_id,
                ena: ereport_types::Ena(4),
            },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.three".to_string()),
            report: serde_json::json!({"info": "third ereport"}),
        };
        datastore
            .ereports_insert(
                opctx,
                reporter,
                vec![
                    ereport1_data.clone(),
                    ereport2_data.clone(),
                    ereport3_data.clone(),
                ],
            )
            .await
            .expect("failed to insert ereports");

        // Verify all three ereports are currently unseen.
        assert_ereports_unseen(
            &datastore,
            opctx,
            &[ereport1_data.id, ereport2_data.id, ereport3_data.id],
        )
        .await;

        // Build a sitrep that references ereport1 and ereport2 (but NOT
        // ereport3)
        let sitrep1_id = SitrepUuid::new_v4();
        let case1_id = CaseUuid::new_v4();
        let case1 = {
            let mut ereports = iddqd::IdOrdMap::new();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::ereport::Ereport::new(
                        ereport1_data.clone(),
                        reporter,
                    )),
                    assigned_sitrep_id: sitrep1_id,
                    comment: "ereport 1 for case 1".to_string(),
                })
                .unwrap();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::ereport::Ereport::new(
                        ereport2_data.clone(),
                        reporter,
                    )),
                    assigned_sitrep_id: sitrep1_id,
                    comment: "ereport 2 for case 1".to_string(),
                })
                .unwrap();
            fm::Case {
                id: case1_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: sitrep1_id,
                    closed_sitrep_id: None,
                    de: fm::DiagnosisEngineKind::PowerShelf,
                    comment: "case with two ereports".to_string(),
                },
                ereports,
                alerts_requested: iddqd::IdOrdMap::new(),
                support_bundles_requested: iddqd::IdOrdMap::new(),
            }
        };

        let sitrep1 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case1.clone()).unwrap();
            let mut ereports_by_id = iddqd::IdOrdMap::new();
            for case in cases.iter() {
                ereports_by_id
                    .extend(case.ereports.iter().map(|ce| ce.ereport.clone()));
            }
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep1_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "sitrep with ereports 1 and 2".to_string(),
                    time_created: Utc::now(),
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id,
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

        // First activation should mark ereport1 and ereport2 as seen
        let Status { sitrep_id, ereport_marking, .. } =
            dbg!(task.actually_activate(opctx).await);
        assert_eq!(sitrep_id, Some(sitrep1_id));
        assert_eq!(
            ereport_marking.details.total_ereports_in_sitrep, 2,
            "sitrep should contain 2 ereports"
        );
        assert_eq!(
            ereport_marking.details.ereports_marked_seen, 2,
            "both ereports should have been newly marked as seen"
        );
        assert!(
            ereport_marking.details.errors.is_empty(),
            "there should be no errors: {:?}",
            ereport_marking.details.errors
        );

        // Verify the database records: ereport1 and ereport2 should have
        // `marked_seen_in` set to sitrep1_id, and ereport3 should still
        // be unseen.
        assert_ereports_seen_in(
            &datastore,
            opctx,
            &[ereport1_data.id, ereport2_data.id],
            sitrep1_id,
        )
        .await;
        assert_ereports_unseen(&datastore, opctx, &[ereport3_data.id]).await;

        // Second activation: ereport1 and ereport2 are already marked
        // seen in the sitrep, so they should NOT be marked again
        let Status { sitrep_id, ereport_marking, .. } =
            dbg!(task.actually_activate(opctx).await);
        assert_eq!(sitrep_id, Some(sitrep1_id));

        assert_eq!(
            ereport_marking.details.total_ereports_in_sitrep, 2,
            "sitrep still contains 2 ereports"
        );
        assert_eq!(
            ereport_marking.details.ereports_marked_seen, 0,
            "no ereports should be newly marked as seen on re-activation, \
             because both were already marked seen"
        );
        assert!(
            ereport_marking.details.errors.is_empty(),
            "there should be no errors on re-activation: {:?}",
            ereport_marking.details.errors
        );

        // Verify the database records haven't changed: ereport1 and ereport2
        // should still reference sitrep1_id, and ereport3 should still be
        // unseen.
        assert_ereports_seen_in(
            &datastore,
            opctx,
            &[ereport1_data.id, ereport2_data.id],
            sitrep1_id,
        )
        .await;
        assert_ereports_unseen(&datastore, opctx, &[ereport3_data.id]).await;

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Test that when a new sitrep adds ereport3 (previously unseen), only
    /// ereport3 gets marked, while ereport1 and ereport2 (already marked by
    /// the previous sitrep) are not re-marked.
    #[tokio::test]
    async fn test_mark_ereports_seen_across_sitreps() {
        let logctx =
            dev::test_setup_log("test_mark_ereports_seen_across_sitreps");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);

        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        let restart_id = EreporterRestartUuid::new_v4();
        let collector_id = OmicronZoneUuid::new_v4();
        let reporter = Reporter::Sp {
            sp_type: nexus_types::inventory::SpType::Sled,
            slot: 1,
        };

        let ereport1_data = EreportData {
            id: ereport_types::EreportId {
                restart_id,
                ena: ereport_types::Ena(2),
            },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.one".to_string()),
            report: serde_json::json!({"info": "first ereport"}),
        };
        let ereport2_data = EreportData {
            id: ereport_types::EreportId {
                restart_id,
                ena: ereport_types::Ena(3),
            },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.two".to_string()),
            report: serde_json::json!({"info": "second ereport"}),
        };
        let ereport3_data = EreportData {
            id: ereport_types::EreportId {
                restart_id,
                ena: ereport_types::Ena(4),
            },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.three".to_string()),
            report: serde_json::json!({"info": "third ereport"}),
        };
        datastore
            .ereports_insert(
                opctx,
                reporter,
                vec![
                    ereport1_data.clone(),
                    ereport2_data.clone(),
                    ereport3_data.clone(),
                ],
            )
            .await
            .expect("failed to insert ereports");

        //  Sitrep 1: references only ereport1
        let sitrep1_id = SitrepUuid::new_v4();
        let case1 = {
            let mut ereports = iddqd::IdOrdMap::new();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::ereport::Ereport::new(
                        ereport1_data.clone(),
                        reporter,
                    )),
                    assigned_sitrep_id: sitrep1_id,
                    comment: "ereport 1".to_string(),
                })
                .unwrap();
            fm::Case {
                id: CaseUuid::new_v4(),
                metadata: fm::case::Metadata {
                    created_sitrep_id: sitrep1_id,
                    closed_sitrep_id: None,
                    de: fm::DiagnosisEngineKind::PowerShelf,
                    comment: "case with ereport 1".to_string(),
                },
                ereports,
                alerts_requested: iddqd::IdOrdMap::new(),
                support_bundles_requested: iddqd::IdOrdMap::new(),
            }
        };

        let sitrep1 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case1.clone()).unwrap();
            let mut ereports_by_id = iddqd::IdOrdMap::new();
            for case in cases.iter() {
                ereports_by_id
                    .extend(case.ereports.iter().map(|ce| ce.ereport.clone()));
            }
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep1_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "sitrep 1: only ereport 1".to_string(),
                    time_created: Utc::now(),
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id,
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

        // Activate with sitrep 1 --- should mark only ereport1.
        let Status { ereport_marking, .. } =
            dbg!(task.actually_activate(opctx).await);
        assert_eq!(ereport_marking.details.ereports_marked_seen, 1);

        // Verify DB state after sitrep 1.
        assert_ereports_seen_in(
            &datastore,
            opctx,
            &[ereport1_data.id],
            sitrep1_id,
        )
        .await;
        assert_ereports_unseen(
            &datastore,
            opctx,
            &[ereport2_data.id, ereport3_data.id],
        )
        .await;

        // Sitrep 2: carries forward ereport1 AND adds ereport2 and
        // ereport3
        let sitrep2_id = SitrepUuid::new_v4();
        let ereport1_seen = fm::ereport::Ereport {
            data: ereport1_data.clone(),
            reporter,
            marked_seen_in: Some(sitrep1_id),
        };

        let case2 = {
            let mut ereports = iddqd::IdOrdMap::new();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: CaseEreportUuid::new_v4(),
                    ereport: Arc::new(ereport1_seen),
                    assigned_sitrep_id: sitrep1_id,
                    comment: "ereport 1 (carried forward)".to_string(),
                })
                .unwrap();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::ereport::Ereport::new(
                        ereport2_data.clone(),
                        reporter,
                    )),
                    assigned_sitrep_id: sitrep2_id,
                    comment: "ereport 2 newly added".to_string(),
                })
                .unwrap();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::ereport::Ereport::new(
                        ereport3_data.clone(),
                        reporter,
                    )),
                    assigned_sitrep_id: sitrep2_id,
                    comment: "ereport 3 newly added".to_string(),
                })
                .unwrap();
            fm::Case {
                id: CaseUuid::new_v4(),
                metadata: fm::case::Metadata {
                    created_sitrep_id: sitrep1_id,
                    closed_sitrep_id: None,
                    de: fm::DiagnosisEngineKind::PowerShelf,
                    comment: "case with all three ereports".to_string(),
                },
                ereports,
                alerts_requested: iddqd::IdOrdMap::new(),
                support_bundles_requested: iddqd::IdOrdMap::new(),
            }
        };

        let sitrep2 = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case2.clone()).unwrap();
            let mut ereports_by_id = iddqd::IdOrdMap::new();
            for case in cases.iter() {
                ereports_by_id
                    .extend(case.ereports.iter().map(|ce| ce.ereport.clone()));
            }
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep2_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: Some(sitrep1_id),
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "sitrep 2: all three ereports".to_string(),
                    time_created: Utc::now(),
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id,
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

        // Activate with sitrep 2 --- should mark ereport2 and ereport3, but
        // NOT re-mark ereport1 (it is already seen and filtered out before
        // the query).
        let Status { ereport_marking, .. } =
            dbg!(task.actually_activate(opctx).await);
        assert_eq!(
            ereport_marking.details.total_ereports_in_sitrep, 3,
            "sitrep2 contains all 3 ereports"
        );
        assert_eq!(
            ereport_marking.details.ereports_marked_seen, 2,
            "only ereport2 and ereport3 should be newly marked"
        );
        assert!(ereport_marking.details.errors.is_empty());
        assert_ereports_seen_in(
            &datastore,
            opctx,
            &[ereport1_data.id],
            sitrep1_id,
        )
        .await;
        assert_ereports_seen_in(
            &datastore,
            opctx,
            &[ereport2_data.id, ereport3_data.id],
            sitrep2_id,
        )
        .await;

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Create a sled with zpools and debug datasets so support bundle
    /// allocation can succeed.
    ///
    /// Note, this helper is similar to `TestSled`/`create_sled_and_zpools` in
    /// the `support_bundle` datastore tests. Consider consolidating them if
    /// more tests need similar setup.
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

        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
        } = make_activators();

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
            metadata: fm::case::Metadata {
                created_sitrep_id: sitrep1_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: "case with support bundle request".to_string(),
            },
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
        };
        case1
            .support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle1_id,
                requested_sitrep_id: sitrep1_id,
                data_selection: BundleDataSelection::all(),
                comment: "PSU removed — collecting diagnostics".to_string(),
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
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id: Default::default(),
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
        assert_eq!(status.sitrep_id, Some(sitrep1_id));
        let support_bundles = &status.support_bundles.details;
        assert_eq!(support_bundles.total_bundles_requested, 1);
        assert_eq!(support_bundles.current_sitrep_bundles_requested, 1);
        assert_eq!(support_bundles.bundles_created, 1);
        assert!(support_bundles.errors.is_empty());

        // The bundle should exist in the database in Collecting state.
        let db_bundle = fetch_support_bundle(&datastore, bundle1_id)
            .await
            .expect("bundle1 must have been created");
        assert_eq!(db_bundle.state, db::model::SupportBundleState::Collecting,);
        assert_eq!(db_bundle.fm_case_id.map(|id| id.into()), Some(case1_id),);
        assert_eq!(
            db_bundle.reason_for_creation,
            "PSU removed — collecting diagnostics",
            "DE-provided comment should be used as reason_for_creation",
        );

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

        // Build a second sitrep that carries forward the existing bundle
        // request from sitrep1 and adds a new one. The task should create
        // only the new bundle (the old one already exists and hits a
        // conflict, which is treated as a no-op).
        let sitrep2_id = SitrepUuid::new_v4();
        let bundle2_id = SupportBundleUuid::new_v4();
        case1
            .support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle2_id,
                requested_sitrep_id: sitrep2_id,
                data_selection: BundleDataSelection::all(),
                comment: String::new(),
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
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id: Default::default(),
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

        // Activation with sitrep2: should create only the new bundle.
        let status = dbg!(task.actually_activate(opctx).await);
        let support_bundles = &status.support_bundles.details;
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

        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
        } = make_activators();

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
            metadata: fm::case::Metadata {
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: "case with no capacity".to_string(),
            },
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
        };
        case.support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle_id,
                requested_sitrep_id: sitrep_id,
                data_selection: BundleDataSelection::all(),
                comment: String::new(),
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
                    next_inv_min_time_started: Utc::now(),
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep no capacity".to_string(),
                    time_created: Utc::now(),
                },
                cases,
                ereports_by_id: Default::default(),
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
        let support_bundles = &status.support_bundles.details;
        assert_eq!(support_bundles.errors.len(), 1);
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
