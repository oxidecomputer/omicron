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
use nexus_db_queries::db::DataStore;
use nexus_types::fm::Sitrep;
use nexus_types::fm::SitrepVersion;
use nexus_types::fm::case::AlertRequest;
use nexus_types::internal_api::background::FmAlertStats as AlertStats;
use nexus_types::internal_api::background::FmRendezvousStatus as Status;
use omicron_common::api::external::Error;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

pub struct FmRendezvous {
    datastore: Arc<DataStore>,
    sitrep_watcher: watch::Receiver<CurrentSitrep>,
    alert_dispatcher: Activator,
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
    ) -> Self {
        Self { datastore, sitrep_watcher: rx, alert_dispatcher }
    }

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        let Some(sitrep) = self.sitrep_watcher.borrow_and_update().clone()
        else {
            return Status::NoSitrep;
        };

        // TODO(eliza): as we start doing other things (i.e. requesting support
        // bundles, updating problems), consider spawning these in their own tasks...
        let alerts = self.create_requested_alerts(&sitrep, opctx).await;

        Status::Executed { sitrep_id: sitrep.1.id(), alerts }
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
        // (not allocating) but insert one at a time?
        for (case_id, req) in sitrep.alerts_requested() {
            let &AlertRequest { id, requested_sitrep_id, class, ref payload } =
                req;
            status.total_alerts_requested += 1;
            if requested_sitrep_id == sitrep.id() {
                status.current_sitrep_alerts_requested += 1;
            }
            let class = class.into();
            match self
                .datastore
                .alert_create(&opctx, id, class, payload.clone())
                .await
            {
                // Alert already exists, that's fine.
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
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;

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

        let (sitrep_tx, mut sitrep_rx) = watch::channel(None);

        let alert_dispatcher_activator = Activator::new();
        // We are going to check that the alert dispatcher task is activated, so
        // we must actually wire it up first.
        alert_dispatcher_activator.mark_wired_up().unwrap();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
        );

        // Initial activation should do nothing.
        let status = dbg!(task.actually_activate(opctx).await);
        assert_eq!(status, Status::NoSitrep);

        // Now, create a new sitrep with alert requests.
        let sitrep1_id = SitrepUuid::new_v4();
        let alert1_id = AlertUuid::new_v4();
        let mut case1 = fm::Case {
            id: CaseUuid::new_v4(),
            created_sitrep_id: sitrep1_id,
            closed_sitrep_id: None,
            de: fm::DiagnosisEngineKind::PowerShelf,
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
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
        let Status::Executed { sitrep_id, alerts } = status else {
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

        // Now, create a second sitrep sitrep with more alert requests.
        let sitrep2_id = SitrepUuid::new_v4();
        let alert2_id = AlertUuid::new_v4();
        let alert3_id = AlertUuid::new_v4();
        // Make a new case with its own alert request.
        let mut case2 = fm::Case {
            id: CaseUuid::new_v4(),
            created_sitrep_id: sitrep1_id,
            closed_sitrep_id: None,
            de: fm::DiagnosisEngineKind::PowerShelf,
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
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
        let Status::Executed { sitrep_id, alerts } = status else {
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
        let db_alert2 = fetch_alert(&datastore, alert2_id)
            .await
            .expect("alert2 must have been created");
        assert_eq!(db_alert2.class, db::model::AlertClass::TestFooBar);
        let db_alert3 = fetch_alert(&datastore, alert3_id)
            .await
            .expect("alert3 must have been created");
        assert_eq!(db_alert3.class, db::model::AlertClass::TestFooBaz);

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
