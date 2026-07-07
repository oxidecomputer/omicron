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
use nexus_db_queries::db::datastore::FmRendezvousAlertCreateError;
use nexus_db_queries::db::datastore::FmSupportBundleCreateError;
use nexus_db_queries::db::datastore::MarkerGcResult;
use nexus_db_queries::db::datastore::SupportBundleCreateParams;
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
    sitrep_loader: Activator,
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
        sitrep_loader: Activator,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        Self {
            datastore,
            sitrep_watcher: rx,
            alert_dispatcher,
            support_bundle_collector,
            sitrep_loader,
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
        // GC of `rendezvous_*_created` marker rows runs as peer ops, not as a
        // tail of creation: the sweep's safety does not depend on any creation
        // loop completing (see the `fm_rendezvous_gc` module docs), and running
        // it as its own op lets the operator see GC timing and outcomes
        // separately from creation.
        let alert_marker_gc = self.spawn_op(
            &sitrep,
            opctx,
            "sweeping alert creation markers",
            Self::gc_alert_markers,
        );
        let support_bundle_marker_gc = self.spawn_op(
            &sitrep,
            opctx,
            "sweeping support bundle creation markers",
            Self::gc_support_bundle_markers,
        );

        const TASKS_SHOULDNT_FAIL: &str = "\
            rendezvous op tasks should never return a `JoinError`. Nexus is \
            compiled with `panic = \"abort\"`, so if the spawned task has \
            panicked the whole process should already have panicked. and, \
            we never abort the tasks here, so we will never see a `JoinError` \
            indicating that they were aborted.
        ";

        let status = Status {
            sitrep_id: Some(sitrep.1.id()),
            alerts: alerts.await.expect(TASKS_SHOULDNT_FAIL),
            support_bundles: support_bundles.await.expect(TASKS_SHOULDNT_FAIL),
            ereport_marking: marking.await.expect(TASKS_SHOULDNT_FAIL),
            alert_marker_gc: alert_marker_gc.await.expect(TASKS_SHOULDNT_FAIL),
            support_bundle_marker_gc: support_bundle_marker_gc
                .await
                .expect(TASKS_SHOULDNT_FAIL),
        };

        // If a guarded-create op observed that our sitrep is stale, the db
        // already has a newer current sitrep than the one we have loaded. Poke
        // the sitrep loader so that it picks up the new sitrep promptly, rather
        // than on its next periodic activation; the loader in turn re-activates
        // this task with the fresh sitrep.
        if status.stale_sitrep_detected() {
            self.sitrep_loader.activate();
        }

        status
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
        let expected_alert_generation = sitrep.metadata.alert_generation;

        // Count the request set up front, so the totals describe the sitrep
        // itself even if the loop below aborts partway through on a stale
        // sitrep.
        status.total_alerts_requested = sitrep.alerts_requested().count();
        status.current_sitrep_alerts_requested = sitrep
            .alerts_requested()
            .filter(|(_, req)| req.requested_sitrep_id == sitrep.id())
            .count();

        // XXX(eliza): is it better to allocate all of these into a big array
        // and do a single `INSERT INTO` query, or iterate over them one by one
        // (not allocating) but insert one at a time? Note that a batched insert
        // would require changes to `SitrepGuardedInsert`.
        for (case_id, req) in sitrep.alerts_requested() {
            let &AlertRequest { id, class, .. } = req;
            match self
                .datastore
                .fm_rendezvous_alert_create(
                    &opctx,
                    req,
                    case_id,
                    expected_alert_generation,
                )
                .await
            {
                Ok(_) => {
                    status.alerts_created += 1;
                }
                Err(FmRendezvousAlertCreateError::AlreadyCreated) => {
                    status.alerts_already_existed += 1;
                }
                Err(FmRendezvousAlertCreateError::StaleSitrep) => {
                    // The current sitrep in the database has moved past ours.
                    // Abort the rest of this activation; a fresher one will
                    // pick up where we left off.
                    slog::info!(
                        opctx.log,
                        "aborting alert rendezvous: sitrep is stale";
                        "case_id" => %case_id,
                        "alert_id" => %id,
                        "alert_class" => %class,
                        "expected_alert_generation" => expected_alert_generation,
                    );
                    status.stale_sitrep = true;
                    break;
                }
                Err(FmRendezvousAlertCreateError::Database(e)) => {
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
            }
        }

        let n_errors = status.errors.len();
        if status.stale_sitrep {
            slog::info!(
                opctx.log,
                "alert rendezvous aborted: sitrep stale relative to current";
                "sitrep_id" => %sitrep.id(),
                "expected_alert_generation" => expected_alert_generation,
                "alerts_created" => status.alerts_created,
                "alerts_already_existed" => status.alerts_already_existed,
                "errors" => n_errors,
            );
        } else if n_errors > 0 {
            slog::warn!(
                opctx.log,
                "created {} alerts requested by the current sitrep, but \
                 {n_errors} alerts could not be created!",
                status.alerts_created;
                "sitrep_id" => %sitrep.id(),
                "total_alerts_requested" => status.total_alerts_requested,
                "alerts_created" => status.alerts_created,
                "alerts_already_existed" => status.alerts_already_existed,
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
                "alerts_already_existed" => status.alerts_already_existed,
            );
        } else if status.total_alerts_requested > 0 {
            slog::debug!(
                opctx.log,
                "all alerts requested by the current sitrep already exist";
                "sitrep_id" => %sitrep.id(),
                "total_alerts_requested" => status.total_alerts_requested,
                "alerts_created" => status.alerts_created,
                "alerts_already_existed" => status.alerts_already_existed,
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
                    Some(ereport.id)
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

        let expected_support_bundle_generation =
            sitrep.metadata.support_bundle_generation;

        // Count the request set up front, so the totals describe the sitrep
        // itself even if the loop below aborts partway through on a stale
        // sitrep.
        status.total_bundles_requested =
            sitrep.support_bundles_requested().count();
        status.current_sitrep_bundles_requested = sitrep
            .support_bundles_requested()
            .filter(|(_, req)| req.requested_sitrep_id == sitrep.id())
            .count();

        // Like alert creation (see `create_requested_alerts`), we iterate all
        // bundle requests in the sitrep, not just new ones.
        // `SitrepGuardedInsert` makes each insert idempotent and short-circuits
        // on the `rendezvous_support_bundle_created` marker.
        for (case, req) in sitrep.support_bundles_requested() {
            let case_id = case.id;
            let de = case.metadata.de;
            let fm::case::SupportBundleRequest {
                id: bundle_id,
                requested_sitrep_id: _,
                data_selection,
                comment,
            } = req;
            let bundle_id = *bundle_id;

            // Fall back to a generic reason for now if the diagnosis engine
            // left the comment empty.
            //
            // TODO(#9672): We should generally expect that the DE will provide
            // a comment, and just use it without a fallback. The DE name and
            // case ID should be recorded in bundle metadata via a separate
            // path, reading directly from the existing
            // `support_bundle.fm_case_id` column and maybe a new
            // `support_bundle.fm_diagnosis_engine_name` column.
            let reason = if comment.is_empty() {
                format!(
                    "Requested by {de:?} diagnosis engine for case {case_id}"
                )
            } else {
                comment.clone()
            };
            match self
                .datastore
                .fm_rendezvous_support_bundle_create(
                    &opctx,
                    bundle_id,
                    case_id,
                    expected_support_bundle_generation,
                    SupportBundleCreateParams {
                        reason: &reason,
                        nexus_id: self.nexus_id,
                        user_comment: None,
                        data_selection: data_selection.clone(),
                    },
                )
                .await
            {
                Ok(_) => {
                    status.bundles_created += 1;
                }
                Err(FmSupportBundleCreateError::AlreadyCreated) => {
                    // A prior activation already created this support bundle.
                    // The marker row in `rendezvous_support_bundle_created`
                    // prevents resurrection.
                    status.bundles_already_existed += 1;
                }
                Err(FmSupportBundleCreateError::StaleSitrep) => {
                    // The current sitrep in the database has moved past ours.
                    // Abort the rest of this activation; a fresher one will
                    // pick up where we left off.
                    slog::warn!(
                        opctx.log,
                        "aborting support bundle rendezvous: sitrep is stale";
                        "case_id" => %case_id,
                        "bundle_id" => %bundle_id,
                        "expected_support_bundle_generation" =>
                            expected_support_bundle_generation,
                    );
                    status.stale_sitrep = true;
                    break;
                }
                Err(
                    e @ (FmSupportBundleCreateError::TooManyBundles
                    | FmSupportBundleCreateError::Other(_)),
                ) => {
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
            }
        }

        let n_errors = status.errors.len();
        if status.stale_sitrep {
            slog::warn!(
                opctx.log,
                "support bundle rendezvous aborted: sitrep stale relative to \
                 current";
                "sitrep_id" => %sitrep.id(),
                "expected_support_bundle_generation" =>
                    expected_support_bundle_generation,
                "bundles_created" => status.bundles_created,
                "bundles_already_existed" => status.bundles_already_existed,
                "errors" => n_errors,
            );
        } else if n_errors > 0 {
            slog::warn!(
                opctx.log,
                "created {} support bundles requested by the current sitrep, \
                 but {n_errors} support bundles could not be created!",
                status.bundles_created;
                "sitrep_id" => %sitrep.id(),
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
                "bundles_already_existed" => status.bundles_already_existed,
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
                "bundles_already_existed" => status.bundles_already_existed,
            );
        } else if status.total_bundles_requested > 0 {
            slog::debug!(
                opctx.log,
                "all support bundles requested by the current sitrep \
                 already exist";
                "sitrep_id" => %sitrep.id(),
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
                "bundles_already_existed" => status.bundles_already_existed,
            );
        } else {
            slog::debug!(
                opctx.log,
                "current sitrep requests no support bundles";
                "sitrep_id" => %sitrep.id(),
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
            );
        }

        // We created some bundles, so let the collector know.
        if status.bundles_created > 0 {
            self.support_bundle_collector.activate();
        }

        status
    }

    /// Sweep the `rendezvous_alert_created` marker table, using this
    /// activation's sitrep to decide which rows are still needed.
    async fn gc_alert_markers(
        self,
        sitrep: CurrentSitrep,
        opctx: OpContext,
    ) -> MarkerGcStatus {
        let (_, ref sitrep) = *sitrep;
        let result = self
            .datastore
            .fm_rendezvous_alert_marker_gc(
                &opctx,
                sitrep.id(),
                sitrep.metadata.alert_generation,
            )
            .await;
        Self::marker_gc_status(&opctx.log, result)
    }

    /// Sweep the `rendezvous_support_bundle_created` marker table, using this
    /// activation's sitrep to decide which rows are still needed.
    async fn gc_support_bundle_markers(
        self,
        sitrep: CurrentSitrep,
        opctx: OpContext,
    ) -> MarkerGcStatus {
        let (_, ref sitrep) = *sitrep;
        let result = self
            .datastore
            .fm_rendezvous_support_bundle_marker_gc(
                &opctx,
                sitrep.id(),
                sitrep.metadata.support_bundle_generation,
            )
            .await;
        Self::marker_gc_status(&opctx.log, result)
    }

    /// Map the outcome of a `rendezvous_*_created` GC sweep into a
    /// [`MarkerGcStatus`].
    fn marker_gc_status(
        log: &slog::Logger,
        result: Result<MarkerGcResult, Error>,
    ) -> MarkerGcStatus {
        match result {
            Ok(MarkerGcResult { rows_deleted, batches }) => {
                if rows_deleted > 0 {
                    slog::debug!(log, "GC swept {rows_deleted} marker row(s)",);
                }
                MarkerGcStatus { rows_deleted, batches, errors: Vec::new() }
            }
            Err(e) => {
                slog::warn!(
                    log,
                    "marker GC failed";
                    "error" => InlineErrorChain::new(&e),
                );
                MarkerGcStatus {
                    rows_deleted: 0,
                    batches: 0,
                    errors: vec![InlineErrorChain::new(&e).to_string()],
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
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
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::CaseEreportUuid;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::EreporterRestartUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use omicron_uuid_kinds::SupportBundleUuid;

    /// Activators needed by `FmRendezvous`, pre-wired for testing.
    struct TestActivators {
        alert_dispatcher_activator: Activator,
        support_bundle_collector_activator: Activator,
        sitrep_loader_activator: Activator,
    }

    fn make_activators() -> TestActivators {
        let activators = TestActivators {
            alert_dispatcher_activator: Activator::new(),
            support_bundle_collector_activator: Activator::new(),
            sitrep_loader_activator: Activator::new(),
        };
        activators.alert_dispatcher_activator.mark_wired_up().unwrap();
        activators.support_bundle_collector_activator.mark_wired_up().unwrap();
        activators.sitrep_loader_activator.mark_wired_up().unwrap();
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
            sitrep_loader_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
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
            facts: iddqd::IdOrdMap::new(),
        };
        case1
            .alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert1_id,
                class: AlertClass::TestFoo,
                version: 0,
                requested_sitrep_id: sitrep1_id,
                payload: serde_json::json!({}),
                comment: String::new(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };

        // The sitrep-guard combinator in `fm_rendezvous_alert_create` consults
        // `fm_sitrep_history` to check that the rendezvous task's expected
        // generation matches the current one. Insert the sitrep so it shows up
        // in the DB before activating.
        datastore
            .fm_sitrep_insert(opctx, sitrep1.clone(), None)
            .await
            .expect("inserted sitrep1");

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

        let Status { sitrep_id, alerts, alert_marker_gc, .. } =
            dbg!(task.actually_activate(opctx).await);
        assert_eq!(sitrep_id, Some(sitrep1_id));
        assert_eq!(
            alerts.details,
            AlertCreationStatus {
                total_alerts_requested: 1,
                current_sitrep_alerts_requested: 1,
                alerts_created: 1,
                alerts_already_existed: 0,
                stale_sitrep: false,
                errors: Vec::new(),
            }
        );
        // The single `rendezvous_alert_created` marker row we just
        // inserted for `alert1` was stamped with the current sitrep's
        // `alert_generation`, so its `created_at_generation` *equals* the
        // current sitrep's `alert_generation`. The strict-inequality predicate
        // (`created_at_generation < sitrep_alert_generation`) excludes it.
        // (We don't assert `batches`: the GC op runs concurrently with alert
        // creation, so how many marker rows its pages see is racy.)
        assert_eq!(alert_marker_gc.details.rows_deleted, 0);
        assert!(alert_marker_gc.details.errors.is_empty());
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
            facts: iddqd::IdOrdMap::new(),
        };
        case2
            .alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert2_id,
                class: AlertClass::TestFooBar,
                version: 0,
                requested_sitrep_id: sitrep2_id,
                payload: serde_json::json!({}),
                comment: String::new(),
            })
            .unwrap();
        // Also, add a second alert request to the existing case.
        case1
            .alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert3_id,
                class: AlertClass::TestFooBaz,
                version: 0,
                requested_sitrep_id: sitrep2_id,
                payload: serde_json::json!({}),
                comment: String::new(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };

        datastore
            .fm_sitrep_insert(opctx, sitrep2.clone(), None)
            .await
            .expect("inserted sitrep2");

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
        let Status { sitrep_id, alerts, alert_marker_gc, .. } = status;
        assert_eq!(sitrep_id, Some(sitrep2_id));
        assert_eq!(
            alerts.details,
            AlertCreationStatus {
                total_alerts_requested: 3,
                current_sitrep_alerts_requested: 2,
                alerts_created: 2,
                alerts_already_existed: 1,
                stale_sitrep: false,
                errors: Vec::new(),
            }
        );
        // As above: sitrep1 and sitrep2 carry the same alert_generation, so
        // every marker's `created_at_generation` equals it and nothing is
        // swept.
        assert_eq!(alert_marker_gc.details.rows_deleted, 0);
        assert!(alert_marker_gc.details.errors.is_empty());

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

        // Neither activation saw a stale sitrep, so the sitrep loader should
        // not have been poked.
        sitrep_loader_activator.assert_not_activated(
            "sitrep loader should not be activated without a stale sitrep",
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// A stale activation (one whose sitrep's `alert_generation` is behind the
    /// latest sitrep in `fm_sitrep_history`) should be rejected in
    /// `fm_rendezvous_alert_create`. The rendezvous task is responsible for
    /// translating that `Conflict` into `stale_sitrep = true`, breaking out of
    /// the alert loop without interrupting anything else.
    #[tokio::test]
    async fn test_alert_requests_aborted_when_sitrep_is_stale() {
        let logctx = dev::test_setup_log(
            "test_alert_requests_aborted_when_sitrep_is_stale",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);
        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
            sitrep_loader_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        // The stale activation's sitrep carries two alert requests and stamps
        // `alert_generation = 1`. Using two requests (rather than one) lets the
        // test confirm that the reported totals count every request in the
        // sitrep, not just how far the loop got before it aborted on the first
        // insert.
        let stale_sitrep_id = SitrepUuid::new_v4();
        let stale_alert_id = AlertUuid::new_v4();
        let stale_alert2_id = AlertUuid::new_v4();
        let stale_case_id = CaseUuid::new_v4();
        let stale_case = {
            let mut c = fm::Case {
                id: stale_case_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: stale_sitrep_id,
                    closed_sitrep_id: None,
                    de: fm::DiagnosisEngineKind::PowerShelf,
                    comment: "stale case".to_string(),
                },
                alerts_requested: iddqd::IdOrdMap::new(),
                ereports: iddqd::IdOrdMap::new(),
                support_bundles_requested: iddqd::IdOrdMap::new(),
                facts: iddqd::IdOrdMap::new(),
            };
            for id in [stale_alert_id, stale_alert2_id] {
                c.alerts_requested
                    .insert_unique(fm::case::AlertRequest {
                        id,
                        class: AlertClass::TestFoo,
                        version: 0,
                        requested_sitrep_id: stale_sitrep_id,
                        payload: serde_json::json!({}),
                        comment: String::new(),
                    })
                    .unwrap();
            }
            c
        };
        let stale_sitrep = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(stale_case).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: stale_sitrep_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "stale sitrep".to_string(),
                    time_created: Utc::now(),
                    next_inv_min_time_started: Utc::now(),
                    alert_generation: Generation::from_u32(1),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };
        datastore
            .fm_sitrep_insert(opctx, stale_sitrep.clone(), None)
            .await
            .expect("inserted stale sitrep");

        // A newer sitrep whose alert generation is ahead. Once it lands in
        // `fm_sitrep_history`, the database considers the stale activation's
        // sitrep out of date.
        let current_sitrep_id = SitrepUuid::new_v4();
        let current_sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: current_sitrep_id,
                inv_collection_id: CollectionUuid::new_v4(),
                parent_sitrep_id: Some(stale_sitrep_id),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "current sitrep".to_string(),
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                alert_generation: Generation::from_u32(2),
                support_bundle_generation: Generation::new(),
            },
            cases: iddqd::IdOrdMap::new(),
            ereports_by_id: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, current_sitrep, None)
            .await
            .expect("inserted current sitrep");

        // Hand the stale sitrep to the rendezvous task. Attempting to insert
        // the first alert should result in an error because the sitrep is out
        // of date, and rendezvous execution should be aborted before
        // attempting to insert the other alert.
        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: stale_sitrep_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                stale_sitrep,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        assert_eq!(status.sitrep_id, Some(stale_sitrep_id));
        assert_eq!(
            status.alerts.details,
            AlertCreationStatus {
                // The totals count the sitrep's request set, not loop
                // progress: both requests are reported even though the loop
                // aborted on the first.
                total_alerts_requested: 2,
                current_sitrep_alerts_requested: 2,
                alerts_created: 0,
                alerts_already_existed: 0,
                stale_sitrep: true,
                errors: Vec::new(),
            }
        );
        // Neither alert row may have been inserted: the sitrep-guard fired
        // before the first request's inner INSERT could run, and the abort
        // skipped the second request entirely.
        for id in [stale_alert_id, stale_alert2_id] {
            assert_matches!(
                fetch_alert(&datastore, id).await,
                Err(diesel::result::Error::NotFound)
            );
        }
        // Support bundle processing should still have run: the stale-sitrep
        // outcome aborts only the alert loop, not the whole activation.
        assert_matches!(
            status.support_bundles.result,
            OpResult::Executed { .. }
        );
        // Detecting a stale sitrep should poke the sitrep loader so the
        // newer sitrep is picked up promptly.
        sitrep_loader_activator.assert_activated(
            "sitrep loader should be activated when a stale sitrep is detected",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// A stale activation whose sitrep's `support_bundle_generation`
    /// is behind the latest sitrep should be rejected by the sitrep-guard
    /// combinator inside `fm_rendezvous_support_bundle_create`. The rendezvous
    /// task translates that `StaleSitrep` error into `stale_sitrep = true`,
    /// breaks out of the bundle loop without inserting any rows, and still
    /// runs the alert op.
    #[tokio::test]
    async fn test_bundle_requests_aborted_when_sitrep_is_stale() {
        let logctx = dev::test_setup_log(
            "test_bundle_requests_aborted_when_sitrep_is_stale",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (sitrep_tx, sitrep_rx) = watch::channel(None);
        let TestActivators {
            alert_dispatcher_activator,
            support_bundle_collector_activator,
            sitrep_loader_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        // Provision a free dataset so `fm_rendezvous_support_bundle_create`
        // reaches the sitrep-guard (the free-dataset lookup precedes the
        // guarded insert; without one we'd hit a capacity error before the
        // staleness check).
        create_sled_with_zpools(&datastore, &opctx, 1).await;

        // The stale activation's sitrep: carries two support bundle requests
        // and stamps the initial `support_bundle_generation`. Two requests
        // (rather than one) pin the totals' semantics: they count the
        // sitrep's full request set, not how far the loop got before
        // aborting on the first insert.
        let stale_sitrep_id = SitrepUuid::new_v4();
        let stale_bundle_id = SupportBundleUuid::new_v4();
        let stale_bundle2_id = SupportBundleUuid::new_v4();
        let stale_case_id = CaseUuid::new_v4();
        let stale_case = {
            let mut c = fm::Case {
                id: stale_case_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: stale_sitrep_id,
                    closed_sitrep_id: None,
                    de: fm::DiagnosisEngineKind::PowerShelf,
                    comment: "stale case".to_string(),
                },
                alerts_requested: iddqd::IdOrdMap::new(),
                ereports: iddqd::IdOrdMap::new(),
                support_bundles_requested: iddqd::IdOrdMap::new(),
                facts: iddqd::IdOrdMap::new(),
            };
            for id in [stale_bundle_id, stale_bundle2_id] {
                c.support_bundles_requested
                    .insert_unique(fm::case::SupportBundleRequest {
                        id,
                        requested_sitrep_id: stale_sitrep_id,
                        data_selection: BundleDataSelection::all(),
                        comment: String::new(),
                    })
                    .unwrap();
            }
            c
        };
        let stale_sitrep = {
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(stale_case).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: stale_sitrep_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "stale sitrep".to_string(),
                    time_created: Utc::now(),
                    next_inv_min_time_started: Utc::now(),
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };
        datastore
            .fm_sitrep_insert(opctx, stale_sitrep.clone(), None)
            .await
            .expect("inserted stale sitrep");

        // A newer sitrep whose support bundle generation is ahead. Once it
        // lands in `fm_sitrep_history`, the database considers the stale
        // activation's sitrep out of date.
        let current_sitrep_id = SitrepUuid::new_v4();
        let current_sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: current_sitrep_id,
                inv_collection_id: CollectionUuid::new_v4(),
                parent_sitrep_id: Some(stale_sitrep_id),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "current sitrep".to_string(),
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                alert_generation: Generation::new(),
                support_bundle_generation: Generation::new().next(),
            },
            cases: iddqd::IdOrdMap::new(),
            ereports_by_id: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, current_sitrep, None)
            .await
            .expect("inserted current sitrep");

        // Hand the stale sitrep to the rendezvous task. Its first support
        // bundle request should trip the sitrep-guard combinator and surface
        // as a `Conflict`, which the task translates into `stale_sitrep = true`
        // and breaks out of the bundle loop before the second request is
        // attempted.
        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: stale_sitrep_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                stale_sitrep,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        assert_eq!(status.sitrep_id, Some(stale_sitrep_id));
        assert_eq!(
            status.support_bundles.details,
            SupportBundleCreationStatus {
                // The totals count the sitrep's request set, not loop
                // progress: both requests are reported even though the loop
                // aborted on the first.
                total_bundles_requested: 2,
                current_sitrep_bundles_requested: 2,
                bundles_created: 0,
                bundles_already_existed: 0,
                stale_sitrep: true,
                errors: Vec::new(),
            }
        );
        // Neither bundle row may have been inserted: the sitrep-guard fired
        // before the first request's inner INSERT could run, and the abort
        // skipped the second request entirely.
        for id in [stale_bundle_id, stale_bundle2_id] {
            assert_matches!(
                fetch_support_bundle(&datastore, id).await,
                Err(diesel::result::Error::NotFound)
            );
        }
        // Alert processing should still have run: the stale-sitrep outcome
        // aborts only the bundle loop, not the whole activation.
        assert_matches!(status.alerts.result, OpResult::Executed { .. });
        // Detecting a stale sitrep should poke the sitrep loader so the
        // newer sitrep is picked up promptly.
        sitrep_loader_activator.assert_activated(
            "sitrep loader should be activated when a stale sitrep is detected",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
    /// Paginate through all unseen ereports, collecting every row.
    ///
    /// This bypasses the production `ereports_list_unmarked` (which filters
    /// by `known_ereport_classes`) because the test wants to verify that
    /// rendezvous marked *every* ereport in a sitrep, regardless of class.
    async fn list_all_unseen_ereports(
        datastore: &DataStore,
    ) -> Vec<db::model::Ereport> {
        use nexus_db_schema::schema::ereport::dsl;
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        dsl::ereport
            .filter(dsl::marked_seen_in.is_null())
            .filter(dsl::time_deleted.is_null())
            .order_by((dsl::restart_id, dsl::ena))
            .select(db::model::Ereport::as_select())
            .load_async(&*conn)
            .await
            .expect("failed to list unseen ereports")
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
        let unseen = list_all_unseen_ereports(datastore).await;
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
        let unseen = list_all_unseen_ereports(datastore).await;
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
            sitrep_loader_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        let restart_id = EreporterRestartUuid::new_v4();
        let collector_id = OmicronZoneUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let time_collected = Utc::now();
        let reporter = Reporter::Sp {
            sp_type: nexus_types::inventory::SpType::Sled,
            slot: 0,
        };

        let ereport1_id =
            ereport_types::EreportId { restart_id, ena: ereport_types::Ena(2) };
        let ereport1_data = EreportData {
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.one".to_string()),
            report: serde_json::json!({"info": "first ereport"}),
        };
        let ereport2_id =
            ereport_types::EreportId { restart_id, ena: ereport_types::Ena(3) };
        let ereport2_data = EreportData {
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.two".to_string()),
            report: serde_json::json!({"info": "second ereport"}),
        };
        let ereport3_id =
            ereport_types::EreportId { restart_id, ena: ereport_types::Ena(4) };
        let ereport3_data = EreportData {
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.three".to_string()),
            report: serde_json::json!({"info": "third ereport"}),
        };
        datastore
            .ereports_insert(
                &opctx,
                restart_id,
                time_collected,
                collector_id,
                rack_id,
                reporter,
                vec![
                    (ereport1_id.ena, ereport1_data.clone()),
                    (ereport2_id.ena, ereport2_data.clone()),
                    (ereport3_id.ena, ereport3_data.clone()),
                ],
            )
            .await
            .expect("failed to insert ereports");

        // Verify all three ereports are currently unseen.
        assert_ereports_unseen(
            &datastore,
            opctx,
            &[ereport1_id, ereport2_id, ereport3_id],
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
                        ereport1_id,
                        time_collected,
                        collector_id,
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
                        ereport2_id,
                        time_collected,
                        collector_id,
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
                facts: iddqd::IdOrdMap::new(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
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
            &[ereport1_id, ereport2_id],
            sitrep1_id,
        )
        .await;
        assert_ereports_unseen(&datastore, opctx, &[ereport3_id]).await;

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
            &[ereport1_id, ereport2_id],
            sitrep1_id,
        )
        .await;
        assert_ereports_unseen(&datastore, opctx, &[ereport3_id]).await;

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
            sitrep_loader_activator,
        } = make_activators();

        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
            OmicronZoneUuid::new_v4(),
        );

        let restart_id = EreporterRestartUuid::new_v4();
        let collector_id = OmicronZoneUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let reporter = Reporter::Sp {
            sp_type: nexus_types::inventory::SpType::Sled,
            slot: 1,
        };
        let time_collected = Utc::now();
        let ereport1_id =
            ereport_types::EreportId { restart_id, ena: ereport_types::Ena(2) };
        let ereport1_data = EreportData {
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.one".to_string()),
            report: serde_json::json!({"info": "first ereport"}),
        };
        let ereport2_id =
            ereport_types::EreportId { restart_id, ena: ereport_types::Ena(3) };
        let ereport2_data = EreportData {
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.two".to_string()),
            report: serde_json::json!({"info": "second ereport"}),
        };
        let ereport3_id =
            ereport_types::EreportId { restart_id, ena: ereport_types::Ena(4) };
        let ereport3_data = EreportData {
            part_number: Some("9130000019".to_string()),
            serial_number: Some("BRM420069".to_string()),
            class: Some("ereport.test.three".to_string()),
            report: serde_json::json!({"info": "third ereport"}),
        };
        datastore
            .ereports_insert(
                opctx,
                restart_id,
                time_collected,
                collector_id,
                rack_id,
                reporter,
                vec![
                    (ereport1_id.ena, ereport1_data.clone()),
                    (ereport2_id.ena, ereport2_data.clone()),
                    (ereport3_id.ena, ereport3_data.clone()),
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
                        ereport1_id,
                        time_collected,
                        collector_id,
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
                facts: iddqd::IdOrdMap::new(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
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
        assert_ereports_seen_in(&datastore, opctx, &[ereport1_id], sitrep1_id)
            .await;
        assert_ereports_unseen(&datastore, opctx, &[ereport2_id, ereport3_id])
            .await;

        // Sitrep 2: carries forward ereport1 AND adds ereport2 and
        // ereport3
        let sitrep2_id = SitrepUuid::new_v4();
        let ereport1_seen = fm::ereport::Ereport {
            id: ereport1_id,
            time_collected,
            collector_id,
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
                        ereport2_id,
                        time_collected,
                        collector_id,
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
                        ereport3_id,
                        time_collected,
                        collector_id,
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
                facts: iddqd::IdOrdMap::new(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
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
        assert_ereports_seen_in(&datastore, opctx, &[ereport1_id], sitrep1_id)
            .await;
        assert_ereports_seen_in(
            &datastore,
            opctx,
            &[ereport2_id, ereport3_id],
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
        use omicron_uuid_kinds::RackUuid;
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
            RackUuid::new_v4(),
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
            sitrep_loader_activator,
        } = make_activators();

        let nexus_id = OmicronZoneUuid::new_v4();
        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
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
            facts: iddqd::IdOrdMap::new(),
        };
        case1
            .support_bundles_requested
            .insert_unique(fm::case::SupportBundleRequest {
                id: bundle1_id,
                requested_sitrep_id: sitrep1_id,
                data_selection: BundleDataSelection::all(),
                comment: "test support bundle".to_string(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };

        // The sitrep-guard combinator in `fm_rendezvous_support_bundle_create`
        // consults `fm_sitrep_history` to check that the rendezvous task's
        // expected generation matches the current one.
        // Insert the sitrep so it shows up in the DB before activating.
        datastore
            .fm_sitrep_insert(opctx, sitrep1.clone(), None)
            .await
            .expect("inserted sitrep1");

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
        assert_eq!(
            status.support_bundles.details,
            SupportBundleCreationStatus {
                total_bundles_requested: 1,
                current_sitrep_bundles_requested: 1,
                bundles_created: 1,
                bundles_already_existed: 0,
                stale_sitrep: false,
                errors: Vec::new(),
            }
        );

        // The bundle-side GC op ran against the real
        // `rendezvous_support_bundle_created` / `fm_support_bundle_request`
        // tables; an error here would mean the identifiers the generic query
        // splices for `SupportBundle` don't match the real schema.
        assert!(
            status.support_bundle_marker_gc.details.errors.is_empty(),
            "support bundle marker GC should not have failed: {:?}",
            status.support_bundle_marker_gc.details.errors,
        );

        // The bundle should exist in the database in Collecting state.
        let db_bundle = fetch_support_bundle(&datastore, bundle1_id)
            .await
            .expect("bundle1 must have been created");
        assert_eq!(db_bundle.state, db::model::SupportBundleState::Collecting,);
        assert_eq!(db_bundle.fm_case_id.map(|id| id.into()), Some(case1_id),);
        assert_eq!(
            db_bundle.reason_for_creation, "test support bundle",
            "DE-provided comment should be used as reason_for_creation",
        );

        // The collector should have been activated.
        support_bundle_collector_activator.assert_activated(
            "collector should be activated when bundles are created",
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };

        datastore
            .fm_sitrep_insert(opctx, sitrep2.clone(), None)
            .await
            .expect("inserted sitrep2");

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

        // Activation with sitrep2: should create only the new bundle. The
        // carried-forward request is satisfied by its marker, so it counts as
        // already-existed rather than created.
        let status = dbg!(task.actually_activate(opctx).await);
        assert_eq!(
            status.support_bundles.details,
            SupportBundleCreationStatus {
                total_bundles_requested: 2,
                current_sitrep_bundles_requested: 1,
                bundles_created: 1,
                bundles_already_existed: 1,
                stale_sitrep: false,
                errors: Vec::new(),
            }
        );

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
            sitrep_loader_activator,
        } = make_activators();

        let nexus_id = OmicronZoneUuid::new_v4();
        let mut task = FmRendezvous::new(
            datastore.clone(),
            sitrep_rx,
            alert_dispatcher_activator.clone(),
            support_bundle_collector_activator.clone(),
            sitrep_loader_activator.clone(),
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
            facts: iddqd::IdOrdMap::new(),
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
                    alert_generation: Generation::new(),
                    support_bundle_generation: Generation::new(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };

        datastore
            .fm_sitrep_insert(opctx, sitrep.clone(), None)
            .await
            .expect("inserted sitrep");

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
        support_bundle_collector_activator.assert_not_activated(
            "collector should not be activated when no bundles were created",
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
