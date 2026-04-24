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

        let (ref version, ref current_sitrep) = *sitrep;
        let sitrep_version = i64::from(version.version);
        let sitrep_id = current_sitrep.id();

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

        let alerts = alerts.await.expect(TASKS_SHOULDNT_FAIL);
        let support_bundles = support_bundles.await.expect(TASKS_SHOULDNT_FAIL);
        let ereport_marking = marking.await.expect(TASKS_SHOULDNT_FAIL);

        // Advance the rendezvous progress tracker to this sitrep's version, but
        // only if every subtask completed without error. If there were any
        // errors, we'll retry on the next activation.
        //
        // Note: holding the tracker back on errors is a conservative choice. If
        // we advanced the tracker regardless, then `SitrepVersionGuardedInsert`
        // would prevent future retries from executing, meaning that some
        // requested alerts or support bundles would potentially be missed. The
        // consequence is that, if there is a persistent failure here and the
        // tracker never advances, the tombstone sweeps below may never be able
        // to clean up resources.
        let any_errors = !alerts.details.errors.is_empty()
            || !support_bundles.details.errors.is_empty()
            || !ereport_marking.details.errors.is_empty();
        let latest_processed_sitrep_version = if any_errors {
            slog::warn!(
                &opctx.log,
                "skipping rendezvous progress tracker advance: subtasks \
                 recorded per-request errors";
                "sitrep_version" => sitrep_version,
                "sitrep_id" => ?sitrep_id,
                "alert_errors" => alerts.details.errors.len(),
                "support_bundle_errors" => support_bundles.details.errors.len(),
                "ereport_marking_errors" => ereport_marking.details.errors.len(),
            );
            None
        } else {
            match self
                .datastore
                .fm_rendezvous_progress_advance(opctx, sitrep_version)
                .await
            {
                Ok(v) => Some(v),
                Err(e) => {
                    slog::warn!(
                        &opctx.log,
                        "failed to advance rendezvous progress tracker";
                        "sitrep_version" => sitrep_version,
                        "sitrep_id" => ?sitrep_id,
                        "error" => InlineErrorChain::new(&e).to_string(),
                    );
                    None
                }
            }
        };

        let status = Status {
            sitrep_id: Some(sitrep_id),
            alerts,
            support_bundles,
            ereport_marking,
            latest_processed_sitrep_version,
        };

        // Sweep eligible tombstoned alerts and support bundles.
        match self.datastore.fm_alert_tombstone_sweep(opctx).await {
            Ok(n) => {
                if n > 0 {
                    slog::info!(
                        &opctx.log,
                        "swept alert tombstones";
                        "count" => n,
                    );
                }
            }
            Err(e) => slog::warn!(
                &opctx.log,
                "alert tombstone sweep failed";
                "error" => InlineErrorChain::new(&e).to_string(),
            ),
        }
        match self.datastore.fm_support_bundle_tombstone_sweep(opctx).await {
            Ok(n) => {
                if n > 0 {
                    slog::info!(
                        &opctx.log,
                        "swept support bundle tombstones";
                        "count" => n,
                    );
                }
            }
            Err(e) => slog::warn!(
                &opctx.log,
                "support bundle tombstone sweep failed";
                "error" => InlineErrorChain::new(&e).to_string(),
            ),
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
        let (ref version, ref sitrep) = *sitrep;
        let sitrep_version = i64::from(version.version);
        let mut status = AlertCreationStatus::default();

        // XXX(eliza): is it better to allocate all of these into a big array
        // and do a single `INSERT INTO` query, or iterate over them one by one
        // (not allocating) but insert one at a time? Note that a batched insert
        // would need to use `ON CONFLICT DO NOTHING` rather than checking for
        // `Conflict` errors from individual inserts, since multiple Nexus
        // instances may run this task concurrently.
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
                    Some(sitrep_version),
                )
                .await
            {
                // Alert already exists --- this is expected, since multiple
                // Nexus instances may run this task concurrently for the same
                // sitrep, or a previous activation may have partially completed.
                Err(Error::ObjectAlreadyExists { .. }) => {}
                Err(Error::Conflict { ref message })
                    if message.external_message().contains("stale sitrep") =>
                {
                    // The FM sitrep-version guard rejected the insert
                    // because the rendezvous progress tracker is already
                    // ahead of this activation's sitrep. Some other (more
                    // current) Nexus has moved past us; skipping the insert
                    // is the correct behavior, not an error.
                    slog::info!(
                        opctx.log,
                        "alert_create skipped: stale sitrep version";
                        "sitrep_version" => sitrep_version,
                        "case_id" => %case_id,
                        "alert_id" => %id,
                        "alert_class" => %class,
                    );
                    status.stale_sitrep += 1;
                }
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
                Ok(_alert) => {
                    slog::debug!(
                        opctx.log,
                        "published alert";
                        "case_id" => %case_id,
                        "alert_id" => %id,
                        "alert_class" => %class,
                    );
                    status.alerts_created += 1;
                }
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
                "stale_sitrep" => status.stale_sitrep,
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
                "stale_sitrep" => status.stale_sitrep,
            );
        } else if status.total_alerts_requested > 0 {
            // No errors, nothing newly created, but we did process requests.
            // Branch the human-readable message on whether any inserts were
            // skipped because the sitrep was stale: those alerts may not
            // exist yet (a more current Nexus might be racing with this one
            // and hasn't created them either), so saying "already exist"
            // would be misleading.
            if status.stale_sitrep > 0 {
                slog::debug!(
                    opctx.log,
                    "no alerts created from the current sitrep \
                     (some skipped due to stale sitrep version)";
                    "sitrep_id" => %sitrep.id(),
                    "total_alerts_requested" => status.total_alerts_requested,
                    "alerts_created" => status.alerts_created,
                    "stale_sitrep" => status.stale_sitrep,
                );
            } else {
                slog::debug!(
                    opctx.log,
                    "all alerts requested by the current sitrep already exist";
                    "sitrep_id" => %sitrep.id(),
                    "total_alerts_requested" => status.total_alerts_requested,
                    "alerts_created" => status.alerts_created,
                    "stale_sitrep" => status.stale_sitrep,
                );
            }
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
        let (ref version, ref sitrep) = *sitrep;
        let sitrep_version = i64::from(version.version);
        let mut status = SupportBundleCreationStatus::default();

        // Like alert creation (see `create_requested_alerts`), we iterate all
        // bundle requests in the sitrep, not just new ones. Multiple Nexus
        // instances may run this concurrently. Bundle creation is idempotent,
        // so racing instances won't inadvertently create duplicates.
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
                .support_bundle_create(
                    &opctx,
                    SupportBundleCreateParams {
                        provenance: SupportBundleProvenance::Fm {
                            id: bundle_id,
                            case_id,
                            sitrep_version,
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
                // The FM sitrep-version guard rejected the insert because the
                // rendezvous progress tracker is already ahead of this
                // activation's sitrep. Some other (more current) Nexus has
                // moved past us; skipping the insert is the correct behavior,
                // not an error.
                Err(Error::Conflict { ref message })
                    if message.external_message().contains("stale sitrep") =>
                {
                    slog::info!(
                        opctx.log,
                        "support_bundle_create skipped: stale sitrep version";
                        "sitrep_version" => sitrep_version,
                        "case_id" => %case_id,
                        "bundle_id" => %bundle_id,
                    );
                    status.stale_sitrep += 1;
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
                "stale_sitrep" => status.stale_sitrep,
                "errors" => n_errors,
            );
        } else if status.bundles_created > 0 {
            slog::info!(
                opctx.log,
                "created {} support bundles requested by the current sitrep",
                status.bundles_created;
                "total_bundles_requested" => status.total_bundles_requested,
                "bundles_created" => status.bundles_created,
                "stale_sitrep" => status.stale_sitrep,
            );
        } else if status.total_bundles_requested > 0 {
            // No errors, nothing newly created, but we did process requests.
            // Same reasoning as `create_requested_alerts`'s aggregate log:
            // if any inserts were skipped because the sitrep was stale,
            // those bundles may not exist yet, so "already exist" would be
            // misleading.
            if status.stale_sitrep > 0 {
                slog::debug!(
                    opctx.log,
                    "no support bundles created from the current sitrep \
                     (some skipped due to stale sitrep version)";
                    "total_bundles_requested" => status.total_bundles_requested,
                    "stale_sitrep" => status.stale_sitrep,
                );
            } else {
                slog::debug!(
                    opctx.log,
                    "all support bundles requested by the current sitrep \
                     already exist";
                    "total_bundles_requested" => status.total_bundles_requested,
                    "stale_sitrep" => status.stale_sitrep,
                );
            }
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
                stale_sitrep: 0,
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
                comment: String::new(),
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
                stale_sitrep: 0,
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
            db_bundle.reason_for_creation, "test support bundle",
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
    async fn test_rendezvous_activation_advances_tracker() {
        let logctx =
            dev::test_setup_log("test_rendezvous_activation_advances_tracker");
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

        // Tracker bootstraps at 0 before any activation. `advance(0)` is
        // the natural read idiom: GREATEST(0, 0) = 0.
        assert_eq!(
            datastore.fm_rendezvous_progress_advance(opctx, 0).await.unwrap(),
            0,
            "tracker should bootstrap at 0",
        );

        // Publish a sitrep with version=3 and no cases.
        let sitrep_id = SitrepUuid::new_v4();
        let sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: sitrep_id,
                inv_collection_id: CollectionUuid::new_v4(),
                parent_sitrep_id: None,
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep for tracker advance".to_string(),
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
            },
            cases: iddqd::IdOrdMap::new(),
            ereports_by_id: Default::default(),
        };
        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep_id,
                    version: 3,
                    time_made_current: Utc::now(),
                },
                sitrep,
            ))))
            .unwrap();

        // One full activation against version=3.
        let _ = dbg!(task.actually_activate(opctx).await);

        // The tracker should have advanced to 3.
        let v =
            datastore.fm_rendezvous_progress_advance(opctx, 0).await.unwrap();
        assert_eq!(
            v, 3,
            "tracker should advance to the activated sitrep's version",
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_rendezvous_activation_runs_sweep() {
        let logctx =
            dev::test_setup_log("test_rendezvous_activation_runs_sweep");
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

        // Build sitrep v=1 carrying a single case, and persist it (which
        // assigns it a row in `fm_sitrep_history`). The sweep query joins
        // `fm_case` to `fm_sitrep_history`, so the case must really exist
        // in the DB at this version for the case-lifetime check to be
        // meaningful.
        let sitrep1_id = SitrepUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let sitrep1 = {
            let case = fm::Case {
                id: case_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: sitrep1_id,
                    closed_sitrep_id: None,
                    de: fm::DiagnosisEngineKind::PowerShelf,
                    comment: "case in v1".to_string(),
                },
                alerts_requested: iddqd::IdOrdMap::new(),
                ereports: iddqd::IdOrdMap::new(),
                support_bundles_requested: iddqd::IdOrdMap::new(),
            };
            let mut cases = iddqd::IdOrdMap::new();
            cases.insert_unique(case).unwrap();
            fm::Sitrep {
                metadata: fm::SitrepMetadata {
                    id: sitrep1_id,
                    inv_collection_id: CollectionUuid::new_v4(),
                    parent_sitrep_id: None,
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "test sitrep v1".to_string(),
                    time_created: Utc::now(),
                    next_inv_min_time_started: Utc::now(),
                },
                cases,
                ereports_by_id: Default::default(),
            }
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep1)
            .await
            .expect("insert sitrep v1");

        // Read back the assigned version so we can later send it through
        // the watch channel.
        use nexus_db_schema::schema::fm_sitrep_history::dsl as history_dsl;
        let v1: i64 = {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            history_dsl::fm_sitrep_history
                .filter(
                    history_dsl::sitrep_id.eq(sitrep1_id.into_untyped_uuid()),
                )
                .select(history_dsl::version)
                .first_async::<i64>(&*conn)
                .await
                .expect("read v1")
        };

        // Insert an FM-originated alert tied to the case and tombstone it.
        let alert_id = AlertUuid::new_v4();
        {
            let mut alert = nexus_db_model::Alert::new(
                alert_id,
                AlertClass::TestFoo,
                serde_json::json!({}),
            );
            alert.case_id = Some(case_id.into());
            alert.time_deleted = None;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::insert_into(nexus_db_schema::schema::alert::table)
                .values(alert)
                .execute_async(&*conn)
                .await
                .expect("inserting fm alert");
            diesel::update(alert_dsl::alert)
                .filter(alert_dsl::id.eq(alert_id.into_untyped_uuid()))
                .set(alert_dsl::time_deleted.eq(Utc::now()))
                .execute_async(&*conn)
                .await
                .expect("tombstoning alert");
        }

        // Build sitrep v=2 that does NOT carry the case forward. Persist it
        // so it gets a row in `fm_sitrep_history` at version 2.
        let sitrep2_id = SitrepUuid::new_v4();
        let sitrep2 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: sitrep2_id,
                inv_collection_id: CollectionUuid::new_v4(),
                parent_sitrep_id: Some(sitrep1_id),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep v2 (case dropped)".to_string(),
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
            },
            cases: iddqd::IdOrdMap::new(),
            ereports_by_id: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep2.clone())
            .await
            .expect("insert sitrep v2");

        let v2: i64 = {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            history_dsl::fm_sitrep_history
                .filter(
                    history_dsl::sitrep_id.eq(sitrep2_id.into_untyped_uuid()),
                )
                .select(history_dsl::version)
                .first_async::<i64>(&*conn)
                .await
                .expect("read v2")
        };
        assert!(v2 > v1, "v2 ({v2}) must be greater than v1 ({v1})");

        // Publish sitrep v2 through the watch channel and activate.
        sitrep_tx
            .send(Some(Arc::new((
                fm::SitrepVersion {
                    id: sitrep2_id,
                    version: u32::try_from(v2).expect("v2 fits in u32"),
                    time_made_current: Utc::now(),
                },
                sitrep2,
            ))))
            .unwrap();

        let _ = dbg!(task.actually_activate(opctx).await);

        // Tracker advanced to v2; sweep ran; the tombstoned alert should be
        // gone (no `fm_case` row references this case at version >= v2).
        let count: i64 = {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            alert_dsl::alert
                .filter(alert_dsl::id.eq(alert_id.into_untyped_uuid()))
                .count()
                .get_result_async::<i64>(&*conn)
                .await
                .expect("counting alerts")
        };
        assert_eq!(
            count, 0,
            "tombstoned FM alert should have been swept after activation",
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// When the rendezvous progress tracker is ahead of the sitrep this
    /// activation is processing, `alert_create` should reject the insert via
    /// the FM sitrep-version guard and the per-subtask status should record
    /// the rejection in `stale_sitrep` (rather than treating it as an error
    /// or silently counting it as created).
    #[tokio::test]
    async fn test_rendezvous_alert_stale_sitrep_is_recorded() {
        let logctx = dev::test_setup_log(
            "test_rendezvous_alert_stale_sitrep_is_recorded",
        );
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

        // Advance the rendezvous progress tracker well past anything we'll
        // publish on the watch channel, so the guarded insert that
        // `alert_create` performs in the FM path will reject our v=1 sitrep.
        datastore
            .fm_rendezvous_progress_advance(opctx, 10)
            .await
            .expect("advance tracker");

        // Build a sitrep at v=1 with a single case that requests one alert.
        let sitrep_id = SitrepUuid::new_v4();
        let alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let mut case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: "stale-sitrep test case".to_string(),
            },
            alerts_requested: iddqd::IdOrdMap::new(),
            ereports: iddqd::IdOrdMap::new(),
            support_bundles_requested: iddqd::IdOrdMap::new(),
        };
        case.alerts_requested
            .insert_unique(fm::case::AlertRequest {
                id: alert_id,
                class: AlertClass::TestFoo,
                requested_sitrep_id: sitrep_id,
                payload: serde_json::json!({}),
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
                    creator_id: OmicronZoneUuid::new_v4(),
                    comment: "stale-sitrep test sitrep".to_string(),
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
                    id: sitrep_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                sitrep,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        let alerts = &status.alerts.details;
        assert_eq!(
            alerts.total_alerts_requested, 1,
            "subtask should still observe the alert request",
        );
        assert_eq!(
            alerts.current_sitrep_alerts_requested, 1,
            "subtask should still observe the alert as new in this sitrep",
        );
        assert_eq!(
            alerts.alerts_created, 0,
            "no alert should be created when the guard rejects the insert",
        );
        assert_eq!(
            alerts.stale_sitrep, 1,
            "the stale-sitrep rejection should be recorded in the status",
        );
        assert!(
            alerts.errors.is_empty(),
            "stale-sitrep is not an error: {:?}",
            alerts.errors,
        );

        // The alert row must NOT be present in the database.
        let result = fetch_alert(&datastore, alert_id).await;
        assert!(
            result.is_err(),
            "alert row should not exist when the guard rejected the insert",
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// When the rendezvous progress tracker is ahead of the sitrep this
    /// activation is processing, `support_bundle_create` should reject the
    /// insert via the FM sitrep-version guard and the per-subtask status
    /// should record the rejection in `stale_sitrep` (rather than treating
    /// it as an error or silently counting it as created).
    #[tokio::test]
    async fn test_rendezvous_bundle_stale_sitrep_is_recorded() {
        let logctx = dev::test_setup_log(
            "test_rendezvous_bundle_stale_sitrep_is_recorded",
        );
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

        // Provide free debug datasets so allocation doesn't fail before the
        // guarded insert has a chance to run.
        create_sled_with_zpools(&datastore, &opctx, 3).await;

        // Advance the rendezvous progress tracker well past anything we'll
        // publish on the watch channel, so the guarded insert that
        // `support_bundle_create` performs in the FM path will reject our
        // v=1 sitrep.
        datastore
            .fm_rendezvous_progress_advance(opctx, 10)
            .await
            .expect("advance tracker");

        // Build a sitrep at v=1 with a single case that requests one bundle.
        let sitrep_id = SitrepUuid::new_v4();
        let bundle_id = SupportBundleUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let mut case = fm::Case {
            id: case_id,
            metadata: fm::case::Metadata {
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                comment: "stale-sitrep test case".to_string(),
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
                comment: "stale-sitrep test bundle".to_string(),
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
                    comment: "stale-sitrep test sitrep".to_string(),
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
                    id: sitrep_id,
                    version: 1,
                    time_made_current: Utc::now(),
                },
                sitrep,
            ))))
            .unwrap();

        let status = dbg!(task.actually_activate(opctx).await);
        let bundles = &status.support_bundles.details;
        assert_eq!(
            bundles.total_bundles_requested, 1,
            "subtask should still observe the bundle request",
        );
        assert_eq!(
            bundles.current_sitrep_bundles_requested, 1,
            "subtask should still observe the bundle as new in this sitrep",
        );
        assert_eq!(
            bundles.bundles_created, 0,
            "no bundle should be created when the guard rejects the insert",
        );
        assert_eq!(
            bundles.stale_sitrep, 1,
            "the stale-sitrep rejection should be recorded in the status",
        );
        assert!(
            bundles.errors.is_empty(),
            "stale-sitrep is not an error: {:?}",
            bundles.errors,
        );

        // The bundle row must NOT be present in the database.
        let result = fetch_support_bundle(&datastore, bundle_id).await;
        assert!(
            result.is_err(),
            "bundle row should not exist when the guard rejected the insert",
        );

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

        // Per-request errors hold the tracker still: the activation has not
        // earned the right to advance, so `latest_processed_sitrep_version`
        // stays at the bootstrap 0 and the `Status` field is `None`.
        assert_eq!(
            status.latest_processed_sitrep_version, None,
            "tracker advance should be skipped when a subtask records errors",
        );
        assert_eq!(
            datastore.fm_rendezvous_progress_advance(opctx, 0).await.unwrap(),
            0,
            "tracker should remain at the bootstrap value when the activation \
             had per-request errors",
        );

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

    /// End-to-end concurrent-Nexus zombie-prevention test for alerts.
    ///
    /// This composes the primitives built in earlier tasks (the
    /// `fm_rendezvous_progress` tracker, the `SitrepVersionGuardedInsert` CTE,
    /// and the `alert_create` FM path that wraps the insert in that guard) and
    /// asserts the spec-mandated race contract:
    ///
    /// "Concurrent Nexuses at adjacent sitreps. Nexus C is executing sitrep N.
    /// Nexus A is executing sitrep N+1 (N+1 became current after C started). A
    /// completes first, advancing the tracker and running the sweep — which
    /// drops tombstones for cases that dropped out between N and N+1. C then
    /// inserts one of those resources, creating a zombie. The CTE guard closes
    /// both scenarios structurally by making the tracker read atomic with each
    /// individual insert."
    ///
    /// Both directions are checked: the older sitrep's insert must surface
    /// `StaleSitrep` (and not land a row); a fresh insert at the current
    /// sitrep version must succeed.
    #[tokio::test]
    async fn test_concurrent_nexus_cannot_zombie_alert_after_sweep() {
        use nexus_types::identity::Asset;

        let logctx = dev::test_setup_log(
            "test_concurrent_nexus_cannot_zombie_alert_after_sweep",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Simulate Nexus A's outcome: it has activated for sitrep v=N+1=2,
        // advanced the tracker to 2, and run the sweep — so any case whose
        // requested resources hadn't yet been inserted by N=1 has had its
        // tombstones dropped.
        datastore
            .fm_rendezvous_progress_advance(opctx, 2)
            .await
            .expect("advance tracker to N+1");

        // Nexus C is still holding sitrep v=N=1 and tries to insert an alert.
        // The CTE guard must reject it as `StaleSitrep` — without this guard,
        // C would create a zombie row whose case-lifetime tombstone has
        // already been swept.
        let stale_alert = nexus_db_model::Alert::new(
            AlertUuid::new_v4(),
            AlertClass::TestFoo,
            json!({}),
        );
        let err = datastore
            .alert_create(opctx, stale_alert.clone(), Some(1))
            .await
            .expect_err(
                "the older Nexus's insert must surface a stale-sitrep error",
            );
        match err {
            Error::Conflict { ref message }
                if message.external_message().contains("stale sitrep") => {}
            other => {
                panic!("expected Conflict with stale sitrep, got {other:?}",)
            }
        }
        let result = fetch_alert(&datastore, stale_alert.id()).await;
        assert!(
            result.is_err(),
            "stale-sitrep insert must NOT have landed a row \
             (would have been a zombie)",
        );

        // Bonus: an insert at the current sitrep version must still succeed.
        // This verifies the guard isn't accidentally blocking everything.
        let fresh_alert = nexus_db_model::Alert::new(
            AlertUuid::new_v4(),
            AlertClass::TestFoo,
            json!({}),
        );
        let _ = datastore
            .alert_create(opctx, fresh_alert.clone(), Some(2))
            .await
            .expect("an insert at the current sitrep version must succeed");
        let row = fetch_alert(&datastore, fresh_alert.id())
            .await
            .expect("fresh alert row should exist");
        assert_eq!(row.id(), fresh_alert.id());

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// End-to-end concurrent-Nexus zombie-prevention test for support bundles.
    ///
    /// Symmetric to `test_concurrent_nexus_cannot_zombie_alert_after_sweep`,
    /// but exercising the same race for support-bundle creation. See that
    /// test's doc-comment for the spec quote.
    #[tokio::test]
    async fn test_concurrent_nexus_cannot_zombie_bundle_after_sweep() {
        use omicron_uuid_kinds::CaseUuid;

        let logctx = dev::test_setup_log(
            "test_concurrent_nexus_cannot_zombie_bundle_after_sweep",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Provide free debug datasets so allocation does not short-circuit
        // before the guarded insert has a chance to run.
        create_sled_with_zpools(&datastore, &opctx, 3).await;

        // Simulate Nexus A having advanced the tracker to N+1 and swept.
        datastore
            .fm_rendezvous_progress_advance(opctx, 2)
            .await
            .expect("advance tracker to N+1");

        // Nexus C, still holding sitrep v=N=1, tries to create a bundle.
        // The guard must reject the insert as a stale-sitrep conflict.
        let stale_bundle_id = SupportBundleUuid::new_v4();
        let err = datastore
            .support_bundle_create(
                opctx,
                SupportBundleCreateParams {
                    provenance: SupportBundleProvenance::Fm {
                        id: stale_bundle_id,
                        case_id: CaseUuid::new_v4(),
                        sitrep_version: 1,
                    },
                    reason: "concurrent-nexus zombie test (stale)",
                    nexus_id: OmicronZoneUuid::new_v4(),
                    user_comment: None,
                    data_selection: BundleDataSelection::all(),
                },
            )
            .await
            .expect_err("support_bundle_create at stale sitrep must fail");
        assert!(
            matches!(
                err,
                Error::Conflict { ref message }
                    if message.external_message().contains("stale sitrep")
            ),
            "expected stale-sitrep conflict, got {err:?}",
        );
        let result = fetch_support_bundle(&datastore, stale_bundle_id).await;
        assert!(
            result.is_err(),
            "stale-sitrep insert must NOT have landed a row \
             (would have been a zombie)",
        );

        // Bonus: a bundle insert at the current sitrep version must still
        // succeed end-to-end (allocation + guarded insert).
        let fresh_bundle_id = SupportBundleUuid::new_v4();
        let bundle = datastore
            .support_bundle_create(
                opctx,
                SupportBundleCreateParams {
                    provenance: SupportBundleProvenance::Fm {
                        id: fresh_bundle_id,
                        case_id: CaseUuid::new_v4(),
                        sitrep_version: 2,
                    },
                    reason: "concurrent-nexus zombie test (fresh)",
                    nexus_id: OmicronZoneUuid::new_v4(),
                    user_comment: None,
                    data_selection: BundleDataSelection::all(),
                },
            )
            .await
            .expect("support_bundle_create at current sitrep should succeed");
        assert_eq!(bundle.id, fresh_bundle_id.into());
        let row = fetch_support_bundle(&datastore, fresh_bundle_id)
            .await
            .expect("fresh bundle row should exist");
        assert_eq!(row.id, fresh_bundle_id.into());

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
