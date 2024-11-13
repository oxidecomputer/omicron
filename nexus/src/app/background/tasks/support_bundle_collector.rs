// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing Support Bundles

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

pub struct SupportBundleCollector {
    datastore: Arc<DataStore>,
    disable: bool,
    nexus_id: OmicronZoneUuid,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

#[derive(Debug, Default, Serialize)]
struct CleanupReport {
    // Responses from our requests to Sled Agents
    sled_bundles_deleted_ok: usize,
    sled_bundles_deleted_not_found: usize,
    sled_bundles_delete_failed: usize,

    // Results from updating our database records
    db_destroying_bundles_removed: usize,
    db_failing_bundles_updated: usize,
}

#[derive(Debug, Default, Serialize)]
struct CollectionReport {}

impl SupportBundleCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        disable: bool,
        nexus_id: OmicronZoneUuid,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
    ) -> Self {
        SupportBundleCollector { datastore, disable, nexus_id, rx_blueprint }
    }

    // Monitors all bundles that are "destroying" or "failing" and assigned to
    // this Nexus, and attempts to clear their storage from Sled Agents.
    async fn cleanup_destroyed_bundles<'a>(
        &'a self,
        opctx: &'a OpContext,
    ) -> Result<CleanupReport, String> {
        let pagparams = DataPageParams::max_page();
        let result = self
            .datastore
            .support_bundle_list_assigned_to_nexus(
                opctx,
                &pagparams,
                self.nexus_id,
                vec![
                    SupportBundleState::Destroying,
                    SupportBundleState::Failing,
                ],
            )
            .await;

        let bundles_to_destroy = match result {
            Ok(r) => r,
            Err(err) => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Failed to list terminating bundles";
                    "err" => %err
                );
                return Err(format!("failed to query database: {:#}", err));
            }
        };

        let mut report = CleanupReport::default();

        // NOTE: This could be concurrent?
        for bundle in bundles_to_destroy {
            // XXX: Contact the Sled Agent holding this bundle, release it.
            // If OK: report.bundles_deleted_ok++
            // If 404: report.bundles_deleted_not_found++
            // Otherwise: report.bundles_deleted_failed++, and continue.

            match bundle.state {
                SupportBundleState::Destroying => {
                    // Destroying is a terminal state; no one should be able to
                    // change this state from underneath us.
                    self.datastore.support_bundle_delete(
                        opctx,
                        bundle.id.into(),
                    ).await.map_err(|err| {
                        warn!(
                            &opctx.log,
                            "SupportBundleCollector: Could not delete 'destroying' bundle";
                            "err" => %err
                        );
                        format!("Could not delete 'destroying' bundle: {:#}", err)
                    })?;

                    report.db_destroying_bundles_removed += 1;
                }
                SupportBundleState::Failing => {
                    if let Err(err) = self
                        .datastore
                        .support_bundle_update(
                            &opctx,
                            bundle.id.into(),
                            SupportBundleState::Failed,
                        )
                        .await
                    {
                        if matches!(err, Error::InvalidRequest { .. }) {
                            // It's possible that the bundle is marked "destroying" by a
                            // user request, concurrently with our operation.
                            //
                            // In this case, we log that this happened, but do nothing.
                            // The next iteration of this background task should treat
                            // this as the "Destroying" case, and delete the bundle.
                            info!(
                                &opctx.log,
                                "SupportBundleCollector: Concurrent state change";
                                "err" => ?err,
                            );
                        } else {
                            return Err(format!(
                                "Could not delete 'failing' bundle: {:#}",
                                err
                            ));
                        }
                    }

                    report.db_failing_bundles_updated += 1;
                }
                other => {
                    // We should be filtering to only see "Destroying" and
                    // "Failing" bundles in our database request above.
                    error!(
                        &opctx.log,
                        "SupportBundleCollector: Cleaning bundle in unexpected state";
                        "id" => %bundle.id,
                        "state" => ?other
                    );
                }
            }
        }
        Ok(report)
    }

    async fn collect_bundle<'a>(
        &'a self,
        opctx: &'a OpContext,
    ) -> Result<CollectionReport, String> {
        let mut report = CollectionReport::default();

        let pagparams = DataPageParams::max_page();
        let result = self
            .datastore
            .support_bundle_list_assigned_to_nexus(
                opctx,
                &pagparams,
                self.nexus_id,
                vec![SupportBundleState::Collecting],
            )
            .await;

        let bundle_to_collect = match result {
            Ok(bundles) => {
                if let Some(bundle) = bundles.get(0) {
                    info!(
                        &opctx.log,
                        "SupportBundleCollector: Found bundle to collect";
                        "bundle" => %bundle.id,
                        "bundles_in_queue" => bundles.len()
                    );
                    bundle.clone()
                } else {
                    info!(&opctx.log, "No bundles to collect");
                    return Ok(report);
                }
            }
            Err(err) => {
                warn!(
                    &opctx.log,
                    "SupportBundleCollector: Failed to list collecting bundles";
                    "err" => %err
                );
                return Err(format!("failed to query database: {:#}", err));
            }
        };

        // TODO: Actually collect the bundle here.

        // TODO: Periodically check the state of the bundle we're collecting -
        // it might have been cancelled if this is taking a while.
        //
        // Whenever we check, we're basically creating a "yield" point.
        // This should perhaps be set up on a timer.

        // TODO: Store the bundle once collection has finished.

        Ok(report)
    }
}

impl BackgroundTask for SupportBundleCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            if self.disable {
                return json!({ "error": "task disabled" });
            }

            let mut cleanup_report = None;
            let mut cleanup_err = None;
            let mut collection_report = None;
            let mut collection_err = None;

            match self.cleanup_destroyed_bundles(&opctx).await {
                Ok(report) => cleanup_report = Some(report),
                Err(err) => cleanup_err = Some(json!({ "cleanup_error": err })),
            };

            match self.collect_bundle(&opctx).await {
                Ok(report) => collection_report = Some(report),
                Err(err) => {
                    collection_err = Some(json!({ "collect_error": err }))
                }
            };

            json!({
                "cleanup_report": cleanup_report,
                "cleanup_err": cleanup_err,
                "collection_report": collection_report,
                "collection_err": collection_err,
            })
        }
        .boxed()
    }
}
