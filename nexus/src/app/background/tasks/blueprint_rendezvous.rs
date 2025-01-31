// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reconciling blueprints and inventory, updating
//! Reconfigurator rendezvous tables

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_rendezvous::reconcile_blueprint_rendezvous_tables;
use nexus_types::{
    deployment::{Blueprint, BlueprintTarget},
    internal_api::background::BlueprintRendezvousStatus,
};
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that takes a [`Blueprint`] and an inventory `Collection`
/// and updates any rendezvous tables to track resources under Reconfigurator's
/// control for other parts of Nexus to consume.
pub struct BlueprintRendezvous {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl BlueprintRendezvous {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
    ) -> Self {
        Self { datastore, rx_blueprint }
    }

    /// Implementation for `BackgroundTask::activate` for `BlueprintRendezvous`,
    /// added here to produce better compile errors.
    ///
    /// The presence of `boxed()` in `BackgroundTask::activate` has caused some
    /// confusion with compilation errors in the past. So separate this method
    /// out.
    async fn activate_impl(&mut self, opctx: &OpContext) -> serde_json::Value {
        // Get the latest blueprint, cloning to prevent holding a read lock
        // on the watch.
        let update = self.rx_blueprint.borrow_and_update().clone();
        let Some((_, blueprint)) = update.as_deref() else {
            warn!(
                &opctx.log, "Blueprint rendezvous: skipped";
                "reason" => "no blueprint",
            );
            return json!({"error": "no blueprint" });
        };

        // Get the latest inventory collection
        let maybe_collection = match self
            .datastore
            .inventory_get_latest_collection(opctx)
            .await
        {
            Ok(maybe_collection) => maybe_collection,
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    &opctx.log, "Blueprint rendezvous: skipped";
                    "reason" => "failed to read latest inventory collection",
                    &err,
                );
                return json!({ "error":
                    format!("failed reading inventory collection: {err}"),
                });
            }
        };

        let Some(collection) = maybe_collection else {
            warn!(
                &opctx.log, "Blueprint rendezvous: skipped";
                "reason" => "no inventory collection",
            );
            return json!({"error": "no inventory collection" });
        };

        // Actually perform rendezvous table reconciliation
        let result = reconcile_blueprint_rendezvous_tables(
            opctx,
            &self.datastore,
            blueprint,
            &collection,
        )
        .await;

        // Return the result as a `serde_json::Value`
        match result {
            Ok(stats) => {
                let status = BlueprintRendezvousStatus {
                    blueprint_id: blueprint.id,
                    inventory_collection_id: collection.id,
                    stats,
                };
                json!(status)
            }
            Err(err) => json!({ "error":
                format!("rendezvous reconciliation failed: {err:#}"),
            }),
        }
    }
}

impl BackgroundTask for BlueprintRendezvous {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        self.activate_impl(opctx).boxed()
    }
}
