// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for collecting the Cockroach Node ID for running CRDB zones
//!
//! Cockroach assigns a node ID when the node is initially started and joins the
//! cluster. The node IDs are 1-up counters that are never reused. Cluster
//! management operations (e.g., decommissioning nodes) are keyed off of the
//! node ID. However, because node IDs aren't assigned until the node has
//! started and joins the cluster, it means there is a gap between when Omicron
//! creates a CRDB zone (and picks an Omicron zone ID for it) and when that zone
//! gets a CRDB node ID. This RPW exists to backfill the mapping from Omicron
//! zone ID <-> CRDB node ID for Cockroach zones.
//!
//! This isn't foolproof. If a Cockroach node fails to start, it won't have a
//! node ID and therefore this RPW won't be able to make an assignment. If a
//! Cockroach node succeeds in starting and gets a node ID but then fails in an
//! unrecoverable way before this RPW has collected its node ID, that will also
//! result in a missing assignment. Consumers of the Omicron zone ID <-> CRDB
//! node ID don't have a way of distinguishing these two failure modes from this
//! RPW alone, and will need to gather other information (e.g., asking CRDB for
//! the status of all nodes and looking for orphans, perhaps) to determine
//! whether a zone without a known node ID ever existed.

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

pub struct CockroachNodeIdCollector {
    datastore: Arc<DataStore>,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl CockroachNodeIdCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
    ) -> Self {
        Self { datastore, rx_blueprint }
    }

    /// Implementation for `BackgroundTask::activate` for `BlueprintExecutor`,
    /// added here to produce better compile errors.
    ///
    /// The presence of `boxed()` in `BackgroundTask::activate` has caused some
    /// confusion with compilation errors in the past. So separate this method
    /// out.
    async fn activate_impl<'a>(
        &mut self,
        opctx: &OpContext,
    ) -> serde_json::Value {
        // Get the latest blueprint, cloning to prevent holding a read lock
        // on the watch.
        let update = self.rx_blueprint.borrow_and_update().clone();

        let Some((_bp_target, blueprint)) = update.as_deref() else {
            warn!(
                &opctx.log, "Blueprint execution: skipped";
                "reason" => "no blueprint",
            );
            return json!({"error": "no blueprint" });
        };

        // We can only actively collect from zones that should be running; if
        // there are CRDB zones in other states that still need their node ID
        // collected, we have to wait until they're running.
        for (_sled_id, zone) in
            blueprint.all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
        {
            let BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { address, .. },
            ) = &zone.zone_type
            else {
                continue;
            };

            // TODO 1 continue if we already know the node ID for this zone
            _ = &self.datastore;

            // TODO 2 collect and insert the node ID for this zone
            _ = address;
        }

        json!({})
    }
}

impl BackgroundTask for CockroachNodeIdCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        self.activate_impl(opctx).boxed()
    }
}
