// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for loading the target blueprint from the DB
//!
//! This task triggers the `blueprint_execution` background task when the
//! blueprint changes.

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

pub struct TargetBlueprintLoader {
    datastore: Arc<DataStore>,
    last: Option<Arc<(BlueprintTarget, Blueprint)>>,
    tx: watch::Sender<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl TargetBlueprintLoader {
    pub fn new(datastore: Arc<DataStore>) -> TargetBlueprintLoader {
        let (tx, _) = watch::channel(None);
        TargetBlueprintLoader { datastore, last: None, tx }
    }

    /// Expose the target blueprint
    pub fn watcher(
        &self,
    ) -> watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>> {
        self.tx.subscribe()
    }
}

impl BackgroundTask for TargetBlueprintLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // Set up a logger for this activation that includes metadata about
            // the current target.
            let log = match &self.last {
                None => opctx.log.clone(),
                Some(old) => opctx.log.new(o!(
                    "original_target_id" => old.1.id.to_string(),
                    "original_time_created" => old.1.time_created.to_string(),
                )),
            };

            // Retrieve the latest target blueprint
            let (new_bp_target, new_blueprint) = match self
                .datastore
                .blueprint_target_get_current_full(opctx)
                .await
            {
                Ok((new_bp_target, new_blueprint)) => {
                    (new_bp_target, new_blueprint)
                }
                Err(error) => {
                    // We failed to read the blueprint. There's nothing to do
                    // but log an error. We'll retry when we're activated again.
                    let message = format!("{:#}", error);
                    warn!(
                        &log,
                        "failed to read target blueprint";
                        "error" => &message
                    );
                    let e =
                        format!("failed to read target blueprint: {message}");
                    return json!({"error": e});
                }
            };

            // Decide what to do with the new blueprint
            let enabled = new_bp_target.enabled;
            let Some((old_bp_target, old_blueprint)) = self.last.as_deref()
            else {
                // We've found a target blueprint for the first time.
                // Save it and notify any watchers.
                let target_id = new_blueprint.id;
                let time_created = new_blueprint.time_created;
                info!(
                    log,
                    "found new target blueprint (first find)";
                    "target_id" => %target_id,
                    "time_created" => %time_created
                );
                self.last = Some(Arc::new((new_bp_target, new_blueprint)));
                self.tx.send_replace(self.last.clone());
                return json!({
                    "target_id": target_id,
                    "time_created": time_created,
                    "time_found": chrono::Utc::now(),
                    "status": "first target blueprint",
                    "enabled": enabled,
                });
            };

            let target_id = new_blueprint.id;
            let time_created = new_blueprint.time_created;
            if old_blueprint.id != new_blueprint.id {
                // The current target blueprint has been updated
                info!(
                    log,
                    "found new target blueprint";
                    "target_id" => %target_id,
                    "time_created" => %time_created
                );
                self.last = Some(Arc::new((new_bp_target, new_blueprint)));
                self.tx.send_replace(self.last.clone());
                json!({
                    "target_id": target_id,
                    "time_created": time_created,
                    "time_found": chrono::Utc::now(),
                    "status": "target blueprint updated",
                    "enabled": enabled,
                })
            } else {
                // The new target id matches the old target id
                //
                // Let's see if the blueprints hold the same contents.
                // It should not be possible for the contents of a
                // blueprint to change, but we check to catch possible
                // bugs further up the stack.
                if *old_blueprint != new_blueprint {
                    let message = format!(
                        "blueprint for id {} changed. Blueprints are supposed \
                         to be immutable.",
                        target_id
                    );
                    error!(&log, "{}", message);
                    json!({
                        "target_id": target_id,
                        "status": "target blueprint unchanged (error)",
                        "error": message
                    })
                } else if old_bp_target.enabled != new_bp_target.enabled {
                    // The blueprints have the same contents, but its
                    // enabled bit has flipped.
                    let status = if new_bp_target.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    };
                    info!(
                        log,
                        "target blueprint enabled state changed";
                        "target_id" => %target_id,
                        "time_created" => %time_created,
                        "state" => status,
                    );
                    self.last = Some(Arc::new((new_bp_target, new_blueprint)));
                    self.tx.send_replace(self.last.clone());
                    json!({
                        "target_id": target_id,
                        "time_created": time_created,
                        "time_found": chrono::Utc::now(),
                        "status": format!("target blueprint {status}"),
                        "enabled": enabled,
                    })
                } else {
                    // We found a new target blueprint that exactly
                    // matches the old target blueprint. This is the
                    // common case when we're activated by a timeout.
                    debug!(
                       log,
                        "found latest target blueprint (unchanged)";
                        "target_id" => %target_id,
                        "time_created" => %time_created.clone()
                    );
                    json!({
                        "target_id": target_id,
                        "time_created": time_created,
                        "status": "target blueprint unchanged",
                        "enabled": enabled,
                    })
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::BackgroundTask;
    use nexus_inventory::now_db_precision;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        Blueprint, BlueprintTarget, CockroachDbPreserveDowngrade,
        OximeterReadMode, PendingMgsUpdates, PlanningReport,
    };
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::BlueprintUuid;
    use serde::Deserialize;
    use std::collections::BTreeMap;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn create_blueprint(
        parent_blueprint_id: BlueprintUuid,
    ) -> (BlueprintTarget, Blueprint) {
        let id = BlueprintUuid::new_v4();
        (
            BlueprintTarget {
                target_id: id,
                enabled: true,
                time_made_target: now_db_precision(),
            },
            Blueprint {
                id,
                sleds: BTreeMap::new(),
                pending_mgs_updates: PendingMgsUpdates::new(),
                cockroachdb_setting_preserve_downgrade:
                    CockroachDbPreserveDowngrade::DoNotModify,
                parent_blueprint_id: Some(parent_blueprint_id),
                internal_dns_version: Generation::new(),
                external_dns_version: Generation::new(),
                target_release_minimum_generation: Generation::new(),
                nexus_generation: Generation::new(),
                cockroachdb_fingerprint: String::new(),
                clickhouse_cluster_config: None,
                oximeter_read_version: Generation::new(),
                oximeter_read_mode: OximeterReadMode::SingleNode,
                time_created: now_db_precision(),
                creator: "test".to_string(),
                comment: "test blueprint".to_string(),
                report: PlanningReport::new(id),
            },
        )
    }

    #[derive(Deserialize)]
    #[allow(unused)]
    struct TargetUpdate {
        pub target_id: BlueprintUuid,
        pub time_created: chrono::DateTime<chrono::Utc>,
        pub time_found: Option<chrono::DateTime<chrono::Utc>>,
        pub status: String,
    }

    #[nexus_test(server = crate::Server)]
    async fn test_load_blueprints(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let mut task = TargetBlueprintLoader::new(datastore.clone());
        let mut rx = task.watcher();

        // We expect to see the initial blueprint set up by nexus-test-utils
        // (emulating RSS).
        let value = task.activate(&opctx).await;
        let initial_blueprint =
            rx.borrow_and_update().clone().expect("no initial blueprint");
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, initial_blueprint.1.id);
        assert_eq!(update.status, "first target blueprint");

        let (target, blueprint) = create_blueprint(update.target_id);

        // Inserting a blueprint, but not making it the target return status
        // indicating that the target hasn't changed
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap();
        let value = task.activate(&opctx).await;
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, initial_blueprint.1.id);
        assert_eq!(update.status, "target blueprint unchanged");

        // Setting a target blueprint makes the loader see it and broadcast it
        datastore.blueprint_target_set_current(&opctx, target).await.unwrap();
        let value = task.activate(&opctx).await;
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, blueprint.id);
        assert_eq!(update.status, "target blueprint updated");
        let rx_update = rx.borrow_and_update().clone().unwrap();
        assert_eq!(rx_update.0, target);
        assert_eq!(rx_update.1, blueprint);

        // Activation without changing the target blueprint results in no update
        let value = task.activate(&opctx).await;
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, blueprint.id);
        assert_eq!(update.status, "target blueprint unchanged");
        assert_eq!(false, rx.has_changed().unwrap());

        // Adding a new blueprint and updating the target triggers a change
        let (new_target, new_blueprint) = create_blueprint(blueprint.id);
        datastore.blueprint_insert(&opctx, &new_blueprint).await.unwrap();
        datastore
            .blueprint_target_set_current(&opctx, new_target)
            .await
            .unwrap();
        let value = task.activate(&opctx).await;
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, new_blueprint.id);
        assert_eq!(update.status, "target blueprint updated");
        let rx_update = rx.borrow_and_update().clone().unwrap();
        assert_eq!(rx_update.0, new_target);
        assert_eq!(rx_update.1, new_blueprint);

        // Activating again without changing the target blueprint results in
        // no update
        let value = task.activate(&opctx).await;
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, new_blueprint.id);
        assert_eq!(update.status, "target blueprint unchanged");
        assert_eq!(false, rx.has_changed().unwrap());
    }
}
