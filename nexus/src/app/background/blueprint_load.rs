// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for loading the target blueprint from the DB
//!
//! This task triggers the `blueprint_execution` background task when the
//! blueprint changes.

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
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
            let result =
                self.datastore.blueprint_target_get_current_full(opctx).await;

            // Decide what to do with the result
            match (&mut self.last, result) {
                (_, Err(error)) => {
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
                    json!({"error": e})
                }
                (None, Ok(None)) => {
                    // We haven't found a blueprint yet. Do nothing.
                    json!({"status": "no target blueprint"})
                }
                (Some(old), Ok(None)) => {
                    // We have transitioned from having a blueprint to not
                    // having one. This should not happen.
                    let message = format!(
                        "target blueprint with id {} was removed. There is no \
                        longer any target blueprint",
                        old.1.id
                    );
                    let old_id = old.1.id.to_string();
                    self.last = None;
                    self.tx.send_replace(self.last.clone());
                    error!(&log, "{message:?}");
                    json!({
                        "removed_target_id": old_id,
                        "status": "no target blueprint (removed)",
                        "error": message
                    })
                }
                (None, Ok(Some((new_bp_target, new_blueprint)))) => {
                    // We've found a target blueprint for the first time.
                    // Save it and notify any watchers.
                    let target_id = new_blueprint.id.to_string();
                    let time_created = new_blueprint.time_created.to_string();
                    info!(
                        log,
                        "found new target blueprint (first find)";
                        "target_id" => &target_id,
                        "time_created" => &time_created
                    );
                    self.last = Some(Arc::new((new_bp_target, new_blueprint)));
                    self.tx.send_replace(self.last.clone());
                    json!({
                        "target_id": target_id,
                        "time_created": time_created,
                        "time_found": chrono::Utc::now().to_string(),
                        "status": "first target blueprint"
                    })
                }
                (Some(old), Ok(Some((new_bp_target, new_blueprint)))) => {
                    let target_id = new_blueprint.id.to_string();
                    let time_created = new_blueprint.time_created.to_string();
                    if old.1.id != new_blueprint.id {
                        // The current target blueprint has been updated
                        info!(
                            log,
                            "found new target blueprint";
                            "target_id" => &target_id,
                            "time_created" => &time_created
                        );
                        self.last =
                            Some(Arc::new((new_bp_target, new_blueprint)));
                        self.tx.send_replace(self.last.clone());
                        json!({
                            "target_id": target_id,
                            "time_created": time_created,
                            "time_found": chrono::Utc::now().to_string(),
                            "status": "target blueprint updated"
                        })
                    } else {
                        // The new target id matches the old target id
                        //
                        // Let's see if the blueprints hold the same contents.
                        // It should not be possible for the contents of a
                        // blueprint to change, but we check to catch possible
                        // bugs further up the stack.
                        if old.1 != new_blueprint {
                            let message = format!(
                                "blueprint for id {} changed. \
                                Blueprints are supposed to be immutable.",
                                target_id
                            );
                            error!(&log, "{}", message);
                            json!({
                                "target_id": target_id,
                                "status": "target blueprint unchanged (error)",
                                "error": message
                            })
                        } else {
                            // We found a new target blueprint that exactly matches
                            // the old target blueprint. This is the common case
                            // when we're activated by a timeout.
                            debug!(
                               log,
                                "found latest target blueprint (unchanged)";
                                "target_id" => &target_id,
                                "time_created" => &time_created.clone()
                            );
                            json!({
                                "target_id": target_id,
                                "time_created": time_created,
                                "status": "target blueprint unchanged"
                            })
                        }
                    }
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::common::BackgroundTask;
    use nexus_inventory::now_db_precision;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{Blueprint, BlueprintTarget};
    use omicron_common::api::external::Generation;
    use serde::Deserialize;
    use std::collections::{BTreeMap, BTreeSet};
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn create_blueprint(
        parent_blueprint_id: Option<Uuid>,
    ) -> (BlueprintTarget, Blueprint) {
        let id = Uuid::new_v4();
        (
            BlueprintTarget {
                target_id: id,
                enabled: true,
                time_made_target: now_db_precision(),
            },
            Blueprint {
                id,
                omicron_zones: BTreeMap::new(),
                parent_blueprint_id,
                internal_dns_version: Generation::new(),
                time_created: now_db_precision(),
                creator: "test".to_string(),
                comment: "test blueprint".to_string(),
            },
        )
    }

    #[derive(Deserialize)]
    #[allow(unused)]
    struct TargetUpdate {
        pub target_id: Uuid,
        pub time_created: chrono::DateTime<chrono::Utc>,
        pub time_found: Option<chrono::DateTime<chrono::Utc>>,
        pub status: String,
    }

    #[nexus_test(server = crate::Server)]
    async fn test_load_blueprints(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let mut task = TargetBlueprintLoader::new(datastore.clone());
        let mut rx = task.watcher();

        // We expect an appropriate status with no blueprint in the datastore
        let value = task.activate(&opctx).await;
        assert_eq!(json!({"status": "no target blueprint"}), value);
        assert!(rx.borrow().is_none());

        let (target, blueprint) = create_blueprint(None);

        // Inserting a blueprint, but not making it the target returns the same
        // status
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap();
        let value = task.activate(&opctx).await;
        assert_eq!(json!({"status": "no target blueprint"}), value);
        assert!(rx.borrow().is_none());

        // Setting a target blueprint makes the loader see it and broadcast it
        datastore.blueprint_target_set_current(&opctx, target).await.unwrap();
        let value = task.activate(&opctx).await;
        let update = serde_json::from_value::<TargetUpdate>(value).unwrap();
        assert_eq!(update.target_id, blueprint.id);
        assert_eq!(update.status, "first target blueprint");
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
        let (new_target, new_blueprint) = create_blueprint(Some(blueprint.id));
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
