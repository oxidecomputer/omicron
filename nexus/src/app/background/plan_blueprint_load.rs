// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for loading the target blueprint from the DB
//!
//! This task triggers the `plan_execution` background task when the blueprint
//! changes.

use super::common::BackgroundTask;
use crate::app::deployment;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::authz;
use nexus_db_queries::authz::Action;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

pub struct TargetBlueprintLoader {
    blueprints: std::sync::Mutex<deployment::Blueprints>,
    #[allow(unused)]
    datastore: Arc<DataStore>,
    last: Option<Arc<Blueprint>>,
    tx: watch::Sender<Option<Arc<Blueprint>>>,
    rx: watch::Receiver<Option<Arc<Blueprint>>>,
}

impl TargetBlueprintLoader {
    pub fn new(
        blueprints: std::sync::Mutex<deployment::Blueprints>,
        datastore: Arc<DataStore>,
    ) -> TargetBlueprintLoader {
        let (tx, rx) = watch::channel(None);
        TargetBlueprintLoader { blueprints, datastore, last: None, tx, rx }
    }

    /// Expose the target blueprint
    pub fn watcher(&self) -> watch::Receiver<Option<Arc<Blueprint>>> {
        self.rx.clone()
    }

    // This functoin is a modified copy from `nexus/src/app/deployment.rs` for
    // use until the types are in the datastore.
    //
    // This is a stand-in for a datastore function that fetches the current
    // target information and the target blueprint's contents.  This helper
    // exists to combine the authz check with the lookup, which is what the
    // datastore function will eventually do.
    async fn blueprint_target(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<Blueprint>, anyhow::Error> {
        opctx.authorize(Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let blueprints = self.blueprints.lock().unwrap();
        Ok(blueprints.target.target_id.and_then(|target_id| {
            blueprints.all_blueprints.get(&target_id).cloned()
        }))
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
                    "current_target_id" => old.id.to_string(),
                    "current_time_created" => old.time_created.to_string(),
                )),
            };

            // Retrieve the latest target blueprint
            let result = self.blueprint_target(opctx).await;

            // Decide what to do with the result
            match (&self.last, result) {
                (_, Err(error)) => {
                    // We failed to read the blueprint. There's nothing to do
                    // but log an error. We'll retry when we're activated again.
                    let message = format!("{:#}", error);
                    warn!(
                        &log,
                        "failed to read target blueprint";
                        "error" => &message
                    );
                    json!({
                        "error": 
                        format!("failed to read target blueprint: {message}")})
                }
                (None, Ok(None)) => {
                    // We haven't found a blueprint yet. Do nothing.
                    json!({})
                }
                (Some(old), Ok(None)) => {
                    // We have transitioned from having a blueprint to not having one.
                    // This should not happen.
                    let message = format!(
                        "target blueprint with id {} was removed. There is no \
                        longer any target blueprint",
                        old.id
                    );
                    error!(&log, "{}", message);
                    json!({"error": message})

                }
                (None, Ok(Some(new_target))) => {
                    // We've found a new target blueprint for the first time.
                    // Save it and notify any watchers.
                    let target_id = new_target.id.to_string();
                    let time_created = new_target.time_created.to_string();
                    info!(
                        log,
                        "found new target blueprint (first find)";
                        "target_id" => &target_id,
                        "time_created" => &time_created
                    );
                    self.last = Some(Arc::new(new_target));
                    self.tx.send_replace(self.last.clone());
                    json!({"target_id": target_id, "time_created": time_created})
                }
                (Some(old), Ok(Some(new))) => {
                    let target_id = new.id.to_string();
                    let time_created = new.time_created.to_string();
                    if old.id != new.id {
                        // The current target blueprint has been updated
                        info!(
                            log,
                            "found new target blueprint";
                            "target_id" => &target_id,
                            "time_created" => &time_created
                        );
                        self.last = Some(Arc::new(new));
                        self.tx.send_replace(self.last.clone());
                        json!({"target_id": target_id, "time_created": time_created})
                    } else {
                        // The new target id matches the old target id
                        //
                        // Let's see if the blueprints hold the same contents.
                        // It should not be possible for the contents of a
                        // blueprint to change, but we check to catch possible
                        // bugs further up the stack.
                        if **old != new {
                            let message = format!(
                                "blueprint for id {} changed. \
                                Blueprints are supposed to be immutable.",
                                target_id
                            );
                            error!(&log, "{}", message);
                            json!({"error": message})
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
                        json!({"target_id": target_id, "time_created": time_created})
                       }
                    }
                }
            }
        }.boxed()
    }
}
