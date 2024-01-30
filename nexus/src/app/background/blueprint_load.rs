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
use nexus_types::deployment::Blueprint;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

pub struct TargetBlueprintLoader {
    datastore: Arc<DataStore>,
    last: Option<Arc<Blueprint>>,
    tx: watch::Sender<Option<Arc<Blueprint>>>,
    rx: watch::Receiver<Option<Arc<Blueprint>>>,
}

impl TargetBlueprintLoader {
    pub fn new(datastore: Arc<DataStore>) -> TargetBlueprintLoader {
        let (tx, rx) = watch::channel(None);
        TargetBlueprintLoader { datastore, last: None, tx, rx }
    }

    /// Expose the target blueprint
    pub fn watcher(&self) -> watch::Receiver<Option<Arc<Blueprint>>> {
        self.rx.clone()
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
            let result =
                self.datastore.blueprint_target_get_current_full(opctx).await;

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
                    let e =
                        format!("failed to read target blueprint: {message}");
                    json!({"error": e})
                }
                (None, Ok(None)) => {
                    // We haven't found a blueprint yet. Do nothing.
                    json!({"status": "no blueprint"})
                }
                (Some(old), Ok(None)) => {
                    // We have transitioned from having a blueprint to not
                    // having one. This should not happen.
                    let message = format!(
                        "target blueprint with id {} was removed. There is no \
                        longer any target blueprint",
                        old.id
                    );
                    self.last = None;
                    self.tx.send_replace(self.last.clone());
                    error!(&log, "{message:?}");
                    json!({"error": message})
                }
                (None, Ok(Some((_, new_target)))) => {
                    // We've found a target blueprint for the first time.
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
                    json!({
                        "target_id": target_id,
                        "time_created": time_created,
                        "time_found": chrono::Utc::now().to_string(),
                        "status": "first target blueprint"
                    })
                }
                (Some(old), Ok(Some((_, new)))) => {
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
