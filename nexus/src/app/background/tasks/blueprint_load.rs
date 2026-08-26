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
use nexus_types::internal_api::background::BlueprintLoadOutcome;
use nexus_types::internal_api::background::BlueprintLoaded;
use nexus_types::internal_api::background::BlueprintLoaderStatus;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct LoadedTargetBlueprint {
    pub target: BlueprintTarget,
    pub blueprint: Arc<Blueprint>,
}

pub struct TargetBlueprintLoader {
    datastore: Arc<DataStore>,
    tx: watch::Sender<Option<LoadedTargetBlueprint>>,
}

impl TargetBlueprintLoader {
    pub fn new(
        datastore: Arc<DataStore>,
        tx: watch::Sender<Option<LoadedTargetBlueprint>>,
    ) -> TargetBlueprintLoader {
        TargetBlueprintLoader { datastore, tx }
    }

    /// Expose the target blueprint
    pub fn watcher(&self) -> watch::Receiver<Option<LoadedTargetBlueprint>> {
        self.tx.subscribe()
    }

    async fn load(&self, opctx: &OpContext) -> BlueprintLoaderStatus {
        // Clone the most-recently-loaded blueprint (if any), so we can check
        // whether the current target is different.
        let last = self.tx.borrow().clone();

        // Set up a logger for this activation that includes metadata about
        // the current target.
        let log = match &last {
            None => opctx.log.clone(),
            Some(LoadedTargetBlueprint { blueprint, .. }) => opctx.log.new(o!(
                "original_target_id" => blueprint.id.to_string(),
                "original_time_created" =>
                    blueprint.time_created.to_string(),
            )),
        };

        // Retrieve the latest target blueprint
        let (new_bp_target, new_blueprint) =
            match self.datastore.blueprint_target_get_current_full(opctx).await
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
                    return BlueprintLoaderStatus::Error(format!(
                        "failed to read target blueprint: {message}"
                    ));
                }
            };

        // Decide what to do with the new blueprint
        let Some(LoadedTargetBlueprint {
            target: old_bp_target,
            blueprint: old_blueprint,
        }) = last
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
            self.tx.send_replace(Some(LoadedTargetBlueprint {
                target: new_bp_target,
                blueprint: Arc::new(new_blueprint),
            }));
            return BlueprintLoaderStatus::Loaded(BlueprintLoaded {
                target: new_bp_target,
                time_created,
                outcome: BlueprintLoadOutcome::FirstTarget {
                    time_found: chrono::Utc::now(),
                },
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
            self.tx.send_replace(Some(LoadedTargetBlueprint {
                target: new_bp_target,
                blueprint: Arc::new(new_blueprint),
            }));
            BlueprintLoaderStatus::Loaded(BlueprintLoaded {
                target: new_bp_target,
                time_created,
                outcome: BlueprintLoadOutcome::Updated {
                    time_found: chrono::Utc::now(),
                },
            })
        } else {
            // The new target id matches the old target id
            //
            // Let's see if the blueprints hold the same contents.
            // It should not be possible for the contents of a
            // blueprint to change, but we check to catch possible
            // bugs further up the stack.
            if *old_blueprint != new_blueprint {
                error!(
                    &log,
                    "blueprint changed, but blueprints are supposed to be \
                     immutable";
                    "target_id" => %target_id,
                );
                BlueprintLoaderStatus::ImmutableBlueprintChanged { target_id }
            } else if old_bp_target.enabled != new_bp_target.enabled {
                // The blueprints have the same contents, but its
                // enabled bit has flipped.
                let status =
                    if new_bp_target.enabled { "enabled" } else { "disabled" };
                info!(
                    log,
                    "target blueprint enabled state changed";
                    "target_id" => %target_id,
                    "time_created" => %time_created,
                    "state" => status,
                );
                self.tx.send_replace(Some(LoadedTargetBlueprint {
                    target: new_bp_target,
                    blueprint: Arc::new(new_blueprint),
                }));
                BlueprintLoaderStatus::Loaded(BlueprintLoaded {
                    target: new_bp_target,
                    time_created,
                    outcome: BlueprintLoadOutcome::EnabledChanged {
                        time_found: chrono::Utc::now(),
                    },
                })
            } else {
                // We found a new target blueprint that exactly
                // matches the old target blueprint. This is the
                // common case when we're activated by a timeout.
                debug!(
                   log,
                    "found latest target blueprint (unchanged)";
                    "target_id" => %target_id,
                    "time_created" => %time_created
                );
                BlueprintLoaderStatus::Loaded(BlueprintLoaded {
                    target: new_bp_target,
                    time_created,
                    outcome: BlueprintLoadOutcome::Unchanged,
                })
            }
        }
    }
}

impl BackgroundTask for TargetBlueprintLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let status = self.load(opctx).await;
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
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::BackgroundTask;
    use assert_matches::assert_matches;
    use nexus_inventory::now_db_precision;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::{
        Blueprint, BlueprintSource, BlueprintTarget,
        CockroachDbPreserveDowngrade, OximeterReadMode, PendingMgsUpdates,
    };
    use omicron_generation_kinds::{
        Generation, NexusGeneration, TargetReleaseGeneration,
    };
    use omicron_uuid_kinds::BlueprintUuid;
    use std::collections::BTreeMap;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn create_blueprint(
        parent_blueprint_id: BlueprintUuid,
    ) -> (BlueprintTarget, Arc<Blueprint>) {
        let id = BlueprintUuid::new_v4();
        (
            BlueprintTarget {
                target_id: id,
                enabled: true,
                time_made_target: now_db_precision(),
            },
            Arc::new(Blueprint {
                id,
                sleds: BTreeMap::new(),
                pending_mgs_updates: PendingMgsUpdates::new(),
                cockroachdb_setting_preserve_downgrade:
                    CockroachDbPreserveDowngrade::DoNotModify,
                parent_blueprint_id: Some(parent_blueprint_id),
                internal_dns_version: Generation::new(),
                external_dns_version: Generation::new(),
                target_release_minimum_generation: TargetReleaseGeneration::new(
                ),
                nexus_generation: NexusGeneration::new(),
                external_networking_generation: Generation::new(),
                cockroachdb_fingerprint: String::new(),
                clickhouse_cluster_config: None,
                oximeter_read_version: Generation::new(),
                oximeter_read_mode: OximeterReadMode::SingleNode,
                time_created: now_db_precision(),
                creator: "test".to_string(),
                comment: "test blueprint".to_string(),
                source: BlueprintSource::Test,
            }),
        )
    }

    fn expect_loaded(value: serde_json::Value) -> BlueprintLoaded {
        let status =
            serde_json::from_value::<BlueprintLoaderStatus>(value).unwrap();
        match status {
            BlueprintLoaderStatus::Loaded(loaded) => loaded,
            BlueprintLoaderStatus::Error(_)
            | BlueprintLoaderStatus::ImmutableBlueprintChanged { .. } => {
                panic!("loader did not load a blueprint: {status:?}")
            }
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_load_blueprints(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (tx, _) = watch::channel(None);
        let mut task = TargetBlueprintLoader::new(datastore.clone(), tx);
        let mut rx = task.watcher();

        // We expect to see the initial blueprint set up by nexus-test-utils
        // (emulating RSS).
        let value = task.activate(&opctx).await;
        let initial_blueprint =
            rx.borrow_and_update().clone().expect("no initial blueprint");
        let update = expect_loaded(value);
        assert_eq!(update.target.target_id, initial_blueprint.blueprint.id);
        assert_matches!(
            update.outcome,
            BlueprintLoadOutcome::FirstTarget { .. }
        );

        let (target, blueprint) = create_blueprint(update.target.target_id);

        // Inserting a blueprint, but not making it the target return status
        // indicating that the target hasn't changed
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap();
        let value = task.activate(&opctx).await;
        let update = expect_loaded(value);
        assert_eq!(update.target.target_id, initial_blueprint.blueprint.id);
        assert_matches!(update.outcome, BlueprintLoadOutcome::Unchanged);

        // Setting a target blueprint makes the loader see it and broadcast it
        datastore.blueprint_target_set_current(&opctx, target).await.unwrap();
        let value = task.activate(&opctx).await;
        let update = expect_loaded(value);
        assert_eq!(update.target.target_id, blueprint.id);
        assert_matches!(update.outcome, BlueprintLoadOutcome::Updated { .. });
        let rx_update = rx.borrow_and_update().clone().unwrap();
        assert_eq!(rx_update.target, target);
        assert_eq!(rx_update.blueprint, blueprint);

        // Activation without changing the target blueprint results in no update
        let value = task.activate(&opctx).await;
        let update = expect_loaded(value);
        assert_eq!(update.target.target_id, blueprint.id);
        assert_matches!(update.outcome, BlueprintLoadOutcome::Unchanged);
        assert_eq!(false, rx.has_changed().unwrap());

        // Adding a new blueprint and updating the target triggers a change
        let (new_target, new_blueprint) = create_blueprint(blueprint.id);
        datastore.blueprint_insert(&opctx, &new_blueprint).await.unwrap();
        datastore
            .blueprint_target_set_current(&opctx, new_target)
            .await
            .unwrap();
        let value = task.activate(&opctx).await;
        let update = expect_loaded(value);
        assert_eq!(update.target.target_id, new_blueprint.id);
        assert_matches!(update.outcome, BlueprintLoadOutcome::Updated { .. });
        let rx_update = rx.borrow_and_update().clone().unwrap();
        assert_eq!(rx_update.target, new_target);
        assert_eq!(rx_update.blueprint, new_blueprint);

        // Activating again without changing the target blueprint results in
        // no update
        let value = task.activate(&opctx).await;
        let update = expect_loaded(value);
        assert_eq!(update.target.target_id, new_blueprint.id);
        assert_matches!(update.outcome, BlueprintLoadOutcome::Unchanged);
        assert_eq!(false, rx.has_changed().unwrap());
    }
}
