// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for loading the latest inventory collection from the DB

use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::InventoryLoadStatus;
use nexus_types::inventory::Collection;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

pub struct InventoryLoader {
    datastore: Arc<DataStore>,
    // We store an `Arc<Collection>` in this channel instead of just a
    // `Collection` so that cloning it is cheap: we want callers to just grab a
    // snapshot of the collection to do whatever they're going to do without
    // holding the lock on the channel.
    tx: watch::Sender<Option<Arc<Collection>>>,
}

impl BackgroundTask for InventoryLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.load_if_needed(opctx).await;
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

impl InventoryLoader {
    pub fn new(
        datastore: Arc<DataStore>,
        tx: watch::Sender<Option<Arc<Collection>>>,
    ) -> Self {
        Self { datastore, tx }
    }

    pub fn watcher(&self) -> watch::Receiver<Option<Arc<Collection>>> {
        self.tx.subscribe()
    }

    async fn load_if_needed(&self, opctx: &OpContext) -> InventoryLoadStatus {
        // Set up a logger for this activation that includes metadata about
        // the current target.
        let (old, log) = match &*self.tx.borrow() {
            None => (None, opctx.log.clone()),
            Some(old) => {
                let log = opctx.log.new(slog::o!(
                    "original_id" => old.id.to_string(),
                    "original_time_started" => old.time_started.to_string(),
                ));
                (Some((old.id, old.time_started)), log)
            }
        };

        // Get the ID of the latest collection.
        let time_loaded = Utc::now();
        let latest_id = match self
            .datastore
            .inventory_get_latest_collection_id(opctx)
            .await
        {
            Ok(Some(id)) => id,
            Ok(None) => match old {
                Some((old_id, _)) => {
                    // We should never go from "some collection" to "no
                    // collections"; pruning should always keep a small number
                    // of old collections around until we have new ones to
                    // replace them.
                    //
                    // In this case we won't replace our channel contents with
                    // `None`; we'll keep around whatever old collection we had
                    // loaded.
                    warn!(
                        log,
                        "previously had a collection, but now none exist"
                    );
                    return InventoryLoadStatus::Error(format!(
                        "previously loaded collection {old_id}, but now \
                         no collections exist"
                    ));
                }
                None => {
                    // Had no collections; still have no collections.
                    return InventoryLoadStatus::NoCollections;
                }
            },
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log,
                    "failed to read latest inventory collection ID";
                    &err
                );
                return InventoryLoadStatus::Error(format!(
                    "failed to read latest inventory collection ID: {err}"
                ));
            }
        };

        // Have we already loaded this collection?
        match old {
            Some((old_id, old_time_started)) if old_id == latest_id => {
                debug!(log, "latest inventory collection is unchanged");
                return InventoryLoadStatus::Loaded {
                    collection_id: old_id,
                    time_started: old_time_started,
                    time_loaded,
                };
            }
            _ => (),
        }

        // It's new - load the full collection.
        let collection = match self
            .datastore
            .inventory_collection_read(opctx, latest_id)
            .await
        {
            Ok(collection) => collection,
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log,
                    "failed to read inventory collection {latest_id}";
                    &err
                );
                return InventoryLoadStatus::Error(format!(
                    "failed to read inventory collection {latest_id}: {err}"
                ));
            }
        };

        let new_id = collection.id;
        let new_time_started = collection.time_started;
        let collection = Arc::new(collection);
        self.tx.send_modify(|c| {
            *c = Some(collection);
        });

        InventoryLoadStatus::Loaded {
            collection_id: new_id,
            time_started: new_time_started,
            time_loaded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_inventory::CollectionBuilder;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_inventory_loader() {
        let logctx = dev::test_setup_log("test_inventory_loader");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (tx, mut rx) = watch::channel(None);
        let loader = InventoryLoader::new(datastore.clone(), tx);

        // Initial state is `None`
        assert_eq!(*rx.borrow_and_update(), None);

        // We haven't inserted any collections.
        let status = loader.load_if_needed(opctx).await;
        assert_eq!(status, InventoryLoadStatus::NoCollections);
        assert!(!rx.has_changed().unwrap());

        // Insert a collection and activate; we should load it.
        let coll0 = Arc::new(CollectionBuilder::new("test").build());
        datastore
            .inventory_insert_collection(opctx, &coll0)
            .await
            .expect("inserted collection");
        let status = loader.load_if_needed(opctx).await;
        let first_time_loaded = match status {
            InventoryLoadStatus::Loaded {
                collection_id,
                time_started,
                time_loaded,
            } => {
                assert_eq!(collection_id, coll0.id);
                assert_eq!(time_started, coll0.time_started);
                time_loaded
            }
            InventoryLoadStatus::Error(_)
            | InventoryLoadStatus::NoCollections => {
                panic!("unexpected status: {status:?}")
            }
        };
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), Some(coll0.clone()));

        // Activating again should result in a later `time_loaded` but still the
        // same collection.
        let status = loader.load_if_needed(opctx).await;
        match status {
            InventoryLoadStatus::Loaded {
                collection_id,
                time_started,
                time_loaded,
            } => {
                assert_eq!(collection_id, coll0.id);
                assert_eq!(time_started, coll0.time_started);
                assert!(time_loaded > first_time_loaded);
            }
            InventoryLoadStatus::Error(_)
            | InventoryLoadStatus::NoCollections => {
                panic!("unexpected status: {status:?}")
            }
        }
        assert!(!rx.has_changed().unwrap());

        // Insert two more collections.
        let coll1 = CollectionBuilder::new("test").build();
        datastore
            .inventory_insert_collection(opctx, &coll1)
            .await
            .expect("inserted collection");
        let coll2 = Arc::new(CollectionBuilder::new("test").build());
        datastore
            .inventory_insert_collection(opctx, &coll2)
            .await
            .expect("inserted collection");

        // Activating should find the latest.
        let status = loader.load_if_needed(opctx).await;
        match status {
            InventoryLoadStatus::Loaded {
                collection_id,
                time_started,
                time_loaded: _,
            } => {
                assert_eq!(collection_id, coll2.id);
                assert_eq!(time_started, coll2.time_started);
            }
            InventoryLoadStatus::Error(_)
            | InventoryLoadStatus::NoCollections => {
                panic!("unexpected status: {status:?}")
            }
        }
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), Some(coll2.clone()));

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
