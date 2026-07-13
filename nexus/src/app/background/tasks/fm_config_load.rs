// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for loading the current fault management configuration
//! from the DB.

use crate::app::background::BackgroundTask;
use chrono::DateTime;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::fm::FmConfigView;
use nexus_types::internal_api::background::FmConfigLoadStatus as Status;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that tracks the current FM config from the DB.
///
/// The watch channel contains `None` until the first successful load; because
/// the `fm_config` table is initialized with a default configuration
/// when the database is populated, every successful load thereafter yields
/// `Some`.
pub struct FmConfigLoader {
    datastore: Arc<DataStore>,
    tx: watch::Sender<Option<FmConfigView>>,
    time_updated: DateTime<Utc>,
}

impl FmConfigLoader {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self { datastore, tx, time_updated: Utc::now() }
    }

    #[allow(dead_code)] // subsequent PRs will consume this
    pub fn watcher(&self) -> watch::Receiver<Option<FmConfigView>> {
        self.tx.subscribe()
    }

    async fn load(&mut self, opctx: &OpContext) -> Status {
        let config = match self.datastore.fm_config_get_latest(opctx).await {
            Ok(config) => config,
            Err(err) => {
                let error = InlineErrorChain::new(&err);
                slog::error!(opctx.log, "failed to load FM config"; &error);
                return Status::Error(error.to_string());
            }
        };
        let time_loaded = Utc::now();
        let updated = self.tx.send_if_modified(|current| {
            if current.as_ref() != Some(&config) {
                *current = Some(config);
                true
            } else {
                false
            }
        });
        if updated {
            self.time_updated = time_loaded;
            info!(
                opctx.log,
                "loaded new FM config version";
                "version" => %config.config.version,
                "time_modified" => %config.time_modified,
            );
        } else {
            debug!(
                opctx.log,
                "FM config has not changed";
                "version" => %config.config.version,
            );
        }
        Status::Loaded { config, updated, time_updated: self.time_updated }
    }
}

impl BackgroundTask for FmConfigLoader {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
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
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::BackgroundTask;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_types::fm::FmConfigParam;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_load_fm_config() {
        let logctx = dev::test_setup_log("test_load_fm_config");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut task = FmConfigLoader::new(datastore.clone());
        let mut rx = task.watcher();

        // Initial state should be `None`: nothing loaded yet.
        assert_eq!(*rx.borrow_and_update(), None);

        // The first activation should load initial config created by
        // `dbinit.sql`.
        let initial = datastore
            .fm_config_get_latest(opctx)
            .await
            .expect("initial config must exist");
        assert_eq!(initial.config.version.get(), 1);

        let status = task.activate(&opctx).await;
        let status = serde_json::from_value::<Status>(status).unwrap();
        let Status::Loaded { config, updated, time_updated } = status else {
            panic!("expected Status::Loaded, got: {status:?}");
        };
        assert!(updated);
        assert_eq!(config, initial);
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), Some(initial));

        // Activating again should not change anything.
        let status = task.activate(&opctx).await;
        let status = serde_json::from_value::<Status>(status).unwrap();
        assert_eq!(
            status,
            Status::Loaded { config: initial, updated: false, time_updated }
        );
        assert!(!rx.has_changed().unwrap());

        // Insert a new config version; the next activation should load it.
        let param = FmConfigParam {
            version: 2,
            sitrep_limit: 500,
            sitrep_deletion_threshold: 400,
        };
        datastore
            .fm_config_insert_latest_version(opctx, param)
            .await
            .expect("inserting version 2 should succeed");

        let status = task.activate(&opctx).await;
        let status = serde_json::from_value::<Status>(status).unwrap();
        let Status::Loaded { config: loaded, updated: true, .. } = status
        else {
            panic!("expected updated Status::Loaded, got {status:?}");
        };
        assert_eq!(loaded.config.version.get(), 2);
        assert_eq!(loaded.config.sitrep_limit.get(), 500);
        assert_eq!(loaded.config.sitrep_deletion_threshold.get(), 400);
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), Some(loaded));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
