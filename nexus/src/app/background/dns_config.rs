// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of DNS configuration

use super::common::BackgroundTask;
use dns_service_client::types::DnsConfigParams;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that keeps track of the latest configuration for a DNS group
pub struct DnsConfigWatcher {
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    last: Option<DnsConfigParams>,
    tx: watch::Sender<Option<DnsConfigParams>>,
    rx: watch::Receiver<Option<DnsConfigParams>>,
}

impl DnsConfigWatcher {
    pub fn new(
        datastore: Arc<DataStore>,
        dns_group: DnsGroup,
    ) -> DnsConfigWatcher {
        let (tx, rx) = watch::channel(None);
        DnsConfigWatcher { datastore, dns_group, last: None, tx, rx }
    }

    /// Exposes the latest DNS configuration for this DNS group
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// configuration or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<DnsConfigParams>> {
        self.rx.clone()
    }
}

impl BackgroundTask for DnsConfigWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // Set up a logger for this activation that includes metadata about
            // the current generation.
            let log = match &self.last {
                None => opctx.log.clone(),
                Some(old) => opctx.log.new(o!(
                    "current_generation" => old.generation,
                    "current_time_created" => old.time_created.to_string(),
                )),
            };

            // Read the latest configuration for this DNS group.
            let result =
                self.datastore.dns_config_read(opctx, self.dns_group).await;

            // Decide what to do with the result.
            match (&self.last, result) {
                (_, Err(error)) => {
                    // We failed to read the DNS configuration.  There's nothing
                    // to do but log an error.  We'll retry when we're activated
                    // again.
                    let message = format!("{:#}", error);
                    warn!(
                        &log,
                        "failed to read DNS config";
                        "error" => &message,
                    );
                    json!({
                        "error":
                            format!("failed to read DNS config: {}", message)
                    })
                }

                (None, Ok(new_config)) => {
                    // We've found a DNS configuration for the first time.
                    // Save it and notify any watchers.
                    let generation = new_config.generation;
                    info!(
                        &log,
                        "found latest generation (first find)";
                        "generation" => new_config.generation
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                    json!({ "generation": generation })
                }

                (Some(old), Ok(new)) => {
                    if old.generation > new.generation {
                        // We previously had a generation that's newer than what
                        // we just read.  This should never happen because we
                        // never remove the latest generation.
                        let message = format!(
                            "found latest DNS generation ({}) is older \
                            than the one we already know about ({})",
                            new.generation, old.generation
                        );

                        error!(&log, "{}", message);
                        json!({ "error": message })
                    } else if old.generation == new.generation {
                        if *old != new {
                            // We found the same generation _number_ as what we
                            // already had, but the contents were different.
                            // This should never happen because generations are
                            // immutable once created.
                            let message = format!(
                                "found DNS config at generation {} that does \
                                not match the config that we already have for \
                                the same generation",
                                new.generation
                            );
                            error!(&log, "{}", message);
                            json!({ "error": message })
                        } else {
                            // We found a DNS configuration and it exactly
                            // matches what we already had.  This is the common
                            // case when we're activated by a timeout.
                            debug!(
                                &log,
                                "found latest DNS generation (unchanged)";
                                "generation" => new.generation,
                            );
                            json!({ "generation": new.generation })
                        }
                    } else {
                        // We found a DNS configuration that's newer than what
                        // we currently have.  Save it and notify any watchers.
                        let generation = new.generation;
                        info!(
                            &log,
                            "found latest DNS generation (newer than we had)";
                            "generation" => new.generation,
                            "time_created" => new.time_created.to_string(),
                            "old_generation" => old.generation,
                            "old_time_created" => old.time_created.to_string(),
                        );
                        self.last = Some(new.clone());
                        self.tx.send_replace(Some(new));
                        json!({ "generation": generation })
                    }
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::dns_config::DnsConfigWatcher;
    use crate::app::background::init::test::write_test_dns_generation;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use nexus_db_model::DnsGroup;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use serde_json::json;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Verify the initial state.
        let mut task =
            DnsConfigWatcher::new(datastore.clone(), DnsGroup::Internal);
        let watcher = task.watcher();
        assert_matches!(*watcher.borrow(), None);

        // The datastore from the ControlPlaneTestContext is initialized with a
        // DNS config with generation 1.
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 1);
        assert_eq!(value, json!({ "generation": 1 }));

        // Now write generation 2, activate again, and verify that the update
        // was sent to the watcher.
        write_test_dns_generation(&opctx, &datastore).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 1);
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);
        assert_eq!(value, json!({ "generation": 2 }));

        // Activate again and make sure it does nothing.
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);
        assert_eq!(value, json!({ "generation": 2 }));

        // Simulate the configuration going backwards.  This should not be
        // possible, but it's easy to check that we at least don't panic.
        {
            use nexus_db_queries::db::schema::dns_version::dsl;
            diesel::delete(dsl::dns_version.filter(dsl::version.eq(2)))
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);
        assert_eq!(
            value,
            json!({
                "error": "found latest DNS generation (1) is older \
                    than the one we already know about (2)"
            })
        );

        // Similarly, wipe all of the state and verify that we handle that okay.
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .transaction_retry_wrapper("dns_config_test_basic")
            .transaction(&conn, |conn| async move {
                conn.batch_execute_async(
                    nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
                )
                .await
                .unwrap();
                diesel::delete(
                    nexus_db_queries::db::schema::dns_version::dsl::dns_version,
                )
                .execute_async(&conn)
                .await
                .unwrap();
                diesel::delete(
                    nexus_db_queries::db::schema::dns_name::dsl::dns_name,
                )
                .execute_async(&conn)
                .await
                .unwrap();
                diesel::delete(
                    nexus_db_queries::db::schema::dns_zone::dsl::dns_zone,
                )
                .execute_async(&conn)
                .await
                .unwrap();
                Ok(())
            })
            .await
            .unwrap();

        let _ = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);

        // Verify that a new watcher also handles this okay. (i.e., that we can
        // come up with no state in the database).
        let mut task =
            DnsConfigWatcher::new(datastore.clone(), DnsGroup::Internal);
        let watcher = task.watcher();
        assert_matches!(*watcher.borrow(), None);
        let _ = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref(), None);

        // TODO-coverage It would be nice to verify the behavior when the
        // database is offline.  As of this writing, it works correctly: we
        // maintain what we currently have.  However, it takes 30s for bb8 to
        // time out waiting for a connection.  That's a little much for an
        // automatied test.
    }
}
