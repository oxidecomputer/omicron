// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that hard-deletes expired console sessions.

use crate::app::background::BackgroundTask;
use chrono::TimeDelta;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::SessionCleanupStatus;
use serde_json::json;
use std::sync::Arc;

pub struct SessionCleanup {
    datastore: Arc<DataStore>,
    absolute_timeout: TimeDelta,
    max_delete_per_activation: u32,
}

impl SessionCleanup {
    pub fn new(
        datastore: Arc<DataStore>,
        absolute_timeout: TimeDelta,
        max_delete_per_activation: u32,
    ) -> Self {
        Self { datastore, absolute_timeout, max_delete_per_activation }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> SessionCleanupStatus {
        let cutoff = Utc::now() - self.absolute_timeout;
        let limit = self.max_delete_per_activation;
        let (deleted, error) = match self
            .datastore
            .session_cleanup_batch(opctx, cutoff, limit)
            .await
        {
            Ok(deleted) => {
                if deleted > 0 {
                    slog::info!(
                        &opctx.log,
                        "session cleanup deleted {deleted} expired sessions";
                        "cutoff" => %cutoff,
                        "limit" => limit,
                    );
                } else {
                    slog::debug!(
                        &opctx.log,
                        "session cleanup found no expired sessions";
                        "cutoff" => %cutoff,
                    );
                }
                (deleted, None)
            }
            Err(err) => {
                let msg = format!("session cleanup failed: {err:#}");
                slog::error!(&opctx.log, "{msg}");
                (0, Some(msg))
            }
        };

        SessionCleanupStatus { deleted, cutoff, limit, error }
    }
}

impl BackgroundTask for SessionCleanup {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    json!({ "error": format!("failed to serialize status: {err}") })
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use nexus_db_model::ConsoleSession;
    use nexus_db_queries::authn;
    use nexus_db_queries::authz;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SiloUserUuid;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_session_cleanup_activation() {
        let logctx = dev::test_setup_log("test_session_cleanup_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // session_create requires external_authn context
        let authn_opctx = OpContext::for_background(
            logctx.log.new(slog::o!("component" => "TestExternalAuthn")),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::external_authn(),
            Arc::clone(datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        let silo_user_id = SiloUserUuid::new_v4();
        let now = Utc::now();

        // Create an old session (48 hours ago) — should be cleaned up
        let old_session = ConsoleSession::new_with_times(
            "old_token".to_string(),
            silo_user_id,
            now - TimeDelta::try_hours(48).unwrap(),
            now - TimeDelta::try_hours(48).unwrap(),
        );
        datastore.session_create(&authn_opctx, old_session).await.unwrap();

        // Create a recent session (1 hour ago) — should be preserved
        let new_session = ConsoleSession::new_with_times(
            "new_token".to_string(),
            silo_user_id,
            now - TimeDelta::try_hours(1).unwrap(),
            now - TimeDelta::try_hours(1).unwrap(),
        );
        datastore.session_create(&authn_opctx, new_session).await.unwrap();

        // absolute_timeout = 24 hours
        let mut task = SessionCleanup::new(
            datastore.clone(),
            TimeDelta::try_hours(24).unwrap(),
            10_000,
        );

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 1);
        assert!(status.error.is_none());
        assert_eq!(status.limit, 10_000);

        // Second activation should find nothing
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 0);
        assert!(status.error.is_none());

        // The recent session should still exist
        let (_, fetched) = datastore
            .session_lookup_by_token(&authn_opctx, "new_token".to_string())
            .await
            .unwrap();
        assert_eq!(fetched.token, "new_token");

        // The old session should be gone
        datastore
            .session_lookup_by_token(&authn_opctx, "old_token".to_string())
            .await
            .expect_err("old session should have been deleted");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_session_cleanup_respects_limit() {
        let logctx = dev::test_setup_log("test_session_cleanup_respects_limit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let authn_opctx = OpContext::for_background(
            logctx.log.new(slog::o!("component" => "TestExternalAuthn")),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::external_authn(),
            Arc::clone(datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        let silo_user_id = SiloUserUuid::new_v4();
        let now = Utc::now();

        // Create 5 old sessions
        for i in 0..5 {
            let session = ConsoleSession::new_with_times(
                format!("old_token_{i}"),
                silo_user_id,
                now - TimeDelta::try_hours(48 + i).unwrap(),
                now - TimeDelta::try_hours(48 + i).unwrap(),
            );
            datastore.session_create(&authn_opctx, session).await.unwrap();
        }

        // Use a limit of 2 — only 2 should be deleted per activation
        let mut task = SessionCleanup::new(
            datastore.clone(),
            TimeDelta::try_hours(24).unwrap(),
            2,
        );

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 2);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 2);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 1);

        // All gone now
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 0);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
