// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that transitions stale incomplete audit log entries to
//! `result_kind = timeout`, making them visible through the audit log API.

use crate::app::background::BackgroundTask;
use chrono::TimeDelta;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::AuditLogTimeoutIncompleteStatus;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub struct AuditLogTimeoutIncomplete {
    datastore: Arc<DataStore>,
    timeout: Duration,
    max_update_per_activation: u32,
}

impl AuditLogTimeoutIncomplete {
    pub fn new(
        datastore: Arc<DataStore>,
        timeout: Duration,
        max_update_per_activation: u32,
    ) -> Self {
        Self { datastore, timeout, max_update_per_activation }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> AuditLogTimeoutIncompleteStatus {
        let timeout_delta = match TimeDelta::from_std(self.timeout) {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("invalid timeout duration: {e:#}");
                slog::error!(&opctx.log, "{msg}");
                return AuditLogTimeoutIncompleteStatus {
                    timed_out: 0,
                    cutoff: Utc::now(),
                    max_update_per_activation: self.max_update_per_activation,
                    error: Some(msg),
                };
            }
        };

        let cutoff = Utc::now() - timeout_delta;
        let timed_out = match self
            .datastore
            .audit_log_timeout_incomplete(
                opctx,
                cutoff,
                self.max_update_per_activation,
            )
            .await
        {
            Ok(count) => count,
            Err(err) => {
                let msg =
                    format!("audit log timeout incomplete failed: {err:#}");
                slog::error!(&opctx.log, "{msg}");
                return AuditLogTimeoutIncompleteStatus {
                    timed_out: 0,
                    cutoff,
                    max_update_per_activation: self.max_update_per_activation,
                    error: Some(msg),
                };
            }
        };

        if timed_out > 0 {
            slog::warn!(
                &opctx.log,
                "audit log timeout: timed out {timed_out} incomplete \
                 entries (may indicate Nexus crashes or bugs)";
                "cutoff" => %cutoff,
                "limit" => self.max_update_per_activation,
            );
        } else {
            slog::debug!(
                &opctx.log,
                "audit log timeout: no stale entries found";
                "cutoff" => %cutoff,
            );
        }

        AuditLogTimeoutIncompleteStatus {
            timed_out,
            cutoff,
            max_update_per_activation: self.max_update_per_activation,
            error: None,
        }
    }
}

impl BackgroundTask for AuditLogTimeoutIncomplete {
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
    use chrono::TimeDelta;
    use chrono::Utc;
    use diesel::sql_types;
    use nexus_db_model::{AuditLogActor, AuditLogEntryInitParams};
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    fn make_entry_params(request_id: &str) -> AuditLogEntryInitParams {
        AuditLogEntryInitParams {
            request_id: request_id.to_string(),
            operation_id: "project_create".to_string(),
            request_uri: "/v1/projects".to_string(),
            source_ip: "1.1.1.1".parse().unwrap(),
            user_agent: None,
            actor: AuditLogActor::Unauthenticated,
            auth_method: None,
            credential_id: None,
        }
    }

    async fn set_time_started(
        datastore: &DataStore,
        id: Uuid,
        time_started: chrono::DateTime<Utc>,
    ) {
        use async_bb8_diesel::AsyncRunQueryDsl;
        diesel::sql_query(
            "UPDATE omicron.public.audit_log \
             SET time_started = $1 WHERE id = $2",
        )
        .bind::<sql_types::Timestamptz, _>(time_started)
        .bind::<sql_types::Uuid, _>(id)
        .execute_async(&*datastore.pool_connection_for_tests().await.unwrap())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_audit_log_timeout_incomplete_activation() {
        let logctx =
            dev::test_setup_log("test_audit_log_timeout_incomplete_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let now = Utc::now();
        let two_hours_ago = now - TimeDelta::try_hours(2).unwrap();

        // Create 3 stale incomplete entries
        for i in 0..3 {
            let entry = datastore
                .audit_log_entry_init(
                    opctx,
                    make_entry_params(&format!("req-{i}")).into(),
                )
                .await
                .unwrap();
            set_time_started(datastore, entry.id, two_hours_ago).await;
        }

        // Create 1 recent incomplete entry (should not be timed out)
        datastore
            .audit_log_entry_init(opctx, make_entry_params("recent-req").into())
            .await
            .unwrap();

        // timeout = 1 hour, max_update_per_activation = 100
        let mut task = AuditLogTimeoutIncomplete::new(
            datastore.clone(),
            Duration::from_secs(3600),
            100,
        );

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 3);
        assert!(status.error.is_none());

        // Second activation should find nothing
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 0);
        assert!(status.error.is_none());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_audit_log_timeout_incomplete_respects_max_update_per_activation()
     {
        let logctx = dev::test_setup_log(
            "test_audit_log_timeout_incomplete_respects_max_update_per_activation",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let now = Utc::now();
        let two_hours_ago = now - TimeDelta::try_hours(2).unwrap();

        // Create 5 stale entries
        for i in 0..5 {
            let entry = datastore
                .audit_log_entry_init(
                    opctx,
                    make_entry_params(&format!("req-{i}")).into(),
                )
                .await
                .unwrap();
            set_time_started(datastore, entry.id, two_hours_ago).await;
        }

        // max_update_per_activation = 2 — should process 2 per activation.
        let mut task = AuditLogTimeoutIncomplete::new(
            datastore.clone(),
            Duration::from_secs(3600),
            2,
        );

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 2);
        assert!(status.error.is_none());

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 2);
        assert!(status.error.is_none());

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 1);
        assert!(status.error.is_none());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
