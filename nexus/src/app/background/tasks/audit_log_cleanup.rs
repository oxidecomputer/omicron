// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that hard-deletes completed audit log entries older than
//! the retention period.

use crate::app::background::BackgroundTask;
use chrono::TimeDelta;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::AuditLogCleanupStatus;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::Arc;

pub struct AuditLogCleanup {
    datastore: Arc<DataStore>,
    retention_days: NonZeroU32,
    max_delete_per_activation: u32,
}

impl AuditLogCleanup {
    pub fn new(
        datastore: Arc<DataStore>,
        retention_days: NonZeroU32,
        max_delete_per_activation: u32,
    ) -> Self {
        Self { datastore, retention_days, max_delete_per_activation }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> AuditLogCleanupStatus {
        let retention_delta =
            TimeDelta::try_days(i64::from(self.retention_days.get()))
                .expect("retention_days fits in TimeDelta");
        let cutoff = Utc::now() - retention_delta;

        let rows_deleted = match self
            .datastore
            .audit_log_cleanup(opctx, cutoff, self.max_delete_per_activation)
            .await
        {
            Ok(count) => count,
            Err(err) => {
                let msg = format!("audit log cleanup failed: {err:#}");
                slog::error!(&opctx.log, "{msg}");
                return AuditLogCleanupStatus {
                    rows_deleted: 0,
                    cutoff,
                    max_delete_per_activation: self.max_delete_per_activation,
                    error: Some(msg),
                };
            }
        };

        if rows_deleted > 0 {
            slog::info!(
                &opctx.log,
                "audit log cleanup: deleted {rows_deleted} old entries";
                "cutoff" => %cutoff,
                "limit" => self.max_delete_per_activation,
            );
        } else {
            slog::debug!(
                &opctx.log,
                "audit log cleanup: no old entries to delete";
                "cutoff" => %cutoff,
            );
        }

        AuditLogCleanupStatus {
            rows_deleted,
            cutoff,
            max_delete_per_activation: self.max_delete_per_activation,
            error: None,
        }
    }
}

impl BackgroundTask for AuditLogCleanup {
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
    use nexus_db_model::{
        AuditLogActor, AuditLogCompletion, AuditLogEntryInitParams,
    };
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

    async fn set_time_completed(
        datastore: &DataStore,
        id: Uuid,
        time_completed: chrono::DateTime<Utc>,
    ) {
        use async_bb8_diesel::AsyncRunQueryDsl;
        diesel::sql_query(
            "UPDATE omicron.public.audit_log \
             SET time_completed = $1 WHERE id = $2",
        )
        .bind::<sql_types::Timestamptz, _>(time_completed)
        .bind::<sql_types::Uuid, _>(id)
        .execute_async(&*datastore.pool_connection_for_tests().await.unwrap())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_audit_log_cleanup_activation() {
        let logctx = dev::test_setup_log("test_audit_log_cleanup_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let now = Utc::now();
        let eight_days_ago = now - TimeDelta::try_days(8).unwrap();

        // Create 3 old completed entries
        for i in 0..3 {
            let entry = datastore
                .audit_log_entry_init(
                    opctx,
                    make_entry_params(&format!("req-{i}")).into(),
                )
                .await
                .unwrap();
            datastore
                .audit_log_entry_complete(
                    opctx,
                    &entry,
                    AuditLogCompletion::Success { http_status_code: 200 }
                        .into(),
                )
                .await
                .unwrap();
            set_time_completed(datastore, entry.id, eight_days_ago).await;
        }

        // Create 1 recent completed entry (should not be deleted)
        let recent = datastore
            .audit_log_entry_init(opctx, make_entry_params("recent-req").into())
            .await
            .unwrap();
        datastore
            .audit_log_entry_complete(
                opctx,
                &recent,
                AuditLogCompletion::Success { http_status_code: 200 }.into(),
            )
            .await
            .unwrap();

        // Create 1 incomplete entry (should not be deleted)
        datastore
            .audit_log_entry_init(
                opctx,
                make_entry_params("incomplete-req").into(),
            )
            .await
            .unwrap();

        let mut task = AuditLogCleanup::new(
            datastore.clone(),
            NonZeroU32::new(7).unwrap(),
            100,
        );

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 3);
        assert!(status.error.is_none());

        // Second activation should find nothing
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 0);
        assert!(status.error.is_none());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_audit_log_cleanup_respects_max_delete_per_activation() {
        let logctx = dev::test_setup_log(
            "test_audit_log_cleanup_respects_max_delete_per_activation",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let now = Utc::now();
        let eight_days_ago = now - TimeDelta::try_days(8).unwrap();

        // Create 5 old completed entries
        for i in 0..5 {
            let entry = datastore
                .audit_log_entry_init(
                    opctx,
                    make_entry_params(&format!("req-{i}")).into(),
                )
                .await
                .unwrap();
            datastore
                .audit_log_entry_complete(
                    opctx,
                    &entry,
                    AuditLogCompletion::Success { http_status_code: 200 }
                        .into(),
                )
                .await
                .unwrap();
            set_time_completed(datastore, entry.id, eight_days_ago).await;
        }

        // max_delete_per_activation = 2
        let mut task = AuditLogCleanup::new(
            datastore.clone(),
            NonZeroU32::new(7).unwrap(),
            2,
        );

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 2);
        assert!(status.error.is_none());

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 2);
        assert!(status.error.is_none());

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 1);
        assert!(status.error.is_none());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
