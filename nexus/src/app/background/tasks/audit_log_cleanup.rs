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
    max_deleted_per_activation: u32,
}

impl AuditLogCleanup {
    pub fn new(
        datastore: Arc<DataStore>,
        retention_days: NonZeroU32,
        max_deleted_per_activation: u32,
    ) -> Self {
        Self { datastore, retention_days, max_deleted_per_activation }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> AuditLogCleanupStatus {
        let cutoff = TimeDelta::try_days(i64::from(self.retention_days.get()))
            .and_then(|d| Utc::now().checked_sub_signed(d));
        let cutoff = match cutoff {
            Some(c) => c,
            None => {
                let msg = format!(
                    "retention_days {} overflows date arithmetic",
                    self.retention_days,
                );
                slog::error!(&opctx.log, "{msg}");
                return AuditLogCleanupStatus {
                    rows_deleted: 0,
                    cutoff: Utc::now(),
                    max_deleted_per_activation: self.max_deleted_per_activation,
                    error: Some(msg),
                };
            }
        };

        let rows_deleted = match self
            .datastore
            .audit_log_cleanup(opctx, cutoff, self.max_deleted_per_activation)
            .await
        {
            Ok(count) => count,
            Err(err) => {
                let msg = format!("audit log cleanup failed: {err:#}");
                slog::error!(&opctx.log, "{msg}");
                return AuditLogCleanupStatus {
                    rows_deleted: 0,
                    cutoff,
                    max_deleted_per_activation: self.max_deleted_per_activation,
                    error: Some(msg),
                };
            }
        };

        if rows_deleted > 0 {
            slog::info!(
                &opctx.log,
                "audit log cleanup: deleted {rows_deleted} old entries";
                "cutoff" => %cutoff,
                "limit" => self.max_deleted_per_activation,
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
            max_deleted_per_activation: self.max_deleted_per_activation,
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
    use std::collections::BTreeSet;

    use super::*;
    use chrono::TimeDelta;
    use chrono::Utc;
    use nexus_db_model::{
        AuditLogActor, AuditLogCompletion, AuditLogEntryInitParams,
    };
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    async fn get_audit_log_ids(datastore: &DataStore) -> BTreeSet<Uuid> {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use diesel::prelude::*;
        use nexus_db_schema::schema::audit_log;
        audit_log::table
            // fake filter required to get around the table scan rule
            .filter(audit_log::id.ne(uuid::Uuid::nil()))
            .select(audit_log::id)
            .load_async::<Uuid>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .expect("could not load audit log ids")
            .into_iter()
            .collect()
    }

    fn make_entry(request_id: &str) -> AuditLogEntryInitParams {
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
        use diesel::prelude::*;
        use nexus_db_schema::schema::audit_log;
        diesel::update(audit_log::table)
            .filter(audit_log::id.eq(id))
            .set(audit_log::time_completed.eq(Some(time_completed)))
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .expect("could not set time_completed");
    }

    #[tokio::test]
    async fn test_audit_log_cleanup_activation() {
        let logctx = dev::test_setup_log("test_audit_log_cleanup_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let now = Utc::now();
        // The task uses retention_days=7, so cutoff ≈ now - 7 days.
        // Give each old entry a distinct time_completed so we can verify
        // oldest-first deletion order.
        let completion = AuditLogCompletion::Success { http_status_code: 200 };

        // 5 old entries with distinct times, all before cutoff.
        // Oldest first: 12d, 11d, 10d, 9d, 8d ago.
        let mut old_ids = Vec::new();
        for i in 0..5u32 {
            let age_days = 12 - i; // 12, 11, 10, 9, 8
            let entry = datastore
                .audit_log_entry_init(
                    opctx,
                    make_entry(&format!("old-{i}")).into(),
                )
                .await
                .unwrap();
            datastore
                .audit_log_entry_complete(
                    opctx,
                    &entry,
                    completion.clone().into(),
                )
                .await
                .unwrap();
            let completed_at =
                now - TimeDelta::try_days(i64::from(age_days)).unwrap();
            set_time_completed(datastore, entry.id, completed_at).await;
            old_ids.push(entry.id);
        }

        // 1 completed entry just inside the retention window (6d ago).
        // With retention_days=7, cutoff ≈ now - 7d, so 6d ago is safely
        // after the cutoff and must not be deleted.
        let within_retention = datastore
            .audit_log_entry_init(opctx, make_entry("within-retention").into())
            .await
            .unwrap();
        datastore
            .audit_log_entry_complete(
                opctx,
                &within_retention,
                completion.clone().into(),
            )
            .await
            .unwrap();
        let six_days_ago = now - TimeDelta::try_days(6).unwrap();
        set_time_completed(datastore, within_retention.id, six_days_ago).await;

        // 1 incomplete entry (should not be deleted)
        let incomplete = datastore
            .audit_log_entry_init(opctx, make_entry("incomplete-req").into())
            .await
            .unwrap();

        let expected_survivors: BTreeSet<Uuid> =
            [within_retention.id, incomplete.id].into();

        assert_eq!(get_audit_log_ids(&datastore).await.len(), 7);

        // max_deleted_per_activation = 3, so it takes two activations to
        // delete all 5 old entries
        let mut task = AuditLogCleanup::new(
            datastore.clone(),
            NonZeroU32::new(7).unwrap(),
            3,
        );

        // First activation deletes the 3 oldest (12d, 11d, 10d ago).
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 3);
        assert!(status.error.is_none());

        // The reported cutoff should be between our old entries and the
        // within-retention entry.
        let eight_days_ago = now - TimeDelta::try_days(8).unwrap();
        assert!(
            status.cutoff > eight_days_ago && status.cutoff < six_days_ago,
            "cutoff {} should be between 8d ago ({}) and 6d ago ({})",
            status.cutoff,
            eight_days_ago,
            six_days_ago,
        );

        let remaining = get_audit_log_ids(&datastore).await;
        assert_eq!(remaining.len(), 4);
        // The 3 oldest should be gone
        for &id in &old_ids[..3] {
            assert!(!remaining.contains(&id), "old entry should be deleted");
        }
        // The 2 newest old entries should still be present
        for &id in &old_ids[3..] {
            assert!(remaining.contains(&id), "newer old entry should remain");
        }
        assert!(expected_survivors.is_subset(&remaining));

        // Second activation deletes remaining 2 old (9d, 8d ago).
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 2);
        assert!(status.error.is_none());
        assert_eq!(get_audit_log_ids(&datastore).await, expected_survivors);

        // Third activation: within-retention entry survives.
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 0);
        assert!(status.error.is_none());
        assert_eq!(get_audit_log_ids(&datastore).await, expected_survivors);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_audit_log_cleanup_retention_days_overflow() {
        let logctx = dev::test_setup_log(
            "test_audit_log_cleanup_retention_days_overflow",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, _datastore) = (db.opctx(), db.datastore());

        let mut task = AuditLogCleanup::new(
            db.datastore().clone(),
            NonZeroU32::new(u32::MAX).unwrap(),
            100,
        );
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.rows_deleted, 0);
        assert!(
            status.error.as_ref().unwrap().contains("overflows"),
            "expected overflow error, got: {:?}",
            status.error,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
