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
    timeout: TimeDelta,
    max_timed_out_per_activation: u32,
}

impl AuditLogTimeoutIncomplete {
    pub fn new(
        datastore: Arc<DataStore>,
        timeout: Duration,
        max_timed_out_per_activation: u32,
    ) -> Self {
        let timeout = TimeDelta::from_std(timeout)
            .expect("timeout must be representable as a TimeDelta");
        Self { datastore, timeout, max_timed_out_per_activation }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> AuditLogTimeoutIncompleteStatus {
        let cutoff = Utc::now() - self.timeout;
        let timed_out = match self
            .datastore
            .audit_log_timeout_incomplete(
                opctx,
                cutoff,
                self.max_timed_out_per_activation,
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
                    max_timed_out_per_activation: self
                        .max_timed_out_per_activation,
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
                "limit" => self.max_timed_out_per_activation,
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
            max_timed_out_per_activation: self.max_timed_out_per_activation,
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
    use nexus_db_model::{
        AuditLogActor, AuditLogCompletion, AuditLogEntry,
        AuditLogEntryInitParams, AuditLogResultKind,
    };
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    async fn get_completed_entry(
        datastore: &DataStore,
        id: Uuid,
    ) -> AuditLogEntry {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use diesel::prelude::*;
        use nexus_db_schema::schema::audit_log_complete;
        audit_log_complete::table
            .filter(audit_log_complete::id.eq(id))
            .select(AuditLogEntry::as_select())
            .first_async(&*datastore.pool_connection_for_tests().await.unwrap())
            .await
            .expect("could not load audit log entry")
    }

    async fn assert_incomplete(datastore: &DataStore, id: Uuid) {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use diesel::prelude::*;
        use nexus_db_schema::schema::audit_log;
        let time_completed: Option<chrono::DateTime<Utc>> = audit_log::table
            .filter(audit_log::id.eq(id))
            .select(audit_log::time_completed)
            .first_async(&*datastore.pool_connection_for_tests().await.unwrap())
            .await
            .expect("could not load audit log entry");
        assert!(
            time_completed.is_none(),
            "expected entry {id} to be incomplete, \
             but time_completed = {time_completed:?}"
        );
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

    async fn set_time_started(
        datastore: &DataStore,
        id: Uuid,
        time_started: chrono::DateTime<Utc>,
    ) {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use diesel::prelude::*;
        use nexus_db_schema::schema::audit_log;
        diesel::update(audit_log::table)
            .filter(audit_log::id.eq(id))
            .set(audit_log::time_started.eq(time_started))
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .expect("could not set time_started");
    }

    async fn assert_timed_out(datastore: &DataStore, id: Uuid) {
        let entry = get_completed_entry(datastore, id).await;
        assert_eq!(entry.result_kind, AuditLogResultKind::Timeout);
        assert!(entry.http_status_code.is_none());
        assert!(entry.error_code.is_none());
        assert!(entry.error_message.is_none());
    }

    #[tokio::test]
    async fn test_audit_log_timeout_incomplete_activation() {
        let logctx =
            dev::test_setup_log("test_audit_log_timeout_incomplete_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let now = Utc::now();

        // Create 5 stale incomplete entries at different times in the past.
        // The query orders by (time_started, id), so they'll be processed
        // oldest first: req-0 through req-4.
        let mut stale_ids = Vec::new();
        for i in 0..5u32 {
            let age = TimeDelta::try_hours(i64::from(6 - i)).unwrap();
            let params = make_entry(&format!("req-{i}"));
            let entry = datastore
                .audit_log_entry_init(opctx, params.into())
                .await
                .unwrap();
            set_time_started(datastore, entry.id, now - age).await;
            stale_ids.push(entry.id);
        }

        // Create 1 recent incomplete entry (should not be timed out)
        let recent = datastore
            .audit_log_entry_init(opctx, make_entry("recent-req").into())
            .await
            .unwrap();

        // Create 1 old completed entry (should not be timed out)
        let completed = datastore
            .audit_log_entry_init(opctx, make_entry("completed-req").into())
            .await
            .unwrap();
        let two_hours_ago = now - TimeDelta::try_hours(2).unwrap();
        set_time_started(datastore, completed.id, two_hours_ago).await;
        datastore
            .audit_log_entry_complete(
                opctx,
                &completed,
                AuditLogCompletion::Success { http_status_code: 200 }.into(),
            )
            .await
            .unwrap();

        // hold onto completed entry so we can check at the end it hasn't changed
        let completed_before =
            get_completed_entry(&datastore, completed.id).await;

        // max_timed_out_per_activation = 3, so it takes two activations to
        // time out all 5 stale entries
        let mut task = AuditLogTimeoutIncomplete::new(
            datastore.clone(),
            Duration::from_secs(3600),
            3,
        );

        // first activation times out the 3 earliest entries
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 3);
        assert!(status.error.is_none());
        for &id in &stale_ids[..3] {
            assert_timed_out(&datastore, id).await;
        }
        for &id in &stale_ids[3..] {
            assert_incomplete(&datastore, id).await;
        }
        assert_incomplete(&datastore, recent.id).await;

        // second activation times out remaining 2 stale
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 2);
        assert!(status.error.is_none());
        for &id in &stale_ids {
            assert_timed_out(&datastore, id).await;
        }
        assert_incomplete(&datastore, recent.id).await;

        // third activation does nothing
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.timed_out, 0);
        assert!(status.error.is_none());

        // completed entry is unaffected
        assert_eq!(
            get_completed_entry(&datastore, completed.id).await,
            completed_before
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
