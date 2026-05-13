// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that hard-deletes expired device access tokens.

use crate::app::background::BackgroundTask;
use chrono::Utc;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::TokenCleanupStatus;
use serde_json::json;
use std::sync::Arc;

pub struct TokenCleanup {
    datastore: Arc<DataStore>,
    max_delete_per_activation: u32,
}

impl TokenCleanup {
    pub fn new(
        datastore: Arc<DataStore>,
        max_delete_per_activation: u32,
    ) -> Self {
        Self { datastore, max_delete_per_activation }
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> TokenCleanupStatus {
        // Token expiration is encoded per-row in `time_expires`, so the
        // eligibility cutoff is simply "now". Tokens with NULL `time_expires`
        // never expire and are not affected.
        let cutoff = Utc::now();
        let limit = self.max_delete_per_activation;
        let (deleted, error) = match self
            .datastore
            .token_cleanup_batch(opctx, cutoff, limit)
            .await
        {
            Ok(deleted) => {
                if deleted > 0 {
                    slog::info!(
                        &opctx.log,
                        "token cleanup deleted {deleted} expired tokens";
                        "cutoff" => %cutoff,
                        "limit" => limit,
                    );
                } else {
                    slog::debug!(
                        &opctx.log,
                        "token cleanup found no expired tokens";
                        "cutoff" => %cutoff,
                    );
                }
                (deleted, None)
            }
            Err(err) => {
                let msg = format!("token cleanup failed: {err:#}");
                slog::error!(&opctx.log, "{msg}");
                (0, Some(msg))
            }
        };

        TokenCleanupStatus { deleted, cutoff, limit, error }
    }
}

impl BackgroundTask for TokenCleanup {
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
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::TimeDelta;
    use chrono::Utc;
    use diesel::prelude::*;
    use nexus_db_model::DeviceAccessToken;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_schema::schema::device_access_token;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::SiloUserUuid;
    use uuid::Uuid;

    /// Insert a token directly via diesel, bypassing the datastore's transaction
    /// path (which requires a matching device auth request).
    async fn insert_token(datastore: &DataStore, token: DeviceAccessToken) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::insert_into(device_access_token::table)
            .values(token)
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    fn make_token(
        silo_user_id: SiloUserUuid,
        time_expires: Option<chrono::DateTime<Utc>>,
    ) -> DeviceAccessToken {
        // `new()` asserts time_expires is in the future, so construct with a
        // future expiry and overwrite afterward.
        let placeholder = Utc::now() + TimeDelta::try_hours(1).unwrap();
        // `device_code` column is STRING(40); a bare UUID (36 chars) fits.
        let mut token = DeviceAccessToken::new(
            Uuid::new_v4(),
            Uuid::new_v4().to_string(),
            Utc::now(),
            silo_user_id,
            Some(placeholder),
        );
        token.time_expires = time_expires;
        token
    }

    #[tokio::test]
    async fn test_token_cleanup_activation() {
        let logctx = dev::test_setup_log("test_token_cleanup_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let silo_user_id = SiloUserUuid::new_v4();
        let now = Utc::now();

        // Expired an hour ago — should be cleaned up.
        let expired = make_token(
            silo_user_id,
            Some(now - TimeDelta::try_hours(1).unwrap()),
        );
        let expired_id = expired.id();
        insert_token(datastore, expired).await;

        // Expires in an hour — should be preserved.
        let live = make_token(
            silo_user_id,
            Some(now + TimeDelta::try_hours(1).unwrap()),
        );
        let live_id = live.id();
        insert_token(datastore, live).await;

        // Never expires — should be preserved.
        let perpetual = make_token(silo_user_id, None);
        let perpetual_id = perpetual.id();
        insert_token(datastore, perpetual).await;

        let mut task = TokenCleanup::new(datastore.clone(), 10_000);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 1);
        assert!(status.error.is_none());
        assert_eq!(status.limit, 10_000);

        // Second activation should find nothing.
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 0);
        assert!(status.error.is_none());

        // Verify which rows remain — look up by ID to avoid full table scans.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        for (id, should_exist) in
            [(expired_id, false), (live_id, true), (perpetual_id, true)]
        {
            let found: Option<DeviceAccessToken> = device_access_token::table
                .filter(device_access_token::id.eq(id.into_untyped_uuid()))
                .select(DeviceAccessToken::as_select())
                .first_async(&*conn)
                .await
                .optional()
                .unwrap();
            assert_eq!(
                found.is_some(),
                should_exist,
                "token {id} exists?={should_exist:?}",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_token_cleanup_respects_limit() {
        let logctx = dev::test_setup_log("test_token_cleanup_respects_limit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let silo_user_id = SiloUserUuid::new_v4();
        let now = Utc::now();

        // Create 5 expired tokens.
        for i in 0..5 {
            let token = make_token(
                silo_user_id,
                Some(now - TimeDelta::try_hours(1 + i).unwrap()),
            );
            insert_token(datastore, token).await;
        }

        let mut task = TokenCleanup::new(datastore.clone(), 2);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 2);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 2);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 1);

        let status = task.actually_activate(opctx).await;
        assert_eq!(status.deleted, 0);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
