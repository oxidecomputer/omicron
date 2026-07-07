// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use uuid::Uuid;

// Migration 229 fixes column ordering in console_session and device_access_token.
// The "id" column was added via ALTER TABLE in migration 145, placing it at the
// end. This migration recreates the tables with "id" as the first column.
// It also drops console sessions created more than 24 hours ago.
const SESSION_229_ID: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000001);
const SESSION_229_TOKEN: &str = "tok-console-229-migration-test";
const SESSION_229_SILO_USER: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000002);

const SESSION_229_OLD_ID: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000003);
const SESSION_229_OLD_TOKEN: &str = "tok-console-229-old-session";
const SESSION_229_OLD_SILO_USER: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000004);

const DEVICE_229_ID: Uuid =
    Uuid::from_u128(0x22900002_0000_0000_0000_000000000001);
const DEVICE_229_TOKEN: &str = "tok-device-229-migration-test";
const DEVICE_229_CLIENT_ID: Uuid =
    Uuid::from_u128(0x22900002_0000_0000_0000_000000000002);
const DEVICE_229_DEVICE_CODE: &str = "code-229-migration-test";
const DEVICE_229_SILO_USER: Uuid =
    Uuid::from_u128(0x22900002_0000_0000_0000_000000000003);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert test data into console_session and device_access_token.
        // These tables currently have "id" as the last column (from migration 145).
        // We insert two console sessions: one recent (should be kept) and one
        // created over 24 hours ago (should be dropped).
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.console_session
                    (id, token, time_created, time_last_used, silo_user_id)
                VALUES
                    ('{SESSION_229_ID}', '{SESSION_229_TOKEN}', now(), now(), '{SESSION_229_SILO_USER}');

                INSERT INTO omicron.public.console_session
                    (id, token, time_created, time_last_used, silo_user_id)
                VALUES
                    ('{SESSION_229_OLD_ID}', '{SESSION_229_OLD_TOKEN}',
                     now() - INTERVAL '48 hours', now() - INTERVAL '48 hours',
                     '{SESSION_229_OLD_SILO_USER}');

                INSERT INTO omicron.public.device_access_token
                    (id, token, client_id, device_code, silo_user_id, time_requested, time_created)
                VALUES
                    ('{DEVICE_229_ID}', '{DEVICE_229_TOKEN}', '{DEVICE_229_CLIENT_ID}',
                     '{DEVICE_229_DEVICE_CODE}', '{DEVICE_229_SILO_USER}', now(), now());
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 229");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Verify data was preserved after the column reordering migration.

        // Check that the recent console_session was kept
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, token, silo_user_id FROM omicron.public.console_session
                     WHERE id = '{SESSION_229_ID}'"
                ),
                &[],
            )
            .await
            .expect("failed to query post-migration console_session");
        assert_eq!(
            rows.len(),
            1,
            "recent console_session row should still exist"
        );

        let id: Uuid = rows[0].get("id");
        assert_eq!(id, SESSION_229_ID);
        let token: &str = rows[0].get("token");
        assert_eq!(token, SESSION_229_TOKEN);
        let silo_user_id: Uuid = rows[0].get("silo_user_id");
        assert_eq!(silo_user_id, SESSION_229_SILO_USER);

        // Check that the old (>24h) console_session was dropped
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id FROM omicron.public.console_session
                     WHERE id = '{SESSION_229_OLD_ID}'"
                ),
                &[],
            )
            .await
            .expect(
                "failed to query post-migration console_session for old row",
            );
        assert_eq!(
            rows.len(),
            0,
            "console_session row older than 24h should have been dropped"
        );

        // Check device_access_token
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, token, client_id, device_code, silo_user_id
                     FROM omicron.public.device_access_token
                     WHERE id = '{DEVICE_229_ID}'"
                ),
                &[],
            )
            .await
            .expect("failed to query post-migration device_access_token");
        assert_eq!(rows.len(), 1, "device_access_token row should still exist");

        let id: Uuid = rows[0].get("id");
        assert_eq!(id, DEVICE_229_ID);
        let token: &str = rows[0].get("token");
        assert_eq!(token, DEVICE_229_TOKEN);
        let client_id: Uuid = rows[0].get("client_id");
        assert_eq!(client_id, DEVICE_229_CLIENT_ID);
        let device_code: &str = rows[0].get("device_code");
        assert_eq!(device_code, DEVICE_229_DEVICE_CODE);
        let silo_user_id: Uuid = rows[0].get("silo_user_id");
        assert_eq!(silo_user_id, DEVICE_229_SILO_USER);

        // Clean up test data
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.console_session WHERE id = '{SESSION_229_ID}';
                DELETE FROM omicron.public.device_access_token WHERE id = '{DEVICE_229_ID}';
                "
            ))
            .await
            .expect("failed to clean up migration 229 test data");
    })
}
