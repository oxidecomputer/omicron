// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use uuid::Uuid;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert an audit_log entry with auth_method = 'session_cookie'.
        // After version 222's up1.sql adds credential_id, this row will have
        // credential_id = NULL. The migration must handle this case.
        let id = Uuid::new_v4();
        let actor_id = Uuid::new_v4();

        ctx.client
            .execute(
                "INSERT INTO omicron.public.audit_log (
                    id,
                    time_started,
                    request_id,
                    request_uri,
                    operation_id,
                    source_ip,
                    actor_id,
                    actor_kind,
                    auth_method
                ) VALUES (
                    $1,
                    now(),
                    'test-request-id',
                    '/test/uri',
                    'test_operation',
                    '127.0.0.1',
                    $2,
                    'user_builtin',
                    'session_cookie'
                )",
                &[&id, &actor_id],
            )
            .await
            .expect("inserted audit_log row with session_cookie auth_method");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Clean up the test row
        ctx.client
            .execute(
                "DELETE FROM omicron.public.audit_log WHERE request_id = 'test-request-id'",
                &[],
            )
            .await
            .expect("cleaned up audit_log test row");
    })
}
