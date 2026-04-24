// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const INV_COLLECTION_ID: Uuid =
    Uuid::from_u128(25301230_0000_0000_0000_000000000001);
const SLED_ID: Uuid = Uuid::from_u128(25301230_0000_0000_0000_000000000002);

const SVC_UNINITIALIZED: Uuid =
    Uuid::from_u128(25301231_0000_0000_0000_000000000001);
const SVC_OFFLINE: Uuid = Uuid::from_u128(25301231_0000_0000_0000_000000000002);
const SVC_DEGRADED: Uuid =
    Uuid::from_u128(25301231_0000_0000_0000_000000000003);
const SVC_MAINTENANCE: Uuid =
    Uuid::from_u128(25301231_0000_0000_0000_000000000004);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.inv_svc_enabled_not_online_service
                    (inv_collection_id, sled_id, id, fmri, zone, state)
                VALUES
                    ('{INV_COLLECTION_ID}', '{SLED_ID}', '{SVC_UNINITIALIZED}',
                     'svc:/site/uninit:default', 'global', 'uninitialized'),
                    ('{INV_COLLECTION_ID}', '{SLED_ID}', '{SVC_OFFLINE}',
                     'svc:/site/offline:default', 'global', 'offline'),
                    ('{INV_COLLECTION_ID}', '{SLED_ID}', '{SVC_DEGRADED}',
                     'svc:/site/degraded:default', 'global', 'degraded'),
                    ('{INV_COLLECTION_ID}', '{SLED_ID}', '{SVC_MAINTENANCE}',
                     'svc:/site/maintenance:default', 'global', 'maintenance');
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 253");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // The 'uninitialized' row should have been removed by the migration.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id \
                     FROM omicron.public.inv_svc_enabled_not_online_service \
                     WHERE id = '{SVC_UNINITIALIZED}'"
                ),
                &[],
            )
            .await
            .expect("failed to query for uninitialized row after 253");
        assert_eq!(
            rows.len(),
            0,
            "'uninitialized' row should have been deleted by the migration"
        );

        // The other rows should be preserved with their states intact. Cast
        // the enum to text so this test doesn't need to know about the
        // current enum type. The migration check guide in schema.rs recommneds
        // schema checks that are not strongly-typed.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, state::text AS state \
                     FROM omicron.public.inv_svc_enabled_not_online_service \
                     WHERE inv_collection_id = '{INV_COLLECTION_ID}' \
                     ORDER BY id"
                ),
                &[],
            )
            .await
            .expect("failed to query preserved rows after 253");

        let observed: Vec<(Uuid, &str)> = rows
            .iter()
            .map(|r| (r.get::<_, Uuid>("id"), r.get::<_, &str>("state")))
            .collect();
        assert_eq!(
            observed,
            vec![
                (SVC_OFFLINE, "offline"),
                (SVC_DEGRADED, "degraded"),
                (SVC_MAINTENANCE, "maintenance"),
            ],
            "offline/degraded/maintenance rows should survive with original \
             states"
        );

        // Inserting 'uninitialized' should now fail, the enum no longer
        // accepts that value.
        let err = ctx
            .client
            .batch_execute(&format!(
                "INSERT INTO omicron.public.inv_svc_enabled_not_online_service \
                     (inv_collection_id, sled_id, id, fmri, zone, state) \
                 VALUES \
                     ('{INV_COLLECTION_ID}', '{SLED_ID}', \
                      '25300002-0000-0000-0000-000000000001', \
                      'svc:/site/new-uninit:default', 'global', \
                      'uninitialized');"
            ))
            .await
            .expect_err("should not be able to insert row");
        let error_message = err.as_db_error().unwrap().message();
        let expected_error_msg = "invalid input value for enum inv_svc_enabled_not_online_state: \"uninitialized\"";
        assert!(
            error_message.contains(expected_error_msg),
            "expected error message: {expected_error_msg}; got: {error_message}"
        );

        // Clean up test data
        ctx.client
            .batch_execute(&format!(
                "DELETE FROM omicron.public.inv_svc_enabled_not_online_service \
                 WHERE inv_collection_id = '{INV_COLLECTION_ID}';"
            ))
            .await
            .expect("failed to clean up test data for 253");
    })
}
