// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use uuid::Uuid;

const BGP_ANNOUNCE_SET_230: Uuid =
    Uuid::from_u128(0x23000001_0000_0000_0000_000000000001);
// Row inserted before migration 230 (no max_paths column yet).
const BGP_CONFIG_230_PRE: Uuid =
    Uuid::from_u128(0x23000002_0000_0000_0000_000000000001);
// Row inserted after migration 230 with an explicit max_paths value.
const BGP_CONFIG_231_EXPLICIT: Uuid =
    Uuid::from_u128(0x23100002_0000_0000_0000_000000000001);
// Row inserted after migration 230 with NULL max_paths.
const BGP_CONFIG_231_NULL: Uuid =
    Uuid::from_u128(0x23100002_0000_0000_0000_000000000002);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Now max_paths column exists (added by migration 230) but is nullable.
        // Insert one row with an explicit value and one with NULL.
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.bgp_config
                    (id, name, description, time_created, time_modified,
                     asn, bgp_announce_set_id, max_paths)
                VALUES
                    ('{BGP_CONFIG_231_EXPLICIT}', 'bgp-explicit-max-paths-231',
                     'has explicit max_paths', now(), now(),
                     64501, '{BGP_ANNOUNCE_SET_230}', 4);

                INSERT INTO omicron.public.bgp_config
                    (id, name, description, time_created, time_modified,
                     asn, bgp_announce_set_id)
                VALUES
                    ('{BGP_CONFIG_231_NULL}', 'bgp-null-max-paths-231',
                     'has NULL max_paths', now(), now(),
                     64502, '{BGP_ANNOUNCE_SET_230}');
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 231");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // The pre-230 row (no max_paths column when inserted) should now be 1.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT max_paths FROM omicron.public.bgp_config
                     WHERE id = '{BGP_CONFIG_230_PRE}'"
                ),
                &[],
            )
            .await
            .expect("failed to query bgp_config for pre-230 row");
        assert_eq!(rows.len(), 1);
        let max_paths: i16 = rows[0].get("max_paths");
        assert_eq!(
            max_paths, 1,
            "pre-230 row should have max_paths backfilled to 1"
        );

        // The row with explicit max_paths = 4 should be preserved.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT max_paths FROM omicron.public.bgp_config
                     WHERE id = '{BGP_CONFIG_231_EXPLICIT}'"
                ),
                &[],
            )
            .await
            .expect("failed to query bgp_config for explicit row");
        assert_eq!(rows.len(), 1);
        let max_paths: i16 = rows[0].get("max_paths");
        assert_eq!(max_paths, 4, "explicit max_paths should be preserved");

        // The row with NULL max_paths should now be 1.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT max_paths FROM omicron.public.bgp_config
                     WHERE id = '{BGP_CONFIG_231_NULL}'"
                ),
                &[],
            )
            .await
            .expect("failed to query bgp_config for NULL row");
        assert_eq!(rows.len(), 1);
        let max_paths: i16 = rows[0].get("max_paths");
        assert_eq!(max_paths, 1, "NULL max_paths should be backfilled to 1");

        // Clean up test data
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.bgp_config
                    WHERE id IN ('{BGP_CONFIG_230_PRE}',
                                 '{BGP_CONFIG_231_EXPLICIT}',
                                 '{BGP_CONFIG_231_NULL}');
                DELETE FROM omicron.public.bgp_announce_set
                    WHERE id = '{BGP_ANNOUNCE_SET_230}';
                "
            ))
            .await
            .expect("failed to clean up migration 231 test data");
    })
}
