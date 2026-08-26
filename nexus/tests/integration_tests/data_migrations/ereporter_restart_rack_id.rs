// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// The single rack that all pre-existing ereporter restarts should be
// backfilled to. The migration relies on there being exactly one rack present
// when it runs.
const RACK_ID: Uuid = Uuid::from_u128(0xeb030000_0000_0000_0000_000000000001);

// An SP reporter restart and a host OS reporter restart that predate the
// `rack_id` column.
const SP_RESTART: Uuid =
    Uuid::from_u128(0xeb030001_0000_0000_0000_000000000001);
const HOST_RESTART: Uuid =
    Uuid::from_u128(0xeb030001_0000_0000_0000_000000000002);

// Hard-coded timestamps; their exact values are irrelevant to
// this test.
const T_00: &str = "2024-01-01T00:00:00Z";
const T_10: &str = "2024-01-01T00:00:10Z";
const T_20: &str = "2024-01-01T00:00:20Z";

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert exactly one rack. The migration's backfill selects the single
        // rack ID via a scalar subquery, which would fail if more than one rack
        // were present. Insert ereporter restarts that predate the `rack_id`
        // column so that the migration must backfill them.
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.rack (
                    id, time_created, time_modified, initialized
                ) VALUES
                    ('{RACK_ID}', '{T_00}', '{T_00}', true);

                INSERT INTO omicron.public.ereporter_restart (
                    id, time_first_seen, reporter, slot_type, slot
                ) VALUES
                    ('{SP_RESTART}', '{T_10}', 'sp', 'switch', 1),
                    ('{HOST_RESTART}', '{T_20}', 'host', 'sled', NULL);
                "
            ))
            .await
            .expect(
                "failed to insert pre-migration rack and ereporter restarts",
            );
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, rack_id
                     FROM omicron.public.ereporter_restart
                     WHERE id IN ('{SP_RESTART}', '{HOST_RESTART}')"
                ),
                &[],
            )
            .await
            .expect("failed to query backfilled ereporter_restart rows");

        assert_eq!(
            rows.len(),
            2,
            "both pre-existing ereporter restarts should still be present"
        );

        for row in &rows {
            let id: Uuid = row.get("id");
            let rack_id: Uuid = row.get("rack_id");
            assert_eq!(
                rack_id, RACK_ID,
                "ereporter restart {id} should be backfilled with the only \
                 rack's ID"
            );
        }

        // Clean up test data so it doesn't interfere with later checks.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.ereporter_restart
                    WHERE id IN ('{SP_RESTART}', '{HOST_RESTART}');
                DELETE FROM omicron.public.rack WHERE id = '{RACK_ID}';
                "
            ))
            .await
            .expect("failed to clean up ereporter-restart-rack-id test data");
    })
}
