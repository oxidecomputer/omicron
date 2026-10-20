// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// The single rack that all pre-existing inventoried SPs should be backfilled
// to. The migration relies on there being exactly one rack present when it
// runs.
const RACK_ID: Uuid = Uuid::from_u128(0x5e305000_0000_0000_0000_000000000001);

// Two inventory collections, each containing service processors that predate
// the `rack_id` column.
const COLLECTION_1: Uuid =
    Uuid::from_u128(0x5e305001_0000_0000_0000_000000000001);
const COLLECTION_2: Uuid =
    Uuid::from_u128(0x5e305001_0000_0000_0000_000000000002);
const SLED_BASEBOARD: Uuid =
    Uuid::from_u128(0x5e305002_0000_0000_0000_000000000001);
const SWITCH_BASEBOARD: Uuid =
    Uuid::from_u128(0x5e305002_0000_0000_0000_000000000002);

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
        // were present. Insert inventoried SPs that predate the `rack_id`
        // column so that the migration must backfill them.
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.rack (
                    id, time_created, time_modified, initialized
                ) VALUES
                    ('{RACK_ID}', '{T_00}', '{T_00}', true);

                INSERT INTO omicron.public.inv_service_processor (
                    inv_collection_id,
                    hw_baseboard_id,
                    time_collected,
                    source,
                    sp_type,
                    sp_slot,
                    baseboard_revision,
                    hubris_archive_id,
                    power_state
                ) VALUES
                    ('{COLLECTION_1}', '{SLED_BASEBOARD}', '{T_10}',
                     'fake MGS 1', 'sled', 3, 0, 'hubris1', 'A0'),
                    ('{COLLECTION_1}', '{SWITCH_BASEBOARD}', '{T_10}',
                     'fake MGS 1', 'switch', 0, 1, 'hubris2', 'A2'),
                    ('{COLLECTION_2}', '{SLED_BASEBOARD}', '{T_20}',
                     'fake MGS 2', 'sled', 3, 0, 'hubris1', 'A0'),
                    ('{COLLECTION_2}', '{SWITCH_BASEBOARD}', '{T_20}',
                     'fake MGS 2', 'switch', 0, 1, 'hubris2', 'A2');
                "
            ))
            .await
            .expect("failed to insert pre-migration records");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT inv_collection_id, hw_baseboard_id, rack_id
                     FROM omicron.public.inv_service_processor
                     WHERE inv_collection_id IN
                        ('{COLLECTION_1}', '{COLLECTION_2}')"
                ),
                &[],
            )
            .await
            .expect("failed to query backfilled inv_service_processor rows");

        assert_eq!(
            rows.len(),
            4,
            "all pre-existing inventory service processors should still be \
             present"
        );

        for row in &rows {
            let collection_id: Uuid = row.get("inv_collection_id");
            let baseboard_id: Uuid = row.get("hw_baseboard_id");
            let rack_id: Uuid = row.get("rack_id");
            assert_eq!(
                rack_id, RACK_ID,
                "service processor {baseboard_id} in collection \
                 {collection_id} should be backfilled with the only rack's ID"
            );
        }

        // Clean up test data so it doesn't interfere with later checks.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.inv_service_processor
                    WHERE inv_collection_id IN
                        ('{COLLECTION_1}', '{COLLECTION_2}');
                DELETE FROM omicron.public.rack WHERE id = '{RACK_ID}';
                "
            ))
            .await
            .expect(
                "failed to clean up inv-service-processor-rack-id test data",
            );
    })
}
