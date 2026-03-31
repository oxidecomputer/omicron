// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use uuid::Uuid;

// Two inventory collections with different timestamps to verify the
// migration picks the slot from the newest one.
const INV_COLL_OLDER: Uuid =
    Uuid::from_u128(24200001_0000_0000_0000_000000000001);
const INV_COLL_NEWER: Uuid =
    Uuid::from_u128(24200001_0000_0000_0000_000000000002);

// Baseboard ID associated with sled 1.
const BASEBOARD_1: Uuid = Uuid::from_u128(24200002_0000_0000_0000_000000000001);

// Sled 1: present in inventory with a baseboard -> SP slot determinable.
const SLED_1: Uuid = Uuid::from_u128(24200003_0000_0000_0000_000000000001);
// Sled 2: present in inventory but hw_baseboard_id is NULL.
const SLED_2: Uuid = Uuid::from_u128(24200003_0000_0000_0000_000000000002);
// Sled 3: not present in any inventory collection.
const SLED_3: Uuid = Uuid::from_u128(24200003_0000_0000_0000_000000000003);

// Ereport restart IDs -- one per test case.
const SP_SLED: Uuid = Uuid::from_u128(24200004_0000_0000_0000_000000000001);
const SP_SWITCH: Uuid = Uuid::from_u128(24200004_0000_0000_0000_000000000002);
const SP_POWER: Uuid = Uuid::from_u128(24200004_0000_0000_0000_000000000003);
const HOST_WITH_INV: Uuid =
    Uuid::from_u128(24200004_0000_0000_0000_000000000004);
const HOST_NO_BB: Uuid = Uuid::from_u128(24200004_0000_0000_0000_000000000005);
const HOST_NO_INV: Uuid = Uuid::from_u128(24200004_0000_0000_0000_000000000006);

const COLLECTOR: Uuid = Uuid::from_u128(24200005_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!(
                "
                -- Two inventory collections: older and newer.
                INSERT INTO omicron.public.inv_collection
                    (id, time_started, time_done, collector)
                VALUES
                    ('{INV_COLL_OLDER}',
                     '2024-01-01 00:00:00+00',
                     '2024-01-01 00:01:00+00',
                     'test'),
                    ('{INV_COLL_NEWER}',
                     '2024-06-01 00:00:00+00',
                     '2024-06-01 00:01:00+00',
                     'test');

                -- A baseboard for sled 1.
                INSERT INTO omicron.public.hw_baseboard_id
                    (id, part_number, serial_number)
                VALUES
                    ('{BASEBOARD_1}', 'test-part', 'test-serial');

                -- Sled 1 in both collections (with baseboard).
                -- Sled 2 in only the newer collection (no baseboard).
                -- Sled 3 is deliberately absent from inventory.
                INSERT INTO omicron.public.inv_sled_agent (
                    inv_collection_id, time_collected, source,
                    sled_id, hw_baseboard_id,
                    sled_agent_ip, sled_agent_port, sled_role,
                    usable_hardware_threads, usable_physical_ram,
                    reservoir_size, reconciler_status_kind,
                    zone_manifest_boot_disk_path,
                    zone_manifest_source,
                    mupdate_override_boot_disk_path,
                    cpu_family,
                    measurement_manifest_boot_disk_path,
                    measurement_manifest_source
                ) VALUES
                    -- sled 1 in OLDER collection
                    ('{INV_COLL_OLDER}', now(), 'test',
                     '{SLED_1}', '{BASEBOARD_1}',
                     '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', 'sled-agent', '/test', 'unknown',
                     '/test', 'sled-agent'),
                    -- sled 1 in NEWER collection
                    ('{INV_COLL_NEWER}', now(), 'test',
                     '{SLED_1}', '{BASEBOARD_1}',
                     '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', 'sled-agent', '/test', 'unknown',
                     '/test', 'sled-agent'),
                    -- sled 2 in NEWER collection (no `hw_baseboard_id`)
                    ('{INV_COLL_NEWER}', now(), 'test',
                     '{SLED_2}', NULL,
                     '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', 'sled-agent', '/test', 'unknown',
                     '/test', 'sled-agent');

                -- SP records for sled 1 in both collections,
                -- with DIFFERENT slot numbers (3 vs 7) so we can
                -- verify the migration picks the newer one.
                --
                -- In REAL LIFE, this case doesn't actually happen.
                -- If the sled is removed from the rack and reinserted,
                -- it will have a different `inv_sled_agent` record with
                -- a different sled UUID. But, since the database *can*
                -- represent this via multiple inv collection rows, let's
                -- make sure that the migration tries to pick the newest
                -- just in case...
                INSERT INTO omicron.public.inv_service_processor (
                    inv_collection_id, hw_baseboard_id,
                    time_collected, source,
                    sp_type, sp_slot,
                    baseboard_revision, hubris_archive_id,
                    power_state
                ) VALUES
                    ('{INV_COLL_OLDER}', '{BASEBOARD_1}',
                     now(), 'test', 'sled', 3, 1, 'test', 'A0'),
                    ('{INV_COLL_NEWER}', '{BASEBOARD_1}',
                     now(), 'test', 'sled', 7, 1, 'test', 'A0');

                -- Ereport rows for every test case.
                -- At schema version 241 the table still has sp_type
                -- and sp_slot.
                INSERT INTO omicron.public.ereport (
                    restart_id, ena, time_collected, collector_id,
                    report, reporter, sp_type, sp_slot, sled_id
                ) VALUES
                    -- Case 1: SP sled reporter
                    ('{SP_SLED}', 1, now(), '{COLLECTOR}',
                     '{{}}', 'sp', 'sled', 0, NULL),
                    -- Case 2: SP switch reporter
                    ('{SP_SWITCH}', 1, now(), '{COLLECTOR}',
                     '{{}}', 'sp', 'switch', 1, NULL),
                    -- Case 3: SP power reporter
                    ('{SP_POWER}', 1, now(), '{COLLECTOR}',
                     '{{}}', 'sp', 'power', 2, NULL),
                    -- Case 4: Host, sled in inventory with SP also in inventory
                    ('{HOST_WITH_INV}', 1, now(), '{COLLECTOR}',
                     '{{}}', 'host', NULL, NULL, '{SLED_1}'),
                    -- Case 5: Host, sled in inventory, no `hw_baseboard_id`
                    ('{HOST_NO_BB}', 1, now(), '{COLLECTOR}',
                     '{{}}', 'host', NULL, NULL, '{SLED_2}'),
                    -- Case 6: Host, sled not in inventory at all
                    ('{HOST_NO_INV}', 1, now(), '{COLLECTOR}',
                     '{{}}', 'host', NULL, NULL, '{SLED_3}');
                "
            ))
            .await
            .expect("failed to insert test data for migration 242");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Query all test ereport rows after the migration.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT restart_id, slot_type::text, slot
                     FROM omicron.public.ereport
                     WHERE restart_id IN (
                         '{SP_SLED}', '{SP_SWITCH}', '{SP_POWER}',
                         '{HOST_WITH_INV}', '{HOST_NO_BB}',
                         '{HOST_NO_INV}'
                     )"
                ),
                &[],
            )
            .await
            .expect("failed to query migrated ereport rows");

        assert_eq!(rows.len(), 6, "expected 6 test ereport rows");

        let results: HashMap<Uuid, (String, Option<i32>)> = rows
            .iter()
            .map(|row| {
                let id: Uuid = row.get("restart_id");
                let slot_type: String = row.get("slot_type");
                let slot: Option<i32> = row.get("slot");
                (id, (slot_type, slot))
            })
            .collect();

        // Case 1: SP sled -> slot_type='sled', slot=0
        let (st, s) = &results[&SP_SLED];
        assert_eq!(st, "sled", "SP sled reporter slot_type");
        assert_eq!(*s, Some(0), "SP sled reporter slot");

        // Case 2: SP switch -> slot_type='switch', slot=1
        let (st, s) = &results[&SP_SWITCH];
        assert_eq!(st, "switch", "SP switch reporter slot_type");
        assert_eq!(*s, Some(1), "SP switch reporter slot");

        // Case 3: SP power -> slot_type='power', slot=2
        let (st, s) = &results[&SP_POWER];
        assert_eq!(st, "power", "SP power reporter slot_type");
        assert_eq!(*s, Some(2), "SP power reporter slot");

        // Case 4: Host with inventory + baseboard -> slot_type='sled',
        // slot=7 (from the newer collection, NOT 3 from the older one).
        // This case really shouldn't happen, but let's test it anyway.
        let (st, s) = &results[&HOST_WITH_INV];
        assert_eq!(st, "sled", "host with inventory slot_type");
        assert_eq!(
            *s,
            Some(7),
            "should use SP slot from the newest inventory collection"
        );

        // Case 5: Host in inventory but no baseboard -> slot NULL
        let (st, s) = &results[&HOST_NO_BB];
        assert_eq!(st, "sled", "host without baseboard slot_type");
        assert_eq!(
            *s, None,
            "host without `hw_baseboard_id` should have NULL slot"
        );

        // Case 6: Host not in inventory at all -> slot NULL
        let (st, s) = &results[&HOST_NO_INV];
        assert_eq!(st, "sled", "host not in inventory slot_type");
        assert_eq!(*s, None, "host not in inventory should have NULL slot");

        // Verify the old columns have been dropped.
        let err = ctx
            .client
            .query("SELECT sp_type FROM omicron.public.ereport LIMIT 0", &[])
            .await;
        assert!(err.is_err(), "sp_type column should have been dropped");

        let err = ctx
            .client
            .query("SELECT sp_slot FROM omicron.public.ereport LIMIT 0", &[])
            .await;
        assert!(err.is_err(), "sp_slot column should have been dropped");

        // Clean up test data.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.ereport
                    WHERE restart_id IN (
                        '{SP_SLED}', '{SP_SWITCH}', '{SP_POWER}',
                        '{HOST_WITH_INV}', '{HOST_NO_BB}',
                        '{HOST_NO_INV}'
                    );
                DELETE FROM omicron.public.inv_service_processor
                    WHERE hw_baseboard_id = '{BASEBOARD_1}';
                DELETE FROM omicron.public.inv_sled_agent
                    WHERE sled_id IN ('{SLED_1}', '{SLED_2}');
                DELETE FROM omicron.public.inv_collection
                    WHERE id IN (
                        '{INV_COLL_OLDER}', '{INV_COLL_NEWER}'
                    );
                DELETE FROM omicron.public.hw_baseboard_id
                    WHERE id = '{BASEBOARD_1}';
                "
            ))
            .await
            .expect("failed to clean up migration 242 test data");
    })
}
