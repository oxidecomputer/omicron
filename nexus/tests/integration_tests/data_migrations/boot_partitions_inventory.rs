// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use uuid::Uuid;

const INV_COLLECTION_ID_1: &str = "5cb42909-d94a-4903-be72-330eea0325d9";
const INV_COLLECTION_ID_2: &str = "142b62c2-9348-4530-9eed-7077351fb94b";
const SLED_ID_1: &str = "3b5b7861-03aa-420a-a057-0a14347dc4c0";
const SLED_ID_2: &str = "de7ab4c0-30d4-4e9e-b620-3a959a9d59dd";
const SLED_CONFIG_ID_1: &str = "a1637607-c49e-4658-b637-96473a72bb32";
const SLED_CONFIG_ID_2: &str = "695de2f0-9c09-42b0-a22a-1e5b783a38c0";
const SLED_CONFIG_ID_3: &str = "50a8d074-879b-4c59-a2ef-700156e49a97";

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert these tuples:
        //
        // (collection 1, sled 1, sled config ID 1)
        // (collection 1, sled 2, NULL)
        // (collection 2, sled 1, sled config ID 2)
        // (collection 2, sled 2, sled config ID 3)
        ctx.client
            .batch_execute(&format!(
                "
                    INSERT INTO omicron.public.inv_sled_agent (
                        inv_collection_id, time_collected, source, sled_id,
                        sled_agent_ip, sled_agent_port, sled_role,
                        usable_hardware_threads, usable_physical_ram,
                        reservoir_size, last_reconciliation_sled_config,
                        reconciler_status_kind, zone_manifest_boot_disk_path,
                        zone_manifest_source, mupdate_override_boot_disk_path
                    )
                    VALUES (
                        '{INV_COLLECTION_ID_1}', now(), 'test-source',
                        '{SLED_ID_1}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_1}',
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    ), (
                        '{INV_COLLECTION_ID_1}', now(), 'test-source',
                        '{SLED_ID_2}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, NULL,
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    ), (
                        '{INV_COLLECTION_ID_2}', now(), 'test-source',
                        '{SLED_ID_1}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_2}',
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    ), (
                        '{INV_COLLECTION_ID_2}', now(), 'test-source',
                        '{SLED_ID_2}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_3}',
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    );
                    "
            ))
            .await
            .expect("inserted pre-migration data");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Verify that the new inv_sled_config_reconciler table has 3 rows
        // corresponding to the three inv_sled_agent rows that had a
        // non-NULL last_reconciliation_sled_config inserted in `before()`.
        let expected = {
            let mut expected = BTreeSet::<(Uuid, Uuid, Uuid)>::new();
            expected.insert((
                INV_COLLECTION_ID_1.parse().unwrap(),
                SLED_ID_1.parse().unwrap(),
                SLED_CONFIG_ID_1.parse().unwrap(),
            ));
            expected.insert((
                INV_COLLECTION_ID_2.parse().unwrap(),
                SLED_ID_1.parse().unwrap(),
                SLED_CONFIG_ID_2.parse().unwrap(),
            ));
            expected.insert((
                INV_COLLECTION_ID_2.parse().unwrap(),
                SLED_ID_2.parse().unwrap(),
                SLED_CONFIG_ID_3.parse().unwrap(),
            ));
            expected
        };

        let rows = ctx
            .client
            .query(
                "
                    SELECT
                    inv_collection_id, sled_id, last_reconciled_config,
                    boot_disk_slot, boot_disk_error, boot_partition_a_error,
                    boot_partition_b_error
                    FROM omicron.public.inv_sled_config_reconciler
                    ",
                &[],
            )
            .await
            .expect("queried post-migration data");

        let mut seen = BTreeSet::new();
        for row in rows {
            let inv_collection_id: Uuid = row.get("inv_collection_id");
            let sled_id: Uuid = row.get("sled_id");
            let last_reconciled_config: Uuid =
                row.get("last_reconciled_config");
            let boot_disk_slot: Option<i16> = row.get("boot_disk_slot");
            let boot_disk_error: Option<String> = row.get("boot_disk_error");
            let boot_partition_a_error: Option<String> =
                row.get("boot_partition_a_error");
            let boot_partition_b_error: Option<String> =
                row.get("boot_partition_b_error");

            seen.insert((inv_collection_id, sled_id, last_reconciled_config));

            // Migration should have populated all the error fields.
            assert_eq!(boot_disk_slot, None);
            assert_eq!(
                boot_disk_error.as_deref(),
                Some("old collection, data missing")
            );
            assert_eq!(
                boot_partition_a_error.as_deref(),
                Some("old collection, data missing")
            );
            assert_eq!(
                boot_partition_b_error.as_deref(),
                Some("old collection, data missing")
            );
        }

        assert_eq!(seen, expected);
    })
}
