// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `add-rack-id-to-inv-sled-agent` migration, with checks for the
//! backfill of the newly added `rack_id` column.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const RACK_ID: Uuid = Uuid::from_u128(0x30700001_0000_0000_0000_000000000001);

const SLED_1: Uuid = Uuid::from_u128(0x30700002_0000_0000_0000_000000000001);
const SLED_2: Uuid = Uuid::from_u128(0x30700002_0000_0000_0000_000000000002);

const INV_COLL_ID: Uuid =
    Uuid::from_u128(0x30700003_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!(
            "
                INSERT INTO omicron.public.rack (
                    id, time_created, time_modified, initialized
                ) VALUES
                    ('{RACK_ID}', now(), now(), true);

                INSERT INTO omicron.public.inv_sled_agent (
                    inv_collection_id, time_collected, source,
                    sled_id, sled_agent_ip, sled_agent_port, sled_role,
                    usable_hardware_threads, usable_physical_ram,
                    reservoir_size, reconciler_status_kind,
                    zone_manifest_boot_disk_path,
                    mupdate_override_boot_disk_path, cpu_family,
                    measurement_manifest_boot_disk_path,
                    instance_manager_num_registered_vmms
                ) VALUES
                    ('{INV_COLL_ID}', now(), 'test',
                     '{SLED_1}', '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', '/test', 'unknown',
                     '/test', 0),
                    ('{INV_COLL_ID}', now(), 'test',
                     '{SLED_2}', '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', '/test', 'unknown',
                     '/test', 0);
            "
        ))
        .await
        .expect("migration 307 test data insertion should succeeed");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let expected = vec![(SLED_1, RACK_ID), (SLED_2, RACK_ID)];
    let actual: Vec<_> = ctx
        .client
        .query(
            "
            SELECT
                sled_id, rack_id
            FROM
                omicron.public.inv_sled_agent
            ORDER BY
                sled_id
        ",
            &[],
        )
        .await
        .expect("migration 307 test query should succeed")
        .into_iter()
        .map(|row| {
            (row.get::<_, Uuid>("sled_id"), row.get::<_, Uuid>("rack_id"))
        })
        .collect();
    assert_eq!(expected, actual);

    // Now that the test has passed, clean up after ourselves:
    ctx.client
        .batch_execute(&format!(
            "
                DELETE FROM omicron.public.rack WHERE id = '{RACK_ID}';

                DELETE FROM
                    omicron.public.inv_sled_agent
                WHERE sled_id IN
                    ('{SLED_1}', '{SLED_2}');
            "
        ))
        .await
        .expect("migration 307 test data cleanup should succeed");
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
