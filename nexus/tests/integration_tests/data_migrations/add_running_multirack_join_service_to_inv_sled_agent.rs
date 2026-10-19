// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `add-running-multirack-join-service-to-inv-sled-agent` migration,
//! with checks that pre-existing rows are backfilled with `false`.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const RACK_ID: Uuid = Uuid::from_u128(0x31000001_0000_0000_0000_000000000001);

const SLED_1: Uuid = Uuid::from_u128(0x31000002_0000_0000_0000_000000000001);
const SLED_2: Uuid = Uuid::from_u128(0x31000002_0000_0000_0000_000000000002);

const INV_COLL_ID: Uuid =
    Uuid::from_u128(0x31000003_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!(
            "
                INSERT INTO omicron.public.inv_sled_agent (
                    inv_collection_id, time_collected, source,
                    sled_id, sled_agent_ip, sled_agent_port, sled_role,
                    usable_hardware_threads, usable_physical_ram,
                    reservoir_size, reconciler_status_kind,
                    zone_manifest_boot_disk_path,
                    mupdate_override_boot_disk_path, cpu_family,
                    measurement_manifest_boot_disk_path,
                    instance_manager_num_registered_vmms, rack_id
                ) VALUES
                    ('{INV_COLL_ID}', now(), 'test',
                     '{SLED_1}', '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', '/test', 'unknown',
                     '/test', 0, '{RACK_ID}'),
                    ('{INV_COLL_ID}', now(), 'test',
                     '{SLED_2}', '192.168.1.1', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', '/test', 'unknown',
                     '/test', 0, '{RACK_ID}');
            "
        ))
        .await
        .expect("migration 309 test data insertion should succeeed");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let expected = vec![(SLED_1, false), (SLED_2, false)];
    let actual: Vec<_> = ctx
        .client
        .query(
            "
            SELECT
                sled_id, running_multirack_join_service
            FROM
                omicron.public.inv_sled_agent
            ORDER BY
                sled_id
        ",
            &[],
        )
        .await
        .expect("migration 309 test query should succeed")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Uuid>("sled_id"),
                row.get::<_, bool>("running_multirack_join_service"),
            )
        })
        .collect();
    assert_eq!(expected, actual);

    // Now that the test has passed, clean up after ourselves:
    ctx.client
        .batch_execute(&format!(
            "
                DELETE FROM
                    omicron.public.inv_sled_agent
                WHERE sled_id IN
                    ('{SLED_1}', '{SLED_2}');
            "
        ))
        .await
        .expect("migration 309 test data cleanup should succeed");
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
