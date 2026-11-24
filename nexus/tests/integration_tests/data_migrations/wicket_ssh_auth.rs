// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `wicket-ssh-auth` migration, with checks for the backfill of
//! the newly added `wicket_ssh_auth_generation` columns (in both blueprints and
//! inventory collections) and the new `inv_sled_agent` status columns.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const RACK_ID: Uuid = Uuid::from_u128(0x32100001_0000_0000_0000_000000000001);

const BP_ID: Uuid = Uuid::from_u128(0x32100002_0000_0000_0000_000000000001);
const SLED_1: Uuid = Uuid::from_u128(0x32100003_0000_0000_0000_000000000001);
const SLED_2: Uuid = Uuid::from_u128(0x32100003_0000_0000_0000_000000000002);

const INV_COLL_ID: Uuid =
    Uuid::from_u128(0x32100004_0000_0000_0000_000000000001);
const SLED_CONFIG_1: Uuid =
    Uuid::from_u128(0x32100005_0000_0000_0000_000000000001);
const SLED_CONFIG_2: Uuid =
    Uuid::from_u128(0x32100005_0000_0000_0000_000000000002);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    // Insert pre-migration rows in each of the three existing tables that gain
    // columns. None of the wicket SSH auth columns exist yet.
    ctx.client
        .batch_execute(&format!(
            "
                INSERT INTO omicron.public.bp_sled_metadata (
                    blueprint_id, sled_id, sled_state, sled_agent_generation,
                    subnet, last_allocated_ip_subnet_offset, measurements,
                    update_disposition_generation, update_availability
                ) VALUES
                    ('{BP_ID}', '{SLED_1}', 'active', 1,
                     'fd00:1122:3344:0101::/64', 32, 'unknown',
                     1, 'available'),
                    ('{BP_ID}', '{SLED_2}', 'active', 1,
                     'fd00:1122:3344:0102::/64', 32, 'unknown',
                     1, 'available');

                INSERT INTO omicron.public.inv_omicron_sled_config (
                    inv_collection_id, id, generation, update_disposition
                ) VALUES
                    ('{INV_COLL_ID}', '{SLED_CONFIG_1}', 1, 'available'),
                    ('{INV_COLL_ID}', '{SLED_CONFIG_2}', 2, 'available');

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
                     '{SLED_1}', '192.168.1.1', 8080, 'scrimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', '/test', 'unknown',
                     '/test', 0, '{RACK_ID}'),
                    ('{INV_COLL_ID}', now(), 'test',
                     '{SLED_2}', '192.168.1.2', 8080, 'gimlet',
                     32, 68719476736, 1073741824, 'not-yet-run',
                     '/test', '/test', 'unknown',
                     '/test', 0, '{RACK_ID}');
            "
        ))
        .await
        .expect("wicket-ssh-auth test data insertion should succeed");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    // Every pre-existing blueprint sled is backfilled to generation 1.
    let actual: Vec<_> = ctx
        .client
        .query(
            "
            SELECT sled_id, wicket_ssh_auth_generation
            FROM omicron.public.bp_sled_metadata
            WHERE blueprint_id = $1
            ORDER BY sled_id
            ",
            &[&BP_ID],
        )
        .await
        .expect("queried bp_sled_metadata")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Uuid>("sled_id"),
                row.get::<_, i64>("wicket_ssh_auth_generation"),
            )
        })
        .collect();
    assert_eq!(actual, vec![(SLED_1, 1), (SLED_2, 1)]);

    // Every pre-existing inventory sled config is backfilled to generation 1.
    let actual: Vec<_> = ctx
        .client
        .query(
            "
            SELECT id, wicket_ssh_auth_generation
            FROM omicron.public.inv_omicron_sled_config
            WHERE inv_collection_id = $1
            ORDER BY id
            ",
            &[&INV_COLL_ID],
        )
        .await
        .expect("queried inv_omicron_sled_config")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Uuid>("id"),
                row.get::<_, i64>("wicket_ssh_auth_generation"),
            )
        })
        .collect();
    assert_eq!(actual, vec![(SLED_CONFIG_1, 1), (SLED_CONFIG_2, 1)]);

    // Pre-existing inventory sled-agent rows have no wicket SSH auth status
    // (which reads back as "no switch zone", even for scrimlets).
    let actual: Vec<_> = ctx
        .client
        .query(
            "
            SELECT
                sled_id,
                wicket_ssh_auth_effective_gen,
                wicket_ssh_auth_last_error
            FROM omicron.public.inv_sled_agent
            WHERE inv_collection_id = $1
            ORDER BY sled_id
            ",
            &[&INV_COLL_ID],
        )
        .await
        .expect("queried inv_sled_agent")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Uuid>("sled_id"),
                row.get::<_, Option<i64>>("wicket_ssh_auth_effective_gen"),
                row.get::<_, Option<String>>("wicket_ssh_auth_last_error"),
            )
        })
        .collect();
    assert_eq!(actual, vec![(SLED_1, None, None), (SLED_2, None, None)]);

    // The new CHECK constraint rejects an error without an effective
    // generation...
    let err = ctx
        .client
        .batch_execute(&format!(
            "
            UPDATE omicron.public.inv_sled_agent
            SET wicket_ssh_auth_last_error = 'oops'
            WHERE inv_collection_id = '{INV_COLL_ID}'
              AND sled_id = '{SLED_1}';
            "
        ))
        .await
        .expect_err("last error without effective gen violates constraint");
    assert_eq!(
        err.as_db_error().expect("error came from the database").constraint(),
        Some("wicket_ssh_auth_last_error_only_if_have_effective_gen"),
    );

    // ... but accepts an effective generation with or without an error.
    ctx.client
        .batch_execute(&format!(
            "
            UPDATE omicron.public.inv_sled_agent
            SET wicket_ssh_auth_effective_gen = 1
            WHERE inv_collection_id = '{INV_COLL_ID}'
              AND sled_id = '{SLED_1}';

            UPDATE omicron.public.inv_sled_agent
            SET wicket_ssh_auth_last_error = 'oops'
            WHERE inv_collection_id = '{INV_COLL_ID}'
              AND sled_id = '{SLED_1}';
            "
        ))
        .await
        .expect("effective gen with and without error satisfies constraint");

    // `wicket_ssh_auth` rejects generation 0.
    let err = ctx
        .client
        .batch_execute(
            "
            INSERT INTO omicron.public.wicket_ssh_auth (
                generation, time_created, description, allow_support_keys
            ) VALUES (0, now(), 'invalid generation', NULL);
            ",
        )
        .await
        .expect_err("generation 0 violates constraint");
    assert_eq!(
        err.as_db_error().expect("error came from the database").code(),
        &tokio_postgres::error::SqlState::CHECK_VIOLATION,
    );

    // Clean up after ourselves.
    ctx.client
        .batch_execute(&format!(
            "
            DELETE FROM omicron.public.bp_sled_metadata
                WHERE blueprint_id = '{BP_ID}';
            DELETE FROM omicron.public.inv_omicron_sled_config
                WHERE inv_collection_id = '{INV_COLL_ID}';
            DELETE FROM omicron.public.inv_sled_agent
                WHERE inv_collection_id = '{INV_COLL_ID}';
            "
        ))
        .await
        .expect("wicket-ssh-auth test data cleanup should succeed");
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
