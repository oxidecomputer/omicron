// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use uuid::Uuid;

// randomly-generated IDs
const SLED_ID_1: &str = "ee1f2a5e-6b82-4487-8e78-779cf2b0a860";
const SLED_ID_2: &str = "a653a2d2-91f7-4604-adda-9bcc525db254";
const SLED_ID_3: &str = "8317bea6-bebc-4e58-ae09-8f1cef72ff4d";

const BP_ID_1: &str = "64ec2dba-922c-43b3-a576-699c3564d729";
const BP_ID_2: &str = "dd58e3d3-2760-48e9-8669-6ff0f076ba84";
const BP_ID_3: &str = "046b9d8f-f152-4370-86b8-e48445670aff";

const SLED_SUBNET_1: &str = "fd00:1122:3344:0101";
const SLED_SUBNET_2: &str = "fd00:1122:3344:0102";
const SLED_SUBNET_3: &str = "fd00:1122:3344:0103";

async fn before_impl(ctx: &MigrationContext<'_>) {
    // Blueprint 1: 2 sleds with 3 zones each, IPs with final hextet both
    // above and below ::20 (the legacy value of SLED_RESERVED_ADDRESSES).
    //
    // Blueprint 2: all 3 sleds, but the third sled has no zones
    //
    // Blueprint 3: all 3 sleds with zones, but the third has no zones with
    // IPs above ::20.
    ctx.client
        .batch_execute(&format!(
            "
    -- Remove detritus from earlier migrations.
    DELETE FROM omicron.public.bp_sled_metadata WHERE 1=1;
    DELETE FROM omicron.public.bp_omicron_zone WHERE 1=1;

    -- Blueprint 1
    INSERT INTO omicron.public.bp_sled_metadata (
        blueprint_id, sled_id, sled_state, sled_agent_generation,
        remove_mupdate_override, host_phase_2_desired_slot_a,
        host_phase_2_desired_slot_b, subnet
    )
    VALUES
    ('{BP_ID_1}', '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_1}::/64'),
    ('{BP_ID_1}', '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_2}::/64');

    -- Blueprint 1 sled 1 zones
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_1}', '{SLED_ID_1}', gen_random_uuid(), 'clickhouse',
     '{SLED_SUBNET_1}::10', 8080, gen_random_uuid(), 'in_service',
     false, 'install_dataset'),
    ('{BP_ID_1}', '{SLED_ID_1}', gen_random_uuid(), 'oximeter',
     '{SLED_SUBNET_1}::50', 8081, gen_random_uuid(), 'in_service',
     false, 'install_dataset'),
    ('{BP_ID_1}', '{SLED_ID_1}', gen_random_uuid(), 'crucible',
     '{SLED_SUBNET_1}::100', 8082, gen_random_uuid(), 'in_service',
     false, 'install_dataset');

    -- Blueprint 1 sled 2 zones
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'clickhouse',
     '{SLED_SUBNET_2}::15', 8080, gen_random_uuid(), 'in_service',
     false, 'install_dataset'),
    ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'oximeter',
     '{SLED_SUBNET_2}::25', 8081, gen_random_uuid(), 'in_service',
     false, 'install_dataset'),
    ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'crucible',
     '{SLED_SUBNET_2}::200', 8082, gen_random_uuid(), 'in_service',
     false, 'install_dataset'),
    ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'internal_dns',
     '{SLED_SUBNET_2}::ffff', 8053, gen_random_uuid(), 'in_service',
     false, 'install_dataset');

    -- Blueprint 2
    INSERT INTO omicron.public.bp_sled_metadata (
        blueprint_id, sled_id, sled_state, sled_agent_generation,
        remove_mupdate_override, host_phase_2_desired_slot_a,
        host_phase_2_desired_slot_b, subnet
    )
    VALUES
    ('{BP_ID_2}', '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_1}::/64'),
    ('{BP_ID_2}', '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_2}::/64'),
    ('{BP_ID_2}', '{SLED_ID_3}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_3}::/64');

    -- Blueprint 2 sled 1 zones
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_2}', '{SLED_ID_1}', gen_random_uuid(), 'clickhouse',
     '{SLED_SUBNET_1}::150', 8080, gen_random_uuid(), 'in_service',
     false, 'install_dataset');

    -- Blueprint 2 sled 2 zones
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_2}', '{SLED_ID_2}', gen_random_uuid(), 'oximeter',
     '{SLED_SUBNET_2}::250', 8081, gen_random_uuid(), 'in_service',
     false, 'install_dataset');

    -- Blueprint 2 sled 3: NO zones

    -- Blueprint 3
    INSERT INTO omicron.public.bp_sled_metadata (
        blueprint_id, sled_id, sled_state, sled_agent_generation,
        remove_mupdate_override, host_phase_2_desired_slot_a,
        host_phase_2_desired_slot_b, subnet
    )
    VALUES
    ('{BP_ID_3}', '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_1}::/64'),
    ('{BP_ID_3}', '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_2}::/64'),
    ('{BP_ID_3}', '{SLED_ID_3}', 'active', 1, NULL, NULL, NULL,
     '{SLED_SUBNET_3}::/64');

    -- Blueprint 3 sled 1
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_3}', '{SLED_ID_1}', gen_random_uuid(), 'clickhouse',
     '{SLED_SUBNET_1}::300', 8080, gen_random_uuid(), 'in_service',
     false, 'install_dataset');

    -- Blueprint 3 sled 2
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_3}', '{SLED_ID_2}', gen_random_uuid(), 'oximeter',
     '{SLED_SUBNET_2}::400', 8081, gen_random_uuid(), 'in_service',
     false, 'install_dataset');

    -- Blueprint 3 sled 3
    INSERT INTO omicron.public.bp_omicron_zone (
        blueprint_id, sled_id, id, zone_type,
        primary_service_ip, primary_service_port,
        filesystem_pool, disposition,
        disposition_expunged_ready_for_cleanup, image_source
    )
    VALUES
    ('{BP_ID_3}', '{SLED_ID_3}', gen_random_uuid(), 'crucible',
     '{SLED_SUBNET_3}::5', 8082, gen_random_uuid(), 'in_service',
     false, 'install_dataset'),
    ('{BP_ID_3}', '{SLED_ID_3}', gen_random_uuid(), 'clickhouse',
     '{SLED_SUBNET_3}::1f', 8080, gen_random_uuid(), 'in_service',
     false, 'install_dataset');
            "
        ))
        .await
        .expect("inserted pre-migration data");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let bp_id_1: Uuid = BP_ID_1.parse().unwrap();
    let bp_id_2: Uuid = BP_ID_2.parse().unwrap();
    let bp_id_3: Uuid = BP_ID_3.parse().unwrap();
    let sled_id_1: Uuid = SLED_ID_1.parse().unwrap();
    let sled_id_2: Uuid = SLED_ID_2.parse().unwrap();
    let sled_id_3: Uuid = SLED_ID_3.parse().unwrap();

    // BP1, Sled1: max IP is ::100
    // BP1, Sled2: max IP is ::200
    // BP2, Sled1: max IP is ::150
    // BP2, Sled2: max IP is ::250
    // BP2, Sled3: no zones, default to 32
    // BP3, Sled1: max IP is ::300
    // BP3, Sled2: max IP is ::400
    // BP3, Sled3: max IP is ::1f (31); should get bumped to 32
    let expected = [
        ((bp_id_1, sled_id_1), 0x100),
        ((bp_id_1, sled_id_2), 0x200),
        ((bp_id_2, sled_id_1), 0x150),
        ((bp_id_2, sled_id_2), 0x250),
        ((bp_id_2, sled_id_3), 32),
        ((bp_id_3, sled_id_1), 0x300),
        ((bp_id_3, sled_id_2), 0x400),
        ((bp_id_3, sled_id_3), 32),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();

    let rows = ctx
        .client
        .query(
            "
            SELECT blueprint_id, sled_id, last_allocated_ip_subnet_offset
            FROM omicron.public.bp_sled_metadata
            WHERE blueprint_id IN ($1, $2, $3)
            ORDER BY blueprint_id, sled_id
            ",
            &[&bp_id_1, &bp_id_2, &bp_id_3],
        )
        .await
        .expect("queried post-migration data");

    let got = rows
        .into_iter()
        .map(|row| {
            (
                (
                    row.get::<_, Uuid>("blueprint_id"),
                    row.get::<_, Uuid>("sled_id"),
                ),
                row.get::<_, i32>("last_allocated_ip_subnet_offset"),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(expected, got);
}

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
