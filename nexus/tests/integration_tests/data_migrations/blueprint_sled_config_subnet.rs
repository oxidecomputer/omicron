// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use uuid::Uuid;

// randomly-generated IDs
const SLED_ID_1: &str = "48e1d10b-3ec8-4abd-95e2-5f37f79da546";
const SLED_ID_2: &str = "2144219e-d989-4929-b0b3-db895f51b7b5";
const SLED_ID_3: &str = "d2de378b-c788-4940-9a11-60ac4e633f0c";

// randomly-generated IPs
const SLED_IP_1: &str = "445c:212f:f2bc:f202:2b90:5b0e:2d6c:585e";
const SLED_IP_2: &str = "05fb:7e45:22b1:8dfb:9a06:0dbf:22ab:3eb4";
const SLED_IP_3: &str = "92a6:42c8:0cf2:7556:5ace:5bab:26e4:4dd1";

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!(
            "
    INSERT INTO omicron.public.sled (
        id, time_created, time_modified, rcgen, rack_id, is_scrimlet,
        serial_number, part_number, revision, usable_hardware_threads,
        usable_physical_ram, reservoir_size, ip, port, last_used_address,
        sled_policy, sled_state, repo_depot_port, cpu_family
    )
    VALUES
    ('{SLED_ID_1}', now(), now(), 1, gen_random_uuid(), false,
     'migration-210-serial1', 'part1', 1, 64, 12345678, 123456,
     '{SLED_IP_1}', 12345, '{SLED_IP_1}', 'in_service', 'active',
     12346, 'unknown'),
    ('{SLED_ID_2}', now(), now(), 1, gen_random_uuid(), false,
     'migration-210-serial2', 'part1', 1, 64, 12345678, 123456,
     '{SLED_IP_2}', 12345, '{SLED_IP_2}', 'no_provision', 'active',
     12346, 'unknown'),
    ('{SLED_ID_3}', now(), now(), 1, gen_random_uuid(), false,
     'migration-210-serial3', 'part1', 1, 64, 12345678, 123456,
     '{SLED_IP_3}', 12345, '{SLED_IP_3}', 'expunged', 'decommissioned',
     12346, 'unknown');

    INSERT INTO omicron.public.bp_sled_metadata (
        blueprint_id, sled_id, sled_state, sled_agent_generation,
        remove_mupdate_override, host_phase_2_desired_slot_a,
        host_phase_2_desired_slot_b
    )
    VALUES
    (gen_random_uuid(), '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL),
    (gen_random_uuid(), '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL),
    (gen_random_uuid(), '{SLED_ID_3}', 'decommissioned', 1,
     NULL, NULL, NULL);
            "
        ))
        .await
        .expect("inserted pre-migration data");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let sled_id_1: Uuid = SLED_ID_1.parse().unwrap();
    let sled_id_2: Uuid = SLED_ID_2.parse().unwrap();
    let sled_id_3: Uuid = SLED_ID_3.parse().unwrap();

    let expected = [
        (
            sled_id_1,
            Ipv6Subnet::<SLED_PREFIX>::new(SLED_IP_1.parse().unwrap())
                .to_string(),
        ),
        (
            sled_id_2,
            Ipv6Subnet::<SLED_PREFIX>::new(SLED_IP_2.parse().unwrap())
                .to_string(),
        ),
        (
            sled_id_3,
            Ipv6Subnet::<SLED_PREFIX>::new(SLED_IP_3.parse().unwrap())
                .to_string(),
        ),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();

    let rows = ctx
        .client
        .query(
            "
            SELECT sled_id, subnet::text
            FROM omicron.public.bp_sled_metadata
            WHERE sled_id IN ($1, $2, $3)
            ",
            &[&sled_id_1, &sled_id_2, &sled_id_3],
        )
        .await
        .expect("queried post-migration data");
    let got = rows
        .into_iter()
        .map(|row| {
            (row.get::<_, Uuid>("sled_id"), row.get::<_, String>("subnet"))
        })
        .collect::<BTreeSet<_>>();
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
