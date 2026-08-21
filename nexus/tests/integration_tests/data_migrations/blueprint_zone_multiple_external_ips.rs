// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data migration test for moving blueprint zone external IPs from inline
//! columns on `bp_omicron_zone` into the new `bp_omicron_zone_external_ip`
//! child table.

use crate::integration_tests::schema::DataMigrationFns;
use crate::integration_tests::schema::MigrationContext;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use uuid::Uuid;

// IDs for the inserted rows.
const BLUEPRINT_ID: &str = "6b7e5a1a-8f1c-4a1b-9b6d-2f6b0e7c1a2b";
const SLED_ID: &str = "0b6f1a3c-2e4d-4a5b-8c9d-1e2f3a4b5c6d";
const FILESYSTEM_POOL: &str = "9d8c7b6a-5e4f-3a2b-1c0d-9e8f7a6b5c4d";
const NEXUS_ZONE_ID: &str = "ab1d1dc9-2737-4c60-8aeb-f1da4ac92d93";
const DNS_ZONE_ID: &str = "55b704d9-0118-4f34-974a-87a27186d564";
const NTP_ZONE_ID: &str = "1af903af-131a-463c-b086-c7aded7fb2c1";
const INTERNAL_DNS_ZONE_ID: &str = "675dc9cb-2d77-4d67-9ca2-cec939fa797c";
const CRUCIBLE_ZONE_ID: &str = "8639e2e7-9499-46a7-9927-ea6e6a81b1d3";

// The allocated external IP IDs for the three external-networking zones.
const NEXUS_EIP_ID: &str = "3c2b1a09-8f7e-4d6c-5b4a-3928170615a4";
const DNS_EIP_ID: &str = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d";
const NTP_EIP_ID: &str = "d4c3b2a1-f6e5-4b7a-9d8c-5d4c3b2a1f6e";

// The internal DNS zone's underlay `dns_address`, which stays in
// `second_service_ip` / `second_service_port` columns. This is to check that
// the migration doesn't remove these, since they aren't external IPs.
const INTERNAL_DNS_IP: &str = "fd00::1";
const INTERNAL_DNS_PORT: i32 = 53;

pub(super) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    before_impl(ctx).boxed()
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    // Insert blueprint zone rows with the old, inlined external IPs directly in
    // the row. The zones are:
    //
    // - Nexus: external IP but no ports at all
    // - External DNS: external IP and one port, but no SNAT first / last.
    // - NTP: external IP, no `port`, and an SNAT first / last port.
    //
    // Also insert an _internal_ DNS zone, which reuses the `second_service_ip`
    // column for its _underlay_ address, and a Crucible zone, which has no
    // secondary or external IPs at all.
    let sql = format!(
        "INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            second_service_ip, second_service_port,
            snat_ip, snat_first_port, snat_last_port,
            external_ip_id, filesystem_pool,
            disposition, disposition_expunged_ready_for_cleanup,
            image_source, nexus_generation, nexus_lockstep_port
        ) VALUES
        ('{BLUEPRINT_ID}', '{SLED_ID}', '{NEXUS_ZONE_ID}', 'nexus',
         '::1', 12345, '192.0.2.1', NULL, NULL, NULL, NULL,
         '{NEXUS_EIP_ID}', '{FILESYSTEM_POOL}',
         'in_service', FALSE, 'install_dataset', 1, 12346),
        ('{BLUEPRINT_ID}', '{SLED_ID}', '{DNS_ZONE_ID}', 'external_dns',
         '::1', 5353, '192.0.2.2', 53, NULL, NULL, NULL,
         '{DNS_EIP_ID}', '{FILESYSTEM_POOL}',
         'in_service', FALSE, 'install_dataset', NULL, NULL),
        ('{BLUEPRINT_ID}', '{SLED_ID}', '{NTP_ZONE_ID}', 'boundary_ntp',
         '::1', 123, NULL, NULL, '192.0.2.3', 0, 16383,
         '{NTP_EIP_ID}', '{FILESYSTEM_POOL}',
         'in_service', FALSE, 'install_dataset', NULL, NULL),
        ('{BLUEPRINT_ID}', '{SLED_ID}', '{INTERNAL_DNS_ZONE_ID}', \
         'internal_dns',
         '::1', 5353, '{INTERNAL_DNS_IP}', {INTERNAL_DNS_PORT}, NULL, NULL, \
         NULL, NULL, '{FILESYSTEM_POOL}',
         'in_service', FALSE, 'install_dataset', NULL, NULL),
        ('{BLUEPRINT_ID}', '{SLED_ID}', '{CRUCIBLE_ZONE_ID}', 'crucible',
         '::1', 32345, NULL, NULL, NULL, NULL, NULL,
         NULL, '{FILESYSTEM_POOL}',
         'in_service', FALSE, 'install_dataset', NULL, NULL)",
    );
    ctx.client
        .batch_execute(&sql)
        .await
        .expect("inserted old-schema blueprint zone rows");
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    after_impl(ctx).boxed()
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let blueprint_id = Uuid::parse_str(BLUEPRINT_ID).unwrap();
    let rows = ctx
        .client
        .query(
            "SELECT \
                zone_id, \
                external_ip_id, \
                host(ip) AS ip, \
                port, \
                snat_first_port, \
                snat_last_port \
            FROM omicron.public.bp_omicron_zone_external_ip \
            WHERE blueprint_id = $1",
            &[&blueprint_id],
        )
        .await
        .expect("queried external IP child table");

    let mut by_zone = BTreeMap::new();
    for row in rows {
        let zone_id: Uuid = row.get("zone_id");
        let external_ip_id: Uuid = row.get("external_ip_id");
        let ip: String = row.get("ip");
        let port: Option<i32> = row.get("port");
        let snat_first_port: Option<i32> = row.get("snat_first_port");
        let snat_last_port: Option<i32> = row.get("snat_last_port");
        let maybe_old = by_zone.insert(
            zone_id,
            (external_ip_id, ip, port, snat_first_port, snat_last_port),
        );
        assert!(
            maybe_old.is_none(),
            "Duplicate zone ID '{}' when reading EIP rows",
            zone_id,
        );
    }

    // Only the three zones with actual external IPs should have been
    // backfilled.
    assert_eq!(
        by_zone.len(),
        3,
        "expected exactly three external IP rows, found {}",
        by_zone.len(),
    );

    let nexus = NEXUS_ZONE_ID.parse().unwrap();
    assert_eq!(
        by_zone.get(&nexus),
        Some(&(
            NEXUS_EIP_ID.parse().unwrap(),
            "192.0.2.1".to_string(),
            None,
            None,
            None,
        )),
        "Nexus external IP should be a floating IP with no ports",
    );

    let dns = DNS_ZONE_ID.parse().unwrap();
    assert_eq!(
        by_zone.get(&dns),
        Some(&(
            DNS_EIP_ID.parse().unwrap(),
            "192.0.2.2".to_string(),
            Some(53),
            None,
            None,
        )),
        "external DNS external IP should carry its DNS port",
    );

    let ntp = NTP_ZONE_ID.parse().unwrap();
    assert_eq!(
        by_zone.get(&ntp),
        Some(&(
            NTP_EIP_ID.parse().unwrap(),
            "192.0.2.3".to_string(),
            None,
            Some(0),
            Some(16383),
        )),
        "boundary NTP external IP should carry its SNAT port range",
    );

    // Crucible has only the primary IP and no external IPs.
    let crucible = CRUCIBLE_ZONE_ID.parse().unwrap();
    assert!(
        !by_zone.contains_key(&crucible),
        "a zone without external networking should have no external IP row",
    );

    // The moved rows should have had their inline `second_service_ip` /
    // `second_service_port` cleared, while the internal DNS zone's underlay
    // address must be preserved.
    let zone_rows = ctx
        .client
        .query(
            "SELECT \
                id, \
                host(second_service_ip) AS second_service_ip, \
                second_service_port \
            FROM omicron.public.bp_omicron_zone \
            WHERE blueprint_id = $1",
            &[&blueprint_id],
        )
        .await
        .expect("queried blueprint zone rows");

    let mut second_service_by_zone = BTreeMap::new();
    for row in zone_rows {
        let id: Uuid = row.get("id");
        let ip: Option<String> = row.get("second_service_ip");
        let port: Option<i32> = row.get("second_service_port");
        let maybe_old = second_service_by_zone.insert(id, (ip, port));
        assert!(
            maybe_old.is_none(),
            "Duplicate zone ID '{}' when reading blueprint rows",
            id,
        );
    }

    assert_eq!(
        second_service_by_zone.get(&nexus),
        Some(&(None, None)),
        "Nexus second_service_ip/port should have been cleared",
    );
    assert_eq!(
        second_service_by_zone.get(&dns),
        Some(&(None, None)),
        "External DNS second_service_ip/port should have been cleared",
    );
    assert_eq!(
        second_service_by_zone.get(&ntp),
        Some(&(None, None)),
        "Boundary NTP second_service_ip/port should have been cleared",
    );

    let internal_dns = Uuid::parse_str(INTERNAL_DNS_ZONE_ID).unwrap();
    assert_eq!(
        second_service_by_zone.get(&internal_dns),
        Some(&(Some(INTERNAL_DNS_IP.to_string()), Some(INTERNAL_DNS_PORT))),
        "internal DNS underlay dns_address must be preserved",
    );
}
