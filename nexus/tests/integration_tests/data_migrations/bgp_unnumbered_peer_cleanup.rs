// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data migration test for BGP unnumbered peer cleanup (migration 253).
//!
//! This migration normalizes the `addr` representation of BGP unnumbered peers
//! to `NULL` across four tables and adds CHECK constraints to reject the
//! sentinel values `0.0.0.0` and `::`.
//!
//! In `switch_port_settings_bgp_peer_config`, `addr` was already nullable, so
//! unnumbered peers should have used `NULL`, but `0.0.0.0` and `::` were not
//! rejected. The migration also constrains `router_lifetime`: it must be 0 for
//! numbered peers (non-NULL addr) and in [0, 9000] for unnumbered peers.
//!
//! In the other three tables (`*_communities`, `*_allow_import`,
//! `*_allow_export`), `addr` was `NOT NULL`, so unnumbered peers used `0.0.0.0`
//! as a sentinel; `::` was not rejected. This migration makes `addr` nullable,
//! replaces the old composite primary key (which included `addr`) with a new
//! `id UUID` primary key, and converts sentinel values to `NULL`.
//!
//! For each table, we test:
//! - Rows with a numbered peer address are preserved unchanged
//! - Single unnumbered entries (`NULL`, `0.0.0.0`, or `::`) are normalized to
//!   `NULL`
//! - Duplicate unnumbered entries (`NULL` + sentinel(s), or both sentinels) are
//!   collapsed to a single `NULL` row
//! - For `bgp_peer_config` specifically: `router_lifetime` is forced to 0 for
//!   numbered peers and clamped to [0, 9000] for unnumbered peers

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

// Shared parent IDs. No FK constraints exist on these tables, so the
// referenced rows don't need to exist.
const PORT_SETTINGS: Uuid =
    Uuid::from_u128(0x25300001_0000_0000_0000_000000000001);
const BGP_CONFIG_1: Uuid =
    Uuid::from_u128(0x25300001_0000_0000_0000_000000000002);
const BGP_CONFIG_2: Uuid =
    Uuid::from_u128(0x25300001_0000_0000_0000_000000000003);

// bgp_peer_config row IDs
const PEER_NUMBERED_1: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000001);
const PEER_NUMBERED_2: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000002);
const PEER_SINGLE_NULL: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000003);
const PEER_SINGLE_V4: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000004);
const PEER_SINGLE_V6: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000005);
const PEER_NULL_V4_NULL: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000006);
const PEER_NULL_V4_V4: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000007);
const PEER_NULL_V6_NULL: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000008);
const PEER_NULL_V6_V6: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_000000000009);
const PEER_NULL_BOTH_NULL: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_00000000000a);
const PEER_NULL_BOTH_V4: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_00000000000b);
const PEER_NULL_BOTH_V6: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_00000000000c);
const PEER_V4_V6_V4: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_00000000000d);
const PEER_V4_V6_V6: Uuid =
    Uuid::from_u128(0x25300002_0000_0000_0000_00000000000e);

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!(
                "
                -- bgp_peer_config: test all addr/router_lifetime combos.
                INSERT INTO
                    omicron.public.switch_port_settings_bgp_peer_config
                    (port_settings_id, bgp_config_id, interface_name,
                     addr, id, router_lifetime)
                VALUES
                    -- Numbered peer: real addr, non-zero router_lifetime
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'numbered',
                     '10.0.0.1', '{PEER_NUMBERED_1}', 42),
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_2}', 'numbered',
                     '10.0.0.2', '{PEER_NUMBERED_2}', 42),
                    -- Single NULL addr, router_lifetime above max
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'single-null',
                     NULL, '{PEER_SINGLE_NULL}', 12000),
                    -- Single 0.0.0.0, valid router_lifetime
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'single-v4',
                     '0.0.0.0', '{PEER_SINGLE_V4}', 500),
                    -- Single ::
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'single-v6',
                     '::', '{PEER_SINGLE_V6}', 0),
                    -- NULL + 0.0.0.0
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'null-v4',
                     NULL, '{PEER_NULL_V4_NULL}', 100),
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_2}', 'null-v4',
                     '0.0.0.0', '{PEER_NULL_V4_V4}', 200),
                    -- NULL + ::
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'null-v6',
                     NULL, '{PEER_NULL_V6_NULL}', 100),
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'null-v6',
                     '::', '{PEER_NULL_V6_V6}', 200),
                    -- NULL + 0.0.0.0 + ::
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'null-both',
                     NULL, '{PEER_NULL_BOTH_NULL}', 100),
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'null-both',
                     '0.0.0.0', '{PEER_NULL_BOTH_V4}', 200),
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_2}', 'null-both',
                     '::', '{PEER_NULL_BOTH_V6}', 300),
                    -- 0.0.0.0 + :: (no NULL row)
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_2}', 'v4-v6',
                     '0.0.0.0', '{PEER_V4_V6_V4}', 400),
                    ('{PORT_SETTINGS}', '{BGP_CONFIG_1}', 'v4-v6',
                     '::', '{PEER_V4_V6_V6}', 500);

                INSERT INTO
                    omicron.public.switch_port_settings_bgp_peer_config_communities
                    (port_settings_id, interface_name, addr, community)
                VALUES
                    ('{PORT_SETTINGS}', 'numbered', '10.0.0.1', 100),
                    ('{PORT_SETTINGS}', 'single-v4', '0.0.0.0', 200),
                    ('{PORT_SETTINGS}', 'single-v6', '::', 300),
                    ('{PORT_SETTINGS}', 'both', '0.0.0.0', 400),
                    ('{PORT_SETTINGS}', 'both', '::', 400);

                INSERT INTO
                    omicron.public.switch_port_settings_bgp_peer_config_allow_import
                    (port_settings_id, interface_name, addr, prefix)
                VALUES
                    ('{PORT_SETTINGS}', 'numbered', '10.0.0.1',
                     '192.168.0.0/24'),
                    ('{PORT_SETTINGS}', 'single-v4', '0.0.0.0',
                     '10.0.0.0/8'),
                    ('{PORT_SETTINGS}', 'single-v6', '::',
                     'fd00::/64'),
                    ('{PORT_SETTINGS}', 'both', '0.0.0.0',
                     '172.16.0.0/12'),
                    ('{PORT_SETTINGS}', 'both', '::',
                     '172.16.0.0/12');

                INSERT INTO
                    omicron.public.switch_port_settings_bgp_peer_config_allow_export
                    (port_settings_id, interface_name, addr, prefix)
                VALUES
                    ('{PORT_SETTINGS}', 'numbered', '10.0.0.1',
                     '192.168.1.0/24'),
                    ('{PORT_SETTINGS}', 'single-v4', '0.0.0.0',
                     '10.0.0.0/8'),
                    ('{PORT_SETTINGS}', 'single-v6', '::',
                     'fd00::/64'),
                    ('{PORT_SETTINGS}', 'both', '0.0.0.0',
                     '172.16.0.0/12'),
                    ('{PORT_SETTINGS}', 'both', '::',
                     '172.16.0.0/12');
                "
            ))
            .await
            .expect("failed to insert test data for migration 251");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Check switch_port_settings_bgp_peer_config
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT \
                         id, \
                         bgp_config_id, \
                         interface_name, \
                         addr::text, \
                         router_lifetime \
                     FROM omicron.public.switch_port_settings_bgp_peer_config \
                     WHERE port_settings_id = '{PORT_SETTINGS}'"
                ),
                &[],
            )
            .await
            .expect("query bgp_peer_config");

        // 14 rows inserted, 5 deleted by migration → 9 remaining
        assert_eq!(rows.len(), 9, "bgp_peer_config row count");

        let peers: HashMap<(Uuid, Uuid), (Option<String>, i32)> = rows
            .iter()
            .map(|row| {
                let id: Uuid = row.get("id");
                let bgp_id: Uuid = row.get("bgp_config_id");
                let addr: Option<String> = match row.try_get("addr") {
                    Ok(addr) => addr,
                    Err(err) => panic!("couldn't get addr: {err:?}"),
                };
                let rl: i32 = row.get("router_lifetime");
                ((id, bgp_id), (addr, rl))
            })
            .collect();

        // Numbered: addr preserved, router_lifetime forced to 0
        let (addr, rl) = &peers[&(PEER_NUMBERED_1, BGP_CONFIG_1)];
        assert_eq!(addr.as_deref(), Some("10.0.0.1"));
        assert_eq!(*rl, 0, "numbered: router_lifetime forced to 0");
        let (addr, rl) = &peers[&(PEER_NUMBERED_2, BGP_CONFIG_2)];
        assert_eq!(addr.as_deref(), Some("10.0.0.2"));
        assert_eq!(*rl, 0, "numbered: router_lifetime forced to 0");

        // Single NULL: addr stays NULL, router_lifetime clamped to 9000
        let (addr, rl) = &peers[&(PEER_SINGLE_NULL, BGP_CONFIG_1)];
        assert_eq!(*addr, None);
        assert_eq!(*rl, 9000, "single-null: router_lifetime clamped");

        // Single 0.0.0.0 → NULL, router_lifetime preserved (in range)
        let (addr, rl) = &peers[&(PEER_SINGLE_V4, BGP_CONFIG_1)];
        assert_eq!(*addr, None);
        assert_eq!(*rl, 500, "single-v4: router_lifetime preserved");

        // Single :: → NULL
        let (addr, rl) = &peers[&(PEER_SINGLE_V6, BGP_CONFIG_1)];
        assert_eq!(*addr, None);
        assert_eq!(*rl, 0);

        // NULL + 0.0.0.0: NULL row kept, 0.0.0.0 deleted
        assert!(peers.contains_key(&(PEER_NULL_V4_NULL, BGP_CONFIG_1)));
        assert!(!peers.contains_key(&(PEER_NULL_V4_V4, BGP_CONFIG_2)));
        assert_eq!(peers[&(PEER_NULL_V4_NULL, BGP_CONFIG_1)].1, 100);

        // NULL + ::: NULL row kept, :: deleted
        assert!(peers.contains_key(&(PEER_NULL_V6_NULL, BGP_CONFIG_1)));
        assert!(!peers.contains_key(&(PEER_NULL_V6_V6, BGP_CONFIG_1)));
        assert_eq!(peers[&(PEER_NULL_V6_NULL, BGP_CONFIG_1)].1, 100);

        // NULL + 0.0.0.0 + ::: NULL row kept, both sentinels deleted
        assert!(peers.contains_key(&(PEER_NULL_BOTH_NULL, BGP_CONFIG_1)));
        assert!(!peers.contains_key(&(PEER_NULL_BOTH_V4, BGP_CONFIG_1)));
        assert!(!peers.contains_key(&(PEER_NULL_BOTH_V6, BGP_CONFIG_2)));
        assert_eq!(peers[&(PEER_NULL_BOTH_NULL, BGP_CONFIG_1)].1, 100);

        // 0.0.0.0 + ::: 0.0.0.0 row → NULL, :: deleted
        assert!(peers.contains_key(&(PEER_V4_V6_V4, BGP_CONFIG_2)));
        assert!(!peers.contains_key(&(PEER_V4_V6_V6, BGP_CONFIG_1)));
        let (addr, rl) = &peers[&(PEER_V4_V6_V4, BGP_CONFIG_2)];
        assert_eq!(*addr, None);
        assert_eq!(*rl, 400);

        // Each child table started with 5 rows; after migration, the
        // duplicate sentinel in the "both" group is removed → 4 rows.
        // Each row should now have a non-nil `id` column.
        check_child_table(
            ctx,
            "switch_port_settings_bgp_peer_config_communities",
            "community",
            &[
                // iface     addr              community
                ("numbered", Some("10.0.0.1"), "100"),
                ("single-v4", None, "200"),
                ("single-v6", None, "300"),
                ("both", None, "400"),
            ],
        )
        .await;

        check_child_table(
            ctx,
            "switch_port_settings_bgp_peer_config_allow_import",
            "prefix",
            &[
                // iface     addr              prefix
                ("numbered", Some("10.0.0.1"), "192.168.0.0/24"),
                ("single-v4", None, "10.0.0.0/8"),
                ("single-v6", None, "fd00::/64"),
                ("both", None, "172.16.0.0/12"),
            ],
        )
        .await;

        check_child_table(
            ctx,
            "switch_port_settings_bgp_peer_config_allow_export",
            "prefix",
            &[
                // iface     addr              prefix
                ("numbered", Some("10.0.0.1"), "192.168.1.0/24"),
                ("single-v4", None, "10.0.0.0/8"),
                ("single-v6", None, "fd00::/64"),
                ("both", None, "172.16.0.0/12"),
            ],
        )
        .await;

        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM
                    omicron.public.switch_port_settings_bgp_peer_config
                    WHERE port_settings_id = '{PORT_SETTINGS}';
                DELETE FROM
                    omicron.public.switch_port_settings_bgp_peer_config_communities
                    WHERE port_settings_id = '{PORT_SETTINGS}';
                DELETE FROM
                    omicron.public.switch_port_settings_bgp_peer_config_allow_import
                    WHERE port_settings_id = '{PORT_SETTINGS}';
                DELETE FROM
                    omicron.public.switch_port_settings_bgp_peer_config_allow_export
                    WHERE port_settings_id = '{PORT_SETTINGS}';
                "
            ))
            .await
            .expect("cleanup migration 251 test data");
    })
}

/// Verify a child table after migration: each row has a non-nil `id`, all the
/// `id`s are distinct, the expected number of rows survived, and `addr` was
/// normalized correctly.
async fn check_child_table(
    ctx: &MigrationContext<'_>,
    table: &str,
    value_col: &str,
    expected: &[(&str, Option<&str>, &str)],
) {
    let query = format!(
        "SELECT id, interface_name, addr::text, {value_col}::text as val \
         FROM omicron.public.{table} \
         WHERE port_settings_id = '{PORT_SETTINGS}' \
         ORDER BY interface_name, val"
    );
    let rows = ctx
        .client
        .query(&query, &[])
        .await
        .unwrap_or_else(|e| panic!("query {table}: {e}"));

    assert_eq!(rows.len(), expected.len(), "{table}: unexpected row count");

    let mut ids_seen = BTreeSet::new();
    for row in &rows {
        let id: Uuid = row.get("id");
        assert!(!id.is_nil(), "{table}: id should be non-nil");
        ids_seen.insert(id);

        let iface: String = row.get("interface_name");
        let addr: Option<String> = row.get("addr");
        let val: String = row.get("val");

        let (_, exp_addr, _) = expected
            .iter()
            .find(|(i, _, v)| *i == iface.as_str() && *v == val.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "{table}: didn't find expected row: \
                     interface={iface}, {value_col}={val}"
                )
            });

        assert_eq!(
            addr.as_deref(),
            *exp_addr,
            "{table}: wrong addr for interface={iface}, {value_col}={val}"
        );
    }

    assert_eq!(
        ids_seen.len(),
        rows.len(),
        "unexpected set of unique IDs: {ids_seen:?}",
    );
}
