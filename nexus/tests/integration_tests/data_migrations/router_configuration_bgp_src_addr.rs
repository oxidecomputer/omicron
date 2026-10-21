// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::schema::{DataMigrationFns, MigrationContext};
use futures::FutureExt as _;
use futures::future::BoxFuture;

const CONFIG_ID: &str = "2f94753d-3bfa-4567-90a7-ab3f8b084632";

pub(super) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    async move {
        // Existing peers must remain usable when the optional column arrives.
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO omicron.public.router_configuration_bgp_peer (
                router_configuration_id, name, addr, port_name,
                router_lifetime, hold_time, keepalive, connect_retry,
                delay_open, idle_hold_time, communities, enforce_first_as
            ) VALUES
                ('{CONFIG_ID}', 'v4', '10.99.0.3', NULL, NULL,
                 6, 2, 3, 0, 3, ARRAY[]::INT8[], FALSE),
                ('{CONFIG_ID}', 'v6', '2001:db8::3', NULL, NULL,
                 6, 2, 3, 0, 3, ARRAY[]::INT8[], FALSE),
                ('{CONFIG_ID}', 'unnumbered', NULL, 'qsfp0', 0,
                 6, 2, 3, 0, 3, ARRAY[]::INT8[], FALSE)"
            ))
            .await
            .unwrap();
    }
    .boxed()
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    async move {
        let rows = ctx.client.query(&format!(
            "SELECT name, host(addr), port_name, host(src_addr)
             FROM omicron.public.router_configuration_bgp_peer
             WHERE router_configuration_id = '{CONFIG_ID}' ORDER BY name"
        ), &[]).await.unwrap();
        let peers: Vec<_> = rows.iter().map(|row| (
            row.get::<_, String>(0), row.get::<_, Option<String>>(1),
            row.get::<_, Option<String>>(2), row.get::<_, Option<String>>(3),
        )).collect();
        assert_eq!(peers, vec![
            ("unnumbered".into(), None, Some("qsfp0".into()), None),
            ("v4".into(), Some("10.99.0.3".into()), None, None),
            ("v6".into(), Some("2001:db8::3".into()), None, None),
        ]);

        for (name, source, invalid) in [
            ("v4", "10.99.0.4", "2001:db8::4"),
            ("v6", "2001:db8::4", "10.99.0.4"),
        ] {
            ctx.client.batch_execute(&format!(
                "UPDATE omicron.public.router_configuration_bgp_peer
                 SET src_addr = '{source}'
                 WHERE router_configuration_id = '{CONFIG_ID}' AND name = '{name}'"
            )).await.unwrap();
            for invalid in [invalid, "0.0.0.0", "::"] {
                let err = ctx.client.batch_execute(&format!(
                    "UPDATE omicron.public.router_configuration_bgp_peer
                     SET src_addr = '{invalid}'
                     WHERE router_configuration_id = '{CONFIG_ID}' AND name = '{name}'"
                )).await.unwrap_err();
                assert_eq!(err.code(), Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION));
            }
            let row = ctx.client.query_one(&format!(
                "SELECT host(src_addr) FROM omicron.public.router_configuration_bgp_peer
                 WHERE router_configuration_id = '{CONFIG_ID}' AND name = '{name}'"
            ), &[]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), source);
        }
        let err = ctx.client.batch_execute(&format!(
            "UPDATE omicron.public.router_configuration_bgp_peer
             SET src_addr = '10.99.0.4'
             WHERE router_configuration_id = '{CONFIG_ID}' AND name = 'unnumbered'"
        )).await.unwrap_err();
        assert_eq!(err.code(), Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION));
        // Clearing is valid for both families; clean up our rows for later checks.
        ctx.client.batch_execute(&format!(
            "UPDATE omicron.public.router_configuration_bgp_peer SET src_addr = NULL
             WHERE router_configuration_id = '{CONFIG_ID}';
             DELETE FROM omicron.public.router_configuration_bgp_peer
             WHERE router_configuration_id = '{CONFIG_ID}'"
        )).await.unwrap();
    }.boxed()
}
