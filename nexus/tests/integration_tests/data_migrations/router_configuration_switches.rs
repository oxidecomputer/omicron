// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Preserve existing RC identities, settings and assignments through migration 302.
use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;

const RC: &str = "30200000-0000-4000-8000-000000000001";
const SILO: &str = "30200000-0000-4000-8000-000000000002";
const SET: &str = "30200000-0000-4000-8000-000000000003";
const DEFAULT: &str = "001de000-defa-4000-8000-000000000000";

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client.batch_execute(&format!("
            INSERT INTO omicron.public.router_configuration
                (id, name, description, time_created, time_modified, switch,
                 bgp_asn, bgp_max_paths, bgp_announce_set_id)
            VALUES ('{RC}', 'multi-switch-migration', 'preserve me', now(), now(),
                    'switch1', 65042, 4, '{SET}'),
                   ('{DEFAULT}', 'default', 'populated builtin override', now(), now(),
                    'switch0', 65043, 2, '{SET}');
            INSERT INTO omicron.public.router_configuration_bgp_peer
                (router_configuration_id, name, addr, src_addr, hold_time, keepalive,
                 connect_retry, delay_open, idle_hold_time, communities, enforce_first_as)
            VALUES ('{RC}', 'peer', '192.0.2.1', '192.0.2.2', 6, 2, 3, 0, 3, ARRAY[42], true);
            INSERT INTO omicron.public.router_configuration_static_route
                (router_configuration_id, name, dst, gw, rib_priority, vlan_id)
            VALUES ('{RC}', 'route', '0.0.0.0/0', '192.0.2.1', 4, 100);
            INSERT INTO omicron.public.router_configuration_bfd_peer
                (router_configuration_id, name, remote, local, mode, detection_threshold, required_rx)
            VALUES ('{RC}', 'bfd', '192.0.2.1', '192.0.2.2', 'single_hop', 3, 4294967295);
            INSERT INTO omicron.public.silo_router_configuration
                (silo_id, router_configuration_id, priority) VALUES ('{SILO}', '{RC}', 123);
            DELETE FROM omicron.public.control_plane_router_configuration;
        ")).await.unwrap();
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let row = ctx.client.query_one(&format!("
            SELECT switch::TEXT, bgp_asn, bgp_max_paths::INT8, bgp_announce_set_id::TEXT
            FROM omicron.public.router_configuration_switch WHERE router_configuration_id='{RC}'"), &[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "switch1");
        assert_eq!(row.get::<_, i64>(1), 65042);
        assert_eq!(row.get::<_, i64>(2), 4);
        assert_eq!(row.get::<_, String>(3), SET);
        let row = ctx.client.query_one(&format!("SELECT bgp_asn FROM omicron.public.router_configuration_switch WHERE router_configuration_id='{DEFAULT}'"), &[]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 65043);
        for table in ["bgp_peer", "static_route", "bfd_peer"] {
            let row = ctx.client.query_one(&format!("SELECT switch::TEXT FROM omicron.public.router_configuration_{table} WHERE router_configuration_id='{RC}'"), &[]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), "switch1");
        }
        let row = ctx.client.query_one(&format!("SELECT required_rx FROM omicron.public.router_configuration_bfd_peer WHERE router_configuration_id='{RC}'"), &[]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), i64::from(u32::MAX));
        let row = ctx.client.query_one(&format!("SELECT router_configuration_id::TEXT, priority::INT8 FROM omicron.public.silo_router_configuration WHERE silo_id='{SILO}'"), &[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), RC);
        assert_eq!(row.get::<_, i64>(1), 123);
        let row = ctx.client.query_one("SELECT count(*) FROM omicron.public.control_plane_router_configuration", &[]).await.unwrap();
        assert_eq!(
            row.get::<_, i64>(0),
            0,
            "deliberately empty assignment must remain empty"
        );
        // The same name and peer address are now valid on the other switch.
        ctx.client.batch_execute(&format!("
            INSERT INTO omicron.public.router_configuration_switch (router_configuration_id, switch) VALUES ('{RC}', 'switch0');
            INSERT INTO omicron.public.router_configuration_bgp_peer
                (router_configuration_id, switch, name, addr, hold_time, keepalive,
                 connect_retry, delay_open, idle_hold_time, communities, enforce_first_as)
            VALUES ('{RC}', 'switch0', 'peer', '192.0.2.1', 6, 2, 3, 0, 3, ARRAY[42], true);
            DELETE FROM omicron.public.router_configuration_bgp_peer WHERE router_configuration_id='{RC}';
            DELETE FROM omicron.public.router_configuration_static_route WHERE router_configuration_id='{RC}';
            DELETE FROM omicron.public.router_configuration_bfd_peer WHERE router_configuration_id='{RC}';
            DELETE FROM omicron.public.silo_router_configuration WHERE silo_id='{SILO}';
            DELETE FROM omicron.public.router_configuration_switch WHERE router_configuration_id='{RC}';
            DELETE FROM omicron.public.router_configuration WHERE id='{RC}';
            DELETE FROM omicron.public.router_configuration_switch WHERE router_configuration_id='{DEFAULT}';
            DELETE FROM omicron.public.router_configuration WHERE id='{DEFAULT}';
            INSERT INTO omicron.public.control_plane_router_configuration (router_configuration_id, priority) VALUES ('{DEFAULT}', 1000);
        ")).await.unwrap();
    })
}
