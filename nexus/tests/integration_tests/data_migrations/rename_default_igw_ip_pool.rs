// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// A fake IPv4 pool and IGW IP pool row named "default" backed by it.
const POOL_V4: Uuid = Uuid::from_u128(24300001_0000_0000_0000_000000000001);
const IGW_POOL_V4: Uuid = Uuid::from_u128(24300002_0000_0000_0000_000000000001);

// A fake IPv6 pool and IGW IP pool row named "default" backed by it.
const POOL_V6: Uuid = Uuid::from_u128(24300001_0000_0000_0000_000000000002);
const IGW_POOL_V6: Uuid = Uuid::from_u128(24300002_0000_0000_0000_000000000002);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.ip_pool
                    (id, name, description, time_created, time_modified,
                     rcgen, ip_version, reservation_type, pool_type)
                VALUES
                    ('{POOL_V4}', 'test-v4-pool-243', 'IPv4 pool',
                     now(), now(), 0, 'v4', 'external_silos', 'unicast'),
                    ('{POOL_V6}', 'test-v6-pool-243', 'IPv6 pool',
                     now(), now(), 0, 'v6', 'external_silos', 'unicast');

                INSERT INTO omicron.public.internet_gateway_ip_pool
                    (id, name, description, time_created, time_modified,
                     internet_gateway_id, ip_pool_id)
                VALUES
                    ('{IGW_POOL_V4}', 'default', 'default v4 pool',
                     now(), now(), gen_random_uuid(), '{POOL_V4}'),
                    ('{IGW_POOL_V6}', 'default', 'default v6 pool',
                     now(), now(), gen_random_uuid(), '{POOL_V6}');
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 243");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT name FROM omicron.public.internet_gateway_ip_pool
                     WHERE id = '{IGW_POOL_V4}'"
                ),
                &[],
            )
            .await
            .expect("failed to query internet_gateway_ip_pool after 243");
        assert_eq!(rows.len(), 1);
        let name: &str = rows[0].get("name");
        assert_eq!(
            name, "default-v4",
            "IGW IP pool named 'default' backed by a v4 pool \
             should have been renamed to 'default-v4'"
        );

        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT name FROM omicron.public.internet_gateway_ip_pool
                     WHERE id = '{IGW_POOL_V6}'"
                ),
                &[],
            )
            .await
            .expect("failed to query internet_gateway_ip_pool after 243");
        assert_eq!(rows.len(), 1);
        let name: &str = rows[0].get("name");
        assert_eq!(
            name, "default-v6",
            "IGW IP pool named 'default' backed by a v6 pool \
             should have been renamed to 'default-v6'"
        );
    })
}
