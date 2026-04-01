// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use uuid::Uuid;

// Migration 230 added bgp_config.max_paths as a nullable column.
// Migration 231 backfills NULL rows with 1 and sets NOT NULL.
// We test both: a row that existed before the column was added (pre-230),
// and rows inserted after the column exists but before the NOT NULL fix (pre-231).
const BGP_ANNOUNCE_SET_230: Uuid =
    Uuid::from_u128(0x23000001_0000_0000_0000_000000000001);
// Row inserted before migration 230 (no max_paths column yet).
const BGP_CONFIG_230_PRE: Uuid =
    Uuid::from_u128(0x23000002_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert a bgp_announce_set (required FK) and a bgp_config row
        // BEFORE migration 230 adds the max_paths column.
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.bgp_announce_set
                    (id, name, description, time_created, time_modified)
                VALUES
                    ('{BGP_ANNOUNCE_SET_230}', 'test-announce-set-230',
                     'test', now(), now());

                INSERT INTO omicron.public.bgp_config
                    (id, name, description, time_created, time_modified,
                     asn, bgp_announce_set_id)
                VALUES
                    ('{BGP_CONFIG_230_PRE}', 'bgp-pre-max-paths-230',
                     'inserted before max_paths column exists', now(), now(),
                     64500, '{BGP_ANNOUNCE_SET_230}');
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 230");
    })
}
