// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const LLDP_LINK_CONFIG_ID: Uuid =
    Uuid::from_u128(31600000_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

async fn system_description_type(ctx: &MigrationContext<'_>) -> String {
    ctx.client
        .query_one(
            "SELECT crdb_sql_type FROM information_schema.columns \
             WHERE table_name = 'lldp_link_config' \
             AND column_name = 'system_description'",
            &[],
        )
        .await
        .expect("failed to query system_description type")
        .get(0)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // The base schema comes from dbinit.sql, which already uses
        // STRING(612). Narrow the column to match racks that were upgraded
        // through "collapse_lldp_settings".
        ctx.client
            .batch_execute(
                "SET enable_experimental_alter_column_type_general = true;
                 ALTER TABLE omicron.public.lldp_link_config
                     ALTER COLUMN system_description TYPE STRING(512);
                 SET enable_experimental_alter_column_type_general = false;",
            )
            .await
            .expect("failed to narrow system_description");
        assert_eq!(system_description_type(ctx).await, "STRING(512)");

        ctx.client
            .batch_execute(&format!(
                "INSERT INTO omicron.public.lldp_link_config
                    (id, enabled, system_description, time_created,
                     time_modified)
                 VALUES
                    ('{LLDP_LINK_CONFIG_ID}', true, repeat('a', 512), now(),
                     now());"
            ))
            .await
            .expect("failed to insert pre-migration lldp_link_config row");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        assert_eq!(system_description_type(ctx).await, "STRING(612)");

        let row = ctx
            .client
            .query_one(
                &format!(
                    "SELECT system_description \
                     FROM omicron.public.lldp_link_config \
                     WHERE id = '{LLDP_LINK_CONFIG_ID}'"
                ),
                &[],
            )
            .await
            .expect("failed to query lldp_link_config row");
        assert_eq!(row.get::<_, String>(0), "a".repeat(512));

        ctx.client
            .batch_execute(&format!(
                "UPDATE omicron.public.lldp_link_config \
                 SET system_description = repeat('b', 612) \
                 WHERE id = '{LLDP_LINK_CONFIG_ID}';"
            ))
            .await
            .expect("failed to store a 612-character system_description");

        ctx.client
            .batch_execute(&format!(
                "DELETE FROM omicron.public.lldp_link_config \
                 WHERE id = '{LLDP_LINK_CONFIG_ID}';"
            ))
            .await
            .expect("failed to clean up lldp_link_config row");
    })
}
