// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks that adding rack.time_deleted preserves existing rack records.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(
                "INSERT INTO omicron.public.rack
                (id, time_created, time_modified, initialized, rack_subnet)
             VALUES
                ('a7115830-0000-4000-8000-000000000001',
                 '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z',
                 false, NULL),
                ('a7115830-0000-4000-8000-000000000002',
                 '2026-02-01T00:00:00Z', '2026-02-02T00:00:00Z',
                 true, 'fd00:1158:3000::/56');",
            )
            .await
            .expect("insert pre-migration rack rows");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT id,
                    time_created = CASE
                      WHEN id = 'a7115830-0000-4000-8000-000000000001'
                      THEN '2026-01-01T00:00:00Z'::TIMESTAMPTZ
                      ELSE '2026-02-01T00:00:00Z'::TIMESTAMPTZ
                    END AS created_ok,
                    time_modified = CASE
                      WHEN id = 'a7115830-0000-4000-8000-000000000001'
                      THEN '2026-01-02T00:00:00Z'::TIMESTAMPTZ
                      ELSE '2026-02-02T00:00:00Z'::TIMESTAMPTZ
                    END AS modified_ok,
                    CASE
                      WHEN id = 'a7115830-0000-4000-8000-000000000001'
                      THEN NOT initialized AND rack_subnet IS NULL
                      ELSE initialized AND
                        rack_subnet IS NOT DISTINCT FROM
                          'fd00:1158:3000::/56'::INET
                    END AS old_fields_ok,
                    time_deleted IS NULL AS deleted_is_null
             FROM omicron.public.rack
             WHERE id IN (
                'a7115830-0000-4000-8000-000000000001',
                'a7115830-0000-4000-8000-000000000002')",
                &[],
            )
            .await
            .expect("read migrated rack rows");
        assert_eq!(rows.len(), 2);
        for row in rows {
            for column in [
                "created_ok",
                "modified_ok",
                "old_fields_ok",
                "deleted_is_null",
            ] {
                assert!(row.get::<_, bool>(column), "{column}");
            }
        }

        let column = ctx
            .client
            .query_one(
                "SELECT is_nullable, column_default
             FROM information_schema.columns
             WHERE table_catalog = 'omicron' AND table_schema = 'public'
               AND table_name = 'rack' AND column_name = 'time_deleted'",
                &[],
            )
            .await
            .expect("rack.time_deleted metadata exists");
        assert_eq!(column.get::<_, String>("is_nullable"), "YES");
        assert!(column.get::<_, Option<String>>("column_default").is_none());

        ctx.client
            .batch_execute(
                "DELETE FROM omicron.public.rack WHERE id IN (
                'a7115830-0000-4000-8000-000000000001',
                'a7115830-0000-4000-8000-000000000002');",
            )
            .await
            .expect("clean up rack migration fixtures");
    })
}
