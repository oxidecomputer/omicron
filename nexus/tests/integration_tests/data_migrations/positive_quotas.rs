// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::Row;
use nexus_test_utils::sql::process_rows;
use uuid::Uuid;

const NEGATIVE_QUOTA: Uuid =
    Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f351dda7ce1);
const POSITIVE_QUOTA: Uuid =
    Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f351dda7ce2);
const MIXED_QUOTA: Uuid =
    Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f351dda7ce3);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.silo_quotas (
                        silo_id,
                        time_created,
                        time_modified,
                        cpus,
                        memory_bytes,
                        storage_bytes
                    )
                     VALUES
                     ('{NEGATIVE_QUOTA}', now(), now(), -1, -2, -3),
                     ('{POSITIVE_QUOTA}', now(), now(), 1, 2, 3),
                     ('{MIXED_QUOTA}', now(), now(), -1, 0, 3);",
                ),
                &[],
            )
            .await
            .expect("inserted silo_quotas");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT
                   silo_id,
                   cpus,
                   memory_bytes,
                   storage_bytes
                 FROM omicron.public.silo_quotas ORDER BY silo_id ASC;",
                &[],
            )
            .await
            .expect("queried post-migration positive-quotas");

        fn check_quota(
            row: &Row,
            id: Uuid,
            cpus: i64,
            memory: i64,
            storage: i64,
        ) {
            assert_eq!(
                row.values[0].expect("silo_id").unwrap(),
                &AnySqlType::Uuid(id)
            );
            assert_eq!(
                row.values[1].expect("cpus").unwrap(),
                &AnySqlType::Int8(cpus)
            );
            assert_eq!(
                row.values[2].expect("memory_bytes").unwrap(),
                &AnySqlType::Int8(memory)
            );
            assert_eq!(
                row.values[3].expect("storage_bytes").unwrap(),
                &AnySqlType::Int8(storage)
            );
        }

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 3);

        check_quota(&rows[0], NEGATIVE_QUOTA, 0, 0, 0);
        check_quota(&rows[1], POSITIVE_QUOTA, 1, 2, 3);
        check_quota(&rows[2], MIXED_QUOTA, 0, 0, 3);
    })
}
