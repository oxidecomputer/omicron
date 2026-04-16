// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::SqlEnum;
use nexus_test_utils::sql::process_rows;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .execute(
                "INSERT INTO omicron.public.disk (
                    id,
                    name,
                    description,
                    time_created,
                    time_modified,
                    time_deleted,

                    rcgen,
                    project_id,

                    volume_id,

                    disk_state,
                    attach_instance_id,
                    state_generation,
                    slot,
                    time_state_updated,

                    size_bytes,
                    block_size,
                    origin_snapshot,
                    origin_image,

                    pantry_address
                  )
                  VALUES
                  (
                    'd8b7ba02-4bd5-417d-84d3-67b4e65bec70',
                    'regular',
                    'just a disk',
                    '2025-10-21T20:26:25+0000',
                    '2025-10-21T20:26:25+0000',
                    NULL,

                    0,
                    'd904d22e-cc45-4b2e-b37f-5864d8eba323',

                    '1e17286f-107d-494c-8b2d-904b70d5b706',

                    'attached',
                    '9c1f6ee5-478f-4e6a-b481-be8b69f1daab',
                    75,
                    0,
                    '2025-10-21T20:27:11+0000',

                    1073741824,
                    '512',
                    'b017fe60-9a4a-4fe6-a7d3-4a7c3ab23a98',
                    NULL,

                    '[fd00:1122:3344:101::7]:4567'
                  ),
                  (
                    '336ed8ca-9bbf-4db9-9f2d-627f8d156f91',
                    'deleted',
                    'should migrate deleted disks too, even if they are old',
                    '2024-10-21T20:26:25+0000',
                    '2024-10-21T20:26:25+0000',
                    '2025-10-15T21:38:05+0000',

                    2,
                    'c9cfb288-a39e-4ebb-ac27-b26c9248a46a',

                    '0a1d0a38-f927-4260-ad23-7f9c04277a23',

                    'detached',
                    NULL,
                    75786,
                    1,
                    '2025-10-14T20:27:11+0000',

                    1073741824,
                    '4096',
                    NULL,
                    '971395a7-4206-4dc4-ba58-e2402724270a',

                    NULL
                  );",
                &[],
            )
            .await
            .expect("inserted disks");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // first disk

        let rows = ctx
            .client
            .query(
                "SELECT
                   id,
                   name,
                   description,
                   time_created,
                   time_modified,
                   time_deleted,

                   rcgen,
                   project_id,

                   disk_state,
                   attach_instance_id,
                   state_generation,
                   slot,
                   time_state_updated,

                   size_bytes,
                   block_size
                 FROM
                   omicron.public.disk
                 WHERE
                   id = 'd8b7ba02-4bd5-417d-84d3-67b4e65bec70'
                 ;",
                &[],
            )
            .await
            .expect("queried disk");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[5].expect("time_deleted").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("id").unwrap(),
                row.values[1].expect("name").unwrap(),
                row.values[2].expect("description").unwrap(),
                row.values[3].expect("time_created").unwrap(),
                row.values[4].expect("time_modified").unwrap(),
                row.values[6].expect("rcgen").unwrap(),
                row.values[7].expect("project_id").unwrap(),
                row.values[8].expect("disk_state").unwrap(),
                row.values[9].expect("attach_instance_id").unwrap(),
                row.values[10].expect("state_generation").unwrap(),
                row.values[11].expect("slot").unwrap(),
                row.values[12].expect("time_state_updated").unwrap(),
                row.values[13].expect("size_bytes").unwrap(),
                row.values[14].expect("block_size").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "d8b7ba02-4bd5-417d-84d3-67b4e65bec70".parse().unwrap()
                ),
                &AnySqlType::String("regular".to_string()),
                &AnySqlType::String("just a disk".to_string()),
                &AnySqlType::DateTime,
                &AnySqlType::DateTime,
                &AnySqlType::Int8(0),
                &AnySqlType::Uuid(
                    "d904d22e-cc45-4b2e-b37f-5864d8eba323".parse().unwrap()
                ),
                &AnySqlType::String("attached".to_string()),
                &AnySqlType::Uuid(
                    "9c1f6ee5-478f-4e6a-b481-be8b69f1daab".parse().unwrap()
                ),
                &AnySqlType::Int8(75),
                &AnySqlType::Int2(0),
                &AnySqlType::DateTime,
                &AnySqlType::Int8(1073741824),
                &AnySqlType::Enum(SqlEnum::from(("block_size", "512"))),
            ],
        );

        let rows = ctx
            .client
            .query(
                "SELECT
                   disk_id,
                   volume_id,
                   origin_snapshot,
                   origin_image,
                   pantry_address
                 FROM
                   omicron.public.disk_type_crucible
                 WHERE
                   disk_id = 'd8b7ba02-4bd5-417d-84d3-67b4e65bec70'
                 ;",
                &[],
            )
            .await
            .expect("queried disk_type_crucible");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[3].expect("origin_image").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("disk_id").unwrap(),
                row.values[1].expect("volume_id").unwrap(),
                row.values[2].expect("origin_snapshot").unwrap(),
                row.values[4].expect("pantry_address").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "d8b7ba02-4bd5-417d-84d3-67b4e65bec70".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "1e17286f-107d-494c-8b2d-904b70d5b706".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "b017fe60-9a4a-4fe6-a7d3-4a7c3ab23a98".parse().unwrap()
                ),
                &AnySqlType::String("[fd00:1122:3344:101::7]:4567".to_string()),
            ]
        );

        // second disk

        let rows = ctx
            .client
            .query(
                "SELECT
                   id,
                   name,
                   description,
                   time_created,
                   time_modified,
                   time_deleted,

                   rcgen,
                   project_id,

                   disk_state,
                   attach_instance_id,
                   state_generation,
                   slot,
                   time_state_updated,

                   size_bytes,
                   block_size
                 FROM
                   omicron.public.disk
                 WHERE
                   id = '336ed8ca-9bbf-4db9-9f2d-627f8d156f91'
                 ;",
                &[],
            )
            .await
            .expect("queried disk");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[9].expect("attach_instance_id").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("id").unwrap(),
                row.values[1].expect("name").unwrap(),
                row.values[2].expect("description").unwrap(),
                row.values[3].expect("time_created").unwrap(),
                row.values[4].expect("time_modified").unwrap(),
                row.values[5].expect("time_deleted").unwrap(),
                row.values[6].expect("rcgen").unwrap(),
                row.values[7].expect("project_id").unwrap(),
                row.values[8].expect("disk_state").unwrap(),
                row.values[10].expect("state_generation").unwrap(),
                row.values[11].expect("slot").unwrap(),
                row.values[12].expect("time_state_updated").unwrap(),
                row.values[13].expect("size_bytes").unwrap(),
                row.values[14].expect("block_size").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "336ed8ca-9bbf-4db9-9f2d-627f8d156f91".parse().unwrap()
                ),
                &AnySqlType::String("deleted".to_string()),
                &AnySqlType::String(
                    "should migrate deleted disks too, even if they are old"
                        .to_string()
                ),
                &AnySqlType::DateTime,
                &AnySqlType::DateTime,
                &AnySqlType::DateTime,
                &AnySqlType::Int8(2),
                &AnySqlType::Uuid(
                    "c9cfb288-a39e-4ebb-ac27-b26c9248a46a".parse().unwrap()
                ),
                &AnySqlType::String("detached".to_string()),
                &AnySqlType::Int8(75786),
                &AnySqlType::Int2(1),
                &AnySqlType::DateTime,
                &AnySqlType::Int8(1073741824),
                &AnySqlType::Enum(SqlEnum::from(("block_size", "4096"))),
            ],
        );

        let rows = ctx
            .client
            .query(
                "SELECT
                   disk_id,
                   volume_id,
                   origin_snapshot,
                   origin_image,
                   pantry_address
                 FROM
                   omicron.public.disk_type_crucible
                 WHERE
                   disk_id = '336ed8ca-9bbf-4db9-9f2d-627f8d156f91'
                 ;",
                &[],
            )
            .await
            .expect("queried disk_type_crucible");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[2].expect("origin_snapshot").is_none());
        assert!(row.values[4].expect("pantry_address").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("disk_id").unwrap(),
                row.values[1].expect("volume_id").unwrap(),
                row.values[3].expect("origin_image").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "336ed8ca-9bbf-4db9-9f2d-627f8d156f91".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "0a1d0a38-f927-4260-ad23-7f9c04277a23".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "971395a7-4206-4dc4-ba58-e2402724270a".parse().unwrap()
                ),
            ]
        );
    })
}
