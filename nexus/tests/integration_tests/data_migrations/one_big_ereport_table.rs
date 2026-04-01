// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::Row;
use nexus_test_utils::sql::SqlEnum;
use nexus_test_utils::sql::process_rows;
use uuid::Uuid;

const SP_RESTART_ID: Uuid =
    Uuid::from_u128(0x0358a488_3dd1_4da2_a783_570f3a149058);
const HOST_RESTART_ID: Uuid =
    Uuid::from_u128(0x786d5a82_da41_4d28_9901_650bf168e0f3);
const HOST_EREPORT_SLED_ID: Uuid =
    Uuid::from_u128(0xdf221406_85db_414b_bb3f_5b1323592bbe);
const EREPORT_SLED_SERIAL: &str = "BRM6900420";
const GIMLET_PART_NUMBER: &str = "913-0000019";

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.sp_ereport (
                        restart_id,
                        ena,
                        time_deleted,
                        time_collected,
                        collector_id,
                        sp_type,
                        sp_slot,
                        serial_number,
                        part_number,
                        class,
                        report
                    )
                     VALUES (
                         '{SP_RESTART_ID}',
                         1,
                         NULL,
                         now(),
                         gen_random_uuid(),
                         'sled',
                         19,
                         '{EREPORT_SLED_SERIAL}',
                         '{GIMLET_PART_NUMBER}',
                         'my.cool.sp.ereport',
                         '{{}}'
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted sp_ereport");
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.host_ereport (
                        restart_id,
                        ena,
                        time_deleted,
                        time_collected,
                        collector_id,
                        sled_id,
                        sled_serial,
                        class,
                        report,
                        part_number
                    )
                     VALUES (
                         '{HOST_RESTART_ID}',
                         1,
                         NULL,
                         now(),
                         gen_random_uuid(),
                         '{HOST_EREPORT_SLED_ID}',
                         '{EREPORT_SLED_SERIAL}',
                         'my.cool.host.ereport',
                         '{{}}',
                         '{GIMLET_PART_NUMBER}'
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted host_ereport");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let host_rows = ctx
            .client
            .query(
                "SELECT
                   restart_id,
                   ena,
                   reporter,
                   sled_id,
                   sp_type,
                   sp_slot,
                   serial_number,
                   part_number
                 FROM omicron.public.ereport
                 WHERE
                    reporter = 'host';",
                &[],
            )
            .await
            .expect("queried post-migration host ereports");
        let rows = process_rows(&host_rows);
        assert_eq!(rows.len(), 1);
        check_row(
            &rows[0],
            HOST_RESTART_ID,
            1,
            "host",
            Some(HOST_EREPORT_SLED_ID),
            None,
            None,
        );

        let sp_rows = ctx
            .client
            .query(
                "SELECT
                   restart_id,
                   ena,
                   reporter,
                   sled_id,
                   sp_type,
                   sp_slot,
                   serial_number,
                   part_number
                 FROM omicron.public.ereport
                 WHERE
                    reporter = 'sp';",
                &[],
            )
            .await
            .expect("queried post-migration sp ereports");
        let rows = process_rows(&sp_rows);
        assert_eq!(rows.len(), 1);
        check_row(
            &rows[0],
            SP_RESTART_ID,
            1,
            "sp",
            None,
            Some("sled"),
            Some(19),
        );

        fn check_row(
            row: &Row,
            restart_id: Uuid,
            ena: i64,
            reporter: &str,
            sled_id: Option<Uuid>,
            sp_type: Option<&str>,
            sp_slot: Option<i32>,
        ) {
            assert_eq!(
                row.values[0].expect("restart_id").unwrap(),
                &AnySqlType::Uuid(restart_id)
            );
            assert_eq!(
                row.values[1].expect("ena").unwrap(),
                &AnySqlType::Int8(ena)
            );
            assert_eq!(
                row.values[2].expect("reporter").unwrap(),
                &AnySqlType::Enum(SqlEnum::from(("ereporter_type", reporter))),
            );
            assert_eq!(
                row.values[3].expect("sled_id"),
                sled_id.map(AnySqlType::Uuid).as_ref(),
            );
            assert_eq!(
                row.values[4].expect("sp_type"),
                sp_type
                    .map(|t| AnySqlType::Enum(SqlEnum::from(("sp_type", t))))
                    .as_ref(),
            );
            assert_eq!(
                row.values[5].expect("sp_slot"),
                sp_slot.map(AnySqlType::Int4).as_ref(),
            );
            assert_eq!(
                row.values[6].expect("serial_number").unwrap(),
                &AnySqlType::String(EREPORT_SLED_SERIAL.to_string()),
            );
            assert_eq!(
                row.values[7].expect("part_number").unwrap(),
                &AnySqlType::String(GIMLET_PART_NUMBER.to_string()),
            );
        }
    })
}
