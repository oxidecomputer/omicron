// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// Ereport restart IDs — one per test case.
const CLEAN_SERIAL: Uuid =
    Uuid::from_u128(25800001_0000_0000_0000_000000000001);
const NULL_PADDED_SERIAL: Uuid =
    Uuid::from_u128(25800001_0000_0000_0000_000000000002);
const NULL_SERIAL: Uuid = Uuid::from_u128(25800001_0000_0000_0000_000000000003);

const COLLECTOR: Uuid = Uuid::from_u128(25800002_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert ereport rows with various serial_number values:
        //  1. A clean serial number with no trailing nulls.
        //  2. A serial number with trailing null bytes.
        //  3. A NULL serial_number (should remain NULL).
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.ereport (
                    restart_id, ena, time_collected, collector_id,
                    serial_number, report, reporter, slot_type, slot
                ) VALUES
                    ('{CLEAN_SERIAL}', 1, now(), '{COLLECTOR}',
                     'BRM42220031',
                     '{{}}', 'sp', 'sled', 0),
                    ('{NULL_PADDED_SERIAL}', 1, now(), '{COLLECTOR}',
                     E'2EKRJYKV\\x00\\x00\\x00',
                     '{{}}', 'sp', 'sled', 1),
                    ('{NULL_SERIAL}', 1, now(), '{COLLECTOR}',
                     NULL,
                     '{{}}', 'sp', 'sled', 2);
                "
            ))
            .await
            .expect(
                "failed to insert test data for ereport-trim-serial-trailing-nulls"
            );

        // Verify that the null-padded serial number actually has the expected
        // nulls in it.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT restart_id, serial_number
                     FROM omicron.public.ereport
                     WHERE restart_id IN (
                         '{CLEAN_SERIAL}',
                         '{NULL_PADDED_SERIAL}',
                         '{NULL_SERIAL}'
                     )
                     ORDER BY restart_id"
                ),
                &[],
            )
            .await
            .expect("failed to query ereport rows after migration");

        assert_eq!(rows.len(), 3, "expected 3 test ereport rows");

        for row in &rows {
            let id: Uuid = row.get("restart_id");
            let serial: Option<String> = row.get("serial_number");

            match id {
                id if id == CLEAN_SERIAL => {
                    assert_eq!(serial.as_deref(), Some("BRM42220031"),);
                }
                id if id == NULL_PADDED_SERIAL => {
                    assert_eq!(
                        serial.as_deref(),
                        Some("2EKRJYKV\0\0\0"),
                        "2EKRJYKV should be null-padded",
                    );
                }
                id if id == NULL_SERIAL => {
                    assert_eq!(serial, None, "NULL serial_number be None");
                }
                other => panic!("unexpected restart_id: {other}"),
            }
        }
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Verify the serial numbers after the migration.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT restart_id, serial_number
                     FROM omicron.public.ereport
                     WHERE restart_id IN (
                         '{CLEAN_SERIAL}',
                         '{NULL_PADDED_SERIAL}',
                         '{NULL_SERIAL}'
                     )
                     ORDER BY restart_id"
                ),
                &[],
            )
            .await
            .expect("failed to query ereport rows after migration");

        assert_eq!(rows.len(), 3, "expected 3 test ereport rows");

        for row in &rows {
            let id: Uuid = row.get("restart_id");
            let serial: Option<String> = row.get("serial_number");

            match id {
                id if id == CLEAN_SERIAL => {
                    assert_eq!(
                        serial.as_deref(),
                        Some("BRM42220031"),
                        "clean serial number should be unchanged"
                    );
                }
                id if id == NULL_PADDED_SERIAL => {
                    assert_eq!(
                        serial.as_deref(),
                        Some("2EKRJYKV"),
                        "serial with trailing NUL bytes should be trimmed"
                    );
                }
                id if id == NULL_SERIAL => {
                    assert_eq!(
                        serial, None,
                        "NULL serial_number should remain NULL"
                    );
                }
                other => panic!("unexpected restart_id: {other}"),
            }
        }

        // Clean up test data.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.ereport
                    WHERE restart_id IN (
                        '{CLEAN_SERIAL}',
                        '{NULL_PADDED_SERIAL}',
                        '{NULL_SERIAL}'
                    );
                "
            ))
            .await
            .unwrap();
    })
}
