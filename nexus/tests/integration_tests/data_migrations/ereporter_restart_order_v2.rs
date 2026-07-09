// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use uuid::Uuid;

const COLLECTOR: Uuid = Uuid::from_u128(0xeb020000_0000_0000_0000_000000000001);

// A sled SP restart ID with a single ereport.
const SP_SLED: Uuid = Uuid::from_u128(0xeb020001_0000_0000_0000_000000000001);
// A switch SP reporter with multiple ereports at different times.
const SP_SWITCH: Uuid = Uuid::from_u128(0xeb020001_0000_0000_0000_000000000002);
// A host OS reporter whose slot is NULL in its earliest ereports and then
// resolved to a non-NULL value. The non-NULL slot should be recovered, and
// `time_first_seen` should still come from the earliest (NULL-slot) ereport.
const HOST_NULL_THEN_SLOT: Uuid =
    Uuid::from_u128(0xeb020001_0000_0000_0000_000000000003);
// A host OS reporter whose slot number is NULL in *every* ereport. A row for
// this restart ID should still created, with a NULL slot.
const HOST_SLOT_ALL_NULL: Uuid =
    Uuid::from_u128(0xeb020001_0000_0000_0000_000000000004);
// A host OS reporter whose slot is known in its earliest ereport and then NULL
// in a later one. The known slot must not be clobbered back to NULL.
const HOST_SLOT_THEN_NULL: Uuid =
    Uuid::from_u128(0xeb020001_0000_0000_0000_000000000005);
// A host OS reporter whose only ereport is soft-deleted.
const HOST_ONLY_DELETED: Uuid =
    Uuid::from_u128(0xeb020001_0000_0000_0000_000000000006);
// A host OS reporter with a mix of soft-deleted and live ereports, where the
// *earliest* ereport is soft-deleted. The restart ID is included, and
// `time_first_seen` must come from that earliest (soft-deleted) ereport.
const HOST_SOME_DELETED: Uuid =
    Uuid::from_u128(0xeb020001_0000_0000_0000_000000000007);

// Sled IDs for the host OS reporters (host ereports require a non-NULL
// `sled_id`, which is otherwise irrelevant to the test).
const SLED_A: Uuid = Uuid::from_u128(0xeb020002_0000_0000_0000_000000000001);
const SLED_B: Uuid = Uuid::from_u128(0xeb020002_0000_0000_0000_000000000002);
const SLED_C: Uuid = Uuid::from_u128(0xeb020002_0000_0000_0000_000000000003);
const SLED_D: Uuid = Uuid::from_u128(0xeb020002_0000_0000_0000_000000000004);
const SLED_E: Uuid = Uuid::from_u128(0xeb020002_0000_0000_0000_000000000005);

// Some hard-coded timestamps that will parse as both `TIMESTAMPTZ` literals and
// via `str::parse::<DateTime<Utc>>`.
const T_05: &str = "2024-01-01T00:00:05Z";
const T_10: &str = "2024-01-01T00:00:10Z";
const T_15: &str = "2024-01-01T00:00:15Z";
const T_20: &str = "2024-01-01T00:00:20Z";
const T_25: &str = "2024-01-01T00:00:25Z";
const T_30: &str = "2024-01-01T00:00:30Z";
const T_40: &str = "2024-01-01T00:00:40Z";
const T_50: &str = "2024-01-01T00:00:50Z";
const T_60: &str = "2024-01-01T00:01:00Z";
const T_70: &str = "2024-01-01T00:01:10Z";
const T_120: &str = "2024-01-01T00:02:00Z";
const T_180: &str = "2024-01-01T00:03:00Z";
const T_240: &str = "2024-01-01T00:04:00Z";
const T_270: &str = "2024-01-01T00:04:30Z";
const T_300: &str = "2024-01-01T00:05:00Z";

fn ts(s: &str) -> DateTime<Utc> {
    s.parse().expect("test timestamp should be valid RFC 3339")
}

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.ereport (
                    restart_id, ena, time_collected, collector_id, report,
                    reporter, slot_type, slot, sled_id, time_deleted
                ) VALUES
                    ('{SP_SLED}', 1, '{T_10}', '{COLLECTOR}', '{{}}',
                     'sp', 'sled', 5, NULL, NULL),

                    ('{SP_SWITCH}', 1, '{T_30}', '{COLLECTOR}', '{{}}',
                     'sp', 'switch', 1, NULL, NULL),
                    ('{SP_SWITCH}', 2, '{T_10}', '{COLLECTOR}', '{{}}',
                     'sp', 'switch', 1, NULL, NULL),
                    ('{SP_SWITCH}', 3, '{T_20}', '{COLLECTOR}', '{{}}',
                     'sp', 'switch', 1, NULL, NULL),

                    ('{HOST_NULL_THEN_SLOT}', 1, '{T_05}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', NULL, '{SLED_A}', NULL),
                    ('{HOST_NULL_THEN_SLOT}', 2, '{T_15}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', NULL, '{SLED_A}', NULL),
                    ('{HOST_NULL_THEN_SLOT}', 3, '{T_25}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', 7, '{SLED_A}', NULL),

                    ('{HOST_SLOT_ALL_NULL}', 1, '{T_40}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', NULL, '{SLED_B}', NULL),
                    ('{HOST_SLOT_ALL_NULL}', 2, '{T_50}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', NULL, '{SLED_B}', NULL),

                    ('{HOST_SLOT_THEN_NULL}', 1, '{T_60}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', 3, '{SLED_C}', NULL),
                    ('{HOST_SLOT_THEN_NULL}', 2, '{T_70}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', NULL, '{SLED_C}', NULL),

                    ('{HOST_ONLY_DELETED}', 1, '{T_120}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', 4, '{SLED_D}', '{T_180}'),

                    ('{HOST_SOME_DELETED}', 1, '{T_240}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', NULL, '{SLED_E}', '{T_300}'),
                    ('{HOST_SOME_DELETED}', 2, '{T_270}', '{COLLECTOR}', '{{}}',
                     'host', 'sled', 8, '{SLED_E}', NULL);
                "
            ))
            .await
            .expect("failed to insert test ereports");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, reporter::text, slot_type::text, slot,
                            time_first_seen
                     FROM omicron.public.ereporter_restart
                     WHERE id IN (
                         '{SP_SLED}', '{SP_SWITCH}', '{HOST_NULL_THEN_SLOT}',
                         '{HOST_SLOT_ALL_NULL}', '{HOST_SLOT_THEN_NULL}',
                         '{HOST_ONLY_DELETED}', '{HOST_SOME_DELETED}'
                     )"
                ),
                &[],
            )
            .await
            .expect("failed to query backfilled ereporter_restart rows");

        assert_eq!(rows.len(), 7, "expected one row per restart ID");

        let results: HashMap<
            Uuid,
            (String, String, Option<i32>, DateTime<Utc>),
        > = rows
            .iter()
            .map(|row| {
                let id: Uuid = row.get("id");
                let reporter: String = row.get("reporter");
                let slot_type: String = row.get("slot_type");
                let slot: Option<i32> = row.get("slot");
                let time_first_seen: DateTime<Utc> = row.get("time_first_seen");
                (id, (reporter, slot_type, slot, time_first_seen))
            })
            .collect();

        // SP sled: reports its slot and (only) collection time.
        let (reporter, slot_type, slot, first_seen) = &results[&SP_SLED];
        assert_eq!(reporter, "sp");
        assert_eq!(slot_type, "sled");
        assert_eq!(*slot, Some(5));
        assert_eq!(*first_seen, ts(T_10));

        // SP switch: time_first_seen is the earliest of the three ereports.
        let (reporter, slot_type, slot, first_seen) = &results[&SP_SWITCH];
        assert_eq!(reporter, "sp");
        assert_eq!(slot_type, "switch",);
        assert_eq!(*slot, Some(1));
        assert_eq!(
            *first_seen,
            ts(T_10),
            "time_first_seen should be the earliest time_collected of \
            multiple ereports"
        );

        // Host, slot number NULL then resolved. The slot should be set to the
        // value from the non-NULL value, while time_first_seen comes from the
        // earliest (NULL-slot) ereport.
        let (reporter, slot_type, slot, first_seen) =
            &results[&HOST_NULL_THEN_SLOT];
        assert_eq!(reporter, "host");
        assert_eq!(slot_type, "sled");
        assert_eq!(
            *slot,
            Some(7),
            "a host's NULL slot should be backfilled from a later non-NULL one"
        );
        assert_eq!(
            *first_seen,
            ts(T_05),
            "time_first_seen should span the NULL-slot ereports"
        );

        // Host, all NULL: a row is created with a NULL slot.
        let (reporter, slot_type, slot, first_seen) =
            &results[&HOST_SLOT_ALL_NULL];
        assert_eq!(reporter, "host");
        assert_eq!(slot_type, "sled");
        assert_eq!(
            *slot, None,
            "a host reporter with no known slot should have a NULL slot"
        );
        assert_eq!(*first_seen, ts(T_40), "all-NULL host time_first_seen");

        // Host, slot known then NULL: the known slot is not clobbered. This is
        // an edge case that shouldn't occur in production but which is
        // nonetheless worth checking.
        let (reporter, slot_type, slot, first_seen) =
            &results[&HOST_SLOT_THEN_NULL];
        assert_eq!(reporter, "host");
        assert_eq!(slot_type, "sled");
        assert_eq!(
            *slot,
            Some(3),
            "a known slot must not be clobbered by a later NULL one"
        );
        assert_eq!(
            *first_seen,
            ts(T_60),
            "known-then-NULL host time_first_seen"
        );

        // All ereports soft-deleted: there should still be an entry for the
        // reporter.
        let (reporter, slot_type, slot, first_seen) =
            &results[&HOST_ONLY_DELETED];
        assert_eq!(reporter, "host",);
        assert_eq!(slot_type, "sled");
        assert_eq!(*slot, Some(4));
        assert_eq!(
            *first_seen,
            ts(T_120),
            "time_first_seen should be from the soft-deleted ereport"
        );

        // Mixed deleted/live: included, with the slot recovered from the live
        // ereport (8) and time_first_seen from the earliest ereport, even
        // though that ereport is soft-deleted.
        let (reporter, slot_type, slot, first_seen) =
            &results[&HOST_SOME_DELETED];
        assert_eq!(reporter, "host",);
        assert_eq!(slot_type, "sled");
        assert_eq!(*slot, Some(8));
        assert_eq!(
            *first_seen,
            ts(T_240),
            "time_first_seen should span the soft-deleted earliest ereport"
        );

        // Clean up test data.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.ereporter_restart
                    WHERE id IN (
                        '{SP_SLED}', '{SP_SWITCH}', '{HOST_NULL_THEN_SLOT}',
                        '{HOST_SLOT_ALL_NULL}', '{HOST_SLOT_THEN_NULL}',
                        '{HOST_ONLY_DELETED}', '{HOST_SOME_DELETED}'
                    );
                DELETE FROM omicron.public.ereport
                    WHERE restart_id IN (
                        '{SP_SLED}', '{SP_SWITCH}', '{HOST_NULL_THEN_SLOT}',
                        '{HOST_SLOT_ALL_NULL}', '{HOST_SLOT_THEN_NULL}',
                        '{HOST_ONLY_DELETED}', '{HOST_SOME_DELETED}'
                    );
                "
            ))
            .await
            .expect("failed to clean up ereporter-restart-order-v2 test data");
    })
}
