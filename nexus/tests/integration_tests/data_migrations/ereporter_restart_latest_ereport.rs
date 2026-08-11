// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the migration that backfills the `time_latest_ereport_received`
//! column in `ereporter_restart` from

use super::super::schema::{DataMigrationFns, MigrationContext};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const RACK_ID: Uuid = Uuid::from_u128(0xeb050000_0000_0000_0000_000000000001);

// An SP reporter restart with multiple ereports, where the *newest* ereport is
// soft-deleted. We expect the `time_latest_ereport_received` field to be
// reconstructed from the newest ereport's `time_collected`, even though that
// ereport is soft-deleted.
const SP_RESTART: Uuid =
    Uuid::from_u128(0xeb050001_0000_0000_0000_000000000001);

// A host OS reporter restart with no ereports at all (assuming they have been
// hard-deleted).
//
// Here, we expect that the `time_latest_ereport_received` field will be set to
// `time_first_seen`, which is the best possible guess we can make in the
// absence of any actual ereport records.
const HOST_RESTART: Uuid =
    Uuid::from_u128(0xeb050001_0000_0000_0000_000000000002);

const COLLECTOR_ID: Uuid =
    Uuid::from_u128(0xeb050003_0000_0000_0000_000000000001);

// Some hard-coded timestamps that will parse as both `TIMESTAMPTZ` literals
// and via `str::parse::<DateTime<Utc>>`.
const T_10: &str = "2025-06-08T00:10:00Z";
const T_20: &str = "2025-06-08T00:20:00Z";
const T_30: &str = "2025-06-08T00:30:00Z";
const T_40: &str = "2025-06-08T00:40:00Z";

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
                INSERT INTO omicron.public.ereporter_restart (
                    id, time_first_seen, reporter, slot_type, slot, rack_id
                ) VALUES
                    ('{SP_RESTART}', '{T_10}', 'sp', 'switch', 1, '{RACK_ID}'),
                    ('{HOST_RESTART}', '{T_20}', 'host', 'sled', NULL,
                     '{RACK_ID}');

                INSERT INTO omicron.public.ereport (
                    restart_id, ena, time_deleted, time_collected,
                    collector_id, report, reporter, sled_id, slot_type, slot
                ) VALUES
                    ('{SP_RESTART}', 1, NULL, '{T_10}', '{COLLECTOR_ID}',
                     '{{}}', 'sp', NULL, 'switch', 1),
                    ('{SP_RESTART}', 2, '{T_40}', '{T_30}', '{COLLECTOR_ID}',
                     '{{}}', 'sp', NULL, 'switch', 1);
                "
            ))
            .await
            .expect("failed to insert pre-migration ereporter restarts");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    const COLUMN: &str = "time_latest_ereport_received";
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, {COLUMN}
                     FROM omicron.public.ereporter_restart
                     WHERE id IN ('{SP_RESTART}', '{HOST_RESTART}')"
                ),
                &[],
            )
            .await
            .expect("failed to query backfilled ereporter_restart rows");

        assert_eq!(
            rows.len(),
            2,
            "both pre-existing ereporter restarts should still be present"
        );

        for row in &rows {
            let id: Uuid = row.get("id");
            let time_latest: DateTime<Utc> =
                row.get("time_latest_ereport_received");
            if id == SP_RESTART {
                assert_eq!(
                    time_latest,
                    ts(T_30),
                    "SP restart's `{COLUMN}` mark should be the \
                    `time_collected` of the newest ereport, even though that \
                    ereport is soft-deleted"
                );
            } else {
                assert_eq!(
                    time_latest,
                    ts(T_20),
                    "a restart with no remaining ereports' `{COLUMN}` should \
                    fall back to `time_first_seen`"
                );
            }
        }

        // Clean up test data so it doesn't interfere with later checks.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.ereport
                    WHERE restart_id = '{SP_RESTART}';
                DELETE FROM omicron.public.ereporter_restart
                    WHERE id IN ('{SP_RESTART}', '{HOST_RESTART}');
                "
            ))
            .await
            .expect(
                "failed to clean up ereporter-restart-latest-ereport test data",
            );
    })
}
