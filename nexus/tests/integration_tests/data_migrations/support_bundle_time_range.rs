// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the support-bundle-time-range migration, which moves time
//! bounds from the per-category ereports tables into bundle-wide time range
//! tables, and backfills a start bound so that `start_time` can be made
//! NOT NULL for support bundles.

use super::super::schema::{DataMigrationFns, MigrationContext};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// Collecting, with no ereports row: up8 inserts a range whose start is the
// 7-day lookback from the bundle's creation.
const COLLECTING_NO_ROW: Uuid =
    Uuid::from_u128(0xd6e64ddf_04d3_4edb_84a3_cb8393632184);
// Collecting, with an unbounded ereports row: nothing is promoted by up3, so
// up8 inserts a range just as for a bundle with no ereports row.
const COLLECTING_UNBOUNDED: Uuid =
    Uuid::from_u128(0x58704b75_d52a_4236_b71c_a4bce51ee8d4);
// Collecting, with an end-only ereports filter: up3 promotes it, and up8
// fills the start with the 7-day lookback from the end bound.
const COLLECTING_END_ONLY: Uuid =
    Uuid::from_u128(0x158c21be_c204_42b8_adba_526d1cbfb8b4);
// Collecting, with both bounds on its ereports filter: promoted as-is.
const COLLECTING_BOTH: Uuid =
    Uuid::from_u128(0x69b176ff_f3f0_498d_bfc4_80e781ffa4d5);
// Terminal, with an end-only ereports filter: up3 promotes it, and up8
// fills the start with the "no lower bound" sentinel.
const ACTIVE_END_ONLY: Uuid =
    Uuid::from_u128(0x4e4921e3_4f6b_4566_bbda_624c903d778a);
// Terminal, with no ereports row: left without a range.
const ACTIVE_NO_ROW: Uuid =
    Uuid::from_u128(0xe653a3b3_e87d_48de_a8b2_f1c10492b620);

const BUNDLES: [(Uuid, &str); 6] = [
    (COLLECTING_NO_ROW, "collecting"),
    (COLLECTING_UNBOUNDED, "collecting"),
    (COLLECTING_END_ONLY, "collecting"),
    (COLLECTING_BOTH, "collecting"),
    (ACTIVE_END_ONLY, "active"),
    (ACTIVE_NO_ROW, "active"),
];

// FM support bundle requests: time bounds are promoted, but no start is
// backfilled (FM requests are stamped when they become bundles).
const SITREP_ID: Uuid = Uuid::from_u128(0x441afb4a_f63a_46d5_9ecb_0d053432d957);
const FM_REQUEST_END_ONLY: Uuid =
    Uuid::from_u128(0x4a56101c_5568_46ee_9294_8746844326cb);
const FM_REQUEST_BOTH: Uuid =
    Uuid::from_u128(0xabd399e0_80ef_468b_853b_c335585e786d);
const FM_REQUEST_UNBOUNDED: Uuid =
    Uuid::from_u128(0xf20948d1_9d25_47cc_b273_7816704837ea);

// Some hard-coded timestamps that will parse as both `TIMESTAMPTZ` literals
// and via `str::parse::<DateTime<Utc>>`.
const T_CREATED: &str = "2026-06-15T00:00:00Z";
const T_CREATED_MINUS_7D: &str = "2026-06-08T00:00:00Z";
const T_START: &str = "2026-06-01T00:00:00Z";
const T_END: &str = "2026-06-10T00:00:00Z";
const T_END_MINUS_7D: &str = "2026-06-03T00:00:00Z";
const T_SENTINEL: &str = "2000-01-01T00:00:00Z";

fn ts(s: &str) -> DateTime<Utc> {
    s.parse().expect("test timestamp should be valid RFC 3339")
}

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Each bundle needs its own dataset (one_bundle_per_dataset).
        let bundles = BUNDLES
            .iter()
            .map(|(id, state)| {
                format!(
                    "('{id}', '{T_CREATED}', 'test', '{state}', \
                      gen_random_uuid(), gen_random_uuid())"
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.support_bundle (
                    id, time_created, reason_for_creation, state,
                    zpool_id, dataset_id
                ) VALUES {bundles};

                INSERT INTO omicron.public.support_bundle_data_selection_ereports (
                    bundle_id, start_time, end_time, only_serials
                ) VALUES
                    ('{COLLECTING_UNBOUNDED}', NULL, NULL, ARRAY['serial']),
                    ('{COLLECTING_END_ONLY}', NULL, '{T_END}', ARRAY[]::TEXT[]),
                    ('{COLLECTING_BOTH}', '{T_START}', '{T_END}', ARRAY[]::TEXT[]),
                    ('{ACTIVE_END_ONLY}', NULL, '{T_END}', ARRAY[]::TEXT[]);

                INSERT INTO omicron.public.fm_support_bundle_request_data_selection_ereports (
                    sitrep_id, request_id, start_time, end_time
                ) VALUES
                    ('{SITREP_ID}', '{FM_REQUEST_END_ONLY}', NULL, '{T_END}'),
                    ('{SITREP_ID}', '{FM_REQUEST_BOTH}', '{T_START}', '{T_END}'),
                    ('{SITREP_ID}', '{FM_REQUEST_UNBOUNDED}', NULL, NULL);
                "
            ))
            .await
            .expect("inserted pre-migration support bundle rows");
    })
}

type Range = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let ids = BUNDLES
            .iter()
            .map(|(id, _)| format!("'{id}'"))
            .collect::<Vec<_>>()
            .join(", ");

        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT bundle_id, start_time, end_time \
                     FROM omicron.public.support_bundle_data_selection_time_range \
                     WHERE bundle_id IN ({ids}) \
                     ORDER BY bundle_id"
                ),
                &[],
            )
            .await
            .expect("queried support bundle time ranges");
        let observed: Vec<(Uuid, Range)> = rows
            .iter()
            .map(|r| {
                (r.get("bundle_id"), (r.get("start_time"), r.get("end_time")))
            })
            .collect();
        let mut expected: Vec<(Uuid, Range)> = vec![
            (COLLECTING_NO_ROW, (Some(ts(T_CREATED_MINUS_7D)), None)),
            (COLLECTING_UNBOUNDED, (Some(ts(T_CREATED_MINUS_7D)), None)),
            (COLLECTING_END_ONLY, (Some(ts(T_END_MINUS_7D)), Some(ts(T_END)))),
            (COLLECTING_BOTH, (Some(ts(T_START)), Some(ts(T_END)))),
            (ACTIVE_END_ONLY, (Some(ts(T_SENTINEL)), Some(ts(T_END)))),
            // ACTIVE_NO_ROW: no range row.
        ];
        expected.sort_by_key(|(id, _)| *id);
        assert_eq!(observed, expected);

        // The ereports rows survive with their remaining columns intact.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT bundle_id, only_serials \
                     FROM omicron.public.support_bundle_data_selection_ereports \
                     WHERE bundle_id IN ({ids}) \
                     ORDER BY bundle_id"
                ),
                &[],
            )
            .await
            .expect("queried support bundle ereports filters");
        let observed: Vec<(Uuid, Vec<String>)> = rows
            .iter()
            .map(|r| (r.get("bundle_id"), r.get("only_serials")))
            .collect();
        let mut expected: Vec<(Uuid, Vec<String>)> = vec![
            (COLLECTING_UNBOUNDED, vec!["serial".to_string()]),
            (COLLECTING_END_ONLY, vec![]),
            (COLLECTING_BOTH, vec![]),
            (ACTIVE_END_ONLY, vec![]),
        ];
        expected.sort_by_key(|(id, _)| *id);
        assert_eq!(observed, expected);

        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT request_id, start_time, end_time \
                     FROM omicron.public.fm_support_bundle_request_data_selection_time_range \
                     WHERE sitrep_id = '{SITREP_ID}' \
                     ORDER BY request_id"
                ),
                &[],
            )
            .await
            .expect("queried FM support bundle request time ranges");
        let observed: Vec<(Uuid, Range)> = rows
            .iter()
            .map(|r| {
                (r.get("request_id"), (r.get("start_time"), r.get("end_time")))
            })
            .collect();
        let mut expected: Vec<(Uuid, Range)> = vec![
            (FM_REQUEST_END_ONLY, (None, Some(ts(T_END)))),
            (FM_REQUEST_BOTH, (Some(ts(T_START)), Some(ts(T_END)))),
            // FM_REQUEST_UNBOUNDED: no range row.
        ];
        expected.sort_by_key(|(id, _)| *id);
        assert_eq!(observed, expected);

        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.support_bundle_data_selection_time_range
                    WHERE bundle_id IN ({ids});
                DELETE FROM omicron.public.support_bundle_data_selection_ereports
                    WHERE bundle_id IN ({ids});
                DELETE FROM omicron.public.support_bundle
                    WHERE id IN ({ids});
                DELETE FROM omicron.public.fm_support_bundle_request_data_selection_time_range
                    WHERE sitrep_id = '{SITREP_ID}';
                DELETE FROM omicron.public.fm_support_bundle_request_data_selection_ereports
                    WHERE sitrep_id = '{SITREP_ID}';
                "
            ))
            .await
            .expect("cleaned up test support bundle rows");
    })
}
