// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `add-rendezvous-sled-bp-availability` migration, in particular its
//! backfill of `rendezvous_sled_bp_availability` from the current target blueprint.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const BP_TARGET: Uuid = Uuid::from_u128(0xa0c1f2e3_4d5b_46a7_8b9c_0d1e2f3a4b5c);
const BP_OTHER: Uuid = Uuid::from_u128(0xb1d2e3f4_5a6c_47b8_9cad_1e2f3a4b5c6d);

// In the target blueprint: active + available -> backfilled available.
const SLED_AVAIL: Uuid =
    Uuid::from_u128(0x10000000_0000_4000_8000_000000000001);
// In the target blueprint: active + evacuating -> backfilled unavailable.
const SLED_EVAC: Uuid = Uuid::from_u128(0x10000000_0000_4000_8000_000000000002);
// In the target blueprint: decommissioned -> backfilled as decommissioned.
const SLED_DECOM: Uuid =
    Uuid::from_u128(0x10000000_0000_4000_8000_000000000003);
// Active + available, but in an older target blueprint -> not backfilled, since
// the backfill follows only the newest target.
const SLED_OTHER_BP: Uuid =
    Uuid::from_u128(0x10000000_0000_4000_8000_000000000004);

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!(
            "
    -- Remove bp_sled_metadata detritus from earlier migrations.
    DELETE FROM omicron.public.bp_sled_metadata WHERE 1=1;

    -- Two targets: an older one (BP_OTHER, at MAX(version) + 1) superseded by
    -- the newest one (BP_TARGET, at MAX(version) + 2). The backfill is expected
    -- to only use BP_TARGET and ignore BP_OTHER.
    INSERT INTO omicron.public.bp_target (
        version, blueprint_id, enabled, time_made_target
    )
    SELECT COALESCE(MAX(version), 0) + 1, '{BP_OTHER}', true, now()
        FROM omicron.public.bp_target
    UNION ALL
    SELECT COALESCE(MAX(version), 0) + 2, '{BP_TARGET}', true, now()
        FROM omicron.public.bp_target;

    INSERT INTO omicron.public.bp_sled_metadata (
        blueprint_id, sled_id, sled_state, sled_agent_generation,
        subnet, last_allocated_ip_subnet_offset, measurements,
        update_disposition_generation, update_availability,
        update_disruption_policy
    )
    VALUES
    -- Target blueprint, active + available at generation 1.
    ('{BP_TARGET}', '{SLED_AVAIL}', 'active', 1,
     'fd00:1122:3344:0101::/64', 32, 'unknown', 1, 'available', NULL),
    -- Target blueprint, active + evacuating at generation 3.
    ('{BP_TARGET}', '{SLED_EVAC}', 'active', 1,
     'fd00:1122:3344:0102::/64', 32, 'unknown', 3, 'evacuating',
     'migrate_only'),
    -- Target blueprint, but decommissioned.
    ('{BP_TARGET}', '{SLED_DECOM}', 'decommissioned', 1,
     'fd00:1122:3344:0103::/64', 32, 'unknown', 1, 'available', NULL),
    -- Active + available, but in the older blueprint: must be ignored
    -- by the backfill.
    ('{BP_OTHER}', '{SLED_OTHER_BP}', 'active', 1,
     'fd00:1122:3344:0104::/64', 32, 'unknown', 1, 'available', NULL);
            "
        ))
        .await
        .expect("inserted pre-migration data");
}

const UP4_SQL: &str = include_str!(
    "../../../../schema/crdb/add-rendezvous-sled-bp-availability/up4.sql"
);

async fn query_backfilled_rows(
    ctx: &MigrationContext<'_>,
) -> Vec<(Uuid, String, Option<i64>, Uuid, String, String)> {
    ctx.client
        .query(
            "
            SELECT
                sled_id,
                bp_availability::text AS bp_availability,
                update_disposition_generation,
                blueprint_id,
                time_created::text AS time_created,
                time_modified::text AS time_modified
            FROM omicron.public.rendezvous_sled_bp_availability
            ORDER BY sled_id
            ",
            &[],
        )
        .await
        .expect("queried backfilled rendezvous_sled_bp_availability")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Uuid>("sled_id"),
                row.get::<_, String>("bp_availability"),
                row.get::<_, Option<i64>>("update_disposition_generation"),
                row.get::<_, Uuid>("blueprint_id"),
                row.get::<_, String>("time_created"),
                row.get::<_, String>("time_modified"),
            )
        })
        .collect()
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let rows = query_backfilled_rows(ctx).await;

    let got = rows
        .iter()
        .map(|(sled_id, bp_availability, generation, blueprint_id, _, _)| {
            (*sled_id, bp_availability.clone(), *generation, *blueprint_id)
        })
        .collect::<Vec<_>>();

    // Only sleds from the newest target blueprint are backfilled (
    // SLED_OTHER_BP is excluded).
    assert_eq!(
        got,
        vec![
            (SLED_AVAIL, "available".to_string(), Some(1), BP_TARGET),
            (SLED_EVAC, "unavailable".to_string(), Some(3), BP_TARGET),
            (SLED_DECOM, "decommissioned".to_string(), None, BP_TARGET),
        ],
        "backfill should include only target-blueprint sleds, with \
         bp_availability and generation derived from bp_sled_metadata",
    );

    // Run up4.sql again to ensure it is idempotent.
    ctx.client
        .batch_execute(&format!("BEGIN; {UP4_SQL}; COMMIT;"))
        .await
        .expect("re-ran the up4 backfill");

    let rows_after_rerun = query_backfilled_rows(ctx).await;
    assert_eq!(
        rows, rows_after_rerun,
        "re-running the up4 backfill must be a no-op, leaving all rows \
         (including timestamps) unchanged",
    );
}

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
