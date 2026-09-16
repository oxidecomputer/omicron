// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `add-sled-update-disposition` migration.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// randomly-generated IDs
const BP_ID: &str = "9f1c2f9c-6c2a-4d2f-8d0f-2a3b0c1d4e5f";
const SLED_ID_1: &str = "1f0c9b8a-7d6e-4c3b-9a2f-0e1d2c3b4a59";
const SLED_ID_2: &str = "2e1d0c9b-8a7f-4d3c-8b1a-9f8e7d6c5b40";

const SLED_SUBNET_1: &str = "fd00:1122:3344:0101";
const SLED_SUBNET_2: &str = "fd00:1122:3344:0102";

async fn before_impl(ctx: &MigrationContext<'_>) {
    // Insert two pre-migration `bp_sled_metadata` rows. The update-disposition
    // columns don't exist yet, so we only populate the columns present at the
    // prior schema version.
    ctx.client
        .batch_execute(&format!(
            "
    -- Remove detritus from earlier migrations.
    DELETE FROM omicron.public.bp_sled_metadata WHERE 1=1;

    INSERT INTO omicron.public.bp_sled_metadata (
        blueprint_id, sled_id, sled_state, sled_agent_generation,
        subnet, last_allocated_ip_subnet_offset, measurements
    )
    VALUES
    ('{BP_ID}', '{SLED_ID_1}', 'active', 1, '{SLED_SUBNET_1}::/64', 32,
     'unknown'),
    ('{BP_ID}', '{SLED_ID_2}', 'active', 1, '{SLED_SUBNET_2}::/64', 32,
     'unknown');
            "
        ))
        .await
        .expect("inserted pre-migration data");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let bp_id: Uuid = BP_ID.parse().unwrap();
    let sled_id_1: Uuid = SLED_ID_1.parse().unwrap();
    let sled_id_2: Uuid = SLED_ID_2.parse().unwrap();

    // Both pre-existing rows must be backfilled to the `initial()` disposition:
    // generation 1, `available`, NULL policy.
    let rows = ctx
        .client
        .query(
            "
            SELECT
                sled_id,
                update_disposition_generation,
                update_availability::text AS update_availability,
                update_disruption_policy::text AS update_disruption_policy
            FROM omicron.public.bp_sled_metadata
            WHERE blueprint_id = $1
            ORDER BY sled_id
            ",
            &[&bp_id],
        )
        .await
        .expect("queried post-migration data");

    let got = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Uuid>("sled_id"),
                (
                    row.get::<_, i64>("update_disposition_generation"),
                    row.get::<_, String>("update_availability"),
                    row.get::<_, Option<String>>("update_disruption_policy"),
                ),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        got,
        vec![
            (sled_id_1, (1, "available".to_string(), None)),
            (sled_id_2, (1, "available".to_string(), None)),
        ],
        "pre-existing rows backfilled to the initial disposition",
    );

    // The CHECK constraint must reject inconsistent rows. `available` with a
    // policy is illegal...
    let err = ctx
        .client
        .batch_execute(
            "
            INSERT INTO omicron.public.bp_sled_metadata (
                blueprint_id, sled_id, sled_state, sled_agent_generation,
                subnet, last_allocated_ip_subnet_offset, measurements,
                update_disposition_generation, update_availability,
                update_disruption_policy
            )
            VALUES
            (gen_random_uuid(), gen_random_uuid(), 'active', 1,
             'fd00:1122:3344:0103::/64', 32, 'unknown',
             1, 'available', 'migrate_or_terminate');
            ",
        )
        .await
        .expect_err("available + policy violates the constraint");
    assert_check_violation(&err, "available + a disruption policy");

    // ...and `evacuating` without a policy is illegal.
    let err = ctx
        .client
        .batch_execute(
            "
            INSERT INTO omicron.public.bp_sled_metadata (
                blueprint_id, sled_id, sled_state, sled_agent_generation,
                subnet, last_allocated_ip_subnet_offset, measurements,
                update_disposition_generation, update_availability,
                update_disruption_policy
            )
            VALUES
            (gen_random_uuid(), gen_random_uuid(), 'active', 1,
             'fd00:1122:3344:0104::/64', 32, 'unknown',
             1, 'evacuating', NULL);
            ",
        )
        .await
        .expect_err("evacuating + NULL policy violates the constraint");
    assert_check_violation(&err, "evacuating + a NULL disruption policy");

    // A consistent `evacuating` row with a policy is accepted.
    ctx.client
        .batch_execute(
            "
            INSERT INTO omicron.public.bp_sled_metadata (
                blueprint_id, sled_id, sled_state, sled_agent_generation,
                subnet, last_allocated_ip_subnet_offset, measurements,
                update_disposition_generation, update_availability,
                update_disruption_policy
            )
            VALUES
            (gen_random_uuid(), gen_random_uuid(), 'active', 1,
             'fd00:1122:3344:0105::/64', 32, 'unknown',
             2, 'evacuating', 'migrate_only');
            ",
        )
        .await
        .expect("evacuating + policy satisfies the constraint");
}

/// Asserts that `err` is a violation of the
/// `update_disruption_policy_set_iff_evacuating` CHECK constraint.
///
/// CockroachDB reports the constraint name in the error's `constraint` field.
fn assert_check_violation(err: &tokio_postgres::Error, context: &str) {
    let db_err = err.as_db_error().expect("error came from the database");
    assert_eq!(
        db_err.constraint(),
        Some("update_disruption_policy_set_iff_evacuating"),
        "{context} should violate the update-disposition CHECK constraint; \
         got: {}",
        db_err.message(),
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
