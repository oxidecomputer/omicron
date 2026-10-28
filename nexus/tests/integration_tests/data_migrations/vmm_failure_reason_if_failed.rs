// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the vmm-failure-reason-if-failed migration.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;

// Randomly-generated IDs.
const INSTANCE_ID: &str = "5d3c8e2a-41f7-4b9e-a6d0-7c2f1e9b3a84";
const SLED_ID: &str = "b8e4a1f2-9c3d-4e6b-8f7a-2d5c0b1e4a93";
const VMM_FAILED: &str = "3a7f2c9e-6b1d-4f8a-9e2c-5d4b7a1f0e62";
const VMM_UNWOUND_WITH_REASON: &str = "e1c9b5a3-2f7d-4a6e-b8c4-9d0f3e2a7b15";
const VMM_FAILED_NO_REASON: &str = "7b2e4d9c-1a5f-4c8b-a3e6-0f9d2c7b5e48";

fn insert_vmm_sql(id: &str, state: &str, failure_reason: &str) -> String {
    format!(
        "INSERT INTO omicron.public.vmm (
            id, time_created, time_deleted, instance_id, time_state_updated,
            state_generation, sled_id, propolis_ip, propolis_port, state,
            cpu_platform, failure_reason
        ) VALUES (
            '{id}', now(), NULL, '{INSTANCE_ID}', now(),
            1, '{SLED_ID}', 'fd00:1122:3344:101::1', 12400, '{state}',
            'sled_default', {failure_reason}
        );"
    )
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&insert_vmm_sql(
            VMM_FAILED,
            "failed",
            "'no_such_instance'",
        ))
        .await
        .expect("inserted pre-migration failed VMM");

    // Pre-migration, the iff (two-directional) constraint rejects a non-failed
    // VMM with a reason.
    let err = ctx
        .client
        .batch_execute(&insert_vmm_sql(
            VMM_UNWOUND_WITH_REASON,
            "saga_unwound",
            "'no_such_instance'",
        ))
        .await
        .expect_err("saga_unwound + reason violates the old constraint");
    assert_check_violation(
        &err,
        "failure_reason_iff_failed",
        "pre-migration saga_unwound + a failure reason",
    );
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let rows = ctx
        .client
        .query(
            &format!(
                "SELECT state::text AS state, \
                        failure_reason::text AS failure_reason \
                 FROM omicron.public.vmm WHERE id = '{VMM_FAILED}'"
            ),
            &[],
        )
        .await
        .expect("queried pre-migration failed VMM");
    let observed: Vec<(&str, Option<&str>)> = rows
        .iter()
        .map(|r| (r.get("state"), r.get("failure_reason")))
        .collect();
    assert_eq!(observed, vec![("failed", Some("no_such_instance"))]);

    let rows = ctx
        .client
        .query(
            "SELECT constraint_name \
             FROM [SHOW CONSTRAINTS FROM omicron.public.vmm] \
             WHERE constraint_name LIKE 'failure_reason_%' \
             ORDER BY constraint_name",
            &[],
        )
        .await
        .expect("listed vmm constraints");
    let constraints: Vec<&str> =
        rows.iter().map(|r| r.get("constraint_name")).collect();
    assert_eq!(constraints, vec!["failure_reason_if_failed"]);

    ctx.client
        .batch_execute(&insert_vmm_sql(
            VMM_UNWOUND_WITH_REASON,
            "saga_unwound",
            "'no_such_instance'",
        ))
        .await
        .expect("saga_unwound + stale reason is accepted post-migration");

    let err = ctx
        .client
        .batch_execute(&insert_vmm_sql(VMM_FAILED_NO_REASON, "failed", "NULL"))
        .await
        .expect_err("failed + NULL reason violates the new constraint");
    assert_check_violation(
        &err,
        "failure_reason_if_failed",
        "post-migration failed + a NULL failure reason",
    );

    let ids = [VMM_FAILED, VMM_UNWOUND_WITH_REASON, VMM_FAILED_NO_REASON]
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.client
        .batch_execute(&format!(
            "DELETE FROM omicron.public.vmm WHERE id IN ({ids});"
        ))
        .await
        .expect("cleaned up test VMMs");
}

fn assert_check_violation(
    err: &tokio_postgres::Error,
    constraint: &str,
    context: &str,
) {
    let db_err = err.as_db_error().expect("error came from the database");
    assert_eq!(
        db_err.constraint(),
        Some(constraint),
        "{context} should violate {constraint}; got: {}",
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
