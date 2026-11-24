// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `reconfigurator-config-enable-sled-evacuation` migration.
//! If the latest `reconfigurator_config` version uses
//! `immediate_no_evacuation`, the migration inserts a new version with the
//! same configuration as the latest version, except the
//! `sled_update_reboot_policy`, which is set to `evacuate`.

use super::super::schema::{DataMigrationFns, MigrationContext};
use chrono::{DateTime, TimeZone, Utc};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;

const UP1_SQL: &str = include_str!(
    "../../../../schema/crdb/reconfigurator-config-enable-sled-evacuation/up1.sql"
);

#[derive(Debug, PartialEq)]
struct ConfigRow {
    version: i64,
    planner_enabled: bool,
    time_modified: DateTime<Utc>,
    tuf_repo_pruner_enabled: bool,
    disruption_policy: String,
    blueprint_pruner_enabled: bool,
    blueprint_pruner_nkeep: i64,
    sled_update_reboot_policy: String,
}

fn version_1() -> ConfigRow {
    ConfigRow {
        version: 1,
        planner_enabled: true,
        time_modified: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        tuf_repo_pruner_enabled: true,
        disruption_policy: "terminate".to_string(),
        blueprint_pruner_enabled: true,
        blueprint_pruner_nkeep: 1000,
        sled_update_reboot_policy: "evacuate".to_string(),
    }
}

fn version_2() -> ConfigRow {
    ConfigRow {
        version: 2,
        planner_enabled: false,
        time_modified: Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap(),
        tuf_repo_pruner_enabled: false,
        disruption_policy: "terminate".to_string(),
        blueprint_pruner_enabled: false,
        blueprint_pruner_nkeep: 17,
        sled_update_reboot_policy: "immediate_no_evacuation".to_string(),
    }
}

async fn insert_row(ctx: &MigrationContext<'_>, row: &ConfigRow) {
    ctx.client
        .execute(
            "
            INSERT INTO omicron.public.reconfigurator_config (
                version, planner_enabled, time_modified,
                tuf_repo_pruner_enabled, disruption_policy,
                blueprint_pruner_enabled, blueprint_pruner_nkeep,
                sled_update_reboot_policy
            ) VALUES (
                $1, $2, $3, $4,
                $5::TEXT::omicron.public.reconfigurator_disruption_policy,
                $6, $7,
                $8::TEXT::omicron.public.sled_update_reboot_policy
            )
            ",
            &[
                &row.version,
                &row.planner_enabled,
                &row.time_modified,
                &row.tuf_repo_pruner_enabled,
                &row.disruption_policy,
                &row.blueprint_pruner_enabled,
                &row.blueprint_pruner_nkeep,
                &row.sled_update_reboot_policy,
            ],
        )
        .await
        .expect("inserted pre-migration reconfigurator_config row");
}

async fn query_rows(ctx: &MigrationContext<'_>) -> Vec<ConfigRow> {
    ctx.client
        .query(
            "
            SELECT
                version,
                planner_enabled,
                time_modified,
                tuf_repo_pruner_enabled,
                disruption_policy::text AS disruption_policy,
                blueprint_pruner_enabled,
                blueprint_pruner_nkeep,
                sled_update_reboot_policy::text AS sled_update_reboot_policy
            FROM omicron.public.reconfigurator_config
            ORDER BY version
            ",
            &[],
        )
        .await
        .expect("queried reconfigurator_config")
        .into_iter()
        .map(|row| ConfigRow {
            version: row.get("version"),
            planner_enabled: row.get("planner_enabled"),
            time_modified: row.get("time_modified"),
            tuf_repo_pruner_enabled: row.get("tuf_repo_pruner_enabled"),
            disruption_policy: row.get("disruption_policy"),
            blueprint_pruner_enabled: row.get("blueprint_pruner_enabled"),
            blueprint_pruner_nkeep: row.get("blueprint_pruner_nkeep"),
            sled_update_reboot_policy: row.get("sled_update_reboot_policy"),
        })
        .collect()
}

async fn run_up1(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!("BEGIN; {UP1_SQL}; COMMIT;"))
        .await
        .expect("ran up1.sql again");
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    insert_row(ctx, &version_1()).await;
    insert_row(ctx, &version_2()).await;
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    // The latest version (2) uses `immediate_no_evacuation`, so the migration
    // should have inserted version 3. Version 3 copies every column from
    // version 2, except `time_modified` (set to the time of the migration) and
    // `sled_update_reboot_policy` (set to `evacuate`). Versions 1 and 2 must be
    // unchanged.
    let rows = query_rows(ctx).await;
    assert_eq!(rows.len(), 3, "migration should insert exactly one version");
    let new_time_modified = rows[2].time_modified;
    assert!(
        new_time_modified > version_2().time_modified,
        "new version's time_modified ({new_time_modified}) should be later \
         than the previous version's",
    );
    assert_eq!(
        rows,
        vec![
            version_1(),
            version_2(),
            ConfigRow {
                version: 3,
                time_modified: new_time_modified,
                sled_update_reboot_policy: "evacuate".to_string(),
                ..version_2()
            },
        ],
    );

    // The latest version now uses `evacuate`, so running the migration again
    // should not insert anything.
    run_up1(ctx).await;
    assert_eq!(
        query_rows(ctx).await,
        rows,
        "re-running the migration when the latest version uses `evacuate` \
         must not change the table",
    );

    // With an empty table, the migration should not insert anything.
    ctx.client
        .batch_execute(
            "DELETE FROM omicron.public.reconfigurator_config WHERE 1=1;",
        )
        .await
        .expect("deleted reconfigurator_config rows");
    run_up1(ctx).await;
    assert_eq!(
        query_rows(ctx).await,
        Vec::<ConfigRow>::new(),
        "running the migration on an empty table must not insert anything",
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
