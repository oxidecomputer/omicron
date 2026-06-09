// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::SqlEnum;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

const PROJECT_ID: Uuid =
    Uuid::from_u128(0x7fa091df_5fe2_4cbd_9cc1_8742116546f1);

const REGULAR_INSTANCE_ID: Uuid =
    Uuid::from_u128(0x3272631c_9117_4170_b979_cc2fcade2129);
const REGULAR_ACTIVE_VMM_ID: Uuid =
    Uuid::from_u128(0xcde8f413_9af3_48cb_9fda_21b8c18c61e2);

const MIGRATING_INSTANCE_ID: Uuid =
    Uuid::from_u128(0x96f6cd58_b09b_4445_a382_8a3a20858f61);
const MIGRATING_ACTIVE_VMM_ID: Uuid =
    Uuid::from_u128(0x9c1653c0_eee8_4772_b125_33ccf48d0cdd);
const MIGRATING_TARGET_VMM_ID: Uuid =
    Uuid::from_u128(0x4057e883_9210_479f_a080_2cc1b9c386fc);

const DESTROYED_INSTANCE_ID: Uuid =
    Uuid::from_u128(0x126eb09a_a03f_4c66_8d08_b228a5b72f81);
const DESTROYED_ACTIVE_VMM_ID: Uuid =
    Uuid::from_u128(0x7aad4bf3_34ee_4468_a4de_3bdc6e567662);

const ORPHANED_VMM_ID: Uuid =
    Uuid::from_u128(0x81713959_c89a_4cc5_b542_b9d945969393);

fn insert_instance_query(
    id: Uuid,
    name: &str,
    state: &str,
    active_vmm_id: Option<Uuid>,
    target_vmm_id: Option<Uuid>,
    migration_id: Option<Uuid>,
) -> String {
    format!(
        r#"
        INSERT INTO omicron.public.instance (
            id,
            name,
            description,
            time_created,
            time_modified,
            time_deleted,

            project_id,
            user_data,
            time_state_updated,
            state_generation,

            active_propolis_id,
            target_propolis_id,
            migration_id,

            ncpus,
            memory,
            hostname,

            updater_id,
            updater_gen,
            state,

            time_last_auto_restarted,
            auto_restart_policy,
            auto_restart_cooldown,

            boot_disk_id,
            intended_state,
            cpu_platform,
            enable_jumbo_frames
        ) VALUES (
            '{id}',
            '{name}',
            '{name}',
            now(),
            now(),
            null,

            '{PROJECT_ID}',
            b'',
            now(),
            1,

            {},
            {},
            {},

            1,
            655360,
            '{name}',

            null,
            0,
            '{state}',

            null,
            'best_effort',
            null,

            null,
            'running',
            'amd_turin',
            false
        );
    "#,
        match active_vmm_id {
            Some(id) => format!("'{id}'"),
            None => String::from("null"),
        },
        match target_vmm_id {
            Some(id) => format!("'{id}'"),
            None => String::from("null"),
        },
        match migration_id {
            Some(id) => format!("'{id}'"),
            None => String::from("null"),
        },
    )
}

fn insert_sled_resource_vmm_query(id: Uuid, instance_id: Uuid) -> String {
    format!(
        r#"
        INSERT INTO omicron.public.sled_resource_vmm (
            id,
            sled_id,
            hardware_threads,
            rss_ram,
            reservoir_ram,
            instance_id
        ) VALUES (
            '{id}',
            '{}',
            1,
            655360,
            655360,
            '{instance_id}'
        );
    "#,
        Uuid::new_v4(), // random sled
    )
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert a few instances along with their sled_resource_vmm_records:

        // a regular instance

        ctx.client
            .batch_execute(&insert_instance_query(
                REGULAR_INSTANCE_ID,
                "ok",
                "vmm",
                Some(REGULAR_ACTIVE_VMM_ID),
                None,
                None,
            ))
            .await
            .unwrap();

        ctx.client
            .batch_execute(&insert_sled_resource_vmm_query(
                REGULAR_ACTIVE_VMM_ID,
                REGULAR_INSTANCE_ID,
            ))
            .await
            .unwrap();

        // a mid-migration instance

        ctx.client
            .batch_execute(&insert_instance_query(
                MIGRATING_INSTANCE_ID,
                "migrating",
                "vmm",
                Some(MIGRATING_ACTIVE_VMM_ID),
                Some(MIGRATING_TARGET_VMM_ID),
                Some(Uuid::new_v4()),
            ))
            .await
            .unwrap();

        ctx.client
            .batch_execute(&insert_sled_resource_vmm_query(
                MIGRATING_ACTIVE_VMM_ID,
                MIGRATING_INSTANCE_ID,
            ))
            .await
            .unwrap();

        ctx.client
            .batch_execute(&insert_sled_resource_vmm_query(
                MIGRATING_TARGET_VMM_ID,
                MIGRATING_INSTANCE_ID,
            ))
            .await
            .unwrap();

        // one destroyed instance that still has a sled_resource_vmm

        ctx.client
            .batch_execute(&insert_instance_query(
                DESTROYED_INSTANCE_ID,
                "destroyed",
                "destroyed",
                None,
                None,
                None,
            ))
            .await
            .unwrap();

        ctx.client
            .batch_execute(&insert_sled_resource_vmm_query(
                DESTROYED_ACTIVE_VMM_ID,
                DESTROYED_INSTANCE_ID,
            ))
            .await
            .unwrap();

        // also insert a orphaned sled_resource_vmm record

        ctx.client
            .batch_execute(&insert_sled_resource_vmm_query(
                ORPHANED_VMM_ID,
                Uuid::new_v4(),
            ))
            .await
            .unwrap();
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT id, state FROM omicron.public.sled_resource_vmm",
                &[],
            )
            .await
            .expect("failed to query rows after migration");

        assert_eq!(rows.len(), 5);

        let mut expected: HashMap<Uuid, &str> = HashMap::default();

        // The regular instance's VMM should be active
        expected.insert(REGULAR_ACTIVE_VMM_ID, "active");

        // The migrating instance's VMMs should be target and active as
        // appropriate
        expected.insert(MIGRATING_ACTIVE_VMM_ID, "active");
        expected.insert(MIGRATING_TARGET_VMM_ID, "target");

        // The destroyed instance's VMM should be tombstoned
        expected.insert(DESTROYED_ACTIVE_VMM_ID, "tombstoned");

        // The orphaned VMM id should also be tombstoned
        expected.insert(ORPHANED_VMM_ID, "tombstoned");

        for row in &rows {
            let id: Uuid = row.get("id");
            let state: AnySqlType = row.get("state");
            let expected_state = AnySqlType::Enum(SqlEnum::from((
                "sled_resource_vmm_state",
                *expected.get(&id).unwrap(),
            )));

            if expected_state != state {
                panic!("{id} state is {state:?}, should be {expected_state:?}");
            }
        }
    })
}
