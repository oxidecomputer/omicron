// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `remove-cross-project-affinity-memberships` migration, which
//! deletes affinity and anti-affinity group memberships whose instance is in
//! a different project than the group.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const GROUP_PROJECT_ID: Uuid =
    Uuid::from_u128(0xaff10000_0000_0000_0000_000000000001);
const OTHER_PROJECT_ID: Uuid =
    Uuid::from_u128(0xaff10000_0000_0000_0000_000000000002);

const LOCAL_INSTANCE_ID: Uuid =
    Uuid::from_u128(0xaff10001_0000_0000_0000_000000000001);
const OTHER_INSTANCE_ID: Uuid =
    Uuid::from_u128(0xaff10001_0000_0000_0000_000000000002);

const AFFINITY_GROUP_ID: Uuid =
    Uuid::from_u128(0xaff10002_0000_0000_0000_000000000001);
const ANTI_AFFINITY_GROUP_ID: Uuid =
    Uuid::from_u128(0xaff10002_0000_0000_0000_000000000002);

const INSTANCES: [(Uuid, &str, Uuid); 2] = [
    (LOCAL_INSTANCE_ID, "migration-local-instance", GROUP_PROJECT_ID),
    (OTHER_INSTANCE_ID, "migration-other-instance", OTHER_PROJECT_ID),
];

// (group table, membership table, group ID)
const GROUPS: [(&str, &str, Uuid); 2] = [
    ("affinity_group", "affinity_group_instance_membership", AFFINITY_GROUP_ID),
    (
        "anti_affinity_group",
        "anti_affinity_group_instance_membership",
        ANTI_AFFINITY_GROUP_ID,
    ),
];

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        for (id, name, project_id) in INSTANCES {
            ctx.client
                .execute(
                    "INSERT INTO omicron.public.instance (
                        id, name, description, time_created, time_modified,
                        project_id, user_data, time_state_updated,
                        state_generation, ncpus, memory, hostname, state,
                        intended_state, enable_jumbo_frames
                    ) VALUES (
                        $1, $2, '', now(), now(),
                        $3, b'', now(),
                        1, 1, 655360, $2, 'no_vmm',
                        'stopped', false
                    )",
                    &[&id, &name, &project_id],
                )
                .await
                .expect("failed to insert pre-migration instance");
        }

        for (group_table, membership_table, group_id) in GROUPS {
            ctx.client
                .execute(
                    &format!(
                        "INSERT INTO omicron.public.{group_table} (
                            id, name, description, time_created,
                            time_modified, project_id, policy, failure_domain
                        ) VALUES (
                            $1, 'migration-group', '', now(),
                            now(), $2, 'fail', 'sled'
                        )"
                    ),
                    &[&group_id, &GROUP_PROJECT_ID],
                )
                .await
                .expect("failed to insert pre-migration group");

            for (instance_id, ..) in INSTANCES {
                ctx.client
                    .execute(
                        &format!(
                            "INSERT INTO omicron.public.{membership_table}
                                (group_id, instance_id)
                            VALUES ($1, $2)"
                        ),
                        &[&group_id, &instance_id],
                    )
                    .await
                    .expect("failed to insert pre-migration membership");
            }
        }
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        for (group_table, membership_table, group_id) in GROUPS {
            let rows = ctx
                .client
                .query(
                    &format!(
                        "SELECT instance_id
                        FROM omicron.public.{membership_table}
                        WHERE group_id = $1"
                    ),
                    &[&group_id],
                )
                .await
                .expect("failed to query migrated memberships");
            let members: Vec<Uuid> =
                rows.iter().map(|row| row.get("instance_id")).collect();
            assert_eq!(
                members,
                vec![LOCAL_INSTANCE_ID],
                "{membership_table}: only the same-project member should \
                 remain",
            );

            ctx.client
                .execute(
                    &format!(
                        "DELETE FROM omicron.public.{membership_table}
                        WHERE group_id = $1"
                    ),
                    &[&group_id],
                )
                .await
                .expect("failed to clean up memberships");
            ctx.client
                .execute(
                    &format!(
                        "DELETE FROM omicron.public.{group_table}
                        WHERE id = $1"
                    ),
                    &[&group_id],
                )
                .await
                .expect("failed to clean up group");
        }

        let instance_ids: Vec<Uuid> =
            INSTANCES.iter().map(|(id, ..)| *id).collect();
        ctx.client
            .execute(
                "DELETE FROM omicron.public.instance WHERE id = ANY($1)",
                &[&instance_ids],
            )
            .await
            .expect("failed to clean up instances");
    })
}
