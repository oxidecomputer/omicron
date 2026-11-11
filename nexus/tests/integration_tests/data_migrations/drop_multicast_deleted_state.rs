// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `drop-multicast-deleted-state` migration, which removes the
//! `deleted` value from `multicast_group_state` by moving the `state` column
//! onto a recreated enum type.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const POOL_ID: Uuid = Uuid::from_u128(31700000_0000_0000_0000_000000000001);
const RANGE_ID: Uuid = Uuid::from_u128(31700000_0000_0000_0000_000000000002);
const PARENT_ID: Uuid = Uuid::from_u128(31700000_0000_0000_0000_000000000004);

const CREATING_ID: Uuid = Uuid::from_u128(31700001_0000_0000_0000_000000000001);
const ACTIVE_ID: Uuid = Uuid::from_u128(31700001_0000_0000_0000_000000000002);
const DELETING_ID: Uuid = Uuid::from_u128(31700001_0000_0000_0000_000000000003);
const DELETED_ID: Uuid = Uuid::from_u128(31700001_0000_0000_0000_000000000004);

const GROUPS: [(Uuid, &str, &str, bool); 4] = [
    (CREATING_ID, "creating", "239.255.0.1", false),
    (ACTIVE_ID, "active", "239.255.0.2", false),
    (DELETING_ID, "deleting", "239.255.0.3", true),
    (DELETED_ID, "deleted", "239.255.0.4", true),
];

const ACTIVE_UNDERLAY_ID: Uuid =
    Uuid::from_u128(31700002_0000_0000_0000_000000000002);
const DELETED_UNDERLAY_ID: Uuid =
    Uuid::from_u128(31700002_0000_0000_0000_000000000004);
const ACTIVE_MEMBER_ID: Uuid =
    Uuid::from_u128(31700003_0000_0000_0000_000000000002);
const DELETED_MEMBER_ID: Uuid =
    Uuid::from_u128(31700003_0000_0000_0000_000000000004);

const DEPS: [(Uuid, Uuid, Uuid, &str, &str); 2] = [
    (ACTIVE_ID, ACTIVE_UNDERLAY_ID, ACTIVE_MEMBER_ID, "239.255.0.2", "ff04::2"),
    (
        DELETED_ID,
        DELETED_UNDERLAY_ID,
        DELETED_MEMBER_ID,
        "239.255.0.4",
        "ff04::4",
    ),
];

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        for (id, state, address, soft_deleted) in GROUPS {
            let name = format!("migration-mcast-{state}");
            ctx.client
                .execute(
                    "INSERT INTO omicron.public.multicast_group (
                        id, name, description, time_created, time_modified,
                        time_deleted, vni, ip_pool_id, ip_pool_range_id,
                        multicast_ip, state
                    ) VALUES (
                        $1, $2, '', now(), now(),
                        CASE WHEN $7::BOOL THEN now() ELSE NULL END,
                        1024, $4, $5, $6::TEXT::INET,
                        $3::TEXT::omicron.public.multicast_group_state
                    )",
                    &[
                        &id,
                        &name,
                        &state,
                        &POOL_ID,
                        &RANGE_ID,
                        &address,
                        &soft_deleted,
                    ],
                )
                .await
                .expect("failed to insert pre-migration multicast groups");
        }

        for (group_id, underlay_id, member_id, group_ip, underlay_ip) in DEPS {
            ctx.client
                .execute(
                    "INSERT INTO omicron.public.underlay_multicast_group (
                        id, time_created, time_modified, multicast_ip
                    ) VALUES ($1, now(), now(), $2::TEXT::INET)",
                    &[&underlay_id, &underlay_ip],
                )
                .await
                .expect("failed to insert pre-migration underlay group");

            ctx.client
                .execute(
                    "UPDATE omicron.public.multicast_group
                     SET underlay_group_id = $1
                     WHERE id = $2",
                    &[&underlay_id, &group_id],
                )
                .await
                .expect("failed to link pre-migration underlay group");

            ctx.client
                .execute(
                    "INSERT INTO omicron.public.multicast_group_member (
                        id, time_created, time_modified, external_group_id,
                        parent_id, state, multicast_ip
                    ) VALUES (
                        $1, now(), now(), $2, $3, 'joined', $4::TEXT::INET
                    )",
                    &[&member_id, &group_id, &PARENT_ID, &group_ip],
                )
                .await
                .expect("failed to insert pre-migration multicast member");
        }
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let ids: Vec<_> = GROUPS.iter().map(|(id, _, _, _)| *id).collect();

        let rows = ctx
            .client
            .query(
                "SELECT id, state::TEXT AS state,
                        time_deleted IS NOT NULL AS soft_deleted
                 FROM omicron.public.multicast_group
                 WHERE id = ANY($1)
                 ORDER BY id",
                &[&ids],
            )
            .await
            .expect("failed to query migrated multicast groups");
        let observed: Vec<(Uuid, &str, bool)> = rows
            .iter()
            .map(|row| {
                (row.get("id"), row.get("state"), row.get("soft_deleted"))
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                (CREATING_ID, "creating", false),
                (ACTIVE_ID, "active", false),
                (DELETING_ID, "deleting", true),
            ]
        );

        let underlay_ids = vec![ACTIVE_UNDERLAY_ID, DELETED_UNDERLAY_ID];
        let rows = ctx
            .client
            .query(
                "SELECT id FROM omicron.public.underlay_multicast_group
                 WHERE id = ANY($1)",
                &[&underlay_ids],
            )
            .await
            .expect("failed to query migrated underlay groups");
        let observed: Vec<Uuid> =
            rows.iter().map(|row| row.get("id")).collect();
        assert_eq!(observed, vec![ACTIVE_UNDERLAY_ID]);

        let member_ids = vec![ACTIVE_MEMBER_ID, DELETED_MEMBER_ID];
        let rows = ctx
            .client
            .query(
                "SELECT id FROM omicron.public.multicast_group_member
                 WHERE id = ANY($1)",
                &[&member_ids],
            )
            .await
            .expect("failed to query migrated multicast members");
        let observed: Vec<Uuid> =
            rows.iter().map(|row| row.get("id")).collect();
        assert_eq!(observed, vec![ACTIVE_MEMBER_ID]);

        ctx.client
            .execute(
                "DELETE FROM omicron.public.multicast_group_member
                 WHERE id = ANY($1)",
                &[&member_ids],
            )
            .await
            .expect("failed to clean up multicast member fixtures");

        ctx.client
            .execute(
                "DELETE FROM omicron.public.underlay_multicast_group
                 WHERE id = ANY($1)",
                &[&underlay_ids],
            )
            .await
            .expect("failed to clean up underlay group fixtures");

        ctx.client
            .execute(
                "DELETE FROM omicron.public.multicast_group
                 WHERE id = ANY($1)",
                &[&ids],
            )
            .await
            .expect("failed to clean up multicast migration fixtures");
    })
}
