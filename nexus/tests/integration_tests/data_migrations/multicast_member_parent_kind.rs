// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `multicast-member-parent-kind` migration.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

// randomly-generated IDs
const GROUP_ID: &str = "86108882-b539-4ed0-bbd9-83043e073c41";
const PARENT_ID: &str = "09214958-8b50-4b0c-a35d-a20e5a758d12";
const MEMBER_ID: &str = "ff5d5cf3-4a3d-48e3-9e7c-c091385aed3b";

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!(
            "
    INSERT INTO omicron.public.multicast_group_member (
        id, time_created, time_modified, external_group_id, parent_id,
        state, multicast_ip
    )
    VALUES
    ('{MEMBER_ID}', now(), now(), '{GROUP_ID}', '{PARENT_ID}', 'joined',
     '224.2.0.1');
            "
        ))
        .await
        .expect("inserted pre-migration data");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let member_id: Uuid = MEMBER_ID.parse().unwrap();

    let row = ctx
        .client
        .query_one(
            "SELECT parent_kind::text AS parent_kind
             FROM omicron.public.multicast_group_member
             WHERE id = $1",
            &[&member_id],
        )
        .await
        .expect("queried post-migration data");
    assert_eq!(row.get::<_, String>("parent_kind"), "instance");

    let err = ctx
        .client
        .batch_execute(&format!(
            "
            INSERT INTO omicron.public.multicast_group_member (
                id, time_created, time_modified, external_group_id,
                parent_id, state, multicast_ip
            )
            VALUES
            (gen_random_uuid(), now(), now(), '{GROUP_ID}',
             gen_random_uuid(), 'joined', '224.2.0.1');
            "
        ))
        .await
        .expect_err("insert without parent_kind should fail");
    assert_eq!(err.code(), Some(&SqlState::NOT_NULL_VIOLATION));

    ctx.client
        .batch_execute(&format!(
            "DELETE FROM omicron.public.multicast_group_member
             WHERE external_group_id = '{GROUP_ID}';"
        ))
        .await
        .expect("cleaned up test data");
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
