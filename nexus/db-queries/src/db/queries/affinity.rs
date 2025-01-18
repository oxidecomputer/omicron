// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for affinity groups

use crate::db::model::AffinityPolicyEnum;
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use diesel::sql_types;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

// TODO: Check for "failure_domain = 'sled'"?

/// For an instance, look up all anti-affinity groups, and return
/// a list of sleds with other instances in that anti-affinity group
/// already reserved.
pub fn lookup_anti_affinity_sleds_query(
    instance_id: InstanceUuid,
) -> TypedSqlQuery<(AffinityPolicyEnum, sql_types::Uuid)> {
    QueryBuilder::new().sql(
        "WITH our_groups AS (
            SELECT group_id
            FROM anti_affinity_group_instance_membership
            WHERE instance_id = ").param().sql("
         ),
         other_instances AS (
            SELECT anti_affinity_group_instance_membership.group_id,instance_id
            FROM anti_affinity_group_instance_membership
            JOIN our_groups
            ON anti_affinity_group_instance_membership.group_id = our_groups.group_id
            WHERE instance_id != ").param().sql("
         ),
         other_instances_by_policy AS (
            SELECT policy,instance_id
            FROM other_instances
            JOIN anti_affinity_group
            ON anti_affinity_group.id = other_instances.group_id
            WHERE anti_affinity_group.time_deleted IS NULL
         )
         SELECT policy,sled_id
         FROM other_instances_by_policy
         JOIN sled_resource
         ON
            sled_resource.id = other_instances_by_policy.instance_id AND
            sled_resource.kind = 'instance'")
     .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
     .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
     .query()
}

/// For an instance, look up all affinity groups, and return a list of sleds
/// with other instances in that affinity group already reserved.
pub fn lookup_affinity_sleds_query(
    instance_id: InstanceUuid,
) -> TypedSqlQuery<(AffinityPolicyEnum, sql_types::Uuid)> {
    QueryBuilder::new()
        .sql(
            "WITH our_groups AS (
            SELECT group_id
            FROM affinity_group_instance_membership
            WHERE instance_id = ",
        )
        .param()
        .sql(
            "
         ),
         other_instances AS (
            SELECT affinity_group_instance_membership.group_id,instance_id
            FROM affinity_group_instance_membership
            JOIN our_groups
            ON affinity_group_instance_membership.group_id = our_groups.group_id
            WHERE instance_id != ",
        )
        .param()
        .sql(
            "
         ),
         other_instances_by_policy AS (
            SELECT policy,instance_id
            FROM other_instances
            JOIN affinity_group
            ON affinity_group.id = other_instances.group_id
            WHERE affinity_group.time_deleted IS NULL
         )
         SELECT policy,sled_id
         FROM other_instances_by_policy
         JOIN sled_resource
         ON
            sled_resource.id = other_instances_by_policy.instance_id AND
            sled_resource.kind = 'instance'",
        )
        .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
        .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
        .query()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn expectorate_lookup_anti_affinity_sleds_query() {
        let id = InstanceUuid::nil();

        let query = lookup_anti_affinity_sleds_query(id);
        expectorate_query_contents(
            &query,
            "tests/output/lookup_anti_affinity_sleds_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_lookup_affinity_sleds_query() {
        let id = InstanceUuid::nil();

        let query = lookup_affinity_sleds_query(id);
        expectorate_query_contents(
            &query,
            "tests/output/lookup_affinity_sleds_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_lookup_anti_affinity_sleds_query() {
        let logctx =
            dev::test_setup_log("explain_lookup_anti_affinity_sleds_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let query = lookup_anti_affinity_sleds_query(id);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_lookup_affinity_sleds_query() {
        let logctx = dev::test_setup_log("explain_lookup_affinity_sleds_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let query = lookup_affinity_sleds_query(id);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
