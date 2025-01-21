// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for affinity groups

use crate::db::model::AffinityPolicyEnum;
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use diesel::sql_types;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

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
            ON
                anti_affinity_group.id = other_instances.group_id AND
                anti_affinity_group.failure_domain = 'sled'
            WHERE anti_affinity_group.time_deleted IS NULL
         )
         SELECT DISTINCT policy,sled_id
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
            ON
                affinity_group.id = other_instances.group_id AND
                affinity_group.failure_domain = 'sled'
            WHERE affinity_group.time_deleted IS NULL
         )
         SELECT DISTINCT policy,sled_id
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
    use crate::db::model;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;

    use anyhow::Context;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_types::external_api::params;
    use nexus_types::identity::Resource;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AffinityGroupUuid;
    use omicron_uuid_kinds::AntiAffinityGroupUuid;
    use omicron_uuid_kinds::SledUuid;
    use uuid::Uuid;

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

    async fn make_affinity_group(
        project_id: Uuid,
        name: &'static str,
        policy: external::AffinityPolicy,
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
    ) -> anyhow::Result<model::AffinityGroup> {
        let group = model::AffinityGroup::new(
            project_id,
            params::AffinityGroupCreate {
                identity: external::IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "desc".to_string(),
                },
                policy,
                failure_domain: external::FailureDomain::Sled,
            },
        );
        use crate::db::schema::affinity_group::dsl;
        diesel::insert_into(dsl::affinity_group)
            .values(group.clone())
            .execute_async(conn)
            .await
            .context("Cannot create affinity group")?;
        Ok(group)
    }

    async fn make_anti_affinity_group(
        project_id: Uuid,
        name: &'static str,
        policy: external::AffinityPolicy,
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
    ) -> anyhow::Result<model::AntiAffinityGroup> {
        let group = model::AntiAffinityGroup::new(
            project_id,
            params::AntiAffinityGroupCreate {
                identity: external::IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "desc".to_string(),
                },
                policy,
                failure_domain: external::FailureDomain::Sled,
            },
        );
        use crate::db::schema::anti_affinity_group::dsl;
        diesel::insert_into(dsl::anti_affinity_group)
            .values(group.clone())
            .execute_async(conn)
            .await
            .context("Cannot create anti affinity group")?;
        Ok(group)
    }

    async fn make_affinity_group_instance_membership(
        group_id: AffinityGroupUuid,
        instance_id: InstanceUuid,
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
    ) -> anyhow::Result<()> {
        // Let's claim an instance belongs to that group
        let membership =
            model::AffinityGroupInstanceMembership::new(group_id, instance_id);
        use crate::db::schema::affinity_group_instance_membership::dsl;
        diesel::insert_into(dsl::affinity_group_instance_membership)
            .values(membership)
            .execute_async(conn)
            .await
            .context("Cannot create affinity group instance membership")?;
        Ok(())
    }

    async fn make_anti_affinity_group_instance_membership(
        group_id: AntiAffinityGroupUuid,
        instance_id: InstanceUuid,
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
    ) -> anyhow::Result<()> {
        // Let's claim an instance belongs to that group
        let membership = model::AntiAffinityGroupInstanceMembership::new(
            group_id,
            instance_id,
        );
        use crate::db::schema::anti_affinity_group_instance_membership::dsl;
        diesel::insert_into(dsl::anti_affinity_group_instance_membership)
            .values(membership)
            .execute_async(conn)
            .await
            .context("Cannot create anti affinity group instance membership")?;
        Ok(())
    }

    async fn make_instance_sled_resource(
        sled_id: SledUuid,
        instance_id: InstanceUuid,
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
    ) -> anyhow::Result<()> {
        let resource = model::SledResource::new(
            instance_id.into_untyped_uuid(),
            sled_id.into_untyped_uuid(),
            model::SledResourceKind::Instance,
            model::Resources::new(
                0,
                model::ByteCount::from(
                    external::ByteCount::from_gibibytes_u32(0),
                ),
                model::ByteCount::from(
                    external::ByteCount::from_gibibytes_u32(0),
                ),
            ),
        );
        use crate::db::schema::sled_resource::dsl;
        diesel::insert_into(dsl::sled_resource)
            .values(resource)
            .execute_async(conn)
            .await
            .context("Cannot create sled resource")?;
        Ok(())
    }

    #[tokio::test]
    async fn lookup_affinity_sleds() {
        let logctx = dev::test_setup_log("lookup_affinity_sleds");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let our_instance_id = InstanceUuid::new_v4();
        let other_instance_id = InstanceUuid::new_v4();

        // With no groups and no instances, we should see no other instances
        // belonging to our affinity group.
        assert_eq!(
            lookup_affinity_sleds_query(our_instance_id,)
                .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
                .await
                .unwrap(),
            vec![],
        );

        let sled_id = SledUuid::new_v4();
        let project_id = Uuid::new_v4();

        // Make a group, add our instance to it
        let group = make_affinity_group(
            project_id,
            "group-allow",
            external::AffinityPolicy::Allow,
            &conn,
        )
        .await
        .unwrap();
        make_affinity_group_instance_membership(
            AffinityGroupUuid::from_untyped_uuid(group.id()),
            our_instance_id,
            &conn,
        )
        .await
        .unwrap();

        // Create an instance which belongs to that group
        make_affinity_group_instance_membership(
            AffinityGroupUuid::from_untyped_uuid(group.id()),
            other_instance_id,
            &conn,
        )
        .await
        .unwrap();

        make_instance_sled_resource(sled_id, other_instance_id, &conn)
            .await
            .unwrap();

        // Now if we look, we'll find the "other sled", on which the "other
        // instance" was placed.
        assert_eq!(
            lookup_affinity_sleds_query(our_instance_id,)
                .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
                .await
                .unwrap(),
            vec![(model::AffinityPolicy::Allow, sled_id.into_untyped_uuid())],
        );

        // If we look from the perspective of the other instance,
        // we "ignore ourselves" for placement, so the set of sleds to consider
        // is still empty.
        assert_eq!(
            lookup_affinity_sleds_query(other_instance_id,)
                .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
                .await
                .unwrap(),
            vec![]
        );

        // If we make another group (note the policy is different this time!)
        // it'll also be reflected in the results.
        let group = make_affinity_group(
            project_id,
            "group-fail",
            external::AffinityPolicy::Fail,
            &conn,
        )
        .await
        .unwrap();
        make_affinity_group_instance_membership(
            AffinityGroupUuid::from_untyped_uuid(group.id()),
            our_instance_id,
            &conn,
        )
        .await
        .unwrap();
        make_affinity_group_instance_membership(
            AffinityGroupUuid::from_untyped_uuid(group.id()),
            other_instance_id,
            &conn,
        )
        .await
        .unwrap();

        // We see the outcome of both groups, with a different policy for each.
        let mut results = lookup_affinity_sleds_query(our_instance_id)
            .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
            .await
            .unwrap();
        results.sort();
        assert_eq!(
            results,
            vec![
                (model::AffinityPolicy::Fail, sled_id.into_untyped_uuid()),
                (model::AffinityPolicy::Allow, sled_id.into_untyped_uuid()),
            ],
        );

        // Let's add one more group, to see what happens to the query output.
        let group = make_affinity_group(
            project_id,
            "group-fail2",
            external::AffinityPolicy::Fail,
            &conn,
        )
        .await
        .unwrap();
        make_affinity_group_instance_membership(
            AffinityGroupUuid::from_untyped_uuid(group.id()),
            our_instance_id,
            &conn,
        )
        .await
        .unwrap();
        make_affinity_group_instance_membership(
            AffinityGroupUuid::from_untyped_uuid(group.id()),
            other_instance_id,
            &conn,
        )
        .await
        .unwrap();
        let mut results = lookup_affinity_sleds_query(our_instance_id)
            .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
            .await
            .unwrap();
        results.sort();

        // Since we use "SELECT DISTINCT", the results are bounded, and do not
        // grow as we keep on finding more sleds that have the same policy.
        assert_eq!(
            results,
            vec![
                (model::AffinityPolicy::Fail, sled_id.into_untyped_uuid()),
                (model::AffinityPolicy::Allow, sled_id.into_untyped_uuid()),
            ],
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn lookup_anti_affinity_sleds() {
        let logctx = dev::test_setup_log("lookup_anti_affinity_sleds");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let our_instance_id = InstanceUuid::new_v4();
        let other_instance_id = InstanceUuid::new_v4();

        // With no groups and no instances, we should see no other instances
        // belonging to our anti-affinity group.
        assert_eq!(
            lookup_anti_affinity_sleds_query(our_instance_id,)
                .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
                .await
                .unwrap(),
            vec![],
        );

        let sled_id = SledUuid::new_v4();
        let project_id = Uuid::new_v4();

        // Make a group, add our instance to it
        let group = make_anti_affinity_group(
            project_id,
            "group-allow",
            external::AffinityPolicy::Allow,
            &conn,
        )
        .await
        .unwrap();
        make_anti_affinity_group_instance_membership(
            AntiAffinityGroupUuid::from_untyped_uuid(group.id()),
            our_instance_id,
            &conn,
        )
        .await
        .unwrap();

        // Create an instance which belongs to that group
        make_anti_affinity_group_instance_membership(
            AntiAffinityGroupUuid::from_untyped_uuid(group.id()),
            other_instance_id,
            &conn,
        )
        .await
        .unwrap();

        make_instance_sled_resource(sled_id, other_instance_id, &conn)
            .await
            .unwrap();

        // Now if we look, we'll find the "other sled", on which the "other
        // instance" was placed.
        assert_eq!(
            lookup_anti_affinity_sleds_query(our_instance_id,)
                .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
                .await
                .unwrap(),
            vec![(model::AffinityPolicy::Allow, sled_id.into_untyped_uuid())],
        );

        // If we look from the perspective of the other instance,
        // we "ignore ourselves" for placement, so the set of sleds to consider
        // is still empty.
        assert_eq!(
            lookup_anti_affinity_sleds_query(other_instance_id,)
                .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
                .await
                .unwrap(),
            vec![]
        );

        // If we make another group (note the policy is different this time!)
        // it'll also be reflected in the results.
        let group = make_anti_affinity_group(
            project_id,
            "group-fail",
            external::AffinityPolicy::Fail,
            &conn,
        )
        .await
        .unwrap();
        make_anti_affinity_group_instance_membership(
            AntiAffinityGroupUuid::from_untyped_uuid(group.id()),
            our_instance_id,
            &conn,
        )
        .await
        .unwrap();
        make_anti_affinity_group_instance_membership(
            AntiAffinityGroupUuid::from_untyped_uuid(group.id()),
            other_instance_id,
            &conn,
        )
        .await
        .unwrap();

        // We see the outcome of both groups, with a different policy for each.
        let mut results = lookup_anti_affinity_sleds_query(our_instance_id)
            .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
            .await
            .unwrap();
        results.sort();
        assert_eq!(
            results,
            vec![
                (model::AffinityPolicy::Fail, sled_id.into_untyped_uuid()),
                (model::AffinityPolicy::Allow, sled_id.into_untyped_uuid()),
            ],
        );

        // Let's add one more group, to see what happens to the query output.
        let group = make_anti_affinity_group(
            project_id,
            "group-fail2",
            external::AffinityPolicy::Fail,
            &conn,
        )
        .await
        .unwrap();
        make_anti_affinity_group_instance_membership(
            AntiAffinityGroupUuid::from_untyped_uuid(group.id()),
            our_instance_id,
            &conn,
        )
        .await
        .unwrap();
        make_anti_affinity_group_instance_membership(
            AntiAffinityGroupUuid::from_untyped_uuid(group.id()),
            other_instance_id,
            &conn,
        )
        .await
        .unwrap();
        let mut results = lookup_anti_affinity_sleds_query(our_instance_id)
            .get_results_async::<(model::AffinityPolicy, Uuid)>(&*conn)
            .await
            .unwrap();
        results.sort();

        // Since we use "SELECT DISTINCT", the results are bounded, and do not
        // grow as we keep on finding more sleds that have the same policy.
        assert_eq!(
            results,
            vec![
                (model::AffinityPolicy::Fail, sled_id.into_untyped_uuid()),
                (model::AffinityPolicy::Allow, sled_id.into_untyped_uuid()),
            ],
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
