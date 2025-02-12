// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for affinity groups

use crate::db::model::AffinityPolicyEnum;
use crate::db::model::Resources;
use crate::db::model::SledResourceVmm;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use diesel::sql_types;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

/// Return all possible Sleds where we might perform allocation
///
/// The rows returned by this CTE indicate:
///
/// - The Sled which we're considering
/// - A bool indicating whether the allocation fits
/// - Affinity Policy
/// - Anti-Affinity Policy
///
/// Generally, we'd like to only return sleds where an allocation of [Resources]
/// could fit. However, we also need to observe all affinity groups, in case
/// affinity group policy forces us to allocate on a sled where insufficient
/// space exists.
///
/// Note that we don't bother checking if the VMM has already been provisioned,
/// we're just searching for spots where it might fit.
pub fn sled_find_targets_query(
    instance_id: InstanceUuid,
    resources: &Resources,
) -> TypedSqlQuery<(
    sql_types::Uuid,
    sql_types::Bool,
    sql_types::Nullable<AffinityPolicyEnum>,
    sql_types::Nullable<AffinityPolicyEnum>,
)> {
    QueryBuilder::new().sql("
        WITH sled_targets AS (
            SELECT sled.id as sled_id
            FROM sled
            LEFT JOIN sled_resource_vmm
            ON sled_resource_vmm.sled_id = sled.id
            WHERE
                sled.time_deleted IS NULL AND
                sled.sled_policy = 'in_service' AND
                sled.sled_state = 'active'
            GROUP BY sled.id
            HAVING
                COALESCE(SUM(CAST(sled_resource_vmm.hardware_threads AS INT8)), 0) + "
            ).param().sql(" <= sled.usable_hardware_threads AND
                COALESCE(SUM(CAST(sled_resource_vmm.rss_ram AS INT8)), 0) + "
            ).param().sql(" <= sled.usable_physical_ram AND
                COALESCE(SUM(CAST(sled_resource_vmm.reservoir_ram AS INT8)), 0) + "
            ).param().sql(" <= sled.reservoir_size
        ),
        our_aa_groups AS (
            SELECT group_id
            FROM anti_affinity_group_instance_membership
            WHERE instance_id = ").param().sql("
        ),
        other_aa_instances AS (
            SELECT anti_affinity_group_instance_membership.group_id,instance_id
            FROM anti_affinity_group_instance_membership
            JOIN our_aa_groups
            ON anti_affinity_group_instance_membership.group_id = our_aa_groups.group_id
            WHERE instance_id != ").param().sql("
        ),
        other_aa_instances_by_policy AS (
            SELECT policy,instance_id
            FROM other_aa_instances
            JOIN anti_affinity_group
            ON
                anti_affinity_group.id = other_aa_instances.group_id AND
                anti_affinity_group.failure_domain = 'sled'
            WHERE anti_affinity_group.time_deleted IS NULL
        ),
        aa_policy_and_sleds AS (
            SELECT DISTINCT policy,sled_id
            FROM other_aa_instances_by_policy
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = other_aa_instances_by_policy.instance_id
        ),
        our_a_groups AS (
            SELECT group_id
            FROM affinity_group_instance_membership
            WHERE instance_id = ").param().sql("
        ),
        other_a_instances AS (
            SELECT affinity_group_instance_membership.group_id,instance_id
            FROM affinity_group_instance_membership
            JOIN our_a_groups
            ON affinity_group_instance_membership.group_id = our_a_groups.group_id
            WHERE instance_id != ").param().sql("
        ),
        other_a_instances_by_policy AS (
            SELECT policy,instance_id
            FROM other_a_instances
            JOIN affinity_group
            ON
                affinity_group.id = other_a_instances.group_id AND
                affinity_group.failure_domain = 'sled'
            WHERE affinity_group.time_deleted IS NULL
        ),
        a_policy_and_sleds AS (
            SELECT DISTINCT policy,sled_id
            FROM other_a_instances_by_policy
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = other_a_instances_by_policy.instance_id
        ),
        sleds_with_space AS (
            SELECT
                s.sled_id,
                a.policy as a_policy,
                aa.policy as aa_policy
            FROM
                sled_targets s
                LEFT JOIN a_policy_and_sleds a ON a.sled_id = s.sled_id
                LEFT JOIN aa_policy_and_sleds aa ON aa.sled_id = s.sled_id
        ),
        sleds_without_space AS (
            SELECT
                sled_id,
                policy as a_policy,
                NULL as aa_policy
            FROM
                a_policy_and_sleds
            WHERE
                a_policy_and_sleds.sled_id NOT IN (SELECT sled_id from sleds_with_space)
        )
        SELECT sled_id, TRUE, a_policy, aa_policy FROM sleds_with_space
        UNION
        SELECT sled_id, FALSE, a_policy, aa_policy FROM sleds_without_space
    ")
    .bind::<sql_types::BigInt, _>(resources.hardware_threads)
    .bind::<sql_types::BigInt, _>(resources.rss_ram)
    .bind::<sql_types::BigInt, _>(resources.reservoir_ram)
    .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
    .query()
}

/// Inserts a sled_resource_vmm record into the database, if it is
/// a valid reservation.
pub fn sled_insert_resource_query(
    resource: &SledResourceVmm,
) -> TypedSqlQuery<(sql_types::Numeric,)> {
    QueryBuilder::new().sql("
        WITH sled_has_space AS (
            SELECT 1
            FROM sled
            LEFT JOIN sled_resource_vmm
            ON sled_resource_vmm.sled_id = sled.id
            WHERE
                sled.id = ").param().sql(" AND
                sled.time_deleted IS NULL AND
                sled.sled_policy = 'in_service' AND
                sled.sled_state = 'active'
            GROUP BY sled.id
            HAVING
                COALESCE(SUM(CAST(sled_resource_vmm.hardware_threads AS INT8)), 0) + "
            ).param().sql(" <= sled.usable_hardware_threads AND
                COALESCE(SUM(CAST(sled_resource_vmm.rss_ram AS INT8)), 0) + "
            ).param().sql(" <= sled.usable_physical_ram AND
                COALESCE(SUM(CAST(sled_resource_vmm.reservoir_ram AS INT8)), 0) + "
            ).param().sql(" <= sled.reservoir_size
        ),
        our_aa_groups AS (
            SELECT group_id
            FROM anti_affinity_group_instance_membership
            WHERE instance_id = ").param().sql("
        ),
        other_aa_instances AS (
            SELECT anti_affinity_group_instance_membership.group_id,instance_id
            FROM anti_affinity_group_instance_membership
            JOIN our_aa_groups
            ON anti_affinity_group_instance_membership.group_id = our_aa_groups.group_id
            WHERE instance_id != ").param().sql("
        ),
        banned_instances AS (
            SELECT instance_id
            FROM other_aa_instances
            JOIN anti_affinity_group
            ON
                anti_affinity_group.id = other_aa_instances.group_id AND
                anti_affinity_group.failure_domain = 'sled' AND
                anti_affinity_group.policy = 'fail'
            WHERE anti_affinity_group.time_deleted IS NULL
        ),
        banned_sleds AS (
            SELECT DISTINCT sled_id
            FROM banned_instances
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = banned_instances.instance_id
        ),
        our_a_groups AS (
            SELECT group_id
            FROM affinity_group_instance_membership
            WHERE instance_id = ").param().sql("
        ),
        other_a_instances AS (
            SELECT affinity_group_instance_membership.group_id,instance_id
            FROM affinity_group_instance_membership
            JOIN our_a_groups
            ON affinity_group_instance_membership.group_id = our_a_groups.group_id
            WHERE instance_id != ").param().sql("
        ),
        required_instances AS (
            SELECT policy,instance_id
            FROM other_a_instances
            JOIN affinity_group
            ON
                affinity_group.id = other_a_instances.group_id AND
                affinity_group.failure_domain = 'sled' AND
                affinity_group.policy = 'fail'
            WHERE affinity_group.time_deleted IS NULL
        ),
        required_sleds AS (
            SELECT DISTINCT sled_id
            FROM required_instances
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = required_instances.instance_id
        ),
        insert_valid AS (
            SELECT 1
            WHERE
                EXISTS(SELECT 1 FROM sled_has_space) AND
                NOT(EXISTS(SELECT 1 FROM banned_sleds WHERE sled_id = ").param().sql(")) AND
                (
                    EXISTS(SELECT 1 FROM required_sleds WHERE sled_id = ").param().sql(") OR
                    NOT EXISTS (SELECT 1 FROM required_sleds)
                )
        )
        INSERT INTO sled_resource_vmm (id, sled_id, hardware_threads, rss_ram, reservoir_ram, instance_id)
        SELECT
            ").param().sql(",
            ").param().sql(",
            ").param().sql(",
            ").param().sql(",
            ").param().sql(",
            ").param().sql("
        WHERE EXISTS(SELECT 1 FROM insert_valid)
    ")
    .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
    .bind::<sql_types::BigInt, _>(resource.resources.hardware_threads)
    .bind::<sql_types::BigInt, _>(resource.resources.rss_ram)
    .bind::<sql_types::BigInt, _>(resource.resources.reservoir_ram)
    .bind::<sql_types::Uuid, _>(resource.instance_id.unwrap().into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.instance_id.unwrap().into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.instance_id.unwrap().into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.instance_id.unwrap().into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.id.into_untyped_uuid())
    .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
    .bind::<sql_types::BigInt, _>(resource.resources.hardware_threads)
    .bind::<sql_types::BigInt, _>(resource.resources.rss_ram)
    .bind::<sql_types::BigInt, _>(resource.resources.reservoir_ram)
    .bind::<sql_types::Uuid, _>(resource.instance_id.unwrap().into_untyped_uuid())
    .query()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::model;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;

    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::PropolisUuid;
    use omicron_uuid_kinds::SledUuid;

    #[tokio::test]
    async fn expectorate_sled_find_targets_query() {
        let id = InstanceUuid::nil();
        let resources = Resources::new(
            0,
            model::ByteCount::from(external::ByteCount::from_gibibytes_u32(0)),
            model::ByteCount::from(external::ByteCount::from_gibibytes_u32(0)),
        );

        let query = sled_find_targets_query(id, &resources);
        expectorate_query_contents(
            &query,
            "tests/output/sled_find_targets_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_sled_find_targets_query() {
        let logctx = dev::test_setup_log("explain_sled_find_targets_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let resources = Resources::new(
            0,
            model::ByteCount::from(external::ByteCount::from_gibibytes_u32(0)),
            model::ByteCount::from(external::ByteCount::from_gibibytes_u32(0)),
        );

        let query = sled_find_targets_query(id, &resources);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_sled_insert_resource_query() {
        let resource = SledResourceVmm::new(
            PropolisUuid::nil(),
            InstanceUuid::nil(),
            SledUuid::nil(),
            Resources::new(
                0,
                model::ByteCount::from(
                    external::ByteCount::from_gibibytes_u32(0),
                ),
                model::ByteCount::from(
                    external::ByteCount::from_gibibytes_u32(0),
                ),
            ),
        );

        let query = sled_insert_resource_query(&resource);
        expectorate_query_contents(
            &query,
            "tests/output/sled_insert_resource_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_sled_insert_resource_query() {
        let logctx = dev::test_setup_log("explain_sled_insert_resource_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let resource = SledResourceVmm::new(
            PropolisUuid::nil(),
            InstanceUuid::nil(),
            SledUuid::nil(),
            Resources::new(
                0,
                model::ByteCount::from(
                    external::ByteCount::from_gibibytes_u32(0),
                ),
                model::ByteCount::from(
                    external::ByteCount::from_gibibytes_u32(0),
                ),
            ),
        );

        let query = sled_insert_resource_query(&resource);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // NOTE: There are some tests you might wanna nab from "affinity.rs"
}
