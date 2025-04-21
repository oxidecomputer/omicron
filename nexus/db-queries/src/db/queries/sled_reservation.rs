// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for affinity groups

use crate::db::model::Resources;
use crate::db::model::SledResourceVmm;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use diesel::sql_types;
use nexus_db_model::SledCpuFamily;
use nexus_db_schema::enums::AffinityPolicyEnum;
use nexus_db_schema::enums::SledCpuFamilyEnum;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

fn subquery_our_aa_groups(query: &mut QueryBuilder) {
    query
        .sql(
            "
        our_aa_groups AS (
            SELECT group_id
            FROM anti_affinity_group_instance_membership
            WHERE instance_id = ",
        )
        .param()
        .sql(
            "
        ),",
        );
}

fn subquery_other_aa_instances(query: &mut QueryBuilder) {
    query.sql("
        other_aa_instances AS (
            SELECT anti_affinity_group_instance_membership.group_id,instance_id
            FROM anti_affinity_group_instance_membership
            JOIN our_aa_groups
            ON anti_affinity_group_instance_membership.group_id = our_aa_groups.group_id
            WHERE instance_id != ").param().sql("
        ),");
}

fn subquery_our_a_groups(query: &mut QueryBuilder) {
    query
        .sql(
            "
        our_a_groups AS (
            SELECT group_id
            FROM affinity_group_instance_membership
            WHERE instance_id = ",
        )
        .param()
        .sql(
            "
        ),",
        );
}

fn subquery_other_a_instances(query: &mut QueryBuilder) {
    query.sql("
        other_a_instances AS (
            SELECT affinity_group_instance_membership.group_id,instance_id
            FROM affinity_group_instance_membership
            JOIN our_a_groups
            ON affinity_group_instance_membership.group_id = our_a_groups.group_id
            WHERE instance_id != ").param().sql("
        ),");
}

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
    sled_families: Option<&[SledCpuFamily]>,
) -> TypedSqlQuery<(
    sql_types::Uuid,
    sql_types::Bool,
    sql_types::Nullable<AffinityPolicyEnum>,
    sql_types::Nullable<AffinityPolicyEnum>,
)> {
    let mut query = QueryBuilder::new();
    query.sql(
        "
        WITH sled_targets AS (
            SELECT sled.id as sled_id
            FROM sled
            LEFT JOIN sled_resource_vmm
            ON sled_resource_vmm.sled_id = sled.id
            WHERE
                sled.time_deleted IS NULL AND
                sled.sled_policy = 'in_service' AND
                sled.sled_state = 'active'
        ",
    );

    // TODO(gjc): eww. the correct way to do this is to write this as
    //
    // "AND sled.cpu_family = ANY ("
    //
    // and then just have one `param` which can be bound to a
    // `sql_types::Array<SledCpuFamilyEnum>`
    if let Some(families) = sled_families {
        query.sql(" AND sled.cpu_family IN (");
        for i in 0..families.len() {
            if i > 0 {
                query.sql(", ");
            }
            query.param();
        }
        query.sql(")");
    }

    query.sql("GROUP BY sled.id
            HAVING
                COALESCE(SUM(CAST(sled_resource_vmm.hardware_threads AS INT8)), 0) + "
            ).param().sql(" <= sled.usable_hardware_threads AND
                COALESCE(SUM(CAST(sled_resource_vmm.rss_ram AS INT8)), 0) + "
            ).param().sql(" <= sled.usable_physical_ram AND
                COALESCE(SUM(CAST(sled_resource_vmm.reservoir_ram AS INT8)), 0) + "
            ).param().sql(" <= sled.reservoir_size
        ),");

    // "our_aa_groups": All the anti-affinity group_ids to which our instance belongs.
    subquery_our_aa_groups(&mut query);
    // "other_aa_instances": All the group_id,instance_ids of instances (other
    // than our own) belonging to "our_aa_groups".
    subquery_other_aa_instances(&mut query);

    // Gather together all anti-affinity instances in the sled failure domain
    // by policy. This will let us evaluate whether or not it's essential to
    // fulfill the placement request.
    query.sql(
        "
        other_aa_instances_by_policy AS (
            SELECT policy,instance_id
            FROM other_aa_instances
            JOIN anti_affinity_group
            ON
                anti_affinity_group.id = other_aa_instances.group_id AND
                anti_affinity_group.failure_domain = 'sled'
            WHERE anti_affinity_group.time_deleted IS NULL
        ),",
    );

    // Transform "other_aa_instances_by_policy" into a sled-specific view,
    // which is what we actually care about when making placement decisions.
    query.sql("
        aa_policy_and_sleds AS (
            SELECT DISTINCT policy,sled_id
            FROM other_aa_instances_by_policy
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = other_aa_instances_by_policy.instance_id
        ),");

    // "our_a_groups": All the affinity group_ids to which our instance belongs.
    subquery_our_a_groups(&mut query);
    // "other_a_instances": All the group_id,instance_ids of instances (other
    // than our own) belonging to "our_a_instances").
    subquery_other_a_instances(&mut query);

    // Gather together all affinity instances in the sled failure domain
    // by policy. This will let us evaluate whether or not it's essential to
    // fulfill the placement request.
    query.sql(
        "
        other_a_instances_by_policy AS (
            SELECT policy,instance_id
            FROM other_a_instances
            JOIN affinity_group
            ON
                affinity_group.id = other_a_instances.group_id AND
                affinity_group.failure_domain = 'sled'
            WHERE affinity_group.time_deleted IS NULL
        ),",
    );

    // Transform "other_a_instances_by_policy" into a sled-specific view,
    // which is what we actually care about when making placement decisions.
    query.sql("
        a_policy_and_sleds AS (
            SELECT DISTINCT policy,sled_id
            FROM other_a_instances_by_policy
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = other_a_instances_by_policy.instance_id
        ),");

    // Aggregate "sleds that have space", and provide context on the
    // affinity/anti-affinity policies which are relevant to those sleds.
    query.sql(
        "
        sleds_with_space AS (
            SELECT
                s.sled_id,
                a.policy as a_policy,
                aa.policy as aa_policy
            FROM
                sled_targets s
                LEFT JOIN a_policy_and_sleds a ON a.sled_id = s.sled_id
                LEFT JOIN aa_policy_and_sleds aa ON aa.sled_id = s.sled_id
        ),",
    );

    // Also return affinity/anti-affinity policy information for sleds that do
    // NOT have space. This is critical for cases where affinity rules are
    // strict, but we might not be able to fit on a desired sled.
    query.sql("
        sleds_without_space AS (
            SELECT
                sled_id,
                policy as a_policy,
                NULL as aa_policy
            FROM
                a_policy_and_sleds
            WHERE
                a_policy_and_sleds.sled_id NOT IN (SELECT sled_id from sleds_with_space)
        )");

    // Return all known information about relevant sleds, including:
    //
    // - Sled UUID
    // - Whether or not there is space on the sled for the specified instance
    // - What affinity/anti-affinity policies apply
    query.sql(
        "
        SELECT sled_id, TRUE, a_policy, aa_policy FROM sleds_with_space
        UNION
        SELECT sled_id, FALSE, a_policy, aa_policy FROM sleds_without_space
    ",
    );

    if let Some(families) = sled_families {
        for f in families {
            query.bind::<SledCpuFamilyEnum, _>(*f);
        }
    }

    query
        .bind::<sql_types::BigInt, _>(resources.hardware_threads)
        .bind::<sql_types::BigInt, _>(resources.rss_ram)
        .bind::<sql_types::BigInt, _>(resources.reservoir_ram)
        .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
        .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
        .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid())
        .bind::<sql_types::Uuid, _>(instance_id.into_untyped_uuid());

    query.query()
}

/// Inserts a sled_resource_vmm record into the database, if it is
/// a valid reservation.
pub fn sled_insert_resource_query(
    resource: &SledResourceVmm,
) -> TypedSqlQuery<(sql_types::Numeric,)> {
    let mut query = QueryBuilder::new();

    // This is similar to the "sled_targets" subquery in
    // "sled_find_targets_query", but it's scoped to the single sled we're
    // trying to select.
    query.sql("
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
        ),");

    // "our_aa_groups": All the anti-affinity group_ids to which our instance belongs.
    subquery_our_aa_groups(&mut query);
    // "other_aa_instances": All the group_id,instance_ids of instances (other
    // than our own) belonging to "our_aa_groups".
    subquery_other_aa_instances(&mut query);

    // Find instances with a strict anti-affinity policy in the sled failure
    // domain. We must ensure we do not co-locate with these instances.
    query.sql(
        "
        banned_instances AS (
            SELECT instance_id
            FROM other_aa_instances
            JOIN anti_affinity_group
            ON
                anti_affinity_group.id = other_aa_instances.group_id AND
                anti_affinity_group.failure_domain = 'sled' AND
                anti_affinity_group.policy = 'fail'
            WHERE anti_affinity_group.time_deleted IS NULL
        ),",
    );
    query.sql(
        "
        banned_sleds AS (
            SELECT DISTINCT sled_id
            FROM banned_instances
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = banned_instances.instance_id
        ),",
    );

    // "our_a_groups": All the affinity group_ids to which our instance belongs.
    subquery_our_a_groups(&mut query);
    // "other_a_instances": All the group_id,instance_ids of instances (other
    // than our own) belonging to "our_a_instances").
    subquery_other_a_instances(&mut query);

    // Find instances with a strict affinity policy in the sled failure
    // domain. We must ensure we co-locate with these instances.
    query.sql(
        "
        required_instances AS (
            SELECT policy,instance_id
            FROM other_a_instances
            JOIN affinity_group
            ON
                affinity_group.id = other_a_instances.group_id AND
                affinity_group.failure_domain = 'sled' AND
                affinity_group.policy = 'fail'
            WHERE affinity_group.time_deleted IS NULL
        ),",
    );
    query.sql(
        "
        required_sleds AS (
            SELECT DISTINCT sled_id
            FROM required_instances
            JOIN sled_resource_vmm
            ON
                sled_resource_vmm.instance_id = required_instances.instance_id
        ),",
    );

    // The insert is only valid if:
    //
    // - The sled still has space for our isntance
    // - The sled is not banned (due to anti-affinity rules)
    // - If the sled is required (due to affinity rules) we're selecting it
    query
        .sql(
            "
        insert_valid AS (
            SELECT 1
            WHERE
                EXISTS(SELECT 1 FROM sled_has_space) AND
                NOT(EXISTS(SELECT 1 FROM banned_sleds WHERE sled_id = ",
        )
        .param()
        .sql(
            ")) AND
                (
                    EXISTS(SELECT 1 FROM required_sleds WHERE sled_id = ",
        )
        .param()
        .sql(
            ") OR
                    NOT EXISTS (SELECT 1 FROM required_sleds)
                )
        )",
        );

    // Finally, perform the INSERT if it's still valid.
    query.sql("
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
    .bind::<sql_types::Uuid, _>(resource.instance_id.unwrap().into_untyped_uuid());

    query.query()
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

        let query = sled_find_targets_query(id, &resources, None);
        expectorate_query_contents(
            &query,
            "tests/output/sled_find_targets_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_sled_find_targets_query_with_cpu() {
        let id = InstanceUuid::nil();
        let resources = Resources::new(
            0,
            model::ByteCount::from(external::ByteCount::from_gibibytes_u32(0)),
            model::ByteCount::from(external::ByteCount::from_gibibytes_u32(0)),
        );

        let query = sled_find_targets_query(
            id,
            &resources,
            Some(&[SledCpuFamily::AmdMilan]),
        );
        expectorate_query_contents(
            &query,
            "tests/output/sled_find_targets_query_with_cpu.sql",
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

        let query = sled_find_targets_query(
            id,
            &resources,
            Some(&[SledCpuFamily::AmdMilan]),
        );

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

    // NOTE: These queries are more exhaustively tested in db/datastore/sled.rs,
    // where they are used.
}
