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
use nonempty::NonEmpty;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use uuid::Uuid;

#[derive(Clone)]
pub struct LocalStorageAllocation {
    pub disk_id: Uuid,
    pub local_storage_dataset_allocation_id: DatasetUuid,
    pub required_dataset_size: i64,
    pub local_storage_dataset_id: DatasetUuid,
    pub pool_id: ZpoolUuid,
    pub sled_id: SledUuid,
}

#[derive(Clone)]
pub enum LocalStorageAllocationRequired {
    No,
    Yes { allocations: NonEmpty<LocalStorageAllocation> },
}

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

    if sled_families.is_some() {
        query.sql(" AND sled.cpu_family = ANY (");
        query.param();
        query.sql(") ");
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
        query.bind::<sql_types::Array<SledCpuFamilyEnum>, _>(families.to_vec());
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

/// Attempts to:
///
/// 1. Insert a sled_resource_vmm record, if it is still a valid reservation
///
/// 2. Optionally perform local storage allocation by updating local storage
///    disk records, if those are still valid with respect to the amount of
///    available zpool space.
pub fn sled_insert_resource_query(
    resource: &SledResourceVmm,
    local_storage_allocation_required: &LocalStorageAllocationRequired,
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
        ),")
        .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
        .bind::<sql_types::BigInt, _>(resource.resources.hardware_threads)
        .bind::<sql_types::BigInt, _>(resource.resources.rss_ram)
        .bind::<sql_types::BigInt, _>(resource.resources.reservoir_ram);

    // "our_aa_groups": All the anti-affinity group_ids to which our instance belongs.
    subquery_our_aa_groups(&mut query);
    query.bind::<sql_types::Uuid, _>(
        resource.instance_id.unwrap().into_untyped_uuid(),
    );

    // "other_aa_instances": All the group_id,instance_ids of instances (other
    // than our own) belonging to "our_aa_groups".
    subquery_other_aa_instances(&mut query);
    query.bind::<sql_types::Uuid, _>(
        resource.instance_id.unwrap().into_untyped_uuid(),
    );

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
    query.bind::<sql_types::Uuid, _>(
        resource.instance_id.unwrap().into_untyped_uuid(),
    );

    // "other_a_instances": All the group_id,instance_ids of instances (other
    // than our own) belonging to "our_a_instances").
    subquery_other_a_instances(&mut query);
    query.bind::<sql_types::Uuid, _>(
        resource.instance_id.unwrap().into_untyped_uuid(),
    );

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

    match local_storage_allocation_required {
        LocalStorageAllocationRequired::No => {}

        LocalStorageAllocationRequired::Yes { allocations } => {
            // Update each local storage disk's allocation id

            query.sql(" UPDATED_LOCAL_STORAGE_DISK_RECORDS AS (");
            query.sql("  UPDATE disk_type_local_storage ");

            query.sql(
                "    SET local_storage_dataset_allocation_id = CASE disk_id ",
            );

            for allocation in allocations {
                let LocalStorageAllocation {
                    disk_id,
                    local_storage_dataset_allocation_id,
                    ..
                } = allocation;

                query.sql("WHEN ");
                query.param().bind::<sql_types::Uuid, _>(*disk_id);
                query.sql(" THEN ");
                query.param().bind::<sql_types::Uuid, _>(
                    local_storage_dataset_allocation_id.into_untyped_uuid(),
                );
                query.sql(" ");
            }

            query.sql("    END");

            query.sql("  WHERE disk_id in ( ");

            for (index, allocation) in allocations.iter().enumerate() {
                let LocalStorageAllocation { disk_id, .. } = allocation;

                query.param().bind::<sql_types::Uuid, _>(*disk_id);

                if index != (allocations.len() - 1) {
                    query.sql(",");
                }
            }

            query.sql("  )");

            query.sql("  RETURNING *");
            query.sql(" ), ");

            // Create new local storage dataset allocation records.

            query.sql(" NEW_LOCAL_STORAGE_ALLOCATION_RECORDS AS (");
            query.sql("  INSERT INTO local_storage_dataset_allocation VALUES ");

            for (index, allocation) in allocations.iter().enumerate() {
                let LocalStorageAllocation {
                    local_storage_dataset_allocation_id,
                    local_storage_dataset_id,
                    pool_id,
                    sled_id,
                    required_dataset_size,
                    ..
                } = allocation;

                query.sql("(");

                query.param().bind::<sql_types::Uuid, _>(
                    local_storage_dataset_allocation_id.into_untyped_uuid(),
                );
                query.sql(",");

                query.sql("NOW(),");
                query.sql("NULL,");

                query.param().bind::<sql_types::Uuid, _>(
                    local_storage_dataset_id.into_untyped_uuid(),
                );
                query.sql(",");

                query
                    .param()
                    .bind::<sql_types::Uuid, _>(pool_id.into_untyped_uuid());
                query.sql(",");

                query
                    .param()
                    .bind::<sql_types::Uuid, _>(sled_id.into_untyped_uuid());
                query.sql(",");

                query
                    .param()
                    .bind::<sql_types::BigInt, _>(*required_dataset_size);

                query.sql(")");

                if index != (allocations.len() - 1) {
                    query.sql(",");
                }
            }

            query.sql(" RETURNING *), ");

            // Update the rendezvous_local_storage_dataset table's size_used
            // column by adding these new rows

            query.sql("UPDATE_RENDEZVOUS_TABLES as (");
            query.sql("
            UPDATE rendezvous_local_storage_dataset
            SET size_used = size_used + NEW_LOCAL_STORAGE_ALLOCATION_RECORDS.dataset_size
            FROM NEW_LOCAL_STORAGE_ALLOCATION_RECORDS
            WHERE
              NEW_LOCAL_STORAGE_ALLOCATION_RECORDS.local_storage_dataset_id = rendezvous_local_storage_dataset.id AND
              rendezvous_local_storage_dataset.time_tombstoned is null and
              rendezvous_local_storage_dataset.no_provision = FALSE
            RETURNING *");
            query.sql("), ");
        }
    }

    // The insert is only valid if:
    //
    // - The sled still has space for our instance
    // - The sled is not banned (due to anti-affinity rules)
    // - If the sled is required (due to affinity rules) we're selecting it
    // - If there are requested local storage allocations:
    //   - they all fit on the target zpools: combine the new local storage
    //     allocation records with the existing ones and test each zpool.
    //   - the rendezvous local storage dataset is not tombstoned or marked
    //     no_provision

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
        .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
        .sql(
            ")) AND
                (
                    EXISTS(SELECT 1 FROM required_sleds WHERE sled_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(resource.sled_id.into_untyped_uuid())
        .sql(
            ") OR
                    NOT EXISTS (SELECT 1 FROM required_sleds)
                )",
        );

    match local_storage_allocation_required {
        LocalStorageAllocationRequired::No => {}

        LocalStorageAllocationRequired::Yes { allocations } => {
            query.sql("AND (");

            for (index, allocation) in allocations.iter().enumerate() {
                query.sql("(");

                // First, make sure that the additional usage fits in the zpool

                query.sql("(");

                // Add up crucible and local dataset usage, plus the altered
                // local_storage_dataset_allocation records

                query.sql("(");

                query.sql("SELECT
                  SUM(
                    crucible_dataset.size_used +
                    rendezvous_local_storage_dataset.size_used +
                    NEW_LOCAL_STORAGE_ALLOCATION_RECORDS.dataset_size
                  )
                FROM
                  crucible_dataset
                JOIN
                  rendezvous_local_storage_dataset
                ON
                  crucible_dataset.pool_id = rendezvous_local_storage_dataset.pool_id
                JOIN
                  NEW_LOCAL_STORAGE_ALLOCATION_RECORDS
                ON
                  crucible_dataset.pool_id = NEW_LOCAL_STORAGE_ALLOCATION_RECORDS.pool_id
                WHERE
                  (crucible_dataset.size_used IS NOT NULL) AND
                  (crucible_dataset.time_deleted IS NULL) AND
                  (rendezvous_local_storage_dataset.time_tombstoned IS NULL) AND
                  (rendezvous_local_storage_dataset.no_provision IS FALSE) AND
                  (crucible_dataset.pool_id = ")
                .param()
                .bind::<sql_types::Uuid, _>(allocation.pool_id.into_untyped_uuid())
                .sql(")
                GROUP BY
                  crucible_dataset.pool_id");

                query.sql(") < (");

                // and compare that to the zpool's available space (minus the
                // control plane storage buffer) as reported by the latest
                // inventory collection.

                query
                    .sql(
                        "(SELECT
                  total_size
                 FROM
                  inv_zpool
                 WHERE
                  inv_zpool.id = ",
                    )
                    .param()
                    .bind::<sql_types::Uuid, _>(
                        allocation.pool_id.into_untyped_uuid(),
                    )
                    .sql(" ORDER BY inv_zpool.time_collected DESC LIMIT 1)");

                query.sql(" - ");

                query
                    .sql(
                        "(SELECT
                  control_plane_storage_buffer
                 FROM
                  zpool
                 WHERE id = ",
                    )
                    .param()
                    .bind::<sql_types::Uuid, _>(
                        allocation.pool_id.into_untyped_uuid(),
                    )
                    .sql(")");

                query.sql(")");

                query.sql(")");

                query.sql(") AND ");

                // and, the zpool must be available

                query.sql("(");

                query
                    .sql(
                        "SELECT
                  sled.sled_policy = 'in_service'
                  AND sled.sled_state = 'active'
                  AND physical_disk.disk_policy = 'in_service'
                  AND physical_disk.disk_state = 'active'
                FROM
                  zpool
                JOIN
                  sled ON (zpool.sled_id = sled.id)
                JOIN
                  physical_disk ON (zpool.physical_disk_id = physical_disk.id)
                WHERE
                  zpool.id = ",
                    )
                    .param()
                    .bind::<sql_types::Uuid, _>(
                        allocation.pool_id.into_untyped_uuid(),
                    );

                query.sql(") AND ");

                // and the rendezvous local dataset must be available

                query
                    .sql(
                        "(
                     SELECT time_tombstoned IS NULL AND no_provision IS FALSE
                     FROM rendezvous_local_storage_dataset
                     WHERE rendezvous_local_storage_dataset.id = ",
                    )
                    .param()
                    .bind::<sql_types::Uuid, _>(
                        allocation.local_storage_dataset_id.into_untyped_uuid(),
                    );

                query.sql(")");

                if index != (allocations.len() - 1) {
                    query.sql(" AND ");
                }
            }

            query.sql(")");
        }
    }

    query.sql(")");

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

    use nonempty::nonempty;

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

        // with no local storage

        let query = sled_insert_resource_query(
            &resource,
            &LocalStorageAllocationRequired::No,
        );

        expectorate_query_contents(
            &query,
            "tests/output/sled_insert_resource_query.sql",
        )
        .await;

        let query = sled_insert_resource_query(
            &resource,
            &LocalStorageAllocationRequired::Yes {
                allocations: nonempty![LocalStorageAllocation {
                    disk_id: Uuid::nil(),
                    local_storage_dataset_allocation_id: DatasetUuid::nil(),
                    required_dataset_size: 128 * 1024 * 1024 * 1024,
                    local_storage_dataset_id: DatasetUuid::nil(),
                    pool_id: ZpoolUuid::nil(),
                    sled_id: SledUuid::nil(),
                }],
            },
        );

        expectorate_query_contents(
            &query,
            "tests/output/sled_insert_resource_query_with_local_storage.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_sled_insert_resource_query() {
        let logctx = dev::test_setup_log("explain_sled_insert_resource_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        // with no local storage

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

        let query = sled_insert_resource_query(
            &resource,
            &LocalStorageAllocationRequired::No,
        );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        // with local storage

        let query = sled_insert_resource_query(
            &resource,
            &LocalStorageAllocationRequired::Yes {
                allocations: nonempty![LocalStorageAllocation {
                    disk_id: Uuid::nil(),
                    local_storage_dataset_allocation_id: DatasetUuid::nil(),
                    required_dataset_size: 128 * 1024 * 1024 * 1024,
                    local_storage_dataset_id: DatasetUuid::nil(),
                    pool_id: ZpoolUuid::nil(),
                    sled_id: SledUuid::nil(),
                }],
            },
        );

        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        let query = sled_insert_resource_query(
            &resource,
            &LocalStorageAllocationRequired::Yes {
                allocations: nonempty![
                    LocalStorageAllocation {
                        disk_id: Uuid::nil(),
                        local_storage_dataset_allocation_id: DatasetUuid::nil(),
                        required_dataset_size: 128 * 1024 * 1024 * 1024,
                        local_storage_dataset_id: DatasetUuid::nil(),
                        pool_id: ZpoolUuid::nil(),
                        sled_id: SledUuid::nil(),
                    },
                    LocalStorageAllocation {
                        disk_id: Uuid::nil(),
                        local_storage_dataset_allocation_id: DatasetUuid::nil(),
                        required_dataset_size: 256 * 1024 * 1024 * 1024,
                        local_storage_dataset_id: DatasetUuid::nil(),
                        pool_id: ZpoolUuid::nil(),
                        sled_id: SledUuid::nil(),
                    },
                ],
            },
        );

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
