// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for operating on external subnet pools.

use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::SelectableSql;
use crate::db::raw_query_builder::TypedSqlQuery;
use diesel::sql_types;
use nexus_db_model::ExternalSubnet;
use nexus_db_model::ExternalSubnetIdentity;
use nexus_db_model::Generation;
use nexus_db_model::IpNet;
use nexus_db_model::IpVersion;
use nexus_db_model::SubnetPoolMember;
use nexus_db_model::SubnetPoolSiloLink;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_schema::enums::IpVersionEnum;
use nexus_types::external_api::params::ExternalSubnetCreate;
use nexus_types::external_api::params::ExternalSubnetSelector;
use nexus_types::external_api::params::PoolSelector;
use omicron_common::api::external::Error;
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::ExternalSubnetUuid;
use omicron_uuid_kinds::SubnetPoolUuid;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use uuid::Uuid;

/// Query to insert a member in a Subnet Pool, which checks for overlapping
/// subnets.
pub fn insert_subnet_pool_member_query(
    member: &SubnetPoolMember,
) -> Result<TypedSqlQuery<SelectableSql<SubnetPoolMember>>, Error> {
    let mut builder = QueryBuilder::new();
    let (first_address, last_address) = network_edges(member.subnet)?;

    // We have indexes on the first address and last address, and we need to
    // detect any overlap. We can do that a few separate clauses, which check
    // for
    //
    // - an existing subnet that contains the candidate first address
    // - an existing subnet that contains the candidate last address
    // - a candidate subnet that contains any existing first address
    // - a candidate subnet that contains any existing last address
    //
    // Expressing these as a bunch of ANDs is important. They could be done as
    // ORs, but that implies a full table scan.
    builder.sql("\
        WITH candidate_contains_existing_first AS (\
            SELECT 1 \
            FROM subnet_pool_member \
            WHERE \
                first_address BETWEEN "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" AND ")
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" AND time_deleted IS NULL LIMIT 1), \
        candidate_contains_existing_last AS (\
            SELECT 1 \
            FROM subnet_pool_member \
            WHERE \
                last_address BETWEEN "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" AND ")
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" AND time_deleted IS NULL LIMIT 1), \
        existing_contains_candidate_first AS (\
            SELECT 1 \
            FROM subnet_pool_member \
            WHERE "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" BETWEEN first_address AND last_address AND time_deleted IS NULL LIMIT 1), \
        existing_contains_candidate_last AS (\
            SELECT 1 \
            FROM subnet_pool_member \
            WHERE "
        )
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" BETWEEN first_address AND last_address AND time_deleted IS NULL LIMIT 1), \
        candidate_does_not_overlap AS (\
                SELECT 1 \
                WHERE NOT EXISTS(SELECT 1 FROM candidate_contains_existing_first) \
                  AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_last) \
                  AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_first) \
                  AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_last)\
            ), new_record_values AS (SELECT ")
        .param()
        .bind::<sql_types::Uuid, _>(member.id)
        .sql(" AS id, ")
        .param()
        .bind::<sql_types::Timestamptz, _>(member.time_created)
        .sql(" AS time_created, ")
        .param()
        .bind::<sql_types::Timestamptz, _>(member.time_modified)
        .sql(" AS time_modified, ")
        .param()
        .bind::<sql_types::Uuid, _>(member.subnet_pool_id)
        .sql(" AS subnet_pool_id, ")
        .param()
        .bind::<sql_types::Inet, _>(member.subnet)
        .sql(" AS subnet, ")
        .param()
        .bind::<sql_types::Int2, _>(member.min_prefix_length())
        .sql(" AS min_prefix_length, ")
        .param()
        .bind::<sql_types::Int2, _>(member.max_prefix_length())
        .sql(" AS max_prefix_length, ")
        .param()
        .bind::<sql_types::BigInt, _>(member.rcgen())
        .sql("), \
            updated_pool AS (\
                UPDATE subnet_pool \
                SET \
                    time_modified = NOW(), \
                    rcgen = rcgen + 1 \
                WHERE \
                    id = "
        )
        .param()
        .bind::<sql_types::Uuid, _>(member.subnet_pool_id)
        .sql(" \
                    AND EXISTS(SELECT 1 FROM candidate_does_not_overlap) \
                    AND time_deleted IS NULL \
                RETURNING rcgen\
            ) \
            INSERT INTO subnet_pool_member (\
                id, \
                time_created, \
                time_modified, \
                subnet_pool_id, \
                subnet, \
                min_prefix_length, \
                max_prefix_length, \
                rcgen\
            ) \
            SELECT * \
            FROM new_record_values \
            WHERE EXISTS(SELECT 1 FROM candidate_does_not_overlap) \
            RETURNING \
                id, \
                time_created, \
                time_modified, \
                time_deleted, \
                subnet_pool_id, \
                subnet, \
                min_prefix_length, \
                max_prefix_length, \
                rcgen\
            "
        );
    Ok(builder.query())
}

// Return the first / last addresses in the network.
//
// Note that this returns model network types, so we can insert them directly in
// the above query.
fn network_edges(subnet: IpNet) -> Result<(IpNet, IpNet), Error> {
    let make_error = |e: oxnet::IpNetPrefixError| -> Error {
        Error::internal_error(e.to_string().as_str())
    };
    match subnet {
        IpNet::V4(v4) => {
            let net = Ipv4Net::from(v4);
            let width = net.width();
            let first =
                Ipv4Net::new(net.first_addr(), width).map_err(make_error)?;
            Ipv4Net::new(net.last_addr(), width)
                .map(|last| (first.into(), last.into()))
                .map_err(make_error)
        }
        IpNet::V6(v6) => {
            let net = Ipv6Net::from(v6);
            let width = net.width();
            let first =
                Ipv6Net::new(net.first_addr(), width).map_err(make_error)?;
            Ipv6Net::new(net.last_addr(), width)
                .map(|last| (first.into(), last.into()))
                .map_err(make_error)
        }
    }
}

/// Query to link a Subnet Pool to a Silo, checking that both exist.
///
/// This query will fail with violations of the non-NULL constraint on the Pool
/// or Silo ID columns, if those respective objects have been deleted when the
/// query is executed.
pub fn link_subnet_pool_to_silo_query(
    pool_id: SubnetPoolUuid,
    silo_id: Uuid,
    is_default: bool,
) -> TypedSqlQuery<SelectableSql<SubnetPoolSiloLink>> {
    let mut builder = QueryBuilder::new();
    builder.sql("\
    WITH subnet_pool AS (\
        SELECT id, ip_version \
        FROM subnet_pool \
        WHERE id = "
    )
        .param()
        .bind::<sql_types::Uuid, _>(to_db_typed_uuid(pool_id))
    .sql("\
        AND time_deleted IS NULL\
    ), silo AS (\
        SELECT id \
        FROM silo \
        WHERE id = "
    )
        .param()
        .bind::<sql_types::Uuid, _>(silo_id)
    .sql("\
        AND time_deleted IS NULL), \
    updated_pool AS (\
        UPDATE subnet_pool
        SET \
            time_modified = NOW(), \
            rcgen = rcgen + 1 \
        WHERE \
            id = "
    )
    .param()
    .bind::<sql_types::Uuid, _>(to_db_typed_uuid(pool_id))
    .sql("\
        AND time_deleted IS NULL \
        RETURNING 1 \
    )
    INSERT \
    INTO \
        subnet_pool_silo_link (subnet_pool_id, silo_id, ip_version, is_default) \
    SELECT \
        sp.id, \
        s.id, \
        sp.ip_version, "
    )
    .param()
    .bind::<sql_types::Bool, _>(is_default)
    .sql("\
    FROM \
        (SELECT 1) AS dummy \
        LEFT JOIN subnet_pool AS sp ON TRUE \
        LEFT JOIN silo AS s ON TRUE \
    RETURNING *"
    );
    builder.query()
}

/// Unlink a Silo and Subnet Pool.
///
/// This deletes the link between these objects, conditional on the generation
/// of the Subnet Pool not having changed. We increment that whenever we create
/// either a new pool member or a new External Subnet in the given Silo.
pub fn unlink_subnet_pool_from_silo_query(
    pool_id: SubnetPoolUuid,
    silo_id: Uuid,
    rcgen: Generation,
) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    let pool_id = to_db_typed_uuid(pool_id);
    builder
        .sql(
            "\
    WITH subnet_pool AS (\
        SELECT 1 \
        FROM subnet_pool \
        WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(pool_id)
        .sql(" AND time_deleted IS NULL AND rcgen = ")
        .param()
        .bind::<sql_types::BigInt, _>(rcgen)
        .sql(
            ") \
    DELETE FROM \
    subnet_pool_silo_link \
    WHERE \
        subnet_pool_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(pool_id)
        .sql(" AND silo_id = ")
        .param()
        .bind::<sql_types::Uuid, _>(silo_id)
        .sql(" AND EXISTS(SELECT 1 FROM subnet_pool)");
    builder.query()
}

/// Query to insert an External Subnet.
pub fn insert_external_subnet_query(
    silo_id: &Uuid,
    project_id: &Uuid,
    params: &ExternalSubnetCreate,
) -> TypedSqlQuery<SelectableSql<ExternalSubnet>> {
    let ExternalSubnetCreate { identity, subnet } = params;
    let identity = ExternalSubnetIdentity::new(
        ExternalSubnetUuid::new_v4(),
        identity.clone(),
    );
    // - CTE to select pool
    //  - use pool by name or id if given, checking it's linked to the silo
    //  - use default for silo / version, if not given, or fail.
    //  - use pool containing explicit subnet, checking it's linked, or fail
    // - CTE to check that the silo is not deleted
    // - CTE to check that the subnet pool is not deleted
    // - CTE to check that the project is not deleted
    // - CTE to select the subnet itself
    //  - auto:
    //      - take "next", also verifying prefix limits.
    //  - explicit:
    //      - verify it's within the prefix-length limits of the pool member
    //      - verify it doesn't overlap.
    // - CTE to select subnet record itself
    //  - use explicit subnet if provided and other parts don't fail
    //  - use next in pool, if we're doing auto
    //  - either way, use the other data (metadata, mostly)
    // - update pool member's rcgen, also need to update the _parent_ pool's gen
    // - insert actual record
    let mut builder = QueryBuilder::new();
    builder.sql("WITH ");
    match subnet {
        ExternalSubnetSelector::Explicit { subnet } => {
            push_cte_to_select_pool_from_explicit_subnet(
                &mut builder,
                *silo_id,
                *subnet,
            );
        }
        ExternalSubnetSelector::Auto {
            pool: PoolSelector::Explicit { pool },
            prefix,
        } => {
            push_cte_to_select_pool_id(&mut builder, pool);
            builder.sql(", ");
            push_cte_to_ensure_pool_is_linked_to_silo(&mut builder, silo_id);
            builder.sql(", ");
            push_cte_to_select_next_subnet_from_pool(&mut builder, *prefix);
        }
        ExternalSubnetSelector::Auto {
            pool: PoolSelector::Auto { ip_version },
            prefix,
        } => {
            // We are given only the IP version.
            //
            // Select the default subnet pool of this version linked to the
            // current silo, if it exists. If not, fail. If it does exist,
            // proceed like the previous match arm, selecting the next subnet
            // pool member that has an available subnet of the requested prefix
            // size or greater.
            todo!();
        }
    }
    builder.sql(", ");
    push_cte_to_check_for_deleted_project(&mut builder, project_id);
    builder.sql(", ");
    push_cte_to_check_for_deleted_silo(&mut builder, silo_id);
    builder.sql(", ");
    push_cte_to_update_subnet_pool_rcgen(&mut builder);
    builder.sql(", ");
    push_cte_to_update_subnet_pool_member_rcgen(&mut builder);
    builder.sql(", ");
    push_cte_to_insert_actual_ip_subnet(
        &mut builder,
        silo_id,
        project_id,
        identity,
    );
    builder.sql(" SELECT * FROM new_record");
    builder.query()
}

// Push a CTE that selects a subnet pool from an IP subnet.
//
// Given an IP subnet and a Silo ID, add to the query CTEs that search the
// Subnet Pools linked to this Silo for one containing the IP subnet. Return the
// Subnet Pool, Subnet Pool Member, and the IP subnet itself.
fn push_cte_to_select_pool_from_explicit_subnet(
    builder: &mut QueryBuilder,
    silo_id: Uuid,
    subnet: oxnet::IpNet,
) {
    // WITH pool_member AS (
    //   SELECT id, min_alloc_prefix, max_alloc_prefix
    //   FROM subnet_pool_member
    //   WHERE first_address <= subnet
    //     AND last_address >= subnet
    //     AND time_deleted IS NULL
    //     AND min_alloc_prefix <= masklen(subnet)
    //     AND max_alloc_prefix >= masklen(subnet)
    //   LIMIT 1
    // ),
    // linked_pools AS (
    //   SELECT subnet_pool_id
    //   FROM subnet_pool_silo_link
    //   WHERE silo_id = $silo_id
    //     AND ip_version = family(subnet)
    // )
    //
    //
    // ......
    //
    //
    // WITH subnet_pool_and_member AS (
    //   SELECT
    //     sp.id AS subnet_pool_id
    //     spm.id AS subnet_pool_member_id,
    //     spm.subnet,
    //     spm.min_prefix_length,
    //     spm.max_prefix_length,
    //   FROM subnet_pool_member AS spm
    //   INNER JOIN subnet_pool AS sp
    //   ON subnet_pool.id = subnet_pool_member.subnet_pool_id
    //   INNER JOIN subnet_pool_silo_link AS l
    //   ON sp.id = l.subnet_pool_id
    //   WHERE l.silo_id = $silo_id
    //     AND l.ip_version = $ip_version
    //     AND spm.first_address <= $subnet
    //     AND spm.last_address >= $subnet
    //     AND spm.min_prefix_length <= masklen($subnet)
    //     AND spm.max_prefix_length >= masklen($subnet)
    //   LIMIT 1
    // ), subnet_exists_in_linked_pool AS (
    //   SELECT CAST(IF(
    //      EXISTS((SELECT 1 FROM subnet_pool_and_member)),
    //      'true',
    //      'no-valid-subnet'
    //      ) AS BOOL)
    // ), subnet_does_not_overlap AS (
    //   SELECT CAST(IF(EXISTS((
    //     SELECT 1
    //     FROM external_subnet AS es
    //     WHERE
    //       -- existing completely contains candidate
    //       NOT (es.first_address <= $first_address AND es.last_address >= $last_address)
    //       -- candidate completely contains existing
    //       AND NOT ($first_address <= es.first_address AND $last_address >= es.last_address)
    //       -- existing end is inside the candidate
    //       AND NOT (es.last_address >= $first_address AND es.last_address <= $last_address)
    //       -- existing start is inside the candidate
    //       AND NOT (es.first_address >= $first_address and es.first_address <= $last_address)
    //   )),
    //   'true',
    //   'overlaps-with-existing',
    //   AS BOOL)
    // )
    let prefix_len = i32::from(subnet.width());
    let first_address = IpNet::from(subnet.prefix());
    let (last_address, ip_version) = match subnet {
        oxnet::IpNet::V4(ipv4) => {
            (IpNet::from(std::net::IpAddr::V4(ipv4.last_addr())), IpVersion::V4)
        }
        oxnet::IpNet::V6(ipv6) => {
            (IpNet::from(std::net::IpAddr::V6(ipv6.last_addr())), IpVersion::V6)
        }
    };
    builder.sql(
        "subnet_pool_and_member AS (\
            SELECT \
                sp.id AS subnet_pool_id, \
                spm.id AS subnet_pool_member_id, "
        )
        .param()
        .bind::<sql_types::Inet, _>(IpNet::from(subnet))
        .sql(" AS subnet \
            FROM subnet_pool_member AS spm \
            INNER JOIN subnet_pool AS sp \
            ON sp.id = spm.subnet_pool_id \
            INNER JOIN subnet_pool_silo_link AS l \
            ON sp.id = l.subnet_pool_id \
            WHERE l.silo_id = "
    )
        .param()
        .bind::<sql_types::Uuid, _>(silo_id)
        .sql(" AND l.ip_version = ")
        .param()
        .bind::<IpVersionEnum, _>(ip_version)
        .sql(" \
            AND sp.time_deleted IS NULL \
            AND spm.time_deleted IS NULL \
            AND spm.first_address <= "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" AND spm.last_address >= ")
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" AND spm.min_prefix_length <= ")
        .param()
        .bind::<sql_types::Integer, _>(prefix_len)
        .sql(" AND spm.max_prefix_length >= ")
        .param()
        .bind::<sql_types::Integer, _>(prefix_len)
        .sql(" LIMIT 1), subnet_exists_in_linked_pool AS MATERIALIZED (\
            SELECT CAST(\
                IF(EXISTS((SELECT 1 FROM subnet_pool_and_member)), \
                'true', '"
        )
        .sql(NO_LINKED_POOL_CONTAINS_REQUESTED_SUBNET_SENTINEL)
        .sql("') AS BOOL)), candidate_contains_existing_first AS MATERIALIZED (\
            SELECT 1 \
            FROM external_subnet \
            WHERE \
                first_address BETWEEN "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" AND ")
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" AND time_deleted IS NULL LIMIT 1), \
        candidate_contains_existing_last AS MATERIALIZED (\
            SELECT 1 \
            FROM external_subnet \
            WHERE \
                last_address BETWEEN "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" AND ")
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" AND time_deleted IS NULL LIMIT 1), \
        existing_contains_candidate_first AS MATERIALIZED (\
            SELECT 1 \
            FROM external_subnet \
            WHERE "
        )
        .param()
        .bind::<sql_types::Inet, _>(first_address)
        .sql(" BETWEEN first_address AND last_address AND time_deleted IS NULL LIMIT 1), \
        existing_contains_candidate_last AS MATERIALIZED (\
            SELECT 1 \
            FROM external_subnet \
            WHERE "
        )
        .param()
        .bind::<sql_types::Inet, _>(last_address)
        .sql(" BETWEEN first_address AND last_address AND time_deleted IS NULL LIMIT 1), \
        subnet_does_not_overlap AS MATERIALIZED (SELECT CAST(IF(EXISTS((\
                SELECT 1 \
                WHERE NOT EXISTS(SELECT 1 FROM candidate_contains_existing_first) \
                  AND NOT EXISTS(SELECT 1 FROM candidate_contains_existing_last) \
                  AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_first) \
                  AND NOT EXISTS(SELECT 1 FROM existing_contains_candidate_last)\
            )), 'true', '"
        )
        .sql(SUBNET_OVERLAPS_EXISTING_SENTINEL)
        .sql("') AS BOOL))");
}

fn push_cte_to_check_for_deleted_project(
    builder: &mut QueryBuilder,
    project_id: &Uuid,
) {
    builder
        .sql(
            "project_is_not_deleted AS MATERIALIZED(\
        SELECT CAST(\
            IF(EXISTS(\
                SELECT 1 \
                FROM project \
                WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*project_id)
        .sql(
            " AND time_deleted IS NULL LIMIT 1), \
            'true', \
            '",
        )
        .sql(PROJECT_DELETED_SENTINEL)
        .sql("') AS BOOL))");
}

fn push_cte_to_check_for_deleted_silo(
    builder: &mut QueryBuilder,
    silo_id: &Uuid,
) {
    builder
        .sql(
            "silo_is_not_deleted AS MATERIALIZED( \
        SELECT CAST(\
            IF(EXISTS(\
                SELECT 1 \
                FROM silo \
                WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*silo_id)
        .sql(
            " AND time_deleted IS NULL LIMIT 1), \
            'true', \
            '",
        )
        .sql(SILO_DELETED_SENTINEL)
        .sql("') AS BOOL))");
}

fn push_cte_to_update_subnet_pool_rcgen(builder: &mut QueryBuilder) {
    builder.sql(
        "updated_pool AS (\
        UPDATE subnet_pool \
        SET \
            time_modified = NOW(), \
            rcgen = rcgen + 1 \
        WHERE id = (SELECT subnet_pool_id FROM subnet_pool_and_member) \
          AND time_deleted IS NULL \
        RETURNING 1\
    )",
    );
}

fn push_cte_to_update_subnet_pool_member_rcgen(builder: &mut QueryBuilder) {
    builder.sql(
        "updated_pool_member AS (\
        UPDATE subnet_pool_member \
        SET \
            time_modified = NOW(), \
            rcgen = rcgen + 1 \
        WHERE id = (SELECT subnet_pool_member_id FROM subnet_pool_and_member) \
          AND time_deleted IS NULL \
        RETURNING 1\
    )",
    );
}

fn push_cte_to_insert_actual_ip_subnet(
    builder: &mut QueryBuilder,
    silo_id: &Uuid,
    project_id: &Uuid,
    identity: ExternalSubnetIdentity,
) {
    builder
        .sql(
            " new_record AS (\
        INSERT INTO external_subnet (\
            id, \
            name, \
            description, \
            time_created, \
            time_modified, \
            time_deleted, \
            subnet_pool_id, \
            subnet_pool_member_id, \
            silo_id, \
            project_id, \
            subnet, \
            attach_state, \
            instance_id\
        ) \
        SELECT ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(identity.id)
        .sql(" AS id, ")
        .param()
        .bind::<sql_types::Text, _>(identity.name)
        .sql(" AS name, ")
        .param()
        .bind::<sql_types::Text, _>(identity.description)
        .sql(" AS description, ")
        .param()
        .bind::<sql_types::Timestamptz, _>(identity.time_created)
        .sql(" AS time_created, ")
        .param()
        .bind::<sql_types::Timestamptz, _>(identity.time_modified)
        .sql(
            " AS time_modified, \
            NULL::TIMESTAMPTZ AS time_deleted, \
            subnet_pool_id, \
            subnet_pool_member_id, ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*silo_id)
        .sql(" AS silo_id, ")
        .param()
        .bind::<sql_types::Uuid, _>(*project_id)
        .sql(
            " AS project_id, \
            subnet AS subnet, \
            'detached' AS attach_state, \
            NULL AS instance_id \
            FROM subnet_pool_and_member \
            RETURNING \
                id, \
                name, \
                description, \
                time_created, \
                time_modified, \
                time_deleted, \
                subnet_pool_id, \
                subnet_pool_member_id, \
                silo_id, \
                project_id, \
                subnet, \
                attach_state, \
                instance_id\
        )",
        );
}

/// Delete an External Subnet by ID, failing if there is an instance attached.
pub fn delete_external_subnet_query(subnet_id: &Uuid) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    builder
        .sql(
            "WITH no_instance_attached AS MATERIALIZED (\
        SELECT CAST(IF(instance_id IS NULL, 'true', '",
        )
        .sql(INSTANCE_STILL_ATTACHED_SENTINEL)
        .sql(
            "') AS BOOL) \
            FROM external_subnet \
            WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*subnet_id)
        .sql(
            " AND time_deleted IS NULL) \
            UPDATE external_subnet \
            SET time_deleted = NOW() \
            WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*subnet_id)
        .sql(" AND time_deleted IS NULL RETURNING 1");
    builder.query()
}

// Push a CTE that either returns the pool ID directly, or looks the pool up by
// name and returns its ID.
fn push_cte_to_select_pool_id(builder: &mut QueryBuilder, pool: &NameOrId) {
    match pool {
        NameOrId::Id(pool_id) => {
            builder
                .sql("pool_id AS (SELECT id, name FROM subnet_pool WHERE id = ")
                .param()
                .bind::<sql_types::Uuid, _>(*pool_id)
                .sql(" AND time_deleted IS NULL)");
        }
        NameOrId::Name(pool_name) => {
            builder.sql("pool_id AS (SELECT id, name FROM subnet_pool WHERE name = ")
                .param()
                .bind::<sql_types::Text, _>(pool_name.to_string())
                .sql(" AND time_deleted IS NULL)");
        }
    }
}

fn push_cte_to_ensure_pool_is_linked_to_silo(
    builder: &mut QueryBuilder,
    silo_id: &Uuid,
) {
    builder
        .sql(
            "ensure_silo_is_linked_to_pool AS (\
        SELECT CAST(IF(EXISTS((\
            SELECT 1 \
            FROM subnet_pool_silo_link AS l \
            WHERE l.subnet_pool_id = (SELECT id FROM pool_id) \
            AND l.silo_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*silo_id)
        .sql(")), 'true', '")
        .param()
        .bind::<sql_types::Text, _>(REQUESTED_POOL_NOT_LINKED_TO_SILO_SENTINEL)
        .sql("') AS BOOL))");
}

fn push_cte_to_select_next_subnet_from_pool(
    builder: &mut QueryBuilder,
    prefix: u8,
) {
    let prefix = i32::from(prefix);
    builder
        .sql(
            "existing_external_subnets AS (\
        SELECT \
            m.id AS subnet_pool_member_id, \
            m.subnet_pool_id, \
            m.first_address AS member_start, \
            m.last_address AS member_end, \
            m.min_prefix_length, \
            m.max_prefix_length, \
            e.first_address AS allocated_start, \
            e.last_address AS allocated_end, \
            LEAD(e.first_address) OVER (\
                PARTITION BY m.id \
                ORDER BY e.first_address\
            ) AS next_allocated_start \
        FROM subnet_pool_member AS m \
        LEFT JOIN external_subnet AS e \
            ON e.subnet_pool_member_id = m.id \
            AND e.time_deleted IS NULL \
        WHERE m.subnet_pool_id = (SELECT id FROM pool_id) \
            AND m.time_deleted IS NULL \
            AND ",
        )
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(
            " BETWEEN m.min_prefix_length AND m.max_prefix_length), \
    gaps AS (\
        SELECT \
            subnet_pool_id, \
            subnet_pool_member_id, \
            member_start, \
            member_end, \
            min_prefix_length, \
            max_prefix_length, \
            CASE \
                WHEN allocated_start IS NULL THEN member_start \
                ELSE allocated_end + 1 \
            END AS gap_start, \
            CASE \
                WHEN next_allocated_start IS NULL THEN member_end \
                ELSE next_allocated_start - 1 \
            END AS gap_end \
        FROM existing_external_subnets\
    ), candidate_subnets AS ( \
        SELECT \
            subnet_pool_id, \
            subnet_pool_member_id, \
            gap_start, \
            gap_end, \
            CASE \
                WHEN set_masklen(gap_start, ",
        )
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(") & netmask(set_masklen(gap_start, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(")) >= gap_start THEN set_masklen(gap_start, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(") & netmask(set_masklen(gap_start, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(")) ELSE set_masklen(gap_start + 1, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(") & netmask(set_masklen(gap_start + 1, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(
            ")) END AS candidate_start FROM gaps), \
        subnet_pool_and_member AS ( \
            SELECT \
                subnet_pool_id, \
                subnet_pool_member_id, \
                set_masklen(candidate_start, ",
        )
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(
            ") AS subnet \
            FROM candidate_subnets \
            WHERE candidate_start <= gap_end \
            AND broadcast(set_masklen(candidate_start, ",
        )
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(")) <= gap_end ORDER BY candidate_start LIMIT 1)");
    // existing_external_subnets AS (
    //  SELECT
    //      md.id AS subnet_pool_member_id,
    //      m.subnet_pool_id,
    //      m.first_address AS member_start,
    //      m.last_address AS member_end,
    //      m.min_prefix_length,
    //      m.max_prefix_length,
    //      e.first_address AS allocated_start,
    //      e.last_address AS allocated_end,
    //      LEAD(e.first_adress) OVER (
    //          PARTITION by m.id
    //          ORDER BY e.first_address
    //      ) AS next_allocated_start
    //  FROM subnet_pool_member AS m
    //  LEFT JOIN external_subnet AS e
    //    ON e.subnet_pool_member_id = m.id
    //    AND e.time_deleted IS NULL
    //  WHERE m.subnet_pool_id = $pool_id
    //    AND m.time_deleted IS NULL
    //    AND $requested_prefix BETWEEN m.min_prefix_length AND m.max_prefix_length
    // ),
    // gaps AS (
    //  SELECT
    //      subnet_pool_id,
    //      subnet_pool_member_id,
    //      member_start,
    //      member_end,
    //      min_prefix_length,
    //      max_prefix_length,
    //      CASE
    //          WHEN allocated_start IS NULL THEN member_start
    //          ELSE allocated_end + 1
    //      END AS gap_start,
    //      CASE
    //          WHEN next_allocated_start IS NULL THEN member_end
    //          ELSE next_allocated_start - 1
    //      END AS gap_end
    //      FROM existing_external_subnets
    // ),
    // candidates AS (
    //  SELECT
    //      subnet_pool_member_id,
    //      gap_start,
    //      gap_end,
    //      CASE
    //          WHEN network(set_masklen(gap_start, $requested_prefix)) >= gap_start
    //          THEN
    //              network(set_masklen(gap_start, $requested_prefix))
    //          ELSE
    //              network(set_masklen(gap_start + 1, $requested_prefix))
    //          END AS candidate_start
    //  FROM gaps
    // )
    // SELECT
    //  subnet_pool_id,
    //  subnet_pool_member_id,
    //  set_masklen(candidate_start, $requested_prefix) AS subnet
    // FROM candidates
    // WHERE candidate_start <= gap_end
    //   AND broadcast(set_masklen(candidate_start, $requested_prefix)) <= gap_end
    // ORDER BY candidate_start
    // LIMIT 1
    //
    //
    // ----------- original
    //
    // WITH ordered_allocs AS (
    //   SELECT
    //     m.id                AS member_id,
    //     m.first_address     AS member_start,
    //     m.last_address      AS member_end,
    //     m.min_prefix,
    //     m.max_prefix,
    //     e.first_address     AS alloc_start,
    //     e.last_address      AS alloc_end,
    //     LEAD(e.first_address) OVER (
    //       PARTITION BY m.id
    //       ORDER BY e.first_address
    //     ) AS next_alloc_start
    //   FROM subnet_pool_member AS m
    //   LEFT JOIN external_subnet AS e
    //     ON e.subnet_pool_member_id = m.id
    //    AND e.time_deleted IS NULL
    //   WHERE m.subnet_pool_id = :pool_id
    //     AND m.time_deleted IS NULL
    //     AND :requested_prefix BETWEEN m.min_prefix AND m.max_prefix
    // ),
    // gaps AS (
    //   SELECT
    //     member_id,
    //     member_start,
    //     member_end,
    //     min_prefix,
    //     max_prefix,
    //     CASE
    //       WHEN alloc_start IS NULL THEN member_start
    //       ELSE alloc_end + 1
    //     END AS gap_start,
    //     CASE
    //       WHEN next_alloc_start IS NULL THEN member_end
    //       ELSE next_alloc_start - 1
    //     END AS gap_end
    //   FROM ordered_allocs
    // ),
    // candidates AS (
    //   SELECT
    //     member_id,
    //     gap_start,
    //     gap_end,
    //     CASE
    //       WHEN network(set_masklen(gap_start, :requested_prefix)) >= gap_start
    //       THEN
    //         network(set_masklen(gap_start, :requested_prefix))
    //       ELSE
    //         network(set_masklen(gap_start + 1, :requested_prefix))
    //     END AS candidate_start
    //   FROM gaps
    // )
    // SELECT
    //   member_id,
    //   set_masklen(candidate_start, :requested_prefix) AS allocated_subnet
    // FROM candidates
    // WHERE
    //   candidate_start <= gap_end
    //   AND broadcast(set_masklen(candidate_start, :requested_prefix)) <= gap_end
    // ORDER BY candidate_start
    // LIMIT 1;
}

/// Error sentinel emitted when we can't find a Subnet Pool linked to the
/// current Silo that contains an explicitly-requested subnet. This could be
/// because the prefix is invalid or the actual subnet isn't part of the pool.
pub const NO_LINKED_POOL_CONTAINS_REQUESTED_SUBNET_SENTINEL: &str =
    "no-linked-pool";

/// Error sentinel emitted when requesting an explicit subnet, and it overlaps
/// an existing subnet that's already allocated.
pub const SUBNET_OVERLAPS_EXISTING_SENTINEL: &str = "overlap-existing";

/// Error sentinel emitted when we try to insert a subnet into a project that
/// has now been deleted.
pub const PROJECT_DELETED_SENTINEL: &str = "project-deleted";

/// Error sentinel emitted when we try to insert a subnet into a silo that
/// has now been deleted.
pub const SILO_DELETED_SENTINEL: &str = "silo-deleted";

/// Error sentinel emitted when attempting to delete External Subnet that's
/// attached to an instance.
pub const INSTANCE_STILL_ATTACHED_SENTINEL: &str = "instance-attached";

/// Error sentinel emitted when requesting a subnet from an explicit pool, by ID
/// or name, but the pool is not linked to the provided Silo. This should result
/// in a NotFound error to the client.
pub const REQUESTED_POOL_NOT_LINKED_TO_SILO_SENTINEL: &str = "pool-not-linked";

#[cfg(test)]
mod tests {
    use super::insert_external_subnet_query;
    use super::insert_subnet_pool_member_query;
    use crate::db::explain::ExplainableAsync as _;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use chrono::TimeZone as _;
    use chrono::Utc;
    use nexus_db_model::SubnetPoolMember;
    use nexus_types::external_api::params::ExternalSubnetCreate;
    use nexus_types::external_api::params::ExternalSubnetSelector;
    use nexus_types::external_api::params::SubnetPoolMemberCreate;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid as _;
    use omicron_uuid_kinds::SubnetPoolMemberUuid;
    use omicron_uuid_kinds::SubnetPoolUuid;

    #[tokio::test]
    async fn expectorate_insert_subnet_pool_member_query() {
        let t = Utc.with_ymd_and_hms(2020, 1, 1, 12, 12, 12).unwrap();
        let pool_id = SubnetPoolUuid::from_untyped_uuid(uuid::uuid!(
            "3ab604dc-35d2-4ebd-8c0b-30973979e0c8"
        ));
        let params = SubnetPoolMemberCreate {
            pool: pool_id.into_untyped_uuid().into(),
            subnet: "10.0.0.1/16".parse().unwrap(),
            min_prefix_length: 16,
            max_prefix_length: 24,
        };
        let mut member = SubnetPoolMember::new(&params, pool_id).unwrap();
        // Set some data of the member to be consistent.
        member.id = SubnetPoolMemberUuid::from_untyped_uuid(uuid::uuid!(
            "1e70cb86-2c1c-4e9a-a73c-21aa2129bb93"
        ))
        .into();
        member.time_created = t;
        member.time_modified = t;
        expectorate_query_contents(
            insert_subnet_pool_member_query(&member).unwrap(),
            "tests/output/insert_subnet_pool_member.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn can_explain_insert_external_subnet_query() {
        let logctx =
            dev::test_setup_log("can_explain_insert_external_subnet_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();
        let silo_id = uuid::uuid!("a561c167-a22a-463d-8614-912370453bb1");
        let project_id = uuid::uuid!("c7ab1509-a928-4f1c-b0e6-83cbbab78690");
        let params = ExternalSubnetCreate {
            identity: IdentityMetadataCreateParams {
                name: "ext".parse().unwrap(),
                description: String::new(),
            },
            subnet: ExternalSubnetSelector::Explicit {
                subnet: "192.168.1.0/24".parse().unwrap(),
            },
        };
        let query =
            insert_external_subnet_query(&silo_id, &project_id, &params);
        let exp = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        println!("{exp}");
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
