// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for operating on external subnet pools.

use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::SelectableSql;
use crate::db::raw_query_builder::TypedSqlQuery;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_auth::authz;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::ExternalSubnet;
use nexus_db_model::ExternalSubnetIdentity;
use nexus_db_model::Generation;
use nexus_db_model::IpNet;
use nexus_db_model::IpVersion;
use nexus_db_model::SubnetPoolMember;
use nexus_db_model::SubnetPoolSiloLink;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_schema::enums::IpVersionEnum;
use nexus_types::external_api::params::ExternalSubnetSelector;
use nexus_types::external_api::params::PoolSelector;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::SubnetPoolUuid;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use uuid::Uuid;

/// Query to insert a member in a Subnet Pool, which checks for overlapping
/// subnets.
pub fn insert_subnet_pool_member_query(
    member: &SubnetPoolMember,
) -> TypedSqlQuery<SelectableSql<SubnetPoolMember>> {
    let mut builder = QueryBuilder::new();
    let (first_address, last_address) = network_edges(member.subnet);

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
    builder.query()
}

// Return the first / last addresses in the network.
//
// Note that this returns model network types, so we can insert them directly in
// the above query. It also returns the exact address, e.g, a /32 or /128,
// regardless of the prefix of `subnet`.
fn network_edges(subnet: IpNet) -> (IpNet, IpNet) {
    match subnet {
        IpNet::V4(v4) => (
            Ipv4Net::host_net(v4.first_addr()).into(),
            Ipv4Net::host_net(v4.last_addr()).into(),
        ),
        IpNet::V6(v6) => (
            Ipv6Net::host_net(v6.first_addr()).into(),
            Ipv6Net::host_net(v6.last_addr()).into(),
        ),
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
///
/// # Notes
///
/// This query is pretty complicated. First, we support creating a subnet in a
/// few different ways, such as by an explicit IP subnet, automatically from a
/// specific pool, or automatically from a pool linked to the current Silo.
///
/// The first of those is relatively straightfoward: select a constant, and
/// insert it into the table conditional on it not overlapping with an existing
/// row.
///
/// The latter are subtle and complex. In all the cases, we first find a pool
/// linked to the current Silo. Here, "find" could mean look it up by ID and
/// check that it's linked, if we're given an explicit pool to take a subnet
/// from.
///
/// Next, we have to find gaps in the subnet pool members from that pool. To do
/// that, we create rows that have existing subnets along with the start of the
/// _next_ subnet, grouped by pool member (i.e., don't span members, even if
/// they're contiguous). Then we compute the gaps as:
///
/// - The start of the member, if there are no subnets or the first one doesn't
///   start at the member start.
/// - The space between any subnets.
/// - The end of the last subnet to the end of the member.
///
/// Finally, filter that to those who can fit the requested subnet. All the
/// automatic-allocation versions require a prefix, so we know immediately
/// whether the gaps will fit.
///
/// The last tricky bit is that we have to "snap" all the gaps to be aligned to
/// the prefix boundary. We do this by using functions to compute the network
/// address (which CockroachDB unfortunately lacks), or the broadcast address.
/// To find the _next_ such gap, we can take the broadcast address, _add one_,
/// and then mask it to the requested prefix.
///
/// As said above, this is all pretty complicated. We have a lot of tests for
/// the behavior, checking that we never overlap with existing subnets, or that
/// we fail in predictable and helpful ways when the automatic allocations fail.
/// We also check that we fill the first gap that fits the subnet, and that we
/// skip gaps that are too small to fit the requested prefix.
///
/// We also have a big, though `#[ignored]`, stress test, which does a bunch of
/// random allocations, of all the supported kinds, or deletions, and ensures
/// our invariants are still upheld.
///
/// The expectorated SQL files, such as
/// `nexus/db-queries/tests/output/insert_external_subnet_from_explicit_pool.sql`
/// will be very helpful to actually inspect and understand the query.
pub fn insert_external_subnet_query(
    silo_id: &Uuid,
    project_id: &Uuid,
    identity: ExternalSubnetIdentity,
    subnet_selector: &ExternalSubnetSelector,
) -> TypedSqlQuery<SelectableSql<ExternalSubnet>> {
    let mut builder = QueryBuilder::new();
    builder.sql("WITH ");
    match subnet_selector {
        ExternalSubnetSelector::Explicit { subnet } => {
            // In this case, we're given the IP subnet. We need to find the
            // Subnet Pool and Subnet Pool Member containing it (if any), ensure
            // they're linked to the current Silo, and then return those values.
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
            // We're given the pool itself. Select it as a constant, ensure it's
            // actually linked to the current Silo, and then push the CTE that
            // does the automatic allocation of the next subnet from the members
            // of that pool which can fit the requested prefix.
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
            // THis is the same as the above, but we're taking the pool by
            // looking up the default pool linked to the current silo for the
            // current version.
            //
            // This will fail if the version is not specified, and there are
            // both default IPv4 and IPv6 subnet pools for the silo, rather than
            // attempting to pick one.
            push_cte_to_select_default_pool_by_version(
                &mut builder,
                silo_id,
                ip_version.map(Into::into),
            );
            builder.sql(", ");
            push_cte_to_select_next_subnet_from_pool(&mut builder, *prefix);
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

// Add a CTE that fails with a bool-parse error if the Project we're trying to
// create the External Subnet in is deleted.
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

// Add a CTE that fails with a bool-parse error if the Silo we're trying to
// create the External Subnet in is deleted.
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

// Add a CTE that updates the generation Subnet Pool that contains the candidate
// External Subnet we'll insert.
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

// Add a CTE that updates the generation Subnet Pool Member that contains the
// candidate External Subnet we'll insert.
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

// This is the "last" CTE, which inserts in the `external_subnet` table all the
// records we've selected in earlier CTEs.
fn push_cte_to_insert_actual_ip_subnet(
    builder: &mut QueryBuilder,
    silo_id: &Uuid,
    project_id: &Uuid,
    identity: ExternalSubnetIdentity,
) {
    builder
        .sql(
            "new_record AS (\
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

// Push a CTE that either returns the pool ID directly, or looks the pool up by
// name and returns its ID.
fn push_cte_to_select_pool_id(builder: &mut QueryBuilder, pool: &NameOrId) {
    match pool {
        NameOrId::Id(pool_id) => {
            builder
                .sql("pool_id AS (SELECT id FROM subnet_pool WHERE id = ")
                .param()
                .bind::<sql_types::Uuid, _>(*pool_id)
                .sql(" AND time_deleted IS NULL)");
        }
        NameOrId::Name(pool_name) => {
            builder
                .sql("pool_id AS (SELECT id FROM subnet_pool WHERE name = ")
                .param()
                .bind::<sql_types::Text, _>(pool_name.to_string())
                .sql(" AND time_deleted IS NULL)");
        }
    }
}

// Push a CTE that fails with a bool-parse error if the current Pool is not
// linked to the current Silo.
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

// Push a CTE that returns the ID of the default pool linked to the silo, or
// fails. If the IP version is provided, use exactly that default. If not, this
// query also ensures we have only one default pool, rather than picking one of
// the two.
fn push_cte_to_select_default_pool_by_version(
    builder: &mut QueryBuilder,
    silo_id: &Uuid,
    maybe_ip_version: Option<IpVersion>,
) {
    builder
        .sql(
            "pool_id AS (\
        SELECT subnet_pool_id AS id \
        FROM subnet_pool_silo_link \
        WHERE silo_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(*silo_id);

    // Add the version filter if it's provided.
    if let Some(ip_version) = &maybe_ip_version {
        builder
            .sql(" AND ip_version = ")
            .param()
            .bind::<IpVersionEnum, _>(*ip_version);
    }
    builder.sql(" AND is_default)");

    // Add a CTE to check the count of default pools.
    builder
        .sql(
            ", exactly_one_default_pool AS MATERIALIZED (\
        SELECT CAST(CASE (SELECT COUNT(1) FROM pool_id) \
            WHEN 0 THEN '",
        )
        .sql(NO_LINKED_DEFAULT_POOL)
        .sql("' WHEN 1 THEN 'true' WHEN 2 THEN '")
        .sql(MULTIPLE_LINKED_DEFAULT_POOLS)
        .sql("' END AS BOOL))");
}

// Push a CTE to automatically select the next subnet from a pool with the
// provided prefix length.
fn push_cte_to_select_next_subnet_from_pool(
    builder: &mut QueryBuilder,
    prefix: u8,
) {
    let prefix = i32::from(prefix);

    // In this first CTE, we're joining the external subnets to the members of
    // the pool we selected in an earlier CTE.
    //
    // The important bit here is that we're also using `LEAD()` to get the start
    // of the next subnet _after_ the current one. This helps compute the gaps,
    // which we do below. Crucially, we're doing that window partitioned by the
    // member ID. That means we never cross member boundaries, only looking at
    // gaps wholly contained within each member.
    builder
        .sql(
            "existing_external_subnets AS (\
        SELECT \
            m.subnet_pool_id, \
            m.id AS subnet_pool_member_id, \
            m.first_address AS member_start, \
            m.last_address AS member_end, \
            m.min_prefix_length, \
            m.max_prefix_length, \
            e.first_address AS subnet_start, \
            e.last_address AS subnet_end, \
            LEAD(e.first_address) OVER (\
                PARTITION BY m.id \
                ORDER BY e.first_address\
            ) AS next_subnet_start \
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
        .sql(" BETWEEN m.min_prefix_length AND m.max_prefix_length), ");

    // This CTE computes the gaps _between_ all the subnets.
    //
    // We use the existing subnets from the previous CTE, and generate either
    //
    // - The full member, if it's empty
    // - In between every existing subnet, and
    // - From the end of the last subnet to the end of the member.
    //
    // Specifically, the gap start is:
    //
    // - the last address in the subnet + 1, if there is a subnet
    // - the start of the member, if there is no subnet.
    //
    // The gap end is:
    //
    // - The next subnet start - 1, if there is a next subnet.
    // - The member end, if there is no next subnet.
    //
    // The careful reader will note that in that first case, the gap end may be
    // _less_ than the gap start. That's intentional. When the existing subnets
    // are back-to-back, without any space, we get a negative "gap", which we
    // filter out in the next CTE. But this works as you'd expect if there is a
    // non-empty space between the subnets.
    builder.sql(
        "gaps_between_subnets AS (\
        SELECT \
            subnet_pool_id, \
            subnet_pool_member_id, \
            CASE \
                WHEN subnet_start IS NULL THEN member_start \
                ELSE subnet_end + 1 \
            END AS gap_start, \
            CASE \
                WHEN next_subnet_start IS NULL THEN member_end \
                ELSE next_subnet_start - 1 \
            END AS gap_end \
        FROM existing_external_subnets), ",
    );

    // CTE to generate any gap _before_ the first subnet in a member.
    //
    // The `LEAD()` window function lets us generate all the gaps trailing an
    // existing subnet, and the above CASE statement handles when there are no
    // subnets at all. But so far we haven't generated gaps _preceding_ the
    // first subnet, i.e., from the member start to just before the first subnet
    // start.
    //
    // These have to be their own CTE. The left join used to match subnets with
    // the next subnet start by definition produces one row per subnet in a
    // member, with the next subnet start attached as a new column. We can't use
    // that to generate a row that has no corresponding subnet already. We need
    // another one, and then to union the two into all the gaps.
    builder
        .sql(
            "gaps_before_first_subnet AS (\
        SELECT DISTINCT ON (m.id) \
            m.subnet_pool_id, \
            m.id AS subnet_pool_member_id, \
            m.first_address AS gap_start, \
            e.first_address - 1 AS gap_end \
        FROM subnet_pool_member AS m \
        JOIN external_subnet AS e \
            ON e.subnet_pool_member_id = m.id \
            AND e.time_deleted IS NULL \
        WHERE m.subnet_pool_id = (SELECT id FROM pool_id) \
            AND m.time_deleted IS NULL \
            AND ",
        )
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(
            " BETWEEN m.min_prefix_length AND m.max_prefix_length \
            ORDER BY m.id, e.first_address), ",
        );

    // Union the two sets of gaps.
    builder.sql(
        "gaps AS (\
        SELECT * FROM gaps_between_subnets \
        UNION ALL \
        SELECT * FROM gaps_before_first_subnet\
    ), ",
    );

    // This CTE generates candidate subnets.
    //
    // This is probably the most subtle CTE in this query. The critical part is
    // that we need to align all the gaps we're considering to the prefix in the
    // request. This is made annoyingly-verbose by CockroachDB's lack of a
    // `network()` function for computing the network address of a subnet.
    // Instead, we use `ip & netmask(ip)` to emulate that.
    //
    // For every gap, we compute the subnet as either:
    //
    // - the start of the gap, if the network address of the gap is aligned to
    //   the requested prefix, or
    // - the start of the _next_ prefix-aligned subnet after that. This works by
    //   taking `network(broadcast(gap_start) + 1)`, though we emulate the
    //   `network()` function as before.
    //
    // In all cases, we filter out candidates where the gap is empty, which
    // could be generated by the previous CTE in the case of back-to-back
    // subnets.
    builder
        .sql(
            "candidate_subnets AS (\
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
        .sql(")) = gap_start THEN set_masklen(gap_start, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(") ELSE set_masklen(broadcast(set_masklen(gap_start, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(")) + 1, ")
        .param()
        .bind::<sql_types::Integer, _>(prefix)
        .sql(
            ") END AS candidate_subnet \
            FROM gaps \
            WHERE gap_start < gap_end\
        ), ",
        );

    // In this final CTE, we filter the candidate subnets to only those which
    // actually _fit_ in the gap. At the end, we have to compare the broadcast
    // of the candidate subnet _as an address_ (/128 or /32) to the gap end. Not
    // doing so means we're comparing a true subnet (say, /64) with a true
    // address, and CockroachDB always sorts the former before the latter. For
    // example `SELECT 'fd00:1::/56'::INET < 'fd00::'::INET` returns true, which
    // is a little counterintuitive, probably wrong, and definitely different
    // from what PostgreSQL does. But that means we'd always return true in
    // the last filtering clause, since all subnets sort before any IP address.
    builder.sql(
        "subnet_pool_and_member AS (\
            SELECT \
                subnet_pool_id, \
                subnet_pool_member_id, \
                candidate_subnet AS subnet \
            FROM candidate_subnets \
            WHERE candidate_subnet & netmask(candidate_subnet) >= gap_start \
              AND set_masklen( \
                    broadcast(candidate_subnet), \
                    IF(family(candidate_subnet) = 4, 32, 128) \
                ) <= gap_end \
            ORDER BY candidate_subnet \
            LIMIT 1\
        )",
    );
}

/// Decode the errors emitted by `insert_external_subnet_query()`.
pub fn decode_insert_external_subnet_error(
    e: DieselError,
    silo_id: &Uuid,
    authz_project: &authz::Project,
    subnet: &ExternalSubnetSelector,
) -> Error {
    match &e {
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, info) => {
            let msg = info.message();
            if is_bool_parse_error(msg, SUBNET_OVERLAPS_EXISTING_SENTINEL) {
                Error::invalid_request(SUBNET_OVERLAPS_EXISTING_ERR_MSG)
            } else if is_bool_parse_error(
                msg,
                NO_LINKED_POOL_CONTAINS_REQUESTED_SUBNET_SENTINEL,
            ) {
                Error::invalid_request(NO_LINKED_POOL_CONTAINS_SUBNET_ERR_MSG)
            } else if is_bool_parse_error(msg, MULTIPLE_LINKED_DEFAULT_POOLS) {
                Error::invalid_request(MULTIPLE_LINKED_DEFAULT_POOLS_ERR_MSG)
            } else if is_bool_parse_error(msg, NO_LINKED_DEFAULT_POOL) {
                Error::invalid_request(NO_LINKED_DEFAULT_POOL_ERR_MSG)
            } else if is_bool_parse_error(msg, PROJECT_DELETED_SENTINEL) {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            } else if is_bool_parse_error(msg, SILO_DELETED_SENTINEL) {
                // It's not really clear what to do here. It's both unlikely and
                // weird. We were able to get this far in the overall API
                // handler, but between looking up the Silo and running this
                // query the whole Silo was deleted.
                //
                // It's also a bit odd to return a 404, because the user never
                // normally "looks up" a silo, they're just inside it. For now,
                // return a 500.
                Error::internal_error(&format!(
                    "Silo appears to have been deleted between \
                    looking it up and running the query to insert \
                    the new External Subnet in it. silo_id = {silo_id}",
                ))
            } else if is_bool_parse_error(
                msg,
                REQUESTED_POOL_NOT_LINKED_TO_SILO_SENTINEL,
            ) {
                // This should only happen if we were given an explicit pool by
                // name or ID.
                let ExternalSubnetSelector::Auto {
                    pool: PoolSelector::Explicit { pool },
                    ..
                } = subnet
                else {
                    return Error::internal_error(&format!(
                        "Query to insert External Subnet failed \
                        because an explicitly requested Silo was not \
                        linked, but the parameters for the query \
                        do not have an explicit Silo by name or ID. \
                        This is a programmer error. \
                        selector = {subnet:#?}"
                    ));
                };
                let lookup_type = match pool {
                    NameOrId::Id(id) => LookupType::ById(*id),
                    NameOrId::Name(name) => {
                        LookupType::ByName(name.to_string())
                    }
                };
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SubnetPool,
                        lookup_type,
                    ),
                )
            } else {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        }
        DieselError::NotFound => {
            // The only situation where we insert 0 rows if is there is no
            // available subnet during an automatic allocation. Report a useful
            // error in that case, based on how the automatic allocation was
            // requested.
            match subnet {
                ExternalSubnetSelector::Explicit { .. } => {
                    Error::internal_error(
                        "Inserted 0 rows during the External Subnet \
                        insert query, which should only happen when \
                        we're doing an automatic allocation from a \
                        pool or IP version. This is a programmer bug.",
                    )
                }
                ExternalSubnetSelector::Auto { pool, .. } => {
                    let msg = match pool {
                        PoolSelector::Explicit { pool } => match pool {
                            NameOrId::Id(id) => {
                                format!(
                                    "All subnets are used in the \
                                        Subnet Pool with ID '{id}'"
                                )
                            }
                            NameOrId::Name(name) => {
                                format!(
                                    "All subnets are used in the \
                                        Subnet Pool with name '{name}'"
                                )
                            }
                        },
                        PoolSelector::Auto { ip_version } => {
                            let version = ip_version
                                .map(|v| format!("IP{v} "))
                                .unwrap_or_else(String::new);
                            format!(
                                "All subnets are used in the default \
                                {version}Subnet Pool for the current Silo"
                            )
                        }
                    };
                    Error::invalid_request(msg)
                }
            }
        }
        _ => public_error_from_diesel(e, ErrorHandler::Server),
    }
}

fn is_bool_parse_error(message: &str, sentinel: &str) -> bool {
    let expected = format!(
        "could not parse \"{sentinel}\" as type bool: invalid bool value"
    );
    message == expected
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

/// Decode the erros emitted by `delete_external_subnet_query()`.
pub fn decode_delete_external_subnet_error(e: DieselError) -> Error {
    match e {
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
            if is_bool_parse_error(
                info.message(),
                INSTANCE_STILL_ATTACHED_SENTINEL,
            ) =>
        {
            Error::invalid_request(INSTANCE_STILL_ATTACHED_ERR_MSG)
        }
        _ => public_error_from_diesel(e, ErrorHandler::Server),
    }
}

// Error sentinel emitted when we can't find a Subnet Pool linked to the
// current Silo that contains an explicitly-requested subnet. This could be
// because the prefix is invalid or the actual subnet isn't part of the pool.
const NO_LINKED_POOL_CONTAINS_REQUESTED_SUBNET_SENTINEL: &str =
    "no-linked-pool";
pub const NO_LINKED_POOL_CONTAINS_SUBNET_ERR_MSG: &'static str = "\
The requested IP subnet is not contained in \
    any Subnet Pool available in the current Silo.";

// Error sentinel emitted when requesting an explicit subnet, and it overlaps
// an existing subnet that's already allocated.
const SUBNET_OVERLAPS_EXISTING_SENTINEL: &str = "overlap-existing";
pub const SUBNET_OVERLAPS_EXISTING_ERR_MSG: &'static str =
    "The requested IP subnet overlaps with an existing External Subnet";

// Error sentinel emitted when we try to insert a subnet into a project that
// has now been deleted.
const PROJECT_DELETED_SENTINEL: &str = "project-deleted";

// Error sentinel emitted when we try to insert a subnet into a silo that
// has now been deleted.
const SILO_DELETED_SENTINEL: &str = "silo-deleted";

// Error sentinel emitted when attempting to delete External Subnet that's
// attached to an instance.
const INSTANCE_STILL_ATTACHED_SENTINEL: &str = "instance-attached";
pub const INSTANCE_STILL_ATTACHED_ERR_MSG: &'static str = "\
Cannot delete external subnet while it is attached \
            to an instance. Detach it and try again.";

// Error sentinel emitted when requesting a subnet from an explicit pool, by ID
// or name, but the pool is not linked to the provided Silo. This should result
// in a NotFound error to the client.
const REQUESTED_POOL_NOT_LINKED_TO_SILO_SENTINEL: &str = "pool-not-linked";

// Error emitted when there are no default pools for the silo.
const NO_LINKED_DEFAULT_POOL: &str = "no-linked-default";
pub const NO_LINKED_DEFAULT_POOL_ERR_MSG: &str = "\
Must specify a Subnet Pool, as there is no linked default pool for the \
        current Silo";

// Error emitted when there are multiple default pools for the silo.
const MULTIPLE_LINKED_DEFAULT_POOLS: &str = "multiple-linked-defaults";
pub const MULTIPLE_LINKED_DEFAULT_POOLS_ERR_MSG: &str = "\
Must specify an IP version when there is more than one default \
        Subnet Pool for the Silo";

#[cfg(test)]
mod tests {
    use super::insert_external_subnet_query;
    use super::insert_subnet_pool_member_query;
    use crate::db::explain::ExplainableAsync as _;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use chrono::TimeZone as _;
    use chrono::Utc;
    use nexus_db_model::ExternalSubnetIdentity;
    use nexus_db_model::SubnetPoolMember;
    use nexus_types::external_api::params::ExternalSubnetSelector;
    use nexus_types::external_api::params::PoolSelector;
    use nexus_types::external_api::params::SubnetPoolMemberCreate;
    use omicron_common::address::IpVersion;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::NameOrId;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ExternalSubnetUuid;
    use omicron_uuid_kinds::GenericUuid as _;
    use omicron_uuid_kinds::SubnetPoolMemberUuid;
    use omicron_uuid_kinds::SubnetPoolUuid;
    use uuid::Uuid;

    #[tokio::test]
    async fn expectorate_insert_subnet_pool_member_query() {
        let t = Utc.with_ymd_and_hms(2020, 1, 1, 12, 12, 12).unwrap();
        let pool_id = SubnetPoolUuid::from_untyped_uuid(uuid::uuid!(
            "3ab604dc-35d2-4ebd-8c0b-30973979e0c8"
        ));
        let params = SubnetPoolMemberCreate {
            pool: pool_id.into_untyped_uuid().into(),
            subnet: "10.1.0.0/16".parse().unwrap(),
            min_prefix_length: 16,
            max_prefix_length: 24,
        };
        let mut member = SubnetPoolMember::new(&params, pool_id).unwrap();
        // Set some data of the member to be consistent.
        member.id =
            SubnetPoolMemberUuid::from_untyped_uuid(SUBNET_MEMBER_ID).into();
        member.time_created = t;
        member.time_modified = t;
        expectorate_query_contents(
            insert_subnet_pool_member_query(&member),
            "tests/output/insert_subnet_pool_member.sql",
        )
        .await;
    }

    const SILO_ID: Uuid = uuid::uuid!("1a597381-b5a1-43da-bd72-bb1ac0786c21");
    const PROJECT_ID: Uuid =
        uuid::uuid!("e9c2d699-c60d-4486-81c6-63de6897a4ed");
    const SUBNET_ID: Uuid = uuid::uuid!("56ba77dc-6116-4537-a858-d3d56e34a222");
    const SUBNET_MEMBER_ID: Uuid =
        uuid::uuid!("b694e771-a21d-4022-9acf-1e1f5169945e");
    const SUBNET_POOL_ID: Uuid =
        uuid::uuid!("3d0525d9-4f7a-4fbd-bdca-f170aefb0499");

    #[tokio::test]
    async fn can_explain_insert_external_subnet_from_explicit_ip_query() {
        let logctx =
            dev::test_setup_log("can_explain_insert_external_subnet_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();
        let subnet = ExternalSubnetSelector::Explicit {
            subnet: "192.168.1.0/24".parse().unwrap(),
        };
        let identity = ExternalSubnetIdentity::new(
            ExternalSubnetUuid::from_untyped_uuid(SUBNET_ID),
            IdentityMetadataCreateParams {
                name: "ext".parse().unwrap(),
                description: String::new(),
            },
        );
        let query = insert_external_subnet_query(
            &SILO_ID,
            &PROJECT_ID,
            identity,
            &subnet,
        );
        let exp = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        println!("{exp}");
        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn expectorate_insert_external_subnet_query_impl(
        subnet: ExternalSubnetSelector,
        path: &str,
    ) {
        let identity = ExternalSubnetIdentity::new(
            ExternalSubnetUuid::from_untyped_uuid(SUBNET_ID),
            IdentityMetadataCreateParams {
                name: "ext".parse().unwrap(),
                description: String::new(),
            },
        );
        let query = insert_external_subnet_query(
            &SILO_ID,
            &PROJECT_ID,
            identity,
            &subnet,
        );
        expectorate_query_contents(query, path).await;
    }

    #[tokio::test]
    async fn expectorate_insert_external_subnet_from_explicit_subnet_query() {
        let subnet = ExternalSubnetSelector::Explicit {
            subnet: "192.168.1.0/24".parse().unwrap(),
        };
        let path =
            "tests/output/insert_external_subnet_from_explicit_subnet.sql";
        expectorate_insert_external_subnet_query_impl(subnet, path).await;
    }

    #[tokio::test]
    async fn expectorate_insert_external_subnet_from_explicit_pool_query() {
        let subnet = ExternalSubnetSelector::Auto {
            pool: PoolSelector::Explicit { pool: NameOrId::Id(SUBNET_POOL_ID) },
            prefix: 64,
        };
        let path = "tests/output/insert_external_subnet_from_explicit_pool.sql";
        expectorate_insert_external_subnet_query_impl(subnet, path).await;
    }

    #[tokio::test]
    async fn expectorate_insert_external_subnet_from_ip_version_query() {
        let subnet = ExternalSubnetSelector::Auto {
            pool: PoolSelector::Auto { ip_version: Some(IpVersion::V6) },
            prefix: 64,
        };
        let path = "tests/output/insert_external_subnet_from_ip_version.sql";
        expectorate_insert_external_subnet_query_impl(subnet, path).await;
    }

    #[tokio::test]
    async fn expectorate_insert_external_subnet_from_default_pool_query() {
        let subnet = ExternalSubnetSelector::Auto {
            pool: PoolSelector::Auto { ip_version: None },
            prefix: 64,
        };
        let path = "tests/output/insert_external_subnet_from_default_pool.sql";
        expectorate_insert_external_subnet_query_impl(subnet, path).await;
    }
}
