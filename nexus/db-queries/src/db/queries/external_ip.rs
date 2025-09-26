// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for operating on external IP addresses from IP
//! Pools.

use crate::db::model::ExternalIp;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::IpKind;
use crate::db::model::Name;
use crate::db::true_or_cast_error::{TrueOrCastError, matches_sentinel};
use chrono::DateTime;
use chrono::Utc;
use diesel::Column;
use diesel::Expression;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_lookup::DbConnection;
use nexus_db_model::InstanceState as DbInstanceState;
use nexus_db_model::IpAttachState;
use nexus_db_model::VmmState as DbVmmState;
use nexus_db_schema::enums::IpAttachStateEnum;
use nexus_db_schema::enums::IpKindEnum;
use nexus_db_schema::schema;
use omicron_common::address::NUM_SOURCE_NAT_PORTS;
use omicron_common::api::external;
use uuid::Uuid;

// Broadly, we want users to be able to attach/detach at will
// once an instance is created and functional.
pub const SAFE_TO_ATTACH_INSTANCE_STATES_CREATING: [DbInstanceState; 3] =
    [DbInstanceState::NoVmm, DbInstanceState::Vmm, DbInstanceState::Creating];
pub const SAFE_TO_ATTACH_INSTANCE_STATES: [DbInstanceState; 2] =
    [DbInstanceState::NoVmm, DbInstanceState::Vmm];
// If we're in a state which will naturally resolve to either
// stopped/running, we want users to know that the request can be
// retried safely via Error::unavail.
// TODO: We currently stop if there's a migration or other state change.
//       There may be a good case for RPWing
//       external_ip_state -> { NAT RPW, sled-agent } in future.
pub const SAFE_TRANSIENT_INSTANCE_STATES: [DbVmmState; 4] = [
    DbVmmState::Starting,
    DbVmmState::Stopping,
    DbVmmState::Rebooting,
    DbVmmState::Migrating,
];

/// The maximum number of disks that can be attached to an instance.
pub const MAX_EXTERNAL_IPS_PER_INSTANCE: u32 = 32;

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type IpPoolRangeFromClause = FromClause<schema::ip_pool_range::table>;
const IP_POOL_RANGE_FROM_CLAUSE: IpPoolRangeFromClause =
    IpPoolRangeFromClause::new();
type ExternalIpFromClause = FromClause<schema::external_ip::table>;
const EXTERNAL_IP_FROM_CLAUSE: ExternalIpFromClause =
    ExternalIpFromClause::new();

const REALLOCATION_WITH_DIFFERENT_IP_SENTINEL: &'static str =
    "Reallocation of IP with different value";

/// Translates a generic pool error to an external error.
pub fn from_diesel(e: DieselError) -> external::Error {
    let sentinels = [REALLOCATION_WITH_DIFFERENT_IP_SENTINEL];
    if let Some(sentinel) = matches_sentinel(&e, &sentinels) {
        match sentinel {
            REALLOCATION_WITH_DIFFERENT_IP_SENTINEL => {
                return external::Error::invalid_request(
                    "Re-allocating IP address with a different value",
                );
            }
            // Fall-through to the generic error conversion.
            _ => {}
        }
    }

    nexus_db_errors::public_error_from_diesel(
        e,
        nexus_db_errors::ErrorHandler::Server,
    )
}

const MAX_PORT: u16 = u16::MAX;

/// Select the next available IP address and port range for an instance's
/// external connectivity.
///
/// All guest instances by default are able to make outbound network
/// connections, for example to `ping 8.8.8.8`. That requires an external IP
/// address, selected from an IP Pool maintained by rack operators. This query
/// can be used to select a portion of the full port-range of one IP address,
/// reserving them for use by an instance.
///
/// In general, the query:
///
/// - Selects the next available IP address and port range from the specified IP pool
/// - Inserts that record into the `external_ip` table
/// - Updates the rcgen and time modified of the parent `ip_pool_range` table
///
/// In detail, the query is:
///
/// ```sql
/// WITH next_external_ip AS (
///     -- Create a new IP address record
///     SELECT
///         <ip_id> AS id,
///         <name> AS name,
///         <description> AS description,
///         <now> AS time_created,
///         <now> AS time_modified,
///         NULL AS time_deleted,
///         ip_pool_id,
///         ip_pool_range_id,
///         <is_service> AS is_service,
///         <parent_id> AS parent_id,
///         <kind> AS kind,
///         candidate_ip AS ip,
///         CAST(candidate_first_port AS INT4) AS first_port,
///         CAST(candidate_last_port AS INT4) AS last_port,
///         <project_id> AS project_id,
///         <is_probe> AS is_probe,
///         <state> AS state
///     FROM
///         SELECT * FROM (
///             -- Select all IP addresses by pool and range.
///             SELECT
///                 ip_pool_id,
///                 id AS ip_pool_range_id,
///                 <Candidate IP Query> AS candidate_ip
///             FROM
///                 ip_pool_range
///             WHERE
///                 <pool restriction clause> AND
///                 time_deleted IS NULL
///         ) AS candidates
///         WHERE candidates.candidate_ip IS NOT NULL
///     CROSS JOIN
///         (
///             -- Cartesian product with all first/last port values
///             SELECT
///                 candidate_first_port,
///                 candidate_first_port +
///                     <NUM_SOURCE_NAT_PORTS - 1>
///                     AS candidate_last_port
///             FROM
///                 generate_series(0, <MAX_PORT>, <NUM_SOURCE_NAT_PORTS>)
///                     AS candidate_first_port
///         )
///     LEFT OUTER JOIN
///         -- Join with existing IPs, selecting the first row from the
///         -- address and port sequence subqueries that has no match. I.e.,
///         -- is not yet reserved.
///         external_ip
///     ON
///         -- The JOIN conditions depend on the IP kind:
///
///         -- For Floating and Ephemeral IPs, we need to reserve the entire
///         -- port range which makes the condition pretty simple: We don't
///         -- care what the port is, any record with that IP is considered
///         -- a match.
///         (ip, time_deleted IS NULL) = (candidate_ip, TRUE)
///
///         -- SNAT IPs are a little more complicated; we need to prevent
///         -- SNAT IPs from "carving out" any part of the port range of an
///         -- IP if there is an Ephemeral or Floating IP with the same
///         -- address. However, we want to _allow_ taking the next available
///         -- chunk of ports when there is an existing SNAT IP with the same
///         -- address. We do this by preventing _overlapping_ port ranges
///         -- within the same IP.
///         (
///             ip,
///             candidate_first_port >= first_port AND
///                 candidate_last_port <= last_port AND
///                 time_deleted IS NULL
///         ) = (candidate_ip, TRUE)
///     WHERE
///         (ip IS NULL) OR (id = <ip_id>)
///     ORDER BY
///         candidate_ip, candidate_first_port
///     LIMIT 1
/// ),
/// -- Identify if the IP address has already been allocated, to validate
/// -- that the request looks the same (for idempotency).
/// previously_allocated_ip AS (
///     SELECT
///         id as old_id,
///         ip as old_ip,
///         first_port as old_first_port,
///         last_port as old_last_port
///     FROM external_ip
///     WHERE
///         id = <id> AND time_deleted IS NULL
/// ),
/// -- Compare `next_external_ip` with `previously_allocated_ip`, and throw
/// -- an error if the request is not idempotent.
/// validate_prior_allocation AS MATERIALIZED (
///     CAST(
///         -- If this expression evaluates to false, we throw an error.
///         IF(
///             -- Either the previously_allocated_ip does not exist, or...
///             NOT EXISTS(SELECT 1 FROM previously_allocated_ip) OR
///             -- ... If it does exist, the IP address must be the same for
///             -- both the old and new request.
///             (
///                 SELECT
///                    ip = old_ip AND
///                    first_port = old_first_port AND
///                    last_port = old_last_port
///                 FROM
///                     (
///                         SELECT
///                             ip, first_port, last_port,
///                             old_ip, old_first_port, old_last_port
///                         FROM next_external_ip
///                         INNER JOIN
///                         previously_allocated_ip ON old_id = id
///                     )
///             ),
///             TRUE,
///             <Error Message> AS BOOL
///         )
///     )
/// ),
/// external_ip AS (
///     -- Insert the record into the actual table.
///     -- When a conflict is detected, we'll update the timestamps but leave
///     -- everything else as it exists in the record. This should only be
///     -- possible on replay of a saga node.
///     INSERT INTO
///         external_ip
///     (SELECT * FROM next_external_ip)
///     ON CONFLICT (id)
///     DO UPDATE SET
///         time_created = excluded.time_created,
///         time_modified = excluded.time_modified,
///         time_deleted = excluded.time_deleted
///     RETURNING *
/// ),
/// updated_pool AS (
///     UPDATE SET
///         ip_pool_range
///     SET
///         time_modified = NOW(),
///         rcgen = rcgen + 1
///     WHERE
///         id = (SELECT ip_pool_id FROM next_external_ip) AND
///         time_deleted IS NULL
///     RETURNING id
/// )
/// SELECT * FROM external_ip;
/// ```
///
/// Performance notes
/// -----------------
///
/// This query currently searches _all_ available IP Pools and _all_ contained
/// ranges. It is similar to the common "next item" queries, in that its
/// worst-case peformance is proportional to the total number of IP addresses in
/// all pools. Specifically, its runtime is proportional to the lowest
/// unallocated address in any IP pool. However, it's important to note that
/// Cockroach currently completely runs all subqueries and caches their results
/// in memory, meaning this query will need to be redesigned to limit the
/// number of results it checks. As an example, suppose there were a large
/// number of IP pools each containing an IPv6 /64 subnet. That's 2**64 items
/// _per_ pool.
///
/// A good way to start limiting this is cap the address-search subquery. It's
/// not clear what that limit should be though.
///
/// Casting
/// -------
///
/// There are a couple of explicit `CAST` expressions above, which are required.
/// The `generate_series` call creating the port ranges defaults to generating
/// 64-bit integer values, since the call isn't tied to any particular table
/// (even though we send the start/stop to the database as `i32`s). We need to
/// cast it on the way out of the database, so that Diesel can correctly
/// deserialize it into an `i32`.
///
/// Pool restriction
/// ----------------
///
/// Clients must supply the UUID of an IP pool from which they are allocating.
#[derive(Debug, Clone)]
pub struct NextExternalIp {
    ip: IncompleteExternalIp,
    // Number of ports reserved per IP address. Only applicable if the IP kind
    // is snat.
    n_ports_per_chunk: i32,
    // The offset from the first port to the last, inclusive. This is required
    // because the ranges must not overlap and `generate_series` is inclusive of
    // its endpoints.
    last_port_offset: i32,
    now: DateTime<Utc>,
}

impl NextExternalIp {
    pub fn new(ip: IncompleteExternalIp) -> Self {
        let now = Utc::now();
        let n_ports_per_chunk = i32::from(NUM_SOURCE_NAT_PORTS);
        Self {
            ip,
            n_ports_per_chunk,
            last_port_offset: n_ports_per_chunk - 1,
            now,
        }
    }

    fn push_next_ip_and_port_range_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::external_ip::dsl;
        out.push_sql("SELECT ");

        // NOTE: These columns must be selected in the order in which they
        // appear in the table, to avoid needing to push the explicit column
        // names in the RETURNING clause and final SELECT from the CTE.

        // Id
        out.push_bind_param::<sql_types::Uuid, Uuid>(self.ip.id())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");

        // Name, possibly null
        out.push_bind_param::<sql_types::Nullable<sql_types::Text>, Option<Name>>(self.ip.name())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");

        // Description, possibly null
        out.push_bind_param::<sql_types::Nullable<sql_types::Text>, Option<String>>(self.ip.description())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");

        // Time-created
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            self.ip.time_created(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");

        // Time-modified, same as created
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            self.ip.time_created(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");

        // Time-deleted
        out.push_bind_param::<sql_types::Nullable<sql_types::Timestamptz>, Option<DateTime<Utc>>>(&None)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(", ");

        // Pool ID
        out.push_identifier(dsl::ip_pool_id::NAME)?;
        out.push_sql(", ");

        // Pool Range ID
        out.push_identifier(dsl::ip_pool_range_id::NAME)?;
        out.push_sql(", ");

        // is_service flag
        out.push_bind_param::<sql_types::Bool, bool>(self.ip.is_service())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::is_service::NAME)?;
        out.push_sql(", ");

        // Parent (Instance/Service) ID
        out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, Option<Uuid>>(self.ip.parent_id())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::parent_id::NAME)?;
        out.push_sql(", ");

        // IP kind
        out.push_bind_param::<IpKindEnum, IpKind>(self.ip.kind())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::kind::NAME)?;
        out.push_sql(", ");

        // Candidate IP from the subquery
        out.push_sql("candidate_ip AS ");
        out.push_identifier(dsl::ip::NAME)?;

        // Candidate first / last port from the subquery
        out.push_sql(", CAST(candidate_first_port AS INT4) AS ");
        out.push_identifier(dsl::first_port::NAME)?;
        out.push_sql(", CAST(candidate_last_port AS INT4) AS ");
        out.push_identifier(dsl::last_port::NAME)?;
        out.push_sql(", ");

        // Project ID, possibly null
        out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, Option<Uuid>>(self.ip.project_id())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::project_id::NAME)?;
        out.push_sql(", ");

        // Initial state, mainly needed by Ephemeral/Floating IPs.
        out.push_bind_param::<IpAttachStateEnum, IpAttachState>(
            self.ip.state(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::state::NAME)?;
        out.push_sql(", ");

        // is_probe flag
        out.push_bind_param::<sql_types::Bool, bool>(self.ip.is_probe())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::is_probe::NAME)?;

        out.push_sql(" FROM (");
        self.push_address_sequence_subquery(out.reborrow())?;
        out.push_sql(") CROSS JOIN (");
        self.push_port_sequence_subquery(out.reborrow())?;
        out.push_sql(") LEFT OUTER JOIN ");
        EXTERNAL_IP_FROM_CLAUSE.walk_ast(out.reborrow())?;

        // The JOIN conditions depend on the IP type. For automatic SNAT IP
        // addresses, we need to consider existing records with their port
        // ranges. That's because we want to allow providing two different
        // chunks of ports from the same IP to two different guests or services.
        //
        // However, for Floating and Ephemeral IPs, we need to reserve the
        // entire port range. An Instance or Service may start listening on
        // any port, and we need to allow inbound connections to that port.
        // (It can't be rewritten on the way in.)
        //
        // The second case is much simpler, so let's start with that.
        //
        // ```sql
        // ON (ip, time_deleted IS NULL) = (candidate_ip, TRUE)
        // ```
        //
        // Here, we don't care what the port is. Any record in the table that
        // has that IP should be considered a match. This prevents creating
        // Ephemeral or Floating IPs when there is any other record with the
        // same address, even an SNAT IP only consuming a portion of the port
        // range.
        //
        // Now for the first case, SNAT. Here, we need to prevent SNAT IPs from
        // "carving out" any part of the port range of an IP if there is an
        // Ephemeral or Floating IP with the same address. However, we want to
        // _allow_ taking the next available chunk of ports when there is an
        // existing SNAT IP with the same address.
        //
        // This is done by preventing _overlapping_ port ranges within the same
        // IP. There's a clause in the JOIN condition:
        //
        // ```
        // candidate_first_port >= first_port
        // AND
        // candidate_last_port <= last_port
        // ```
        //
        // which, if `TRUE`, results in a match in the left outer join. That is,
        // if there is any existing address whose port range contains the
        // candidate port range, we return that as a match. That means we cannot
        // double-allocate matching port ranges for an SNAT address, nor can we
        // double-allocate a port range that's already been claimed by an
        // Ephemeral or Floating address.
        //
        // In either case, we follow this with a filter `WHERE ip IS NULL`,
        // meaning we select the candidate address and first port that does not
        // have a matching record in the table already.
        out.push_sql(" ON (");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(",");
        if matches!(self.ip.kind(), &IpKind::SNat) {
            out.push_sql(" candidate_first_port >= ");
            out.push_identifier(dsl::first_port::NAME)?;
            out.push_sql(" AND candidate_last_port <= ");
            out.push_identifier(dsl::last_port::NAME)?;
            out.push_sql(" AND ");
        }
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL) = (candidate_ip, TRUE) ");

        // In all cases, we're selecting rows from the join that don't have a
        // match in the existing table.
        //
        // This is a bit subtle. The join condition considers rows a match if
        // the `time_deleted` is null. That means that if the record has been
        // soft-deleted, it won't be considered a match, and thus both the IP
        // and first port (in the join result) will be null. Note that there
        // _is_ a record in the `external_ip` table that has that IP
        // and possibly first port, but since it's been soft-deleted, it's not a
        // match. In that case, we can get away with _only_ filtering the join
        // results on the IP from the `external_ip` table being NULL.
        out.push_sql(" WHERE (");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(" IS NULL) OR (");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(self.ip.id())?;
        out.push_sql(
            ") \
            ORDER BY candidate_ip, candidate_first_port \
            LIMIT 1",
        );

        Ok(())
    }

    fn push_prior_allocation_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::external_ip::dsl;

        out.push_sql("SELECT ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" AS old_id, ");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(" AS old_ip, ");
        out.push_identifier(dsl::first_port::NAME)?;
        out.push_sql(" AS old_first_port, ");
        out.push_identifier(dsl::last_port::NAME)?;
        out.push_sql(" AS old_last_port FROM ");
        EXTERNAL_IP_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(self.ip.id())?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL");

        Ok(())
    }

    fn push_validate_prior_allocation_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        // Describes a boolean expression within the CTE to check if the "old IP
        // address" matches the "to-be-allocated" IP address.
        //
        // This validates that, in a case where we're re-allocating the same IP,
        // we don't attempt to assign a different value.
        struct CheckIfOldAllocationHasSameIp {}
        impl Expression for CheckIfOldAllocationHasSameIp {
            type SqlType = diesel::sql_types::Bool;
        }
        impl QueryFragment<Pg> for CheckIfOldAllocationHasSameIp {
            fn walk_ast<'a>(
                &'a self,
                mut out: AstPass<'_, 'a, Pg>,
            ) -> diesel::QueryResult<()> {
                use schema::external_ip::dsl;
                out.unsafe_to_cache_prepared();
                // Either the allocation to this UUID needs to be new...
                out.push_sql(
                    "NOT EXISTS(SELECT 1 FROM previously_allocated_ip) OR",
                );
                // ... Or we are allocating the same IP address...
                out.push_sql("(SELECT ");
                out.push_identifier(dsl::ip::NAME)?;
                out.push_sql(" = old_ip AND ");
                // (as well as the same port range)
                out.push_identifier(dsl::first_port::NAME)?;
                out.push_sql(" = old_first_port AND ");
                out.push_identifier(dsl::last_port::NAME)?;
                out.push_sql(" = old_last_port FROM (SELECT ");
                out.push_identifier(dsl::ip::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::first_port::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::last_port::NAME)?;
                out.push_sql(
                    ", old_ip, old_first_port, old_last_port \
                    FROM next_external_ip INNER JOIN previously_allocated_ip \
                    ON old_id = id))",
                );
                Ok(())
            }
        }

        out.push_sql("SELECT ");

        const QUERY: TrueOrCastError<CheckIfOldAllocationHasSameIp> =
            TrueOrCastError::new(
                CheckIfOldAllocationHasSameIp {},
                REALLOCATION_WITH_DIFFERENT_IP_SENTINEL,
            );
        QUERY.walk_ast(out.reborrow())?;
        Ok(())
    }

    // Push a subquery which selects either:
    // - A sequence of candidate IP addresses from the IP pool range, if no
    // explicit IP address has been supplied, or
    // - A single IP address within the range, if an explicit IP address has
    // been supplied.
    //
    // ```sql
    // SELECT * FROM (
    //   SELECT
    //       ip_pool_id,
    //       id AS ip_pool_range_id,
    //       -- Candidates with no explicit IP:
    //       first_address + generate_series(0, last_address - first_address)
    //       -- Candidates with explicit IP:
    //       CASE
    //          first_address <= <explicit_ip> AND
    //          <explicit_ip> <= last_address
    //       WHEN TRUE THEN <explicit_ip> ELSE NULL END
    //       -- Either way:
    //           AS candidate_ip
    //   FROM
    //       ip_pool_range
    //   WHERE
    //       <pool_restriction> AND
    //       time_deleted IS NULL
    // ) AS candidates
    // WHERE candidates.candidate_ip IS NOT NULL
    // ```
    fn push_address_sequence_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::ip_pool_range::dsl;
        out.push_sql("SELECT * FROM (");

        out.push_sql("SELECT ");
        out.push_identifier(dsl::ip_pool_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" AS ip_pool_range_id, ");

        if let Some(explicit_ip) = self.ip.explicit_ip() {
            out.push_sql("CASE ");
            out.push_identifier(dsl::first_address::NAME)?;
            out.push_sql(" <= ");
            out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
                explicit_ip,
            )?;
            out.push_sql(" AND ");
            out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
                explicit_ip,
            )?;
            out.push_sql(" <= ");
            out.push_identifier(dsl::last_address::NAME)?;
            out.push_sql(" WHEN TRUE THEN ");
            out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
                explicit_ip,
            )?;
            out.push_sql(" ELSE NULL END");
        } else {
            out.push_identifier(dsl::first_address::NAME)?;
            out.push_sql(" + generate_series(0, ");
            out.push_identifier(dsl::last_address::NAME)?;
            out.push_sql(" - ");
            out.push_identifier(dsl::first_address::NAME)?;
            out.push_sql(") ");
        }

        out.push_sql(" AS candidate_ip FROM ");
        IP_POOL_RANGE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::ip_pool_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(self.ip.pool_id())?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL");
        out.push_sql(") AS candidates ");
        out.push_sql("WHERE candidates.candidate_ip IS NOT NULL");
        Ok(())
    }

    // Push a subquery that selects the possible values for a first port, based on
    // the defined spacing. Note that there are two forms, depending on whether
    // the IP type we're allocating for is Floating/Ephemeral, or an SNAT IP
    // address.
    //
    // For SNAT addresses, we want to provide port ranges. Those ranges must not
    // overlap between different records, but there will be more than one record
    // with the same IP. This subquery then generates the port-range chunks
    // sequentially.
    //
    // ```sql
    // SELECT
    //     candidate_first_port,
    //     candidate_first_port +
    //         <NUM_SOURCE_NAT_PORTS - 1>
    //         AS candidate_last_port
    // FROM
    //     generate_series(0, <MAX_PORT>, <NUM_SOURCE_NAT_PORTS>)
    //         AS candidate_first_port
    // ```
    //
    // If an explicit port range is requested, we generate a simple
    // SELECT clause:
    //
    // ```sql
    // SELECT
    //     <explicit_first_port> AS candidate_first_port,
    //     <explicit_last_port> AS candidate_last_port
    // ```
    //
    // For Floating or Ephemeral IP addresses, we reserve the entire port range
    // for the guest/service. In this case, we generate the static values 0 and 65535:
    //
    // ```sql
    // SELECT
    //     0 AS candidate_first_port,
    //     65535 AS candidate_last_port
    // ```
    fn push_port_sequence_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        const MAX_PORT: i32 = self::MAX_PORT as i32;
        if let IpKind::SNat = self.ip.kind() {
            if let Some((first_port, last_port)) =
                &self.ip.explicit_port_range()
            {
                out.push_sql("SELECT ");
                out.push_bind_param::<sql_types::Int4, i32>(first_port)?;
                out.push_sql(" AS candidate_first_port, ");
                out.push_bind_param::<sql_types::Int4, i32>(last_port)?;
                out.push_sql(" AS candidate_last_port");
            } else {
                out.push_sql(
                    "SELECT candidate_first_port, candidate_first_port + ",
                );
                out.push_bind_param::<sql_types::Int4, i32>(
                    &self.last_port_offset,
                )?;
                out.push_sql(
                    " AS candidate_last_port FROM generate_series(0, ",
                );
                out.push_bind_param::<sql_types::Int4, i32>(&MAX_PORT)?;
                out.push_sql(", ");
                out.push_bind_param::<sql_types::Int4, i32>(
                    &self.n_ports_per_chunk,
                )?;
                out.push_sql(") AS candidate_first_port");
            }
        } else {
            out.push_sql("SELECT 0 AS candidate_first_port, ");
            out.push_bind_param::<sql_types::Int4, i32>(&MAX_PORT)?;
            out.push_sql(" AS candidate_last_port");
        }
        Ok(())
    }

    // Push a subquery to update the `ip_pool_range` table's rcgen, if we've
    // successfully allocate an IP from that range.
    fn push_update_ip_pool_range_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::ip_pool_range::dsl;
        out.push_sql("UPDATE ");
        IP_POOL_RANGE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.now,
        )?;
        out.push_sql(", ");
        out.push_identifier(dsl::rcgen::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(dsl::rcgen::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = (SELECT ");
        out.push_identifier(schema::external_ip::ip_pool_range_id::NAME)?;
        out.push_sql(" FROM next_external_ip) AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL RETURNING ");
        out.push_identifier(dsl::id::NAME)?;
        Ok(())
    }

    // Push the subquery that updates the actual `external_ip` table
    // with the candidate record created in the main `next_external_ip` CTE and
    // returns it. Note that this may just update the timestamps, if a record
    // with the same primary key is found.
    fn push_update_external_ip_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql("INSERT INTO ");
        EXTERNAL_IP_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(
            " (SELECT * FROM next_external_ip) \
            ON CONFLICT (id) \
            DO UPDATE SET \
                time_created = excluded.time_created,
                time_modified = excluded.time_modified,
                time_deleted = excluded.time_deleted
            RETURNING *",
        );
        Ok(())
    }
}

impl QueryId for NextExternalIp {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for NextExternalIp {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Push the first CTE, creating the candidate record by selecting the
        // next available IP address and port range, across all IP Pools and
        // their IP address ranges.
        out.push_sql("WITH next_external_ip AS (");
        self.push_next_ip_and_port_range_subquery(out.reborrow())?;
        out.push_sql("), ");

        out.push_sql("previously_allocated_ip AS (");
        self.push_prior_allocation_subquery(out.reborrow())?;
        out.push_sql("), ");
        out.push_sql("validate_previously_allocated_ip AS MATERIALIZED(");
        self.push_validate_prior_allocation_subquery(out.reborrow())?;
        out.push_sql("), ");

        // Push the subquery that potentially inserts this record, or ignores
        // primary key conflicts (for idempotency).
        out.push_sql("external_ip AS (");
        self.push_update_external_ip_subquery(out.reborrow())?;
        out.push_sql("), ");

        // Push the subquery that bumps the `rcgen` of the IP Pool range table
        out.push_sql("updated_pool_range AS (");
        self.push_update_ip_pool_range_subquery(out.reborrow())?;
        out.push_sql(") ");

        // Select the contents of the actual record that was created or updated.
        out.push_sql("SELECT * FROM external_ip");

        Ok(())
    }
}

impl Query for NextExternalIp {
    type SqlType = <<ExternalIp as
        diesel::Selectable<Pg>>::SelectExpression as diesel::Expression>::SqlType;
}

impl RunQueryDsl<DbConnection> for NextExternalIp {}

#[cfg(test)]
mod tests {
    use crate::authz;
    use crate::db::datastore::SERVICE_IPV4_POOL_NAME;
    use crate::db::identity::Resource;
    use crate::db::model::IpKind;
    use crate::db::model::IpPool;
    use crate::db::model::IpPoolRange;
    use crate::db::pub_test_utils::TestDatabase;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use dropshot::test_util::LogContext;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::ByteCount;
    use nexus_db_model::Instance;
    use nexus_db_model::InstanceCpuCount;
    use nexus_db_model::IpPoolResource;
    use nexus_db_model::IpPoolResourceType;
    use nexus_db_model::IpVersion;
    use nexus_sled_agent_shared::inventory::ZoneKind;
    use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    use nexus_types::deployment::OmicronZoneExternalIp;
    use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use nexus_types::external_api::params::InstanceCreate;
    use nexus_types::external_api::shared::IpRange;
    use nexus_types::inventory::SourceNatConfig;
    use omicron_common::address::NUM_SOURCE_NAT_PORTS;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use uuid::Uuid;

    struct TestContext {
        logctx: LogContext,
        db: TestDatabase,
    }

    impl TestContext {
        async fn new(test_name: &str) -> Self {
            let logctx = dev::test_setup_log(test_name);
            let log = logctx.log.new(o!());
            let db = TestDatabase::new_with_datastore(&log).await;
            Self { logctx, db }
        }

        /// Create pool, associate with current silo
        async fn create_ip_pool(
            &self,
            name: &str,
            range: IpRange,
            is_default: bool,
        ) -> authz::IpPool {
            let pool = IpPool::new(
                &IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: format!("ip pool {}", name),
                },
                IpVersion::V4,
            );

            self.db
                .datastore()
                .ip_pool_create(self.db.opctx(), pool.clone())
                .await
                .expect("Failed to create IP pool");

            let silo_id = self.db.opctx().authn.silo_required().unwrap().id();
            let association = IpPoolResource {
                resource_id: silo_id,
                resource_type: IpPoolResourceType::Silo,
                ip_pool_id: pool.id(),
                is_default,
            };
            self.db
                .datastore()
                .ip_pool_link_silo(self.db.opctx(), association)
                .await
                .expect("Failed to associate IP pool with silo");

            self.initialize_ip_pool(name, range).await;

            LookupPath::new(self.db.opctx(), self.db.datastore())
                .ip_pool_id(pool.id())
                .lookup_for(authz::Action::Read)
                .await
                .unwrap()
                .0
        }

        async fn initialize_ip_pool(&self, name: &str, range: IpRange) {
            // Find the target IP pool
            use nexus_db_schema::schema::ip_pool::dsl as ip_pool_dsl;
            let conn = self
                .db
                .datastore()
                .pool_connection_authorized(self.db.opctx())
                .await
                .unwrap();
            let pool = ip_pool_dsl::ip_pool
                .filter(ip_pool_dsl::name.eq(name.to_string()))
                .filter(ip_pool_dsl::time_deleted.is_null())
                .select(IpPool::as_select())
                .get_result_async(&*conn)
                .await
                .expect("Failed to 'SELECT' IP Pool");

            // Insert a range into this IP pool
            let pool_range = IpPoolRange::new(&range, pool.id());
            diesel::insert_into(
                nexus_db_schema::schema::ip_pool_range::dsl::ip_pool_range,
            )
            .values(pool_range)
            .execute_async(
                &*self
                    .db
                    .datastore()
                    .pool_connection_authorized(self.db.opctx())
                    .await
                    .unwrap(),
            )
            .await
            .expect("Failed to create IP Pool range");
        }

        async fn create_instance(&self, name: &str) -> InstanceUuid {
            let instance_id = InstanceUuid::new_v4();
            let project_id = Uuid::new_v4();
            let instance = Instance::new(instance_id, project_id, &InstanceCreate {
                identity: IdentityMetadataCreateParams { name: String::from(name).parse().unwrap(), description: format!("instance {}", name) },
                ncpus: InstanceCpuCount(omicron_common::api::external::InstanceCpuCount(1)).into(),
                memory: ByteCount(omicron_common::api::external::ByteCount::from_gibibytes_u32(1)).into(),
                hostname: "test".parse().unwrap(),
                ssh_public_keys: None,
                user_data: vec![],
                network_interfaces: Default::default(),
                external_ips: vec![],
                disks: vec![],
                boot_disk: None,
                cpu_platform: None,
                start: false,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
                multicast_groups: Vec::new(),
            });

            let conn = self
                .db
                .datastore()
                .pool_connection_authorized(self.db.opctx())
                .await
                .unwrap();

            use nexus_db_schema::schema::instance::dsl as instance_dsl;
            diesel::insert_into(instance_dsl::instance)
                .values(instance.clone())
                .execute_async(&*conn)
                .await
                .expect("Failed to create Instance");

            instance_id
        }

        async fn default_pool_id(&self) -> Uuid {
            let (.., pool) = self
                .db
                .datastore()
                .ip_pools_fetch_default(self.db.opctx())
                .await
                .expect("Failed to lookup default ip pool");
            pool.identity.id
        }

        async fn success(self) {
            self.db.terminate().await;
            self.logctx.cleanup_successful();
        }
    }

    #[tokio::test]
    async fn test_next_external_ip_allocation_and_exhaustion() {
        let context =
            TestContext::new("test_next_external_ip_allocation_and_exhaustion")
                .await;
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 1),
        ))
        .unwrap();
        context.create_ip_pool("default", range, true).await;
        for first_port in
            (0..super::MAX_PORT).step_by(NUM_SOURCE_NAT_PORTS.into())
        {
            let id = Uuid::new_v4();
            let instance_id = InstanceUuid::new_v4();
            let ip = context
                .db
                .datastore()
                .allocate_instance_snat_ip(
                    context.db.opctx(),
                    id,
                    instance_id,
                    context.default_pool_id().await,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.ip.ip(), range.first_address());
            assert_eq!(ip.first_port.0, first_port);
            assert_eq!(ip.last_port.0, first_port + (NUM_SOURCE_NAT_PORTS - 1));
        }

        // The next allocation should fail, due to IP exhaustion
        let instance_id = InstanceUuid::new_v4();
        let err = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                context.default_pool_id().await,
            )
            .await
            .expect_err(
                "An error should be received when the IP pools are exhausted",
            );
        assert_eq!(
            err,
            Error::insufficient_capacity(
                "No external IP addresses available",
                "NextExternalIp::new returned NotFound",
            ),
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_ephemeral_and_snat_ips_do_not_overlap() {
        let context =
            TestContext::new("test_ephemeral_and_snat_ips_do_not_overlap")
                .await;
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 1),
        ))
        .unwrap();
        context.create_ip_pool("default", range, true).await;

        // Allocate an Ephemeral IP, which should take the entire port range of
        // the only address in the pool.
        let instance_id = context.create_instance("for-eph").await;
        let ephemeral_ip = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                /* pool_name = */ None,
                true,
            )
            .await
            .expect("Failed to allocate Ephemeral IP when there is space")
            .0;
        assert_eq!(ephemeral_ip.ip.ip(), range.last_address());
        assert_eq!(ephemeral_ip.first_port.0, 0);
        assert_eq!(ephemeral_ip.last_port.0, super::MAX_PORT);

        // At this point, we should be able to allocate neither a new Ephemeral
        // nor any SNAT IPs.
        let instance_id = context.create_instance("for-snat").await;
        let res = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                context.default_pool_id().await,
            )
            .await;
        assert!(
            res.is_err(),
            "Expected an allocation of an SNAT IP to fail, \
            but found {:#?}",
            res.unwrap(),
        );
        assert_eq!(
            res.unwrap_err(),
            Error::insufficient_capacity(
                "No external IP addresses available",
                "NextExternalIp::new returned NotFound",
            ),
        );

        let res = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                /* pool_name = */ None,
                true,
            )
            .await;
        assert!(
            res.is_err(),
            "Expected an allocation of an Ephemeral IP to fail, \
            but found {:#?}",
            res.unwrap(),
        );
        assert_eq!(
            res.unwrap_err(),
            Error::insufficient_capacity(
                "No external IP addresses available",
                "NextExternalIp::new returned NotFound",
            ),
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_out_of_order_allocation_ok() {
        let context = TestContext::new(
            "test_next_external_ip_out_of_order_allocation_ok",
        )
        .await;
        // Need a larger range, since we're currently limited to the whole port
        // range for each external IP.
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("default", range, true).await;

        // TODO-completeness: Implementing Iterator for IpRange would be nice.
        let addresses = [
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(10, 0, 0, 3),
        ];
        let ports = (0..super::MAX_PORT).step_by(NUM_SOURCE_NAT_PORTS.into());
        let mut external_ips = itertools::iproduct!(addresses, ports);

        // Allocate two addresses
        let mut ips = Vec::with_capacity(2);
        for (expected_ip, expected_first_port) in external_ips.clone().take(2) {
            let instance_id = InstanceUuid::new_v4();
            let ip = context
                .db
                .datastore()
                .allocate_instance_snat_ip(
                    context.db.opctx(),
                    Uuid::new_v4(),
                    instance_id,
                    context.default_pool_id().await,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.ip.ip(), expected_ip);
            assert_eq!(ip.first_port.0, expected_first_port);
            let expected_last_port =
                expected_first_port + (NUM_SOURCE_NAT_PORTS - 1);
            assert_eq!(ip.last_port.0, expected_last_port);
            ips.push(ip);
        }

        // Release the first
        context
            .db
            .datastore()
            .deallocate_external_ip(context.db.opctx(), ips[0].id)
            .await
            .expect("Failed to release the first external IP address");

        // Allocate a new one, ensure it's the same as the first one we
        // released.
        let instance_id = InstanceUuid::new_v4();
        let ip = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                context.default_pool_id().await,
            )
            .await
            .expect("Failed to allocate instance external IP address");
        println!("{:?}\n{:?}", ip, ips[0]);
        assert_eq!(
            ip.ip, ips[0].ip,
            "Expected to reallocate external IPs sequentially"
        );
        assert_eq!(
            ip.first_port, ips[0].first_port,
            "Expected to reallocate external IPs sequentially"
        );
        assert_eq!(
            ip.last_port, ips[0].last_port,
            "Expected to reallocate external IPs sequentially"
        );

        // Allocate one more, ensure it's the next chunk after the second one
        // from the original loop.
        let instance_id = InstanceUuid::new_v4();
        let ip = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                context.default_pool_id().await,
            )
            .await
            .expect("Failed to allocate instance external IP address");
        let (expected_ip, expected_first_port) = external_ips.nth(2).unwrap();
        assert_eq!(ip.ip.ip(), std::net::IpAddr::from(expected_ip));
        assert_eq!(ip.first_port.0, expected_first_port);
        let expected_last_port =
            expected_first_port + (NUM_SOURCE_NAT_PORTS - 1);
        assert_eq!(ip.last_port.0, expected_last_port);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_with_ephemeral_takes_whole_port_range() {
        let context = TestContext::new(
            "test_next_external_ip_with_ephemeral_takes_whole_port_range",
        )
        .await;
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("default", range, true).await;

        let instance_id = context.create_instance("all-the-ports").await;
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                id,
                instance_id,
                pool_name,
                true,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address")
            .0;
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);

        context.success().await;
    }

    #[tokio::test]
    async fn test_external_ip_allocate_omicron_zone_is_idempotent() {
        let context = TestContext::new(
            "test_external_ip_allocate_omicron_zone_is_idempotent",
        )
        .await;

        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 4),
        ))
        .unwrap();
        context.initialize_ip_pool(SERVICE_IPV4_POOL_NAME, ip_range).await;

        let ip_10_0_0_2 =
            OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                id: ExternalIpUuid::new_v4(),
                ip: "10.0.0.2".parse().unwrap(),
            });
        let ip_10_0_0_3 =
            OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                id: ExternalIpUuid::new_v4(),
                ip: "10.0.0.3".parse().unwrap(),
            });

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let service_id = OmicronZoneUuid::new_v4();
        let ip = context
            .db
            .datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                service_id,
                ZoneKind::Nexus,
                ip_10_0_0_3,
            )
            .await
            .expect("Failed to allocate service IP address");
        assert!(ip.is_service);
        assert_eq!(ip.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)));
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.parent_id, Some(service_id.into_untyped_uuid()));

        // Try allocating the same service IP again.
        let ip_again = context
            .db
            .datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                service_id,
                ZoneKind::Nexus,
                ip_10_0_0_3,
            )
            .await
            .expect("Failed to allocate service IP address");
        assert!(ip_again.is_service);
        assert_eq!(ip.id, ip_again.id);
        assert_eq!(ip.ip.ip(), ip_again.ip.ip());

        // Try allocating the same service IP once more, but do it with a
        // different UUID.
        let err = context
            .db.datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                service_id,
                ZoneKind::Nexus,
                OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                    id: ExternalIpUuid::new_v4(),
                    ip: ip_10_0_0_3.ip(),
                }),
            )
            .await
            .expect_err("Should have failed to re-allocate same IP address (different UUID)");
        assert_eq!(
            err.to_string(),
            "Invalid Request: Requested external IP address not available"
        );

        // Try allocating the same service IP once more, but do it with a
        // different input address.
        let err = context
            .db.datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                service_id,
                ZoneKind::Nexus,
                OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                    id: ip_10_0_0_3.id(),
                    ip: ip_10_0_0_2.ip(),
                }),
            )
            .await
            .expect_err("Should have failed to re-allocate different IP address (same UUID)");
        assert_eq!(
            err.to_string(),
            "Invalid Request: Re-allocating IP address with a different value"
        );

        // Try allocating the same service IP once more, but do it with a
        // different port range.
        let ip_10_0_0_3_snat_0 =
            OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
                id: ip_10_0_0_3.id(),
                snat_cfg: SourceNatConfig::new(ip_10_0_0_3.ip(), 0, 16383)
                    .unwrap(),
            });
        let err = context
            .db.datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                service_id,
                ZoneKind::BoundaryNtp,
                ip_10_0_0_3_snat_0,
            )
            .await
            .expect_err("Should have failed to re-allocate different IP address (different port range)");
        assert_eq!(
            err.to_string(),
            "Invalid Request: Re-allocating IP address with a different value"
        );

        // This time start with an explicit SNat
        let ip_10_0_0_1_snat_32768 =
            OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
                id: ExternalIpUuid::new_v4(),
                snat_cfg: SourceNatConfig::new(
                    "10.0.0.1".parse().unwrap(),
                    32768,
                    49151,
                )
                .unwrap(),
            });
        let snat_service_id = OmicronZoneUuid::new_v4();
        let snat_ip = context
            .db
            .datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                snat_service_id,
                ZoneKind::BoundaryNtp,
                ip_10_0_0_1_snat_32768,
            )
            .await
            .expect("Failed to allocate service IP address");
        assert!(snat_ip.is_service);
        assert_eq!(snat_ip.kind, IpKind::SNat);
        assert_eq!(snat_ip.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(snat_ip.first_port.0, 32768);
        assert_eq!(snat_ip.last_port.0, 49151);
        assert_eq!(
            snat_ip.parent_id,
            Some(snat_service_id.into_untyped_uuid())
        );

        // Try allocating the same service IP again.
        let snat_ip_again = context
            .db
            .datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                snat_service_id,
                ZoneKind::BoundaryNtp,
                ip_10_0_0_1_snat_32768,
            )
            .await
            .expect("Failed to allocate service IP address");
        assert!(snat_ip_again.is_service);
        assert_eq!(snat_ip.id, snat_ip_again.id);
        assert_eq!(snat_ip.ip.ip(), snat_ip_again.ip.ip());
        assert_eq!(snat_ip.first_port, snat_ip_again.first_port);
        assert_eq!(snat_ip.last_port, snat_ip_again.last_port);

        // Try allocating the same service IP once more, but do it with a
        // different port range.
        let ip_10_0_0_1_snat_49152 =
            OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
                id: ip_10_0_0_1_snat_32768.id(),
                snat_cfg: SourceNatConfig::new(
                    ip_10_0_0_1_snat_32768.ip(),
                    49152,
                    65535,
                )
                .unwrap(),
            });
        let err = context
            .db.datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                snat_service_id,
                ZoneKind::BoundaryNtp,
                ip_10_0_0_1_snat_49152,
            )
            .await
            .expect_err("Should have failed to re-allocate different IP address (different port range)");
        assert_eq!(
            err.to_string(),
            "Invalid Request: Re-allocating IP address with a different value"
        );

        context.success().await;
    }

    #[tokio::test]
    async fn test_external_ip_allocate_omicron_zone_out_of_range() {
        let context = TestContext::new(
            "test_external_ip_allocate_omicron_zone_out_of_range",
        )
        .await;

        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 4),
        ))
        .unwrap();
        context.initialize_ip_pool(SERVICE_IPV4_POOL_NAME, ip_range).await;

        let ip_10_0_0_5 =
            OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                id: ExternalIpUuid::new_v4(),
                ip: "10.0.0.5".parse().unwrap(),
            });

        let service_id = OmicronZoneUuid::new_v4();
        let err = context
            .db
            .datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                service_id,
                ZoneKind::Nexus,
                ip_10_0_0_5,
            )
            .await
            .expect_err("Should have failed to allocate out-of-bounds IP");
        assert_eq!(
            err.to_string(),
            "Invalid Request: Requested external IP address not available"
        );

        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_external_ip_is_idempotent() {
        let context =
            TestContext::new("test_insert_external_ip_is_idempotent").await;

        // Create an IP pool
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("default", range, true).await;

        // Create one SNAT IP address.
        let instance_id = InstanceUuid::new_v4();
        let id = Uuid::new_v4();
        let ip = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                id,
                instance_id,
                context.default_pool_id().await,
            )
            .await
            .expect("Failed to allocate instance SNAT IP address");
        assert_eq!(ip.kind, IpKind::SNat);
        assert_eq!(ip.ip.ip(), range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, NUM_SOURCE_NAT_PORTS - 1);

        // Create a new IP, with the _same_ ID, and ensure we get back the same
        // value.
        let new_ip = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                id,
                instance_id,
                context.default_pool_id().await,
            )
            .await
            .expect("Failed to allocate instance SNAT IP address");

        // Check identity, not equality. The timestamps will be updated.
        assert_eq!(ip.id, new_ip.id);
        assert_eq!(ip.name, new_ip.name);
        assert_eq!(ip.description, new_ip.description);
        assert!(ip.time_created <= new_ip.time_created);
        assert!(ip.time_modified <= new_ip.time_modified);
        assert_eq!(ip.time_deleted, new_ip.time_deleted);
        assert_eq!(ip.ip_pool_id, new_ip.ip_pool_id);
        assert_eq!(ip.ip_pool_range_id, new_ip.ip_pool_range_id);
        assert_eq!(ip.kind, new_ip.kind);
        assert_eq!(ip.ip, new_ip.ip);
        assert_eq!(ip.first_port, new_ip.first_port);
        assert_eq!(ip.last_port, new_ip.last_port);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_is_restricted_to_pools() {
        let context =
            TestContext::new("test_next_external_ip_is_restricted_to_pools")
                .await;

        // Create two pools
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("default", first_range, true).await;
        let second_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(10, 0, 0, 6),
        ))
        .unwrap();
        let p1 = context.create_ip_pool("p1", second_range, false).await;

        // Allocating an address on an instance in the second pool should be
        // respected, even though there are IPs available in the first.
        let instance_id = context.create_instance("test").await;
        let id = Uuid::new_v4();

        let ip = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                id,
                instance_id,
                Some(p1),
                true,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address")
            .0;
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), second_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);

        context.success().await;
    }

    #[tokio::test]
    async fn test_ensure_pool_exhaustion_does_not_use_other_pool() {
        let context = TestContext::new(
            "test_ensure_pool_exhaustion_does_not_use_other_pool",
        )
        .await;

        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("default", first_range, true).await;
        let first_address = Ipv4Addr::new(10, 0, 0, 4);
        let last_address = Ipv4Addr::new(10, 0, 0, 6);
        let second_range =
            IpRange::try_from((first_address, last_address)).unwrap();
        let p1 = context.create_ip_pool("p1", second_range, false).await;

        // Allocate all available addresses in the second pool.
        let first_octet = first_address.octets()[3];
        let last_octet = last_address.octets()[3];
        for octet in first_octet..=last_octet {
            let instance_id =
                context.create_instance(&format!("o{octet}")).await;
            let ip = context
                .db
                .datastore()
                .allocate_instance_ephemeral_ip(
                    context.db.opctx(),
                    Uuid::new_v4(),
                    instance_id,
                    Some(p1.clone()),
                    true,
                )
                .await
                .expect("Failed to allocate instance ephemeral IP address")
                .0;
            println!("{ip:#?}");
            if let IpAddr::V4(addr) = ip.ip.ip() {
                assert_eq!(addr.octets()[3], octet);
            } else {
                panic!("Expected an IPv4 address");
            }
        }

        // Allocating another address should _fail_, and not use the first pool.
        let instance_id = context.create_instance("final").await;
        context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                Some(p1),
                true,
            )
            .await
            .expect_err("Should not use IP addresses from a different pool");

        context.success().await;
    }
}
