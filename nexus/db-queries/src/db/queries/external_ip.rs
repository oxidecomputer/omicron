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

/// The maximum number of external IPs that can be attached to an instance.
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

const MAX_PORT: i32 = u16::MAX as _;

/// Select the next available IP address and port range for external
/// connectivity.
///
/// # Overview
///
/// This query is used to allocate an IP address from a given IP Pool, both for
/// guest instances and Oxide services that need external connectivity. It
/// accepts the ID of a pool, plus arguments describing the kind of address,
/// the intended parent, whether it's an explicitly-requested address or
/// automatically-allocated, and so on.
///
/// The query is designed to be idempotent. Provided the same arguments, it'll
/// return the exact same record if called more than once. That's so we can use
/// it in sagas or other similar contexts.
///
/// This query is really complicated. See the expectorate test output files, in
/// `tests/output/` to see the full, formatted query. But in general, it:
///
/// - Selects any previously-allocated record, for idempotency
/// - Selects a bunch of caller-provided constants, things like an instance ID
/// - Selects the next free external IP, or chunk of one, in the case of an SNAT
///   address.
/// - Inserts either the new address, or the original, provided the
///   caller-supplied data is the same as that record. (E.g., if the IP has
///   changed, this query will fail.)
/// - Updates the `ip_pool_range` table's `rcgen` column, to reflect the fact
///   that we've allocated a new child object.
///
/// The actual record inserted is returned to the caller.
///
/// # Query details
///
/// Note that the query looks pretty different based on the arguments. The main
/// distinction comes from the kind of address (SNAT or Ephemeral / Floating),
/// and whether we're doing an automatic address allocation or requesting a
/// specific IP address.
///
/// The general strategy for finding an IP address is:
///
/// - Use the previously-allocated address, if it exists.
/// - Use an exact, caller-provided IP address and port-range, if provided.
/// - Check if the smallest address in an IP Pool Range is available, and use it
///   if so.
/// - Search for the next IP or chunk of one, by joining the `external_ip.ip`
///   column with `external_ip.ip + 1`, i.e., the "next address". This is
///   similar to how the "next-item" queries work, though unfortunately we can't
///   use that in this context.
///
/// ## SNAT vs Ephemeral / Floating
///
/// SNAT addresses present a big challenge. They use a portion of the port
/// range, 16k as of this writing. That means multiple SNAT addresses can share
/// the same underlying IP address. An example of this kind of query can be
/// found in `nexus/db-queries/tests/output/next_instance_snat_ip.sql`. It needs
/// to join both IP addresses and also the possible port chunks. The query is
/// technically parametrized by the size of those chunks, though changing it
/// should be done with extreme care.
///
/// In contrast, Ephemeral / Floating IPs take the whole port range. This makes
/// the query quite a bit simpler. This only needs to look for "whole
/// addresses". See
/// `nexus/db-queries/tests/output/next_automatic_floating_ip.sql` for details,
/// noting that the Ephemeral IP version of the query works the same way.
///
/// ## Explicit vs automatic
///
/// The second main distinction is between explicit addresses, where we're
/// asking for an exact IP address (and port range), or searching for one to
/// allocate automatically.
///
/// In the former case, we select the requested address as constants in the SQL
/// query, subject to some validation. The address has to be within an IP Pool
/// Range in the pool itself; and, if this is a reallocation of a previous
/// address, it has to be exactly the same as that previous one. That is, you
/// can't request a new, different address the second time. An example of the
/// explicit-request SQL query can be found in
/// `nexus/db-queries/tests/output/next_explicit_floating_ip.sql`.
///
/// If the allocation is automatic, it gets...complicated. In that case, we do a
/// bunch of joins to find the smallest address that's not yet allocated. It's
/// best to look at the expectorate files with "automatic" in their name.
///
/// # Performance notes
///
/// This query has been written with performance in mind, particuarly memory
/// consumption. The previous versions of the query used `generate_series()` to
/// find the list of _all_ IP addresses in an IP Range, and then join that with
/// the allocated IPs to find the first free one. That works, but since
/// CockroachDB eagerly-evaluates all subqueries, is expensive for larger
/// subnets, and completely infeasible for realistic IPv6 ranges.
///
/// Note that the current version of the query does have some performance
/// issues. Memory consumption and time are dependent on the number of existing,
/// allocated addresses in the provided IP Pool. (Assuming we're doing an
/// automatic allocation. Explicit allocations are constant-time w.r.t. the
/// number of addresses.) This is still better than the previous version, but we
/// may need to reevaluate the runtime as we deploy it. Depending on the
/// statistics of the table, we may need to emply a completely different
/// strategy, such as attempting to pick a random address, or doing a
/// fixed-sized linear search.
#[derive(Debug, Clone)]
pub struct NextExternalIp {
    ip: IncompleteExternalIp,
    // Number of ports reserved per IP address. Only applicable if the IP kind
    // is snat.
    n_ports_per_chunk: i32,
    now: DateTime<Utc>,
}

impl NextExternalIp {
    pub fn new(ip: IncompleteExternalIp) -> Self {
        let now = Utc::now();
        let n_ports_per_chunk = i32::from(NUM_SOURCE_NAT_PORTS);
        Self { ip, n_ports_per_chunk, now }
    }

    // Push the top-level SELECT clauses used in the `next_external_ip` CTE.
    //
    // NOTE: These columns must be selected in the order in which they
    // appear in the table, to avoid needing to push the explicit column
    // names in the RETURNING clause and final SELECT from the CTE.
    fn push_next_external_ip_cte_select_clauses<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::external_ip::dsl;
        out.push_sql("SELECT ");
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

        // Pool ID.
        out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
        out.push_sql(" AS ");
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

        // Next available IP from the following subquery
        out.push_sql("candidate_ip AS ");
        out.push_identifier(dsl::ip::NAME)?;

        // Select the first / last ports.
        out.push_sql(", candidate_first_port AS ");
        out.push_identifier(dsl::first_port::NAME)?;
        out.push_sql(", candidate_last_port AS ");
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

        Ok(())
    }

    // Push the subquery that selects an explicit IP address and port.
    //
    // This selects the caller's provided address and port range, as long as it
    // is within the first / last address of some range in the provided IP Pool.
    // It returns NULL if there is no such address.
    fn push_explicit_ip_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
        addr: &'a ipnetwork::IpNetwork,
        first_port: &'a i32,
        last_port: &'a i32,
    ) -> QueryResult<()> {
        out.push_sql("SELECT r.id AS ip_pool_range_id, CASE ");
        out.push_bind_param::<sql_types::Inet, _>(addr)?;
        out.push_sql(
            " BETWEEN r.first_address AND r.last_address WHEN TRUE THEN ",
        );
        out.push_bind_param::<sql_types::Inet, _>(addr)?;
        out.push_sql(" ELSE NULL END AS candidate_ip, ");
        out.push_bind_param::<sql_types::Int4, _>(first_port)?;
        out.push_sql(" AS candidate_first_port, ");
        out.push_bind_param::<sql_types::Int4, _>(last_port)?;
        out.push_sql(
            " AS candidate_last_port \
            FROM ip_pool_range AS r \
            WHERE ip_pool_id = ",
        );
        out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
        out.push_sql(" AND time_deleted IS NULL");
        Ok(())
    }

    // Push the subquery selecting the next SNAT IP address and port range.
    //
    // This query is the most complicated of the bunch. We need to join all the
    // available IP addresses in a pool with all the available port ranges, and
    // then find the smallest that doesn't overlap any existing entry. We also
    // fall back to picking the next fully-available IP address, i.e., the exact
    // same subquery as we use in the Floating / Ephemeral case. These are all
    // UNIONed together.
    fn push_automatic_snat_ip_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(
            "\
            SELECT \
                ip_pool_range_id, \
                candidate_ip, \
                candidate_first_port, \
                candidate_last_port \
            FROM (\
                SELECT \
                    old_ip_pool_range_id AS ip_pool_range_id, \
                    old_ip AS candidate_ip, \
                    old_first_port AS candidate_first_port, \
                    old_last_port AS candidate_last_port \
                FROM \
                    previously_allocated_ip\
            ) UNION ALL (\
                SELECT DISTINCT ON (iws.ip) \
                    iws.ip_pool_range_id, \
                    iws.ip AS candidate_ip, \
                    pb.candidate_first_port, \
                    pb.candidate_last_port \
                FROM (\
                    SELECT DISTINCT e.ip_pool_range_id, e.ip \
                    FROM external_ip AS e \
                    WHERE e.ip_pool_id = ",
        );
        out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
        out.push_sql(" AND e.kind = ");
        out.push_bind_param::<IpKindEnum, _>(self.ip.kind())?;
        out.push_sql(
            " AND e.time_deleted IS NULL \
                ) AS iws \
                CROSS JOIN (\
                    SELECT \
                        block_start AS candidate_first_port, \
                        block_start + ",
        );
        out.push_bind_param::<sql_types::Int4, _>(&self.n_ports_per_chunk)?;
        out.push_sql(" - 1 AS candidate_last_port FROM generate_series(0, ");
        out.push_bind_param::<sql_types::Int4, _>(&MAX_PORT)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Int4, _>(&self.n_ports_per_chunk)?;
        out.push_sql(
            ") AS block_start\
                ) AS pb \
                WHERE NOT EXISTS (\
                    SELECT 1 \
                    FROM external_ip AS e \
                    WHERE e.ip = iws.ip \
                    AND e.first_port <= pb.candidate_last_port \
                    AND e.last_port >= pb.candidate_first_port \
                    AND e.time_deleted IS NULL \
                ) \
                ORDER BY iws.ip, pb.candidate_first_port \
                LIMIT 1\
                ) \
                UNION ALL \
                SELECT \
                    ip_pool_range_id, \
                    candidate_ip, \
                    0 AS candidate_first_port, ",
        );
        out.push_bind_param::<sql_types::Int4, _>(&self.n_ports_per_chunk)?;
        out.push_sql(" - 1 AS candidate_last_port FROM (");
        self.push_automatic_full_ip_subquery_body(out.reborrow())?;
        out.push_sql(
            "\
                ) AS free_ips \
                WHERE candidate_ip IS NOT NULL \
                ) AS all_candidates \
                ORDER BY candidate_ip, candidate_first_port \
                LIMIT 1",
        );
        Ok(())
    }

    // Push the subquery that automatically selects the next available IP
    // address that has no other entry sharing any of its ports.
    fn push_automatic_full_ip_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(
            "(\
                SELECT \
                    old_ip_pool_range_id AS ip_pool_range_id, \
                    old_ip AS candidate_ip, \
                    old_first_port AS candidate_first_port, \
                    old_last_port AS candidate_last_port \
                FROM \
                    previously_allocated_ip\
                ) \
                UNION ALL ",
        );
        self.push_automatic_full_ip_subquery_body(out.reborrow())?;
        out.push_sql(") AS all_candidates \
            WHERE candidate_ip IS NOT NULL \
            ORDER BY candidate_ip \
            LIMIT 1 ");
        Ok(())
    }

    fn push_automatic_full_ip_subquery_body<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql("SELECT r.id AS ip_pool_range_id, ");

        // Now, if the caller wants an explicit IP, we can push the simpler,
        // direct form of the query that selects the address, if it's within the
        // range itself.
        //
        // Otherwise, push the full address-search query.
        if let Some(addr) = self.ip.explicit_ip() {
            out.push_sql("CASE ");
            out.push_bind_param::<sql_types::Inet, _>(addr)?;
            out.push_sql(
                " BETWEEN r.first_address AND r.last_address WHEN TRUE THEN ",
            );
            out.push_bind_param::<sql_types::Inet, _>(addr)?;
            out.push_sql(" ELSE NULL END ");
        } else {
            out.push_sql(
                "\
                COALESCE(\
                    CASE \
                        WHEN NOT EXISTS (\
                            SELECT 1 \
                            FROM external_ip AS e \
                            WHERE e.ip_pool_id = ",
            );
            out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
            out.push_sql(
                " \
                            AND e.ip_pool_range_id = r.id \
                            AND e.ip = r.first_address \
                            AND e.time_deleted IS NULL\
                        ) \
                        THEN r.first_address \
                    END, \
                    (\
                        SELECT e.ip + 1 \
                        FROM external_ip AS e \
                        WHERE e.ip_pool_id = ",
            );
            out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
            out.push_sql(
                " \
                        AND e.ip_pool_range_id = r.id \
                        AND e.ip < r.last_address \
                        AND e.time_deleted IS NULL \
                        AND NOT EXISTS (\
                            SELECT 1 \
                            FROM external_ip AS e2 \
                            WHERE e2.ip_pool_id = ",
            );
            out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
            out.push_sql(
                " \
                            AND e2.ip_pool_range_id = r.id \
                            AND e2.ip = e.ip + 1 \
                            AND e2.time_deleted IS NULL\
                        ) \
                        ORDER BY e.ip \
                        LIMIT 1\
                    )\
                )",
            );
        }

        // Continue by pushing the bits selecting the first / last ports.
        out.push_sql(" AS candidate_ip, 0 AS candidate_first_port, ");
        out.push_bind_param::<sql_types::Int4, _>(&MAX_PORT)?;
        out.push_sql(
            " \
                AS candidate_last_port \
            FROM \
                ip_pool_range AS r \
                WHERE r.ip_pool_id = ",
        );
        out.push_bind_param::<sql_types::Uuid, _>(self.ip.pool_id())?;
        out.push_sql(" AND r.time_deleted IS NULL ");
        Ok(())
    }

    // Push subquery that selects a previously-allocated external IP by its ID.
    fn push_prior_allocation_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::external_ip::dsl;

        out.push_sql("SELECT ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" AS old_id, ");
        out.push_identifier(dsl::ip_pool_range_id::NAME)?;
        out.push_sql(" AS old_ip_pool_range_id, ");
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

    // Push the subquery that validates that the previously-selected external IP
    // address either:
    //
    // - does not exist, i.e., this is a new entry
    // - is exactly the same as the one we're adding now, i.e., we're
    //   idempotently inserting the same record again, such as in a saga.
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
                    "NOT EXISTS(SELECT 1 FROM previously_allocated_ip) OR ",
                );
                // ... Or we are allocating the same IP address...
                out.push_sql("(SELECT ");
                out.push_identifier(dsl::ip::NAME)?;
                out.push_sql(" = old_ip AND ");
                // (as well as the same port range)
                out.push_identifier(dsl::first_port::NAME)?;
                out.push_sql(" = old_first_port AND ");
                out.push_identifier(dsl::last_port::NAME)?;
                out.push_sql(
                    " = old_last_port \
                    FROM next_external_ip \
                    INNER JOIN previously_allocated_ip \
                    ON previously_allocated_ip.old_id = next_external_ip.id)",
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
                time_created = excluded.time_created, \
                time_modified = excluded.time_modified, \
                time_deleted = excluded.time_deleted \
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

        // We're always going to select any previously-allocated record with the
        // same ID. The way in which that is used depends on the kind of
        // allocation we're doing (automatic or explicit), but we always need
        // it. Push that CTE first.
        out.push_sql("WITH previously_allocated_ip AS (");
        self.push_prior_allocation_subquery(out.reborrow())?;
        out.push_sql("), ");

        // The main difference between the various queries is whether we're
        // trying to allocate an explicit IP, or find the next one
        // automatically.
        //
        // For the explicit IP case, we _also_ know the explicit port range in
        // all cases. For Floating or Ephemeral IPs, that's (0, 65535). The only
        // time we explicitly request an SNAT address is when we're constructing
        // an Omicron zone, in which case we _also_ know the explicit port
        // range.
        //
        // Note that we could enforce these invariants in the type system, but
        // the QueryFragment trait requires borrowed values, which makes that a
        // bit awkward.
        out.push_sql("next_external_ip AS (");
        self.push_next_external_ip_cte_select_clauses(out.reborrow())?;
        out.push_sql(" FROM (");
        match (
            self.ip.kind(),
            self.ip.explicit_ip(),
            self.ip.explicit_port_range(),
        ) {
            (IpKind::SNat, None, None) => {
                self.push_automatic_snat_ip_subquery(out.reborrow())?;
                out.push_sql("), ");
            }

            (IpKind::Ephemeral, None, None)
            | (IpKind::Floating, None, None) => {
                self.push_automatic_full_ip_subquery(out.reborrow())?;
                out.push_sql("), ");
            }

            (IpKind::SNat, Some(ip), Some((first_port, last_port)))
            | (IpKind::Ephemeral, Some(ip), Some((first_port, last_port)))
            | (IpKind::Floating, Some(ip), Some((first_port, last_port))) => {
                self.push_explicit_ip_subquery(
                    out.reborrow(),
                    ip,
                    first_port,
                    last_port,
                )?;
                out.push_sql(
                    ") WHERE candidate_ip IS NOT NULL), \
                    validate_previously_allocated_ip AS MATERIALIZED(",
                );
                self.push_validate_prior_allocation_subquery(out.reborrow())?;
                out.push_sql("), ");
            }

            (IpKind::SNat, None, Some(_))
            | (IpKind::SNat, Some(_), None)
            | (IpKind::Ephemeral, None, Some(_))
            | (IpKind::Ephemeral, Some(_), None)
            | (IpKind::Floating, None, Some(_))
            | (IpKind::Floating, Some(_), None) => unreachable!(
                "IP and port ranges should always be either both \
                Some(_) or both None"
            ),
        }

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
    use crate::db::explain::ExplainableAsync as _;
    use crate::db::identity::Resource;
    use crate::db::model::IpKind;
    use crate::db::model::IpPool;
    use crate::db::model::IpPoolRange;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::queries::external_ip::NextExternalIp;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use dropshot::test_util::LogContext;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::ByteCount;
    use nexus_db_model::ExternalIp;
    use nexus_db_model::IncompleteExternalIp;
    use nexus_db_model::Instance;
    use nexus_db_model::InstanceCpuCount;
    use nexus_db_model::IpPoolReservationType;
    use nexus_db_model::IpPoolResource;
    use nexus_db_model::IpPoolResourceType;
    use nexus_db_model::IpVersion;
    use nexus_db_model::Name;
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
    use std::net::Ipv6Addr;
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
        ) -> (authz::IpPool, IpPool) {
            let pool = IpPool::new(
                &IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: format!("ip pool {}", name),
                },
                IpVersion::V4,
                IpPoolReservationType::ExternalSilos,
            );

            let db_pool = self.db
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

            let authz_pool = LookupPath::new(self.db.opctx(), self.db.datastore())
                .ip_pool_id(pool.id())
                .lookup_for(authz::Action::Read)
                .await
                .unwrap()
                .0;
            (authz_pool, db_pool)
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
                identity: IdentityMetadataCreateParams {
                    name: String::from(name).parse().unwrap(),
                    description: format!("instance {}", name)
                },
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
        for first_port in (0..u16::MAX).step_by(NUM_SOURCE_NAT_PORTS.into()) {
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
        assert_eq!(ephemeral_ip.last_port.0, u16::MAX);

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
                "NextExternalIp::new tried to insert NULL ip",
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
        let ports = (0..u16::MAX).step_by(NUM_SOURCE_NAT_PORTS.into());
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
        assert_external_ips_equivalent(&ip, &new_ip);

        context.success().await;
    }

    fn assert_external_ips_equivalent(ip: &ExternalIp, new_ip: &ExternalIp) {
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
        let (p1, ..) = context.create_ip_pool("p1", second_range, false).await;

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
        let (p1, ..) = context.create_ip_pool("p1", second_range, false).await;

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

    #[tokio::test]
    async fn can_explain_next_external_ip_query() {
        let logctx = dev::test_setup_log("can_explain_next_external_ip_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let conn = db.pool().claim().await.unwrap();
        let params = [
            IncompleteExternalIp::for_ephemeral(Uuid::new_v4(), Uuid::new_v4()),
            IncompleteExternalIp::for_instance_source_nat(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
            ),
        ];
        for prs in &params {
            let query = NextExternalIp::new(prs.clone());
            let _ = query.explain_async(&conn).await.unwrap_or_else(|e| {
                panic!(
                    "Failed to explain query, is it valid \
                    SQL?\nparams: {prs:#?}\nerror: {e:#?}"
                )
            });
        }
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_next_automatic_floating_ip_query() {
        let params = IncompleteExternalIp::for_floating(
            uuid::uuid!("45633e04-3087-47eb-9d1e-8436fb090108"),
            &Name("fip".parse().unwrap()),
            "",
            uuid::uuid!("789fdb15-9790-4df1-9d06-c1ecd72c94ae"),
            uuid::uuid!("37bdd6f6-0adb-46ad-8e5b-083b928ace56"),
        );
        let query = NextExternalIp::new(params);
        expectorate_query_contents(
            query,
            "tests/output/next_automatic_floating_ip.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_next_explicit_floating_ip_query() {
        let params = IncompleteExternalIp::for_floating_explicit(
            uuid::uuid!("45633e04-3087-47eb-9d1e-8436fb090108"),
            &Name("fip".parse().unwrap()),
            "",
            uuid::uuid!("789fdb15-9790-4df1-9d06-c1ecd72c94ae"),
            "10.0.0.1".parse().unwrap(),
            uuid::uuid!("37bdd6f6-0adb-46ad-8e5b-083b928ace56"),
        );
        let query = NextExternalIp::new(params);
        expectorate_query_contents(
            query,
            "tests/output/next_explicit_floating_ip.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_next_instance_snat_ip_query() {
        let params = IncompleteExternalIp::for_instance_source_nat(
            uuid::uuid!("45633e04-3087-47eb-9d1e-8436fb090108"),
            uuid::uuid!("789fdb15-9790-4df1-9d06-c1ecd72c94ae"),
            uuid::uuid!("9fcfee03-173e-4aff-92a3-2fc9da49c008"),
        );
        let query = NextExternalIp::new(params);
        expectorate_query_contents(
            query,
            "tests/output/next_instance_snat_ip.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_next_omicron_zone_snat_ip_query() {
        let ip = OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
            id: ExternalIpUuid::from_untyped_uuid(uuid::uuid!(
                "cd7bf0bc-72f6-497d-89b9-787039da448a"
            )),
            snat_cfg: SourceNatConfig::new(
                "10.0.0.1".parse().unwrap(),
                0,
                (1 << 14) - 1,
            )
            .unwrap(),
        });
        let params = IncompleteExternalIp::for_omicron_zone(
            uuid::uuid!("45633e04-3087-47eb-9d1e-8436fb090108"),
            ip,
            OmicronZoneUuid::from_untyped_uuid(uuid::uuid!(
                "9fcfee03-173e-4aff-92a3-2fc9da49c008"
            )),
            ZoneKind::BoundaryNtp,
        );
        let query = NextExternalIp::new(params);
        expectorate_query_contents(
            query,
            "tests/output/next_omicron_zone_snat_ip.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn can_reallocate_automatic_ephemeral_with_full_range() {
        let context = TestContext::new(
            "can_reallocate_automatic_ephemeral_with_full_range",
        )
        .await;

        let first_address = Ipv4Addr::new(10, 0, 0, 1);
        let last_address = Ipv4Addr::new(10, 0, 0, 3);
        let range = IpRange::try_from((first_address, last_address)).unwrap();
        let (p1, ..) = context.create_ip_pool("default", range, true).await;

        let mut ips = Vec::with_capacity(range.len() as _);
        let mut instance_id = None;

        // Allocate all available addresses in the pool.
        let first_octet = first_address.octets()[3];
        let last_octet = last_address.octets()[3];
        for octet in first_octet..=last_octet {
            let iid = context.create_instance(&format!("o{octet}")).await;
            let _ = instance_id.get_or_insert(iid);
            let ip = context
                .db
                .datastore()
                .allocate_instance_ephemeral_ip(
                    context.db.opctx(),
                    Uuid::new_v4(),
                    iid,
                    Some(p1.clone()),
                    true,
                )
                .await
                .expect("Failed to allocate instance ephemeral IP address")
                .0;
            if let IpAddr::V4(addr) = ip.ip.ip() {
                assert_eq!(addr.octets()[3], octet);
            } else {
                panic!("Expected an IPv4 address");
            }
            ips.push(ip);
        }

        // Now, if we attempt to reallocate the first address, we do not fail,
        // but instead get back the same first address.
        let (new_ip, _) = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                ips[0].id,
                instance_id.unwrap(),
                Some(p1),
                true,
            )
            .await
            .expect("able to reallocate existing IP with full IP Range");
        assert_external_ips_equivalent(&ips[0], &new_ip);

        context.success().await;
    }

    #[tokio::test]
    async fn can_reallocate_automatic_snat_with_full_range() {
        let context =
            TestContext::new("can_reallocate_automatic_snat_with_full_range")
                .await;

        let first_address = Ipv4Addr::new(10, 0, 0, 1);
        let last_address = Ipv4Addr::new(10, 0, 0, 3);
        let range = IpRange::try_from((first_address, last_address)).unwrap();
        let (p1, ..) = context.create_ip_pool("default", range, true).await;

        let mut ips = Vec::with_capacity(range.len() as usize * 4);
        let mut instance_id = None;

        // Allocate all available addresses in the pool.
        for octet in 0.. {
            let iid = context.create_instance(&format!("o{octet}")).await;
            let _ = instance_id.get_or_insert(iid);
            let res = context
                .db
                .datastore()
                .allocate_instance_snat_ip(
                    context.db.opctx(),
                    Uuid::new_v4(),
                    iid,
                    p1.id(),
                )
                .await;
            let ip = match res {
                Ok(ip) => ip,
                Err(Error::InsufficientCapacity { .. }) => break,
                e => e.expect("Failed to allocate instance SNAT IP address"),
            };
            assert!(ip.ip.ip().is_ipv4());
            ips.push(ip);
        }

        // Now, if we attempt to reallocate the first address, we do not fail,
        // but instead get back the same first address.
        let new_ip = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                ips[0].id,
                instance_id.unwrap(),
                p1.id(),
            )
            .await
            .expect("able to reallocate existing IP with full IP Range");
        assert_external_ips_equivalent(&ips[0], &new_ip);

        context.success().await;
    }

    /// Sanity check that we can actually run this query for IPv6 pools and
    /// ranges, without hitting pathological performance issues. This test is
    /// skipped by default, since it can take a while and failures don't provide
    /// much useful information.
    ///
    /// What we can use this for is looking at the asymptotic performance of the
    /// query. As expected, it slows down as more addresses are allocated, but
    /// beyond that we'll need more monitoring to know if the performance is
    /// acceptable. We may need to make the query smarter, with constant runtime
    /// and memory consumption as a function of the number of allocated
    /// addresses. We could do that with something like a randomly-selected
    /// address or searching in limited-size chunks.
    #[tokio::test]
    #[ignore]
    async fn can_allocate_large_ipv6_range() {
        let context = TestContext::new("can_allocate_large_ipv6_range").await;
        let range = IpRange::try_from((
            "fd00::1".parse::<Ipv6Addr>().unwrap(),
            "fd00::ffff".parse::<Ipv6Addr>().unwrap(),
        ))
        .unwrap();
        let (pool, ..) = context.create_ip_pool("default", range, true).await;

        let start = std::time::Instant::now();
        for (i, expected_addr) in range.iter().enumerate() {
            let id = Uuid::new_v4();
            let iid = context.create_instance(&format!("inst-{i}")).await;
            let (ip, _) = context
                .db
                .datastore()
                .allocate_instance_ephemeral_ip(
                    context.db.opctx(),
                    id,
                    iid,
                    Some(pool.clone()),
                    true,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.id, id);
            assert_eq!(ip.ip.ip(), expected_addr);
            assert_eq!(ip.first_port.0, 0u16);
            assert_eq!(ip.last_port.0, u16::MAX);
            assert_eq!(ip.kind, IpKind::Ephemeral);
            assert_eq!(ip.parent_id, Some(iid.into_untyped_uuid()));
            if i > 0 && i % 100 == 0 {
                let time = start.elapsed();
                let ps = i as f64 / time.as_secs_f64();
                println!(
                    ">> allocated {} addresses in {:.02?} ({:.02} / sec)",
                    i, time, ps,
                );
            }
        }

        // The next allocation should fail, due to IP exhaustion
        let instance_id = InstanceUuid::new_v4();
        let err = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                instance_id,
                Some(pool),
                true,
            )
            .await
            .expect_err(
                "An error should be received when an IPv6 pool is exhausted",
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
    async fn can_insert_explicit_address_with_two_ranges_in_pool() {
        let context = TestContext::new(
            "can_insert_explicit_address_with_two_ranges_in_pool",
        )
        .await;

        // Add a new range to the IPv6 service pool.
        //
        // This checks that the subquery for selecting the range this IP
        // overlaps with returns at most one row, even though there are multiple
        // ranges in the pool.
        let range1 = IpRange::try_from((
            "fd00::1".parse::<Ipv6Addr>().unwrap(),
            "fd00::ffff".parse::<Ipv6Addr>().unwrap(),
        ))
        .unwrap();
        let range2 = IpRange::try_from((
            "fd01::1".parse::<Ipv6Addr>().unwrap(),
            "fd01::ffff".parse::<Ipv6Addr>().unwrap(),
        ))
        .unwrap();
        let (authz_pool, db_pool) = context
            .db
            .datastore()
            .ip_pools_service_lookup(
                context.db.opctx(),
                nexus_db_model::IpVersion::V6,
            )
            .await
            .expect("should be able to lookup service IP Pool");
        for range in [range1, range2] {
            let _ = context
                .db
                .datastore()
                .ip_pool_add_range(
                    context.db.opctx(),
                    &authz_pool,
                    &db_pool,
                    &range,
                )
                .await
                .expect("Should be able to add range");
        }

        let expected_addr = "fd00::10".parse().unwrap();
        let first_port = 32768;
        let last_port = 49151;
        let id = ExternalIpUuid::new_v4();
        let snat = OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
            id,
            snat_cfg: SourceNatConfig::new(
                expected_addr,
                first_port,
                last_port,
            )
            .unwrap(),
        });
        let snat_service_id = OmicronZoneUuid::new_v4();
        let ip = context
            .db
            .datastore()
            .external_ip_allocate_omicron_zone(
                context.db.opctx(),
                snat_service_id,
                ZoneKind::BoundaryNtp,
                snat,
            )
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip.id, id.into_untyped_uuid());
        assert_eq!(ip.ip.ip(), expected_addr);
        assert_eq!(ip.first_port.0, first_port);
        assert_eq!(ip.last_port.0, last_port);
        assert_eq!(ip.kind, IpKind::SNat);
        assert_eq!(ip.parent_id, Some(snat_service_id.into_untyped_uuid()));

        context.success().await;
    }

    #[tokio::test]
    async fn can_allocate_ephemeral_ips_from_all_ranges_in_a_pool() {
        let context = TestContext::new(
            "can_allocate_ephemeral_ips_from_all_ranges_in_a_pool",
        )
        .await;

        // Create two ranges in the same pool. Each range will have one address
        // for simplicity.
        let addrs = [
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
        ];
        let range1 = IpRange::try_from((addrs[0], addrs[0])).unwrap();
        let range2 = IpRange::try_from((addrs[1], addrs[1])).unwrap();
        let (authz_pool, db_pool) = context.create_ip_pool("default", range1, true).await;
        let _ = context
            .db
            .datastore()
            .ip_pool_add_range(
                context.db.opctx(),
                &authz_pool,
                &db_pool,
                &range2
            )
            .await
            .expect("able to add a second range to the pool");

        // Allocate an instance and address, which should take the first address
        // (which is the whole range).
        let iid = context.create_instance("inst1").await;
        let ip = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                iid,
                Some(authz_pool.clone()),
                true,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address")
            .0;
        if let IpAddr::V4(addr) = ip.ip.ip() {
            assert_eq!(addr, addrs[0]);
        } else {
            panic!("Expected an IPv4 address");
        }

        // Allocate another one, which should take the second address, which is
        // "all" of the second range.
        let iid = context.create_instance("inst2").await;
        let ip = context
            .db
            .datastore()
            .allocate_instance_ephemeral_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                iid,
                Some(authz_pool.clone()),
                true,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address")
            .0;
        if let IpAddr::V4(addr) = ip.ip.ip() {
            assert_eq!(addr, addrs[1]);
        } else {
            panic!("Expected an IPv4 address");
        }

        context.success().await;
    }

    #[tokio::test]
    async fn can_allocate_snat_ips_from_all_ranges_in_a_pool() {
        let context = TestContext::new(
            "can_allocate_snat_ips_from_all_ranges_in_a_pool",
        )
        .await;

        // Create two ranges in the same pool. Each range will have one address
        // for simplicity.
        let addrs = [
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
        ];
        let range1 = IpRange::try_from((addrs[0], addrs[0])).unwrap();
        let range2 = IpRange::try_from((addrs[1], addrs[1])).unwrap();
        let (authz_pool, db_pool) = context.create_ip_pool("default", range1, true).await;
        let _ = context
            .db
            .datastore()
            .ip_pool_add_range(
                context.db.opctx(),
                &authz_pool,
                &db_pool,
                &range2
            )
            .await
            .expect("able to add a second range to the pool");

        // Allocate 4 instances, to take the whole address that constitutes the
        // first range.
        for i in 0..4 {
            let iid = context.create_instance(&format!("inst{i}")).await;
            let ip = context
                .db
                .datastore()
                .allocate_instance_snat_ip(
                    context.db.opctx(),
                    Uuid::new_v4(),
                    iid,
                    db_pool.id(),
                )
                .await
                .expect("Failed to allocate instance SNAT IP address");
            if let IpAddr::V4(addr) = ip.ip.ip() {
                assert_eq!(addr, addrs[0]);
            } else {
                panic!("Expected an IPv4 address");
            }
        }

        // Allocate another one, which should take the second address, the first
        // port block in the second range.
        let iid = context.create_instance("last").await;
        let ip = context
            .db
            .datastore()
            .allocate_instance_snat_ip(
                context.db.opctx(),
                Uuid::new_v4(),
                iid,
                db_pool.id(),
            )
            .await
            .expect("Failed to allocate instance SNAT IP address");
        if let IpAddr::V4(addr) = ip.ip.ip() {
            assert_eq!(addr, addrs[1]);
        } else {
            panic!("Expected an IPv4 address");
        }

        context.success().await;
    }
}
