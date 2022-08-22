// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for operating on external IP addresses from IP
//! Pools.

use crate::db::model::ExternalIp;
use crate::db::model::IncompleteExternalIp;
use crate::db::model::IpKind;
use crate::db::model::IpKindEnum;
use crate::db::model::IpSource;
use crate::db::model::Name;
use crate::db::pool::DbConnection;
use crate::db::schema;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Column;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use uuid::Uuid;

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type IpPoolRangeFromClause = FromClause<schema::ip_pool_range::table>;
const IP_POOL_RANGE_FROM_CLAUSE: IpPoolRangeFromClause =
    IpPoolRangeFromClause::new();
type ExternalIpFromClause = FromClause<schema::external_ip::table>;
const INSTANCE_EXTERNAL_IP_FROM_CLAUSE: ExternalIpFromClause =
    ExternalIpFromClause::new();

// The number of ports available to an instance when doing source NAT. Note
// that for static NAT, this value isn't used, and all ports are available.
//
// NOTE: This must be a power of 2. We're expecting to provide the Tofino with a
// port mask, e.g., a 16-bit mask such as `0b01...`, where those dots are any 14
// bits. This signifies the port range `[16384, 32768)`. Such a port mask only
// works when the port-ranges are limited to powers of 2, not arbitrary ranges.
//
// Also NOTE: This is not going to work if we modify this value across different
// versions of Nexus. Currently, we're considering a port range free simply by
// checking if the _first_ address in a range is free. However, we'll need to
// instead to check if a candidate port range has any overlap with an existing
// port range, which is more complicated. That's deferred until we actually have
// that situation (which may be as soon as allocating ephemeral IPs).
const NUM_SOURCE_NAT_PORTS: usize = 1 << 14;
const MAX_PORT: i32 = u16::MAX as _;

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
/// - Selects the next available IP address and port range from _any_ IP Pool
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
///         <project_id> AS project_id,
///         <instance_id> AS instance_id,
///         ip_pool_id,
///         ip_pool_range_id,
///         candidate_ip AS ip,
///         CAST(candidate_first_port AS INT4) AS first_port,
///         CAST(candidate_last_port AS INT4) AS last_port
///     FROM
///         (
///             -- Select all IP addresses by pool and range.
///             SELECT
///                 ip_pool_id,
///                 id AS ip_pool_range_id,
///                 first_address +
///                     generate_series(0, last_address - first_address)
///                     AS candidate_ip
///             FROM
///                 ip_pool_range
///             WHERE
///                 <pool restriction clause> AND
///                 time_deleted IS NULL
///         )
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
///         -- address and port sequence subqueryes that has no match. I.e.,
///         -- is not yet reserved.
///         external_ip
///     ON
///         (ip, first_port, time_deleted IS NULL) =
///         (candidate_ip, candidate_first_port, TRUE)
///     WHERE
///         (ip IS NULL) OR (id = <ip_id>)
///     ORDER BY
///         candidate_ip, candidate_first_port
///     LIMIT 1
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
/// Clients may optionally request an external address from a specific IP Pool.
/// If they don't provide a pool, the query is restricted to all IP Pools
/// available to their project (not reserved for a different project). In that
/// case, the pool restriction looks like:
///
/// ```sql
/// (project_id = <project_id> OR project_id IS NULL)
/// ```
///
/// That is, we search for all pools that are unreserved for reserved for _this
/// instance's_ project.
///
/// If the client does provide a pool, an additional filtering clause is added,
/// so that the whole filter looks like:
///
/// ```sql
/// pool_id = <pool_id> AND
/// (project_id = <project_id> OR project_id IS NULL)
/// ```
///
/// That filter on `project_id` is a bit redundant, as the caller should be
/// looking up the pool's ID by name, among those available to the instance's
/// project. However, it helps provides safety in the case that the caller
/// violates that expectation.
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
        let n_ports_per_chunk = i32::try_from(NUM_SOURCE_NAT_PORTS).unwrap();
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

        // Project ID
        match self.ip.source() {
            IpSource::Instance { project_id, .. } => {
                out.push_bind_param::<sql_types::Uuid, Uuid>(project_id)?;
            }
            IpSource::Service { .. } => {
                out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, Option<Uuid>>(&None)?;
            }
        };

        out.push_sql(" AS ");
        out.push_identifier(dsl::project_id::NAME)?;
        out.push_sql(", ");

        // Instance ID
        out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, Option<Uuid>>(self.ip.instance_id())?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::instance_id::NAME)?;
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
        out.push_sql(" FROM (");
        self.push_address_sequence_subquery(out.reborrow())?;
        out.push_sql(") CROSS JOIN (");
        self.push_port_sequence_subquery(out.reborrow())?;
        out.push_sql(") LEFT OUTER JOIN ");
        INSTANCE_EXTERNAL_IP_FROM_CLAUSE.walk_ast(out.reborrow())?;

        // The JOIN conditions depend on the IP type. For automatic SNAT IP
        // addresses, we need to consider existing records with their port
        // ranges. That's because we want to allow providing two different
        // chunks of ports from the same IP to two different guests.
        //
        // However, for Floating and Ephemeral IPs, we need to reserve the
        // entire port range. Guests may start listening on any port, and we
        // need to allow inbound connections to that port. (It can't be
        // rewritten on the way in.)
        //
        // In the first case the JOIN conditions look like:
        //
        // ```sql
        // ON
        //     (ip, first_port, time_deleted IS NULL) =
        //     (candidate_ip, candidate_first_port, TRUE)
        // ```
        //
        // I.e., we're considering records in the list of possibles to match
        // when the IP and the first port both match an existing record.
        //
        // In the second case, the conditions are:
        //
        // ```sql
        // ON (ip, time_deleted IS NULL) = (candidate_ip, TRUE)
        // ```
        //
        // Here, we don't care what the port is. Any record in the table that
        // has that IP should be considered a match.
        //
        // In either case, we follow this with a filter `WHERE ip IS NULL`,
        // meaning we select the candidate address and first port that does not
        // have a matching record in the table already.
        if matches!(self.ip.kind(), &IpKind::SNat) {
            out.push_sql(" ON (");
            out.push_identifier(dsl::ip::NAME)?;
            out.push_sql(", ");
            out.push_identifier(dsl::first_port::NAME)?;
            out.push_sql(", ");
            out.push_identifier(dsl::time_deleted::NAME)?;
            out.push_sql(
                " IS NULL) = (candidate_ip, candidate_first_port, TRUE) ",
            );
        } else {
            out.push_sql(" ON (");
            out.push_identifier(dsl::ip::NAME)?;
            out.push_sql(", ");
            out.push_identifier(dsl::time_deleted::NAME)?;
            out.push_sql(" IS NULL) = (candidate_ip, TRUE) ");
        }

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

    // Push a subquery that selects the sequence of IP addresses, from each range in
    // each IP Pool, along with the pool/range IDs. Note that we only allow
    // allocation out of IP Pools that match the project_id of the instance, or
    // which have no project ID (the pool is not restricted to a project)
    //
    // ```sql
    // SELECT
    //     ip_pool_id,
    //     id AS ip_pool_range_id,
    //     first_address +
    //         generate_series(0, last_address - first_address)
    //         AS candidate_ip
    // FROM
    //     ip_pool_range
    // WHERE
    //     <pool_restriction> AND
    //     time_deleted IS NULL
    // ```
    fn push_address_sequence_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::ip_pool_range::dsl;
        out.push_sql("SELECT ");
        out.push_identifier(dsl::ip_pool_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" AS ip_pool_range_id, ");
        out.push_identifier(dsl::first_address::NAME)?;
        out.push_sql(" + generate_series(0, ");
        out.push_identifier(dsl::last_address::NAME)?;
        out.push_sql(" - ");
        out.push_identifier(dsl::first_address::NAME)?;
        out.push_sql(") AS candidate_ip FROM ");
        IP_POOL_RANGE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        match self.ip.source() {
            IpSource::Instance { project_id, pool_id } => {
                if let Some(pool_id) = pool_id {
                    out.push_identifier(dsl::ip_pool_id::NAME)?;
                    out.push_sql(" = ");
                    out.push_bind_param::<sql_types::Uuid, Uuid>(pool_id)?;
                } else {
                    out.push_sql("(");
                    out.push_identifier(dsl::project_id::NAME)?;
                    out.push_sql(" = ");
                    out.push_bind_param::<sql_types::Uuid, Uuid>(project_id)?;
                    out.push_sql(" OR ");
                    out.push_identifier(dsl::project_id::NAME)?;
                    out.push_sql(" IS NULL)");
                }
            }
            IpSource::Service { pool_id } => {
                out.push_identifier(dsl::ip_pool_id::NAME)?;
                out.push_sql(" = ");
                out.push_bind_param::<sql_types::Uuid, Uuid>(pool_id)?;
            }
        }
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL");
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
    // For Floating or Ephemeral IP addresses, we reserve the entire port range
    // for the guest. In this case, we generate the static values 0 and 65535:
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
        if matches!(self.ip.kind(), &IpKind::SNat) {
            out.push_sql(
                "SELECT candidate_first_port, candidate_first_port + ",
            );
            out.push_bind_param::<sql_types::Int4, i32>(
                &self.last_port_offset,
            )?;
            out.push_sql(" AS candidate_last_port FROM generate_series(0, ");
            out.push_bind_param::<sql_types::Int4, i32>(&MAX_PORT)?;
            out.push_sql(", ");
            out.push_bind_param::<sql_types::Int4, i32>(
                &self.n_ports_per_chunk,
            )?;
            out.push_sql(") AS candidate_first_port");
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
        INSTANCE_EXTERNAL_IP_FROM_CLAUSE.walk_ast(out.reborrow())?;
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

        // Push the subquery that potentially inserts this record, or ignores
        // primary key conflicts (for idempotency).
        out.push_sql("), external_ip AS (");
        self.push_update_external_ip_subquery(out.reborrow())?;

        // Push the subquery that bumps the `rcgen` of the IP Pool range table
        out.push_sql("), updated_pool_range AS (");
        self.push_update_ip_pool_range_subquery(out.reborrow())?;

        // Select the contents of the actual record that was created or updated.
        out.push_sql(") SELECT * FROM external_ip");

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
    use crate::context::OpContext;
    use crate::db::datastore::DataStore;
    use crate::db::identity::Resource;
    use crate::db::model::IpKind;
    use crate::db::model::IpPool;
    use crate::db::model::IpPoolRange;
    use crate::db::model::Name;
    use crate::external_api::shared::IpRange;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use dropshot::test_util::LogContext;
    use nexus_test_utils::db::test_setup_database;
    use nexus_test_utils::RACK_UUID;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::db::CockroachInstance;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use uuid::Uuid;

    struct TestContext {
        logctx: LogContext,
        opctx: OpContext,
        db: CockroachInstance,
        db_datastore: Arc<DataStore>,
    }

    impl TestContext {
        async fn new(test_name: &str) -> Self {
            let logctx = dev::test_setup_log(test_name);
            let log = logctx.log.new(o!());
            let db = test_setup_database(&log).await;
            crate::db::datastore::datastore_test(&logctx, &db).await;
            let cfg = crate::db::Config { url: db.pg_config().clone() };
            let pool = Arc::new(crate::db::Pool::new(&cfg));
            let db_datastore =
                Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));
            let opctx =
                OpContext::for_tests(log.new(o!()), db_datastore.clone());
            Self { logctx, opctx, db, db_datastore }
        }

        async fn create_ip_pool_internal(
            &self,
            name: &str,
            range: IpRange,
            project_id: Option<Uuid>,
            rack_id: Option<Uuid>,
        ) {
            let pool = IpPool::new(
                &IdentityMetadataCreateParams {
                    name: String::from(name).parse().unwrap(),
                    description: format!("ip pool {}", name),
                },
                project_id,
                rack_id,
            );

            diesel::insert_into(crate::db::schema::ip_pool::dsl::ip_pool)
                .values(pool.clone())
                .execute_async(
                    self.db_datastore
                        .pool_authorized(&self.opctx)
                        .await
                        .unwrap(),
                )
                .await
                .expect("Failed to create IP Pool");

            let pool_range = IpPoolRange::new(&range, pool.id(), project_id);
            diesel::insert_into(
                crate::db::schema::ip_pool_range::dsl::ip_pool_range,
            )
            .values(pool_range)
            .execute_async(
                self.db_datastore.pool_authorized(&self.opctx).await.unwrap(),
            )
            .await
            .expect("Failed to create IP Pool range");
        }

        async fn create_rack_ip_pool(
            &self,
            name: &str,
            range: IpRange,
            rack_id: Uuid,
        ) {
            self.create_ip_pool_internal(
                name,
                range,
                /* project_id= */ None,
                Some(rack_id),
            )
            .await;
        }

        async fn create_ip_pool(
            &self,
            name: &str,
            range: IpRange,
            project_id: Option<Uuid>,
        ) {
            self.create_ip_pool_internal(
                name, range, project_id, /* rack_id= */ None,
            )
            .await;
        }

        async fn success(mut self) {
            self.db.cleanup().await.unwrap();
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
        context.create_ip_pool("p0", range, None).await;
        let project_id = Uuid::new_v4();
        for first_port in
            (0..super::MAX_PORT).step_by(super::NUM_SOURCE_NAT_PORTS)
        {
            let id = Uuid::new_v4();
            let instance_id = Uuid::new_v4();
            let ip = context
                .db_datastore
                .allocate_instance_snat_ip(
                    &context.opctx,
                    id,
                    project_id,
                    instance_id,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.ip.ip(), range.first_address());
            assert_eq!(ip.first_port.0, first_port as u16);
            assert_eq!(
                ip.last_port.0,
                (first_port + (super::NUM_SOURCE_NAT_PORTS - 1) as i32) as u16
            );
        }

        // The next allocation should fail, due to IP exhaustion
        let instance_id = Uuid::new_v4();
        let err = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
            )
            .await
            .expect_err(
                "An error should be received when the IP pools are exhausted",
            );
        assert_eq!(
            err,
            Error::InvalidRequest {
                message: String::from(
                    "No external IP addresses available for new instance"
                ),
            }
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
        context.create_ip_pool("p0", range, None).await;

        // TODO-completess: Implementing Iterator for IpRange would be nice.
        let addresses = [
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(10, 0, 0, 3),
        ];
        let ports = (0..super::MAX_PORT).step_by(super::NUM_SOURCE_NAT_PORTS);
        let mut external_ips = itertools::iproduct!(addresses, ports);

        // Allocate two addresses
        let mut ips = Vec::with_capacity(2);
        let project_id = Uuid::new_v4();
        for (expected_ip, expected_first_port) in external_ips.clone().take(2) {
            let instance_id = Uuid::new_v4();
            let ip = context
                .db_datastore
                .allocate_instance_snat_ip(
                    &context.opctx,
                    Uuid::new_v4(),
                    project_id,
                    instance_id,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.ip.ip(), expected_ip);
            assert_eq!(ip.first_port.0, expected_first_port as u16);
            let expected_last_port = (expected_first_port
                + (super::NUM_SOURCE_NAT_PORTS - 1) as i32)
                as u16;
            assert_eq!(ip.last_port.0, expected_last_port);
            ips.push(ip);
        }

        // Release the first
        context
            .db_datastore
            .deallocate_external_ip(&context.opctx, ips[0].id)
            .await
            .expect("Failed to release the first external IP address");

        // Allocate a new one, ensure it's the same as the first one we
        // released.
        let instance_id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
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
        let instance_id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
            )
            .await
            .expect("Failed to allocate instance external IP address");
        let (expected_ip, expected_first_port) = external_ips.nth(2).unwrap();
        assert_eq!(ip.ip.ip(), std::net::IpAddr::from(expected_ip));
        assert_eq!(ip.first_port.0, expected_first_port as u16);
        let expected_last_port = (expected_first_port
            + (super::NUM_SOURCE_NAT_PORTS - 1) as i32)
            as u16;
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
        context.create_ip_pool("p0", range, None).await;

        let instance_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_is_restricted_to_projects() {
        let context =
            TestContext::new("test_next_external_ip_is_restricted_to_projects")
                .await;

        // Create one pool restricted to a project, and one not.
        let project_id = Uuid::new_v4();
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", first_range, Some(project_id)).await;

        let second_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(10, 0, 0, 6),
        ))
        .unwrap();
        context.create_ip_pool("p1", second_range, None).await;

        // Allocating an address on an instance in a _different_ project should
        // get an address from the second pool.
        let instance_id = Uuid::new_v4();
        let instance_project_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                instance_project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), second_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.project_id.unwrap(), instance_project_id);

        // Allocating an address on an instance in the same project should get
        // an address from the first pool.
        let instance_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), first_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.project_id.unwrap(), project_id);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_for_service() {
        let context =
            TestContext::new("test_next_external_ip_for_service").await;

        // Create an IP pool without an associated project.
        let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
        ))
        .unwrap();
        context.create_rack_ip_pool("p0", ip_range, rack_id).await;

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let id1 = Uuid::new_v4();
        let ip1 = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id1, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip1.kind, IpKind::Service);
        assert_eq!(ip1.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip1.first_port.0, 0);
        assert_eq!(ip1.last_port.0, u16::MAX);
        assert!(ip1.instance_id.is_none());
        assert!(ip1.project_id.is_none());

        // Allocate the next (last) IP address
        let id2 = Uuid::new_v4();
        let ip2 = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id2, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip2.kind, IpKind::Service);
        assert_eq!(ip2.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        assert_eq!(ip2.first_port.0, 0);
        assert_eq!(ip2.last_port.0, u16::MAX);
        assert!(ip2.instance_id.is_none());
        assert!(ip2.project_id.is_none());

        // Once we're out of IP addresses, test that we see the right error.
        let id3 = Uuid::new_v4();
        let err = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id3, rack_id)
            .await
            .expect_err("Should have failed to allocate after pool exhausted");
        assert_eq!(
            err,
            Error::InvalidRequest {
                message: String::from(
                    // TODO: The error is a bit misleading; this isn't an IP
                    // intended for an instance necessarily.
                    "No external IP addresses available for new instance"
                ),
            }
        );

        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_external_ip_for_service_is_idempoent() {
        let context = TestContext::new(
            "test_insert_external_ip_for_service_is_idempotent",
        )
        .await;

        // Create an IP pool without an associated project.
        let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
        ))
        .unwrap();
        context.create_rack_ip_pool("p0", ip_range, rack_id).await;

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip.kind, IpKind::Service);
        assert_eq!(ip.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert!(ip.instance_id.is_none());
        assert!(ip.project_id.is_none());

        let ip_again = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");

        assert_eq!(ip.id, ip_again.id);
        assert_eq!(ip.ip.ip(), ip_again.ip.ip());

        context.success().await;
    }

    // This test is identical to "test_insert_external_ip_is_idempotent",
    // but tries to make an idempotent allocation after all addresses in the
    // pool have been allocated.
    #[tokio::test]
    async fn test_insert_external_ip_for_service_is_idempotent_even_when_full()
    {
        let context = TestContext::new(
            "test_insert_external_ip_is_idempotent_even_when_full",
        )
        .await;

        // Create an IP pool without an associated project.
        let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 1),
        ))
        .unwrap();
        context.create_rack_ip_pool("p0", ip_range, rack_id).await;

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip.kind, IpKind::Service);
        assert_eq!(ip.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert!(ip.instance_id.is_none());
        assert!(ip.project_id.is_none());

        let ip_again = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");

        assert_eq!(ip.id, ip_again.id);
        assert_eq!(ip.ip.ip(), ip_again.ip.ip());

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
        context.create_ip_pool("p0", range, None).await;

        // Create one SNAT IP address.
        let instance_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
            )
            .await
            .expect("Failed to allocate instance SNAT IP address");
        assert_eq!(ip.kind, IpKind::SNat);
        assert_eq!(ip.ip.ip(), range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(
            usize::from(ip.last_port.0),
            super::NUM_SOURCE_NAT_PORTS - 1
        );
        assert_eq!(ip.project_id.unwrap(), project_id);

        // Create a new IP, with the _same_ ID, and ensure we get back the same
        // value.
        let new_ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
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

        // Create two pools, neither project-restricted.
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", first_range, None).await;
        let second_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(10, 0, 0, 6),
        ))
        .unwrap();
        context.create_ip_pool("p1", second_range, None).await;

        // Allocating an address on an instance in the second pool should be
        // respected, even though there are IPs available in the first.
        let instance_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = Some(Name("p1".parse().unwrap()));

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), second_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.project_id.unwrap(), project_id);

        context.success().await;
    }

    #[tokio::test]
    async fn test_ensure_pool_exhaustion_does_not_use_other_pool() {
        let context = TestContext::new(
            "test_ensure_pool_exhaustion_does_not_use_other_pool",
        )
        .await;

        // Create two pools, neither project-restricted.
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", first_range, None).await;
        let first_address = Ipv4Addr::new(10, 0, 0, 4);
        let last_address = Ipv4Addr::new(10, 0, 0, 6);
        let second_range =
            IpRange::try_from((first_address, last_address)).unwrap();
        context.create_ip_pool("p1", second_range, None).await;

        // Allocate all available addresses in the second pool.
        let instance_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let pool_name = Some(Name("p1".parse().unwrap()));
        let first_octet = first_address.octets()[3];
        let last_octet = last_address.octets()[3];
        for octet in first_octet..=last_octet {
            let ip = context
                .db_datastore
                .allocate_instance_ephemeral_ip(
                    &context.opctx,
                    Uuid::new_v4(),
                    project_id,
                    instance_id,
                    pool_name.clone(),
                )
                .await
                .expect("Failed to allocate instance ephemeral IP address");
            println!("{ip:#?}");
            if let IpAddr::V4(addr) = ip.ip.ip() {
                assert_eq!(addr.octets()[3], octet);
            } else {
                panic!("Expected an IPv4 address");
            }
        }

        // Allocating another address should _fail_, and not use the first pool.
        context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect_err("Should not use IP addresses from a different pool");

        context.success().await;
    }
}
