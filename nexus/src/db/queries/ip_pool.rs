// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for operating on IP Pools.

use crate::db::model::IpPoolRange;
use crate::db::schema::ip_pool_range::dsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Column;
use diesel::Insertable;
use diesel::QueryResult;
use ipnetwork::IpNetwork;
use uuid::Uuid;

/// A query for filtering out candidate IP ranges that overlap with any
/// existing ranges.
///
/// This query is used when inserting a new IP range into an existing IP Pool.
/// Those ranges must currently be unique globally, across all pools. This query
/// selects the candidate range, _if_ it does not overlap with any existing
/// range. I.e., it filters out the candidate if it overlaps. The query looks
/// like
///
/// ```sql
/// SELECT
///     <candidate_range>
/// WHERE
///     -- Check for ranges that contain the candidate first address
///     NOT EXISTS(
///         SELECT
///             id
///         FROM
///             ip_pool_range
///         WHERE
///             <candidate.first_address> >= first_address AND
///             <candidate.first_address> <= last_address AND
///             time_deleted IS NULL
///         LIMIT 1
///     )
///     AND
///     -- Check for ranges that contain the candidate last address
///     NOT EXISTS(
///         SELECT
///             id
///         FROM
///             ip_pool_range
///         WHERE
///             <candidate.last_address> >= first_address AND
///             <candidate.last_address> <= last_address AND
///             time_deleted IS NULL
///         LIMIT 1
///     )
///     AND
///     -- Check for ranges whose first address is contained by the candidate
///     -- range
///     NOT EXISTS(
///         SELECT
///             id
///         FROM
///             ip_pool_range
///         WHERE
///             first_address >= <candidate.first_address> AND
///             first_address <= <candidate.last_address> AND
///             time_deleted IS NULL
///         LIMIT 1
///     )
///     AND
///     -- Check for ranges whose last address is contained by the candidate
///     -- range
///     NOT EXISTS(
///         SELECT
///             id
///         FROM
///             ip_pool_range
///         WHERE
///             last_address >= <candidate.first_address> AND
///             last_address <= <candidate.last_address> AND
///             time_deleted IS NULL
///         LIMIT 1
///     )
/// ```
///
/// That's a lot of duplication, but it's to help with the scalability of the
/// query. Collapsing those different `EXISTS()` subqueries into one set of
/// `WHERE` clauses would require an `OR`. For example:
///
/// ```sql
/// WHERE
///     (
///         <candidate.first_address> >= first_address AND
///         <candidate.first_address> <= last_address
///     )
///     OR
///     (
///         <candidate.last_address> >= first_address AND
///         <candidate.last_address> <= last_address
///     )
///     AND
///     time_deleted IS NULL
/// ```
///
/// That `OR` means the database cannot use the indexes we've supplied on the
/// `first_address` and `last_address` columns, and must resort to a full table
/// scan.
#[derive(Debug, Clone)]
pub struct FilterOverlappingIpRanges {
    pub range: IpPoolRange,
}

impl QueryId for FilterOverlappingIpRanges {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

// Push the subquery finding any existing record that contains a candidate's
// address (first or last):
//
// ```sql
// SELECT
//      id
// FROM
//      ip_pool_range
// WHERE
//      <address> >= first_address AND
//      <address> <= last_address AND
//      time_deleted IS NULL
//  LIMIT 1
// ```
fn push_record_contains_candidate_subquery<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    address: &'a IpNetwork,
) -> QueryResult<()> {
    out.push_sql("SELECT ");
    out.push_identifier(dsl::id::NAME)?;
    out.push_sql(" FROM ");
    IP_POOL_RANGE_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_bind_param::<sql_types::Inet, IpNetwork>(address)?;
    out.push_sql(" >= ");
    out.push_identifier(dsl::first_address::NAME)?;
    out.push_sql(" AND ");
    out.push_bind_param::<sql_types::Inet, IpNetwork>(address)?;
    out.push_sql(" <= ");
    out.push_identifier(dsl::last_address::NAME)?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(" IS NULL LIMIT 1");
    Ok(())
}

// Push the subquery that finds any records with an address contained within the
// provided candidate range.
//
// ```sql
// SELECT
//      id
// FROM
//      ip_pool_range
// WHERE
//      <column> >= <first_address> AND
//      <column> <= <last_address> AND
//      time_deleted IS NULL
// LIMIT 1
// ```
fn push_candidate_contains_record_subquery<'a, C>(
    mut out: AstPass<'_, 'a, Pg>,
    first_address: &'a IpNetwork,
    last_address: &'a IpNetwork,
) -> QueryResult<()>
where
    C: Column<Table = dsl::ip_pool_range>,
{
    out.push_sql("SELECT ");
    out.push_identifier(dsl::id::NAME)?;
    out.push_sql(" FROM ");
    IP_POOL_RANGE_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_identifier(C::NAME)?;
    out.push_sql(" >= ");
    out.push_bind_param::<sql_types::Inet, IpNetwork>(first_address)?;
    out.push_sql(" AND ");
    out.push_identifier(C::NAME)?;
    out.push_sql(" <= ");
    out.push_bind_param::<sql_types::Inet, IpNetwork>(last_address)?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(" IS NULL LIMIT 1");
    Ok(())
}

impl QueryFragment<Pg> for FilterOverlappingIpRanges {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql("SELECT ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.range.id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.range.time_created,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.range.time_modified,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Nullable<sql_types::Timestamptz>, Option<DateTime<Utc>>>(&self.range.time_deleted)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(
            &self.range.first_address,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(
            &self.range.last_address,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.range.ip_pool_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, Option<Uuid>>(&self.range.project_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::BigInt, i64>(&self.range.rcgen)?;

        out.push_sql(" WHERE NOT EXISTS(");
        push_candidate_contains_record_subquery::<dsl::first_address>(
            out.reborrow(),
            &self.range.first_address,
            &self.range.last_address,
        )?;
        out.push_sql(") AND NOT EXISTS(");
        push_candidate_contains_record_subquery::<dsl::last_address>(
            out.reborrow(),
            &self.range.first_address,
            &self.range.last_address,
        )?;
        out.push_sql(") AND NOT EXISTS(");
        push_record_contains_candidate_subquery(
            out.reborrow(),
            &self.range.first_address,
        )?;
        out.push_sql(") AND NOT EXISTS(");
        push_record_contains_candidate_subquery(
            out.reborrow(),
            &self.range.last_address,
        )?;
        out.push_sql(")");
        Ok(())
    }
}

impl Insertable<dsl::ip_pool_range> for FilterOverlappingIpRanges {
    type Values = FilterOverlappingIpRangesValues;

    fn values(self) -> Self::Values {
        FilterOverlappingIpRangesValues(self)
    }
}

#[derive(Debug, Clone)]
pub struct FilterOverlappingIpRangesValues(pub FilterOverlappingIpRanges);

impl QueryId for FilterOverlappingIpRangesValues {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::insertable::CanInsertInSingleQuery<Pg>
    for FilterOverlappingIpRangesValues
{
    fn rows_to_insert(&self) -> Option<usize> {
        Some(1)
    }
}

impl QueryFragment<Pg> for FilterOverlappingIpRangesValues {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        self.0.walk_ast(out.reborrow())
    }
}

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type IpPoolRangeFromClause = FromClause<dsl::ip_pool_range>;
const IP_POOL_RANGE_FROM_CLAUSE: IpPoolRangeFromClause =
    IpPoolRangeFromClause::new();
