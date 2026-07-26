// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for operating on IP Pools.

use crate::db::model::IpPoolRange;
use chrono::DateTime;
use chrono::Utc;
use diesel::Insertable;
use diesel::QueryResult;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::ip_pool_range::dsl;
use uuid::Uuid;

/// A query for filtering out candidate IP ranges that overlap with any
/// existing ranges.
///
/// This query runs when inserting a new IP range into an existing IP Pool.
/// Ranges must be unique across all pools. This query selects the
/// candidate range only if it does not overlap any existing range. It
/// filters out the candidate if it overlaps an existing `ip_pool_range`
/// row or an existing `subnet_pool_member` row.
///
/// Two closed intervals `[a, b]` and `[c, d]` overlap exactly when
/// `a <= d AND c <= b`. Both tables have a `check_address_order` CHECK
/// constraint guaranteeing `a <= b` and `c <= d`, so this simple form
/// always works. That one inequality is checked once per table below.
///
/// See `tests/output/filter_overlapping_ip_ranges.sql` for the full generated SQL.
#[derive(Debug, Clone)]
pub struct FilterOverlappingIpRanges {
    pub range: IpPoolRange,
}

impl QueryId for FilterOverlappingIpRanges {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

// Push a single `NOT EXISTS` subquery that checks whether any row in
// `table_name` overlaps the candidate range `[first_address, last_address]`.
//
// Two closed intervals overlap when each one starts before or at the
// other one's end. See the module doc comment above for the general
// form. Here, `first_address` and `last_address` name the existing
// row's columns, and the two function parameters name the candidate's
// bounds:
//
// ```sql
// SELECT 1 FROM <table_name>
// WHERE first_address <= <last_address>   -- existing.start <= candidate.end
//   AND last_address >= <first_address>   -- existing.end >= candidate.start
//   AND time_deleted IS NULL
// LIMIT 1
// ```
fn push_overlap_subquery<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    first_address: &'a IpNetwork,
    last_address: &'a IpNetwork,
    table_name: &'static str,
) -> QueryResult<()> {
    out.push_sql("SELECT 1 FROM ");
    out.push_identifier(table_name)?;
    out.push_sql(" WHERE first_address <= ");
    out.push_bind_param::<sql_types::Inet, _>(last_address)?;
    out.push_sql(" AND last_address >= ");
    out.push_bind_param::<sql_types::Inet, _>(first_address)?;
    out.push_sql(" AND time_deleted IS NULL LIMIT 1");
    Ok(())
}

// Thin, named wrappers around `push_overlap_subquery`, one per table. These
// only exist so the call sites in `walk_ast` below read as
// "check ip_pool_range" / "check subnet_pool_member" rather than a bare
// function call with a string literal buried in the argument list.
fn push_ip_pool_range_overlap_subquery<'a>(
    out: AstPass<'_, 'a, Pg>,
    first_address: &'a IpNetwork,
    last_address: &'a IpNetwork,
) -> QueryResult<()> {
    push_overlap_subquery(out, first_address, last_address, "ip_pool_range")
}

fn push_subnet_pool_member_overlap_subquery<'a>(
    out: AstPass<'_, 'a, Pg>,
    first_address: &'a IpNetwork,
    last_address: &'a IpNetwork,
) -> QueryResult<()> {
    push_overlap_subquery(
        out,
        first_address,
        last_address,
        "subnet_pool_member",
    )
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
        out.push_bind_param::<sql_types::BigInt, i64>(&self.range.rcgen)?;

        // Filter out ranges that overlap with an existing IP Pool range, or
        // with an existing Subnet Pool member.
        out.push_sql(" WHERE NOT EXISTS(");
        push_ip_pool_range_overlap_subquery(
            out.reborrow(),
            &self.range.first_address,
            &self.range.last_address,
        )?;
        out.push_sql(") AND NOT EXISTS(");
        push_subnet_pool_member_overlap_subquery(
            out.reborrow(),
            &self.range.first_address,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use omicron_common::address::Ipv4Range;

    #[tokio::test]
    async fn expectorate_filter_overlapping_ip_ranges() {
        let range = IpPoolRange::new(
            &Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 1),
                std::net::Ipv4Addr::new(10, 0, 0, 5),
            )
            .unwrap()
            .into(),
            Uuid::nil(),
        );
        let query = FilterOverlappingIpRanges { range };
        expectorate_query_contents(
            &query,
            "tests/output/filter_overlapping_ip_ranges.sql",
        )
        .await;
    }
}
