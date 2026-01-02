// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single OxQL query.

// Copyright 2024 Oxide Computer Company

use std::collections::BTreeSet;

use super::ast::SplitQuery;
use super::ast::cmp::Comparison;
use super::ast::ident::Ident;
use super::ast::literal::Literal;
use super::ast::logical_op::LogicalOp;
use super::ast::table_ops::BasicTableOp;
use super::ast::table_ops::TableOp;
use super::ast::table_ops::filter::CompoundFilter;
use super::ast::table_ops::filter::FilterExpr;
use super::ast::table_ops::filter::SimpleFilter;
use super::ast::table_ops::group_by::GroupBy;
use super::ast::table_ops::limit::Limit;
use crate::TimeseriesName;
use crate::oxql::Error;
use crate::oxql::ast::Query as QueryNode;
use crate::oxql::ast::grammar;
use crate::oxql::ast::table_ops::filter::Filter;
use crate::oxql::fmt_parse_error;
use chrono::DateTime;
use chrono::Utc;
use uuid::Uuid;

/// A parsed OxQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub(super) parsed: QueryNode,
    pub(super) end_time: DateTime<Utc>,
}

pub enum QueryAuthzScope {
    Fleet,
    Silo { silo_id: Uuid },
    Project { silo_id: Uuid, project_id: Uuid },
}

impl Query {
    /// Construct a query written in OxQL.
    pub fn new(query: impl AsRef<str>) -> Result<Self, Error> {
        let query = query.as_ref().trim();
        const MAX_LEN: usize = 4096;
        anyhow::ensure!(
            query.len() <= MAX_LEN,
            "Queries must be <= {} characters",
            MAX_LEN,
        );
        let parsed = grammar::query_parser::query(query)
            .map_err(|e| fmt_parse_error(query, e))?;

        // Fetch the latest query end time referred to in the parsed query, or
        // use now if there isn't one.
        let query_end_time = parsed.query_end_time().unwrap_or_else(Utc::now);
        Ok(Self { parsed, end_time: query_end_time })
    }

    /// Return the end time of the query.
    pub fn end_time(&self) -> &DateTime<Utc> {
        &self.end_time
    }

    /// Return the next referenced timeseries name.
    ///
    /// Queries always start with either a single `get` operation, which refers
    /// to one timeseries; or a subquery, each component of which is a query. So
    /// it is always true that there is exactly one next timeseries name, since
    /// that comes from the current query, or the next subquery.
    pub fn timeseries_name(&self) -> &TimeseriesName {
        self.parsed.timeseries_name()
    }

    /// Return the transformation table ops, i.e., everything after the initial
    /// get operation or subquery.
    pub fn transformations(&self) -> &[TableOp] {
        self.parsed.transformations()
    }

    /// Return predicates which can be pushed down into the database, if any.
    ///
    /// Query optimization is a large topic. There are few rules, and many
    /// heuristics. However, one of those is extremely useful for our case:
    /// predicate pushdown. This is where one moves predicates as close as
    /// possible to the data, filtering out unused data as early as possible in
    /// query processing.
    ///
    /// In our case, _currently_, we can implement this pretty easily. Filtering
    /// operations can usually be coalesced into a single item. That means:
    ///
    /// - successive filtering operations are merged: `filter a | filter b ->
    /// `filter (a) && (b)`.
    /// - filtering operations are "pushed down", to just after the initial
    /// `get` operation in the query.
    ///
    /// # Group by
    ///
    /// While filters can be combined and pushed down through many operations,
    /// special care is taken for `group_by`. Specifically, the filter must only
    /// name columns explicitly named in the `group_by`. If we pushed through
    /// filters which named one of the columns _within_ the group (one not
    /// named), then that would change the set of data in a group, and thus the
    /// result.
    ///
    /// # Datum filters
    ///
    /// We currently only push down filters on the timestamps, and that is only
    /// because we do _not_ support aggregations across time, only values. If
    /// and when we do support that, then filters which reference time also
    /// cannot be pushed down.
    ///
    /// # No predicates
    ///
    /// Note that this may return `None`, in the case where there are zero
    /// predicates of any kind.
    ///
    /// # Limit operations
    ///
    /// OxQL table operations which limit data, such as `first k` or `last k`,
    /// can also be pushed down into the database in certain cases. Since they
    /// change the number of points, but not the timeseries, they cannot be
    /// pushed through an `align` operation. But they _can_ be pushed through
    /// grouping or other filters.
    //
    // Pushing filters through a group by. Consider the following data:
    //
    // a    b   timestamp   datum
    // 0    0   0           0
    // 0    0   1           1
    // 0    1   0           2
    // 0    1   1           3
    // 1    0   0           4
    // 1    0   1           5
    // 1    1   0           6
    // 1    1   1           7
    //
    // So there are two groups for a and b columns each with two samples.
    //
    // Consider `get a:b | group_by [a] | filter a == 0`.
    //
    // After the group by, the result is:
    //
    // a        timestamp   datum
    // 0        0           avg([0, 2]) -> 1
    // 0        1           avg([1, 3]) -> 2
    // 1        0           avg([4, 6]) -> 5
    // 1        1           avg([5, 7]) -> 6
    //
    // Then after the filter, it becomes:
    //
    // a        timestamp   datum
    // 0        0           avg([0, 2]) -> 1
    // 0        1           avg([1, 3]) -> 2
    //
    // Now, let's do the filter first, as if we pushed that down.
    // i.e., `get a:b | filter a == 0 | group_by [a]`. After the filter, we get:
    //
    // a    b   timestamp   datum
    // 0    0   0           0
    // 0    0   1           1
    // 0    1   0           2
    // 0    1   1           3
    //
    // Then we apply the group by:
    //
    // a        timestamp   datum
    // 0        0           avg([0, 2]) -> 1
    // 0        1           avg([1, 3]) -> 2
    //
    // So we get the same result. Let's suppose we had a filter on the column
    // `b` instead. Doing the group_by first, we get the exact same result as
    // the first one above. Or we really get an error, because the resulting
    // table does not have a `b` column.
    //
    // If instead we did the filter first, we'd get a different result. Starting
    // from:
    //
    // a    b   timestamp   datum
    // 0    0   0           0
    // 0    0   1           1
    // 0    1   0           2
    // 0    1   1           3
    // 1    0   0           4
    // 1    0   1           5
    // 1    1   0           6
    // 1    1   1           7
    //
    // Apply `filter b == 0`:
    //
    //
    // a    b   timestamp   datum
    // 0    0   0           0
    // 0    0   1           1
    // 1    0   0           4
    // 1    0   1           5
    //
    // Then apply group_by [a]
    //
    // a        timestamp   datum
    // 0        0           avg([0, 1]) -> 0.5
    // 0        1           avg([4, 5]) -> 4.5
    //
    // So we get something very different.
    //
    // What about filtering by timestamp? Starting from the raw data again:
    //
    // a    b   timestamp   datum
    // 0    0   0           0
    // 0    0   1           1
    // 0    1   0           2
    // 0    1   1           3
    // 1    0   0           4
    // 1    0   1           5
    // 1    1   0           6
    // 1    1   1           7
    //
    // Let's add a `filter timestamp >= 1`. After the `group_by [a]`, we get:
    //
    // a        timestamp   datum
    // 0        0           avg([0, 2]) -> 1
    // 0        1           avg([1, 3]) -> 2
    // 1        0           avg([4, 6]) -> 5
    // 1        1           avg([5, 7]) -> 6
    //
    // Then after `filter timestamp >= 1`:
    //
    // a        timestamp   datum
    // 0        1           avg([1, 3]) -> 2
    // 1        1           avg([5, 7]) -> 6
    //
    // Now, filtering the timestamps first, after that we get:
    //
    // a    b   timestamp   datum
    // 0    0   1           1
    // 0    1   1           3
    // 1    0   1           5
    // 1    1   1           7
    //
    // Then grouping:
    //
    // a        timestamp   datum
    // 0        1           avg([1, 3]) -> 2
    // 1        1           avg([5, 7]) -> 6
    //
    // So that also works fine.
    pub(crate) fn coalesced_predicates(
        &self,
        outer: Option<Filter>,
    ) -> Option<Filter> {
        self.transformations().iter().rev().fold(
            // We'll start from the predicates passed from the outer query.
            outer,
            |maybe_filter, next_tr| {
                // Transformations only return basic ops, since all the
                // subqueries must be at the prefix of the query.
                let TableOp::Basic(op) = next_tr else {
                    unreachable!();
                };

                match op {
                    BasicTableOp::GroupBy(GroupBy { identifiers, .. }) => {
                        // Only push through columns referred to in the group by
                        // itself, which replaces the current filter.
                        maybe_filter.as_ref().and_then(|current| {
                            restrict_filter_idents(current, identifiers)
                        })
                    }
                    BasicTableOp::Filter(filter) => {
                        // Merge with any existing filter.
                        if let Some(left) = maybe_filter {
                            Some(left.merge(&filter, LogicalOp::And))
                        } else {
                            Some(filter.clone())
                        }
                    }
                    BasicTableOp::Limit(limit) => {
                        // A filter can be pushed through a limiting table
                        // operation in a few cases, see `can_reorder_around`
                        // for details.
                        maybe_filter.and_then(|filter| {
                            if filter.can_reorder_around(limit) {
                                Some(filter)
                            } else {
                                None
                            }
                        })
                    }
                    _ => maybe_filter,
                }
            },
        )
    }

    /// Coalesce any limiting table operations, if possible.
    pub(crate) fn coalesced_limits(
        &self,
        maybe_limit: Option<Limit>,
    ) -> Option<Limit> {
        self.transformations().iter().rev().fold(
            maybe_limit,
            |maybe_limit, next_tr| {
                // Transformations only return basic ops, since all the
                // subqueries must be at the prefix of the query.
                let TableOp::Basic(op) = next_tr else {
                    unreachable!();
                };

                match op {
                    BasicTableOp::Filter(filter) => {
                        // A limit can be pushed through a filter operation, in
                        // only a few cases, see `can_reorder_around` for
                        // details.
                        maybe_limit.and_then(|limit| {
                            if filter.can_reorder_around(&limit) {
                                Some(limit)
                            } else {
                                None
                            }
                        })
                    }
                    BasicTableOp::Limit(limit) => {
                        // It is possible to "merge" limits if they're of the
                        // same kind. To do so, we simply take the one with the
                        // smaller count. For example
                        //
                        // ... | first 10 | first 5
                        //
                        // is equivalent to just
                        //
                        // ... | first 5
                        let new_limit = if let Some(current_limit) = maybe_limit
                        {
                            if limit.kind == current_limit.kind {
                                Limit {
                                    kind: limit.kind,
                                    count: limit.count.min(current_limit.count),
                                }
                            } else {
                                // If the limits are of different kinds, we replace
                                // the current one, i.e., drop it and start passing
                                // through the inner one.
                                *limit
                            }
                        } else {
                            // No outer limit at all, simply take this one.
                            *limit
                        };
                        Some(new_limit)
                    }
                    _ => maybe_limit,
                }
            },
        )
    }

    pub(crate) fn split(&self) -> SplitQuery {
        self.parsed.split(self.end_time)
    }

    /// Return the set of all timeseries names referred to by the query.
    pub(crate) fn all_timeseries_names(&self) -> BTreeSet<&TimeseriesName> {
        self.parsed.all_timeseries_names()
    }

    /// Return the parsed query AST node.
    pub(crate) fn parsed_query(&self) -> &QueryNode {
        &self.parsed
    }

    /// Insert silo and project filters after the `get`, or in the case of
    /// subqueries, recurse down the tree and insert them after each get.
    pub(crate) fn insert_authz_filters(&self, scope: QueryAuthzScope) -> Self {
        let filtered_query = match scope {
            QueryAuthzScope::Fleet => self.parsed.clone(),
            QueryAuthzScope::Silo { silo_id } => self
                .parsed
                .insert_filters(vec![uuid_eq_filter("silo_id", silo_id)]),
            QueryAuthzScope::Project { silo_id, project_id } => {
                self.parsed.insert_filters(vec![
                    uuid_eq_filter("silo_id", silo_id),
                    uuid_eq_filter("project_id", project_id),
                ])
            }
        };
        Self { parsed: filtered_query, end_time: self.end_time }
    }
}

/// Just a helper for creating a UUID filter node concisely
fn uuid_eq_filter(key: impl AsRef<str>, id: Uuid) -> Filter {
    let simple_filter = SimpleFilter {
        ident: Ident(key.as_ref().to_string()),
        cmp: Comparison::Eq,
        value: Literal::Uuid(id),
    };
    let filter_expr = FilterExpr::Simple(simple_filter);
    Filter { negated: false, expr: filter_expr }
}

// Return a new filter containing only parts that refer to either:
//
// - a `timestamp` column
// - a column listed in `identifiers`
fn restrict_filter_idents(
    current_filter: &Filter,
    identifiers: &[Ident],
) -> Option<Filter> {
    match &current_filter.expr {
        FilterExpr::Simple(inner) => {
            let ident = inner.ident.as_str();
            if ident == "timestamp"
                || identifiers.iter().map(Ident::as_str).any(|id| id == ident)
            {
                Some(current_filter.clone())
            } else {
                None
            }
        }
        FilterExpr::Compound(CompoundFilter { left, op, right }) => {
            let maybe_left = restrict_filter_idents(left, identifiers);
            let maybe_right = restrict_filter_idents(right, identifiers);
            match (maybe_left, maybe_right) {
                (Some(left), Some(right)) => Some(Filter {
                    negated: current_filter.negated,
                    expr: FilterExpr::Compound(CompoundFilter {
                        left: Box::new(left),
                        op: *op,
                        right: Box::new(right),
                    }),
                }),
                (Some(single), None) | (None, Some(single)) => Some(single),
                (None, None) => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;
    use super::Ident;
    use super::Query;
    use crate::oxql::ast::SplitQuery;
    use crate::oxql::ast::cmp::Comparison;
    use crate::oxql::ast::literal::Literal;
    use crate::oxql::ast::logical_op::LogicalOp;
    use crate::oxql::ast::table_ops::BasicTableOp;
    use crate::oxql::ast::table_ops::GroupedTableOp;
    use crate::oxql::ast::table_ops::TableOp;
    use crate::oxql::ast::table_ops::filter::CompoundFilter;
    use crate::oxql::ast::table_ops::filter::FilterExpr;
    use crate::oxql::ast::table_ops::filter::SimpleFilter;
    use crate::oxql::ast::table_ops::join::Join;
    use crate::oxql::ast::table_ops::limit::Limit;
    use crate::oxql::ast::table_ops::limit::LimitKind;
    use crate::oxql::query::QueryAuthzScope;
    use crate::oxql::query::restrict_filter_idents;
    use crate::oxql::query::uuid_eq_filter;
    use assert_matches::assert_matches;
    use chrono::NaiveDateTime;
    use chrono::Utc;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_restrict_filter_idents_single_atom() {
        let ident = Ident("foo".into());
        let filter = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: ident.clone(),
                cmp: Comparison::Eq,
                value: Literal::Boolean(false),
            }),
        };
        assert_eq!(
            restrict_filter_idents(&filter, std::slice::from_ref(&ident))
                .unwrap(),
            filter
        );
        assert_eq!(restrict_filter_idents(&filter, &[]), None);
    }

    #[test]
    fn test_restrict_filter_idents_single_atom_with_timestamp() {
        let filter = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("timestamp".into()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(false),
            }),
        };
        assert_eq!(restrict_filter_idents(&filter, &[]).unwrap(), filter);
    }

    #[test]
    fn test_restrict_filter_idents_expr() {
        let idents = [Ident("foo".into()), Ident("bar".into())];
        let left = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: idents[0].clone(),
                cmp: Comparison::Eq,
                value: Literal::Boolean(false),
            }),
        };
        let right = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: idents[1].clone(),
                cmp: Comparison::Eq,
                value: Literal::Boolean(false),
            }),
        };
        let filter = Filter {
            negated: false,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(left.clone()),
                op: LogicalOp::And,
                right: Box::new(right.clone()),
            }),
        };
        assert_eq!(restrict_filter_idents(&filter, &idents).unwrap(), filter);

        // This should remove the right filter.
        assert_eq!(
            restrict_filter_idents(&filter, &idents[..1]).unwrap(),
            left
        );

        // And both
        assert_eq!(restrict_filter_idents(&filter, &[]), None);
    }

    #[test]
    fn test_split_query() {
        let q = Query::new("get a:b").unwrap();
        let split = q.split();
        assert_eq!(split, SplitQuery::Flat(q));

        let q = Query::new("get a:b | filter x == 0").unwrap();
        let split = q.split();
        assert_eq!(split, SplitQuery::Flat(q));

        let q = Query::new("{ get a:b } | join").unwrap();
        let split = q.split();
        let mut inner = Query::new("get a:b").unwrap();
        inner.end_time = q.end_time;
        assert_eq!(
            split,
            SplitQuery::Nested {
                subqueries: vec![inner],
                transformations: vec![TableOp::Basic(BasicTableOp::Join(Join))],
            }
        );

        let q = Query::new("{ get a:b | filter x == 0 } | join").unwrap();
        let split = q.split();
        let mut inner = Query::new("get a:b | filter x == 0").unwrap();
        inner.end_time = q.end_time;
        assert_eq!(
            split,
            SplitQuery::Nested {
                subqueries: vec![inner],
                transformations: vec![TableOp::Basic(BasicTableOp::Join(Join))],
            }
        );

        let q = Query::new("{ get a:b ; get a:b } | join").unwrap();
        let split = q.split();
        let mut inner = Query::new("get a:b").unwrap();
        inner.end_time = q.end_time;
        assert_eq!(
            split,
            SplitQuery::Nested {
                subqueries: vec![inner; 2],
                transformations: vec![TableOp::Basic(BasicTableOp::Join(Join))],
            }
        );

        let q = Query::new("{ { get a:b ; get a:b } | join } | join").unwrap();
        let split = q.split();
        let mut subqueries =
            vec![Query::new("{ get a:b; get a:b } | join").unwrap()];
        subqueries[0].end_time = q.end_time;
        let expected = SplitQuery::Nested {
            subqueries: subqueries.clone(),
            transformations: vec![TableOp::Basic(BasicTableOp::Join(Join))],
        };
        assert_eq!(split, expected);
        let split = subqueries[0].split();
        let mut inner = Query::new("get a:b").unwrap();
        inner.end_time = q.end_time;
        assert_eq!(
            split,
            SplitQuery::Nested {
                subqueries: vec![inner; 2],
                transformations: vec![TableOp::Basic(BasicTableOp::Join(Join))],
            }
        );
    }

    #[test]
    fn test_coalesce_predicates() {
        // Passed through group-by unchanged.
        let q = Query::new("get a:b | group_by [a] | filter a == 0").unwrap();
        let preds = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Integer(0),
            }),
        };
        assert_eq!(q.coalesced_predicates(None), Some(preds));

        // Merge the first two, then pass through group by.
        let q = Query::new(
            "get a:b | group_by [a] | filter a == 0 | filter a == 0",
        )
        .unwrap();
        let atom = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Integer(0),
            }),
        };
        let preds = Filter {
            negated: false,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(atom.clone()),
                op: LogicalOp::And,
                right: Box::new(atom.clone()),
            }),
        };
        assert_eq!(q.coalesced_predicates(None), Some(preds));

        // These are also merged, even though they're on different sides of the
        // group by.
        let q = Query::new(
            "get a:b | filter a == 0 | group_by [a] | filter a == 0",
        )
        .unwrap();
        let atom = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Integer(0),
            }),
        };
        let preds = Filter {
            negated: false,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(atom.clone()),
                op: LogicalOp::And,
                right: Box::new(atom.clone()),
            }),
        };
        assert_eq!(q.coalesced_predicates(None), Some(preds));

        // Second filter is _not_ passed through, because it refers to columns
        // not in the group by. We have only the first filter.
        let q = Query::new(
            "get a:b | filter a == 0 | group_by [a] | filter b == 0",
        )
        .unwrap();
        let preds = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Integer(0),
            }),
        };
        assert_eq!(q.coalesced_predicates(None), Some(preds));
    }

    #[test]
    fn test_coalesce_predicates_into_subqueries() {
        let q = "{ get a:b; get a:b } | join | filter foo == 'bar'";
        let query = Query::new(q).unwrap();
        let preds = query.coalesced_predicates(None).unwrap();
        let expected_predicate = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("foo".to_string()),
                cmp: Comparison::Eq,
                value: Literal::String("bar".into()),
            }),
        };
        assert_eq!(preds, expected_predicate);

        // Split the query, which should give us a list of two subqueries,
        // followed by the join and filter.
        let SplitQuery::Nested { subqueries, .. } = query.split() else {
            panic!();
        };
        for subq in subqueries.iter() {
            let inner = subq
                .coalesced_predicates(Some(expected_predicate.clone()))
                .unwrap();
            assert_eq!(
                inner, expected_predicate,
                "Predicates passed into an inner subquery should be preserved"
            );
        }
    }

    #[test]
    fn test_coalesce_predicates_into_subqueries_with_group_by() {
        let q = "{ get a:b | group_by [baz]; get a:b | group_by [foo] } | \
                 join | filter foo == 'bar'";
        let query = Query::new(q).unwrap();
        let preds = query.coalesced_predicates(None).unwrap();
        let expected_predicate = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("foo".to_string()),
                cmp: Comparison::Eq,
                value: Literal::String("bar".into()),
            }),
        };
        assert_eq!(preds, expected_predicate);

        // Split the query, which should give us a list of two subqueries,
        // followed by the join and filter.
        let SplitQuery::Nested { subqueries, .. } = query.split() else {
            panic!();
        };

        // The first subquery groups by a field "baz", which isn't in the outer
        // filter. It should have that outer predicate removed, and have no
        // predicates at all.
        let subq = &subqueries[0];
        assert!(
            subq.coalesced_predicates(Some(expected_predicate.clone()))
                .is_none(),
            "Should not push an outer predicate into a subquery, when that \
            subquery includes a group_by that does not name a field in the \
            outer predicate"
        );

        // The second subquery should include the expected predicate, since the
        // group_by includes the field named in the filter itself.
        let subq = &subqueries[1];
        let inner = subq
            .coalesced_predicates(Some(expected_predicate.clone()))
            .unwrap();
        assert_eq!(
            inner, expected_predicate,
            "Predicates passed into an inner subquery should be preserved, \
            when that inner subquery includes a group_by that names the \
            ident in the outer filter"
        );
    }

    #[test]
    fn test_coalesce_predicates_merged_into_subqueries() {
        let q = "{ get a:b | filter baz == 0; get a:b | filter baz == 0 } \
                 | join | filter foo == 'bar'";
        let query = Query::new(q).unwrap();
        let preds = query.coalesced_predicates(None).unwrap();
        let expected_predicate = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("foo".to_string()),
                cmp: Comparison::Eq,
                value: Literal::String("bar".into()),
            }),
        };
        assert_eq!(preds, expected_predicate);
        let expected_inner_predicate = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("baz".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Integer(0),
            }),
        };

        // Split the query, which should give us a list of two subqueries,
        // followed by the join and filter.
        let SplitQuery::Nested { subqueries, .. } = query.split() else {
            panic!();
        };
        for subq in subqueries.iter() {
            let inner = subq
                .coalesced_predicates(Some(expected_predicate.clone()))
                .unwrap();
            assert_eq!(
                inner,
                expected_predicate
                    .merge(&expected_inner_predicate, LogicalOp::And),
                "Predicates passed into an inner subquery should be preserved, \
                and merged with any subquery predicates",
            );
        }
    }

    #[test]
    fn test_query_end_time() {
        const MAX_DIFF: i64 = 1_000;
        let q = Query::new("get a:b").unwrap();
        assert!(
            (q.end_time - Utc::now()).num_nanoseconds().unwrap() < MAX_DIFF,
            "Query which does not explicitly name an end time should \
            use now as the end time",
        );

        let q = Query::new("get a:b | filter timestamp > @now() - 1s").unwrap();
        assert!(
            (q.end_time - Utc::now()).num_nanoseconds().unwrap() < MAX_DIFF,
            "Query which does not explicitly name an end time should \
            use now as the end time",
        );

        let then = Utc::now() - Duration::from_secs(60);
        let as_str = then.format("%Y-%m-%dT%H:%M:%S.%f");
        let q = Query::new(&format!("get a:b | filter timestamp < @{as_str}"))
            .unwrap();
        assert_eq!(
            q.end_time, then,
            "Query with a less-than filter and a timestamp should \
            set the query end time"
        );

        let q = Query::new(&format!("get a:b | filter timestamp <= @{as_str}"))
            .unwrap();
        assert_eq!(
            q.end_time, then,
            "Query with a less-than-or-equal filter and a timestamp should \
            set the query end time"
        );

        let q = Query::new(&format!("get a:b | filter timestamp > @{as_str}"))
            .unwrap();
        assert!(
            (q.end_time - Utc::now()).num_nanoseconds().unwrap() < MAX_DIFF,
            "Query with a greater-than timestamp filter should not set an \
            explicit query end time, and so use now"
        );

        let q = Query::new("get a:b | filter timestamp > @now() - 1d").unwrap();
        assert!(
            (q.end_time - Utc::now()).num_nanoseconds().unwrap() < MAX_DIFF,
            "Query which does not explicitly name an end time should \
            use now as the end time",
        );

        let q = Query::new(&format!(
            "get a:b | filter timestamp > @now() - 1d && timestamp < @{as_str}"
        ))
        .unwrap();
        assert_eq!(
            q.end_time, then,
            "Query with a compound less-than-or-equal filter and a timestamp should \
            set the query end time"
        );

        let then = Utc::now() - Duration::from_secs(60);
        let then_as_str = then.format("%Y-%m-%dT%H:%M:%S.%f");
        let even_earlier = then - Duration::from_secs(10);
        let even_earlier_as_str = even_earlier.format("%Y-%m-%dT%H:%M:%S.%f");
        let q = Query::new(&format!(
            "get a:b | filter timestamp < @{then_as_str} || timestamp < @{even_earlier_as_str}"
        ))
        .unwrap();
        assert_eq!(
            q.end_time, then,
            "Query with two less-than timestamp filters should use the later timestamp"
        );

        let expected = NaiveDateTime::parse_from_str(
            "2024-03-13T06:24:00",
            "%Y-%m-%dT%H:%M:%S%.f",
        )
        .unwrap()
        .and_utc();
        let q = "{ \
            get physical_data_link:bytes_sent ; \
            get physical_data_link:bytes_received \
            } | filter timestamp > @2024-03-13T06:20:00 && timestamp < @2024-03-13T06:24:00";
        let query = Query::new(q).unwrap();
        assert_eq!(query.end_time, expected);
    }

    #[test]
    fn test_query_end_time_across_subqueries() {
        let now = Utc::now();
        const FMT: &str = "%Y-%m-%dT%H:%M:%S.%f";
        let first = now - Duration::from_secs(1);
        let second = now - Duration::from_secs_f64(1e-3);
        let q = format!(
            "{{ \
                get a:b | filter timestamp > @{}; \
                get a:b | filter timestamp > @{} \
            }}",
            first.format(FMT),
            second.format(FMT),
        );
        let query = Query::new(q).unwrap();
        assert!(
            query.end_time > second,
            "This nested query should have used Utc::now() as the end time"
        );
        let end_time = query.end_time;
        let SplitQuery::Nested { subqueries, .. } = query.split() else {
            unreachable!();
        };
        for subq in subqueries.iter() {
            assert_eq!(
                subq.end_time, end_time,
                "All subqueries should have the same end time."
            );
        }
    }

    #[test]
    fn test_coalesce_limits() {
        let query = Query::new("get a:b | last 5").unwrap();
        let lim = query.coalesced_limits(None).expect("Should have a limit");
        assert_eq!(
            lim.kind,
            LimitKind::Last,
            "This limit op has the wrong kind"
        );
        assert_eq!(lim.count.get(), 5, "Limit has the wrong count");
    }

    #[test]
    fn test_coalesce_limits_merge_same_kind_within_query() {
        let qs = ["get a:b | last 10 | last 5", "get a:b | last 5 | last 10"];
        for q in qs {
            let query = Query::new(q).unwrap();
            let lim =
                query.coalesced_limits(None).expect("Should have a limit");
            assert_eq!(
                lim.kind,
                LimitKind::Last,
                "This limit op has the wrong kind"
            );
            assert_eq!(
                lim.count.get(),
                5,
                "Should have merged two limits of the same kind, \
                taking the one with the smaller count"
            );
        }
    }

    #[test]
    fn test_coalesce_limits_do_not_merge_different_kinds_within_query() {
        let qs =
            ["get a:b | first 10 | last 10", "get a:b | last 10 | first 10"];
        let kinds = [LimitKind::First, LimitKind::Last];
        for (q, kind) in qs.iter().zip(kinds) {
            let query = Query::new(q).unwrap();
            let lim =
                query.coalesced_limits(None).expect("Should have a limit");
            assert_eq!(lim.kind, kind, "This limit op has the wrong kind");
            assert_eq!(lim.count.get(), 10);
        }
    }

    #[test]
    fn test_coalesce_limits_rearrange_around_timestamp_filters() {
        let qs = [
            "get a:b | filter timestamp < @now() | first 10",
            "get a:b | filter timestamp > @now() | last 10",
        ];
        let kinds = [LimitKind::First, LimitKind::Last];
        for (q, kind) in qs.iter().zip(kinds) {
            let query = Query::new(q).unwrap();
            let lim = query.coalesced_limits(None).expect(
                "This limit op should have been re-arranged around \
                    a compatible timestamp filter",
            );
            assert_eq!(lim.kind, kind, "This limit op has the wrong kind");
            assert_eq!(lim.count.get(), 10);
        }
    }

    #[test]
    fn test_coalesce_limits_do_not_rearrange_around_incompatible_timestamp_filters()
     {
        let qs = [
            "get a:b | filter timestamp < @now() | last 10",
            "get a:b | filter timestamp > @now() | first 10",
        ];
        for q in qs {
            let query = Query::new(q).unwrap();
            assert!(
                query.coalesced_limits(None).is_none(),
                "This limit op should have be merged around an \
                incompatible timestamp filter"
            );
        }
    }

    #[test]
    fn test_coalesce_limits_merge_from_outer_query() {
        let query = Query::new("get a:b | last 10").unwrap();
        let outer =
            Limit { kind: LimitKind::Last, count: 5.try_into().unwrap() };
        let lim = query
            .coalesced_limits(Some(outer))
            .expect("Should have a limit here");
        assert_eq!(lim.kind, LimitKind::Last, "Limit has the wrong kind");
        assert_eq!(
            lim.count.get(),
            5,
            "Did not pass through outer limit correctly"
        );
    }

    #[test]
    fn test_coalesce_limits_do_not_merge_different_kind_from_outer_query() {
        let query = Query::new("get a:b | last 10").unwrap();
        let outer =
            Limit { kind: LimitKind::First, count: 5.try_into().unwrap() };
        let lim = query
            .coalesced_limits(Some(outer))
            .expect("Should have a limit here");
        assert_eq!(lim.kind, LimitKind::Last, "Limit has the wrong kind");
        assert_eq!(
            lim.count.get(),
            10,
            "Inner limit of different kind should ignore the outer one"
        );
    }

    #[test]
    fn test_coalesce_limits_do_not_coalesce_incompatible_kind_from_outer_query()
    {
        let query = Query::new("get a:b | filter timestamp > @now()").unwrap();
        let outer =
            Limit { kind: LimitKind::First, count: 5.try_into().unwrap() };
        assert!(
            query.coalesced_limits(Some(outer)).is_none(),
            "Should not coalesce a limit from the outer query, when the \
            inner query contains an incompatible timestamp filter"
        );
    }

    #[test]
    fn test_insert_filters() {
        let query = Query::new("get a:b | filter timestamp > @now()").unwrap();
        let silo_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let scope = QueryAuthzScope::Project { silo_id, project_id };
        let new_query = query.insert_authz_filters(scope);

        assert_eq!(query.parsed.table_ops().len(), 2);
        assert_eq!(new_query.parsed.table_ops().len(), 4);

        // inserted after the get
        assert_eq!(
            new_query.parsed.table_ops().nth(1).unwrap().to_string(),
            format!("filter (silo_id == \"{}\")", silo_id)
        );
        assert_eq!(
            new_query.parsed.table_ops().nth(2).unwrap().to_string(),
            format!("filter (project_id == \"{}\")", project_id)
        );
    }

    #[test]
    fn test_insert_filters_with_subqueries() {
        let query = Query::new(
            "{ get a:b | filter timestamp > @2025-03-05; get c:d } | filter timestamp > @2025-01-01",
        )
        .unwrap();

        let silo_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        // Define expected filters as AST nodes
        let silo_filter = uuid_eq_filter("silo_id", silo_id);
        let project_filter = uuid_eq_filter("project_id", project_id);

        let expected_silo_op =
            TableOp::Basic(BasicTableOp::Filter(silo_filter.clone()));
        let expected_project_op =
            TableOp::Basic(BasicTableOp::Filter(project_filter.clone()));

        let scope = QueryAuthzScope::Project { silo_id, project_id };
        let new_query = query.insert_authz_filters(scope);

        // Check top-level structure (should remain one grouped op and one filter)
        let orig_ops = query.parsed.table_ops().collect::<Vec<_>>();
        assert_eq!(orig_ops.len(), 2);
        assert_matches!(orig_ops[1], TableOp::Basic(BasicTableOp::Filter(_)));

        let new_ops = new_query.parsed.table_ops().collect::<Vec<_>>();
        assert_eq!(new_ops.len(), 2);

        // second filter op unchanged
        assert_eq!(orig_ops[1], new_ops[1]);

        let only_op = new_query.parsed.first_op();
        let TableOp::Grouped(GroupedTableOp { ops }) = only_op else {
            panic!("Expected the only operation to be TableOp::Grouped");
        };

        assert_eq!(ops.len(), 2, "Expected two subqueries in the group");

        // first subquery has the original get and filter and now two extra filters in the middle
        let subq1: Vec<_> = ops[0].table_ops().cloned().collect();
        assert_eq!(subq1.len(), 4,);
        assert_matches!(subq1[0], TableOp::Basic(BasicTableOp::Get(_)));
        assert_eq!(subq1[1], expected_silo_op);
        assert_eq!(subq1[2], expected_project_op);
        assert_matches!(subq1[3], TableOp::Basic(BasicTableOp::Filter(_)));

        // second subquery has the original get and now two extra filters
        let subq2: Vec<_> = ops[1].table_ops().cloned().collect();
        assert_eq!(subq2.len(), 3);
        assert_matches!(subq2[0], TableOp::Basic(BasicTableOp::Get(_)));
        assert_eq!(subq2[1], expected_silo_op);
        assert_eq!(subq2[2], expected_project_op);
    }

    #[test]
    fn test_insert_filters_with_nested_subqueries() {
        let query_str = "{ get a:b | filter timestamp > @2025-03-05; { get c:d; get e:f | filter timestamp < @2025-04-06 }; get g:h }";
        let query = Query::new(query_str).unwrap();
        let silo_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();

        // Define expected filters as AST nodes
        let silo_filter = uuid_eq_filter("silo_id", silo_id);
        let project_filter = uuid_eq_filter("project_id", project_id);

        let expected_silo_op =
            TableOp::Basic(BasicTableOp::Filter(silo_filter.clone()));
        let expected_project_op =
            TableOp::Basic(BasicTableOp::Filter(project_filter.clone()));

        let scope = QueryAuthzScope::Project { silo_id, project_id };
        let new_query = query.insert_authz_filters(scope);

        // Check top-level structure (should remain a single grouped op)
        assert_eq!(query.parsed.table_ops().len(), 1);
        assert_eq!(new_query.parsed.table_ops().len(), 1);

        let top_op = new_query.parsed.first_op();
        let TableOp::Grouped(GroupedTableOp { ops: top_ops }) = top_op else {
            panic!("Expected the top operation to be TableOp::Grouped");
        };

        assert_eq!(top_ops.len(), 3,);

        // Check first subquery (get a:b | filter ...)
        let subq1: Vec<_> = top_ops[0].table_ops().cloned().collect();
        assert_eq!(subq1.len(), 4, "Expected 4 ops in subquery 1");
        assert_matches!(subq1[0], TableOp::Basic(BasicTableOp::Get(_)));
        assert_eq!(subq1[1], expected_silo_op);
        assert_eq!(subq1[2], expected_project_op);
        assert_matches!(subq1[3], TableOp::Basic(BasicTableOp::Filter(_))); // Original filter

        // Check second subquery (the nested group { get c:d; get e:f | filter ... })
        let nested_ops = &top_ops[1].table_ops().collect::<Vec<_>>();
        assert_eq!(nested_ops.len(), 1);

        let TableOp::Grouped(GroupedTableOp { ops: nested_queries }) =
            nested_ops[0]
        else {
            panic!("Expected the top operation to be TableOp::Grouped");
        };
        let nested_subq1 = nested_queries[0].table_ops().collect::<Vec<_>>();
        assert_eq!(nested_subq1.len(), 3);
        assert_matches!(nested_subq1[0], TableOp::Basic(BasicTableOp::Get(_)));
        assert_eq!(nested_subq1[1], &expected_silo_op);
        assert_eq!(nested_subq1[2], &expected_project_op);

        // Check second nested subquery (get e:f | filter ...)
        let nested_subq2 = nested_queries[1].table_ops().collect::<Vec<_>>();
        assert_eq!(nested_subq2.len(), 4);
        assert_matches!(nested_subq2[0], TableOp::Basic(BasicTableOp::Get(_)));
        assert_eq!(nested_subq2[1], &expected_silo_op);
        assert_eq!(nested_subq2[2], &expected_project_op);
        assert_matches!(
            nested_subq2[3],
            TableOp::Basic(BasicTableOp::Filter(_))
        ); // Original filter

        // Check third subquery (get g:h)
        let subq3: Vec<_> = top_ops[2].table_ops().cloned().collect();
        assert_eq!(subq3.len(), 3, "Expected 3 ops in subquery 3");
        assert_matches!(subq3[0], TableOp::Basic(BasicTableOp::Get(_)));
        assert_eq!(subq3[1], expected_silo_op);
        assert_eq!(subq3[2], expected_project_op);
    }
}
