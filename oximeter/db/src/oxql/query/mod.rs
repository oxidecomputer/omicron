// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single OxQL query.

// Copyright 2024 Oxide Computer Company

use super::ast::ident::Ident;
use super::ast::logical_op::LogicalOp;
use super::ast::table_ops::filter::CompoundFilter;
use super::ast::table_ops::filter::FilterExpr;
use super::ast::table_ops::group_by::GroupBy;
use super::ast::table_ops::BasicTableOp;
use super::ast::table_ops::TableOp;
use super::ast::SplitQuery;
use crate::oxql::ast::grammar;
use crate::oxql::ast::table_ops::filter::Filter;
use crate::oxql::ast::Query as QueryNode;
use crate::oxql::fmt_parse_error;
use crate::oxql::Error;
use crate::TimeseriesName;
use chrono::DateTime;
use chrono::Utc;
use std::time::Duration;

/// Special identifiers for column names or other widely-used values.
pub mod special_idents {
    use oximeter::DatumType;

    pub const TIMESTAMP: &str = "timestamp";
    pub const START_TIME: &str = "start_time";
    pub const DATUM: &str = "datum";
    pub const BINS: &str = "bins";
    pub const COUNTS: &str = "counts";
    pub const DATETIME64: &str = "DateTime64";
    pub const ARRAYU64: &str = "Array[u64]";

    pub fn array_type_name_from_histogram_type(
        type_: DatumType,
    ) -> Option<String> {
        if !type_.is_histogram() {
            return None;
        }
        Some(format!(
            "Array[{}]",
            type_.to_string().strip_prefix("Histogram").unwrap().to_lowercase(),
        ))
    }
}

/// A parsed OxQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub(super) parsed: QueryNode,
    pub(super) end_time: DateTime<Utc>,
}

impl Query {
    /// Construct a query written in OxQL.
    pub fn new(query: impl AsRef<str>) -> Result<Self, Error> {
        let raw = query.as_ref().trim();
        const MAX_LEN: usize = 4096;
        anyhow::ensure!(
            raw.len() <= MAX_LEN,
            "Queries must be <= {} characters",
            MAX_LEN,
        );
        let parsed = grammar::query_parser::query(raw)
            .map_err(|e| fmt_parse_error(raw, e))?;

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

    /// Return the set of all predicates in the query, coalesced.
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
        mut outer: Option<Filter>,
    ) -> Option<Filter> {
        let maybe_filter = self.transformations().iter().rev().fold(
            None,
            |maybe_filter, next_tr| {
                // Transformations only return basic ops, since all the
                // subqueries must be at the prefix of the query.
                let TableOp::Basic(op) = next_tr else {
                    unreachable!();
                };

                match op {
                    BasicTableOp::GroupBy(GroupBy { identifiers, .. }) => {
                        // We may have been passed predicates from an outer
                        // query. Those also need to be restricted, if we're
                        // trying to push them through a group_by operation.
                        outer = outer.as_ref().and_then(|outer| {
                            restrict_filter_idents(outer, identifiers)
                        });

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
                    _ => maybe_filter,
                }
            },
        );

        // Merge in any predicates passed from an outer query, which may have
        // been restricted as we moved through group_by operations.
        match (outer, maybe_filter) {
            (None, any) => any,
            (Some(outer), None) => Some(outer),
            (Some(outer), Some(inner)) => {
                Some(outer.merge(&inner, LogicalOp::And))
            }
        }
    }

    pub(crate) fn split(&self) -> SplitQuery {
        self.parsed.split(self.end_time)
    }
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

/// Describes the time alignment for an OxQL query.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Alignment {
    /// The end time of the query, which the temporal reference point.
    pub end_time: DateTime<Utc>,
    /// The alignment period, the interval on which values are produced.
    pub period: Duration,
}

#[cfg(test)]
mod tests {
    use super::Filter;
    use super::Ident;
    use super::Query;
    use crate::oxql::ast::cmp::Comparison;
    use crate::oxql::ast::literal::Literal;
    use crate::oxql::ast::logical_op::LogicalOp;
    use crate::oxql::ast::table_ops::filter::CompoundFilter;
    use crate::oxql::ast::table_ops::filter::FilterExpr;
    use crate::oxql::ast::table_ops::filter::SimpleFilter;
    use crate::oxql::ast::table_ops::join::Join;
    use crate::oxql::ast::table_ops::BasicTableOp;
    use crate::oxql::ast::table_ops::TableOp;
    use crate::oxql::ast::SplitQuery;
    use crate::oxql::query::restrict_filter_idents;
    use chrono::NaiveDateTime;
    use chrono::Utc;
    use std::time::Duration;

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
            restrict_filter_idents(&filter, &[ident.clone()]).unwrap(),
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
                expected_predicate.merge(&expected_inner_predicate, LogicalOp::And),
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
            q.end_time,
            then,
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
            q.end_time,
            then,
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
}
