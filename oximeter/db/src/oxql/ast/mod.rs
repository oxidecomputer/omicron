// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST for the Oximeter Query Language.

// Copyright 2024 Oxide Computer Company

use std::collections::BTreeSet;
use std::fmt;

use chrono::DateTime;
use chrono::Utc;
use oximeter::TimeseriesName;
use table_ops::filter::Filter;

use self::table_ops::BasicTableOp;
use self::table_ops::GroupedTableOp;
use self::table_ops::TableOp;
pub mod cmp;
pub(crate) mod grammar;
pub mod ident;
pub mod literal;
pub mod logical_op;
pub mod table_ops;

/// An OxQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    ops: Vec<TableOp>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n_ops = self.ops.len();
        for (i, op) in self.ops.iter().enumerate() {
            write!(f, "{op}")?;
            if i < n_ops - 1 {
                write!(f, " | ")?;
            }
        }
        Ok(())
    }
}

impl Query {
    // Return the first operation in the query, which is always a form of `get`.
    pub(crate) fn first_op(&self) -> &TableOp {
        self.ops.first().expect("Should have parsed at least 1 operation")
    }

    /// Iterate over the table operations.
    pub(crate) fn table_ops(
        &self,
    ) -> impl ExactSizeIterator<Item = &'_ TableOp> + '_ {
        self.ops.iter()
    }

    /// Return the name of the first referenced timeseries.
    ///
    /// This is from the first `get`, which might be from a subquery.
    pub(crate) fn timeseries_name(&self) -> &TimeseriesName {
        match self.first_op() {
            TableOp::Basic(BasicTableOp::Get(n)) => n,
            TableOp::Basic(_) => unreachable!(),
            TableOp::Grouped(GroupedTableOp { ops }) => {
                ops.first().unwrap().timeseries_name()
            }
        }
    }

    /// Return _all_ timeseries names referred to by get table operations.
    pub(crate) fn all_timeseries_names(&self) -> BTreeSet<&TimeseriesName> {
        let mut set = BTreeSet::new();
        self.all_timeseries_names_impl(&mut set);
        set
    }

    // Add all timeseries names to the provided set, recursing into subqueries.
    fn all_timeseries_names_impl<'a>(
        &'a self,
        set: &mut BTreeSet<&'a TimeseriesName>,
    ) {
        for op in self.ops.iter() {
            match op {
                TableOp::Basic(BasicTableOp::Get(name)) => {
                    set.insert(name);
                }
                TableOp::Basic(_) => {}
                TableOp::Grouped(GroupedTableOp { ops }) => {
                    for query in ops.iter() {
                        query.all_timeseries_names_impl(set);
                    }
                }
            }
        }
    }

    // Check that this query (and any subqueries) start with a get table op, and
    // that there are no following get operations. I.e., we have:
    //
    // get ... | <no gets>
    // { get .. } | <no gets>
    // { get .. ; get .. } | <no gets>
    pub(crate) fn all_gets_at_query_start(&self) -> bool {
        fn all_gets_at_query_start(ops: &[TableOp]) -> bool {
            let (head, tail) = ops.split_at(1);
            match &head[0] {
                // If the head is a get, check that there are no following get
                // operations.
                TableOp::Basic(BasicTableOp::Get(_)) => {
                    !tail.iter().any(|op| {
                        matches!(op, TableOp::Basic(BasicTableOp::Get(_)))
                    })
                }
                // Cannot start with any other basic op.
                TableOp::Basic(_) => false,
                // Recurse for grouped ops.
                TableOp::Grouped(GroupedTableOp { ops }) => {
                    ops.iter().all(Query::all_gets_at_query_start)
                }
            }
        }
        all_gets_at_query_start(&self.ops)
    }

    // Return the non-get table transformations.
    pub(crate) fn transformations(&self) -> &[TableOp] {
        &self.ops[1..]
    }

    // Split the query into either:
    //
    // - a list of nested queries and the remaining table ops in self, or
    // - the flat query contained in self.
    pub(crate) fn split(&self, query_end_time: DateTime<Utc>) -> SplitQuery {
        match &self.ops[0] {
            TableOp::Basic(BasicTableOp::Get(_)) => {
                SplitQuery::Flat(crate::oxql::Query {
                    parsed: self.clone(),
                    end_time: query_end_time,
                })
            }
            TableOp::Basic(_) => unreachable!(),
            TableOp::Grouped(GroupedTableOp { ops }) => SplitQuery::Nested {
                subqueries: ops
                    .iter()
                    .cloned()
                    .map(|parsed| crate::oxql::Query {
                        parsed,
                        end_time: query_end_time,
                    })
                    .collect(),
                transformations: self.ops[1..].to_vec(),
            },
        }
    }

    // Return the last referenced timestamp in the query, if any.
    pub(crate) fn query_end_time(&self) -> Option<DateTime<Utc>> {
        match &self.ops[0] {
            TableOp::Basic(BasicTableOp::Get(_)) => self
                .transformations()
                .iter()
                .filter_map(|op| {
                    let TableOp::Basic(BasicTableOp::Filter(filter)) = op
                    else {
                        return None;
                    };
                    filter.last_timestamp()
                })
                .max(),
            TableOp::Basic(_) => unreachable!(),
            TableOp::Grouped(GroupedTableOp { ops }) => {
                let grouped_max =
                    ops.iter().filter_map(Self::query_end_time).max();
                let op_max = self
                    .transformations()
                    .iter()
                    .filter_map(|op| {
                        let TableOp::Basic(BasicTableOp::Filter(filter)) = op
                        else {
                            return None;
                        };
                        filter.last_timestamp()
                    })
                    .max();
                grouped_max.max(op_max)
            }
        }
    }

    /// Insert filters after the `get`, or in the case of subqueries, recurse
    /// down the tree and insert them after each get.
    pub(crate) fn insert_filters(&self, filters: Vec<Filter>) -> Self {
        let mut new_ops = self.ops.clone();

        match self.first_op() {
            // for a basic query, just insert the filters after the first entry (the get)
            TableOp::Basic(_) => {
                let filter_ops = filters
                    .iter()
                    .map(|filter| {
                        TableOp::Basic(BasicTableOp::Filter(filter.clone()))
                    })
                    .collect::<Vec<_>>();
                new_ops.splice(1..1, filter_ops);
            }
            // for a grouped query, recurse to insert the filters in all subqueries
            TableOp::Grouped(op) => {
                new_ops[0] = TableOp::Grouped(GroupedTableOp {
                    ops: op
                        .ops
                        .iter()
                        .map(|query| query.insert_filters(filters.clone()))
                        .collect(),
                });
            }
        }

        Self { ops: new_ops }
    }
}

// Either a flat query or one with nested subqueries.
//
// OxQL supports subqueries. Though they can be nested, they must always be at
// the front of a query. This represents either a query that is flat, _or_ that
// prefix of subqueries and the following transformations.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SplitQuery {
    Flat(crate::oxql::Query),
    Nested {
        subqueries: Vec<crate::oxql::Query>,
        transformations: Vec<TableOp>,
    },
}
