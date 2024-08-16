// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST for the Oximeter Query Language.

// Copyright 2024 Oxide Computer Company

use chrono::DateTime;
use chrono::Utc;
use oximeter::TimeseriesName;

use self::table_ops::BasicTableOp;
use self::table_ops::GroupedTableOp;
use self::table_ops::TableOp;
pub mod cmp;
pub(super) mod grammar;
pub mod ident;
pub mod literal;
pub mod logical_op;
pub mod table_ops;

/// An OxQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    ops: Vec<TableOp>,
}

impl Query {
    // Return the first operation in the query, which is always a form of `get`.
    fn first_op(&self) -> &TableOp {
        self.ops.first().expect("Should have parsed at least 1 operation")
    }

    pub(crate) fn timeseries_name(&self) -> &TimeseriesName {
        match self.first_op() {
            TableOp::Basic(BasicTableOp::Get(n)) => n,
            TableOp::Basic(_) => unreachable!(),
            TableOp::Grouped(GroupedTableOp { ops }) => {
                ops.first().unwrap().timeseries_name()
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
