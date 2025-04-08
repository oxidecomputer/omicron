// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST nodes for table operations.

// Copyright 2024 Oxide Computer Company

pub mod align;
pub mod filter;
pub mod get;
pub mod group_by;
pub mod join;
pub mod limit;

use std::fmt;

use self::align::Align;
use self::filter::Filter;
use self::group_by::GroupBy;
use self::join::Join;
use self::limit::Limit;
use crate::oxql::Error;
use crate::oxql::ast::Query;
use chrono::DateTime;
use chrono::Utc;
use oximeter::TimeseriesName;
use oxql_types::Table;

/// A basic table operation, the atoms of an OxQL query.
#[derive(Clone, Debug, PartialEq)]
pub enum BasicTableOp {
    Get(TimeseriesName),
    Filter(Filter),
    GroupBy(GroupBy),
    Join(Join),
    Align(Align),
    Limit(Limit),
}

impl fmt::Display for BasicTableOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BasicTableOp::Get(name) => write!(f, "get {name}"),
            BasicTableOp::Filter(filter) => write!(f, "filter {filter}"),
            BasicTableOp::GroupBy(group_by) => write!(f, "{group_by}"),
            BasicTableOp::Join(_) => write!(f, "join"),
            BasicTableOp::Align(align) => write!(f, "align {align}"),
            BasicTableOp::Limit(limit) => write!(f, "{limit}"),
        }
    }
}

impl BasicTableOp {
    pub(crate) fn apply(
        &self,
        tables: &[Table],
        query_end: &DateTime<Utc>,
    ) -> Result<Vec<Table>, Error> {
        match self {
            BasicTableOp::Get(_) => panic!("Should not apply get table ops"),
            BasicTableOp::Filter(f) => f.apply(tables),
            BasicTableOp::GroupBy(g) => g.apply(tables),
            BasicTableOp::Join(j) => j.apply(tables),
            BasicTableOp::Align(a) => a.apply(tables, query_end),
            BasicTableOp::Limit(l) => l.apply(tables),
        }
    }
}

/// A grouped table operation is a subquery in OxQL.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupedTableOp {
    pub ops: Vec<Query>,
}

impl fmt::Display for GroupedTableOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ ")?;
        let n_queries = self.ops.len();
        for (i, query) in self.ops.iter().enumerate() {
            write!(f, "{query}")?;
            if i < n_queries - 1 {
                write!(f, "; ")?;
            } else {
                write!(f, " ")?;
            }
        }
        write!(f, "}}")
    }
}

/// Any kind of OxQL table operation.
#[derive(Clone, Debug, PartialEq)]
pub enum TableOp {
    Basic(BasicTableOp),
    Grouped(GroupedTableOp),
}

impl fmt::Display for TableOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableOp::Basic(basic) => write!(f, "{basic}"),
            TableOp::Grouped(grouped) => write!(f, "{grouped}"),
        }
    }
}

impl TableOp {
    pub(crate) fn apply(
        &self,
        tables: &[Table],
        query_end: &DateTime<Utc>,
    ) -> Result<Vec<Table>, Error> {
        let TableOp::Basic(basic) = self else {
            panic!("Should not apply grouped table ops");
        };
        basic.apply(tables, query_end)
    }
}

#[cfg(test)]
mod test {
    use super::GroupedTableOp;
    use crate::oxql::ast::Query;
    use crate::oxql::ast::table_ops::BasicTableOp;
    use crate::oxql::ast::table_ops::TableOp;

    #[test]
    fn test_grouped_table_op_display() {
        let op = GroupedTableOp { ops: vec![] };
        assert_eq!(op.to_string(), "{ }");

        let name = "foo:bar".try_into().unwrap();
        let op = TableOp::Basic(BasicTableOp::Get(name));
        let grouped =
            GroupedTableOp { ops: vec![Query { ops: vec![op.clone()] }] };
        assert_eq!(grouped.to_string(), "{ get foo:bar }");

        let grouped = GroupedTableOp {
            ops: vec![Query { ops: vec![op.clone()] }, Query { ops: vec![op] }],
        };
        assert_eq!(grouped.to_string(), "{ get foo:bar; get foo:bar }");
    }
}
