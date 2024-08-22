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

use self::align::Align;
use self::filter::Filter;
use self::group_by::GroupBy;
use self::join::Join;
use self::limit::Limit;
use crate::oxql::ast::Query;
use crate::oxql::Error;
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

/// Any kind of OxQL table operation.
#[derive(Clone, Debug, PartialEq)]
pub enum TableOp {
    Basic(BasicTableOp),
    Grouped(GroupedTableOp),
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
