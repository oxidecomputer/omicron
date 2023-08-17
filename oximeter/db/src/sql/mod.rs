// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oximeter-flavored SQL.

// Copyright 2023 Oxide Computer Company

use sqlparser::dialect::Dialect;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;
use sqlparser::ast::SetExpr;
use sqlparser::ast::TableFactor;

/// The Oximeter SQL dialect.
///
/// This is identical to [`AnsiDialect`], except that it supports `:` in
/// identifier names. That is to support queries against timeseries virtual
/// tables, which are formed by `<target_name>:<metric_name>`.
#[derive(Clone, Copy, Debug)]
struct OxdbDialect;

impl OxdbDialect {
    const PARENT: AnsiDialect = AnsiDialect {};
}

impl Dialect for OxdbDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        Self::PARENT.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        Self::PARENT.is_identifier_part(ch) || ch == ':'
    }
}

#[derive(Clone, Debug, thiserror::Error)]
enum SqlError {
    #[error(transparent)]
    Parse(#[from] ParserError),

    #[error("Multiple SQL statements not supported")]
    MultipleStatements,

    #[error("Unsupported SQL construction: {0}")]
    UnsupportedSql(&'static str),
}

/// Query one timeseries "virtual table".
#[derive(Clone, Debug)]
pub struct VirtualTableQuery {
    table_name: String,
    statements: Vec<Statement>,
}

// Spit out:
//
// filter_{field_name} as
// (
//  select timeseries_name, timeseries_key, field_value
//  from oximeter.fields_{dtype}
//  where field_name = '{field_name}'
// )
// 
// for each field in the timeseries schema.
//
// Then join them all via
//
// with filter_on_{field_name0} as (...), filter_on_{field_name0} as (...)
// select
//  filter_on_{field_name0}.timeseries_key as timeseries_key,
//  filter_on_{field_name0}.field_value as {field_name0},
//  filter_on_{field_name1}.field_value as {field_name1},
//  ...
// from
//  filter_on_{field_name0}
//  inner join filter_on_{field_name1} on
//  (
//      filter_on_{field_name0}.timeseries_name = filter_on_{field_name1}.timeseries_name and
//      filter_on_{field_name0}.timeseries_key = filter_on_{field_name1}.timeseries_key
//   )
//  inner join filter_on_{field_name2} on
//  (
//      filter_on_{field_name1}.timeseries_name = filter_on_{field_name2}.timeseries_name and
//      filter_on_{field_name1}.timeseries_key = filter_on_{field_name2}.timeseries_key
//   )
//   ...
//
//
// This selects the fields as a virtual table, but we still need to get the
// timestamps and measurements. We can't use a join there.
//
// I think we can join actually. We may not want to, but we can. But we
// _definitely_ want to filter things more and possibly put the measurements
// tables on the left of the join.
//
// ClickHouse seems to still have no cost-based optimizer. That is nuts, BTW. If
// you ask it to do a join, it seems to literally do the cartesian product and
// then filter as needed to satisfy the join logic. I.e.
//
// select * from foo inner join bar using id
//
// Literally loads all of `bar`.

impl VirtualTableQuery {
    pub fn from_sql(s: impl AsRef<str>) -> Result<Self, SqlError> {
        let statements = Parser::parse_sql(&OxdbDialect, s.as_ref())?;
        if statements.len() > 1 {
            return Err(SqlError::MultipleStatements);
        }
        let Statement::Query(query) = &statements[0] else {
            return Err(SqlError::UnsupportedSql("only SELECT queries are supported"))
        };
        if query.with.is_some() {
            return Err(SqlError::UnsupportedSql("CTEs are not supported"));
        }
        if !query.order_by.is_empty() {
            return Err(SqlError::UnsupportedSql("ORDER BY is not supported"));
        }
        if query.limit.is_some() {
            return Err(SqlError::UnsupportedSql("LIMIT is not supported"));
        }
        if query.offset.is_some() {
            return Err(SqlError::UnsupportedSql("OFFSET is not supported"));
        }
        if query.fetch.is_some() {
            return Err(SqlError::UnsupportedSql("FETCH is not supported"));
        }
        if !query.locks.is_empty() {
            return Err(SqlError::UnsupportedSql("LOCK clauses are not supported"));
        }
        let SetExpr::Select(body) = &*query.body else {
            return Err(SqlError::UnsupportedSql("only SELECT queries are supported"));
        };
        let TableFactor::Table { name, alias, args, with_hints } = &body.from[0].relation else {
            return Err(SqlError::UnsupportedSql("Selections must be from an existing table"));
        };
        if alias.is_some() {
            return Err(SqlError::UnsupportedSql("Table aliases not supported"));
        };
        if args.is_some() {
            return Err(SqlError::UnsupportedSql("Table functions not supported"));
        }
        if !with_hints.is_empty() {
            return Err(SqlError::UnsupportedSql("Table hints not supported"));
        }
        if name.0.len() > 1 {
            return Err(SqlError::UnsupportedSql("Only a single table may be queried"));
        }
        let table_name = name.0[0].value.clone();
        Ok(Self { table_name, statements })
    }
}

#[cfg(test)]
mod tests {
    use super::Dialect;
    use super::OxdbDialect;
    use super::Parser;
    use super::VirtualTableQuery;
    use sqlparser::ast::Query;
    use sqlparser::ast::TableFactor;
    use sqlparser::ast::ObjectName;
    use sqlparser::ast::SetExpr;
    use sqlparser::ast::Statement;

    #[test]
    fn test_oxdb_identifier() {
        let st = Parser::parse_sql(&OxdbDialect, "select * from 'target:metric'").unwrap();
        assert_eq!(st.len(), 1);
        let Statement::Query(query) = &st[0] else {
            panic!("expected query");
        };
        let SetExpr::Select(body) = &*query.body else {
            panic!("expected select set expression");
        };
        assert_eq!(body.from.len(), 1);
        let TableFactor::Table { name, .. } = &body.from[0].relation else {
            panic!("expected table name as the relation");
        };
        assert_eq!(name.0.len(), 1);
        assert_eq!(name.0[0].value, "target:metric");
    }

    #[test]
    fn test_virtual_table_query_from_sql() {
        VirtualTableQuery::from_sql("select * from 'target:metric' where foo = 'bar'").unwrap();
        assert!(VirtualTableQuery::from_sql("select * from (select * from bar) where foo = 'bar'").is_err());
    }
}
