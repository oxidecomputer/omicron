// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilites to query "any data" from the database.
//!
//! This is useful for parsing internal cockroach structures and schemas, but
//! the typing is dynamic. As a result, this is relegated to a test-only
//! crate.

use chrono::{DateTime, Utc};
use pretty_assertions::assert_eq;
use std::net::IpAddr;
use uuid::Uuid;

/// A named SQL enum, of the form "typename, value"
#[derive(PartialEq, Clone, Debug)]
pub struct SqlEnum {
    name: String,
    variant: String,
}

impl From<(&str, &str)> for SqlEnum {
    fn from((name, variant): (&str, &str)) -> Self {
        Self { name: name.to_string(), variant: variant.to_string() }
    }
}

/// A newtype wrapper around a string, which allows us to more liberally
/// interpret SQL types.
///
/// Note that for the purposes of schema comparisons, we don't care about parsing
/// the contents of the database, merely the schema and equality of contained
/// data.
#[derive(PartialEq, Clone, Debug)]
pub enum AnySqlType {
    Bool(bool),
    DateTime,
    Enum(SqlEnum),
    Float4(f32),
    Int4(i32),
    Int8(i64),
    Json(serde_json::value::Value),
    String(String),
    TextArray(Vec<String>),
    Uuid(Uuid),
    Inet(IpAddr),
    // TODO: This isn't exhaustive, feel free to add more.
    //
    // These should only be necessary for rows where the database schema changes also choose to
    // populate data.
}

impl From<bool> for AnySqlType {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<SqlEnum> for AnySqlType {
    fn from(value: SqlEnum) -> Self {
        Self::Enum(value)
    }
}

impl From<f32> for AnySqlType {
    fn from(value: f32) -> Self {
        Self::Float4(value)
    }
}

impl From<i32> for AnySqlType {
    fn from(value: i32) -> Self {
        Self::Int4(value)
    }
}

impl From<i64> for AnySqlType {
    fn from(value: i64) -> Self {
        Self::Int8(value)
    }
}

impl From<String> for AnySqlType {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Uuid> for AnySqlType {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<IpAddr> for AnySqlType {
    fn from(value: IpAddr) -> Self {
        Self::Inet(value)
    }
}

impl From<serde_json::Value> for AnySqlType {
    fn from(value: serde_json::Value) -> Self {
        Self::Json(value)
    }
}

impl AnySqlType {
    pub fn as_str(&self) -> &str {
        match self {
            AnySqlType::String(s) => s,
            _ => panic!("Not a string type"),
        }
    }
}

impl<'a> tokio_postgres::types::FromSql<'a> for AnySqlType {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        if String::accepts(ty) {
            return Ok(AnySqlType::String(String::from_sql(ty, raw)?));
        }
        if DateTime::<Utc>::accepts(ty) {
            // We intentionally drop the time here -- we only care that there
            // is some value present.
            let _ = DateTime::<Utc>::from_sql(ty, raw)?;
            return Ok(AnySqlType::DateTime);
        }
        if bool::accepts(ty) {
            return Ok(AnySqlType::Bool(bool::from_sql(ty, raw)?));
        }
        if Uuid::accepts(ty) {
            return Ok(AnySqlType::Uuid(Uuid::from_sql(ty, raw)?));
        }
        if i32::accepts(ty) {
            return Ok(AnySqlType::Int4(i32::from_sql(ty, raw)?));
        }
        if i64::accepts(ty) {
            return Ok(AnySqlType::Int8(i64::from_sql(ty, raw)?));
        }
        if f32::accepts(ty) {
            return Ok(AnySqlType::Float4(f32::from_sql(ty, raw)?));
        }
        if serde_json::value::Value::accepts(ty) {
            return Ok(AnySqlType::Json(serde_json::value::Value::from_sql(
                ty, raw,
            )?));
        }
        if Vec::<String>::accepts(ty) {
            return Ok(AnySqlType::TextArray(Vec::<String>::from_sql(
                ty, raw,
            )?));
        }
        if IpAddr::accepts(ty) {
            return Ok(AnySqlType::Inet(IpAddr::from_sql(ty, raw)?));
        }

        use tokio_postgres::types::Kind;
        match ty.kind() {
            Kind::Enum(_) => {
                Ok(AnySqlType::Enum(SqlEnum {
                    name: ty.name().to_string(),
                    variant: std::str::from_utf8(raw)?.to_string(),
                }))
            },
            _ => {
                Err(anyhow::anyhow!(
                    "Cannot parse type {ty:?}. \
                    If you're trying to use this type in a table which is populated \
                    during a schema migration, consider adding it to `AnySqlType`."
                    ).into())
            }
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
}

// It's a little redunant to include the column name alongside each value,
// but it results in a prettier diff.
#[derive(PartialEq, Debug)]
pub struct ColumnValue {
    column: String,
    value: Option<AnySqlType>,
}

impl ColumnValue {
    /// Creates a new column with a non-null value of "value"
    pub fn new<V: Into<AnySqlType>>(column: &str, value: V) -> Self {
        Self { column: String::from(column), value: Some(value.into()) }
    }

    /// Creates a new column with a "NULL" value
    pub fn null(column: &str) -> Self {
        Self { column: String::from(column), value: None }
    }

    /// Returns the name of the column
    pub fn name(&self) -> &str {
        &self.column
    }

    /// Returns the value of the column
    pub fn value(&self) -> Option<&AnySqlType> {
        self.value.as_ref()
    }

    /// Returns the value of the column, asserting the name of the column
    pub fn expect(&self, column: &str) -> Option<&AnySqlType> {
        assert_eq!(self.column, column);
        self.value()
    }
}

/// A generic representation of a row of SQL data
#[derive(PartialEq, Debug)]
pub struct Row {
    pub values: Vec<ColumnValue>,
}

impl Row {
    pub fn new() -> Self {
        Self { values: vec![] }
    }
}

pub fn process_rows(rows: &Vec<tokio_postgres::Row>) -> Vec<Row> {
    let mut result = vec![];
    for row in rows {
        let mut row_result = Row::new();
        for i in 0..row.len() {
            let column_name = row.columns()[i].name();
            row_result.values.push(ColumnValue {
                column: column_name.to_string(),
                value: row.get(i),
            });
        }
        result.push(row_result);
    }
    result
}
