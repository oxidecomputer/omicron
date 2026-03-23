// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions for querying the timeseries database.
// Copyright 2024 Oxide Computer Company

use crate::{DATABASE_NAME, Error, FieldSchema, FieldSource, TimeseriesSchema};
use chrono::{DateTime, Utc};
use dropshot::PaginationOrder;
use oximeter::schema::TimeseriesKey;
use oximeter::types::{DatumType, FieldType, FieldValue};
use oximeter::{Metric, Target};
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

/// The `SelectQueryBuilder` is used to build queries that select timeseries by their names, field
/// values, and time range.
///
/// The query builder is created from a timeseries schema, at which point filters on the fields may
/// be added. The start and stop time may also be set, inclusively or exclusively. After adding all
/// the desired filtering criteria, the builder is consumed to generate a [`SelectQuery`], which is
/// used by the [`crate::Client`] to query the database for field values and measurements
/// matching those criteria.
#[derive(Debug, Clone)]
pub struct SelectQueryBuilder {
    timeseries_schema: TimeseriesSchema,
    field_selectors: BTreeMap<FieldSchema, FieldSelector>,
    time_range: TimeRange,
    limit: Option<NonZeroU32>,
    offset: Option<u32>,
    order: Option<PaginationOrder>,
}

impl SelectQueryBuilder {
    /// Construct a new builder for a timeseries with the given schema.
    pub fn new(schema: &TimeseriesSchema) -> Self {
        Self {
            timeseries_schema: schema.clone(),
            field_selectors: BTreeMap::new(),
            time_range: TimeRange { start: None, end: None },
            limit: None,
            offset: None,
            order: None,
        }
    }

    /// Set the start time for measurements selected from the query.
    pub fn start_time(mut self, start: Option<Timestamp>) -> Self {
        self.time_range.start = start;
        self
    }

    /// Set the end time for measurements selected from the query.
    pub fn end_time(mut self, end: Option<Timestamp>) -> Self {
        self.time_range.end = end;
        self
    }

    /// Set the limit on the number of returned results.
    pub fn limit(mut self, limit: NonZeroU32) -> Self {
        self.limit.replace(limit);
        self
    }

    /// Set the number of rows to skip in the result set.
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset.replace(offset);
        self
    }

    /// Set the order direction for the query
    pub fn order(mut self, order: PaginationOrder) -> Self {
        self.order.replace(order);
        self
    }

    /// Add a filter for a field with the given name, comparison operator, and value.
    ///
    /// An error is returned if the field cannot be found or the field value is not of the correct
    /// type for the field.
    pub fn filter<S, T>(
        mut self,
        field_name: S,
        op: FieldCmp,
        field_value: T,
    ) -> Result<Self, Error>
    where
        S: AsRef<str>,
        T: Into<FieldValue>,
    {
        let field_name = field_name.as_ref().to_string();
        let field_schema = self
            .timeseries_schema
            .schema_for_field(&field_name)
            .ok_or_else(|| Error::NoSuchField {
                timeseries_name: self
                    .timeseries_schema
                    .timeseries_name
                    .to_string(),
                field_name: field_name.clone(),
            })?;
        let field_value: FieldValue = field_value.into();
        let expected_type = field_schema.field_type;
        let found_type = field_value.field_type();
        if expected_type != found_type {
            return Err(Error::IncorrectFieldType {
                field_name: field_name.to_string(),
                expected_type,
                found_type,
            });
        }
        if !op.valid_for_type(found_type) {
            return Err(Error::InvalidFieldCmp {
                op: format!("{:?}", op),
                ty: found_type,
            });
        }
        let comparison = FieldComparison { op, value: field_value };
        let selector = FieldSelector {
            name: field_name.clone(),
            comparison: Some(comparison),
            ty: found_type,
        };
        self.field_selectors.insert(field_schema.clone(), selector);
        Ok(self)
    }

    /// Add a filter for a field by parsing the given string selector into a strongly typed field
    /// selector.
    ///
    /// Field selectors may be specified by a mini-DSL, where selectors are generally of the form:
    /// `NAME OP VALUE`. The name should be the name of the field. `OP` specifies the field
    /// comparison operator (see [`FieldCmp`] for details. `VALUE` should be a string that can be
    /// parsed into a `FieldValue` of the correct type for the given field.
    ///
    /// Whitespace surrounding the `OP` is ignored, but whitespace elsewhere is significant.
    pub fn filter_str(
        mut self,
        selector: &StringFieldSelector,
    ) -> Result<Self, Error> {
        let field_schema = self
            .timeseries_schema
            .schema_for_field(&selector.name)
            .ok_or_else(|| Error::NoSuchField {
                timeseries_name: self
                    .timeseries_schema
                    .timeseries_name
                    .to_string(),
                field_name: selector.name.clone(),
            })?;
        let field_type = field_schema.field_type;
        if !selector.op.valid_for_type(field_type) {
            return Err(Error::InvalidFieldCmp {
                op: format!("{:?}", selector.op),
                ty: field_schema.field_type,
            });
        }
        let field_value = match field_type {
            FieldType::String => FieldValue::from(&selector.value),
            FieldType::I8 => parse_selector_field_value::<i8>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::U8 => parse_selector_field_value::<u8>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::I16 => parse_selector_field_value::<i16>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::U16 => parse_selector_field_value::<u16>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::I32 => parse_selector_field_value::<i32>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::U32 => parse_selector_field_value::<u32>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::I64 => parse_selector_field_value::<i64>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::U64 => parse_selector_field_value::<u64>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::IpAddr => parse_selector_field_value::<IpAddr>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::Uuid => parse_selector_field_value::<Uuid>(
                &field_schema,
                &selector.value,
            )?,
            FieldType::Bool => parse_selector_field_value::<bool>(
                &field_schema,
                &selector.value,
            )?,
        };
        let comparison =
            FieldComparison { op: selector.op, value: field_value };
        let selector = FieldSelector {
            name: field_schema.name.to_string(),
            comparison: Some(comparison),
            ty: field_type,
        };
        self.field_selectors.insert(field_schema.clone(), selector);
        Ok(self)
    }

    /// Add a filter for a field by parsing the given string into a field selector.
    ///
    /// Field selectors may be specified by a mini-DSL, where selectors are generally of the form:
    /// `NAME OP VALUE`. The name should be the name of the field. `OP` specifies the field
    /// comparison operator (see [`FieldCmp`] for details. `VALUE` should be a string that can be
    /// parsed into a `FieldValue` of the correct type for the given field.
    ///
    /// Whitespace surrounding the `OP` is ignored, but whitespace elsewhere is significant.
    ///
    /// An error is returned if the field selector string is not formatted correctly, specifies an
    /// invalid field name or comparison operator, or a value that cannot be parsed into the right
    /// type for the given field.
    pub fn filter_raw<S>(self, selector: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
    {
        self.filter_str(&selector.as_ref().parse()?)
    }

    /// Create a `SelectQueryBuilder` that selects the exact timeseries indicated by the given
    /// target and metric.
    pub fn from_parts<T, M>(target: &T, metric: &M) -> Result<Self, Error>
    where
        T: Target,
        M: Metric,
    {
        let schema = TimeseriesSchema::new(target, metric)?;
        let mut builder = Self::new(&schema);
        let target_fields =
            target.field_names().iter().zip(target.field_values());
        let metric_fields =
            metric.field_names().iter().zip(metric.field_values());
        for (name, value) in target_fields.chain(metric_fields) {
            builder = builder.filter(name, FieldCmp::Eq, value)?;
        }
        Ok(builder)
    }

    /// Return the current field selector, if any, for the given field name and source.
    pub fn field_selector<S>(
        &self,
        source: FieldSource,
        name: S,
    ) -> Option<&FieldSelector>
    where
        S: AsRef<str>,
    {
        find_field_selector(&self.field_selectors, source, name)
    }

    /// Build a query that can be sent to the ClickHouse database from the given query.
    pub fn build(self) -> SelectQuery {
        let timeseries_schema = self.timeseries_schema;
        let mut field_selectors = self.field_selectors;
        for field in timeseries_schema.field_schema.iter() {
            let key = field.clone();
            field_selectors.entry(key).or_insert_with(|| FieldSelector {
                name: field.name.to_string(),
                comparison: None,
                ty: field.field_type,
            });
        }
        SelectQuery {
            timeseries_schema,
            field_selectors,
            time_range: self.time_range,
            limit: self.limit,
            offset: self.offset,
            order: self.order.unwrap_or(PaginationOrder::Ascending),
        }
    }
}

/// Return the name of the measurements table for a datum type.
pub(crate) fn measurement_table_name(ty: DatumType) -> String {
    let suffix = match ty {
        DatumType::Bool => "bool",
        DatumType::I8 => "i8",
        DatumType::U8 => "u8",
        DatumType::I16 => "i16",
        DatumType::U16 => "u16",
        DatumType::I32 => "i32",
        DatumType::U32 => "u32",
        DatumType::I64 => "i64",
        DatumType::U64 => "u64",
        DatumType::F32 => "f32",
        DatumType::F64 => "f64",
        DatumType::String => "string",
        DatumType::Bytes => "bytes",
        DatumType::CumulativeI64 => "cumulativei64",
        DatumType::CumulativeU64 => "cumulativeu64",
        DatumType::CumulativeF32 => "cumulativef32",
        DatumType::CumulativeF64 => "cumulativef64",
        DatumType::HistogramI8 => "histogrami8",
        DatumType::HistogramU8 => "histogramu8",
        DatumType::HistogramI16 => "histogrami16",
        DatumType::HistogramU16 => "histogramu16",
        DatumType::HistogramI32 => "histogrami32",
        DatumType::HistogramU32 => "histogramu32",
        DatumType::HistogramI64 => "histogrami64",
        DatumType::HistogramU64 => "histogramu64",
        DatumType::HistogramF32 => "histogramf32",
        DatumType::HistogramF64 => "histogramf64",
    };
    format!("measurements_{suffix}")
}

fn parse_selector_field_value<T>(
    field: &FieldSchema,
    s: &str,
) -> Result<FieldValue, Error>
where
    T: FromStr,
    FieldValue: From<T>,
{
    Ok(FieldValue::from(s.parse::<T>().map_err(|_| {
        Error::InvalidFieldValue {
            field_name: field.name.to_string(),
            field_type: field.field_type,
            value: s.to_string(),
        }
    })?))
}

/// A `FieldComparison` combines a comparison operation and field value.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldComparison {
    op: FieldCmp,
    value: FieldValue,
}

/// A strongly-typed selector for finding fields by name and comparsion with a given value.
///
/// If the comparison is `None`, then the selector will match any value of the corresponding field
/// name.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSelector {
    name: String,
    ty: FieldType,
    comparison: Option<FieldComparison>,
}

/// Return the name of the field table for the provided field type.
pub(crate) fn field_table_name(ty: FieldType) -> String {
    let suffix = match ty {
        FieldType::String => "string",
        FieldType::I8 => "i8",
        FieldType::U8 => "u8",
        FieldType::I16 => "i16",
        FieldType::U16 => "u16",
        FieldType::I32 => "i32",
        FieldType::U32 => "u32",
        FieldType::I64 => "i64",
        FieldType::U64 => "u64",
        FieldType::IpAddr => "ipaddr",
        FieldType::Uuid => "uuid",
        FieldType::Bool => "bool",
    };
    format!("fields_{suffix}")
}

impl FieldSelector {
    // Return a query selecting records of the field table where the field name and value match the
    // current criteria. The timeseries name is always included in the query.
    fn as_query(&self, timeseries_name: &str) -> String {
        let base_query = self.base_query(timeseries_name);
        if let Some(comparison) = &self.comparison {
            format!(
                "{base_query} AND field_value {op} {field_value}",
                base_query = base_query,
                op = comparison.op.as_db_str(),
                field_value = field_as_db_str(&comparison.value),
            )
        } else {
            base_query
        }
    }

    // Helper to generate the base query that selects from the right table and matches the
    // timeseries name and field name.
    fn base_query(&self, timeseries_name: &str) -> String {
        format!(
            "SELECT * FROM {db_name}.{table_name} WHERE timeseries_name = '{timeseries_name}' AND field_name = '{field_name}'",
            db_name = DATABASE_NAME,
            table_name = field_table_name(self.ty),
            timeseries_name = timeseries_name,
            field_name = self.name,
        )
    }
}

/// A stringly-typed selector for finding fields by name and comparsion with a
/// given value.
///
/// This is used internally to parse comparisons written as strings, such as
/// from the `oxdb` command-line tool or from another external
/// source (Nexus API, for example).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StringFieldSelector {
    name: String,
    op: FieldCmp,
    value: String,
}

impl FromStr for StringFieldSelector {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = Regex::new("\\s*(==|!=|>|>=|<|<=|~=)\\s*").unwrap();
        if let Some(match_) = re.find(s) {
            let name = &s[..match_.start()];
            let op = match_.as_str().parse()?;
            let value = &s[match_.end()..];
            if name.is_empty() || value.is_empty() {
                Err(Error::InvalidFieldSelectorString {
                    selector: s.to_string(),
                })
            } else {
                Ok(StringFieldSelector {
                    name: name.to_string(),
                    op,
                    value: value.to_string(),
                })
            }
        } else {
            Err(Error::InvalidFieldSelectorString { selector: s.to_string() })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum FieldCmp {
    Eq,
    Neq,
    Gt,
    Ge,
    Lt,
    Le,
    Like,
}

impl FromStr for FieldCmp {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "==" => Ok(FieldCmp::Eq),
            "!=" => Ok(FieldCmp::Neq),
            ">" => Ok(FieldCmp::Gt),
            ">=" => Ok(FieldCmp::Ge),
            "<" => Ok(FieldCmp::Lt),
            "<=" => Ok(FieldCmp::Le),
            "~=" => Ok(FieldCmp::Like),
            _ => Err(Error::UnknownFieldComparison),
        }
    }
}

impl FieldCmp {
    // Return `true` if the given comparison may be applied to fields of the given type.
    //
    // All fields may use `Eq` or `Neq`. Strings can use `Like`. All fields by booleans and IP
    // addresses can use the remaining comparisons (orderings).
    fn valid_for_type(&self, ty: FieldType) -> bool {
        match self {
            FieldCmp::Eq | FieldCmp::Neq => true,
            FieldCmp::Like => matches!(ty, FieldType::String),
            _ => !matches!(ty, FieldType::Bool | FieldType::IpAddr),
        }
    }

    // Return the representation of the comparison that is used in SQL, for writing in a query to
    // the database.
    fn as_db_str(&self) -> String {
        match self {
            FieldCmp::Eq => String::from("="),
            FieldCmp::Like => String::from("LIKE"),
            other => format!("{}", other),
        }
    }
}

impl fmt::Display for FieldCmp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FieldCmp::Eq => write!(f, "=="),
            FieldCmp::Neq => write!(f, "!="),
            FieldCmp::Gt => write!(f, ">"),
            FieldCmp::Ge => write!(f, ">="),
            FieldCmp::Lt => write!(f, "<"),
            FieldCmp::Le => write!(f, "<="),
            FieldCmp::Like => write!(f, "~="),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
}

impl TimeRange {
    fn as_query(&self) -> String {
        let format = |direction: &str, timestamp: Timestamp| {
            let (eq, t) = match timestamp {
                Timestamp::Inclusive(ts) => {
                    ("=", ts.format(crate::DATABASE_TIMESTAMP_FORMAT))
                }
                Timestamp::Exclusive(ts) => {
                    ("", ts.format(crate::DATABASE_TIMESTAMP_FORMAT))
                }
            };
            format!("timestamp {}{} '{}'", direction, eq, t)
        };
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                format!(" AND {} AND {} ", format(">", start), format("<", end))
            }
            (Some(start), None) => {
                format!(" AND {} ", format(">", start))
            }
            (None, Some(end)) => {
                format!(" AND {} ", format("<", end))
            }
            (None, None) => String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Timestamp {
    Inclusive(DateTime<Utc>),
    Exclusive(DateTime<Utc>),
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    timeseries_schema: TimeseriesSchema,
    field_selectors: BTreeMap<FieldSchema, FieldSelector>,
    time_range: TimeRange,
    limit: Option<NonZeroU32>,
    offset: Option<u32>,
    order: PaginationOrder,
}

fn create_join_on_condition(columns: &[&str], current: usize) -> String {
    columns
        .iter()
        .map(|column| {
            format!(
                "filter{j}.{column} = filter{i}.{column}",
                column = column,
                j = current - 1,
                i = current,
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn find_field_selector<S>(
    field_selectors: &BTreeMap<FieldSchema, FieldSelector>,
    source: FieldSource,
    name: S,
) -> Option<&FieldSelector>
where
    S: AsRef<str>,
{
    let name = name.as_ref();
    field_selectors
        .iter()
        .find(|(field, _)| field.source == source && field.name == name)
        .map(|(_, selector)| selector)
}

impl SelectQuery {
    pub fn schema(&self) -> &TimeseriesSchema {
        &self.timeseries_schema
    }

    pub fn field_selector<S>(
        &self,
        source: FieldSource,
        name: S,
    ) -> Option<&FieldSelector>
    where
        S: AsRef<str>,
    {
        find_field_selector(&self.field_selectors, source, name)
    }

    /// Construct and return the query used to select the matching field records from the database.
    ///
    /// If there are no fields in the associated timeseries, None is returned.
    pub fn field_query(&self) -> Option<String> {
        match self.field_selectors.len() {
            0 => None,
            n => {
                // Select timeseries key for first column, plus the field value
                // for all columns, aliased to the field name.
                const JOIN_COLUMNS: &[&str] =
                    &["timeseries_name", "timeseries_key"];
                let mut top_level_columns = Vec::with_capacity(2 + n);
                top_level_columns.push(String::from(
                    "filter0.timeseries_key as timeseries_key",
                ));
                let mut from_statements = String::new();
                for (i, (field_name, subquery)) in self
                    .field_selectors
                    .iter()
                    .map(|(field_schema, selector)| {
                        (
                            &field_schema.name,
                            selector.as_query(
                                &self.timeseries_schema.timeseries_name,
                            ),
                        )
                    })
                    .enumerate()
                {
                    top_level_columns.push(format!(
                        "filter{}.field_value AS {}",
                        i, field_name,
                    ));

                    if i == 0 {
                        from_statements.push_str(&format!(
                            "({subquery}) AS filter{i} ",
                            subquery = subquery,
                            i = i
                        ));
                    } else {
                        from_statements.push_str(
                            &format!(
                                "INNER JOIN ({subquery}) AS filter{i} ON ({join_on}) ",
                                subquery = subquery,
                                join_on = create_join_on_condition(&JOIN_COLUMNS, i),
                                i = i,
                        ));
                    }
                }
                let query = format!(
                    concat!(
                        "SELECT {top_level_columns} ",
                        "FROM {from_statements}",
                        "ORDER BY (filter0.timeseries_name, filter0.timeseries_key)",
                    ),
                    top_level_columns = top_level_columns.join(", "),
                    from_statements = from_statements,
                );
                Some(query)
            }
        }
    }

    /// Construct and return the query used to select the measurements, using the associated
    /// timeseries keys. If no keys are specified, then a query selecting the all timeseries with
    /// the given name will be returned. (This is probably not what you want.)
    pub fn measurement_query(&self, keys: &[TimeseriesKey]) -> String {
        let key_clause = if keys.is_empty() {
            String::from(" ")
        } else {
            format!(
                " AND timeseries_key IN ({timeseries_keys}) ",
                timeseries_keys = keys
                    .iter()
                    .map(|key| key.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        };
        let pagination_clause = {
            let mut clause = String::new();
            if let Some(limit) = self.limit {
                clause.push_str(&format!("LIMIT {} ", limit));
            }
            if let Some(offset) = self.offset {
                clause.push_str(&format!("OFFSET {} ", offset));
            };
            clause
        };
        let order_dir = match self.order {
            PaginationOrder::Descending => "DESC ",
            PaginationOrder::Ascending => "",
        };
        format!(
            concat!(
                "SELECT * ",
                "FROM {db_name}.{table_name} ",
                "WHERE ",
                "timeseries_name = '{timeseries_name}'",
                "{key_clause}",
                "{timestamp_clause}",
                "ORDER BY (timeseries_name, timeseries_key, timestamp) {order_dir}",
                "{pagination_clause}",
            ),
            db_name = DATABASE_NAME,
            table_name =
                measurement_table_name(self.timeseries_schema.datum_type),
            timeseries_name = self.timeseries_schema.timeseries_name,
            key_clause = key_clause,
            timestamp_clause = self.time_range.as_query(),
            pagination_clause = pagination_clause,
            order_dir = order_dir,
        )
    }
}

// Format the value for use in a query to the database, e.g., `... WHERE field_value = {}`.
fn field_as_db_str(value: &FieldValue) -> String {
    match value {
        FieldValue::Bool(inner) => {
            format!("{}", if *inner { 1 } else { 0 })
        }
        FieldValue::I8(inner) => format!("{}", inner),
        FieldValue::U8(inner) => format!("{}", inner),
        FieldValue::I16(inner) => format!("{}", inner),
        FieldValue::U16(inner) => format!("{}", inner),
        FieldValue::I32(inner) => format!("{}", inner),
        FieldValue::U32(inner) => format!("{}", inner),
        FieldValue::I64(inner) => format!("{}", inner),
        FieldValue::U64(inner) => format!("{}", inner),
        FieldValue::IpAddr(inner) => {
            let addr = match inner {
                IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                IpAddr::V6(v6) => *v6,
            };
            format!("'{}'", addr)
        }
        FieldValue::String(inner) => format!("'{}'", inner),
        FieldValue::Uuid(inner) => format!("'{}'", inner),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldSchema;
    use crate::FieldSource;
    use crate::TimeseriesName;
    use chrono::NaiveDateTime;
    use std::collections::BTreeSet;
    use std::convert::TryFrom;

    #[test]
    fn test_field_value_as_db_str() {
        assert_eq!(field_as_db_str(&FieldValue::from(false)), "0");
        assert_eq!(field_as_db_str(&FieldValue::from(true)), "1");
        assert_eq!(field_as_db_str(&FieldValue::from(10i64)), "10");
        assert_eq!(
            field_as_db_str(&FieldValue::IpAddr("127.0.0.1".parse().unwrap())),
            "'::ffff:127.0.0.1'"
        );
        assert_eq!(
            field_as_db_str(&FieldValue::Uuid(
                "563f0076-2c22-4510-8fd9-bed1ed8c9ae1".parse().unwrap()
            )),
            "'563f0076-2c22-4510-8fd9-bed1ed8c9ae1'"
        );
    }

    #[test]
    fn test_string_field_selector() {
        assert_eq!(
            StringFieldSelector {
                name: "foo".to_string(),
                op: FieldCmp::Eq,
                value: "bar".to_string(),
            },
            "foo==bar".parse().unwrap(),
        );

        assert_eq!(
            StringFieldSelector {
                name: "foo".to_string(),
                op: FieldCmp::Neq,
                value: "bar".to_string(),
            },
            "foo!=bar".parse().unwrap(),
        );

        assert_eq!(
            StringFieldSelector {
                name: "foo".to_string(),
                op: FieldCmp::Like,
                value: "bar".to_string(),
            },
            "foo~=bar".parse().unwrap(),
        );
    }

    #[test]
    fn test_select_query_builder_filter_raw() {
        let schema = TimeseriesSchema {
            timeseries_name: TimeseriesName::try_from("foo:bar").unwrap(),
            description: Default::default(),
            version: oximeter::schema::default_schema_version(),
            authz_scope: oximeter::schema::AuthzScope::Fleet,
            units: oximeter::schema::Units::Count,
            field_schema: [
                FieldSchema {
                    name: "f0".to_string(),
                    field_type: FieldType::I64,
                    source: FieldSource::Target,
                    description: String::new(),
                },
                FieldSchema {
                    name: "f1".to_string(),
                    field_type: FieldType::Bool,
                    source: FieldSource::Target,
                    description: String::new(),
                },
            ]
            .into_iter()
            .collect(),
            datum_type: DatumType::I64,
            created: Utc::now(),
        };
        let builder = SelectQueryBuilder::new(&schema)
            .filter_raw("f0!=2")
            .expect("Failed to add field filter from string");
        assert_eq!(builder.field_selectors.len(), 1);
        assert_eq!(
            builder.field_selector(FieldSource::Target, "f0").unwrap(),
            &FieldSelector {
                name: "f0".to_string(),
                comparison: Some(FieldComparison {
                    op: FieldCmp::Neq,
                    value: FieldValue::I64(2)
                }),
                ty: FieldType::I64,
            }
        );

        let builder = builder
            .filter_raw("f1==true")
            .expect("Failed to add field filter from string");
        assert_eq!(builder.field_selectors.len(), 2);
        assert_eq!(
            builder.field_selector(FieldSource::Target, "f1").unwrap(),
            &FieldSelector {
                name: "f1".to_string(),
                comparison: Some(FieldComparison {
                    op: FieldCmp::Eq,
                    value: FieldValue::Bool(true)
                }),
                ty: FieldType::Bool,
            }
        );

        builder.clone().filter_raw("f1>true").expect_err("Expected error adding field comparison which isn't valid for the type");

        builder.filter_raw("a=0").expect_err(
            "Expected to fail adding a selector for an unknown field",
        );
    }

    #[test]
    fn test_field_cmp() {
        let cases = &[
            (FieldCmp::Eq, "==", "="),
            (FieldCmp::Neq, "!=", "!="),
            (FieldCmp::Gt, ">", ">"),
            (FieldCmp::Ge, ">=", ">="),
            (FieldCmp::Lt, "<", "<"),
            (FieldCmp::Le, "<=", "<="),
            (FieldCmp::Like, "~=", "LIKE"),
        ];
        for (cmp, from_str, db_str) in cases.iter() {
            assert_eq!(
                cmp,
                &from_str.parse().unwrap(),
                "FromStr implementation failed"
            );
            assert_eq!(
                cmp.as_db_str(),
                *db_str,
                "as_db_str implementation failed"
            );
        }
    }

    #[test]
    fn test_field_selector_as_query() {
        let mut selector = FieldSelector {
            name: "foo".to_string(),
            comparison: Some(FieldComparison {
                op: FieldCmp::Neq,
                value: FieldValue::from(100),
            }),
            ty: FieldType::I64,
        };
        assert_eq!(
            selector.as_query("target:metric"),
            "SELECT * FROM oximeter.fields_i64 WHERE timeseries_name = 'target:metric' AND field_name = 'foo' AND field_value != 100"
        );

        selector.comparison.as_mut().unwrap().op = FieldCmp::Le;
        assert_eq!(
            selector.as_query("target:metric"),
            "SELECT * FROM oximeter.fields_i64 WHERE timeseries_name = 'target:metric' AND field_name = 'foo' AND field_value <= 100"
        );
    }

    #[test]
    fn test_time_range() {
        let s = "2021-01-01 01:01:01.123456789";
        let start_time =
            NaiveDateTime::parse_from_str(s, crate::DATABASE_TIMESTAMP_FORMAT)
                .unwrap()
                .and_utc();
        let e = "2021-01-01 01:01:02.123456789";
        let end_time =
            NaiveDateTime::parse_from_str(e, crate::DATABASE_TIMESTAMP_FORMAT)
                .unwrap()
                .and_utc();
        let range = TimeRange {
            start: Some(Timestamp::Inclusive(start_time)),
            end: Some(Timestamp::Exclusive(end_time)),
        };
        assert_eq!(
            range.as_query(),
            format!(
                " AND timestamp >= '{s}' AND timestamp < '{e}' ",
                s = s,
                e = e
            )
        );
    }

    #[test]
    fn test_select_query_builder_no_fields() {
        let schema = TimeseriesSchema {
            timeseries_name: TimeseriesName::try_from("foo:bar").unwrap(),
            description: Default::default(),
            version: oximeter::schema::default_schema_version(),
            authz_scope: oximeter::schema::AuthzScope::Fleet,
            units: oximeter::schema::Units::Count,
            field_schema: BTreeSet::new(),
            datum_type: DatumType::I64,
            created: Utc::now(),
        };
        let query = SelectQueryBuilder::new(&schema).build();
        assert!(query.field_query().is_none());
        assert_eq!(
            query.measurement_query(&[]).trim(),
            concat!(
                "SELECT * ",
                "FROM oximeter.measurements_i64 ",
                "WHERE timeseries_name = 'foo:bar' ",
                "ORDER BY (timeseries_name, timeseries_key, timestamp)",
            )
        );
    }

    #[test]
    fn test_select_query_builder_limit_offset() {
        let schema = TimeseriesSchema {
            timeseries_name: TimeseriesName::try_from("foo:bar").unwrap(),
            description: Default::default(),
            version: oximeter::schema::default_schema_version(),
            authz_scope: oximeter::schema::AuthzScope::Fleet,
            units: oximeter::schema::Units::Count,
            field_schema: BTreeSet::new(),
            datum_type: DatumType::I64,
            created: Utc::now(),
        };
        let query = SelectQueryBuilder::new(&schema)
            .limit(NonZeroU32::try_from(10).unwrap())
            .offset(5)
            .build();
        assert!(query.field_query().is_none());
        assert_eq!(
            query.measurement_query(&[]).trim(),
            concat!(
                "SELECT * ",
                "FROM oximeter.measurements_i64 ",
                "WHERE timeseries_name = 'foo:bar' ",
                "ORDER BY (timeseries_name, timeseries_key, timestamp) ",
                "LIMIT 10 OFFSET 5",
            )
        );
    }

    #[test]
    fn test_select_query_builder_from_parts() {
        #[derive(oximeter::Target)]
        struct Targ {
            foo: String,
        }

        #[derive(oximeter::Metric)]
        struct Met {
            baz: i64,
            datum: f64,
        }
        let targ = Targ { foo: String::from("bar") };
        let met = Met { baz: 0, datum: 0.0 };
        let builder = SelectQueryBuilder::from_parts(&targ, &met).unwrap();

        assert_eq!(
            builder.field_selector(FieldSource::Target, "foo").unwrap(),
            &FieldSelector {
                name: String::from("foo"),
                ty: FieldType::String,
                comparison: Some(FieldComparison {
                    op: FieldCmp::Eq,
                    value: FieldValue::from("bar"),
                }),
            },
            "Expected an exact comparison when building a query from parts",
        );

        println!("{builder:#?}");
        assert_eq!(
            builder.field_selector(FieldSource::Metric, "baz").unwrap(),
            &FieldSelector {
                name: String::from("baz"),
                ty: FieldType::I64,
                comparison: Some(FieldComparison {
                    op: FieldCmp::Eq,
                    value: FieldValue::from(0i64),
                }),
            },
            "Expected an exact comparison when building a query from parts",
        );
    }

    #[test]
    fn test_select_query_builder_no_selectors() {
        let schema = TimeseriesSchema {
            timeseries_name: TimeseriesName::try_from("foo:bar").unwrap(),
            description: Default::default(),
            version: oximeter::schema::default_schema_version(),
            authz_scope: oximeter::schema::AuthzScope::Fleet,
            units: oximeter::schema::Units::Count,
            field_schema: [
                FieldSchema {
                    name: "f0".to_string(),
                    field_type: FieldType::I64,
                    source: FieldSource::Target,
                    description: String::new(),
                },
                FieldSchema {
                    name: "f1".to_string(),
                    field_type: FieldType::Bool,
                    source: FieldSource::Target,
                    description: String::new(),
                },
            ]
            .into_iter()
            .collect(),
            datum_type: DatumType::I64,
            created: Utc::now(),
        };

        let query = SelectQueryBuilder::new(&schema).build();
        let field_query = query.field_query().unwrap();
        assert_eq!(
            field_query,
            concat!(
                "SELECT ",
                "filter0.timeseries_key as timeseries_key, ",
                "filter0.field_value AS f0, ",
                "filter1.field_value AS f1 ",
                "FROM (",
                "SELECT * FROM oximeter.fields_i64 ",
                "WHERE timeseries_name = 'foo:bar' ",
                "AND field_name = 'f0'",
                ") AS filter0 ",
                "INNER JOIN (",
                "SELECT * FROM oximeter.fields_bool ",
                "WHERE timeseries_name = 'foo:bar' ",
                "AND field_name = 'f1'",
                ") AS filter1 ON (",
                "filter0.timeseries_name = filter1.timeseries_name AND ",
                "filter0.timeseries_key = filter1.timeseries_key) ",
                "ORDER BY (filter0.timeseries_name, filter0.timeseries_key)",
            )
        );

        let keys = &[0, 1];
        let measurement_query = query.measurement_query(keys);
        assert_eq!(
            measurement_query.trim(),
            concat!(
                "SELECT * ",
                "FROM oximeter.measurements_i64 ",
                "WHERE timeseries_name = 'foo:bar' AND ",
                "timeseries_key IN (0, 1) ",
                "ORDER BY (timeseries_name, timeseries_key, timestamp)",
            )
        );
    }

    #[test]
    fn test_select_query_builder_field_selectors() {
        let schema = TimeseriesSchema {
            timeseries_name: TimeseriesName::try_from("foo:bar").unwrap(),
            description: Default::default(),
            version: oximeter::schema::default_schema_version(),
            authz_scope: oximeter::schema::AuthzScope::Fleet,
            units: oximeter::schema::Units::Count,
            field_schema: [
                FieldSchema {
                    name: "f0".to_string(),
                    field_type: FieldType::I64,
                    source: FieldSource::Target,
                    description: String::new(),
                },
                FieldSchema {
                    name: "f1".to_string(),
                    field_type: FieldType::Bool,
                    source: FieldSource::Target,
                    description: String::new(),
                },
            ]
            .into_iter()
            .collect(),
            datum_type: DatumType::I64,
            created: Utc::now(),
        };

        let query = SelectQueryBuilder::new(&schema)
            .filter_raw("f0==0")
            .expect("Failed to add first filter")
            .filter_raw("f1==false")
            .expect("Failed to add second filter")
            .build();
        assert_eq!(
            query.field_query().unwrap(),
            concat!(
                "SELECT ",
                "filter0.timeseries_key as timeseries_key, ",
                "filter0.field_value AS f0, ",
                "filter1.field_value AS f1 ",
                "FROM (",
                "SELECT * FROM oximeter.fields_i64 ",
                "WHERE timeseries_name = 'foo:bar' AND field_name = 'f0' AND field_value = 0",
                ") AS filter0 ",
                "INNER JOIN (",
                "SELECT * FROM oximeter.fields_bool ",
                "WHERE timeseries_name = 'foo:bar' AND field_name = 'f1' AND field_value = 0",
                ") AS filter1 ON (",
                "filter0.timeseries_name = filter1.timeseries_name AND ",
                "filter0.timeseries_key = filter1.timeseries_key) ",
                "ORDER BY (filter0.timeseries_name, filter0.timeseries_key)",
            )
        );
    }

    #[test]
    fn test_select_query_builder_full() {
        let schema = TimeseriesSchema {
            timeseries_name: TimeseriesName::try_from("foo:bar").unwrap(),
            description: Default::default(),
            version: oximeter::schema::default_schema_version(),
            authz_scope: oximeter::schema::AuthzScope::Fleet,
            units: oximeter::schema::Units::Count,
            field_schema: [
                FieldSchema {
                    name: "f0".to_string(),
                    field_type: FieldType::I64,
                    source: FieldSource::Target,
                    description: String::new(),
                },
                FieldSchema {
                    name: "f1".to_string(),
                    field_type: FieldType::Bool,
                    source: FieldSource::Target,
                    description: String::new(),
                },
            ]
            .into_iter()
            .collect(),
            datum_type: DatumType::I64,
            created: Utc::now(),
        };

        let start_time = Utc::now();
        let end_time = start_time + chrono::Duration::seconds(1);

        let query = SelectQueryBuilder::new(&schema)
            .filter_raw("f0==0")
            .expect("Failed to add first filter")
            .filter_raw("f1==false")
            .expect("Failed to add second filter")
            .start_time(Some(Timestamp::Inclusive(start_time)))
            .end_time(Some(Timestamp::Exclusive(end_time)))
            .limit(NonZeroU32::try_from(10).unwrap())
            .offset(5)
            .build();
        assert_eq!(
            query.field_query().unwrap(),
            concat!(
                "SELECT filter0.timeseries_key as timeseries_key, ",
                "filter0.field_value AS f0, ",
                "filter1.field_value AS f1 ",
                "FROM (",
                "SELECT * FROM oximeter.fields_i64 ",
                "WHERE timeseries_name = 'foo:bar' AND field_name = 'f0' AND field_value = 0",
                ") AS filter0 ",
                "INNER JOIN (",
                "SELECT * FROM oximeter.fields_bool ",
                "WHERE timeseries_name = 'foo:bar' AND field_name = 'f1' AND field_value = 0",
                ") AS filter1 ON (",
                "filter0.timeseries_name = filter1.timeseries_name AND ",
                "filter0.timeseries_key = filter1.timeseries_key) ",
                "ORDER BY (filter0.timeseries_name, filter0.timeseries_key)",
            )
        );
        let keys = &[0, 1];
        assert_eq!(
            query.measurement_query(keys).trim(),
            format!(
                concat!(
                    "SELECT * ",
                    "FROM oximeter.measurements_i64 ",
                    "WHERE timeseries_name = 'foo:bar' ",
                    "AND timeseries_key IN (0, 1) ",
                    " AND timestamp >= '{start_time}' ",
                    "AND timestamp < '{end_time}' ",
                    "ORDER BY (timeseries_name, timeseries_key, timestamp) ",
                    "LIMIT 10 OFFSET 5",
                ),
                start_time =
                    start_time.format(crate::DATABASE_TIMESTAMP_FORMAT),
                end_time = end_time.format(crate::DATABASE_TIMESTAMP_FORMAT),
            )
        );
    }
}
