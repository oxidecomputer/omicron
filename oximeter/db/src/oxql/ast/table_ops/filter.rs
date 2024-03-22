// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing filtering table operations.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::cmp::Comparison;
use crate::oxql::ast::ident::Ident;
use crate::oxql::ast::literal::Literal;
use crate::oxql::ast::logical_op::LogicalOp;
use crate::oxql::point::Points;
use crate::oxql::point::ValueArray;
use crate::oxql::Error;
use crate::oxql::Table;
use crate::oxql::Timeseries;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use oximeter::FieldType;
use oximeter::FieldValue;
use regex::Regex;
use std::collections::BTreeSet;
use std::fmt;

/// An AST node for the `filter` table operation.
///
/// This can be a simple operation like `foo == "bar"` or a more complex
/// expression, such as: `filter hostname == "foo" || (hostname == "bar"
/// && id == "baz")`.
#[derive(Clone, Debug, PartialEq)]
pub enum Filter {
    /// An individual filter atom, like `foo == "bar"`.
    Atom(FilterAtom),
    /// A filtering expression combining multiple filters.
    Expr(FilterExpr),
}

impl Filter {
    // Merge this filter with another one.
    pub(crate) fn merge(&self, other: &Filter) -> Self {
        Filter::Expr(FilterExpr {
            negated: false,
            left: Box::new(self.clone()),
            op: LogicalOp::And,
            right: Box::new(other.clone()),
        })
    }

    // Apply the filter to the provided field.
    //
    // This returns `Ok(None)` if the filter doesn't apply. It returns `Ok(x)`
    // if the filter does apply, where `x` is the logical application of the
    // filter to the field.
    //
    // If the filter does apply, but is incompatible or incomparable, return an
    // error.
    fn filter_field(
        &self,
        name: &str,
        value: &FieldValue,
    ) -> Result<Option<bool>, Error> {
        match self {
            Filter::Atom(atom) => atom.filter_field(name, value),
            Filter::Expr(expr) => expr.filter_field(name, value),
        }
    }

    // Apply the filter to the provided points.
    fn filter_points(&self, points: &Points) -> Result<Points, Error> {
        let to_keep = self.filter_points_inner(points)?;
        points.filter(to_keep)
    }

    // Inner implementation of filtering points.
    //
    // Returns an array of bools, where true indicates the point should be kept.
    fn filter_points_inner(&self, points: &Points) -> Result<Vec<bool>, Error> {
        match self {
            Filter::Atom(atom) => atom.filter_points(points),
            Filter::Expr(expr) => expr.filter_points(points),
        }
    }

    // Apply the filtering table operation.
    pub(crate) fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        anyhow::ensure!(
            tables.len() >= 1,
            "Filtering operations require at least one table",
        );
        let mut output_tables = Vec::with_capacity(tables.len());
        // Ensure that all the identifiers in this filter apply to the
        // input timeseries. We can do this once at the beginning, because all
        // the timeseries in a table have the same set of fields.
        let first_timeseries = tables[0]
            .iter()
            .next()
            .context("Table contains no timeseries to filter")?;
        let ident_names = self.ident_names();
        let not_valid = ident_names
            .iter()
            .filter(|&&name| {
                !first_timeseries.fields.contains_key(name)
                    && !matches!(
                        name,
                        "timestamp"
                            | "start_time"
                            | "datum"
                            | "bins"
                            | "counts"
                    )
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            not_valid.is_empty(),
            "The filter expression contains identifiers that are not \
            valid for its input timeseries. Invalid identifiers: {:?}, \
            timeseries fields: {:?}",
            not_valid,
            first_timeseries.fields.keys().collect::<Vec<_>>(),
        );

        // Filter each input table in succession.
        for table in tables.iter() {
            let mut timeseries = Vec::with_capacity(table.len());
            'timeseries: for input in table.iter() {
                // If the filter restricts any of the fields, remove this
                // timeseries altogether.
                for (name, value) in input.fields.iter() {
                    if let Some(false) = self.filter_field(name, value)? {
                        continue 'timeseries;
                    }
                }

                // Apply the filter to the data points as well.
                let points = self.filter_points(&input.points)?;

                // Similar to above, if the filter removes all data points in
                // the timeseries, let's remove the timeseries altogether.
                if points.is_empty() {
                    continue;
                }
                timeseries.push(Timeseries {
                    fields: input.fields.clone(),
                    points,
                    alignment: input.alignment,
                })
            }
            output_tables.push(Table::from_timeseries(
                table.name(),
                timeseries.into_iter(),
            )?);
        }
        Ok(output_tables)
    }

    // Return the last referenced timestamp by this filter, if any.
    //
    // This is the maximum timestamp, before which any filtered point must lie.
    // This is used to determine the query end time.
    pub(crate) fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Filter::Atom(atom) => atom.last_timestamp(),
            Filter::Expr(expr) => expr.last_timestamp(),
        }
    }

    // Return the name of all identifiers listed in this filter.
    fn ident_names(&self) -> BTreeSet<&str> {
        match self {
            Filter::Atom(atom) => {
                let mut out = BTreeSet::new();
                out.insert(atom.ident.as_str());
                out
            }
            Filter::Expr(expr) => {
                let mut all = expr.left.ident_names();
                all.extend(expr.right.ident_names());
                all
            }
        }
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter::Atom(inner) => write!(f, "{}", inner),
            Filter::Expr(inner) => write!(f, "{}", inner),
        }
    }
}

/// A more complicated expression as part of a filtering operation.
///
/// E.g., the `hostname == "bar" && id == "baz"` in the below.
// NOTE: This should really be extended to a generic binary op expression.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterExpr {
    pub negated: bool,
    pub left: Box<Filter>,
    pub op: LogicalOp,
    pub right: Box<Filter>,
}

impl FilterExpr {
    // Apply the filter to the provided field.
    fn filter_field(
        &self,
        name: &str,
        value: &FieldValue,
    ) -> Result<Option<bool>, Error> {
        let left = self.left.filter_field(name, value)?;
        let right = self.right.filter_field(name, value)?;
        match (left, right) {
            (None, None) => Ok(None),
            (Some(x), None) | (None, Some(x)) => Ok(Some(x)),
            (Some(left), Some(right)) => match self.op {
                LogicalOp::And => Ok(Some(left && right)),
                LogicalOp::Or => Ok(Some(left || right)),
                LogicalOp::Xor => Ok(Some(left ^ right)),
            },
        }
    }

    // Apply the filter to the provided points.
    fn filter_points(&self, points: &Points) -> Result<Vec<bool>, Error> {
        let mut left = self.left.filter_points_inner(points)?;
        let right = self.right.filter_points_inner(points)?;
        match self.op {
            LogicalOp::And => {
                for i in 0..left.len() {
                    left[i] &= right[i];
                }
                Ok(left)
            }
            LogicalOp::Or => {
                for i in 0..left.len() {
                    left[i] |= right[i];
                }
                Ok(left)
            }
            LogicalOp::Xor => {
                for i in 0..left.len() {
                    left[i] ^= right[i];
                }
                Ok(left)
            }
        }
    }

    fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        let left = self.left.last_timestamp();
        let right = self.right.last_timestamp();
        match (left, right) {
            (None, None) => None,
            (Some(single), None) | (None, Some(single)) => Some(single),
            (Some(left), Some(right)) => Some(left.max(right)),
        }
    }
}

impl fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({} {} {})",
            if self.negated { "!" } else { "" },
            self.left,
            self.op,
            self.right
        )
    }
}

/// An atom of a filtering expression.
///
/// E.g, the `hostname == "foo"` in the below.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterAtom {
    pub negated: bool,
    pub ident: Ident,
    pub cmp: Comparison,
    pub expr: Literal,
}

impl FilterAtom {
    // Apply this filter to the provided field.
    //
    // If the field name does not match the identifier in `self`, return
    // `Ok(None)`, since this filter does not apply to the provided field.
    //
    // If the name matches and the type of `self` is compatible, return `Ok(x)`
    // where `x` is the logical application of the filter to the field.
    //
    // If the field matches the name, but the type is not compatible, return an
    // error.
    fn filter_field(
        &self,
        name: &str,
        value: &FieldValue,
    ) -> Result<Option<bool>, Error> {
        // If the name matches, this filter does _not_ apply, and so we do not
        // filter the field.
        if self.ident.as_str() != name {
            return Ok(None);
        }
        self.expr
            .compare_field(value, self.cmp)
            .map(|res| {
                // `!expr` has the same truth table as `negated XOR expr`
                Some(self.negated ^ res)
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Filter matches the field named '{}', but \
                    the expression type  is not compatible, or \
                    cannot be applied",
                    name,
                )
            })
    }

    pub(crate) fn expr_type_is_compatible_with_field(
        &self,
        field_type: FieldType,
    ) -> bool {
        self.expr.is_compatible_with_field(field_type)
    }

    pub(crate) fn as_db_safe_string(&self) -> String {
        let not = if self.negated { " NOT " } else { "" };
        let expr = self.expr.as_db_safe_string();
        let fn_name = self.cmp.as_db_function_name();
        format!("{}{}({}, {})", not, fn_name, self.ident, expr)
    }

    // Returns an array of bools, where true indicates the point should be kept.
    fn filter_points(&self, points: &Points) -> Result<Vec<bool>, Error> {
        let ident = self.ident.as_str();
        if ident == "timestamp" {
            self.filter_points_by_timestamp(&points.timestamps)
        } else if ident == "datum" {
            anyhow::ensure!(
                points.dimensionality() == 1,
                "Filtering multidimensional values by datum is not yet supported"
            );
            self.filter_points_by_datum(points.values(0).unwrap())
        } else {
            Ok(vec![true; points.len()])
        }
    }

    fn filter_points_by_timestamp(
        &self,
        timestamps: &[DateTime<Utc>],
    ) -> Result<Vec<bool>, Error> {
        let Literal::Timestamp(timestamp) = &self.expr else {
            anyhow::bail!(
                "Cannot compare non-timestamp filter against a timestamp"
            );
        };
        match self.cmp {
            Comparison::Eq => {
                Ok(timestamps.iter().map(|t| t == timestamp).collect())
            }
            Comparison::Ne => {
                Ok(timestamps.iter().map(|t| t != timestamp).collect())
            }
            Comparison::Gt => {
                Ok(timestamps.iter().map(|t| t > timestamp).collect())
            }
            Comparison::Ge => {
                Ok(timestamps.iter().map(|t| t >= timestamp).collect())
            }
            Comparison::Lt => {
                Ok(timestamps.iter().map(|t| t < timestamp).collect())
            }
            Comparison::Le => {
                Ok(timestamps.iter().map(|t| t <= timestamp).collect())
            }
            Comparison::Like => unreachable!(),
        }
    }

    fn filter_points_by_datum(
        &self,
        values: &ValueArray,
    ) -> Result<Vec<bool>, Error> {
        match (&self.expr, values) {
            (Literal::Integer(int), ValueArray::Integer(ints)) => {
                match self.cmp {
                    Comparison::Eq => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| i128::from(i) == *int)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| i128::from(i) != *int)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| i128::from(i) > *int)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| i128::from(i) >= *int)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| i128::from(i) < *int)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| i128::from(i) <= *int)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => unreachable!(),
                }
            }
            (Literal::Double(double), ValueArray::Double(doubles)) => {
                match self.cmp {
                    Comparison::Eq => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double.map(|d| d == *double).unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double.map(|d| d != *double).unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double.map(|d| d > *double).unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double.map(|d| d >= *double).unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double.map(|d| d < *double).unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double.map(|d| d <= *double).unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => unreachable!(),
                }
            }
            (Literal::String(string), ValueArray::String(strings)) => {
                let string = string.as_str();
                match self.cmp {
                    Comparison::Eq => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| s == string)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| s != string)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| s > string)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| s >= string)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| s < string)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| s <= string)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => {
                        let re = Regex::new(string)?;
                        Ok(strings
                            .iter()
                            .map(|maybe_string| {
                                maybe_string
                                    .as_deref()
                                    .map(|s| re.is_match(s))
                                    .unwrap_or(false)
                            })
                            .collect())
                    }
                }
            }
            (Literal::Boolean(boolean), ValueArray::Boolean(booleans)) => {
                match self.cmp {
                    Comparison::Eq => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| b == *boolean)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| b != *boolean)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| b & !(*boolean))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| b >= *boolean)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| !b & *boolean)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| b <= *boolean)
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => unreachable!(),
                }
            }
            (_, _) => {
                let lit_type = match &self.expr {
                    Literal::Uuid(_) => "UUID",
                    Literal::Duration(_) => "duration",
                    Literal::Timestamp(_) => "timestamp",
                    Literal::IpAddr(_) => "IP address",
                    Literal::Integer(_) => "integer",
                    Literal::Double(_) => "double",
                    Literal::String(_) => "string",
                    Literal::Boolean(_) => "boolean",
                };
                anyhow::bail!(
                    "Cannot compare {} literal against values of type {}",
                    lit_type,
                    values.data_type(),
                )
            }
        }
    }

    fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        if self.ident.as_str() == "timestamp"
            && matches!(
                self.cmp,
                Comparison::Lt | Comparison::Le | Comparison::Eq
            )
        {
            let Literal::Timestamp(t) = self.expr else {
                return None;
            };
            Some(t)
        } else {
            None
        }
    }
}

impl fmt::Display for FilterAtom {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bang = if self.negated { "!" } else { "" };
        write!(f, "{}({} {} {})", bang, self.ident, self.cmp, self.expr,)
    }
}

#[cfg(test)]
mod tests {
    use crate::oxql::ast::grammar::query_parser;
    use crate::oxql::ast::logical_op::LogicalOp;
    use crate::oxql::point::MetricType;
    use crate::oxql::point::Points;
    use crate::oxql::point::ValueArray;
    use crate::oxql::point::Values;
    use chrono::Utc;
    use oximeter::FieldValue;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_atom_filter_double_points() {
        let start_times = None;
        let timestamps =
            vec![Utc::now(), Utc::now() + Duration::from_secs(1000)];
        let values = vec![Values {
            values: ValueArray::Double(vec![Some(0.0), Some(2.0)]),
            metric_type: MetricType::Gauge,
        }];
        let points = Points { start_times, timestamps, values };

        // This filter should remove the first point based on its timestamp.
        let t = Utc::now() + Duration::from_secs(10);
        let q =
            format!("filter timestamp > @{}", t.format("%Y-%m-%dT%H:%M:%S"));
        let filter = query_parser::filter(q.as_str()).unwrap();
        let out = filter.filter_points(&points).unwrap();
        assert!(out.len() == 1);
        assert_eq!(
            out.values(0).unwrap().as_double().unwrap()[0],
            points.values(0).unwrap().as_double().unwrap()[1],
        );

        // And this one the second point based on the datum
        let filter = query_parser::filter("filter datum < 1.0").unwrap();
        let out = filter.filter_points(&points).unwrap();
        assert!(out.len() == 1);
        assert_eq!(
            out.values(0).unwrap().as_double().unwrap()[0],
            points.values(0).unwrap().as_double().unwrap()[0],
        );
    }

    #[test]
    fn test_atom_filter_points_wrong_type() {
        let start_times = None;
        let timestamps =
            vec![Utc::now(), Utc::now() + Duration::from_secs(1000)];
        let values = vec![Values {
            values: ValueArray::Double(vec![Some(0.0), Some(2.0)]),
            metric_type: MetricType::Gauge,
        }];
        let points = Points { start_times, timestamps, values };

        let filter =
            query_parser::filter("filter datum < \"something\"").unwrap();
        assert!(filter.filter_points(&points).is_err());
    }

    #[test]
    fn test_all_ident_names() {
        let f = query_parser::filter("filter timestamp > @now() && datum < 1")
            .unwrap();
        assert_eq!(
            f.ident_names(),
            ["datum", "timestamp"].into_iter().collect()
        );

        let f = query_parser::filter(
            "filter timestamp > @now() - 1m &&  timestamp < @now()",
        )
        .unwrap();
        let idents = f.ident_names();
        assert_eq!(idents.len(), 1);
        assert_eq!(idents.iter().next().unwrap(), &"timestamp");
    }

    #[test]
    fn test_filter_field_logic() {
        for op in [LogicalOp::And, LogicalOp::Or, LogicalOp::Xor] {
            let s = format!("filter (x > 10) {op} (x < 0)");
            let filter = query_parser::filter(&s).unwrap();
            let cases = &[11, 10, 5, 0, -1];
            for &val in cases.iter() {
                let pass = match op {
                    LogicalOp::And => (val > 10) && (val < 0),
                    LogicalOp::Or => (val > 10) || (val < 0),
                    LogicalOp::Xor => (val > 10) ^ (val < 0),
                };
                let result = filter
                    .filter_field("x", &FieldValue::I32(val))
                    .expect("Filter should be considered comparable")
                    .expect("Filter should apply to field of the same name");
                assert_eq!(
                    result,
                    pass,
                    "Filter '{}' should {} the value {}",
                    filter,
                    if pass { "pass" } else { "not pass" },
                    val,
                );
            }

            // This names a different field, so should not apply.
            assert_eq!(
                filter
                    .filter_field("y", &FieldValue::I32(11))
                    .expect("Filter should be considered comparable"),
                None,
                "Filter should not apply, since it names a different field"
            );

            // These values should not be comparable at all, so we'll return an
            // error.
            let incomparable = &[
                FieldValue::String("foo".into()),
                FieldValue::Uuid(Uuid::new_v4()),
                FieldValue::IpAddr("127.0.0.1".parse().unwrap()),
                FieldValue::Bool(false),
            ];
            for na in incomparable.iter() {
                filter
                    .filter_field("x", na)
                    .expect_err("These should not be comparable at all");
            }
        }
    }
}
