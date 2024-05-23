// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node apply limiting timeseries operations.

// Copyright 2024 Oxide Computer Company

use crate::oxql::point::Points;
use crate::oxql::point::ValueArray;
use crate::oxql::point::Values;
use crate::oxql::Error;
use crate::oxql::Table;
use crate::oxql::Timeseries;
use std::num::NonZeroUsize;

/// The kind of limiting operation
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LimitKind {
    /// Limit the timeseries to the first points.
    First,
    /// Limit the timeseries to the last points.
    Last,
}

/// A table operation limiting a timeseries to a number of points.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Limit {
    /// The kind of limit
    pub kind: LimitKind,
    /// The number of points the timeseries is limited to.
    pub count: NonZeroUsize,
}
impl Limit {
    /// Apply the limit operation to the input tables.
    pub(crate) fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        if tables.is_empty() {
            return Ok(vec![]);
        }

        tables
            .iter()
            .map(|table| {
                let timeseries = table.iter().map(|timeseries| {
                    let input_points = &timeseries.points;

                    // Compute the slice indices for this timeseries.
                    let (start, end) = match self.kind {
                        LimitKind::First => {
                            // The count in the limit operation should not be
                            // larger than the number of data points.
                            let end = input_points.len().min(self.count.get());
                            (0, end)
                        }
                        LimitKind::Last => {
                            // When taking the last k points, we need to
                            // subtract the count from the end of the array,
                            // taking care that we don't panic if the count is
                            // larger than the number of data points.
                            let start = input_points
                                .len()
                                .saturating_sub(self.count.get());
                            let end = input_points.len();
                            (start, end)
                        }
                    };

                    // Slice the various data arrays.
                    let start_times = input_points
                        .start_times
                        .as_ref()
                        .map(|s| s[start..end].to_vec());
                    let timestamps =
                        input_points.timestamps[start..end].to_vec();
                    let values = input_points
                        .values
                        .iter()
                        .map(|vals| {
                            let values = match &vals.values {
                                ValueArray::Integer(inner) => {
                                    ValueArray::Integer(
                                        inner[start..end].to_vec(),
                                    )
                                }
                                ValueArray::Double(inner) => {
                                    ValueArray::Double(
                                        inner[start..end].to_vec(),
                                    )
                                }
                                ValueArray::Boolean(inner) => {
                                    ValueArray::Boolean(
                                        inner[start..end].to_vec(),
                                    )
                                }
                                ValueArray::String(inner) => {
                                    ValueArray::String(
                                        inner[start..end].to_vec(),
                                    )
                                }
                                ValueArray::IntegerDistribution(inner) => {
                                    ValueArray::IntegerDistribution(
                                        inner[start..end].to_vec(),
                                    )
                                }
                                ValueArray::DoubleDistribution(inner) => {
                                    ValueArray::DoubleDistribution(
                                        inner[start..end].to_vec(),
                                    )
                                }
                            };
                            Values { values, metric_type: vals.metric_type }
                        })
                        .collect();
                    let points = Points { start_times, timestamps, values };
                    Timeseries {
                        fields: timeseries.fields.clone(),
                        points,
                        alignment: timeseries.alignment,
                    }
                });
                Table::from_timeseries(table.name(), timeseries)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oxql::point::{DataType, MetricType};
    use chrono::Utc;
    use oximeter::FieldValue;
    use std::{collections::BTreeMap, time::Duration};

    fn test_tables() -> Vec<Table> {
        let mut fields = BTreeMap::new();
        fields.insert("foo".to_string(), FieldValue::from("bar"));
        fields.insert("bar".to_string(), FieldValue::from(1u8));

        let now = Utc::now();
        let timestamps = vec![
            now - Duration::from_secs(4),
            now - Duration::from_secs(3),
            now - Duration::from_secs(2),
        ];

        let mut timeseries = Timeseries::new(
            fields.clone().into_iter(),
            DataType::Integer,
            MetricType::Gauge,
        )
        .unwrap();
        timeseries.points.timestamps.clone_from(&timestamps);
        timeseries.points.values[0].values.as_integer_mut().unwrap().extend([
            Some(1),
            Some(2),
            Some(3),
        ]);
        let table1 =
            Table::from_timeseries("first", std::iter::once(timeseries))
                .unwrap();

        let mut timeseries = Timeseries::new(
            fields.clone().into_iter(),
            DataType::Integer,
            MetricType::Gauge,
        )
        .unwrap();
        timeseries.points.timestamps.clone_from(&timestamps);
        timeseries.points.values[0].values.as_integer_mut().unwrap().extend([
            Some(4),
            Some(5),
            Some(6),
        ]);
        let table2 =
            Table::from_timeseries("second", std::iter::once(timeseries))
                .unwrap();

        vec![table1, table2]
    }

    #[test]
    fn test_first_k() {
        test_limit_impl(LimitKind::First);
    }

    #[test]
    fn test_last_k() {
        test_limit_impl(LimitKind::Last);
    }

    fn test_limit_impl(kind: LimitKind) {
        let (start, end) = match kind {
            LimitKind::First => (0, 2),
            LimitKind::Last => (1, 3),
        };

        // Create test data and apply limit operation.
        let tables = test_tables();
        let limit = Limit { kind, count: 2.try_into().unwrap() };
        let limited = limit.apply(&tables).expect("This should be infallible");
        assert_eq!(
            tables.len(),
            limited.len(),
            "Limiting should not change the number of tables"
        );

        // Should apply to all tables the same way.
        for (table, limited_table) in tables.iter().zip(limited.iter()) {
            assert_eq!(
                table.name(),
                limited_table.name(),
                "Limited table whould have the same name"
            );

            // Compare all timeseries.
            for (timeseries, limited_timeseries) in
                table.iter().zip(limited_table.iter())
            {
                // The fields and basic shape should not change.
                assert_eq!(
                    timeseries.fields, limited_timeseries.fields,
                    "Limited table should have the same fields"
                );
                assert_eq!(
                    timeseries.alignment, limited_timeseries.alignment,
                    "Limited timeseries should have the same alignment"
                );
                assert_eq!(
                    timeseries.points.dimensionality(),
                    limited_timeseries.points.dimensionality(),
                    "Limited timeseries should have the same number of dimensions"
                );

                // Compare data points themselves.
                //
                // These depend on the limit operation.
                let points = &timeseries.points;
                let limited_points = &limited_timeseries.points;
                assert_eq!(points.start_times, limited_points.start_times);
                assert_eq!(
                    points.timestamps[start..end],
                    limited_points.timestamps
                );
                assert_eq!(
                    limited_points.values[0].values.as_integer().unwrap(),
                    &points.values[0].values.as_integer().unwrap()[start..end],
                    "Points should be limited to [{start}..{end}]",
                );
            }
        }

        // Check that limiting the table to more points than exist returns the
        // whole table.
        let limit = Limit { kind, count: 100.try_into().unwrap() };
        let limited = limit.apply(&tables).expect("This should be infallible");
        assert_eq!(
            limited,
            tables,
            "Limiting tables to more than their length should return the same thing"
        );
    }
}
