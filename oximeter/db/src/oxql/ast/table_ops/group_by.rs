// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST node for the `group_by` operation.

// Copyright 2024 Oxide Computer Company

use chrono::DateTime;
use chrono::Utc;

use crate::oxql::ast::ident::Ident;
use anyhow::Error;
use oximeter::schema::TimeseriesKey;
use oxql_types::Table;
use oxql_types::Timeseries;
use oxql_types::point::DataType;
use oxql_types::point::MetricType;
use oxql_types::point::ValueArray;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;

/// A table operation for grouping data by fields, apply a reducer to the
/// remaining.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupBy {
    pub identifiers: Vec<Ident>,
    pub reducer: Reducer,
}

impl fmt::Display for GroupBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "group_by [")?;
        let n_idents = self.identifiers.len();
        for (i, ident) in self.identifiers.iter().enumerate() {
            write!(f, "{}", ident.as_str())?;
            if i < n_idents - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "], {}", self.reducer)
    }
}

impl GroupBy {
    // Apply the group_by table operation.
    pub(crate) fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        anyhow::ensure!(
            tables.len() == 1,
            "Group by operations require exactly one table",
        );
        let table = &tables[0];
        anyhow::ensure!(
            table.len() > 0,
            "Input tables to a `group_by` must not be empty"
        );
        anyhow::ensure!(
            table.is_aligned(),
            "Input tables to a `group_by` must be aligned"
        );

        match self.reducer {
            Reducer::Mean => self.reduce_mean(table),
            Reducer::Sum => self.reduce_sum(table),
        }
    }

    fn check_input_timeseries(input: &Timeseries) -> Result<(), Error> {
        anyhow::ensure!(!input.points.is_empty(), "Timeseries cannot be empty");

        // For now, we can only apply this to 1-D timeseries.
        anyhow::ensure!(
            input.points.dimensionality() == 1,
            "Group-by with multi-dimensional timeseries is not yet supported"
        );
        let data_type = input.points.data_types().next().unwrap();
        anyhow::ensure!(
            data_type.is_numeric(),
            "Only numeric data types can be grouped, not {}",
            data_type,
        );
        let metric_type = input.points.metric_types().next().unwrap();
        anyhow::ensure!(
            !matches!(metric_type, MetricType::Cumulative),
            "Cumulative metric types cannot be grouped",
        );
        Ok(())
    }

    // Reduce points in each group by summing.
    fn reduce_sum(&self, table: &Table) -> Result<Vec<Table>, Error> {
        assert_eq!(self.reducer, Reducer::Sum);
        let mut output_table = Table::new(table.name());
        let kept_fields: Vec<_> =
            self.identifiers.iter().map(Ident::as_str).collect();

        for input in table.iter() {
            Self::check_input_timeseries(input)?;

            // Throw away the fields in this timeseries that are not in the
            // group_by list.
            let dropped = input.copy_with_fields(&kept_fields)?;
            let key = dropped.key();

            // Fetch the existing timeseries, if one exists. If one does _not_ exist,
            // we'll insert it as is, without converting. That's because we're
            // just summing, not averaging.
            match output_table.get_mut(key) {
                Some(existing) => {
                    // No casting is done here, we're simply adding T +
                    // T -> T.
                    let new_values = dropped.points.values(0).unwrap();
                    let existing_values = existing.points.values(0).unwrap();
                    match (new_values, existing_values) {
                        (
                            ValueArray::Double(new_values),
                            ValueArray::Double(existing_values),
                        ) => {
                            let new_timestamps = dropped.points.timestamps();

                            // We will be merging the new data with the
                            // existing, but borrow-checking limits the degree
                            // to which we can easily do this on the `existing`
                            // entry in the output table. Instead, aggregate
                            // everything into a copy of the expected data.
                            let mut timestamps =
                                existing.points.timestamps().to_owned();
                            let mut values = existing_values.clone();

                            // Merge in the new values, so long as they actually
                            // exist. That is, we can just skip missing points
                            // in this round, since they do not contribute to
                            // the reduced value.
                            for (new_timestamp, new_value) in new_timestamps
                                .iter()
                                .zip(new_values)
                                .filter_map(|(timestamp, value)| {
                                    if let Some(val) = value {
                                        Some((*timestamp, *val))
                                    } else {
                                        None
                                    }
                                })
                            {
                                // We're really doing binary search, on both the
                                // sample count map and the data array. They
                                // both must exist, or both not, or we've done
                                // our accounting incorrectly.
                                let maybe_index =
                                    timestamps.binary_search(&new_timestamp);
                                match maybe_index {
                                    Err(insert_at) => {
                                        // This is a new timestamp. Insert it
                                        // into the output timeseries.
                                        timestamps
                                            .insert(insert_at, new_timestamp);
                                        values
                                            .insert(insert_at, Some(new_value));
                                    }
                                    Ok(ix) => {
                                        // This is an existing
                                        // timestamp, so we only need to
                                        // add the new value. If the value
                                        // didn't exist before, replace it.
                                        *values[ix].get_or_insert(0.0) +=
                                            new_value;
                                    }
                                }
                            }

                            // Replace the existing output timeseries's
                            // timestamps and data arrays.
                            existing.points.set_timestamps(timestamps);
                            existing
                                .points
                                .values_mut(0)
                                .unwrap()
                                .swap(ValueArray::Double(values));
                        }
                        (
                            ValueArray::Integer(new_values),
                            ValueArray::Integer(existing_values),
                        ) => {
                            let new_timestamps = dropped.points.timestamps();

                            // We will be merging the new data with the
                            // existing, but borrow-checking limits the degree
                            // to which we can easily do this on the `existing`
                            // entry in the output table. Instead, aggregate
                            // everything into a copy of the expected data.
                            let mut timestamps =
                                existing.points.timestamps().to_owned();
                            let mut values = existing_values.clone();

                            // Merge in the new values, so long as they actually
                            // exist. That is, we can just skip missing points
                            // in this round, since they do not contribute to
                            // the reduced value.
                            for (new_timestamp, new_value) in new_timestamps
                                .iter()
                                .zip(new_values)
                                .filter_map(|(timestamp, value)| {
                                    if let Some(val) = value {
                                        Some((*timestamp, *val))
                                    } else {
                                        None
                                    }
                                })
                            {
                                // We're really doing binary search, on both the
                                // sample count map and the data array. They
                                // both must exist, or both not, or we've done
                                // our accounting incorrectly.
                                let maybe_index =
                                    timestamps.binary_search(&new_timestamp);
                                match maybe_index {
                                    Err(insert_at) => {
                                        // This is a new timestamp. Insert it
                                        // into the output timeseries.
                                        timestamps
                                            .insert(insert_at, new_timestamp);
                                        values
                                            .insert(insert_at, Some(new_value));
                                    }
                                    Ok(ix) => {
                                        // This is an existing
                                        // timestamp, so we only need to
                                        // add the new value. If the value
                                        // didn't exist before, replace it.
                                        *values[ix].get_or_insert(0) +=
                                            new_value;
                                    }
                                }
                            }

                            // Replace the existing output timeseries's
                            // timestamps and data arrays.
                            existing.points.set_timestamps(timestamps);
                            existing
                                .points
                                .values_mut(0)
                                .unwrap()
                                .swap(ValueArray::Integer(values));
                        }
                        _ => unreachable!(),
                    }
                }
                None => output_table.insert(dropped)?,
            }
        }
        Ok(vec![output_table])
    }

    // Reduce points in each group by averaging.
    fn reduce_mean(&self, table: &Table) -> Result<Vec<Table>, Error> {
        assert_eq!(self.reducer, Reducer::Mean);
        let mut output_table = Table::new(table.name());
        let kept_fields: Vec<_> =
            self.identifiers.iter().map(Ident::as_str).collect();

        // Keep track of the number of values at each output timestamp, within
        // each group.
        //
        // As we iterate through timeseries, we reduce in-group points, so long
        // as they occur at the same timestamp. And while timeseries must all be
        // aligned the same way, they need not actually have identical
        // timestamps. So what we're producing on the output is data at the
        // union of all the input timestamps.
        //
        // These arrays keeps the count of values at each time, and may be either
        // expanded or have its values incremented. Note that they're all
        // doubles because we will be reducing at the end by dividing the sum at
        // each point by the counts.
        let mut sample_counts_by_group: BTreeMap<
            TimeseriesKey,
            BTreeMap<DateTime<Utc>, f64>,
        > = BTreeMap::new();

        for input in table.iter() {
            Self::check_input_timeseries(input)?;

            // Throw away the fields in this timeseries that are not in the
            // group_by list.
            let dropped = input.copy_with_fields(&kept_fields)?;
            let key = dropped.key();

            // Fetch the existing timeseries, if one exists. If one does _not_ exist,
            // we'll insert the table with the data type converted to a double,
            // since we're always averaging.
            match output_table.get_mut(key) {
                Some(existing) => {
                    // Cast the new points to doubles, since we'll be
                    // aggregating.
                    let new_points =
                        dropped.points.cast(&[DataType::Double])?;
                    let ValueArray::Double(new_values) =
                        new_points.values(0).unwrap()
                    else {
                        unreachable!();
                    };
                    let new_timestamps = new_points.timestamps();

                    // We will be merging the new data with the
                    // existing, but borrow-checking limits the degree
                    // to which we can easily do this on the `existing`
                    // entry in the output table. Instead, aggregate
                    // everything into a copy of the expected data.
                    let mut timestamps =
                        existing.points.timestamps().to_owned();
                    let mut values = existing
                        .points
                        .values(0)
                        .unwrap()
                        .as_double()
                        .unwrap()
                        .clone();

                    // Also fetch a reference to the existing counts by
                    // timestamp for this group. This should exist.
                    let counts = sample_counts_by_group.get_mut(&key).expect(
                        "Should already have some sample counts for this group",
                    );

                    // Merge in the new values, so long as they actually
                    // exist. That is, we can just skip missing points
                    // in this round, since they do not contribute to
                    // the reduced value.
                    for (new_timestamp, new_value) in new_timestamps
                        .iter()
                        .zip(new_values)
                        .filter_map(|(timestamp, value)| {
                            if let Some(val) = value {
                                Some((*timestamp, *val))
                            } else {
                                None
                            }
                        })
                    {
                        // We're really doing binary search, on both the
                        // sample count map and the data array. They
                        // both must exist, or both not, or we've done
                        // our accounting incorrectly.
                        let maybe_index =
                            timestamps.binary_search(&new_timestamp);
                        let count = counts.entry(new_timestamp);
                        match (count, maybe_index) {
                            (Entry::Vacant(entry), Err(insert_at)) => {
                                // This is a new timestamp. Insert it
                                // into the output timeseries, and count
                                // it.
                                timestamps.insert(insert_at, new_timestamp);
                                values.insert(insert_at, Some(new_value));
                                entry.insert(1.0);
                            }
                            (Entry::Occupied(mut entry), Ok(ix)) => {
                                // This is an existing timestamp. _Add_
                                // it into the output timeseries, and
                                // count it. Its timestamp already
                                // exists. If the value was previously None,
                                // replace it now.
                                *values[ix].get_or_insert(0.0) += new_value;
                                *entry.get_mut() += 1.0;
                            }
                            (_, _) => {
                                panic!(
                                    "In-group counts and output \
                                    values must both exist or \
                                    both be missing"
                                );
                            }
                        }
                    }

                    // Replace the existing output timeseries's
                    // timestamps and data arrays.
                    existing.points.set_timestamps(timestamps);
                    existing
                        .points
                        .values_mut(0)
                        .unwrap()
                        .swap(ValueArray::Double(values));
                }
                None => {
                    // There were no previous points for this group.
                    //
                    // We'll cast to doubles, but _keep_ any missing samples
                    // (None) that were in there. Those will have a "count" of
                    // 0, so that we don't incorrectly over-divide in the case
                    // where there are both missing and non-missing samples.
                    let new_timeseries = dropped.cast(&[DataType::Double])?;
                    let values = new_timeseries
                        .points
                        .values(0)
                        .unwrap()
                        .as_double()
                        .unwrap();
                    // Insert a count of 1.0 for each timestamp remaining, and
                    // _zero_ for any where the values are none.
                    let counts = new_timeseries
                        .points
                        .timestamps()
                        .iter()
                        .zip(values)
                        .map(|(timestamp, maybe_value)| {
                            let count = f64::from(maybe_value.is_some());
                            (*timestamp, count)
                        })
                        .collect();
                    let old = sample_counts_by_group.insert(key, counts);
                    assert!(
                        old.is_none(),
                        "Should not have counts entry for first timeseries in the group"
                    );
                    output_table.insert(new_timeseries)?;
                }
            }
        }

        // Since we're computing the mean, we need to divide each output value
        // by the number of values that went into it.
        for each in output_table.iter_mut() {
            let counts = sample_counts_by_group
                .get(&each.key())
                .expect("key should have been inserted earlier");
            let ValueArray::Double(values) = each.points.values_mut(0).unwrap()
            else {
                unreachable!();
            };
            for (val, count) in values.iter_mut().zip(counts.values()) {
                if let Some(x) = val.as_mut() {
                    *x /= *count;
                }
            }
        }
        Ok(vec![output_table])
    }
}

/// A reduction operation applied to unnamed columns during a group by.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Reducer {
    #[default]
    Mean,
    Sum,
}

impl fmt::Display for Reducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reducer::Mean => write!(f, "mean"),
            Reducer::Sum => write!(f, "sum"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupBy, Reducer};
    use crate::oxql::ast::{
        ident::Ident,
        table_ops::align::{Align, AlignmentMethod},
    };
    use chrono::{DateTime, Utc};
    use oximeter::FieldValue;
    use oxql_types::{
        Table, Timeseries,
        point::{DataType, MetricType, ValueArray},
    };
    use std::{collections::BTreeMap, time::Duration};

    // Which timeseries the second data point is missing from.
    #[derive(Clone, Copy, Debug)]
    enum MissingValue {
        Neither,
        First,
        Both,
    }

    #[derive(Clone, Copy, Debug)]
    struct TestConfig {
        missing_value: MissingValue,
        overlapping_times: bool,
        reducer: Reducer,
    }

    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct TestTable {
        aligned_table: Table,
        grouped_table: Table,
        query_end: DateTime<Utc>,
        timestamps: Vec<DateTime<Utc>>,
    }

    impl TestTable {
        fn new(cfg: TestConfig) -> Self {
            let query_end = Utc::now();
            let mut timestamps = vec![
                query_end - Duration::from_secs(2),
                query_end - Duration::from_secs(1),
                query_end,
            ];

            // Create the first timeseries.
            //
            // This has two fields, one of which we'll group by. There are three
            // timepoints of double values.
            let mut fields = BTreeMap::new();
            fields.insert("int".to_string(), FieldValue::U8(0));
            fields.insert(
                "name".to_string(),
                FieldValue::String("whodat".into()),
            );
            let mut ts0 = Timeseries::new(
                fields.into_iter(),
                DataType::Double,
                MetricType::Gauge,
            )
            .unwrap();
            ts0.points.clear_start_times();
            ts0.points.set_timestamps(timestamps.clone());
            *ts0.points.values_mut(0).unwrap() = ValueArray::Double(vec![
                Some(1.0),
                if matches!(
                    cfg.missing_value,
                    MissingValue::First | MissingValue::Both
                ) {
                    None
                } else {
                    Some(2.0)
                },
                Some(3.0),
            ]);

            // Create the second timeseries.
            //
            // This is nearly the same, and shares the same field value for the
            // "int" field. When we group, we should reduce these two timeseries
            // together.
            let mut fields = BTreeMap::new();
            fields.insert("int".to_string(), FieldValue::U8(0));
            fields.insert(
                "name".to_string(),
                FieldValue::String("whodis".into()),
            );
            let mut ts1 = Timeseries::new(
                fields.into_iter(),
                DataType::Double,
                MetricType::Gauge,
            )
            .unwrap();
            ts1.points.clear_start_times();

            // Non-overlapping in this test setup means that we just shift one
            // value from this array backward in time by one additional second.
            // So we should have timestamps like:
            //
            // ts0: [ _, t0, t1, t2 ]
            // ts1: [ t0, _, t1, t2 ]
            //
            // When reducing, t0 is never changed, and t1-t2 are always reduced
            // together, if the values are present.
            let new_timestamps = if cfg.overlapping_times {
                timestamps.clone()
            } else {
                let mut new_timestamps = timestamps.clone();
                new_timestamps[0] = new_timestamps[0] - Duration::from_secs(1);
                timestamps.insert(0, new_timestamps[0]);
                new_timestamps
            };
            ts1.points.set_timestamps(new_timestamps);
            *ts1.points.values_mut(0).unwrap() = ValueArray::Double(vec![
                Some(2.0),
                if matches!(cfg.missing_value, MissingValue::Both) {
                    None
                } else {
                    Some(3.0)
                },
                Some(4.0),
            ]);

            let mut table = Table::new("foo");
            table.insert(ts0).unwrap();
            table.insert(ts1).unwrap();

            // Align the actual table, based on the input, and apply the right
            // group-by
            let align = Align {
                method: AlignmentMethod::MeanWithin,
                period: Duration::from_secs(1),
            };
            let aligned_tables = align.apply(&[table], &query_end).unwrap();
            let group_by = GroupBy {
                identifiers: vec![Ident("int".into())],
                reducer: cfg.reducer,
            };
            let grouped_tables = group_by.apply(&aligned_tables).unwrap();
            assert_eq!(
                grouped_tables.len(),
                1,
                "Group by should produce exaclty 1 table"
            );
            let grouped_table = grouped_tables.into_iter().next().unwrap();
            let aligned_table = aligned_tables.into_iter().next().unwrap();

            let test =
                Self { timestamps, aligned_table, grouped_table, query_end };

            // These checks are all valid for grouping in general, independent
            // of the exact missing values or reducer.
            assert_eq!(
                test.grouped_table.len(),
                1,
                "Should have grouped both timeseries down to 1"
            );
            let grouped_timeseries = test.grouped_table.iter().next().unwrap();
            assert_eq!(
                grouped_timeseries.fields.len(),
                1,
                "Should have only one grouped-by field"
            );
            assert_eq!(
                grouped_timeseries.fields.get("int").unwrap(),
                &FieldValue::U8(0),
                "Grouped-by field was not maintained correctly"
            );
            let points = &grouped_timeseries.points;
            assert_eq!(points.dimensionality(), 1, "Points should still be 1D");
            assert_eq!(
                points.start_times(),
                None,
                "Points should not have start times"
            );
            assert_eq!(
                points.timestamps(),
                test.timestamps,
                "Points do not have correct timestamps"
            );

            test
        }
    }

    #[test]
    fn test_group_by() {
        const TEST_CASES: &[(TestConfig, &[Option<f64>])] = &[
            (
                TestConfig {
                    missing_value: MissingValue::Neither,
                    overlapping_times: true,
                    reducer: Reducer::Mean,
                },
                // This is the most basic case, where we simply average all the
                // values together. They exactly line up and none are missing.
                &[Some(1.5), Some(2.5), Some(3.5)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::Neither,
                    overlapping_times: true,
                    reducer: Reducer::Sum,
                },
                // This is the next-simplest case, where we simply sum all the
                // values together. They exactly line up and none are missing.
                &[Some(3.0), Some(5.0), Some(7.0)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::Neither,
                    overlapping_times: false,
                    reducer: Reducer::Mean,
                },
                // In this case, the timestamps don't all overlap, though some
                // of them do. In particular, the arrays are shifted by one
                // timestamp relative to each other, so there are 2 extra
                // values. The one value that does overlap is averaged, and the
                // other two are unchanged.
                &[Some(2.0), Some(1.0), Some(2.5), Some(3.5)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::Neither,
                    overlapping_times: false,
                    reducer: Reducer::Sum,
                },
                // Here, we should have 4 output samples because the timestamps
                // don't overlap. The second input timeseries has its first
                // point shifted back by one second. That means the first two
                // values are just from one array (no reduction), while the next
                // two are reduced as usual.
                &[Some(2.0), Some(1.0), Some(5.0), Some(7.0)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::First,
                    overlapping_times: true,
                    reducer: Reducer::Mean,
                },
                // In this case, we have a missing value for the middle
                // timestamp of the first input timeseries. That means we should
                // still have 3 output samples, but the second point isn't an
                // aggregation, it's just the input value, from the second
                // timeseries.
                &[Some(1.5), Some(3.0), Some(3.5)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::First,
                    overlapping_times: true,
                    reducer: Reducer::Sum,
                },
                // Same as above, but we're summing, not averaging.
                &[Some(3.0), Some(3.0), Some(7.0)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::First,
                    overlapping_times: false,
                    reducer: Reducer::Mean,
                },
                // We need 4 output points again here, but we also have a
                // missing value. So we'll take the first value from the second
                // timeseries; the second from the first; the second from the
                // second directly, since its corresponding point is missing in
                // the first, and then the average of both in the last point.
                &[Some(2.0), Some(1.0), Some(3.0), Some(3.5)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::First,
                    overlapping_times: false,
                    reducer: Reducer::Sum,
                },
                // Same as above, but summing, instead of averaging.
                &[Some(2.0), Some(1.0), Some(3.0), Some(7.0)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::Both,
                    overlapping_times: true,
                    reducer: Reducer::Mean,
                },
                // In this case, the 2nd timepoint is missing from both
                // timeseries. We should preserve that as a missing value in the
                // output.
                &[Some(1.5), None, Some(3.5)],
            ),
            (
                TestConfig {
                    missing_value: MissingValue::Both,
                    overlapping_times: true,
                    reducer: Reducer::Sum,
                },
                // Same as above, but summing instead of averaging.
                &[Some(3.0), None, Some(7.0)],
            ),
        ];
        for (test_config, expected_data) in TEST_CASES.iter() {
            let test_table = TestTable::new(*test_config);
            let grouped_timeseries =
                test_table.grouped_table.iter().next().unwrap();
            let points = &grouped_timeseries.points;
            let values = points.values(0).unwrap().as_double().unwrap();
            assert_eq!(
                values, expected_data,
                "Timeseries values were not grouped correctly, \
                test_config = {test_config:?}"
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::GroupBy;
    use super::Reducer;
    use crate::oxql::ast::ident::Ident;

    #[test]
    fn test_group_by_display() {
        let g = GroupBy {
            identifiers: vec![
                Ident(String::from("foo")),
                Ident(String::from("bar")),
            ],
            reducer: Reducer::Mean,
        };
        assert_eq!(g.to_string(), "group_by [foo, bar], mean");

        let g = GroupBy { identifiers: vec![], reducer: Reducer::Sum };
        assert_eq!(g.to_string(), "group_by [], sum");
    }
}
