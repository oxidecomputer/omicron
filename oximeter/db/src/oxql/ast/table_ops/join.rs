// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing join table operations.

// Copyright 2024 Oxide Computer Company

use crate::oxql::point::MetricType;
use crate::oxql::point::Points;
use crate::oxql::point::Values;
use crate::oxql::Error;
use crate::oxql::Table;
use anyhow::Context;

/// An AST node for a natural inner join.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Join;
impl Join {
    // Apply the group_by table operation.
    pub(crate) fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        anyhow::ensure!(
            tables.len() > 1,
            "Join operations require more than one table",
        );
        let mut tables = tables.iter().cloned().enumerate();
        let (_, mut out) = tables.next().unwrap();
        anyhow::ensure!(
            out.is_aligned(),
            "Input tables for a join operation must be aligned"
        );
        let metric_types = out
            .iter()
            .next()
            .context("Input tables for a join operation may not be empty")?
            .points
            .metric_types()
            .collect::<Vec<_>>();
        ensure_all_metric_types(metric_types.iter().copied())?;
        let alignment = out.alignment();
        assert!(alignment.is_some());

        for (i, next_table) in tables {
            anyhow::ensure!(
                next_table.alignment() == alignment,
                "All tables to a join operator must have the same \
                alignment. Expected alignment: {:?}, found a table \
                aligned with: {:?}",
                alignment.unwrap(),
                next_table.alignment(),
            );
            let name = next_table.name().to_string();
            for next_timeseries in next_table.into_iter() {
                let new_types =
                    next_timeseries.points.metric_types().collect::<Vec<_>>();
                ensure_all_metric_types(new_types.iter().copied())?;
                anyhow::ensure!(
                    metric_types == new_types,
                    "Input tables do not all share the same metric types"
                );

                let key = next_timeseries.key();
                let Some(timeseries) = out.iter_mut().find(|t| t.key() == key)
                else {
                    anyhow::bail!(
                        "Join failed, input table {} does not \
                        contain a timeseries with key {}",
                        i,
                        key,
                    );
                };

                // Joining the timeseries is done by stacking together the
                // values that have the same timestamp.
                //
                // If two value arrays have different timestamps, which is
                // possible if they're derived from two separately-aligned
                // tables, then we need to correctly ensure that:
                //
                // 1. They have the same alignment, and
                // 2. We merge the timepoints rather than simply creating a
                //    ragged array of points.
                timeseries.points = inner_join_point_arrays(
                    &timeseries.points,
                    &next_timeseries.points,
                )?;
            }
            // We'll also update the name, to indicate the joined data.
            out.name.push(',');
            out.name.push_str(&name);
        }
        Ok(vec![out])
    }
}

// Given two arrays of points, stack them together at matching timepoints.
//
// For time points in either which do not have a corresponding point in the
// other, the entire time point is elided.
fn inner_join_point_arrays(
    left: &Points,
    right: &Points,
) -> Result<Points, Error> {
    // Create an output array with roughly the right capacity, and double the
    // number of dimensions. We're trying to stack output value arrays together
    // along the dimension axis.
    let data_types =
        left.data_types().chain(right.data_types()).collect::<Vec<_>>();
    let metric_types =
        left.metric_types().chain(right.metric_types()).collect::<Vec<_>>();
    let mut out = Points::with_capacity(
        left.len().max(right.len()),
        data_types.iter().copied(),
        metric_types.iter().copied(),
    )?;

    // Iterate through each array until one is exhausted. We're only inserting
    // values from both arrays where the timestamps actually match, since this
    // is an inner join. We may want to insert missing values where timestamps
    // do not match on either side, when we support an outer join of some kind.
    let n_left_dim = left.values.len();
    let mut left_ix = 0;
    let mut right_ix = 0;
    while left_ix < left.len() && right_ix < right.len() {
        let left_timestamp = left.timestamps[left_ix];
        let right_timestamp = right.timestamps[right_ix];
        if left_timestamp == right_timestamp {
            out.timestamps.push(left_timestamp);
            push_concrete_values(
                &mut out.values[..n_left_dim],
                &left.values,
                left_ix,
            );
            push_concrete_values(
                &mut out.values[n_left_dim..],
                &right.values,
                right_ix,
            );
            left_ix += 1;
            right_ix += 1;
        } else if left_timestamp < right_timestamp {
            left_ix += 1;
        } else {
            right_ix += 1;
        }
    }
    Ok(out)
}

// Push the `i`th value from each dimension of `from` onto `to`.
fn push_concrete_values(to: &mut [Values], from: &[Values], i: usize) {
    assert_eq!(to.len(), from.len());
    for (output, input) in to.iter_mut().zip(from.iter()) {
        let input_array = &input.values;
        let output_array = &mut output.values;
        assert_eq!(input_array.data_type(), output_array.data_type());
        if let Ok(ints) = input_array.as_integer() {
            output_array.as_integer_mut().unwrap().push(ints[i]);
            continue;
        }
        if let Ok(doubles) = input_array.as_double() {
            output_array.as_double_mut().unwrap().push(doubles[i]);
            continue;
        }
        if let Ok(bools) = input_array.as_boolean() {
            output_array.as_boolean_mut().unwrap().push(bools[i]);
            continue;
        }
        if let Ok(strings) = input_array.as_string() {
            output_array.as_string_mut().unwrap().push(strings[i].clone());
            continue;
        }
        if let Ok(dists) = input_array.as_integer_distribution() {
            output_array
                .as_integer_distribution_mut()
                .unwrap()
                .push(dists[i].clone());
            continue;
        }
        if let Ok(dists) = input_array.as_double_distribution() {
            output_array
                .as_double_distribution_mut()
                .unwrap()
                .push(dists[i].clone());
            continue;
        }
        unreachable!();
    }
}

// Return an error if any metric types are not suitable for joining.
fn ensure_all_metric_types(
    mut metric_types: impl ExactSizeIterator<Item = MetricType>,
) -> Result<(), Error> {
    anyhow::ensure!(
        metric_types
            .all(|mt| matches!(mt, MetricType::Gauge | MetricType::Delta)),
        "Join operation requires timeseries with gauge or \
        delta metric types",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oxql::point::DataType;
    use crate::oxql::point::Datum;
    use crate::oxql::point::ValueArray;
    use chrono::Utc;
    use std::time::Duration;

    #[test]
    fn test_push_concrete_values() {
        let mut points = Points::with_capacity(
            2,
            [DataType::Integer, DataType::Double].into_iter(),
            [MetricType::Gauge, MetricType::Gauge].into_iter(),
        )
        .unwrap();

        // Push a concrete value for the integer dimension
        let from_ints = vec![Values {
            values: ValueArray::Integer(vec![Some(1)]),
            metric_type: MetricType::Gauge,
        }];
        push_concrete_values(&mut points.values[..1], &from_ints, 0);

        // And another for the double dimension.
        let from_doubles = vec![Values {
            values: ValueArray::Double(vec![Some(2.0)]),
            metric_type: MetricType::Gauge,
        }];
        push_concrete_values(&mut points.values[1..], &from_doubles, 0);

        assert_eq!(
            points.dimensionality(),
            2,
            "Points should have 2 dimensions",
        );
        let ints = points.values[0].values.as_integer().unwrap();
        assert_eq!(
            ints.len(),
            1,
            "Should have pushed one point in the first dimension"
        );
        assert_eq!(
            ints[0],
            Some(1),
            "Should have pushed 1 onto the first dimension"
        );
        let doubles = points.values[1].values.as_double().unwrap();
        assert_eq!(
            doubles.len(),
            1,
            "Should have pushed one point in the second dimension"
        );
        assert_eq!(
            doubles[0],
            Some(2.0),
            "Should have pushed 2.0 onto the second dimension"
        );
    }

    #[test]
    fn test_join_point_arrays() {
        let now = Utc::now();

        // Create a set of integer points to join with.
        //
        // This will have two timestamps, one of which will match the points
        // below that are merged in.
        let int_points = Points {
            start_times: None,
            timestamps: vec![
                now - Duration::from_secs(3),
                now - Duration::from_secs(2),
                now,
            ],
            values: vec![Values {
                values: ValueArray::Integer(vec![Some(1), Some(2), Some(3)]),
                metric_type: MetricType::Gauge,
            }],
        };

        // Create an additional set of double points.
        //
        // This also has two timepoints, one of which matches with the above,
        // and one of which does not.
        let double_points = Points {
            start_times: None,
            timestamps: vec![
                now - Duration::from_secs(3),
                now - Duration::from_secs(1),
                now,
            ],
            values: vec![Values {
                values: ValueArray::Double(vec![
                    Some(4.0),
                    Some(5.0),
                    Some(6.0),
                ]),
                metric_type: MetricType::Gauge,
            }],
        };

        // Merge the arrays.
        let merged =
            inner_join_point_arrays(&int_points, &double_points).unwrap();

        // Basic checks that we merged in the right values and have the right
        // types and dimensions.
        assert_eq!(
            merged.dimensionality(),
            2,
            "Should have appended the dimensions from each input array"
        );
        assert_eq!(merged.len(), 2, "Should have merged two common points",);
        assert_eq!(
            merged.data_types().collect::<Vec<_>>(),
            &[DataType::Integer, DataType::Double],
            "Should have combined the data types of the input arrays"
        );
        assert_eq!(
            merged.metric_types().collect::<Vec<_>>(),
            &[MetricType::Gauge, MetricType::Gauge],
            "Should have combined the metric types of the input arrays"
        );

        // Check the actual values of the array.
        let mut points = merged.iter_points();

        // The first and last timepoint overlapped between the two arrays, so we
        // should have both of them as concrete samples.
        let pt = points.next().unwrap();
        assert_eq!(pt.start_time, None, "Gauges don't have a start time");
        assert_eq!(
            *pt.timestamp, int_points.timestamps[0],
            "Should have taken the first input timestamp from both arrays",
        );
        assert_eq!(
            *pt.timestamp, double_points.timestamps[0],
            "Should have taken the first input timestamp from both arrays",
        );
        let values = pt.values;
        assert_eq!(values.len(), 2, "Should have 2 dimensions");
        assert_eq!(
            &values[0],
            &(Datum::Integer(Some(&1)), MetricType::Gauge),
            "Should have pulled value from first integer array."
        );
        assert_eq!(
            &values[1],
            &(Datum::Double(Some(&4.0)), MetricType::Gauge),
            "Should have pulled value from second double array."
        );

        // And the next point
        let pt = points.next().unwrap();
        assert_eq!(pt.start_time, None, "Gauges don't have a start time");
        assert_eq!(
            *pt.timestamp, int_points.timestamps[2],
            "Should have taken the input timestamp from both arrays",
        );
        assert_eq!(
            *pt.timestamp, double_points.timestamps[2],
            "Should have taken the input timestamp from both arrays",
        );
        let values = pt.values;
        assert_eq!(values.len(), 2, "Should have 2 dimensions");
        assert_eq!(
            &values[0],
            &(Datum::Integer(Some(&3)), MetricType::Gauge),
            "Should have pulled value from first integer array."
        );
        assert_eq!(
            &values[1],
            &(Datum::Double(Some(&6.0)), MetricType::Gauge),
            "Should have pulled value from second double array."
        );

        // And there should be no other values.
        assert!(points.next().is_none(), "There should be no more points");
    }
}
