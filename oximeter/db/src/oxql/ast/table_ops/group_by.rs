// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST node for the `group_by` operation.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::ident::Ident;
use crate::oxql::point::DataType;
use crate::oxql::point::MetricType;
use crate::oxql::point::ValueArray;
use crate::oxql::Error;
use crate::oxql::Table;
use anyhow::Context;
use num::ToPrimitive;
use std::collections::BTreeMap;

/// A table operation for grouping data by fields, apply a reducer to the
/// remaining.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupBy {
    pub identifiers: Vec<Ident>,
    pub reducer: Reducer,
}

impl GroupBy {
    // Apply the group_by table operation.
    pub(crate) fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        anyhow::ensure!(
            tables.len() == 1,
            "Group by operations require exactly one table",
        );
        let table = &tables[0];
        let mut output_table = Table::new(table.name());
        let kept_fields: Vec<_> =
            self.identifiers.iter().map(Ident::as_str).collect();
        let mut counts: BTreeMap<_, u64> = BTreeMap::new();
        for input in table.iter() {
            anyhow::ensure!(
                input.points.len() > 0,
                "Timeseries cannot be empty"
            );

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
            anyhow::ensure!(
                table.is_aligned(),
                "Input tables to a `group_by` must be aligned"
            );

            // Throw away the fields in this timeseries that are not in the
            // group_by list.
            let dropped = input.copy_with_fields(&kept_fields)?;
            let key = dropped.key();

            // Fetch the existing table, if one exists. If one does _not_ exist,
            // we'll insert the table with the data type converted to a thee
            // appropriate type. For a mean reducer, the output is always a
            // double. For others, the value may be the same as the input type.
            //
            // TODO-completeness: We need to support reducers other than `Mean`
            // and `Sum` here.
            match output_table.get_mut(key) {
                Some(existing) => {
                    anyhow::ensure!(
                        existing.points.len() == dropped.points.len(),
                        "Cannot group timeseries with different numbers of data points",
                    );
                    assert!(
                        existing.points.timestamps == dropped.points.timestamps
                    );
                    match self.reducer {
                        Reducer::Mean => {
                            // Cast the input type to a double, and sum in the
                            // values to the existing entry, which must have
                            // also been a double.
                            let new_points =
                                dropped.points.cast(&[DataType::Double])?;
                            let ValueArray::Double(existing_values) =
                                existing.points.values_mut(0).unwrap()
                            else {
                                unreachable!();
                            };
                            let ValueArray::Double(new_values) =
                                new_points.values(0).unwrap()
                            else {
                                unreachable!();
                            };
                            for (mut pt0, pt1) in existing_values
                                .iter_mut()
                                .zip(new_values.into_iter())
                            {
                                match (&mut pt0, pt1) {
                                    (None, None) | (Some(_), None) => {}
                                    (None, Some(new)) => {
                                        pt0.replace(*new);
                                    }
                                    (Some(x), Some(y)) => *x += y,
                                }
                            }
                        }
                        Reducer::Sum => {
                            // No casting is done here, we're simply adding T +
                            // T -> T.
                            let new_values = dropped.points.values(0).unwrap();
                            let existing_values =
                                existing.points.values_mut(0).unwrap();
                            match (new_values, existing_values) {
                                (
                                    ValueArray::Double(new_values),
                                    ValueArray::Double(existing_values),
                                ) => {
                                    for (mut pt0, pt1) in existing_values
                                        .iter_mut()
                                        .zip(new_values.into_iter())
                                    {
                                        match (&mut pt0, pt1) {
                                            (None, None) | (Some(_), None) => {}
                                            (None, Some(new)) => {
                                                pt0.replace(*new);
                                            }
                                            (Some(x), Some(y)) => *x += y,
                                        }
                                    }
                                }
                                (
                                    ValueArray::Integer(new),
                                    ValueArray::Integer(existing),
                                ) => {
                                    for (mut pt0, pt1) in
                                        existing.iter_mut().zip(new.into_iter())
                                    {
                                        match (&mut pt0, pt1) {
                                            (None, None) | (Some(_), None) => {}
                                            (None, Some(new)) => {
                                                pt0.replace(*new);
                                            }
                                            (Some(x), Some(y)) => *x += y,
                                        }
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
                None => {
                    let new_points = match self.reducer {
                        Reducer::Mean => dropped.cast(&[DataType::Double])?,
                        Reducer::Sum => dropped,
                    };
                    output_table.insert(new_points)?;
                }
            }

            // Update the count of tables we've added with this key.
            *counts.entry(key).or_default() += 1;
        }

        // Depending on the reducer, compute the sum (do nothing) or average.
        match self.reducer {
            Reducer::Mean => {
                for each in output_table.iter_mut() {
                    let count = counts
                        .get(&each.key())
                        .expect("key should have been inserted earlier")
                        .to_f64()
                        .context("Failed to convert u64 count to f64 for reducing mean")?;
                    let ValueArray::Double(values) =
                        each.points.values_mut(0).unwrap()
                    else {
                        unreachable!();
                    };
                    for val in values.iter_mut() {
                        if let Some(x) = val.as_mut() {
                            *x /= count;
                        }
                    }
                }
            }
            Reducer::Sum => {}
        }
        Ok(vec![output_table])
    }
}

/// A reduction operation applied to unnamed columns during a group by.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Reducer {
    #[default]
    Mean,
    Sum,
}
