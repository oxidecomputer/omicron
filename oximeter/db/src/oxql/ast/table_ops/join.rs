// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing join table operations.

// Copyright 2024 Oxide Computer Company

use anyhow::Context;
use anyhow::Error;
use oxql_types::Table;
use oxql_types::point::MetricType;

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
            out.len() > 0,
            "Input tables for a join operation must not be empty",
        );
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
                timeseries.points =
                    timeseries.points.inner_join(&next_timeseries.points)?;
            }
            // We'll also update the name, to indicate the joined data.
            out.name.push(',');
            out.name.push_str(&name);
        }
        Ok(vec![out])
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
