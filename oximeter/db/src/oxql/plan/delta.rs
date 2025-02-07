// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Query plan node for constructing a delta from a cumulative timeseries.

// Copyright 2024 Oxide Computer Company

use oxql_types::point::MetricType;

use crate::oxql::schema::TableSchema;

/// A plan node for computing deltas from a cumulative timeseries.
#[derive(Clone, Debug, PartialEq)]
pub struct Delta {
    pub output: TableSchema,
}

impl Delta {
    pub fn new(schema: &TableSchema) -> anyhow::Result<Self> {
        anyhow::ensure!(
            schema.metric_types.len() == 1,
            "Deltas can only be applied to 1-dimensional tables",
        );
        anyhow::ensure!(
            schema.metric_types[0] == MetricType::Cumulative,
            "Deltas can only be applied to cumulative tables",
        );
        anyhow::ensure!(
            schema.data_types[0].is_numeric()
                || schema.data_types[0].is_distribution(),
            "Deltas can only be applied to numeric or distribution \
            data types, not {}",
            schema.data_types[0],
        );
        let output = TableSchema {
            metric_types: vec![MetricType::Delta],
            ..schema.clone()
        };
        Ok(Self { output })
    }

    pub fn plan_tree_entry(&self) -> termtree::Tree<String> {
        termtree::Tree::new(format!(
            "delta: cumulative -> delta ({})",
            self.output.data_types[0]
        ))
    }
}
