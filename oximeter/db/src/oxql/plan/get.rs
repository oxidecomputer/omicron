// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The `get` plan node, for fetching data from the ClickHouse database.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::table_ops::filter;
use crate::oxql::schema::TableSchema;

/// A node for fetching data from the named timeseries in the database.
#[derive(Clone, Debug, PartialEq)]
pub struct Get {
    /// The table schema we're fetching from.
    ///
    /// This is really a _timeseries_ schema, selecting the named timeseries
    /// from the database. These two are synonymous at this point in a plan
    /// tree, but we use the table schema for consistency with other plan nodes.
    pub table_schema: TableSchema,

    /// The filters applied to the database table for this schema.
    ///
    /// There is one entry here for every disjunction in the filters that we
    /// ultimately push down into the get operation. E.g., for a filter like
    /// `filter (x == 0 || x == 1)`, there will be two entries here, `filter (x
    /// == 0)` and `filter x == 1`. These are used to construct "consistent key
    /// groups", sets of timeseries keys that can all be fetched in one
    /// combination of (field SQL query, measurements SQL query).
    pub filters: Vec<filter::Filter>,
}

impl Get {
    /// Convert this node into an entry in a plan tree.
    pub fn plan_tree_entry(&self) -> termtree::Tree<String> {
        // Push each consistent key group as a child tree.
        let mut subtrees = Vec::with_capacity(self.filters.len());
        for (i, filter) in self.filters.iter().enumerate() {
            let mut subtree = termtree::Tree::new(format!("key group {i}"))
                .with_multiline(true);
            let mut is_full_scan = false;
            subtree.push(format!(
                "field filters={}",
                filter
                    .rewrite_for_field_tables(&self.table_schema)
                    .unwrap()
                    .unwrap_or_else(|| {
                        is_full_scan = true;
                        String::from("[]")
                    })
            ));
            subtree.push(format!(
                "measurement filters={}",
                filter
                    .rewrite_for_measurement_table(&self.table_schema)
                    .unwrap()
                    .unwrap_or_else(|| {
                        is_full_scan = true;
                        String::from("[]")
                    })
            ));
            subtree.push(format!(
                "full scan: {}",
                if is_full_scan { "YES" } else { "no" }
            ));
            subtrees.push(subtree);
        }

        // An empty GET node is always a full scan.
        if subtrees.is_empty() {
            subtrees.push(termtree::Tree::new(format!("full scan: YES",)));
        }

        termtree::Tree::new(format!("get: \"{}\"", self.table_schema.name))
            .with_multiline(true)
            .with_leaves(subtrees)
    }
}
