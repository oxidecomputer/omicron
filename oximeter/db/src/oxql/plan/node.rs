// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nodes in an OxQL query plan.

// Copyright 2024 Oxide Computer Company

use crate::oxql::plan::align::Align;
use crate::oxql::plan::delta::Delta;
use crate::oxql::plan::filter::Filter;
use crate::oxql::plan::get::Get;
use crate::oxql::plan::group_by::GroupBy;
use crate::oxql::plan::join::Join;
use crate::oxql::plan::limit::Limit;
use crate::oxql::plan::plan::TableOpData;
use crate::oxql::plan::plan::TableOpOutput;
use crate::oxql::plan::Plan;

/// A node in the query plan.
///
/// This is roughly synonymous with the table operation, but includes more
/// metadata about inputs and outputs for the operation. In addition, some query
/// plan nodes do not have direct analogs in a query itself. For example, a
/// `Delta` node is inserted automatically after fetching data from a cumulative
/// table.
#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    /// A node representing the plan of a subquery.
    Subquery(Vec<Plan>),
    /// A node for fetching data from the database.
    Get(Get),
    /// A node for computing deltas between adjacent samples for a cumulative
    /// table.
    ///
    /// This node is always inserted implicitly after a `Get`, if the table
    /// being selected is cumulative.
    Delta(Delta),
    /// A node for filtering data from its inputs.
    Filter(Filter),
    /// A node for aligning its input data to regular time intervals.
    Align(Align),
    /// A node for grouping timeseries within a table with the same field
    /// values.
    GroupBy(GroupBy),
    /// A node for joining timeseries with the same field values from two or
    /// more tables.
    Join(Join),
    /// A node that limits the number of points the timeseries of its input
    /// tables.
    Limit(Limit),
}

impl Node {
    /// Return the output of a query plan node.
    pub fn output(&self) -> TableOpOutput {
        match self {
            Node::Subquery(subplans) => {
                // The output of a subquery is the last output from every
                // subplan in the subquery.
                let tables = subplans
                    .iter()
                    .flat_map(|plan| plan.output().tables)
                    .collect();
                TableOpOutput { tables }
            }
            Node::Get(Get { table_schema, .. }) => TableOpOutput {
                tables: vec![TableOpData {
                    schema: table_schema.clone(),
                    alignment: None,
                }],
            },
            Node::Delta(Delta { output }) => TableOpOutput {
                tables: vec![TableOpData {
                    schema: output.clone(),
                    alignment: None,
                }],
            },
            Node::Filter(Filter { input, .. }) => {
                TableOpOutput { tables: input.tables.clone() }
            }
            Node::Align(Align { output, .. }) => output.clone(),
            Node::GroupBy(GroupBy { output, .. }) => {
                TableOpOutput { tables: vec![output.clone()] }
            }
            Node::Join(Join { output, .. }) => {
                TableOpOutput { tables: vec![output.clone()] }
            }
            Node::Limit(Limit { output, .. }) => output.clone(),
        }
    }

    /// Return a string summarizing a node as a plan tree entry.
    ///
    /// # Panics
    ///
    /// This panics for subquery plans, that should be printed separately using
    /// the recurisve `to_plan_tree_impl()` method.
    pub fn plan_tree_entry(&self) -> termtree::Tree<String> {
        match self {
            Node::Subquery(_) => unreachable!(),
            Node::Get(get) => get.plan_tree_entry(),
            Node::Delta(delta) => delta.plan_tree_entry(),
            Node::Filter(filter) => filter.plan_tree_entry(),
            Node::Align(align) => align.plan_tree_entry(),
            Node::GroupBy(group_by) => group_by.plan_tree_entry(),
            Node::Join(_) => termtree::Tree::new(String::from("join")),
            Node::Limit(limit) => {
                termtree::Tree::new(format!("{}", limit.limit))
            }
        }
    }
}
