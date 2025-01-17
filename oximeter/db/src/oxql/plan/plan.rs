// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The top-level query plan itself.

// Copyright 2025 Oxide Computer Company

use crate::oxql::ast::table_ops::align;
use crate::oxql::ast::table_ops::BasicTableOp;
use crate::oxql::ast::table_ops::GroupedTableOp;
use crate::oxql::ast::table_ops::TableOp;
use crate::oxql::ast::Query;
use crate::oxql::plan::align::Align;
use crate::oxql::plan::delta::Delta;
use crate::oxql::plan::filter::Filter;
use crate::oxql::plan::get::Get;
use crate::oxql::plan::group_by::GroupBy;
use crate::oxql::plan::join::Join;
use crate::oxql::plan::limit::Limit;
use crate::oxql::plan::node::Node;
use crate::oxql::plan::predicates::Predicates;
use crate::oxql::plan::predicates::SplitPredicates;
use crate::oxql::schema::TableSchema;
use anyhow::Context as _;
use oximeter::TimeseriesName;
use oximeter::TimeseriesSchema;
use oxql_types::point::MetricType;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;
use std::time::Instant;

/// The data for a table operation, either input or output.
#[derive(Clone, Debug, PartialEq)]
pub struct TableOpData {
    /// The table schema.
    pub schema: TableSchema,
    /// The alignment method of the table, if any.
    pub alignment: Option<align::Align>,
}

impl fmt::Display for TableOpData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Table: {}", self.schema.name)?;
        writeln!(
            f,
            "  Metric types: [{}]",
            self.schema
                .metric_types
                .iter()
                .map(|ty| ty.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )?;
        writeln!(
            f,
            "  Data types: [{}]",
            self.schema
                .data_types
                .iter()
                .map(|ty| ty.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )?;
        writeln!(f, "  Fields:")?;
        for (name, ty) in self.schema.fields.iter() {
            writeln!(f, "    {name}: {ty}")?;
        }
        if let Some(alignment) = &self.alignment {
            write!(f, "  Alignment: {:?}", alignment.period)
        } else {
            write!(f, "  Alignment: none")
        }
    }
}

/// The input tables to a table operation.
#[derive(Clone, Debug, PartialEq)]
pub struct TableOpInput {
    pub tables: Vec<TableOpData>,
}

impl From<TableOpInput> for TableOpOutput {
    fn from(input: TableOpInput) -> Self {
        Self { tables: input.tables }
    }
}

/// The output tables from a table operation.
#[derive(Clone, Debug, PartialEq)]
pub struct TableOpOutput {
    pub tables: Vec<TableOpData>,
}

impl fmt::Display for TableOpOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for table in self.tables.iter() {
            writeln!(f, "{table}")?;
        }
        Ok(())
    }
}

impl TableOpOutput {
    /// Convert this output into an input.
    pub fn into_input(self) -> TableOpInput {
        TableOpInput::from(self)
    }

    /// Return the number of tables a table operation emits.
    pub fn n_tables(&self) -> usize {
        self.tables.len()
    }
}

impl From<TableOpOutput> for TableOpInput {
    fn from(output: TableOpOutput) -> Self {
        Self { tables: output.tables }
    }
}

// Helper to distinguish when a plan has changed during optimized.
#[derive(Clone, Debug, PartialEq)]
enum OptimizedPlan {
    Unchanged(Vec<Node>),
    Optimized(Vec<Node>),
}

impl OptimizedPlan {
    // Print this optimized plan as a tree of plan nodes
    fn to_plan_tree(&self) -> termtree::Tree<String> {
        let nodes = match self {
            OptimizedPlan::Unchanged(nodes) => nodes,
            OptimizedPlan::Optimized(nodes) => nodes,
        };
        let mut root =
            termtree::Tree::new(String::from("•")).with_multiline(true);
        Plan::to_plan_tree_impl(&mut root, nodes, /* optimized = */ true);
        root
    }

    // Return the nodes in `self`.
    fn nodes(&self) -> &[Node] {
        match self {
            OptimizedPlan::Unchanged(nodes) => nodes,
            OptimizedPlan::Optimized(nodes) => nodes,
        }
    }

    // Check if the plan has any full table scans.
    fn requires_full_table_scan(&self) -> bool {
        let nodes = match self {
            OptimizedPlan::Unchanged(nodes) => nodes,
            OptimizedPlan::Optimized(nodes) => nodes,
        };
        Self::requires_full_table_scan_impl(nodes)
    }

    // Check if the plan has any full table scans.
    fn requires_full_table_scan_impl(nodes: &[Node]) -> bool {
        nodes.iter().any(|node| match node {
            Node::Subquery(subplans) => {
                subplans.iter().any(Plan::requires_full_table_scan)
            }
            Node::Get(get) => get.filters.is_empty(),
            Node::Delta(_)
            | Node::Filter(_)
            | Node::Align(_)
            | Node::GroupBy(_)
            | Node::Join(_)
            | Node::Limit(_) => false,
        })
    }
}

/// An OxQL query plan.
///
/// The query plan represents the set of database or in-process operations we
/// take to implement the query. These are computed by parsing the original OxQL
/// query; generating a node in the plan for each step; and then possibly
/// rewriting the plan to implement certain kinds of optimizations.
///
/// All query plans start by fetching some set of data from the database,
/// possibly applying some filtering or processing operations in the DB as well.
/// Once data is fetched out of the database, every following step is a table
/// operation implemented in Rust.
#[derive(Clone, Debug)]
pub struct Plan {
    /// The original parsed query.
    pub query: Query,
    // The original query plan.
    nodes: Vec<Node>,
    // The possibly optimized, rewritten query plan.
    optimized: OptimizedPlan,
    /// Time spent generating the query plan.
    pub duration: Duration,
}

impl PartialEq for Plan {
    fn eq(&self, other: &Self) -> bool {
        self.query.eq(&other.query)
            && self.nodes.eq(&other.nodes)
            && self.optimized.eq(&other.optimized)
    }
}

impl Plan {
    /// Generate a plan for the provided query.
    ///
    /// The provided `schema` must include the timeseries schema for any
    /// timeseries referred to in the orginal query. I.e., it lists the schema
    /// for the raw data to be selected from the database.
    pub fn new(
        query: Query,
        schema: &BTreeMap<TimeseriesName, TimeseriesSchema>,
    ) -> anyhow::Result<Self> {
        let start = Instant::now();
        let mut nodes = Vec::with_capacity(query.table_ops().len());
        Self::plan_query(&query, &schema, &mut nodes)?;
        let optimized = Self::optimize_plan(&nodes)?;
        let duration = start.elapsed();
        if let OptimizedPlan::Optimized(optimized_plan) = &optimized {
            let original_output =
                nodes.last().expect("plan cannot be empty").output();
            let optimized_output =
                optimized_plan.last().expect("plan cannot be empty").output();
            assert_eq!(
                original_output, optimized_output,
                "Query optimization resulted in different outputs!\n\
                original = {:?}\n\
                optimized = {:?}",
                original_output, optimized_output,
            );
        }
        Ok(Self { query, nodes, optimized, duration })
    }

    /// Pretty-print the plan as a tree of plan nodes.
    pub fn to_plan_tree(&self) -> (String, String) {
        let mut root =
            termtree::Tree::new(String::from("•")).with_multiline(true);
        Self::to_plan_tree_impl(
            &mut root,
            &self.nodes,
            /* optimized = */ false,
        );
        let original_tree = root.to_string();
        let optimized_tree = self.optimized.to_plan_tree();
        (original_tree, optimized_tree.to_string())
    }

    /// Return the table schema for the output of a plan.
    pub fn output(&self) -> TableOpOutput {
        self.nodes.last().expect("plan cannot be empty").output()
    }

    // Return the new, possibly optimized plan nodes.
    #[cfg(test)]
    pub fn optimized_nodes(&self) -> &[Node] {
        self.optimized.nodes()
    }

    // Return the original, unoptimized plan nodes.
    #[cfg(test)]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    // Push nodes of the plan onto the tree, recursively.
    fn to_plan_tree_impl(
        root: &mut termtree::Tree<String>,
        mut nodes: &[Node],
        optimized: bool,
    ) {
        if let Node::Subquery(plans) = &nodes[0] {
            let subplans = plans.iter().map(|plan| {
                let mut root =
                    termtree::Tree::new(format!("•  {}", plan.query))
                        .with_multiline(true);
                let nodes = if optimized {
                    plan.optimized.nodes()
                } else {
                    &plan.nodes
                };
                Self::to_plan_tree_impl(&mut root, nodes, optimized);
                root
            });
            root.extend(subplans);
            nodes = &nodes[1..];
        }
        for node in nodes.iter() {
            root.push(node.plan_tree_entry());
        }
    }

    // Plan the provided query, inserting the plan nodes into the list.
    fn plan_query(
        query: &Query,
        schema: &BTreeMap<TimeseriesName, TimeseriesSchema>,
        nodes: &mut Vec<Node>,
    ) -> anyhow::Result<()> {
        for table_op in query.table_ops() {
            match table_op {
                TableOp::Basic(basic) => {
                    Self::plan_basic_table_op(&schema, basic, nodes)?
                }
                TableOp::Grouped(grouped) => {
                    Self::plan_subquery(&schema, grouped, nodes)?
                }
            }
        }
        Ok(())
    }

    // Generate plan nodes for a basic table op.
    fn plan_basic_table_op(
        schema: &BTreeMap<TimeseriesName, TimeseriesSchema>,
        op: &BasicTableOp,
        nodes: &mut Vec<Node>,
    ) -> anyhow::Result<()> {
        match op {
            BasicTableOp::Get(name) => {
                let db_schema = schema.get(name).with_context(|| {
                    format!("Schema for timeseries '{}' not found", name)
                })?;
                let table_schema = TableSchema::new(db_schema);
                let node = Node::Get(Get {
                    table_schema: table_schema.clone(),
                    filters: vec![],
                    limit: None,
                });
                nodes.push(node);

                // Insert the plan node for computing deltas for a cumulative
                // timeseries, if needed.
                if table_schema.metric_types[0] == MetricType::Cumulative {
                    let node = Node::Delta(Delta::new(&table_schema)?);
                    nodes.push(node);
                }
            }
            BasicTableOp::Filter(filter) => {
                // A filter doesn't change the schema, but we do have to check
                // that it applies to every input table.
                let input = nodes
                    .last()
                    .expect("Must have a previous node")
                    .output()
                    .into_input();
                let node = Node::Filter(Filter::new(filter, input)?);
                nodes.push(node);
            }
            BasicTableOp::GroupBy(group_by) => {
                // A group_by just drops the fields that are not named, and
                // aggregates the others in a specific way.
                let input = nodes
                    .last()
                    .expect("Must have a previous node")
                    .output()
                    .into_input();
                let node = Node::GroupBy(GroupBy::new(group_by, input)?);
                nodes.push(node);
            }
            BasicTableOp::Join(_) => {
                // A join concatenates all the data from the input tables that
                // have the same values for their fields. All tables have to
                // have the same field names / types.
                let inputs = nodes
                    .last()
                    .expect("Must have a previous node")
                    .output()
                    .into_input();
                let node = Node::Join(Join::new(inputs)?);
                nodes.push(node);
            }
            BasicTableOp::Align(align) => {
                let input = nodes
                    .last()
                    .expect("Must have a previous node")
                    .output()
                    .into_input();
                let node = Node::Align(Align::new(*align, input)?);
                nodes.push(node);
            }
            BasicTableOp::Limit(limit) => {
                // Limit operations do not modify anything about the data at
                // all, so just return the input schema unchanged.
                let output =
                    nodes.last().expect("Must have a previous node").output();
                nodes.push(Node::Limit(Limit { limit: *limit, output }));
            }
        }
        Ok(())
    }

    // Generate a plan node for a grouped table op, i.e., a subquery.
    //
    // Note that subqueries are not optimized themselves. The entire query is
    // optimized as a whole. That means `optimized` is unchanged.
    fn plan_subquery(
        schema: &BTreeMap<TimeseriesName, TimeseriesSchema>,
        grouped: &GroupedTableOp,
        nodes: &mut Vec<Node>,
    ) -> anyhow::Result<()> {
        let mut subplans = Vec::with_capacity(grouped.ops.len());
        for query in grouped.ops.iter().cloned() {
            let start = Instant::now();
            let mut new_nodes = Vec::new();
            Self::plan_query(&query, schema, &mut new_nodes)?;
            subplans.push(Plan {
                query,
                nodes: new_nodes.clone(),
                optimized: OptimizedPlan::Unchanged(new_nodes),
                duration: start.elapsed(),
            });
        }
        nodes.push(Node::Subquery(subplans));
        Ok(())
    }

    // Optimize a plan, if possible.
    //
    // This attempts to apply a number of plan-rewriting steps, to make the
    // query as efficient as possible. Today these steps include:
    //
    // - Predicate pushdown: moving filtering predicates as close to the data as
    // possible
    // - Limit pushdown: Moving `first` or `last` table operations as close to
    // the data as possible.
    //
    // There is a lot of room for new steps here. The most obvious next
    // candidates are:
    //
    // - Deltas: push the computation of deltas from cumulative timeseries into
    // the database
    // - Alignment: push alignment operations into the database
    // - Constant evaluation: As we support richer expressions, such as writing
    // filters like `filter x > 1 / 2` or `filter x > log(5)`, we can evaluate
    // those expressions at query-plan time.
    fn optimize_plan(nodes: &[Node]) -> anyhow::Result<OptimizedPlan> {
        let optimized = Self::pushdown_predicates(nodes)?;
        Self::pushdown_limit(optimized.nodes())
    }

    // Push down limit operations in the list of plan nodes.
    fn pushdown_limit(nodes: &[Node]) -> anyhow::Result<OptimizedPlan> {
        anyhow::ensure!(
            !nodes.is_empty(),
            "Planning error: plan nodes cannot be empty"
        );
        let mut modified = false;

        // Collect nodes in the plan.
        let mut remaining_nodes =
            nodes.iter().cloned().collect::<VecDeque<_>>();
        let mut processed_nodes = VecDeque::with_capacity(nodes.len());

        while let Some(current_node) = remaining_nodes.pop_back() {
            // What we do with the limit node depends on what's in front of it.
            let Some(next_node) = remaining_nodes.pop_back() else {
                // If there _isn't_ one, then the current node must be the start
                // of the plan, and so push it and break out.
                processed_nodes.push_front(current_node);
                break;
            };

            // If this isn't a limit node, just push it and continue. Note that
            // we always pus the next node back onto the list of remaining
            // nodes, to process it on the next pass through the loop.
            let Node::Limit(limit) = current_node else {
                processed_nodes.push_front(current_node);
                remaining_nodes.push_back(next_node);
                continue;
            };

            match next_node {
                Node::Subquery(subplans) => {
                    // Push the limit onto the subquery plans and recurse.
                    let new_subplans = subplans
                        .into_iter()
                        .map(|mut plan| {
                            let start = Instant::now();
                            let nodes = [
                                plan.optimized.nodes(),
                                &[Node::Limit(limit.clone())],
                            ]
                            .concat();
                            plan.optimized = Self::pushdown_limit(&nodes)?;
                            plan.duration += start.elapsed();
                            Ok(plan)
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    processed_nodes.push_front(Node::Subquery(new_subplans));
                    modified = true;
                }
                Node::Get(mut get) => {
                    // If we've gotten here, we _may_ be able to push this into
                    // the databse, but only if there isn't one already in the
                    // get node.
                    match get.limit.as_mut() {
                        Some(existing) => {
                            // There's already a limiting operation here. We may
                            // be able to coalesce them, but only if they're the
                            // same kind. If not, it's not correct to reorder
                            // them, so we have to push the current node and
                            // then the next (get) node onto the processed list.
                            if existing.kind == limit.limit.kind {
                                // Take the smaller of the two!
                                existing.count =
                                    existing.count.min(limit.limit.count);
                                modified = true;
                            } else {
                                // These are different kinds, push them in
                                // order, starting with the limit.
                                processed_nodes.push_front(Node::Limit(limit));
                            }
                        }
                        None => {
                            let old = get.limit.replace(limit.limit);
                            assert!(old.is_none());
                            modified = true;
                        }
                    }

                    // We always push the get node last.
                    processed_nodes.push_front(Node::Get(get));
                }
                Node::Align(_) => {
                    // An alignment operation changes the number of elements, so
                    // pushing a limit through it is not valid. Push both in
                    // order onto the list of processed nodes.
                    processed_nodes.push_front(Node::Limit(limit));
                    processed_nodes.push_front(next_node);
                }
                Node::Delta(_) | Node::GroupBy(_) | Node::Join(_) => {
                    remaining_nodes.push_back(Node::Limit(limit));
                    processed_nodes.push_front(next_node);
                    modified = true;
                }
                Node::Filter(filter) => {
                    // We might be able to reorder the limit through the filter,
                    // but it depends on whether and how the filter references
                    // timestamps. They need to point in the same "direction" --
                    // see `can_reorder_around()` for details.
                    if filter.can_reorder_around(&limit.limit) {
                        processed_nodes.push_front(Node::Filter(filter));
                        remaining_nodes.push_back(Node::Limit(limit));
                        modified = true;
                    } else {
                        processed_nodes.push_front(Node::Limit(limit));
                        processed_nodes.push_front(Node::Filter(filter));
                    }
                }
                Node::Limit(mut other_limit) => {
                    // We might be able to coalesce these, if they're of the
                    // same kind. If they are not, push the current one, and
                    // then start carrying through the next one.
                    if limit.limit.kind == other_limit.limit.kind {
                        other_limit.limit.count =
                            other_limit.limit.count.min(limit.limit.count);
                        remaining_nodes.push_back(Node::Limit(other_limit));
                        modified = true;
                    } else {
                        processed_nodes.push_front(Node::Limit(limit));
                        remaining_nodes.push_back(Node::Limit(other_limit));
                    }
                }
            }
        }

        let out = processed_nodes.make_contiguous().to_vec();
        if modified {
            Ok(OptimizedPlan::Optimized(out))
        } else {
            Ok(OptimizedPlan::Unchanged(out))
        }
    }

    // Push down predicates in the list of plan nodes.
    fn pushdown_predicates(nodes: &[Node]) -> anyhow::Result<OptimizedPlan> {
        Self::pushdown_predicates_impl(nodes, None)
    }

    // Recursive implementation of predicate pushdown.
    fn pushdown_predicates_impl(
        nodes: &[Node],
        outer_predicates: Option<Predicates>,
    ) -> anyhow::Result<OptimizedPlan> {
        anyhow::ensure!(
            !nodes.is_empty(),
            "Planning error: plan nodes cannot be empty"
        );

        // Used to return correct variant of `OptimizedPlan`.
        let mut modified = false;

        // Collect the nodes in the plan.
        //
        // We'll process the query plan from back to front, pushing nodes onto
        // the output plan as we consider them. This is so we can "push" the
        // filters from the back of the plan towards the front, as we process
        // nodes.
        let mut remaining_nodes =
            nodes.iter().cloned().collect::<VecDeque<_>>();
        let mut processed_nodes = VecDeque::with_capacity(nodes.len());

        // If we were provided with outer predicates, let's just add on a filter
        // plan node to the provided plan. It will be processed like any other
        // node.
        if let Some(predicates) = outer_predicates {
            let filt = Filter::from_predicates(
                predicates,
                nodes
                    .last()
                    .expect("length verified above")
                    .output()
                    .into_input(),
            )?;
            remaining_nodes.push_back(Node::Filter(filt));
        };

        // Process nodes in the query plan, in reverse.
        while let Some(current_node) = remaining_nodes.pop_back() {
            // What we do with the node depends on what's in front of it, so
            // take the next node in the plan.
            let Some(next_node) = remaining_nodes.pop_back() else {
                // If there _isn't_ one, then the current node must be the start
                // of the plan, and so push it and break out.
                processed_nodes.push_front(current_node);
                break;
            };

            // If the current node isn't a filter, then this pass doesn't modify
            // it at all. Instead, just push it and continue. But! We have to
            // push back the next node we just popped above.
            let Node::Filter(current_filter) = current_node else {
                processed_nodes.push_front(current_node);
                remaining_nodes.push_back(next_node);
                continue;
            };

            // At this point we are looking at a filter node, and the next one
            // exists. We might be able to modify them in a few ways.
            match next_node {
                Node::Subquery(subplans) => {
                    // We can push the filter into each one of the subquery
                    // plans, as its outer predicates.
                    let new_subplans = subplans
                        .into_iter()
                        .map(|mut plan| {
                            let start = Instant::now();
                            plan.optimized = Self::pushdown_predicates_impl(
                                &plan.nodes,
                                Some(current_filter.predicates.clone()),
                            )?;
                            plan.duration += start.elapsed();
                            Ok(plan)
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    processed_nodes.push_front(Node::Subquery(new_subplans));
                    modified = true;
                }
                Node::Get(mut get) => {
                    // Push the filters into the get plan node.
                    //
                    // At this point, we're at the front of the query plan, so
                    // the filters won't be pushed any farther. It's required
                    // that every filter we push through be _non-empty_. See
                    // `Filter` for details.
                    get.filters = current_filter.predicates.to_required()?;
                    processed_nodes.push_front(Node::Get(get));
                    modified = true;
                }
                Node::Delta(ref delta) => {
                    // We can _sometimes_ push a filter around a delta, but not
                    // the entire filter expression in general.
                    //
                    // Delta nodes change the datum values for a cumulative
                    // timeseries, by computing adjacent differences. In a
                    // logical, unoptimized query plan, we always fetch the
                    // timeseries and immediately (and implicitly) construct the
                    // deltas. So to match the semantics of the literal query,
                    // any filter on the datum must necessarily refer to those
                    // differences, not the original, cumulative values.
                    //
                    // That means we can push through filter expressions, except
                    // those portions which refer to the datum. Field filters
                    // and any filters on the timestamps are valid as well. Note
                    // that the latter are valid because computing differences
                    // doesn't change the timestamps themselves, only the start
                    // times.
                    let SplitPredicates { pushed, not_pushed } =
                        current_filter.predicates.split_around_delta(delta)?;
                    if let Some(after) = not_pushed {
                        let new_input = next_node.output().into_input();
                        let filter = Filter::from_predicates(after, new_input)?;
                        processed_nodes.push_front(Node::Filter(filter));
                    }
                    processed_nodes.push_front(next_node);

                    // The filters we did push through need to go back on the
                    // remaining node queue, with the next node's output as its
                    // input.
                    if let Some(before) = pushed {
                        let new_input = remaining_nodes
                            .back()
                            .as_ref()
                            .expect("Delta nodes cannot start a query")
                            .output()
                            .into_input();
                        let filter =
                            Filter::from_predicates(before, new_input.clone())?;
                        remaining_nodes.push_back(Node::Filter(filter));
                    }
                    modified = true;
                }
                Node::Filter(mut next_filter) => {
                    // If the next node is also a filter, we can just merge the
                    // current one into it, and push it back onto the remaining
                    // nodes. We'll process it again on the next pass through
                    // the loop.
                    let new_predicates = next_filter
                        .predicates
                        .and(&current_filter.predicates)?;
                    next_filter.predicates = new_predicates;
                    remaining_nodes.push_back(Node::Filter(next_filter));
                    modified = true;
                }
                Node::Align(ref align) => {
                    // We can also push a filter around an alignment operation,
                    // but we may need to split it up.
                    //
                    // Filters that apply to the _fields_ can always be pushed,
                    // but the filters that apply to the measurements cannot.
                    // Separate these into a new set of filter nodes that come
                    // before and after the alignment, respectively.
                    let SplitPredicates { pushed, not_pushed } =
                        current_filter.predicates.split_around_align(align)?;
                    if let Some(after) = not_pushed {
                        let new_input = next_node.output().into_input();
                        let filter = Filter::from_predicates(after, new_input)?;
                        processed_nodes.push_front(Node::Filter(filter));
                    }
                    processed_nodes.push_front(next_node);

                    // The filters we did push through need to go back on the
                    // remaining node queue, with the next node's output as its
                    // input.
                    if let Some(before) = pushed {
                        let new_input = remaining_nodes
                            .back()
                            .as_ref()
                            .expect("Align cannot start a query")
                            .output()
                            .into_input();
                        let filter =
                            Filter::from_predicates(before, new_input.clone())?;
                        remaining_nodes.push_back(Node::Filter(filter));
                    }
                    modified = true;
                }
                Node::GroupBy(_) => {
                    // We can always push a filter around a group by, but it's a
                    // bit subtle.
                    //
                    // At this point in the plan creation, we have checked that
                    // the filter node we're operating on only refers to fields
                    // that are valid at this point in the query. That can only
                    // be fields that are named in the group-by table operation!
                    // Any of those can be pushed through without changing the
                    // contents of the group. We do need to change the input
                    // schema the filter considers, however, since the groupby
                    // operation probably has a different one.
                    processed_nodes.push_front(next_node);
                    let input = remaining_nodes
                        .back()
                        .expect("group_by cannot start a query")
                        .output()
                        .into_input();
                    let new_filter = Node::Filter(
                        Filter::from_predicates(
                            current_filter.predicates.clone(),
                            input,
                        )
                        .context("planning error")?,
                    );
                    remaining_nodes.push_back(new_filter);
                    modified = true;
                }
                Node::Join(_) => {
                    // We can also always push a filter around a join operation.
                    //
                    // TODO-completeness: It would be very nice to figure out
                    // how to refer to `datum`s in joins. Right now, we would
                    // apply the filter to both, which is probably nonsensical.
                    // We should consider letting folks name the metric portion
                    // of the timeseries name / table name, in addition to the
                    // special identifier `datum`.
                    //
                    // See https://github.com/oxidecomputer/omicron/issues/6761.
                    processed_nodes.push_front(next_node);
                    let input = remaining_nodes
                        .back()
                        .expect("join cannot start a query")
                        .output()
                        .into_input();
                    let new_filter = Node::Filter(
                        Filter::from_predicates(
                            current_filter.predicates.clone(),
                            input,
                        )
                        .context("planning error")?,
                    );
                    remaining_nodes.push_back(new_filter);
                    modified = true;
                }
                Node::Limit(limit) => {
                    // We _might_ be able to reorder the filter around the
                    // limit, in a few cases. See `can_reorder_around()` for
                    // details.
                    if current_filter
                        .predicates
                        .can_reorder_around(&limit.limit)
                    {
                        // Push the limit node onto the output plan, and then
                        // push the filter back on the remaining nodes.
                        processed_nodes.push_front(Node::Limit(limit));
                        remaining_nodes.push_back(Node::Filter(current_filter));
                        modified = true;
                    } else {
                        // We can't reorder these -- push the filter, and then
                        // put the limit back so we can process it on the next
                        // pass.
                        processed_nodes.push_back(Node::Filter(current_filter));
                        remaining_nodes.push_front(Node::Limit(limit));
                    }
                }
            }
        }

        // If this pass modified anything, return the new plan, else return
        // nothing.
        let output_nodes = processed_nodes.make_contiguous().to_vec();
        if modified {
            Ok(OptimizedPlan::Optimized(output_nodes))
        } else {
            Ok(OptimizedPlan::Unchanged(output_nodes))
        }
    }

    /// Return true if this plan requires at least 1 full table scan.
    pub fn requires_full_table_scan(&self) -> bool {
        self.optimized.requires_full_table_scan()
    }
}

#[cfg(test)]
pub(super) mod test_utils {
    use chrono::NaiveDateTime;
    use oximeter::FieldSchema;
    use oximeter::TimeseriesName;
    use oximeter::TimeseriesSchema;
    use std::collections::BTreeMap;
    use tokio::sync::OnceCell;

    const FILE: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/timeseries-schema.json");

    /// Type representing the direct JSON output of the timeseries_schema table.
    #[derive(Clone, Debug, serde::Deserialize)]
    struct DbTimeseriesSchema {
        timeseries_name: TimeseriesName,
        #[serde(rename = "fields.name")]
        field_names: Vec<String>,
        #[serde(rename = "fields.type")]
        field_types: Vec<String>,
        #[serde(rename = "fields.source")]
        field_sources: Vec<String>,
        datum_type: String,
        created: String,
    }

    impl From<DbTimeseriesSchema> for TimeseriesSchema {
        fn from(value: DbTimeseriesSchema) -> Self {
            assert_eq!(value.field_names.len(), value.field_types.len());
            assert_eq!(value.field_names.len(), value.field_sources.len());
            let field_schema = value
                .field_names
                .into_iter()
                .zip(value.field_types)
                .zip(value.field_sources)
                .map(|((name, field_type), source)| FieldSchema {
                    name,
                    field_type: field_type.parse().unwrap(),
                    source: source.parse().unwrap(),
                    description: String::new(),
                })
                .collect();
            Self {
                timeseries_name: value.timeseries_name,
                description: Default::default(),
                field_schema,
                datum_type: value.datum_type.parse().unwrap(),
                version: std::num::NonZeroU8::new(1).unwrap(),
                authz_scope: oximeter::AuthzScope::Fleet,
                units: oximeter::Units::None,
                created: NaiveDateTime::parse_from_str(
                    &value.created,
                    "%Y-%m-%d %H:%M:%S%.9f",
                )
                .unwrap()
                .and_utc(),
            }
        }
    }

    async fn load_schema() -> BTreeMap<TimeseriesName, TimeseriesSchema> {
        let contents = tokio::fs::read_to_string(&FILE).await.unwrap();
        contents
            .lines()
            .map(|line| {
                let schema: DbTimeseriesSchema =
                    serde_json::from_str(&line).unwrap();
                (
                    TimeseriesName::try_from(schema.timeseries_name.as_str())
                        .unwrap(),
                    schema.into(),
                )
            })
            .collect()
    }

    static ALL_SCHEMA: OnceCell<BTreeMap<TimeseriesName, TimeseriesSchema>> =
        OnceCell::const_new();

    pub async fn all_schema(
    ) -> &'static BTreeMap<TimeseriesName, TimeseriesSchema> {
        ALL_SCHEMA.get_or_init(load_schema).await
    }
}

#[cfg(test)]
mod tests {
    use crate::oxql::ast::grammar::query_parser;
    use crate::oxql::plan::node::Node;
    use crate::oxql::plan::plan::test_utils::all_schema;
    use crate::oxql::plan::Plan;
    use oxql_types::point::DataType;
    use oxql_types::point::MetricType;

    #[tokio::test]
    async fn get_gauge_plan_emits_one_node() {
        const TIMESERIES_NAME: &str = "collection_target:cpus_provisioned";
        let query =
            query_parser::query(&format!("get {TIMESERIES_NAME}")).unwrap();
        let all_schema = all_schema().await;
        let plan = Plan::new(query, &all_schema).unwrap();

        assert_eq!(
            plan.nodes.len(),
            1,
            "getting a single table by name with no filters and \
            a gauge metric type should result in one table operation",
        );
        let Node::Get(get) = &plan.nodes[0] else {
            panic!("expected just a get plan node, found {:?}", plan.nodes[0]);
        };
        let original_schema =
            all_schema.get(&TIMESERIES_NAME.parse().unwrap()).unwrap();
        assert_eq!(get.table_schema.name, TIMESERIES_NAME);
        assert_eq!(get.table_schema.metric_types, &[MetricType::Gauge]);
        assert_eq!(get.table_schema.data_types, &[DataType::Integer]);
        let mut n_seen = 0;
        for field_schema in original_schema.field_schema.iter() {
            let ty = get
                .table_schema
                .fields
                .get(&field_schema.name)
                .expect("Field should be found");
            assert_eq!(ty, &field_schema.field_type);
            n_seen += 1;
        }
        assert_eq!(n_seen, get.table_schema.fields.len());
    }

    #[tokio::test]
    async fn get_cumulative_plan_emits_two_nodes() {
        const TIMESERIES_NAME: &str = "physical_data_link:bytes_sent";
        let query =
            query_parser::query(&format!("get {TIMESERIES_NAME}")).unwrap();
        let all_schema = all_schema().await;
        let plan = Plan::new(query, &all_schema).unwrap();

        assert_eq!(
            plan.nodes.len(),
            2,
            "getting a single table by name with no filters and \
            a cumulative metric type should result in two table operations",
        );
        let Node::Get(get) = &plan.nodes[0] else {
            panic!("expected a get plan node, found {:?}", plan.nodes[0]);
        };
        let original_schema =
            all_schema.get(&TIMESERIES_NAME.parse().unwrap()).unwrap();
        assert_eq!(get.table_schema.name, TIMESERIES_NAME);
        assert_eq!(get.table_schema.metric_types, &[MetricType::Cumulative]);
        assert_eq!(get.table_schema.data_types, &[DataType::Integer]);
        let mut n_seen = 0;
        for field_schema in original_schema.field_schema.iter() {
            let ty = get
                .table_schema
                .fields
                .get(&field_schema.name)
                .expect("Field should be found");
            assert_eq!(ty, &field_schema.field_type);
            n_seen += 1;
        }
        assert_eq!(n_seen, get.table_schema.fields.len());

        let Node::Delta(delta) = &plan.nodes[1] else {
            panic!("expected a delta plan node, found {:?}", plan.nodes[0]);
        };
        assert_eq!(delta.output.name, get.table_schema.name);
        assert_eq!(delta.output.fields, get.table_schema.fields);
        assert_eq!(delta.output.data_types, get.table_schema.data_types);
        assert_eq!(delta.output.metric_types[0], MetricType::Delta);
    }

    #[tokio::test]
    async fn subquery_plan_returns_multiple_tables() {
        let query = query_parser::query(
            "{ \
                get physical_data_link:bytes_received; \
                get physical_data_link:bytes_received \
            }",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        assert_eq!(
            plan.nodes.len(),
            1,
            "Outer query plan should have one node"
        );
        let Node::Subquery(plans) = &plan.nodes[0] else {
            panic!(
                "Plan should have generated a subquery node, found {:?}",
                plan.nodes[0],
            );
        };
        assert_eq!(plans.len(), 2, "Should have generated two subquery plans",);
        assert_eq!(plans[0], plans[1], "Both subquery plans should be equal");
        let nodes = &plans[0].nodes;
        assert_eq!(nodes.len(), 2, "Each subquery should have 2 nodes");
        assert!(matches!(nodes[0], Node::Get(_)));
        assert!(matches!(nodes[1], Node::Delta(_)));

        let output = plan.output();
        assert_eq!(
            output.tables.len(),
            2,
            "Query plan with 2 suqueries should emit 2 tables"
        );
        assert_eq!(output.tables[0], output.tables[1]);
    }

    #[tokio::test]
    async fn group_by_plan_leaves_only_grouped_fields() {
        let query = query_parser::query(
            "get physical_data_link:bytes_received \
                | align mean_within(20s) \
                | group_by [sled_id, serial]",
        )
        .unwrap();
        let all_schema = all_schema().await;
        let plan = Plan::new(query, all_schema).unwrap();
        assert_eq!(
            plan.nodes.len(),
            4,
            "Should have four nodes (+1 is for implicit delta)"
        );
        let Node::GroupBy(group_by) = plan.nodes.last().unwrap() else {
            panic!(
                "expected a group_by as the last node, found {:?}",
                plan.nodes.last().unwrap(),
            );
        };
        let output = &group_by.output;
        let plan_output = plan.output();
        assert_eq!(plan_output.tables.len(), 1);
        assert_eq!(
            &plan_output.tables[0], output,
            "plan output should be the output of the last node"
        );
        assert_eq!(
            output.schema.fields.len(),
            2,
            "group_by should have only resulted in two fields, \
            those listed in the table operation"
        );
        let timeseries_name =
            "physical_data_link:bytes_received".parse().unwrap();
        for field in ["sled_id", "serial"] {
            let output_type = output.schema.fields[field];
            let original_type = all_schema
                .get(&timeseries_name)
                .unwrap()
                .field_schema
                .iter()
                .find_map(|s| {
                    if s.name == field {
                        Some(s.field_type)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert_eq!(
                output_type, original_type,
                "group_by operation should not change field types"
            );
        }
    }

    #[tokio::test]
    async fn filter_plan_node_does_not_change_table_schema() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | filter serial == 'foo'",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        assert_eq!(plan.nodes.len(), 3);
        let input = plan.nodes[1].output();
        let output = plan.output();
        assert_eq!(input, output, "Filter should not change the table schema");
    }

    #[tokio::test]
    async fn limit_plan_node_does_not_change_table_schema() {
        let query =
            query_parser::query("get physical_data_link:bytes_sent | last 1")
                .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        assert_eq!(plan.nodes.len(), 3);
        let input = plan.nodes[1].output();
        let output = plan.output();
        assert_eq!(input, output, "Last should not change the table schema");
    }

    #[tokio::test]
    async fn cannot_group_multiple_tables() {
        let query = query_parser::query(
            "{ \
                get physical_data_link:bytes_sent; \
                get physical_data_link:bytes_received \
             } \
                | align mean_within(10s) \
                | group_by [sled_id]",
        )
        .unwrap();
        let err = Plan::new(query, all_schema().await).expect_err(
            "Should fail to plan query that groups multiple tables",
        );
        assert!(
            err.to_string().contains("require exactly one input table"),
            "Error message should complain about multiple tables, \
            but the error message is: {:#?}",
            err
        );
    }

    #[tokio::test]
    async fn cannot_join_one_table() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent \
                | align mean_within(10s) \
                | join",
        )
        .unwrap();
        let err = Plan::new(query, all_schema().await)
            .expect_err("Should fail to plan query that joins one tables");
        assert!(
            err.to_string().contains("require at least 2 tables"),
            "Error message should complain about only having one \
            table, but the error message is: {:#?}",
            err
        );
    }

    #[tokio::test]
    async fn cannot_join_unaligned_tables() {
        let query = query_parser::query(
            "{ \
                get physical_data_link:bytes_sent; \
                get physical_data_link:bytes_received \
             } | join",
        )
        .unwrap();
        let err = Plan::new(query, all_schema().await).expect_err(
            "Should fail to plan query that joins unaligned tables",
        );
        assert!(
            err.to_string().contains("is not aligned"),
            "Error message should complain that the input tables are \
            not aligned, but the error message is: {:#?}",
            err
        );
    }

    #[tokio::test]
    async fn cannot_group_unaligned_tables() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | group_by [sled_id]",
        )
        .unwrap();
        let err = Plan::new(query, all_schema().await).expect_err(
            "Should fail to plan query that groups unaligned tables",
        );
        assert!(
            err.to_string().contains("is not aligned"),
            "Error message should complain that the input tables are \
            not aligned, but the error message is: {:#?}",
            err
        );
    }

    #[tokio::test]
    async fn cannot_align_non_numeric_tables() {
        let query = query_parser::query(
            "get http_service:request_latency_histogram | align mean_within(10s)"
        ).unwrap();
        let err = Plan::new(query, all_schema().await)
            .expect_err("Should fail to plan query that aligns histograms");
        assert!(
            err.to_string().contains("cannot be aligned"),
            "Error message should complain that histogram tables cannot \
            be aligned, but the error message is: {:#?}",
            err,
        );
    }

    #[tokio::test]
    async fn cannot_filter_with_incomparable_types() {
        let query = query_parser::query(
            "get http_service:request_latency_histogram | filter name == 0",
        )
        .unwrap();
        let err = Plan::new(query, all_schema().await).expect_err(
            "Should fail to plan query with an incomparable filter",
        );
        assert!(
            err.to_string().contains("is not compatible with the expected type"),
            "Error message should complain that a filter cannot compare \
            a field against an incompatible type, but the error message is: {:#?}",
            err,
        );
    }

    #[tokio::test]
    async fn predicate_pushdown_merges_neighboring_filter_nodes() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent \
                | filter serial == 'foo' \
                | filter link_name == 'bar'",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let optimized_nodes = plan.optimized_nodes();
        assert_eq!(optimized_nodes.len(), 2);
        let Node::Get(get) = &optimized_nodes[0] else {
            panic!("Expected a get node, found {:?}", &optimized_nodes[0]);
        };
        assert_eq!(get.filters.len(), 1);
        assert!(matches!(&optimized_nodes[1], Node::Delta(_)));
        assert_eq!(
            plan.nodes.last().unwrap().output(),
            optimized_nodes.last().unwrap().output()
        );
    }

    #[tokio::test]
    async fn predicate_pushdown_pushes_filter_nodes_through_group_by() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent \
                | align mean_within(1m) \
                | group_by [serial] \
                | filter serial == 'foo'",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let optimized_nodes = plan.optimized_nodes();
        assert_eq!(optimized_nodes.len(), 4);
        let Node::Get(get) = &optimized_nodes[0] else {
            panic!("Expected a get node, found {:?}", &optimized_nodes[0]);
        };
        assert_eq!(get.filters.len(), 1);
        assert_eq!(
            plan.nodes.last().unwrap().output(),
            optimized_nodes.last().unwrap().output()
        );
    }

    #[tokio::test]
    async fn predicate_pushdown_pushes_filter_nodes_through_join() {
        let query = query_parser::query(
            "{ \
                get physical_data_link:bytes_sent; \
                get physical_data_link:bytes_received \
            } \
                | align mean_within(1m) \
                | join
                | filter serial == 'foo'",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let optimized_nodes = plan.optimized_nodes();
        assert_eq!(optimized_nodes.len(), 3);
        let Node::Subquery(subqueries) = &optimized_nodes[0] else {
            panic!("Expected a subquery node, found {:?}", &optimized_nodes[0]);
        };
        for subq in subqueries.iter() {
            let nodes = subq.optimized_nodes();
            assert_eq!(nodes.len(), 2);
            let Node::Get(get) = &nodes[0] else {
                panic!("Expected a get node, found {:?}", &nodes[0]);
            };
            assert_eq!(get.filters.len(), 1);
            assert!(matches!(&nodes[1], Node::Delta(_)));
        }
        assert!(matches!(&optimized_nodes[1], Node::Align(_)));
        assert!(matches!(&optimized_nodes[2], Node::Join(_)));
        assert_eq!(
            plan.nodes.last().unwrap().output(),
            optimized_nodes.last().unwrap().output()
        );
    }

    #[tokio::test]
    async fn predicate_pushdown_pushes_filter_nodes_into_subqueries() {
        let query = query_parser::query(
            "{ \
                get physical_data_link:bytes_sent; \
                get physical_data_link:bytes_received \
            } | filter serial == 'foo'",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let optimized_nodes = plan.optimized_nodes();
        assert_eq!(optimized_nodes.len(), 1);
        let Node::Subquery(subqueries) = &optimized_nodes[0] else {
            panic!("Expected a subquery node, found {:?}", &optimized_nodes[0]);
        };
        for subq in subqueries.iter() {
            let nodes = subq.optimized_nodes();
            assert_eq!(nodes.len(), 2);
            let Node::Get(get) = &nodes[0] else {
                panic!("Expected a get node, found {:?}", &nodes[0]);
            };
            assert_eq!(get.filters.len(), 1);
            assert!(matches!(&nodes[1], Node::Delta(_)));
        }
        assert_eq!(
            plan.nodes.last().unwrap().output(),
            optimized_nodes.last().unwrap().output()
        );
    }

    #[tokio::test]
    async fn requires_full_table_scan() {
        let query =
            query_parser::query("get physical_data_link:bytes_sent").unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        assert!(plan.requires_full_table_scan());

        let query = query_parser::query(
            "get physical_data_link:bytes_sent | filter link_name == 'foo'",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        assert!(!plan.requires_full_table_scan());
    }

    #[tokio::test]
    async fn limit_pushdown_does_not_reorder_around_align() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | align mean_within(10m) | first 10"
        ).unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let Node::Get(get) = &plan.optimized_nodes()[0] else {
            unreachable!();
        };
        assert!(
            get.limit.is_none(),
            "Limit should not be pushed through alignment"
        );
    }
}
