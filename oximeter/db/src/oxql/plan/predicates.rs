// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Operate on predicates in a query plan, including spliting, merging and
//! reordering.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::table_ops::filter;
use crate::oxql::ast::table_ops::limit::Limit;
use crate::oxql::plan::align::Align;
use crate::oxql::plan::delta::Delta;
use crate::oxql::plan::filter::Filter;

/// Predicates in an OxQL plan node that filters data.
///
/// # Overview
///
/// During query planning, we try to manipulate the predicates to push as much
/// as possible close to the data. This is called "predicate pushdown". For a
/// variety of reasons, it's not always straightforward to do this. For example,
/// some filters only apply to data that we process in Rust, not the database.
/// This includes queries like:
///
/// align mean_within(1m) | filter field == 0 && datum > 100
///
/// In this case, the field predicate can be pushed into the database, but it's
/// not correct to push the filter on the datum. That only applies to the
/// _aligned_ data, which is only computed in Rust, after fetching the data from
/// the database.
///
/// In this particular case, it's fairly straightforward to split these two.
/// Because they are joined by a logical AND, this is equivalent to:
///
/// filter field == 0 | align mean_within(1m) | filter datum > 100
///
/// And that's exactly what we do: push the field filters (and timestamps,
/// though it's less obvious that that's correct), and keep the datum filters on
/// the "outside" of the alignment.
///
/// # Disjunctions
///
/// This all becomes vastly more complicated in the presence of disjunctions,
/// statements joined by logical ORs. Consider this:
///
/// align mean_within(1m) | filter field == 0 || datum > 100
///
/// In this case, pushing the field filter through is not correct! We actually
/// want _all the data_ to make its way through the alignment operation.
/// However, for scalability, we disallow that kind of "full table scan". But we
/// can still show the complexities of disjunctions with a filter like:
///
/// filter (field == 0) || (field == 1 && datum > 100)
///
/// In this case, we want to push these two disjunctions _separately_ through
/// the alignment. However, we now need to match up the predicates we can push
/// through with those we can't, so we don't accidentally end up applying the
/// `datum > 100` predicate to the `field == 0` data! So we now need to maintain
/// a _list_ of predicates, with those pieces we've pushed through and those we
/// haven't, so we can match them up.
///
/// This enum is used to keep track of the predicates in a query plan, as we
/// perform these kinds of operations. Continuing with the above example, the
/// query plan would look something like this:
///
/// 1. First, we construct predicates from the single query AST node
///    representing the filter. That is, we take `filter (field == 0) || (field
///    == 1 && datum > 100)`, and we construct:
///
///    ```rust,ignore
///    Predicates::Single("(field == 0) || (field == 1 && datum > 100)")
///    ```
///
/// 2. When we push this through an alignment operation, we will split them into
///    two other variants. Those moving through the aligment are:
///
///    ```rust,ignore
///    Predicates::Disjunction(vec![Some("field == 0"), Some("field == 1")])
///    ```
///
///    and those left outside are:
///
///    ```rust,ignore
///    Predicates::Disjunctions(vec![None, Some("datum > 100")])
///    ```
///
/// It's important to note that we _do_ allow pushing through None in this
/// particular piece of code. That's because we cannot tell locally whether
/// we'll end up with a full table scan. The overall plan constructor checks
/// that as one of its invariants at the end of optimization, but we don't know
/// that just when we push predicates around individual nodes.
///
/// # Merging filters
///
/// Another query optimization is to merge neighboring filters. That is, if you
/// write `filter field == 0 | filter other_field == 1`, we merge that into the
/// logically-equivalent `filter field == 0 || other_field == 1`. (Note that we
/// do not handle the case where you happen to write an unsatisfiable filter,
/// that's an extremely hard problem!)
///
/// Query optimization always works "back-to-front", starting from the end of
/// the query and processing / optimizing nodes. We make only one pass rewriting
/// predicates, but we still don't know that all the predicates are "next to"
/// each other in the original query. For example:
///
/// filter field == 0
///     | align mean_within(1m)
///     | filter other_field == 0 && datum > 100
///
/// We should be able to push through the field filter, leave behind the
/// predicate on `datum`, and then merge the two field filters.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicates {
    /// A single filter, derived from an AST node, or known to contain 1
    /// disjunction.
    ///
    /// NOTE: There is no "single + optional" variant in this enum. If we push
    /// the entire predicate through, we drop the outer plan node entirely, so
    /// there is no "piece" of it that could not be pushed through.
    Single(filter::Filter),

    /// A list of at least 1 possibly-empty disjunctions.
    ///
    /// As we push disjunctions through plan nodes, we _may_ leave behind the
    /// parts that can't be pushed. Those are represented here. If an entire
    /// disjunct can be pushed through, then the corresponding entry in this
    /// variant is `None`. That is a placeholder, so that we can correctly match
    /// up predicates we push through with those we cannot push through.
    Disjunctions(OptionalDisjunctions),
}

impl From<filter::Filter> for Predicates {
    fn from(v: filter::Filter) -> Self {
        Self::Single(v)
    }
}

impl From<OptionalDisjunctions> for Predicates {
    fn from(v: OptionalDisjunctions) -> Self {
        Self::Disjunctions(v)
    }
}

/// The return type from `Predicates` functions that split it around table
/// operations.
#[derive(Debug)]
pub struct SplitPredicates {
    /// The predicates, if any, we pushed through an operation.
    pub pushed: Option<Predicates>,
    /// The predicates, if any, we did not push through an operation.
    pub not_pushed: Option<Predicates>,
}

impl SplitPredicates {
    // Construct split predicates from those pushed and not, removing the
    // predicates that are all none entirely.
    fn compress_from(
        pushed: Vec<Option<filter::Filter>>,
        not_pushed: Vec<Option<filter::Filter>>,
    ) -> SplitPredicates {
        let pushed = if pushed.iter().all(Option::is_none) {
            None
        } else {
            Some(Predicates::from(OptionalDisjunctions(pushed)))
        };
        let not_pushed = if not_pushed.iter().all(Option::is_none) {
            None
        } else {
            Some(Predicates::from(OptionalDisjunctions(not_pushed)))
        };
        Self { pushed, not_pushed }
    }
}

impl Predicates {
    /// Split self around an alignment plan node.
    ///
    /// If pushing through any of the predicates in self results in an "empty"
    /// filter being pushed through, that returns None. This is to signal that
    /// _none_ of the filter could be pushed through. This may indicate that the
    /// query will result in a full table scan, which isn't allowed. But we
    /// cannot prevent that here entirely, since there may actually be another
    /// filtering node in the query prior to the alignment. I.e., we can't fail
    /// the whole query until we get to the end of our optimization passes.
    ///
    /// For example, the filter `datum > 100` will return an error, as will
    /// `field == 0 || datum > 100`. The filter `(field == 0 || field == 1 &&
    /// datum > 100)` is fine, and will result in a split predicate.
    ///
    /// An error is returned if `self` isn't `Self::Single`.
    pub fn split_around_align(
        &self,
        align: &Align,
    ) -> anyhow::Result<SplitPredicates> {
        let Self::Single(filter) = self else {
            anyhow::bail!(
                "Cannot split a filter with disjunctions around an \
                alignment table operation"
            );
        };
        // Sanity check that the filter, before being split into disjunctions,
        // applies to the schema.
        let schema = &align.output.tables.first().unwrap().schema;
        Filter::ensure_filter_expr_application_is_valid(&filter.expr, schema)?;
        let disjunctions = filter.simplify_to_dnf()?.flatten_disjunctions();

        // If we have a single disjunction, we are going to return an optional
        // single variant.
        if disjunctions.len() == 1 {
            let disjunct = disjunctions.into_iter().next().unwrap();

            // Remove filters that refer to the datum. Those filters by
            // definition apply to the _aligned_ data, and at this point, we're
            // pushing these filters before the alignment. Those are not
            // commutative.
            let pushed = disjunct
                .remove_datum(schema)?
                .map(|f| f.shift_timestamp_by(align.alignment.period))
                .map(Predicates::Single);
            let not_pushed =
                disjunct.only_datum(schema)?.map(Predicates::Single);
            return Ok(SplitPredicates { pushed, not_pushed });
        }

        // We have multiple predicates, so let's separate out them all.
        let mut pushed = Vec::with_capacity(disjunctions.len());
        let mut not_pushed = Vec::with_capacity(disjunctions.len());
        for disjunct in disjunctions {
            pushed.push(
                disjunct
                    .remove_datum(schema)?
                    .map(|f| f.shift_timestamp_by(align.alignment.period)),
            );
            not_pushed.push(disjunct.only_datum(schema)?);
        }

        // We need to "compress" either side if it's an array of all Nones. That
        // means none of disjuncts could be reordered, and so we'll need to
        // elide the plan node entirely.
        Ok(SplitPredicates::compress_from(pushed, not_pushed))
    }

    /// Split the predicates in self around a delta node.
    pub fn split_around_delta(
        &self,
        delta: &Delta,
    ) -> anyhow::Result<SplitPredicates> {
        match self {
            Predicates::Single(single) => {
                Self::split_single_predicates_around_delta(single, delta)
            }
            Predicates::Disjunctions(disjunctions) => {
                // Even though we're pushing disjunctions, we only get this by
                // splitting around another op. That means each element cannot
                // _contain_ disjunctions -- they are joined by OR, though. So
                // that means each element itself is single filter expr. So
                // calling `flatten_disjunctions()` should return self. That
                // means we know we'll always get a single predicate (or none).
                // Either way, we push it onto our list, and then compress if
                // needed.
                let mut pushed = Vec::with_capacity(disjunctions.len());
                let mut not_pushed = Vec::with_capacity(disjunctions.len());
                for maybe_filter in disjunctions.iter() {
                    let Some(filter) = maybe_filter else {
                        continue;
                    };
                    let this = Self::split_single_predicates_around_delta(
                        filter, delta,
                    )?;
                    match this.pushed {
                        Some(Predicates::Single(filter)) => {
                            pushed.push(Some(filter))
                        }
                        Some(_) => unreachable!(),
                        None => pushed.push(None),
                    }
                    match this.not_pushed {
                        Some(Predicates::Single(filter)) => {
                            not_pushed.push(Some(filter))
                        }
                        Some(_) => unreachable!(),
                        None => not_pushed.push(None),
                    }
                }
                Ok(SplitPredicates::compress_from(pushed, not_pushed))
            }
        }
    }

    fn split_single_predicates_around_delta(
        filter: &filter::Filter,
        delta: &Delta,
    ) -> anyhow::Result<SplitPredicates> {
        let schema = &delta.output;
        let disjunctions = filter.simplify_to_dnf()?.flatten_disjunctions();

        // If we have a single disjunction, we are going to return an optional
        // single variant.
        if disjunctions.len() == 1 {
            let disjunct = disjunctions.into_iter().next().unwrap();
            let pushed = disjunct.remove_datum(schema)?.map(Predicates::Single);
            let not_pushed =
                disjunct.only_datum(schema)?.map(Predicates::Single);
            return Ok(SplitPredicates { pushed, not_pushed });
        }

        // We have multiple predicates, so let's separate out them all.
        let mut pushed = Vec::with_capacity(disjunctions.len());
        let mut not_pushed = Vec::with_capacity(disjunctions.len());
        for disjunct in disjunctions {
            pushed.push(disjunct.remove_datum(schema)?);
            not_pushed.push(disjunct.only_datum(schema)?);
        }

        // We need to "compress" either side if it's an array of all Nones. That
        // means none of disjuncts could be reordered, and so we'll need to
        // elide the plan node entirely.
        Ok(SplitPredicates::compress_from(pushed, not_pushed))
    }

    pub(crate) fn to_required(&self) -> anyhow::Result<Vec<filter::Filter>> {
        match self {
            Predicates::Single(single) => {
                single.simplify_to_dnf().map(|f| f.flatten_disjunctions())
            }
            Predicates::Disjunctions(disjuncts) => {
                let mut out = Vec::with_capacity(disjuncts.len());
                for disjunct in disjuncts.iter() {
                    out.extend(
                        disjunct
                            .as_ref()
                            .expect("Must be Some(_) here")
                            .simplify_to_dnf()?
                            .flatten_disjunctions(),
                    );
                }
                Ok(out)
            }
        }
    }

    /// And this predicate with another.
    ///
    /// This will fail if there are different numbers of predicates in each
    /// argument, since we won't know how to match up the predicates in that
    /// case.
    pub(crate) fn and(&self, other: &Self) -> anyhow::Result<Self> {
        match (self, other) {
            (Predicates::Single(left), Predicates::Single(right)) => {
                Ok(Self::Single(left.and(right)))
            }
            (
                Predicates::Single(single),
                Predicates::Disjunctions(disjunctions),
            )
            | (
                Predicates::Disjunctions(disjunctions),
                Predicates::Single(single),
            ) => {
                anyhow::ensure!(
                    disjunctions.len() == 1,
                    "Can only merge together a list of predicates with a \
                    single predicate if the list has length 1"
                );
                if let Some(right) = &disjunctions[0] {
                    Ok(Self::Single(single.and(right)))
                } else {
                    Ok(Self::Single(single.clone()))
                }
            }
            (
                Predicates::Disjunctions(left),
                Predicates::Disjunctions(right),
            ) => {
                anyhow::ensure!(
                    left.len() == 1 && right.len() == 1,
                    "Can only merge together lists of predicates with length 1"
                );
                match (&left[0], &right[0]) {
                    (None, None) => {
                        anyhow::bail!("Both predicates cannot be empty")
                    }
                    (None, Some(single)) | (Some(single), None) => {
                        Ok(Self::Single(single.clone()))
                    }
                    (Some(left), Some(right)) => {
                        Ok(Self::Single(left.and(right)))
                    }
                }
            }
        }
    }

    /// Convert to entries in a query plan tree, one for each predicate.
    pub fn plan_tree_entries(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(1);
        match self {
            Predicates::Single(single) => {
                out.push(format!("key group 0: {}", single));
            }
            Predicates::Disjunctions(disjuncts) => {
                out.reserve(disjuncts.len().saturating_sub(1));
                for (i, disjunct) in disjuncts.iter().enumerate() {
                    let s = match disjunct {
                        Some(d) => d.to_string(),
                        None => String::from("-"),
                    };
                    out.push(format!("key group {i}: {s}"));
                }
            }
        }
        out
    }

    /// Return `true` if we can reorder predicates around the limit.
    pub(crate) fn can_reorder_around(&self, limit: &Limit) -> bool {
        match self {
            Predicates::Single(single) => single.can_reorder_around(limit),
            Predicates::Disjunctions(disjunctions) => {
                disjunctions.iter().all(|maybe_filter| {
                    maybe_filter
                        .as_ref()
                        .map(|filter| filter.can_reorder_around(limit))
                        .unwrap_or(true)
                })
            }
        }
    }
}

/// A list of optional, disjunctive elements from a filter predicate.
///
/// See [`Predicates`] for details.
#[derive(Clone, Debug, PartialEq)]
pub struct OptionalDisjunctions(Vec<Option<filter::Filter>>);

impl std::ops::Deref for OptionalDisjunctions {
    type Target = [Option<filter::Filter>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::oxql::ast::grammar::query_parser;
    use crate::oxql::plan::node::Node;
    use crate::oxql::plan::plan::test_utils::all_schema;
    use crate::oxql::plan::predicates::Predicates;
    use crate::oxql::plan::predicates::SplitPredicates;
    use crate::oxql::plan::Plan;

    #[tokio::test]
    async fn push_single_predicate_through_alignment() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | align mean_within(1s)",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let Node::Align(align) = plan.nodes().last().unwrap() else {
            panic!(
                "Plan should end with an align node, found {:?}",
                plan.nodes().last().unwrap()
            );
        };
        let filter = query_parser::filter("filter link_name == 'foo'").unwrap();
        let predicates = Predicates::Single(filter.clone());
        let SplitPredicates { pushed, not_pushed } =
            predicates.split_around_align(&align).unwrap();
        let Predicates::Single(single) = pushed.as_ref().unwrap() else {
            panic!("Expected Predicates::Single, found {:?}", pushed);
        };
        assert_eq!(
            &filter, single,
            "Should have pushed entire prediate through"
        );
        assert!(not_pushed.is_none(), "Should have pushed entire predicate");
    }

    #[tokio::test]
    async fn push_single_predicate_partway_through_alignment() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | align mean_within(1s)",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let Node::Align(align) = plan.nodes().last().unwrap() else {
            panic!(
                "Plan should end with an align node, found {:?}",
                plan.nodes().last().unwrap()
            );
        };
        let filter =
            query_parser::filter("filter link_name == 'foo' && datum > 100.0")
                .unwrap();
        let predicates = Predicates::Single(filter.clone());
        let SplitPredicates { pushed, not_pushed } =
            predicates.split_around_align(&align).unwrap();
        let Predicates::Single(single) = pushed.as_ref().unwrap() else {
            panic!("Expected Predicates::Single, found {:?}", pushed);
        };
        let expected =
            query_parser::filter("filter link_name == 'foo'").unwrap();
        assert_eq!(
            single, &expected,
            "Incorrect filter pushed through alignment"
        );
        let Predicates::Single(left) = not_pushed.as_ref().unwrap() else {
            panic!("Expected Predicates::Single, found {:?}", not_pushed,);
        };
        let expected = query_parser::filter("filter datum > 100.0").unwrap();
        assert_eq!(
            left, &expected,
            "The remaining predicate on datum should have been left behind"
        );
    }

    #[tokio::test]
    async fn push_none_of_single_predicate_through_alignment() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | align mean_within(1s)",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let Node::Align(align) = plan.nodes().last().unwrap() else {
            panic!(
                "Plan should end with an align node, found {:?}",
                plan.nodes().last().unwrap()
            );
        };
        let filter = query_parser::filter("filter datum > 100.0").unwrap();
        let predicates = Predicates::Single(filter.clone());
        let SplitPredicates { pushed, not_pushed } =
            predicates.split_around_align(&align).unwrap();
        assert!(
            pushed.is_none(),
            "Should have pushed nothing through alignment"
        );
        let Predicates::Single(left) = not_pushed.as_ref().unwrap() else {
            panic!("Expected Predicates::Single, found {:?}", not_pushed,);
        };
        let expected = query_parser::filter("filter datum > 100.0").unwrap();
        assert_eq!(
            left, &expected,
            "The entire predicate on datum should have been left behind"
        );
    }

    #[tokio::test]
    async fn push_multiple_predicates_partway_through_alignment() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | align mean_within(1s)",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let Node::Align(align) = plan.nodes().last().unwrap() else {
            panic!(
                "Plan should end with an align node, found {:?}",
                plan.nodes().last().unwrap()
            );
        };
        let filter =
            query_parser::filter("filter (link_name == 'foo' || link_name == 'foo') && datum > 100.0")
                .unwrap();
        let predicates = Predicates::Single(filter.clone());
        let SplitPredicates { pushed, not_pushed } =
            predicates.split_around_align(&align).unwrap();
        let Predicates::Disjunctions(list) = pushed.as_ref().unwrap() else {
            panic!("Expected Predicates::Disjunctions, found {:?}", pushed);
        };
        let expected =
            query_parser::filter("filter link_name == 'foo'").unwrap();
        assert_eq!(list.len(), 2);
        for each in list.iter() {
            let each = each.as_ref().expect("Should have pushed through");
            assert_eq!(
                each, &expected,
                "Incorrect filter pushed through alignment"
            );
        }

        let Predicates::Disjunctions(list) = not_pushed.as_ref().unwrap()
        else {
            panic!("Expected Predicates::Disjunctions, found {:?}", not_pushed,);
        };
        let expected = query_parser::filter("filter datum > 100.0").unwrap();
        assert_eq!(list.len(), 2);
        for each in list.iter() {
            let each = each.as_ref().expect("Should have pushed through");
            assert_eq!(
                each, &expected,
                "Incorrect filter pushed through alignment"
            );
        }
    }

    #[tokio::test]
    async fn push_predicates_partway_through_alignment_differently() {
        let query = query_parser::query(
            "get physical_data_link:bytes_sent | align mean_within(1s)",
        )
        .unwrap();
        let plan = Plan::new(query, all_schema().await).unwrap();
        let Node::Align(align) = plan.nodes().last().unwrap() else {
            panic!(
                "Plan should end with an align node, found {:?}",
                plan.nodes().last().unwrap()
            );
        };
        let filter =
            query_parser::filter("filter link_name == 'foo' || (link_name == 'foo' && datum > 100.0)")
                .unwrap();
        let predicates = Predicates::Single(filter.clone());
        let SplitPredicates { pushed, not_pushed } =
            predicates.split_around_align(&align).unwrap();
        let Predicates::Disjunctions(list) = pushed.as_ref().unwrap() else {
            panic!("Expected Predicates::Disjunctions, found {:?}", pushed);
        };
        let expected =
            query_parser::filter("filter link_name == 'foo'").unwrap();
        assert_eq!(list.len(), 2);
        for each in list.iter() {
            let each = each.as_ref().expect("Should have pushed through");
            assert_eq!(
                each, &expected,
                "Incorrect filter pushed through alignment"
            );
        }

        let Predicates::Disjunctions(list) = not_pushed.as_ref().unwrap()
        else {
            panic!("Expected Predicates::Disjunctions, found {:?}", not_pushed,);
        };
        assert_eq!(list.len(), 2);
        assert!(list[0].is_none(), "There is no disjunct to push through");
        let expected = query_parser::filter("filter datum > 100.0").unwrap();
        let each = list[1].as_ref().expect("Should have pushed through");
        assert_eq!(
            each, &expected,
            "Incorrect filter pushed through alignment"
        );
    }
}
