// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A test harness for symbolic model based testing.

use super::{Fleet, FleetDescription};

/// Take a [`FleetDescription`] as an "initial state" of the system and use it
/// to generate a symbolic model of the initial state of the [`Fleet`].
///
/// Also take an ordered list of symbolic operations that act on this [`Fleet`]
/// and transform it into a new [`Fleet`]. These changes are the ones we expect
/// from the reconfigurator planner and executor, but here we treat them as
/// manipulations of the `Fleet` model. After each operation is run, we can
/// assert invariants and other properties of the `Fleet` model to ensure
/// it matches our expecations from transformation. Similarly, before each
/// operation is run, we can check a precondition to see if the operation is
/// actually allowed to be run. This allows us to trim operations that don't
/// make semantic sense so that we can randomly generate the list of symbolic
/// operations against the `Fleet`. We can also have specific postconditions
/// for each specific operation that can ensure symbolic properties hold after
/// that given operation is run against a symbolic fleet and that it actually
/// produced a correct symbolic fleet afterwards.
///
/// At this point, we are sure that our symbolic history is correct and we
/// can begin to generate concrete types and execute the planner against these
/// concrete types. We start by generating an initial [`Blueprint`] from our
/// initial `Fleet` model. From there we step through each symbolic operation
/// and generate a new concrete `PlanningInput` and/or `Inventory` as necessary
/// and  use those along with the current blueprint as the parent, and execute
/// the planner to create a new `Blueprint`. After each step, we execute
/// callbacks that allow specific tests to ensure that the output `Blueprint`
/// matches what is expected given the input symbolic input, and concrete parent
/// `Blueprint`, `PlanningInput`, and `Inventory`.
pub struct TestHarness {}
