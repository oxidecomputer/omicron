// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A test harness for symbolic model based testing.

use super::{
    Error, Fleet, FleetDescription, Op, SymbolicId, SymbolicIdGenerator,
};
use serde::{Deserialize, Serialize};

// TODO: Instead of callbacks via a trait, allow the symbolic and dynamic
// execution to use iterators to run each op and check invariants. This will
// make things much easier with regards to the borrow checker. Update comment
// below to reflect this
//
//
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
#[derive(Debug, Serialize, Deserialize)]
pub struct TestHarness {
    pub initial_fleet_description: FleetDescription,
    pub initial_fleet: Fleet,
    pub symbolic_ops_input: Vec<Op>,
    // Purely an option to prevent borrow checker issues
    pub symbolic_id_gen: Option<SymbolicIdGenerator>,
    pub symbolic_history: Vec<SymbolicEvent>,
    pub discarded_symbolic_ops: Vec<DiscardedOp>,
}

impl TestHarness {
    pub fn new(
        mut symbolic_id_gen: SymbolicIdGenerator,
        initial_fleet_description: FleetDescription,
        ops: Vec<Op>,
    ) -> TestHarness {
        let initial_fleet =
            initial_fleet_description.to_fleet(&mut symbolic_id_gen);
        let symbolic_history = Vec::with_capacity(ops.len());

        TestHarness {
            initial_fleet_description,
            initial_fleet,
            symbolic_ops_input: ops,
            symbolic_id_gen: Some(symbolic_id_gen),
            symbolic_history,
            discarded_symbolic_ops: vec![],
        }
    }

    pub fn current_fleet(&self) -> &Fleet {
        self.symbolic_history
            .last()
            .map(|event| &event.next_fleet)
            .unwrap_or_else(|| &self.initial_fleet)
    }

    pub fn run_symbolic(&mut self) {
        // Take the id generator for borrowck purposes
        let mut id_gen = self.symbolic_id_gen.take().unwrap();
        for op in &self.symbolic_ops_input {
            match self.current_fleet().exec(&mut id_gen, *op) {
                Ok(new_fleet) => {
                    let event = SymbolicEvent::new(&mut id_gen, *op, new_fleet);
                    self.symbolic_history.push(event);
                }
                Err(err) => {
                    self.discarded_symbolic_ops.push(DiscardedOp {
                        history_index: self.symbolic_ops_input.len(),
                        op: *op,
                        reason: err,
                    });
                }
            }
        }
        // Put the id generator back for next time
        self.symbolic_id_gen = Some(id_gen);
    }
}

/// A symbolic operation and the new symbolic `Fleet` generated
/// from that operation.
///
/// This is an element in the totally ordered `TestHarness::symbolic_history`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SymbolicEvent {
    symbolic_id: SymbolicId,
    op: Op,
    next_fleet: Fleet,
}

impl SymbolicEvent {
    pub fn new(
        id_gen: &mut SymbolicIdGenerator,
        op: Op,
        next_fleet: Fleet,
    ) -> SymbolicEvent {
        SymbolicEvent { symbolic_id: id_gen.next(), op, next_fleet }
    }
}

/// Operations discarded during symbolic execution
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscardedOp {
    // The index of the entry in `symbolic_history` where this operation would have
    // gone when it was discarded.
    history_index: usize,
    op: Op,
    // The reason we discarded the operation
    reason: Error,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::symbolic::{
        FleetDescription, Op, SledUuid, SymbolicIdGenerator,
    };

    #[test]
    fn basic_symbolic_add_sled() {
        let mut id_gen = SymbolicIdGenerator::default();
        let initial_fleet_description = FleetDescription::single_sled();
        let ops =
            vec![Op::AddSledToDbState { sled: SledUuid::new(id_gen.next()) }];
        let mut harness =
            TestHarness::new(id_gen, initial_fleet_description, ops);

        // The initial fleet has one sled that is in the db and has zones
        // deployed to the rack
        assert_eq!(harness.initial_fleet.db_state.sleds.len(), 1);
        let rack = harness.initial_fleet.first_rack();
        assert_eq!(rack.sleds.len(), 1);

        // Execute all the symbolic operations
        harness.run_symbolic();

        // We should have generated a new fleet with an added sled in the db,
        // but not deployed to the rack
        assert_eq!(harness.symbolic_history.len(), 1);
        let fleet = &harness.symbolic_history[0].next_fleet;
        assert_eq!(fleet.db_state.sleds.len(), 2);
        assert_eq!(fleet.first_rack().sleds.len(), 1);

        println!("{harness:#?}");
    }
}
