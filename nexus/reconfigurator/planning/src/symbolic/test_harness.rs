// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A test harness for symbolic model based testing.

use crate::symbolic::{
    self, Error, Fleet, FleetDescription, Op, SymbolicId, SymbolicIdGenerator,
};
use omicron_common::address::IpRange;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::{
    OmicronZoneKind, PhysicalDiskKind, PhysicalDiskUuid, SledKind, SledUuid,
};
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;
use std::{collections::BTreeMap, hash::Hash};
use typed_rng::TypedUuidRng;

#[derive(Debug)]
pub struct TestHarnessRng {
    /// Various RNGs, all from a configured seed
    pub sled_rng: TypedUuidRng<SledKind>,
    pub zone_rng: TypedUuidRng<OmicronZoneKind>,
    pub physical_disks_rng: TypedUuidRng<PhysicalDiskKind>,
}

impl TestHarnessRng {
    pub fn new<H: Hash>(seed: H) -> TestHarnessRng {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "symbolic-test-harness";
        let mut parent: StdRng = typed_rng::from_seed(seed, SEED_EXTRA);

        TestHarnessRng {
            sled_rng: TypedUuidRng::from_parent_rng(&mut parent, "sleds"),
            zone_rng: TypedUuidRng::from_parent_rng(&mut parent, "zones"),
            physical_disks_rng: TypedUuidRng::from_parent_rng(
                &mut parent,
                "physical disks",
            ),
        }
    }
}

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
#[derive(Debug)]
pub struct TestHarness {
    pub initial_fleet_description: FleetDescription,
    pub initial_fleet: Fleet,
    pub symbolic_ops_input: Vec<Op>,
    // Purely an option to prevent borrow checker issues
    pub symbolic_id_gen: Option<SymbolicIdGenerator>,
    pub symbolic_history: Vec<SymbolicEvent>,
    pub discarded_symbolic_ops: Vec<DiscardedOp>,

    /// All our RNGs derived from a parent based on a provided seed
    pub rng: TestHarnessRng,
}

impl TestHarness {
    pub fn new<H: Hash>(
        mut symbolic_id_gen: SymbolicIdGenerator,
        initial_fleet_description: FleetDescription,
        ops: Vec<Op>,
        seed: H,
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
            rng: TestHarnessRng::new(seed),
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
pub struct SymbolicEvent {
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
pub struct DiscardedOp {
    // The index of the entry in `symbolic_history` where this operation would have
    // gone when it was discarded.
    history_index: usize,
    op: Op,
    // The reason we discarded the operation
    reason: Error,
}

// Maps of [`SymbolicId`]s or symbolic wrapper types to the runtime values of
// their dynamic types.
//
// Each `SymbolicId` has a 1:1 mapping to an instance of a concrete type. The
// concrete instance value is unknown during the symbolic execution phase, and
// will be filled in during dynamic execution, the first time it is encountered.
// Each subsquent occurrence, the `SymbolicId` will refer to the same value as stored
// in the `SymbolMap`. As such, every `SymbolicId` must map to an _immutable_ value.
//
// Values that are able to mutate are treated as "model" state as part of the
// symbolic `Fleet` model, and not do not have corresponding [`SymbolicId`]s.
// These model variables often contain `SymbolicId`s to simplify creation
// of the model. The actual test runs fill in the actual values either via
// a deterministic mechanism or by pulling from a pool of values like the
// `TestPool`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SymbolMap {
    pub ip_ranges: BTreeMap<SymbolicId, IpRange>,
    pub sled_ids: BTreeMap<symbolic::SledUuid, SledUuid>,
    pub disk_identities: BTreeMap<SymbolicId, DiskIdentity>,
    pub disk_uuids: BTreeMap<SymbolicId, PhysicalDiskUuid>,
}

/// A pool of resources that symbolic tests can use to generate values
/// for various concrete types.
///
/// Values are removed from the pool as they are used. They may also be added
/// back when they are "freed", although this isn't done yet.
///
/// Note: Some values are copied from `SystemDescription` as used in our
/// `ExampleSystem` for current tests, and others are just generated. We generate
/// values ahead of time to make the tests deterministic, as the values in a pool
/// do not matter for the reconfigurator.
pub struct TestPool {
    pub service_ip_pool_ranges: Vec<IpRange>,
    pub disk_identities: Vec<DiskIdentity>,
}

impl TestPool {
    pub fn new(num_sleds: usize) -> TestPool {
        // IPs from TEST-NET-1 (RFC 5737)
        let service_ip_pool_ranges = vec![IpRange::try_from((
            "192.0.2.2".parse::<Ipv4Addr>().unwrap(),
            "192.0.2.20".parse::<Ipv4Addr>().unwrap(),
        ))
        .unwrap()];

        // Allocate a unique `DiskIdentity` for num_sleds * 10 disks/sled U.2
        // disks
        // TODO: Should probably do a deterministic UUID construction
        // so we can add and remove infinite disks without running out.
        let disk_identities = (0..10 * num_sleds)
            .into_iter()
            .map(|i| DiskIdentity {
                vendor: "fake-vendor".to_string(),
                model: "fake-model".to_string(),
                serial: format!("serial-{i}"),
            })
            .collect();

        TestPool { service_ip_pool_ranges, disk_identities }
    }
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
        let mut harness = TestHarness::new(
            id_gen,
            initial_fleet_description,
            ops,
            "basic_symbolic_sled_add",
        );

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
