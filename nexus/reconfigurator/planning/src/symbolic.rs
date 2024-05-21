// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism used to generate symbolic representations of racks and
//! operations on racks that can be useful for reconfigurator testing.

use nexus_types::deployment::{PlanningInput, Policy};
use nexus_types::external_api::views::{
    PhysicalDiskPolicy, PhysicalDiskState, SledPolicy, SledState,
};
use omicron_common::api::external::Generation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod omicron_zones;
mod ops;
mod physical_disks;
mod test_harness;

pub use test_harness::{SymbolMap, TestHarness, TestPool};

pub use omicron_zones::{
    OmicronZoneExternalIp, OmicronZoneExternalIpUuid,
    OmicronZoneNetworkResources, OmicronZoneNic, OmicronZoneUuid,
};

pub use physical_disks::{
    ControlPlanePhysicalDisk, DiskIdentity, PhysicalDiskUuid, ZpoolUuid,
};

pub use ops::Op;

const SLOTS_PER_RACK: usize = 32;
const MAX_DISKS_PER_SLED: usize = 10;

/// A description of a fleet at at given point in time. For now, fleets may only
/// contain one rack.
///
/// This is most useful to generate the "initial state" of a `Fleet` for testing
/// purposes.
///
/// From these symbolic representations we can generate a set of concrete types
/// and use them to generate blueprints from the planner.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct FleetDescription {
    // We only support one rack right now
    num_racks: usize,
    num_ip_ranges: usize,
    pub num_sleds: usize,
    pub num_nexus_zones: usize,
}

impl FleetDescription {
    /// Provision a single sled on a single rack with:
    ///   * a single ip range
    ///   * a single nexus zone.
    ///   * a single IP range with the default number of addresses
    ///   * a full set of 10 U.2 disks, all adopted
    pub fn single_sled() -> FleetDescription {
        FleetDescription {
            num_racks: 1,
            num_sleds: 1,
            num_nexus_zones: 1,
            num_ip_ranges: 1,
        }
    }

    fn policy(&self, id_gen: &mut SymbolicIdGenerator) -> FleetPolicy {
        FleetPolicy {
            // We only support one range for now
            service_ip_pool_ranges: vec![IpRange {
                symbolic_id: id_gen.next(),
            }],
            target_nexus_zone_count: self.num_nexus_zones,
        }
    }

    fn sleds(
        &self,
        id_gen: &mut SymbolicIdGenerator,
    ) -> BTreeMap<SledUuid, Sled> {
        let mut sleds = BTreeMap::new();

        for _ in 0..self.num_sleds {
            let zpools = sled_disks(MAX_DISKS_PER_SLED, id_gen);
            sleds.insert(
                SledUuid::new(id_gen.next()),
                Sled {
                    policy: SledPolicy::provisionable(),
                    state: SledState::Active,
                    resources: SledResources {
                        zpools,
                        subnet: SledSubnet::new(id_gen.next()),
                    },
                },
            );
        }

        sleds
    }

    pub fn to_fleet(&self, id_gen: &mut SymbolicIdGenerator) -> Fleet {
        let sleds = self.sleds(id_gen);
        let zones = BTreeMap::new();
        let network_resources = BTreeMap::new();
        let db_state = DbState {
            policy: self.policy(id_gen),
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            sleds: sleds.clone(),
            zones: zones.clone(),
            network_resources: network_resources.clone(),
        };
        let rack = Rack {
            symbolic_id: id_gen.next(),
            sleds,
            zones,
            network_resources,
        };
        let rack_uuid = RackUuid { symbolic_id: rack.symbolic_id };
        Fleet { db_state, racks: [(rack_uuid, rack)].into() }
    }
}

fn sled_disks(
    num_disks: usize,
    id_gen: &mut SymbolicIdGenerator,
) -> BTreeMap<ZpoolUuid, ControlPlanePhysicalDisk> {
    let mut disks = BTreeMap::new();
    for _ in 0..num_disks {
        disks.insert(
            ZpoolUuid::new(id_gen.next()),
            ControlPlanePhysicalDisk::new(id_gen),
        );
    }
    disks
}

/// An error that results when trying to execute a symbolic operation against
/// a fleet
// TODO: thiserror
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    /// There are no empty slots in the fleet's racks
    NoSlotsAvailable,
    /// The same sled already exists in the db
    SledAlreadyPresent,
}

/// An error that results when trying to turn a symbolic type into a concrete
/// type
// TODO: thiserror
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReificationError {
    SymbolAlreadyMapped(SymbolicId),
    OutOfIpRanges,
    OutOfDiskIdentities,
}

/// A symbolic representation of a Fleet at a given point in time
/// This is the top-level symbolic type used for testing
///
/// TODO-multirack: For now we only allow one rack in a fleet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fleet {
    /// The state of CRDB (presumed as a single instance with nodes running
    /// across the fleet).
    pub db_state: DbState,

    /// The state of hardware and software running on the racks in a Fleet
    pub racks: BTreeMap<RackUuid, Rack>,
}

impl Fleet {
    /// Execute a symbolic operation against a fleet and return a new Fleet
    // TODO-performance: This can almost certainly be made more efficient
    // by reducing copying.
    pub fn exec(
        &self,
        id_gen: &mut SymbolicIdGenerator,
        op: Op,
    ) -> Result<Fleet, Error> {
        match op {
            Op::AddSledToDbState { sled } => {
                if self.db_state.slots_available() {
                    let mut fleet = self.clone();
                    fleet.db_state.add_sled(sled, id_gen)?;
                    Ok(fleet)
                } else {
                    Err(Error::NoSlotsAvailable)
                }
            }
            Op::DeployInitialZones { sled } => {
                todo!()
            }
            Op::GenerateBlueprint => {
                todo!()
            }
        }
    }

    /// Return the first rack
    ///
    /// Currently we only support a fleet with a single rack, so this is a
    /// mighty useful method.
    pub fn first_rack(&self) -> &Rack {
        self.racks.first_key_value().unwrap().1
    }
}

/// The symbolic state of the database at a given point in time.
///
/// The `DbState` can be used to generate a concrete `PlanningInput`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbState {
    pub policy: FleetPolicy,

    pub internal_dns_version: Generation,
    pub external_dns_version: Generation,

    // These are the control plane versions of what exist in `Rack`. `Rack` may
    // not be in sync until the blueprint gets "executed" using this information
    pub sleds: BTreeMap<SledUuid, Sled>,
    pub zones: BTreeMap<OmicronZoneUuid, SledUuid>,
    pub network_resources:
        BTreeMap<OmicronZoneUuid, OmicronZoneNetworkResources>,
}

impl DbState {
    /// Return true if any slots are available to add sleds
    // TODO-multirack: Take into account more than one rack
    pub fn slots_available(&self) -> bool {
        self.sleds.len() < SLOTS_PER_RACK
    }

    /// Add a sled to the db state.

    /// This maps to a sled being added by the operator  and then being
    /// announced to nexus over the underlay and stored in CRDB. All present
    /// physical disks should be treated as adopted by the control plane here.
    ///
    /// For now we use a full sled with 10 disks, but will allow flexibility
    /// here later.
    pub fn add_sled(
        &mut self,
        sled_id: SledUuid,
        id_gen: &mut SymbolicIdGenerator,
    ) -> Result<(), Error> {
        if self.sleds.contains_key(&sled_id) {
            return Err(Error::SledAlreadyPresent);
        }

        let zpools = sled_disks(MAX_DISKS_PER_SLED, id_gen);
        self.sleds.insert(
            sled_id,
            Sled {
                policy: SledPolicy::provisionable(),
                state: SledState::Active,
                resources: SledResources {
                    zpools,
                    subnet: SledSubnet::new(id_gen.next()),
                },
            },
        );

        Ok(())
    }

    /// Generate a concrete `PlanningInput` given a symbolic `DbState`
    pub fn to_planning_input(
        &self,
        pool: &mut TestPool,
        symbol_map: &mut SymbolMap,
    ) -> Result<PlanningInput, ReificationError> {
        let policy = self.policy.reify(pool, symbol_map)?;

        todo!()
    }
}

/// A symbolic representation of a rack at a given point in time.
///
/// The rack can be used to generate a concrete `Collection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rack {
    pub symbolic_id: SymbolicId,
    pub sleds: BTreeMap<SledUuid, Sled>,

    /// When zones are symbolically allocated, we don't know which sled the
    /// reconfigurator will actually place them on, so we use a symbolic
    /// placeholder `SledUuid`, rather than storing the zone under a known sled
    /// in the `sleds` field above.
    pub zones: BTreeMap<OmicronZoneUuid, SledUuid>,

    /// For the same reason as zones above, these are kept outside of `sleds`
    pub network_resources:
        BTreeMap<OmicronZoneUuid, OmicronZoneNetworkResources>,
}

impl Enumerable for Rack {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct RackUuid {
    symbolic_id: SymbolicId,
}

impl Enumerable for RackUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// This maps to `Policy` in `nexus-types`.
///
/// For now it maps to a single rack. Eventually it will support multi-rack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetPolicy {
    pub service_ip_pool_ranges: Vec<IpRange>,
    pub target_nexus_zone_count: usize,
}

impl FleetPolicy {
    /// Generate a [`nexus_types::deployment::Policy`] from a [`FleetPolicy`]
    pub fn reify(
        &self,
        pool: &mut TestPool,
        symbol_map: &mut SymbolMap,
    ) -> Result<nexus_types::deployment::Policy, ReificationError> {
        let mut service_ip_pool_ranges = vec![];
        for range in &self.service_ip_pool_ranges {
            let range = range.reify(pool, symbol_map)?;
            service_ip_pool_ranges.push(range);
        }
        Ok(Policy {
            service_ip_pool_ranges,
            target_nexus_zone_count: self.target_nexus_zone_count,
        })
    }
}

/// An abstract type representing the size of something, where the absolute
/// values are unknown, but the symbolic representation allows generating
/// reasonable values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Capacity {
    Full,
    Empty,
    HalfFull,
    MostlyFull,
    MostlyEmpty,
}

// A mechanism for identifying resources in a unique way without worrying about
// concrete details. `SymbolicId`s are evnetually converted to concrete types
// via the `reify` step.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Default,
)]
pub struct SymbolicId(usize);

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SymbolicIdGenerator {
    next_id: usize,
}

impl SymbolicIdGenerator {
    pub fn next(&mut self) -> SymbolicId {
        let id = self.next_id;
        self.next_id += 1;
        SymbolicId(id)
    }
}

/// A symbolic type that has a unique SymbolicId
pub trait Enumerable {
    fn symbolic_id(&self) -> SymbolicId;
}

/// A symbolic representation of the sled's Ipv6 subnet on the underlay network
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct SledSubnet {
    symbolic_id: SymbolicId,
}

impl SledSubnet {
    pub fn new(symbolic_id: SymbolicId) -> Self {
        SledSubnet { symbolic_id }
    }
}

impl Enumerable for SledSubnet {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// Symbolic representation of the state of a sled inside a rack at a given
/// point in time.
///
/// Note that this type only contains resources that get explicitly allocated
/// by a user, such as a physical disk being inserted into a sled, and not
/// resources, such as `zones` that may be allocated to one or more sleds by
/// the planner. The symbolic representation does not try to reproduce planner
/// logic and therefore does not attempt pinpoint which sled a resource is
/// deployed to when there are multiple possible choices. Instead, the resources
/// themselves are tracked individually inside a `Rack` for now, and eventually
/// perhaps a fleet if reconfigurator and DBs span fleets. The tracked resources
/// point to `UnknownSled` symbols with their own `SymbolicId`s that will be mapped
/// to their real `SledUuid`s when the planner performs a real reconfiguration
/// and generates a `Blueprint`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledResources {
    zpools: BTreeMap<ZpoolUuid, ControlPlanePhysicalDisk>,
    subnet: SledSubnet,
}

/// Symbolic representation of a sled at a given point in time
///
// TODO: Add sled role and slot id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sled {
    pub policy: SledPolicy,
    pub state: SledState,
    pub resources: SledResources,
}

#[derive(
    Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct SledUuid {
    symbolic_id: SymbolicId,
}

impl SledUuid {
    pub fn new(symbolic_id: SymbolicId) -> Self {
        SledUuid { symbolic_id }
    }
}

impl Enumerable for SledUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// Symbolic representation of ranges specified by the IP pool for
/// externally-visible control plane services (e.g., external DNS, Nexus,
/// boundary NTP)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpRange {
    symbolic_id: SymbolicId,
}

impl IpRange {
    /// Return a concrete type that maps to this symbolic_id or an error if this
    /// is not possible.
    pub fn reify(
        &self,
        pool: &mut TestPool,
        symbol_map: &mut SymbolMap,
    ) -> Result<omicron_common::address::IpRange, ReificationError> {
        // First lets check to see if we already have an allocation for this
        // symbolic IpRange. If so, we'll just go ahead and return that.
        if let Some(range) = symbol_map.ip_ranges.get(&self.symbolic_id) {
            return Ok(range.clone());
        }

        // We don't already have an allocation. Attempt to get one from the
        // pool if one exists, and return an error if not.
        let range = pool
            .service_ip_pool_ranges
            .pop()
            .ok_or(ReificationError::OutOfIpRanges)?;

        // Save the range for later
        symbol_map.ip_ranges.insert(self.symbolic_id, range.clone());
        Ok(range)
    }
}

impl Enumerable for IpRange {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn fleet_construction() {
        let mut id_gen = SymbolicIdGenerator::default();
        let fleet = FleetDescription::single_sled().to_fleet(&mut id_gen);
        println!("{fleet:#?}");
    }
}
