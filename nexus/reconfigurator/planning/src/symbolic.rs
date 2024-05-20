// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism used to generate symbolic representations of racks and
//! operations on racks that can be useful for reconfigurator testing.
//!
//! The symbolic types in this module are self contained and are used to
//! generate concrete implementations. In some cases the symbolic and concrete
//! types may look exactly the same - especially in the case of simple enums.
//! In the case that the concrete type changes  the symbolic type may also have
//! to change. This should not happen frequently, and will easily be caught by
//! tests if the change is breaking. If the change is additive, such as adding
//! a new enum variant, this may not be noticed right away and the symbolic type
//! needs to change only when a new test is written to utilize that variant.
//! Otherwise the existing symbolic type will just generate the known concrete
//! variants.

use nexus_types::external_api::params::SubnetSelector;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod omicron_zones;
mod ops;
mod physical_disks;
mod test_harness;

pub use test_harness::TestHarness;

pub use omicron_zones::{
    OmicronZoneExternalIp, OmicronZoneExternalIpUuid,
    OmicronZoneNetworkResources, OmicronZoneNic, OmicronZoneUuid, ZoneCount,
};

pub use physical_disks::{
    ControlPlanePhysicalDisk, DiskIdentity, PhysicalDiskPolicy,
    PhysicalDiskState, PhysicalDiskUuid, ZpoolUuid,
};

pub use ops::Op;

/// The number of IPs in a range
type IpRangeSize = usize;

const DEFAULT_IP_RANGE_SIZE: usize = 5;
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
    pub num_sleds: usize,
    pub num_nexus_zones: usize,
    pub ip_ranges: Vec<IpRangeSize>,
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
            ip_ranges: vec![DEFAULT_IP_RANGE_SIZE],
        }
    }

    fn policy(&self, id_gen: &mut SymbolicIdGenerator) -> FleetPolicy {
        FleetPolicy {
            service_ip_pool_ranges: self
                .ip_ranges
                .iter()
                .map(|size| IpRange { symbolic_id: id_gen.next(), size: *size })
                .collect(),
            target_nexus_zone_count: ZoneCount::new(
                id_gen.next(),
                self.num_nexus_zones,
            ),
        }
    }

    fn sleds(
        &self,
        id_gen: &mut SymbolicIdGenerator,
    ) -> BTreeMap<SledUuid, Sled> {
        let mut sleds = BTreeMap::new();

        for _ in 0..self.num_sleds {
            let zpools = sled_disks(MAX_DISKS_PER_SLED, id_gen);
            let physical_disks = zpools
                .iter()
                .map(|(_, cp_disk)| cp_disk.disk_identity.clone())
                .collect();
            sleds.insert(
                SledUuid::new(id_gen.next()),
                Sled {
                    policy: SledPolicy::InServiceProvisionable,
                    state: SledState::Active,
                    resources: SledResources {
                        physical_disks,
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
}

/// The symbolic state of the database at a given point in time.
///
/// The `DbState` can be used to generate a concrete `PlanningInput`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbState {
    pub policy: FleetPolicy,

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

    /// Add a sled to the db state. This maps to a sled being added by the operator
    /// and then being announced to nexus over the underlay and stored in CRDB.
    /// All present disks should be treated as adopted here.
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
        let physical_disks = zpools
            .iter()
            .map(|(_, cp_disk)| cp_disk.disk_identity.clone())
            .collect();

        self.sleds.insert(
            sled_id,
            Sled {
                policy: SledPolicy::InServiceProvisionable,
                state: SledState::Active,
                resources: SledResources {
                    physical_disks,
                    zpools,
                    subnet: SledSubnet::new(id_gen.next()),
                },
            },
        );

        Ok(())
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
    pub target_nexus_zone_count: ZoneCount,
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

#[derive(Debug, Default)]
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

/// Symbolic representation of a sled policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SledPolicy {
    InServiceProvisionable,
    InServiceNonProvisionable,
    Expunged,
}

/// Symbolic representation of the actual state of a sled as reflected
/// via the latest inventory collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SledState {
    Active,
    Decommissioned,
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
/// logic and therefore does not attempt pinpoint which sled  a resource is
/// deployed to when there are multiple possible choices. Instead, the resources
/// themselves are tracked individually inside a `Rack` for now, and eventually
/// perhaps a fleet if reconfigurator and DBs span fleets. The tracked resources
/// point to `UnknownSled` symbols with their own `SymbolicId`s that will be mapped
/// to their real `SledUuid`s when the planner performs a real reconfiguration
/// and generates a `Blueprint`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledResources {
    physical_disks: BTreeSet<DiskIdentity>,
    /// All zpools must map to `physical_disks` in the above field
    /// This is actually an example of an assertion that can be performed on
    /// symbolic state.
    zpools: BTreeMap<ZpoolUuid, ControlPlanePhysicalDisk>,
    subnet: SledSubnet,
}

impl SledResources {
    pub fn assert_invariants(&self) {
        // Any adopted logical disk must match a physical disk in this sled
        for (_, cp_disk) in &self.zpools {
            assert!(self.physical_disks.contains(&cp_disk.disk_identity));
        }
    }
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
    size: usize,
}

impl Enumerable for IpRange {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// A symbolic representation of a `Generation` that can be incremented and
/// compared.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Generation {}

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
