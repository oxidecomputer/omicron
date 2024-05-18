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

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The number of IPs in a range
type IpRangeSize = usize;

const DEFAULT_IP_RANGE_SIZE: usize = 5;

/// A description of the state of a rack at a given point in time. A user
/// creates one of these and then the corresponding symbolic types are generated
/// from it. These symbolic types can then be used to generate real types in
/// order to test reconfigurator.
///
/// This is most useful to generate the "initial state" of a rack for testing
/// purposes. A symbolic `Collection` and `PlanningInput` can be generated and
/// manipulated to generate new symbolic Racks, collections, and planning inputs.
///
/// From these symbolic representations we can generate a set of concrete types
/// and use them to generate blueprints from the planner.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct RackDescription {
    pub num_sleds: usize,
    pub num_nexus_zones: usize,
    pub ip_ranges: Vec<IpRangeSize>,
}

impl RackDescription {
    /// Provision a single sled with:
    ///   * a single ip range
    ///   * a single nexus zone.
    ///   * A single IP range with the default number of addresses
    ///
    pub fn single_sled() -> RackDescription {
        RackDescription {
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
            target_nexus_zone_count: ZoneCount {
                symbolic_id: id_gen.next(),
                val: self.num_nexus_zones,
            },
        }
    }

    fn sled_disks(
        &self,
        num_disks: usize,
        id_gen: &mut SymbolicIdGenerator,
    ) -> BTreeMap<ZpoolUuid, SledDisk> {
        let mut disks = BTreeMap::new();
        for _ in 0..num_disks {
            disks.insert(ZpoolUuid::new(id_gen.next()), SledDisk::new(id_gen));
        }
        disks
    }

    fn sleds(
        &self,
        id_gen: &mut SymbolicIdGenerator,
    ) -> BTreeMap<SledUuid, Sled> {
        let mut sleds = BTreeMap::new();

        for _ in 0..self.num_sleds {
            let zpools = self.sled_disks(10, id_gen);
            sleds.insert(
                SledUuid::new(id_gen.next()),
                Sled {
                    policy: SledPolicy::InServiceProvisionable,
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

    pub fn to_rack(&self, id_gen: &mut SymbolicIdGenerator) -> Rack {
        Rack { policy: self.policy(id_gen), sleds: self.sleds(id_gen) }
    }
}

// TODO: Should really model the Control Plane state (what's in the database)
// vs what's on each sled at each point in time. This will result in a model different
// from both the way `System` works, and disjoint from `PlanningInput` and `Collection`.
//
// The `ControlPlaneState` combined with the `Rack` can be used to generate the
// `PlanningInput` and `Collection`. Eventually we may also want to change the
// way these look to better reflect the symbolic types, but it's not strictly
// necessary. It depends on how much we like the symbolic model.
//
/// The symbolic state of the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbState {
    pub policy: FleetPolicy,
    // TODO: Add more control plane related stuff
    //  * any known sled ids, external ips, etc...
    //  * any physical disks
    //  * the known state of sleds
    //  * prior collections
    //  * blueprints
}

/// A symbolic representation of a rack at a given point in time.
///
/// The rack can be used to generate a symbolic `Collection` and
/// `PlanningInput`.
///
/// TODO-multirack: `FleetPolicy` will move outside the rack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rack {
    pub policy: FleetPolicy,
    pub sleds: BTreeMap<SledUuid, Sled>,
}

/// A symbolic representation of a PlanningInput
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningInput {
    pub policy: FleetPolicy,
    pub internal_dns_version: Generation,
    pub external_dns_version: Generation,
    pub sleds: BTreeMap<SledUuid, Sled>,
    // TODO: Network resources
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

/// An abstract representation of a zone type
///
/// This should be updated when blueprint generation supports more types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ZoneType {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    CockroachDb,
    Crucible,
    CruciblePantry,
    ExternalDns,
    InternalDns,
    InternalNtp,
    Nexus,
    Oximeter,
}

/// A symbolic type that has a unique SymbolicId
pub trait Enumerable {
    fn symbolic_id(&self) -> SymbolicId;
}

/// A symbolic representation of a `DiskIdentity`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskIdentity {
    pub symbolic_id: SymbolicId,
}

impl DiskIdentity {
    pub fn new(symbolic_id: SymbolicId) -> Self {
        DiskIdentity { symbolic_id }
    }
}

impl Enumerable for DiskIdentity {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// A symbolic representation of a `PhysicalDiskUuid`
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct PhysicalDiskUuid {
    symbolic_id: SymbolicId,
}

impl PhysicalDiskUuid {
    fn new(symbolic_id: SymbolicId) -> Self {
        PhysicalDiskUuid { symbolic_id }
    }
}

impl Enumerable for PhysicalDiskUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// A symbolic representation of a single disk already
/// managed by the sled.
///
/// TODO: This shouldn't be part of the `Sled`, but rather part of
/// the `DbState`. The `Sled` should have a different type where all
/// the control plane parts (disk_id, policy, and state) are optional
/// until they exist in the DB. This allows us to represent disk adoption better.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDisk {
    disk_identity: DiskIdentity,
    disk_id: PhysicalDiskUuid,
    policy: PhysicalDiskPolicy,
    state: PhysicalDiskState,
}

impl SledDisk {
    pub fn new(id_gen: &mut SymbolicIdGenerator) -> SledDisk {
        SledDisk {
            disk_identity: DiskIdentity::new(id_gen.next()),
            disk_id: PhysicalDiskUuid::new(id_gen.next()),
            policy: PhysicalDiskPolicy::InService,
            state: PhysicalDiskState::Active,
        }
    }
}

/// Symbolic representation of a `PhysicalDiskPolicy`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalDiskPolicy {
    InService,
    Expunged,
}

/// Symbolic representation of `PhysicalDiskState`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalDiskState {
    Active,
    Decommissioned,
}

/// Symbolic representation of an external ip type for a zone
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OmicronZoneExternalIpType {
    Floating,
    Snat,
}

/// Symbolic representation of an OmicronZoneUuid
pub struct OmicronZoneUuid {
    symbolic_id: SymbolicId,
}

impl Enumerable for OmicronZoneUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
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

/// A symbolic representation of a `ZpoolUuid`
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct ZpoolUuid {
    symbolic_id: SymbolicId,
}

impl ZpoolUuid {
    fn new(symbolic_id: SymbolicId) -> ZpoolUuid {
        ZpoolUuid { symbolic_id }
    }
}

impl Enumerable for ZpoolUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
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

/// Symbolic representation of `SledResources`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledResources {
    zpools: BTreeMap<ZpoolUuid, SledDisk>,
    subnet: SledSubnet,
    // TODO:  We want to be able to be able to symbolically represent the state
    // of an entire rack. And from that we should be able to generate symbolic
    // versions of a `PlanningInput` and a `Collection`. We can mutate the
    // symbolic `Collection` and `PlanningInput` in order to generate a symbolic
    // `Blueprint` which we can then apply to the symbolic `Rack` to update
    // its state. In order to do this, we need to model more resources:
    //
    //  * zones and network resources
    //  * eventually sp, rot and other hardware
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
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
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

/// Trait representing a symbolic operation across one or more symbolic types
pub trait SymbolicOp {}

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

/// A symbolic version of the number of given types of zone
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneCount {
    symbolic_id: SymbolicId,
    val: usize,
}

impl Enumerable for ZoneCount {
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
    fn initial_rack_construction() {
        let mut id_gen = SymbolicIdGenerator::default();
        let rack = RackDescription::single_sled().to_rack(&mut id_gen);
        println!("{rack:#?}");
    }
}
