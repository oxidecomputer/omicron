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

/// A description of a fleet at at given point in time. For now, fleets may only
/// contain one rack.
///
/// This is most useful to generate the "initial state" of a `Fleet` for testing
/// purposes. A symbolic `Collection` and `PlanningInput` can be generated from
/// a `Fleet`.
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

    pub fn to_fleet(&self, id_gen: &mut SymbolicIdGenerator) -> Fleet {
        let db_state = DbState { policy: self.policy(id_gen) };
        let rack = Rack {
            symbolic_id: id_gen.next(),
            sleds: self.sleds(id_gen),
            zones: BTreeMap::new(),
        };
        let rack_uuid = RackUuid { symbolic_id: rack.symbolic_id };
        Fleet { db_state, racks: [(rack_uuid, rack)].into() }
    }
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
/// The rack can be used to generate a symbolic `Collection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rack {
    pub symbolic_id: SymbolicId,
    pub sleds: BTreeMap<SledUuid, Sled>,

    // When zones are symbolically allocated, we don't know which sled the
    // reconfigurator will actually place them on, so we use a symbolic
    // placeholder `SledUuid`, rather than storing the zone under a known sled
    // in the `sleds` field above.
    pub zones: BTreeMap<OmicronZoneUuid, SledUuid>,
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

/// A symbolic representation a control plane physical disk as stored in CRDB.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPhysicalDisk {
    pub disk_id: PhysicalDiskUuid,
    pub policy: PhysicalDiskPolicy,
    pub state: PhysicalDiskState,
}

impl DbPhysicalDisk {
    pub fn new(id_gen: &mut SymbolicIdGenerator) -> Self {
        DbPhysicalDisk {
            disk_id: PhysicalDiskUuid::new(id_gen.next()),
            policy: PhysicalDiskPolicy::InService,
            state: PhysicalDiskState::Active,
        }
    }
}

/// A symbolic representation of a single disk that may or may not have been
/// adopted by the control plane yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDisk {
    pub disk_identity: DiskIdentity,

    // This is only filled in when the control plane adopts the physical disk.
    pub db_physical_disk: Option<DbPhysicalDisk>,
}

impl SledDisk {
    pub fn new(id_gen: &mut SymbolicIdGenerator) -> SledDisk {
        SledDisk {
            disk_identity: DiskIdentity::new(id_gen.next()),
            db_physical_disk: None,
        }
    }

    pub fn control_plane_adopt(&mut self, db_physical_disk: DbPhysicalDisk) {
        self.db_physical_disk = Some(db_physical_disk);
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
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
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
    zpools: BTreeMap<ZpoolUuid, SledDisk>,
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
        let fleet = FleetDescription::single_sled().to_fleet(&mut id_gen);
        println!("{fleet:#?}");
    }
}
