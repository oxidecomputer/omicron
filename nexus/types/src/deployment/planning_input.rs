// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types describing inputs the Reconfigurator needs to plan and produce new
//! blueprints.

use crate::external_api::views::PhysicalDiskPolicy;
use crate::external_api::views::PhysicalDiskState;
use crate::external_api::views::SledPolicy;
use crate::external_api::views::SledProvisionPolicy;
use crate::external_api::views::SledState;
use ipnetwork::IpNetwork;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::PhysicalDiskKind;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::ZpoolKind;
use serde::Deserialize;
use serde::Serialize;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use strum::IntoEnumIterator;
use uuid::Uuid;

/// Describes a single disk already managed by the sled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDisk {
    pub disk_identity: DiskIdentity,
    pub disk_id: TypedUuid<PhysicalDiskKind>,
    pub policy: PhysicalDiskPolicy,
    pub state: PhysicalDiskState,
}

/// Filters that apply to disks.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum DiskFilter {
    /// All disks
    All,

    /// All disks which are in-service.
    InService,
}

impl DiskFilter {
    fn matches_policy_and_state(
        self,
        policy: PhysicalDiskPolicy,
        state: PhysicalDiskState,
    ) -> bool {
        match self {
            DiskFilter::All => true,
            DiskFilter::InService => match (policy, state) {
                (PhysicalDiskPolicy::InService, PhysicalDiskState::Active) => {
                    true
                }
                _ => false,
            },
        }
    }
}

/// Describes the resources available on each sled for the planner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledResources {
    /// zpools (and their backing disks) on this sled
    ///
    /// (used to allocate storage for control plane zones with persistent
    /// storage)
    pub zpools: BTreeMap<TypedUuid<ZpoolKind>, SledDisk>,

    /// the IPv6 subnet of this sled on the underlay network
    ///
    /// (implicitly specifies the whole range of addresses that the planner can
    /// use for control plane components)
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl SledResources {
    pub fn all_disks(
        &self,
        filter: DiskFilter,
    ) -> impl Iterator<Item = (&TypedUuid<ZpoolKind>, &SledDisk)> + '_ {
        self.zpools.iter().filter_map(move |(zpool, disk)| {
            filter
                .matches_policy_and_state(disk.policy, disk.state)
                .then_some((zpool, disk))
        })
    }
}

/// External IP allocated to a service
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalIp {
    pub id: Uuid,
    pub ip: IpNetwork,
}

/// Network interface allocated to a service
///
/// This is a slimmer `nexus_db_model::ServiceNetworkInterface` that only stores
/// the fields necessary for blueprint planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceNetworkInterface {
    pub id: Uuid,
    pub mac: MacAddr,
    pub ip: IpNetwork,
    pub slot: u8,
    pub primary: bool,
}

/// Filters that apply to sleds.
///
/// This logic lives here rather than within the individual components making
/// decisions, so that this is easier to read.
///
/// The meaning of a particular filter should not be overloaded -- each time a
/// new use case wants to make a decision based on the zone disposition, a new
/// variant should be added to this enum.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum SledFilter {
    // ---
    // Prefer to keep this list in alphabetical order.
    // ---
    /// All sleds.
    All,

    /// Sleds that are eligible for discretionary services.
    EligibleForDiscretionaryServices,

    /// Sleds that are in service (even if they might not be eligible for
    /// discretionary services).
    InService,

    /// Sleds on which reservations can be created.
    ReservationCreate,

    /// Sleds which should be sent VPC firewall rules.
    VpcFirewall,
}

impl SledFilter {
    /// Returns true if self matches the provided policy and state.
    pub fn matches_policy_and_state(
        self,
        policy: SledPolicy,
        state: SledState,
    ) -> bool {
        policy.matches(self) && state.matches(self)
    }
}

impl SledPolicy {
    /// Returns true if self matches the filter.
    ///
    /// Any users of this must also compare against the [`SledState`], if
    /// relevant: a sled filter is fully matched when it matches both the
    /// policy and the state. See [`SledFilter::matches_policy_and_state`].
    pub fn matches(self, filter: SledFilter) -> bool {
        // Some notes:
        //
        // # Match style
        //
        // This code could be written in three ways:
        //
        // 1. match self { match filter { ... } }
        // 2. match filter { match self { ... } }
        // 3. match (self, filter) { ... }
        //
        // We choose 1 here because we expect many filters and just a few
        // policies, and 1 is the easiest form to represent that.
        //
        // # Illegal states
        //
        // Some of the code that checks against both policies and filters is
        // effectively checking for illegal states. We shouldn't be able to
        // have a policy+state combo where the policy says the sled is in
        // service but the state is decommissioned, for example, but the two
        // separate types let us represent that. Code that ANDs
        // policy.matches(filter) and state.matches(filter) naturally guards
        // against those states.
        match self {
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            } => match filter {
                SledFilter::All => true,
                SledFilter::EligibleForDiscretionaryServices => true,
                SledFilter::InService => true,
                SledFilter::ReservationCreate => true,
                SledFilter::VpcFirewall => true,
            },
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            } => match filter {
                SledFilter::All => true,
                SledFilter::EligibleForDiscretionaryServices => false,
                SledFilter::InService => true,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcFirewall => true,
            },
            SledPolicy::Expunged => match filter {
                SledFilter::All => true,
                SledFilter::EligibleForDiscretionaryServices => false,
                SledFilter::InService => false,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcFirewall => false,
            },
        }
    }

    /// Returns all policies matching the given filter.
    ///
    /// This is meant for database access, and is generally paired with
    /// [`SledState::all_matching`]. See `ApplySledFilterExt` in
    /// nexus-db-model.
    pub fn all_matching(filter: SledFilter) -> impl Iterator<Item = Self> {
        Self::iter().filter(move |policy| policy.matches(filter))
    }
}

impl SledState {
    /// Returns true if self matches the filter.
    ///
    /// Any users of this must also compare against the [`SledPolicy`], if
    /// relevant: a sled filter is fully matched when both the policy and the
    /// state match. See [`SledFilter::matches_policy_and_state`].
    pub fn matches(self, filter: SledFilter) -> bool {
        // See `SledFilter::matches` above for some notes.
        match self {
            SledState::Active => match filter {
                SledFilter::All => true,
                SledFilter::EligibleForDiscretionaryServices => true,
                SledFilter::InService => true,
                SledFilter::ReservationCreate => true,
                SledFilter::VpcFirewall => true,
            },
            SledState::Decommissioned => match filter {
                SledFilter::All => true,
                SledFilter::EligibleForDiscretionaryServices => false,
                SledFilter::InService => false,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcFirewall => false,
            },
        }
    }

    /// Returns all policies matching the given filter.
    ///
    /// This is meant for database access, and is generally paired with
    /// [`SledPolicy::all_matching`]. See `ApplySledFilterExt` in
    /// nexus-db-model.
    pub fn all_matching(filter: SledFilter) -> impl Iterator<Item = Self> {
        Self::iter().filter(move |state| state.matches(filter))
    }
}

/// Fleet-wide deployment policy
///
/// The **policy** represents the deployment controls that people (operators and
/// support engineers) can modify directly under normal operation.  In the
/// limit, this would include things like: how many CockroachDB nodes should be
/// part of the cluster, what system version the system should be running, etc.
/// It would _not_ include things like which services should be running on which
/// sleds or which host OS version should be on each sled because that's up to
/// the control plane to decide.  (To be clear, the intent is that for
/// extenuating circumstances, people could exercise control over such things,
/// but that would not be part of normal operation.)
///
/// Conceptually the policy should also include the set of sleds that are
/// supposed to be part of the system and their individual [`SledPolicy`]s;
/// however, those are tracked as a separate part of [`PlanningInput`] as each
/// sled additionally has non-policy [`SledResources`] needed for planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// ranges specified by the IP pool for externally-visible control plane
    /// services (e.g., external DNS, Nexus, boundary NTP)
    pub service_ip_pool_ranges: Vec<IpRange>,

    /// desired total number of deployed Nexus zones
    pub target_nexus_zone_count: usize,
}

/// Policy and database inputs to the Reconfigurator planner
///
/// The primary inputs to the planner are the parent (either a parent blueprint
/// or an inventory collection) and this structure. This type holds the
/// fleet-wide policy as well as any additional information fetched from CRDB
/// that the planner needs to make decisions.
///
///
/// The current policy is pretty limited.  It's aimed primarily at supporting
/// the add/remove sled use case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningInput {
    /// fleet-wide policy
    policy: Policy,

    /// current internal DNS version
    internal_dns_version: Generation,

    /// current external DNS version
    external_dns_version: Generation,

    /// per-sled policy and resources
    sleds: BTreeMap<TypedUuid<SledKind>, SledDetails>,

    /// external IPs allocated to Omicron zones
    omicron_zone_external_ips: BTreeMap<TypedUuid<OmicronZoneKind>, ExternalIp>,

    /// vNICs allocated to Omicron zones
    omicron_zone_nics:
        BTreeMap<TypedUuid<OmicronZoneKind>, ServiceNetworkInterface>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDetails {
    /// current sled policy
    pub policy: SledPolicy,
    /// current sled state
    pub state: SledState,
    /// current resources allocated to this sled
    pub resources: SledResources,
}

impl PlanningInput {
    pub fn internal_dns_version(&self) -> Generation {
        self.internal_dns_version
    }

    pub fn external_dns_version(&self) -> Generation {
        self.external_dns_version
    }

    pub fn target_nexus_zone_count(&self) -> usize {
        self.policy.target_nexus_zone_count
    }

    pub fn service_ip_pool_ranges(&self) -> &[IpRange] {
        &self.policy.service_ip_pool_ranges
    }

    pub fn all_sleds(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = (TypedUuid<SledKind>, &SledDetails)> + '_ {
        self.sleds.iter().filter_map(move |(&sled_id, details)| {
            filter
                .matches_policy_and_state(details.policy, details.state)
                .then_some((sled_id, details))
        })
    }

    pub fn all_sled_ids(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = TypedUuid<SledKind>> + '_ {
        self.all_sleds(filter).map(|(sled_id, _)| sled_id)
    }

    pub fn all_sled_resources(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = (TypedUuid<SledKind>, &SledResources)> + '_ {
        self.all_sleds(filter)
            .map(|(sled_id, details)| (sled_id, &details.resources))
    }

    pub fn sled_policy(
        &self,
        sled_id: &TypedUuid<SledKind>,
    ) -> Option<SledPolicy> {
        self.sleds.get(sled_id).map(|details| details.policy)
    }

    pub fn sled_resources(
        &self,
        sled_id: &TypedUuid<SledKind>,
    ) -> Option<&SledResources> {
        self.sleds.get(sled_id).map(|details| &details.resources)
    }

    // Convert this `PlanningInput` back into a [`PlanningInputBuilder`]
    //
    // This is primarily useful for tests that want to mutate an existing
    // `PlanningInput`.
    pub fn into_builder(self) -> PlanningInputBuilder {
        PlanningInputBuilder {
            policy: self.policy,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
            sleds: self.sleds,
            omicron_zone_external_ips: self.omicron_zone_external_ips,
            omicron_zone_nics: self.omicron_zone_nics,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PlanningInputBuildError {
    #[error("duplicate sled ID: {0}")]
    DuplicateSledId(TypedUuid<SledKind>),
    #[error("Omicron zone {zone_id} already has an external IP ({ip:?})")]
    DuplicateOmicronZoneExternalIp {
        zone_id: TypedUuid<OmicronZoneKind>,
        ip: ExternalIp,
    },
    #[error("Omicron zone {zone_id} already has a NIC ({nic:?})")]
    DuplicateOmicronZoneNic {
        zone_id: TypedUuid<OmicronZoneKind>,
        nic: ServiceNetworkInterface,
    },
}

/// Constructor for [`PlanningInput`].
#[derive(Clone, Debug)]
pub struct PlanningInputBuilder {
    policy: Policy,
    internal_dns_version: Generation,
    external_dns_version: Generation,
    sleds: BTreeMap<TypedUuid<SledKind>, SledDetails>,
    omicron_zone_external_ips: BTreeMap<TypedUuid<OmicronZoneKind>, ExternalIp>,
    omicron_zone_nics:
        BTreeMap<TypedUuid<OmicronZoneKind>, ServiceNetworkInterface>,
}

impl PlanningInputBuilder {
    pub const fn empty_input() -> PlanningInput {
        PlanningInput {
            policy: Policy {
                service_ip_pool_ranges: Vec::new(),
                target_nexus_zone_count: 0,
            },
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            sleds: BTreeMap::new(),
            omicron_zone_external_ips: BTreeMap::new(),
            omicron_zone_nics: BTreeMap::new(),
        }
    }

    pub fn new(
        policy: Policy,
        internal_dns_version: Generation,
        external_dns_version: Generation,
    ) -> Self {
        Self {
            policy,
            internal_dns_version,
            external_dns_version,
            sleds: BTreeMap::new(),
            omicron_zone_external_ips: BTreeMap::new(),
            omicron_zone_nics: BTreeMap::new(),
        }
    }

    pub fn add_sled(
        &mut self,
        sled_id: TypedUuid<SledKind>,
        details: SledDetails,
    ) -> Result<(), PlanningInputBuildError> {
        match self.sleds.entry(sled_id) {
            Entry::Vacant(slot) => {
                slot.insert(details);
                Ok(())
            }
            Entry::Occupied(_) => {
                Err(PlanningInputBuildError::DuplicateSledId(sled_id))
            }
        }
    }

    pub fn add_omicron_zone_external_ip(
        &mut self,
        zone_id: TypedUuid<OmicronZoneKind>,
        ip: ExternalIp,
    ) -> Result<(), PlanningInputBuildError> {
        match self.omicron_zone_external_ips.entry(zone_id) {
            Entry::Vacant(slot) => {
                slot.insert(ip);
                Ok(())
            }
            Entry::Occupied(prev) => {
                Err(PlanningInputBuildError::DuplicateOmicronZoneExternalIp {
                    zone_id,
                    ip: prev.get().clone(),
                })
            }
        }
    }

    pub fn add_omicron_zone_nic(
        &mut self,
        zone_id: TypedUuid<OmicronZoneKind>,
        nic: ServiceNetworkInterface,
    ) -> Result<(), PlanningInputBuildError> {
        match self.omicron_zone_nics.entry(zone_id) {
            Entry::Vacant(slot) => {
                slot.insert(nic);
                Ok(())
            }
            Entry::Occupied(prev) => {
                Err(PlanningInputBuildError::DuplicateOmicronZoneNic {
                    zone_id,
                    nic: prev.get().clone(),
                })
            }
        }
    }

    pub fn policy_mut(&mut self) -> &mut Policy {
        &mut self.policy
    }

    pub fn sleds(&mut self) -> &BTreeMap<TypedUuid<SledKind>, SledDetails> {
        &self.sleds
    }

    pub fn sleds_mut(
        &mut self,
    ) -> &mut BTreeMap<TypedUuid<SledKind>, SledDetails> {
        &mut self.sleds
    }

    pub fn set_internal_dns_version(&mut self, new_version: Generation) {
        self.internal_dns_version = new_version;
    }

    pub fn set_external_dns_version(&mut self, new_version: Generation) {
        self.external_dns_version = new_version;
    }

    pub fn build(self) -> PlanningInput {
        PlanningInput {
            policy: self.policy,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
            sleds: self.sleds,
            omicron_zone_external_ips: self.omicron_zone_external_ips,
            omicron_zone_nics: self.omicron_zone_nics,
        }
    }
}
