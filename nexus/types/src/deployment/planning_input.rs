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
use clap::ValueEnum;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::api::internal::shared::SourceNatConfigError;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use strum::IntoEnumIterator;
use uuid::Uuid;

/// Describes a single disk already managed by the sled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDisk {
    pub disk_identity: DiskIdentity,
    pub disk_id: PhysicalDiskUuid,
    pub policy: PhysicalDiskPolicy,
    pub state: PhysicalDiskState,
}

impl SledDisk {
    fn provisionable(&self) -> bool {
        DiskFilter::InService.matches_policy_and_state(self.policy, self.state)
    }
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

/// Filters that apply to zpools.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ZpoolFilter {
    /// All zpools
    All,

    /// All zpools which are in-service.
    InService,
}

impl ZpoolFilter {
    fn matches_policy_and_state(
        self,
        policy: PhysicalDiskPolicy,
        state: PhysicalDiskState,
    ) -> bool {
        match self {
            ZpoolFilter::All => true,
            ZpoolFilter::InService => match (policy, state) {
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
    pub zpools: BTreeMap<ZpoolUuid, SledDisk>,

    /// the IPv6 subnet of this sled on the underlay network
    ///
    /// (implicitly specifies the whole range of addresses that the planner can
    /// use for control plane components)
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl SledResources {
    /// Returns if the zpool is provisionable (known, in-service, and active).
    pub fn zpool_is_provisionable(&self, zpool: &ZpoolUuid) -> bool {
        let Some(disk) = self.zpools.get(zpool) else { return false };
        disk.provisionable()
    }

    /// Returns all zpools matching the given filter.
    pub fn all_zpools(
        &self,
        filter: ZpoolFilter,
    ) -> impl Iterator<Item = &ZpoolUuid> + '_ {
        self.zpools.iter().filter_map(move |(zpool, disk)| {
            filter
                .matches_policy_and_state(disk.policy, disk.state)
                .then_some(zpool)
        })
    }

    pub fn all_disks(
        &self,
        filter: DiskFilter,
    ) -> impl Iterator<Item = (&ZpoolUuid, &SledDisk)> + '_ {
        self.zpools.iter().filter_map(move |(zpool, disk)| {
            filter
                .matches_policy_and_state(disk.policy, disk.state)
                .then_some((zpool, disk))
        })
    }
}

/// External IP variants possible for Omicron-managed zones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OmicronZoneExternalIp {
    Floating(OmicronZoneExternalFloatingIp),
    Snat(OmicronZoneExternalSnatIp),
    // We may eventually want `Ephemeral(_)` too (arguably Nexus could be
    // ephemeral?), but for now we only have Floating and Snat uses.
}

impl OmicronZoneExternalIp {
    pub fn id(&self) -> ExternalIpUuid {
        match self {
            OmicronZoneExternalIp::Floating(ext) => ext.id,
            OmicronZoneExternalIp::Snat(ext) => ext.id,
        }
    }

    pub fn ip(&self) -> IpAddr {
        match self {
            OmicronZoneExternalIp::Floating(ext) => ext.ip,
            OmicronZoneExternalIp::Snat(ext) => ext.snat_cfg.ip,
        }
    }
}

/// Floating external IP allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning, and requires that the zone have a single
/// IP.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
pub struct OmicronZoneExternalFloatingIp {
    pub id: ExternalIpUuid,
    pub ip: IpAddr,
}

/// Floating external address with port allocated to an Omicron-managed zone.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
pub struct OmicronZoneExternalFloatingAddr {
    pub id: ExternalIpUuid,
    pub addr: SocketAddr,
}

impl OmicronZoneExternalFloatingAddr {
    pub fn into_ip(self) -> OmicronZoneExternalFloatingIp {
        OmicronZoneExternalFloatingIp { id: self.id, ip: self.addr.ip() }
    }
}

/// SNAT (outbound) external IP allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning, and requires that the zone have a single
/// IP.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
pub struct OmicronZoneExternalSnatIp {
    pub id: ExternalIpUuid,
    pub snat_cfg: SourceNatConfig,
}

/// Network interface allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ServiceNetworkInterface` that only stores
/// the fields necessary for blueprint planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OmicronZoneNic {
    pub id: Uuid,
    pub mac: MacAddr,
    pub ip: IpAddr,
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, ValueEnum)]
pub enum SledFilter {
    // ---
    // Prefer to keep this list in alphabetical order.
    // ---
    /// All sleds that are currently part of the control plane cluster.
    ///
    /// Intentionally omits decommissioned sleds, but is otherwise the filter to
    /// fetch "all sleds regardless of current policy or state".
    Commissioned,

    /// Sleds that are eligible for discretionary services.
    Discretionary,

    /// Sleds that are in service (even if they might not be eligible for
    /// discretionary services).
    InService,

    /// Sleds whose sled agents should be queried for inventory
    QueryDuringInventory,

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
                SledFilter::Commissioned => true,
                SledFilter::Discretionary => true,
                SledFilter::InService => true,
                SledFilter::QueryDuringInventory => true,
                SledFilter::ReservationCreate => true,
                SledFilter::VpcFirewall => true,
            },
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            } => match filter {
                SledFilter::Commissioned => true,
                SledFilter::Discretionary => false,
                SledFilter::InService => true,
                SledFilter::QueryDuringInventory => true,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcFirewall => true,
            },
            SledPolicy::Expunged => match filter {
                SledFilter::Commissioned => true,
                SledFilter::Discretionary => false,
                SledFilter::InService => false,
                SledFilter::QueryDuringInventory => false,
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
                SledFilter::Commissioned => true,
                SledFilter::Discretionary => true,
                SledFilter::InService => true,
                SledFilter::QueryDuringInventory => true,
                SledFilter::ReservationCreate => true,
                SledFilter::VpcFirewall => true,
            },
            SledState::Decommissioned => match filter {
                SledFilter::Commissioned => false,
                SledFilter::Discretionary => false,
                SledFilter::InService => false,
                SledFilter::QueryDuringInventory => false,
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
    sleds: BTreeMap<SledUuid, SledDetails>,

    /// external IPs allocated to Omicron zones
    omicron_zone_external_ips: BTreeMap<OmicronZoneUuid, OmicronZoneExternalIp>,

    /// vNICs allocated to Omicron zones
    omicron_zone_nics: BTreeMap<OmicronZoneUuid, OmicronZoneNic>,
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
    ) -> impl Iterator<Item = (SledUuid, &SledDetails)> + '_ {
        self.sleds.iter().filter_map(move |(&sled_id, details)| {
            filter
                .matches_policy_and_state(details.policy, details.state)
                .then_some((sled_id, details))
        })
    }

    pub fn all_sled_ids(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = SledUuid> + '_ {
        self.all_sleds(filter).map(|(sled_id, _)| sled_id)
    }

    pub fn all_sled_resources(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = (SledUuid, &SledResources)> + '_ {
        self.all_sleds(filter)
            .map(|(sled_id, details)| (sled_id, &details.resources))
    }

    pub fn sled_policy(&self, sled_id: &SledUuid) -> Option<SledPolicy> {
        self.sleds.get(sled_id).map(|details| details.policy)
    }

    pub fn sled_resources(&self, sled_id: &SledUuid) -> Option<&SledResources> {
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
    DuplicateSledId(SledUuid),
    #[error("Omicron zone {zone_id} already has an external IP ({ip:?})")]
    DuplicateOmicronZoneExternalIp {
        zone_id: OmicronZoneUuid,
        ip: OmicronZoneExternalIp,
    },
    #[error("Omicron zone {0} has an ephemeral IP (unsupported)")]
    EphemeralIpUnsupported(OmicronZoneUuid),
    #[error("Omicron zone {zone_id} has a bad SNAT config")]
    BadSnatConfig {
        zone_id: OmicronZoneUuid,
        #[source]
        err: SourceNatConfigError,
    },
    #[error("Omicron zone {zone_id} already has a NIC ({nic:?})")]
    DuplicateOmicronZoneNic { zone_id: OmicronZoneUuid, nic: OmicronZoneNic },
}

/// Constructor for [`PlanningInput`].
#[derive(Clone, Debug)]
pub struct PlanningInputBuilder {
    policy: Policy,
    internal_dns_version: Generation,
    external_dns_version: Generation,
    sleds: BTreeMap<SledUuid, SledDetails>,
    omicron_zone_external_ips: BTreeMap<OmicronZoneUuid, OmicronZoneExternalIp>,
    omicron_zone_nics: BTreeMap<OmicronZoneUuid, OmicronZoneNic>,
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
        sled_id: SledUuid,
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
        zone_id: OmicronZoneUuid,
        ip: OmicronZoneExternalIp,
    ) -> Result<(), PlanningInputBuildError> {
        match self.omicron_zone_external_ips.entry(zone_id) {
            Entry::Vacant(slot) => {
                slot.insert(ip);
                Ok(())
            }
            Entry::Occupied(prev) => {
                Err(PlanningInputBuildError::DuplicateOmicronZoneExternalIp {
                    zone_id,
                    ip: *prev.get(),
                })
            }
        }
    }

    pub fn add_omicron_zone_nic(
        &mut self,
        zone_id: OmicronZoneUuid,
        nic: OmicronZoneNic,
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

    pub fn sleds(&mut self) -> &BTreeMap<SledUuid, SledDetails> {
        &self.sleds
    }

    pub fn sleds_mut(&mut self) -> &mut BTreeMap<SledUuid, SledDetails> {
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
