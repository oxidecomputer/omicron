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
use core::fmt;
use ipnetwork::IpNetwork;
use ipnetwork::NetworkSize;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::VnicUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde::Deserialize;
use serde::Serialize;
use std::collections::btree_map::Entry;
use std::collections::hash_map;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use strum::IntoEnumIterator;

/// Policy and database inputs to the Reconfigurator planner
///
/// The primary inputs to the planner are the parent (either a parent blueprint
/// or an inventory collection) and this structure. This type holds the
/// fleet-wide policy as well as any additional information fetched from CRDB
/// that the planner needs to make decisions.
///
/// The current policy is pretty limited.  It's aimed primarily at supporting
/// the add/remove sled use case.
///
/// The planning input has some internal invariants that code outside of this
/// module can rely on. They include:
///
/// - Each Omicron zone has at most one external IP and at most one vNIC.
/// - A given external IP or vNIC is only associated with a single Omicron
///   zone.
/// - The UUIDs are all unique.
#[derive(Debug, Clone)]
pub struct PlanningInput {
    inner: UnvalidatedPlanningInput,
}

impl PlanningInput {
    /// Creates a new `PlanningInput` from the unvalidated form, returning
    /// errors if the input is invalid.
    pub fn from_unvalidated(
        input: UnvalidatedPlanningInput,
    ) -> Result<Self, PlanningInputValidationError> {
        input.validate()?;
        Ok(Self { inner: input })
    }

    /// Returns the unvalidated planning input contained inside.
    ///
    /// This drops the constraint that the planning input is valid.
    pub fn as_unvalidated(&self) -> &UnvalidatedPlanningInput {
        &self.inner
    }

    /// Converts self into the unvalidated planning input.
    ///
    /// This drops the constraint that the planning input is valid. The
    /// constraint can be restored by calling
    /// [`PlanningInput::from_unvalidated`].
    pub fn into_unvalidated(self) -> UnvalidatedPlanningInput {
        self.inner
    }

    /// Convert this `PlanningInput` back into a [`PlanningInputBuilder`]
    ///
    /// This is primarily useful for tests that want to mutate an existing
    /// [`PlanningInput`].
    pub fn into_builder(self) -> PlanningInputBuilder {
        let inner = self.inner;
        let validation_state = ValidationState::new(
            &inner.omicron_zone_external_ips,
            &inner.omicron_zone_nics,
        )
        .expect("this must be a valid planning input");

        PlanningInputBuilder {
            policy: inner.policy,
            internal_dns_version: inner.internal_dns_version,
            external_dns_version: inner.external_dns_version,
            sleds: inner.sleds,
            validation_state,
        }
    }
}

impl Serialize for PlanningInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // A simple forwarding implementation.
        self.inner.serialize(serializer)
    }
}

/// Deserializes a [`PlanningInput`], validating it along the way.
impl<'de> Deserialize<'de> for PlanningInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let inner = UnvalidatedPlanningInput::deserialize(deserializer)?;
        PlanningInput::from_unvalidated(inner).map_err(D::Error::custom)
    }
}

/// A planning input for which internal invariants have not been validated.
///
/// This is helpful for intermediate deserialization -- so that we can both
/// show what's stored and validate it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnvalidatedPlanningInput {
    /// fleet-wide policy
    policy: Policy,

    /// current internal DNS version
    internal_dns_version: Generation,

    /// current external DNS version
    external_dns_version: Generation,

    /// per-sled policy and resources
    sleds: BTreeMap<SledUuid, SledDetails>,

    /// external IPs allocated to Omicron zones
    omicron_zone_external_ips: BTreeMap<OmicronZoneUuid, ServiceExternalIp>,

    /// vNICs allocated to Omicron zones
    omicron_zone_nics: BTreeMap<OmicronZoneUuid, ServiceNetworkInterface>,
}

impl UnvalidatedPlanningInput {
    /// Validates the planning input, returning errors if the input is invalid.
    pub fn validate(&self) -> Result<(), PlanningInputValidationError> {
        // Simply constructing the validation state is enough to show that the
        // input is valid.
        _ = ValidationState::new(
            &self.omicron_zone_external_ips,
            &self.omicron_zone_nics,
        )?;

        Ok(())
    }

    // (Only used in the macro below.)
    #[inline]
    fn as_unvalidated(&self) -> &Self {
        self
    }
}

macro_rules! impl_planning_input {
    ($($ty:ident),*) => {
        $(impl $ty {
            pub fn internal_dns_version(&self) -> Generation {
                self.as_unvalidated().internal_dns_version
            }

            pub fn external_dns_version(&self) -> Generation {
                self.as_unvalidated().external_dns_version
            }

            pub fn target_nexus_zone_count(&self) -> usize {
                self.as_unvalidated().policy.target_nexus_zone_count
            }

            pub fn service_ip_pool_ranges(&self) -> &[IpRange] {
                &self.as_unvalidated().policy.service_ip_pool_ranges
            }

            pub fn all_sleds(
                &self,
                filter: SledFilter,
            ) -> impl Iterator<Item = (SledUuid, &SledDetails)> + '_ {
                self.as_unvalidated().sleds.iter().filter_map(move |(&sled_id, details)| {
                    filter
                        .matches_policy_and_state(details.policy, details.state)
                        .then_some((sled_id, details))
                })
            }

            pub fn all_sled_ids(
                &self,
                filter: SledFilter,
            ) -> impl Iterator<Item = SledUuid> + '_ {
                self.as_unvalidated().all_sleds(filter).map(|(sled_id, _)| sled_id)
            }

            pub fn all_sled_resources(
                &self,
                filter: SledFilter,
            ) -> impl Iterator<Item = (SledUuid, &SledResources)> + '_ {
                self.as_unvalidated().all_sleds(filter)
                    .map(|(sled_id, details)| (sled_id, &details.resources))
            }

            pub fn sled_policy(
                &self,
                sled_id: &SledUuid,
            ) -> Option<SledPolicy> {
                self.as_unvalidated().sleds.get(sled_id).map(|details| details.policy)
            }

            pub fn sled_resources(
                &self,
                sled_id: &SledUuid,
            ) -> Option<&SledResources> {
                self.as_unvalidated().sleds.get(sled_id).map(|details| &details.resources)
            }
        })*
    };
}

// Many of these operations make sense for both validated and unvalidated
// planning inputs -- this allows callers to show the contents of the planning
// input while validating it separately.
impl_planning_input!(PlanningInput, UnvalidatedPlanningInput);

/// Describes a single disk already managed by the sled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDisk {
    pub disk_identity: DiskIdentity,
    pub disk_id: PhysicalDiskUuid,
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
    pub zpools: BTreeMap<ZpoolUuid, SledDisk>,

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
    ) -> impl Iterator<Item = (&ZpoolUuid, &SledDisk)> + '_ {
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
/// necessary for blueprint planning, and requires that the service have a
/// single IP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceExternalIp {
    pub id: ExternalIpUuid,
    pub ip: IpAddr,
}

/// Network interface allocated to a service
///
/// This is a slimmer `nexus_db_model::ServiceNetworkInterface` that only stores
/// the fields necessary for blueprint planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceNetworkInterface {
    pub id: VnicUuid,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDetails {
    /// current sled policy
    pub policy: SledPolicy,
    /// current sled state
    pub state: SledState,
    /// current resources allocated to this sled
    pub resources: SledResources,
}

/// An error that occurred while validating a planning input.
///
/// Consists of a list of [`PlanningInputBuildError`]s.
#[derive(Debug, thiserror::Error)]
pub struct PlanningInputValidationError {
    pub errors: Vec<PlanningInputBuildError>,
}

impl fmt::Display for PlanningInputValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "validation failed with {} errors:", self.errors.len())?;
        for error in &self.errors {
            writeln!(f, "  - {}", error)?;
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PlanningInputBuildError {
    #[error("duplicate sled ID: {0}")]
    DuplicateSledId(SledUuid),
    #[error("Omicron zone {zone_id} has a range of IPs ({ip:?}), only a single IP is supported")]
    NotSingleIp { zone_id: OmicronZoneUuid, ip: IpNetwork },
    #[error("associating Omicron zone {zone_id} with {ip:?} failed due to duplicates: {}", join_dups(dups))]
    DuplicateOmicronZoneExternalIp {
        zone_id: OmicronZoneUuid,
        ip: ServiceExternalIp,
        dups: Vec<ServiceExternalIpEntry>,
    },
    #[error("associating Omicron zone {zone_id} with {nic:?} failed due to duplicates: {}", join_dups(dups))]
    DuplicateOmicronZoneNic {
        zone_id: OmicronZoneUuid,
        nic: ServiceNetworkInterface,
        dups: Vec<ServiceNicEntry>,
    },
}

fn join_dups<T: fmt::Debug>(dups: &[T]) -> String {
    dups.iter().map(|d| format!("{:?}", d)).collect::<Vec<_>>().join(", ")
}

/// Constructor for [`PlanningInput`].
#[derive(Clone, Debug)]
pub struct PlanningInputBuilder {
    policy: Policy,
    internal_dns_version: Generation,
    external_dns_version: Generation,
    sleds: BTreeMap<SledUuid, SledDetails>,
    validation_state: ValidationState,
}

impl PlanningInputBuilder {
    pub const fn empty_input() -> PlanningInput {
        PlanningInput {
            // This empty input is known to be valid.
            inner: UnvalidatedPlanningInput {
                policy: Policy {
                    service_ip_pool_ranges: Vec::new(),
                    target_nexus_zone_count: 0,
                },
                internal_dns_version: Generation::new(),
                external_dns_version: Generation::new(),
                sleds: BTreeMap::new(),
                omicron_zone_external_ips: BTreeMap::new(),
                omicron_zone_nics: BTreeMap::new(),
            },
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
            validation_state: ValidationState::default(),
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

    /// Like `add_omicron_zone_external_ip`, but can accept an [`IpNetwork`],
    /// validating that the IP is a single address.
    pub fn add_omicron_zone_external_ip_network(
        &mut self,
        zone_id: OmicronZoneUuid,
        ip_id: ExternalIpUuid,
        ip: IpNetwork,
    ) -> Result<(), PlanningInputBuildError> {
        let size = match ip.size() {
            NetworkSize::V4(n) => n as u128,
            NetworkSize::V6(n) => n,
        };
        if size != 1 {
            return Err(PlanningInputBuildError::NotSingleIp { zone_id, ip });
        }

        self.add_omicron_zone_external_ip(
            zone_id,
            ServiceExternalIp { id: ip_id, ip: ip.ip() },
        )
    }

    pub fn add_omicron_zone_external_ip(
        &mut self,
        zone_id: OmicronZoneUuid,
        ip: ServiceExternalIp,
    ) -> Result<(), PlanningInputBuildError> {
        self.validation_state.insert_external_ip(zone_id, ip)
    }

    pub fn add_omicron_zone_nic(
        &mut self,
        zone_id: OmicronZoneUuid,
        nic: ServiceNetworkInterface,
    ) -> Result<(), PlanningInputBuildError> {
        self.validation_state.insert_nic(zone_id, nic)
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
        // The builder incrementally validates the planning input, so we don't
        // need further validation here.
        let (omicron_zone_external_ips, omicron_zone_nics) =
            self.validation_state.into_maps();

        PlanningInput {
            inner: UnvalidatedPlanningInput {
                policy: self.policy,
                internal_dns_version: self.internal_dns_version,
                external_dns_version: self.external_dns_version,
                sleds: self.sleds,
                omicron_zone_external_ips,
                omicron_zone_nics,
            },
        }
    }
}

/// Extra state used to validate the planning input, both incrementally while
/// building an input, and while deserializing/validating an unvalidated input.
///
/// The goal of this validation is to ensure that the blueprint builder is
/// working with a reasonably well-shaped planning input. Otherwise, there are
/// a number of annoying checks the blueprint build has to do.
#[derive(Clone, Debug, Default)]
struct ValidationState {
    // We're ensuring a three-way 1:1:1 (trijective?) mapping between service
    // (zone) UUIDs, external IP UUIDs, and IP addresses. The easiest way to do
    // that is to store a list of structs and a series of maps that index into
    // the struct.
    external_ip_entries: Vec<ServiceExternalIpEntry>,
    zone_id_to_ip_entry: HashMap<OmicronZoneUuid, usize>,
    ip_id_to_ip_entry: HashMap<ExternalIpUuid, usize>,
    ip_to_ip_entry: HashMap<IpAddr, usize>,

    // And similar for vNICs.
    external_mac_entries: Vec<ServiceNicEntry>,
    zone_id_to_mac_entry: HashMap<OmicronZoneUuid, usize>,
    vnic_id_to_mac_entry: HashMap<VnicUuid, usize>,
    mac_to_mac_entry: HashMap<MacAddr, usize>,
}

impl ValidationState {
    fn new(
        external_ips: &BTreeMap<OmicronZoneUuid, ServiceExternalIp>,
        nics: &BTreeMap<OmicronZoneUuid, ServiceNetworkInterface>,
    ) -> Result<Self, PlanningInputValidationError> {
        let mut errors = Vec::new();

        // This is slightly non-kosher: we're constructing this intermediate
        // state and then incrementally adding items to it. However, this is
        // the easiest way to reuse logic between batch and incremental
        // validation.
        let mut state = Self::default();

        for (zone_id, ip) in external_ips {
            if let Err(error) = state.insert_external_ip(*zone_id, ip.clone()) {
                errors.push(error);
            }
        }

        for (zone_id, nic) in nics {
            if let Err(error) = state.insert_nic(*zone_id, nic.clone()) {
                errors.push(error);
            }
        }

        Ok(state)
    }

    fn insert_external_ip(
        &mut self,
        zone_id: OmicronZoneUuid,
        ip: ServiceExternalIp,
    ) -> Result<(), PlanningInputBuildError> {
        let mut dups = BTreeSet::new();

        // Check that the entry doesn't already exist in any of the maps. (We
        // need to do a round of this before we insert the entry, because we
        // don't want to leave the validator in an indeterminate state!)
        let e1 =
            or_insert_dup(self.zone_id_to_ip_entry.entry(zone_id), &mut dups);
        let e2 = or_insert_dup(self.ip_id_to_ip_entry.entry(ip.id), &mut dups);
        let e3 = or_insert_dup(self.ip_to_ip_entry.entry(ip.ip), &mut dups);

        if !dups.is_empty() {
            let dups = dups
                .iter()
                .map(|ix| self.external_ip_entries[*ix].clone())
                .collect();
            return Err(
                PlanningInputBuildError::DuplicateOmicronZoneExternalIp {
                    zone_id,
                    ip,
                    dups,
                },
            );
        }

        // Insert the entry into the maps. Returning early above means all of
        // the Options are Some.
        let next_index = self.external_ip_entries.len();
        self.external_ip_entries
            .push(ServiceExternalIpEntry { zone_id, external_ip: ip });
        e1.unwrap().insert(next_index);
        e2.unwrap().insert(next_index);
        e3.unwrap().insert(next_index);

        Ok(())
    }

    fn insert_nic(
        &mut self,
        zone_id: OmicronZoneUuid,
        nic: ServiceNetworkInterface,
    ) -> Result<(), PlanningInputBuildError> {
        let mut dups = BTreeSet::new();

        // Check that the entry doesn't already exist in any of the maps. (We
        // need to do a round of this before we insert the entry, because we
        // don't want to leave the validator in an indeterminate state!)
        let e1 =
            or_insert_dup(self.zone_id_to_mac_entry.entry(zone_id), &mut dups);
        let e2 =
            or_insert_dup(self.vnic_id_to_mac_entry.entry(nic.id), &mut dups);
        let e3 = or_insert_dup(self.mac_to_mac_entry.entry(nic.mac), &mut dups);

        if !dups.is_empty() {
            let dups = dups
                .iter()
                .map(|ix| self.external_mac_entries[*ix].clone())
                .collect();
            return Err(PlanningInputBuildError::DuplicateOmicronZoneNic {
                zone_id,
                nic,
                dups,
            });
        }

        // Insert the entry into the maps. Returning early above means all of
        // the Options are Some.
        let next_index = self.external_mac_entries.len();
        self.external_mac_entries.push(ServiceNicEntry { zone_id, nic });
        e1.unwrap().insert(next_index);
        e2.unwrap().insert(next_index);
        e3.unwrap().insert(next_index);

        Ok(())
    }

    fn into_maps(
        self,
    ) -> (
        BTreeMap<OmicronZoneUuid, ServiceExternalIp>,
        BTreeMap<OmicronZoneUuid, ServiceNetworkInterface>,
    ) {
        // During construction, we've ensured that the maps are 1:1:1, so no
        // further checks are required before constructing this.
        let external_ips = self
            .external_ip_entries
            .into_iter()
            .map(|entry| (entry.zone_id, entry.external_ip))
            .collect();
        let nics = self
            .external_mac_entries
            .into_iter()
            .map(|entry| (entry.zone_id, entry.nic))
            .collect();
        (external_ips, nics)
    }
}

fn or_insert_dup<'a, K>(
    entry: hash_map::Entry<'a, K, usize>,
    dups: &mut BTreeSet<usize>,
) -> Option<hash_map::VacantEntry<'a, K, usize>> {
    match entry {
        hash_map::Entry::Vacant(slot) => Some(slot),
        hash_map::Entry::Occupied(slot) => {
            dups.insert(*slot.get());
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServiceExternalIpEntry {
    pub zone_id: OmicronZoneUuid,
    pub external_ip: ServiceExternalIp,
}

#[derive(Clone, Debug)]
pub struct ServiceNicEntry {
    pub zone_id: OmicronZoneUuid,
    pub nic: ServiceNetworkInterface,
}
