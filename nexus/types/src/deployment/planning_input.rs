// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types describing inputs the Reconfigurator needs to plan and produce new
//! blueprints.

use super::AddNetworkResourceError;
use super::BlueprintZoneImageSource;
use super::OmicronZoneExternalIp;
use super::OmicronZoneNetworkResources;
use super::OmicronZoneNic;
use crate::deployment::PlannerConfig;
use crate::external_api::views::PhysicalDiskPolicy;
use crate::external_api::views::PhysicalDiskState;
use crate::external_api::views::SledPolicy;
use crate::external_api::views::SledProvisionPolicy;
use crate::external_api::views::SledState;
use crate::inventory::BaseboardId;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use clap::ValueEnum;
use daft::Diffable;
use ipnetwork::IpNetwork;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::api::internal::shared::SourceNatConfigError;
use omicron_common::disk::DiskIdentity;
use omicron_common::policy::SINGLE_NODE_CLICKHOUSE_REDUNDANCY;
use omicron_common::update::ArtifactId;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::error;
use std::fmt;
use strum::Display;
use strum::IntoEnumIterator;

/// Amount of time we're willing to let an MGS-managed update sit in an
/// "impossible preconditions" state waiting for it to settle.
///
/// A typical update flow looks something like this:
///
/// 1. Target device has active version A, inactive version B
/// 2. We generate a blueprint containing an MGS-managed update with
///    preconditions on active A + inactive B
/// 3. We start the update process
/// 4. If we generate an inventory collection at this point, we'll see
///    "inactive version invalid", because we've started to overwrite it. This
///    makes the update impossible due to the precondition on "inactive version
///    B", so we want to replace the pending update details with a new one with
///    precondition "inactive version invalid". But in most cases, the inactive
///    version is only temporarily invalid, because it's actively being updated!
///
/// We have to be willing to reevaluate MGS updates that have become impossible
/// eventually to handle cases like the rack losing power mid-update and leaving
/// the inactive slot with "version invalid" indefinitely. But we don't want to
/// churn on blueprints in the common case of "the preconditions are invalid
/// because we're actively updating". This can also cause thrashing where
/// competing Nexuses looking at different inventory collections believe the
/// preconditions are impossible in different ways. See
/// <https://github.com/oxidecomputer/omicron/issues/8483> for examples.
const MGS_UPDATE_SETTLE_TIMEOUT: TimeDelta = TimeDelta::minutes(5);

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningInput {
    /// fleet-wide policy
    policy: Policy,

    /// current internal DNS version
    internal_dns_version: Generation,

    /// current external DNS version
    external_dns_version: Generation,

    /// current CockroachDB settings
    cockroachdb_settings: CockroachDbSettings,

    /// per-sled policy and resources
    ///
    /// This includes decommissioned sleds -- we always make callers pass in a
    /// filter to ensure that they're working within the right subset of sleds.
    sleds: BTreeMap<SledUuid, SledDetails>,

    /// per-zone network resources
    network_resources: OmicronZoneNetworkResources,

    /// age required to reevaluate impossible MGS updates
    ///
    /// MGS-driven updates include preconditions that must be satisfied for the
    /// update to proceed (and must be met to be considered "possible"). While
    /// an update is running, the state of the target device changes in a way
    /// that breaks these preconditions. To avoid churning and replanning /
    /// reevaluating MGS updates, we set an age at which we'll replace
    /// impossible updates; the planner will _ignore_ updates newer than this
    /// mark under the assumption that they may appear to be impossible because
    /// they're currently in progress.
    ignore_impossible_mgs_updates_since: DateTime<Utc>,
}

impl PlanningInput {
    /// current internal DNS version
    pub fn internal_dns_version(&self) -> Generation {
        self.internal_dns_version
    }

    /// current external DNS version
    pub fn external_dns_version(&self) -> Generation {
        self.external_dns_version
    }

    /// current CockroachDB settings
    pub fn cockroachdb_settings(&self) -> &CockroachDbSettings {
        &self.cockroachdb_settings
    }

    pub fn target_boundary_ntp_zone_count(&self) -> usize {
        self.policy.target_boundary_ntp_zone_count
    }

    pub fn target_nexus_zone_count(&self) -> usize {
        self.policy.target_nexus_zone_count
    }

    pub fn target_internal_dns_zone_count(&self) -> usize {
        self.policy.target_internal_dns_zone_count
    }

    pub fn target_oximeter_zone_count(&self) -> usize {
        self.policy.target_oximeter_zone_count
    }

    pub fn target_cockroachdb_zone_count(&self) -> usize {
        self.policy.target_cockroachdb_zone_count
    }

    pub fn target_cockroachdb_cluster_version(
        &self,
    ) -> CockroachDbClusterVersion {
        self.policy.target_cockroachdb_cluster_version
    }

    pub fn target_crucible_pantry_zone_count(&self) -> usize {
        self.policy.target_crucible_pantry_zone_count
    }

    pub fn target_clickhouse_zone_count(&self) -> usize {
        match self.policy.clickhouse_policy.as_ref().map(|policy| &policy.mode)
        {
            Some(&ClickhouseMode::ClusterOnly { .. }) => 0,
            Some(&ClickhouseMode::SingleNodeOnly)
            | Some(&ClickhouseMode::Both { .. })
            | None => SINGLE_NODE_CLICKHOUSE_REDUNDANCY,
        }
    }

    pub fn target_clickhouse_server_zone_count(&self) -> usize {
        self.policy
            .clickhouse_policy
            .as_ref()
            .map(|policy| usize::from(policy.mode.target_servers()))
            .unwrap_or(0)
    }

    pub fn target_clickhouse_keeper_zone_count(&self) -> usize {
        self.policy
            .clickhouse_policy
            .as_ref()
            .map(|policy| usize::from(policy.mode.target_keepers()))
            .unwrap_or(0)
    }

    pub fn tuf_repo(&self) -> &TufRepoPolicy {
        &self.policy.tuf_repo
    }

    pub fn old_repo(&self) -> &TufRepoPolicy {
        &self.policy.old_repo
    }

    pub fn planner_config(&self) -> &PlannerConfig {
        &self.policy.planner_config
    }

    pub fn service_ip_pool_ranges(&self) -> &[IpRange] {
        &self.policy.service_ip_pool_ranges
    }

    pub fn clickhouse_cluster_enabled(&self) -> bool {
        let Some(clickhouse_policy) = &self.policy.clickhouse_policy else {
            return false;
        };
        clickhouse_policy.mode.cluster_enabled()
    }

    pub fn clickhouse_single_node_enabled(&self) -> bool {
        let Some(clickhouse_policy) = &self.policy.clickhouse_policy else {
            // If there is no policy we assume single-node is enabled.
            return true;
        };
        clickhouse_policy.mode.single_node_enabled()
    }

    pub fn oximeter_read_settings(&self) -> &OximeterReadPolicy {
        &self.policy.oximeter_read_policy
    }

    pub fn oximeter_cluster_read_enabled(&self) -> bool {
        self.policy.oximeter_read_policy.mode.cluster_enabled()
    }

    pub fn oximeter_single_node_read_enabled(&self) -> bool {
        self.policy.oximeter_read_policy.mode.single_node_enabled()
    }

    pub fn all_sleds(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = (SledUuid, &SledDetails)> + Clone + '_ {
        self.sleds.iter().filter_map(move |(&sled_id, details)| {
            filter
                .matches_policy_and_state(details.policy, details.state)
                .then_some((sled_id, details))
        })
    }

    pub fn all_sled_ids(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = SledUuid> + Clone + '_ {
        self.all_sleds(filter).map(|(sled_id, _)| sled_id)
    }

    pub fn all_sled_resources(
        &self,
        filter: SledFilter,
    ) -> impl Iterator<Item = (SledUuid, &SledResources)> + '_ {
        self.all_sleds(filter)
            .map(|(sled_id, details)| (sled_id, &details.resources))
    }

    /// Look up a sled in the planning input, returning an error if it is
    /// missing or filtered out.
    ///
    /// This accepts a [`SledFilter`] to ensure that callers are working within
    /// the right subset of sleds.
    pub fn sled_lookup(
        &self,
        filter: SledFilter,
        sled_id: SledUuid,
    ) -> Result<&SledDetails, SledLookupError> {
        match self.sleds.get(&sled_id) {
            Some(details) => {
                if filter
                    .matches_policy_and_state(details.policy, details.state)
                {
                    Ok(details)
                } else {
                    Err(SledLookupError {
                        sled_id,
                        kind: SledLookupErrorKind::Filtered { filter },
                    })
                }
            }
            None => Err(SledLookupError {
                sled_id,
                kind: SledLookupErrorKind::Missing,
            }),
        }
    }

    pub fn network_resources(&self) -> &OmicronZoneNetworkResources {
        &self.network_resources
    }

    pub fn ignore_impossible_mgs_updates_since(&self) -> DateTime<Utc> {
        self.ignore_impossible_mgs_updates_since
    }

    /// Convert this `PlanningInput` back into a [`PlanningInputBuilder`]
    ///
    /// This is primarily useful for tests that want to mutate an existing
    /// [`PlanningInput`].
    pub fn into_builder(self) -> PlanningInputBuilder {
        PlanningInputBuilder {
            policy: self.policy,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
            cockroachdb_settings: self.cockroachdb_settings,
            sleds: self.sleds,
            network_resources: self.network_resources,
            ignore_impossible_mgs_updates_since: self
                .ignore_impossible_mgs_updates_since,
        }
    }
}

/// Error type for sled lookups in a [`PlanningInput`].
///
/// If looking up a sled in a [`PlanningInput`] fails, it can be either a
/// missing sled, or a sled that was filtered out by the provided filter. This
/// error type distinguishes between those two cases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SledLookupError {
    sled_id: SledUuid,
    kind: SledLookupErrorKind,
}

impl SledLookupError {
    /// Returns the ID of the sled that was looked up.
    pub fn sled_id(&self) -> SledUuid {
        self.sled_id
    }

    /// Returns the kind of error that occurred during the lookup.
    pub fn kind(&self) -> SledLookupErrorKind {
        self.kind
    }
}

impl fmt::Display for SledLookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            SledLookupErrorKind::Filtered { filter } => {
                write!(
                    f,
                    "sled {} was filtered out by the {:?} filter",
                    self.sled_id, filter
                )
            }
            SledLookupErrorKind::Missing => {
                write!(
                    f,
                    "sled {} was not found in the planning input",
                    self.sled_id
                )
            }
        }
    }
}

impl error::Error for SledLookupError {}

/// The error kind for a sled lookup.
///
/// Returned by [`SledLookupError::kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SledLookupErrorKind {
    /// The sled was filtered out by the provided filter.
    Filtered { filter: SledFilter },

    /// The sled was not found in the planning input.
    Missing,
}

/// Describes the current values for any CockroachDB settings that we care
/// about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CockroachDbSettings {
    /// A fingerprint representing the current state of the cluster. This must
    /// be recorded in a blueprint and passed to the `DataStore` function when
    /// changing settings.
    pub state_fingerprint: String,

    /// `version`
    ///
    /// WARNING: This value should _not_ be used to set the
    /// `cluster.preserve_downgrade_option` setting. It can potentially reflect
    /// an internal, intermediate upgrade version (e.g. "22.1-12").
    pub version: String,
    /// `cluster.preserve_downgrade_option`
    pub preserve_downgrade: String,
}

impl CockroachDbSettings {
    pub const fn empty() -> CockroachDbSettings {
        CockroachDbSettings {
            state_fingerprint: String::new(),
            version: String::new(),
            preserve_downgrade: String::new(),
        }
    }
}

/// CockroachDB cluster versions we are aware of.
///
/// CockroachDB can be upgraded from one major version to the next, e.g. v22.1
/// -> v22.2. Each major version introduces changes in how it stores data on
/// disk to support new features, and each major version has support for reading
/// the previous version's data so that it can perform an upgrade. The version
/// of the data format is called the "cluster version", which is distinct from
/// but related to the software version that's being run.
///
/// While software version v22.2 is using cluster version v22.1, it's possible
/// to downgrade back to v22.1. Once the cluster version is upgraded, there's no
/// going back.
///
/// To give us some time to evaluate new versions of the software while
/// retaining a downgrade path, we currently deploy new versions of CockroachDB
/// across two releases of the Oxide software, in a "tick-tock" model:
///
/// - In "tick" releases, we upgrade the version of the
///   CockroachDB software to a new major version, and update
///   `CockroachDbClusterVersion::NEWLY_INITIALIZED`. On upgraded racks, the new
///   version is running with the previous cluster version; on newly-initialized
///   racks, the new version is running with the new cluser version.
/// - In "tock" releases, we change `CockroachDbClusterVersion::POLICY` to the
///   major version we upgraded to in the last "tick" release. This results in a
///   new blueprint that upgrades the cluster version, destroying the downgrade
///   path but allowing us to eventually upgrade to the next release.
///
/// These presently describe major versions of CockroachDB. The order of these
/// must be maintained in the correct order (the first variant must be the
/// earliest version).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    parse_display::Display,
    parse_display::FromStr,
    Deserialize,
    Serialize,
    JsonSchema,
    Diffable,
)]
pub enum CockroachDbClusterVersion {
    #[display("22.1")]
    V22_1,
}

impl CockroachDbClusterVersion {
    /// The hardcoded CockroachDB cluster version we want to be on, used in
    /// [`Policy`].
    ///
    /// /!\ WARNING: If you change this, there is no going back. /!\
    pub const POLICY: CockroachDbClusterVersion =
        CockroachDbClusterVersion::V22_1;

    /// The CockroachDB cluster version created as part of newly-initialized
    /// racks.
    ///
    /// CockroachDB knows how to create a new cluster with the current cluster
    /// version, and how to upgrade the cluster version from the previous major
    /// release, but it does not have any ability to create a new cluster with
    /// the previous major release's cluster version.
    ///
    /// During "tick" releases, newly-initialized racks will be running
    /// this cluster version, which will be one major version newer than the
    /// version specified by `CockroachDbClusterVersion::POLICY`. During "tock"
    /// releases, these versions are the same.
    pub const NEWLY_INITIALIZED: CockroachDbClusterVersion =
        CockroachDbClusterVersion::V22_1;
}

/// Whether to set `cluster.preserve_downgrade_option` and what to set it to.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
    Diffable,
)]
#[serde(tag = "action", content = "data", rename_all = "snake_case")]
pub enum CockroachDbPreserveDowngrade {
    /// Do not modify the setting.
    DoNotModify,
    /// Ensure the setting is set to an empty string.
    AllowUpgrade,
    /// Ensure the setting is set to a given cluster version.
    Set(CockroachDbClusterVersion),
}

impl CockroachDbPreserveDowngrade {
    pub fn is_set(self) -> bool {
        match self {
            CockroachDbPreserveDowngrade::Set(_) => true,
            _ => false,
        }
    }

    pub fn from_optional_string(
        value: &Option<String>,
    ) -> Result<Self, parse_display::ParseError> {
        Ok(match value {
            Some(version) => {
                if version.is_empty() {
                    CockroachDbPreserveDowngrade::AllowUpgrade
                } else {
                    CockroachDbPreserveDowngrade::Set(version.parse()?)
                }
            }
            None => CockroachDbPreserveDowngrade::DoNotModify,
        })
    }

    pub fn to_optional_string(self) -> Option<String> {
        match self {
            CockroachDbPreserveDowngrade::DoNotModify => None,
            CockroachDbPreserveDowngrade::AllowUpgrade => Some(String::new()),
            CockroachDbPreserveDowngrade::Set(version) => {
                Some(version.to_string())
            }
        }
    }
}

impl fmt::Display for CockroachDbPreserveDowngrade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CockroachDbPreserveDowngrade::DoNotModify => {
                write!(f, "(do not modify)")
            }
            CockroachDbPreserveDowngrade::AllowUpgrade => {
                write!(f, "\"\" (allow upgrade)")
            }
            CockroachDbPreserveDowngrade::Set(version) => {
                write!(f, "\"{}\"", version)
            }
        }
    }
}

impl From<CockroachDbClusterVersion> for CockroachDbPreserveDowngrade {
    fn from(value: CockroachDbClusterVersion) -> Self {
        CockroachDbPreserveDowngrade::Set(value)
    }
}

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
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, ValueEnum)]
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
        policy.matches(self) && state.matches(self)
    }
}

impl PhysicalDiskPolicy {
    /// Returns true if self matches the filter
    pub fn matches(self, filter: DiskFilter) -> bool {
        match self {
            PhysicalDiskPolicy::InService => match filter {
                DiskFilter::All => true,
                DiskFilter::InService => true,
            },
            PhysicalDiskPolicy::Expunged => match filter {
                DiskFilter::All => true,
                DiskFilter::InService => false,
            },
        }
    }

    /// Returns all policies matching the given filter.
    ///
    /// This is meant for database access, and is generally paired with
    /// [`PhysicalDiskState::all_matching`]. See `ApplyPhysicalDiskFilterExt` in
    /// nexus-db-model.
    pub fn all_matching(filter: DiskFilter) -> impl Iterator<Item = Self> {
        Self::iter().filter(move |state| state.matches(filter))
    }
}

impl PhysicalDiskState {
    /// Returns true if self matches the filter
    pub fn matches(self, filter: DiskFilter) -> bool {
        match self {
            PhysicalDiskState::Active => match filter {
                DiskFilter::All => true,
                DiskFilter::InService => true,
            },
            PhysicalDiskState::Decommissioned => match filter {
                DiskFilter::All => true,
                DiskFilter::InService => false,
            },
        }
    }

    /// Returns all state matching the given filter.
    ///
    /// This is meant for database access, and is generally paired with
    /// [`PhysicalDiskPolicy::all_matching`]. See `ApplyPhysicalDiskFilterExt` in
    /// nexus-db-model.
    pub fn all_matching(filter: DiskFilter) -> impl Iterator<Item = Self> {
        Self::iter().filter(move |state| state.matches(filter))
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
    // NOTE: I'd really like to make this private, to make it harder to
    // accidentally pick a zpool that is not in-service.
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
        let Some(disk) = self.zpools.get(zpool) else {
            return false;
        };
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
    /// All sleds in the system, regardless of policy or state.
    ///
    /// This returns both commissioned and decommissioned sleds. Use with care.
    All,

    /// All sleds that are currently part of the control plane cluster.
    ///
    /// Intentionally omits decommissioned sleds, but is otherwise the filter to
    /// fetch "all sleds regardless of current policy or state".
    Commissioned,

    /// All sleds that were previously part of the control plane cluster but
    /// have been decommissioned.
    ///
    /// Any sleds matching this filter are expected to no longer be present.
    /// This filter is only useful for historical or debugging purposes, such as
    /// listing decommissioned sleds via `omdb`.
    Decommissioned,

    /// Sleds that are eligible for discretionary services.
    Discretionary,

    /// Sleds that are in service (even if they might not be eligible for
    /// discretionary services).
    InService,

    /// Sleds whose sled agents should be queried for inventory
    QueryDuringInventory,

    /// Sleds on which reservations can be created.
    ReservationCreate,

    /// Sleds which should be sent OPTE V2P mappings and Routing rules.
    VpcRouting,

    /// Sleds which should be sent VPC firewall rules.
    VpcFirewall,

    /// Sleds which should have TUF repo artifacts replicated onto them.
    TufArtifactReplication,

    /// Sleds whose SPs should be updated by Reconfigurator
    SpsUpdatedByReconfigurator,
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
                SledFilter::Commissioned => true,
                SledFilter::Decommissioned => false,
                SledFilter::Discretionary => true,
                SledFilter::InService => true,
                SledFilter::QueryDuringInventory => true,
                SledFilter::ReservationCreate => true,
                SledFilter::VpcRouting => true,
                SledFilter::VpcFirewall => true,
                SledFilter::TufArtifactReplication => true,
                SledFilter::SpsUpdatedByReconfigurator => true,
            },
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            } => match filter {
                SledFilter::All => true,
                SledFilter::Commissioned => true,
                SledFilter::Decommissioned => false,
                SledFilter::Discretionary => false,
                SledFilter::InService => true,
                SledFilter::QueryDuringInventory => true,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcRouting => true,
                SledFilter::VpcFirewall => true,
                SledFilter::TufArtifactReplication => true,
                SledFilter::SpsUpdatedByReconfigurator => true,
            },
            SledPolicy::Expunged => match filter {
                SledFilter::All => true,
                SledFilter::Commissioned => true,
                SledFilter::Decommissioned => true,
                SledFilter::Discretionary => false,
                SledFilter::InService => false,
                SledFilter::QueryDuringInventory => false,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcRouting => false,
                SledFilter::VpcFirewall => false,
                SledFilter::TufArtifactReplication => false,
                SledFilter::SpsUpdatedByReconfigurator => false,
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
                SledFilter::Commissioned => true,
                SledFilter::Decommissioned => false,
                SledFilter::Discretionary => true,
                SledFilter::InService => true,
                SledFilter::QueryDuringInventory => true,
                SledFilter::ReservationCreate => true,
                SledFilter::VpcRouting => true,
                SledFilter::VpcFirewall => true,
                SledFilter::TufArtifactReplication => true,
                SledFilter::SpsUpdatedByReconfigurator => true,
            },
            SledState::Decommissioned => match filter {
                SledFilter::All => true,
                SledFilter::Commissioned => false,
                SledFilter::Decommissioned => true,
                SledFilter::Discretionary => false,
                SledFilter::InService => false,
                SledFilter::QueryDuringInventory => false,
                SledFilter::ReservationCreate => false,
                SledFilter::VpcRouting => false,
                SledFilter::VpcFirewall => false,
                SledFilter::TufArtifactReplication => false,
                SledFilter::SpsUpdatedByReconfigurator => false,
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

    /// desired total number of deployed Boundary NTP zones
    pub target_boundary_ntp_zone_count: usize,

    /// desired total number of deployed Nexus zones
    pub target_nexus_zone_count: usize,

    /// desired total number of internal DNS zones.
    /// Must be <= [`omicron_common::policy::INTERNAL_DNS_REDUNDANCY`]; we
    /// expect it to be exactly equal in general (i.e., we should be running an
    /// internal DNS server on each of the expected reserved addresses).
    pub target_internal_dns_zone_count: usize,

    /// desired total number of deployed Oximeter zones
    pub target_oximeter_zone_count: usize,

    /// desired total number of deployed CockroachDB zones
    pub target_cockroachdb_zone_count: usize,

    /// desired total number of deployed CruciblePantry zones
    pub target_crucible_pantry_zone_count: usize,

    /// desired CockroachDB `cluster.preserve_downgrade_option` setting.
    /// at present this is hardcoded based on the version of CockroachDB we
    /// presently ship and the tick-tock pattern described in RFD 469.
    pub target_cockroachdb_cluster_version: CockroachDbClusterVersion,

    /// Policy information for a replicated clickhouse setup
    ///
    /// If this policy is `None`, then we are using a single node clickhouse
    /// setup. Eventually we will only allow multi-node setups and this will no
    /// longer be an option.
    pub clickhouse_policy: Option<ClickhousePolicy>,

    /// Policy information for defining which ClickHouse setup Oximeter reads
    /// from.
    ///
    /// Eventually we will only allow reads from a cluster and this policy will
    /// no longer exist.
    pub oximeter_read_policy: OximeterReadPolicy,

    /// Desired system software release repository.
    ///
    /// New zones may use artifacts in this repo as their image sources,
    /// and at most one extant zone may be modified to use it or replaced
    /// with one that does.
    pub tuf_repo: TufRepoPolicy,

    /// Previous system software release repository.
    ///
    /// On initial system deployment, both `tuf_repo` and `old_repo` will be set
    /// to `TufRepoPolicy::initial()` (the special TUF repo policy for "we don't
    /// have a TUF repo yet"). After one target release has been set, `tuf_repo`
    /// will reflect that target release and `old_repo` will still be
    /// `TufRepoPolicy::initial()`. Once two or more target releases have been
    /// set, `old_repo` will contain "the previous target release" behind
    /// whatever value is in `tuf_repo`.
    ///
    /// New zones deployed mid-update may use artifacts in this repo as
    /// their image sources. See RFD 565 §9.
    pub old_repo: TufRepoPolicy,

    /// Runtime configuration (feature flags) to control planner behavior.
    pub planner_config: PlannerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OximeterReadPolicy {
    // We set the version as `u32` instead of `Generation` because we later need
    // to convert to `SqlU32` and the value of `Generation` is u64.
    pub version: u32,
    pub mode: OximeterReadMode,
    pub time_created: DateTime<Utc>,
}

impl OximeterReadPolicy {
    pub fn new(version: u32) -> Self {
        OximeterReadPolicy {
            version,
            mode: OximeterReadMode::SingleNode,
            time_created: Utc::now(),
        }
    }
}

/// TUF repo-related policy that's part of the planning input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TufRepoPolicy {
    /// The generation of the target release for the TUF repo.
    pub target_release_generation: Generation,

    /// A description of the target release.
    pub description: TargetReleaseDescription,
}

impl TufRepoPolicy {
    /// Returns the initial TUF repo policy for an Oxide deployment:
    ///
    /// * The target release generation is 1.
    /// * There is no target release.
    #[inline]
    pub fn initial() -> Self {
        Self {
            target_release_generation: Generation::new(),
            description: TargetReleaseDescription::Initial,
        }
    }

    #[inline]
    pub fn description(&self) -> &TargetReleaseDescription {
        &self.description
    }
}

/// Source of artifacts for a given target release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetReleaseDescription {
    /// The initial release source for an Oxide deployment, before any TUF repo
    /// has been provided for upgrades.
    Initial,

    /// A TUF repo: this is the target release once an Oxide deployment has
    /// undergone operator-driven updates.
    TufRepo(TufRepoDescription),
}

impl TargetReleaseDescription {
    pub fn tuf_repo(&self) -> Option<&TufRepoDescription> {
        match self {
            Self::Initial => None,
            Self::TufRepo(tuf_repo) => Some(tuf_repo),
        }
    }

    pub fn zone_image_source(
        &self,
        zone_kind: ZoneKind,
    ) -> Result<BlueprintZoneImageSource, TufRepoContentsError> {
        match self {
            Self::Initial => Ok(BlueprintZoneImageSource::InstallDataset),
            Self::TufRepo(tuf_repo) => {
                // We should have exactly one artifact for a given zone kind in
                // every TUF repo; return an error if we have 0 or more than 1.
                let mut matching_artifacts =
                    tuf_repo.artifacts.iter().filter(|artifact| {
                        zone_kind.is_control_plane_zone_artifact(&artifact.id)
                    });
                let artifact = matching_artifacts
                    .next()
                    .ok_or(TufRepoContentsError::MissingZoneKind(zone_kind))?;
                if let Some(extra_artifact) = matching_artifacts.next() {
                    return Err(
                        TufRepoContentsError::MultipleArtifactsSameZoneKind {
                            artifact1: artifact.id.clone(),
                            artifact2: extra_artifact.id.clone(),
                        },
                    );
                }
                Ok(BlueprintZoneImageSource::from_available_artifact(artifact))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TufRepoContentsError {
    #[error("TUF repo is missing an artifact for zone kind {0:?}")]
    MissingZoneKind(ZoneKind),
    #[error(
        "TUF repo contains 2 or more artifacts for the same zone kind: \
         {artifact1:?}, {artifact2:?}"
    )]
    MultipleArtifactsSameZoneKind {
        artifact1: ArtifactId,
        artifact2: ArtifactId,
    },
}

/// Where oximeter should read from
#[derive(
    Debug,
    Display,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    Diffable,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum OximeterReadMode {
    SingleNode,
    Cluster,
}

impl OximeterReadMode {
    pub fn cluster_enabled(&self) -> bool {
        match self {
            OximeterReadMode::SingleNode => false,
            OximeterReadMode::Cluster => true,
        }
    }

    pub fn single_node_enabled(&self) -> bool {
        match self {
            OximeterReadMode::Cluster { .. } => false,
            OximeterReadMode::SingleNode => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClickhousePolicy {
    pub version: u32,
    pub mode: ClickhouseMode,
    pub time_created: DateTime<Utc>,
}

/// How to deploy clickhouse nodes
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ClickhouseMode {
    SingleNodeOnly,
    ClusterOnly { target_servers: u8, target_keepers: u8 },
    Both { target_servers: u8, target_keepers: u8 },
}

impl ClickhouseMode {
    pub fn cluster_enabled(&self) -> bool {
        match self {
            ClickhouseMode::SingleNodeOnly => false,
            ClickhouseMode::ClusterOnly { .. }
            | ClickhouseMode::Both { .. } => true,
        }
    }

    pub fn single_node_enabled(&self) -> bool {
        match self {
            ClickhouseMode::ClusterOnly { .. } => false,
            ClickhouseMode::SingleNodeOnly | ClickhouseMode::Both { .. } => {
                true
            }
        }
    }

    pub fn target_servers(&self) -> u8 {
        match self {
            ClickhouseMode::SingleNodeOnly => 0,
            ClickhouseMode::ClusterOnly { target_servers, .. } => {
                *target_servers
            }
            ClickhouseMode::Both { target_servers, .. } => *target_servers,
        }
    }

    pub fn target_keepers(&self) -> u8 {
        match self {
            ClickhouseMode::SingleNodeOnly => 0,
            ClickhouseMode::ClusterOnly { target_keepers, .. } => {
                *target_keepers
            }
            ClickhouseMode::Both { target_keepers, .. } => *target_keepers,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledDetails {
    /// current sled policy
    pub policy: SledPolicy,
    /// current sled state
    pub state: SledState,
    /// current resources allocated to this sled
    pub resources: SledResources,
    /// baseboard id for this sled
    pub baseboard_id: BaseboardId,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanningInputBuildError {
    #[error("duplicate sled ID: {0}")]
    DuplicateSledId(SledUuid),
    #[error(
        "Omicron zone {zone_id} has a range of IPs ({ip:?}), only a single IP is supported"
    )]
    NotSingleIp { zone_id: OmicronZoneUuid, ip: IpNetwork },
    #[error(transparent)]
    AddNetworkResource(#[from] AddNetworkResourceError),
    #[error("Omicron zone {0} has an ephemeral IP (unsupported)")]
    EphemeralIpUnsupported(OmicronZoneUuid),
    #[error("Omicron zone {zone_id} has a bad SNAT config")]
    BadSnatConfig {
        zone_id: OmicronZoneUuid,
        #[source]
        err: SourceNatConfigError,
    },
    #[error("sled not found: {0}")]
    SledNotFound(SledUuid),
}

/// Constructor for [`PlanningInput`].
#[derive(Clone, Debug)]
pub struct PlanningInputBuilder {
    policy: Policy,
    internal_dns_version: Generation,
    external_dns_version: Generation,
    cockroachdb_settings: CockroachDbSettings,
    sleds: BTreeMap<SledUuid, SledDetails>,
    network_resources: OmicronZoneNetworkResources,
    ignore_impossible_mgs_updates_since: DateTime<Utc>,
}

impl PlanningInputBuilder {
    pub fn empty_input() -> PlanningInput {
        // This empty input is known to be valid.
        PlanningInput {
            policy: Policy {
                service_ip_pool_ranges: Vec::new(),
                target_boundary_ntp_zone_count: 0,
                target_nexus_zone_count: 0,
                target_internal_dns_zone_count: 0,
                target_oximeter_zone_count: 0,
                target_cockroachdb_zone_count: 0,
                target_cockroachdb_cluster_version:
                    CockroachDbClusterVersion::POLICY,
                target_crucible_pantry_zone_count: 0,
                clickhouse_policy: None,
                oximeter_read_policy: OximeterReadPolicy::new(1),
                tuf_repo: TufRepoPolicy::initial(),
                old_repo: TufRepoPolicy::initial(),
                planner_config: PlannerConfig::default(),
            },
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            cockroachdb_settings: CockroachDbSettings::empty(),
            sleds: BTreeMap::new(),
            network_resources: OmicronZoneNetworkResources::new(),
            ignore_impossible_mgs_updates_since: Utc::now(),
        }
    }

    pub fn new(
        policy: Policy,
        internal_dns_version: Generation,
        external_dns_version: Generation,
        cockroachdb_settings: CockroachDbSettings,
    ) -> Self {
        Self {
            policy,
            internal_dns_version,
            external_dns_version,
            cockroachdb_settings,
            sleds: BTreeMap::new(),
            network_resources: OmicronZoneNetworkResources::new(),
            ignore_impossible_mgs_updates_since: Utc::now()
                - MGS_UPDATE_SETTLE_TIMEOUT,
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

    /// Expunge a sled and all its disks
    ///
    /// In the real code, the `PlanningInput` comes from the database. In this
    /// case all disks in a sled are marked expunged when the sled is expunged
    /// inside a transaction. We do the same thing here for testing purposes.
    pub fn expunge_sled(
        &mut self,
        sled_id: &SledUuid,
    ) -> Result<(), PlanningInputBuildError> {
        let sled_details = self
            .sleds_mut()
            .get_mut(&sled_id)
            .ok_or(PlanningInputBuildError::SledNotFound(*sled_id))?;
        sled_details.policy = SledPolicy::Expunged;
        for (_, sled_disk) in sled_details.resources.zpools.iter_mut() {
            sled_disk.policy = PhysicalDiskPolicy::Expunged;
        }
        Ok(())
    }

    pub fn add_omicron_zone_external_ip(
        &mut self,
        zone_id: OmicronZoneUuid,
        ip: OmicronZoneExternalIp,
    ) -> Result<(), PlanningInputBuildError> {
        Ok(self.network_resources.add_external_ip(zone_id, ip)?)
    }

    pub fn add_omicron_zone_nic(
        &mut self,
        zone_id: OmicronZoneUuid,
        nic: OmicronZoneNic,
    ) -> Result<(), PlanningInputBuildError> {
        Ok(self.network_resources.add_nic(zone_id, nic)?)
    }

    pub fn network_resources_mut(
        &mut self,
    ) -> &mut OmicronZoneNetworkResources {
        &mut self.network_resources
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

    pub fn set_ignore_impossible_mgs_updates_since(
        &mut self,
        since: DateTime<Utc>,
    ) {
        self.ignore_impossible_mgs_updates_since = since;
    }

    pub fn set_cockroachdb_settings(
        &mut self,
        cockroachdb_settings: CockroachDbSettings,
    ) {
        self.cockroachdb_settings = cockroachdb_settings;
    }

    pub fn build(self) -> PlanningInput {
        PlanningInput {
            policy: self.policy,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
            cockroachdb_settings: self.cockroachdb_settings,
            sleds: self.sleds,
            network_resources: self.network_resources,
            ignore_impossible_mgs_updates_since: self
                .ignore_impossible_mgs_updates_since,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CockroachDbClusterVersion;

    #[test]
    fn cockroachdb_cluster_versions() {
        // This should always be true.
        assert!(
            CockroachDbClusterVersion::POLICY
                <= CockroachDbClusterVersion::NEWLY_INITIALIZED
        );

        let cockroachdb_version =
            include_str!("../../../../tools/cockroachdb_version")
                .trim_start_matches('v')
                .rsplit_once('.')
                .unwrap()
                .0;
        assert_eq!(
            CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string(),
            cockroachdb_version
        );

        // In the next "tick" release, this version will be stored in a
        // different file.
        assert_eq!(
            CockroachDbClusterVersion::POLICY.to_string(),
            cockroachdb_version
        );
    }
}
