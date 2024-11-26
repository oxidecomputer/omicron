// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::ip_allocator::IpAllocator;
use crate::planner::rng::PlannerRng;
use crate::planner::zone_needs_expungement;
use crate::planner::ZoneExpungeReason;
use anyhow::anyhow;
use clickhouse_admin_types::OXIMETER_CLUSTER;
use datasets_editor::BlueprintDatasetsEditError;
use ipnet::IpAdd;
use nexus_inventory::now_db_precision;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::ClickhouseClusterConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
use nexus_types::deployment::OmicronZoneExternalFloatingIp;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::deployment::ZpoolName;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::address::CLICKHOUSE_HTTP_PORT;
use omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::DNS_PORT;
use omicron_common::address::NTP_PORT;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use once_cell::unsync::OnceCell;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::mem;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use storage_editor::BlueprintStorageEditor;
use thiserror::Error;

use super::clickhouse::ClickhouseAllocator;
use super::external_networking::ensure_input_networking_records_appear_in_parent_blueprint;
use super::external_networking::BuilderExternalNetworking;
use super::external_networking::ExternalNetworkingChoice;
use super::external_networking::ExternalSnatNetworkingChoice;
use super::internal_dns::DnsSubnetAllocator;
use super::zones::is_already_expunged;
use super::zones::BuilderZoneState;
use super::zones::BuilderZonesConfig;

mod datasets_editor;
mod disks_editor;
mod storage_editor;

/// Errors encountered while assembling blueprints
#[derive(Debug, Error)]
pub enum Error {
    #[error("sled {sled_id}: ran out of available addresses for sled")]
    OutOfAddresses { sled_id: SledUuid },
    #[error(
        "sled {sled_id}: no available zpools for additional {kind:?} zones"
    )]
    NoAvailableZpool { sled_id: SledUuid, kind: ZoneKind },
    #[error("no Nexus zones exist in parent blueprint")]
    NoNexusZonesInParentBlueprint,
    #[error("no Boundary NTP zones exist in parent blueprint")]
    NoBoundaryNtpZonesInParentBlueprint,
    #[error("no external DNS IP addresses are available")]
    NoExternalDnsIpAvailable,
    #[error("no external service IP addresses are available")]
    NoExternalServiceIpAvailable,
    #[error("no system MAC addresses are available")]
    NoSystemMacAddressAvailable,
    #[error("exhausted available OPTE IP addresses for service {kind:?}")]
    ExhaustedOpteIps { kind: ZoneKind },
    #[error(
        "invariant violation: found decommissioned sled with \
         {num_zones} non-expunged zones: {sled_id}"
    )]
    DecommissionedSledWithNonExpungedZones {
        sled_id: SledUuid,
        num_zones: usize,
    },
    #[error("programming error in planner")]
    Planner(#[source] anyhow::Error),
    #[error("no reserved subnets available for DNS")]
    NoAvailableDnsSubnets,
    #[error("can only have {INTERNAL_DNS_REDUNDANCY} internal DNS servers")]
    TooManyDnsServers,
    #[error("planner produced too many {kind:?} zones")]
    TooManyZones { kind: ZoneKind },
    #[error(transparent)]
    BlueprintDatasetsEditError(#[from] BlueprintDatasetsEditError),
}

/// Describes the result of an idempotent "ensure" operation
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Ensure {
    /// a new item was added
    Added,
    /// an existing item was updated
    Updated,
    /// no action was necessary
    NotNeeded,
}

/// Describes whether an idempotent "ensure" operation resulted in multiple
/// actions taken or no action was necessary
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EnsureMultiple {
    /// action was taken within the operation
    Changed {
        /// An item was added to the blueprint
        added: usize,
        /// An item was updated within the blueprint
        updated: usize,
        /// An item was expunged in the blueprint
        expunged: usize,
        /// An item was removed from the blueprint.
        ///
        /// This usually happens after the work of expungment has completed.
        removed: usize,
    },

    /// no action was necessary
    NotNeeded,
}

impl From<EditCounts> for EnsureMultiple {
    fn from(value: EditCounts) -> Self {
        let EditCounts { added, updated, expunged, removed } = value;
        if added == 0 && updated == 0 && expunged == 0 && removed == 0 {
            Self::NotNeeded
        } else {
            Self::Changed { added, updated, expunged, removed }
        }
    }
}

/// Counts of changes made by an operation.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct EditCounts {
    /// An item was added to the blueprint
    pub added: usize,
    /// An item was updated within the blueprint
    pub updated: usize,
    /// An item was expunged in the blueprint
    pub expunged: usize,
    /// An item was removed from the blueprint.
    ///
    /// This usually happens after the work of expungment has completed.
    pub removed: usize,
}

impl EditCounts {
    pub fn zeroes() -> Self {
        Self::default()
    }

    pub fn has_nonzero_counts(&self) -> bool {
        *self != Self::zeroes()
    }

    pub fn accum(self, other: Self) -> Self {
        Self {
            added: self.added + other.added,
            updated: self.updated + other.updated,
            expunged: self.expunged + other.expunged,
            removed: self.removed + other.removed,
        }
    }
}

/// Counts of changes made by `BlueprintStorageEditor`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct StorageEditCounts {
    pub disks: EditCounts,
    pub datasets: EditCounts,
}

/// Counts of changes made by operations that may affect multiple resources.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SledEditCounts {
    pub disks: EditCounts,
    pub datasets: EditCounts,
    pub zones: EditCounts,
}

impl SledEditCounts {
    fn accum(self, other: Self) -> Self {
        Self {
            disks: self.disks.accum(other.disks),
            datasets: self.datasets.accum(other.datasets),
            zones: self.zones.accum(other.zones),
        }
    }
}

impl From<StorageEditCounts> for SledEditCounts {
    fn from(value: StorageEditCounts) -> Self {
        let StorageEditCounts { disks, datasets } = value;
        Self { disks, datasets, zones: EditCounts::zeroes() }
    }
}

/// Describes operations which the BlueprintBuilder has performed to arrive
/// at its state.
///
/// This information is meant to act as a more strongly-typed flavor of
/// "comment", identifying which operations have occurred on the blueprint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum Operation {
    AddZone {
        sled_id: SledUuid,
        kind: ZoneKind,
    },
    UpdateDisks {
        sled_id: SledUuid,
        added: usize,
        updated: usize,
        removed: usize,
    },
    UpdateDatasets {
        sled_id: SledUuid,
        added: usize,
        updated: usize,
        expunged: usize,
        removed: usize,
    },
    ZoneExpunged {
        sled_id: SledUuid,
        reason: ZoneExpungeReason,
        count: usize,
    },
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddZone { sled_id, kind } => {
                write!(f, "sled {sled_id}: added zone: {}", kind.report_str())
            }
            Self::UpdateDisks { sled_id, added, updated, removed } => {
                write!(f, "sled {sled_id}: added {added} disks, updated {updated}, removed {removed} disks")
            }
            Self::UpdateDatasets {
                sled_id,
                added,
                updated,
                expunged,
                removed,
            } => {
                write!(f, "sled {sled_id}: added {added} datasets, updated: {updated}, expunged {expunged}, removed {removed} datasets")
            }
            Self::ZoneExpunged { sled_id, reason, count } => {
                let reason = match reason {
                    ZoneExpungeReason::DiskExpunged => {
                        "zone using expunged disk"
                    }
                    ZoneExpungeReason::SledDecommissioned => {
                        "sled state is decomissioned"
                    }
                    ZoneExpungeReason::SledExpunged => {
                        "sled policy is expunged"
                    }
                    ZoneExpungeReason::ClickhouseClusterDisabled => {
                        "clickhouse cluster disabled via policy"
                    }
                    ZoneExpungeReason::ClickhouseSingleNodeDisabled => {
                        "clickhouse single-node disabled via policy"
                    }
                };
                write!(
                    f,
                    "sled {sled_id}: expunged {count} zones because: {reason}"
                )
            }
        }
    }
}

/// Helper for assembling a blueprint
///
/// There are two basic ways to assemble a new blueprint:
///
/// 1. Build one directly. This would generally only be used once in the
///    lifetime of a rack, to assemble the first blueprint during rack setup.
///    It is also common in tests. To start with a blueprint that contains an
///    empty zone config for some number of sleds, use
///    [`BlueprintBuilder::build_empty_with_sleds`].
///
/// 2. Build one _from_ another blueprint, called the "parent", making changes
///    as desired.  Use [`BlueprintBuilder::new_based_on`] for this.  Once the
///    new blueprint is created, there is no dependency on the parent one.
///    However, the new blueprint can only be made the system's target if its
///    parent is the current target.
pub struct BlueprintBuilder<'a> {
    #[allow(dead_code)]
    log: Logger,

    /// previous blueprint, on which this one will be based
    parent_blueprint: &'a Blueprint,

    /// The latest inventory collection
    #[allow(unused)]
    collection: &'a Collection,

    // These fields are used to allocate resources for sleds.
    input: &'a PlanningInput,
    sled_ip_allocators: BTreeMap<SledUuid, IpAllocator>,
    external_networking: OnceCell<BuilderExternalNetworking<'a>>,
    internal_dns_subnets: OnceCell<DnsSubnetAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    pub(super) zones: BlueprintZonesBuilder<'a>,
    storage: BlueprintStorageEditor,
    sled_state: BTreeMap<SledUuid, SledState>,
    cockroachdb_setting_preserve_downgrade: CockroachDbPreserveDowngrade,

    creator: String,
    operations: Vec<Operation>,
    comments: Vec<String>,

    // Random number generator for new UUIDs
    rng: PlannerRng,
}

impl<'a> BlueprintBuilder<'a> {
    /// Directly construct a `Blueprint` that contains an empty zone config for
    /// the given sleds.
    pub fn build_empty_with_sleds(
        sled_ids: impl Iterator<Item = SledUuid>,
        creator: &str,
    ) -> Blueprint {
        Self::build_empty_with_sleds_impl(
            sled_ids,
            creator,
            PlannerRng::from_entropy(),
        )
    }

    /// A version of [`Self::build_empty_with_sleds`] that allows the
    /// blueprint ID to be generated from a deterministic RNG.
    pub fn build_empty_with_sleds_seeded(
        sled_ids: impl Iterator<Item = SledUuid>,
        creator: &str,
        rng: PlannerRng,
    ) -> Blueprint {
        Self::build_empty_with_sleds_impl(sled_ids, creator, rng)
    }

    fn build_empty_with_sleds_impl(
        sled_ids: impl Iterator<Item = SledUuid>,
        creator: &str,
        mut rng: PlannerRng,
    ) -> Blueprint {
        let blueprint_zones = sled_ids
            .map(|sled_id| {
                let config = BlueprintZonesConfig {
                    generation: Generation::new(),
                    zones: Vec::new(),
                };
                (sled_id, config)
            })
            .collect::<BTreeMap<_, _>>();
        let num_sleds = blueprint_zones.len();
        let sled_state = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| (sled_id, SledState::Active))
            .collect();

        Blueprint {
            id: rng.next_blueprint(),
            blueprint_zones,
            blueprint_disks: BTreeMap::new(),
            blueprint_datasets: BTreeMap::new(),
            sled_state,
            parent_blueprint_id: None,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            clickhouse_cluster_config: None,
            time_created: now_db_precision(),
            creator: creator.to_owned(),
            comment: format!("starting blueprint with {num_sleds} empty sleds"),
        }
    }

    /// Construct a new `BlueprintBuilder` based on a previous blueprint,
    /// starting with no changes from that state
    pub fn new_based_on(
        log: &Logger,
        parent_blueprint: &'a Blueprint,
        input: &'a PlanningInput,
        inventory: &'a Collection,
        creator: &str,
    ) -> anyhow::Result<BlueprintBuilder<'a>> {
        let log = log.new(o!(
            "component" => "BlueprintBuilder",
            "parent_id" => parent_blueprint.id.to_string(),
        ));

        // Prefer the sled state from our parent blueprint for sleds
        // that were in it; there may be new sleds in `input`, in which
        // case we'll use their current state as our starting point.
        let mut sled_state = parent_blueprint.sled_state.clone();
        let mut commissioned_sled_ids = BTreeSet::new();
        for (sled_id, details) in input.all_sleds(SledFilter::Commissioned) {
            commissioned_sled_ids.insert(sled_id);
            sled_state.entry(sled_id).or_insert(details.state);
        }

        // Make a garbage collection pass through `sled_state`. We want to keep
        // any sleds which either:
        //
        // 1. do not have a desired state of `Decommissioned`
        // 2. do have a desired state of `Decommissioned` and are still included
        //    in our input's list of commissioned sleds
        //
        // Sleds that don't fall into either of these cases have reached the
        // actual `Decommissioned` state, which means we no longer need to carry
        // forward that desired state.
        sled_state.retain(|sled_id, state| {
            *state != SledState::Decommissioned
                || commissioned_sled_ids.contains(sled_id)
        });

        Ok(BlueprintBuilder {
            log,
            parent_blueprint,
            collection: inventory,
            input,
            sled_ip_allocators: BTreeMap::new(),
            external_networking: OnceCell::new(),
            internal_dns_subnets: OnceCell::new(),
            zones: BlueprintZonesBuilder::new(parent_blueprint),
            storage: BlueprintStorageEditor::new(
                parent_blueprint.blueprint_disks.clone(),
                parent_blueprint.blueprint_datasets.clone(),
            ),
            sled_state,
            cockroachdb_setting_preserve_downgrade: parent_blueprint
                .cockroachdb_setting_preserve_downgrade,
            creator: creator.to_owned(),
            operations: Vec::new(),
            comments: Vec::new(),
            rng: PlannerRng::from_entropy(),
        })
    }

    // Helper method to create a `BuilderExternalNetworking` allocator at the
    // point of first use. This is to work around some timing issues between the
    // planner and the builder: `BuilderExternalNetworking` wants to know what
    // resources are in use by running zones, but the planner constructs the
    // `BlueprintBuilder` before it expunges zones. We want to wait to look at
    // resources are in use until after that expungement happens; this
    // implicitly does that by virtue of knowing that the planner won't try to
    // allocate new external networking until after it's done expunging and has
    // started to provision new zones.
    //
    // This is gross and fragile. We should clean up the planner/builder split
    // soon.
    fn external_networking(
        &mut self,
    ) -> Result<&mut BuilderExternalNetworking<'a>, Error> {
        self.external_networking
            .get_or_try_init(|| {
                // Check the planning input: there shouldn't be any external
                // networking resources in the database (the source of `input`)
                // that we don't know about from the parent blueprint.
                ensure_input_networking_records_appear_in_parent_blueprint(
                    self.parent_blueprint,
                    self.input,
                )?;

                BuilderExternalNetworking::new(
                    self.zones
                        .current_zones(BlueprintZoneFilter::ShouldBeRunning)
                        .flat_map(|(_sled_id, zone_config)| zone_config),
                    self.zones
                        .current_zones(BlueprintZoneFilter::Expunged)
                        .flat_map(|(_sled_id, zone_config)| zone_config),
                    self.input.service_ip_pool_ranges(),
                )
            })
            .map_err(Error::Planner)?;
        Ok(self.external_networking.get_mut().unwrap())
    }

    // See `external_networking`; this is a similar helper for DNS subnet
    // allocation, with deferred creation for the same reasons.
    fn internal_dns_subnets(
        &mut self,
    ) -> Result<&mut DnsSubnetAllocator, Error> {
        self.internal_dns_subnets.get_or_try_init(|| {
            DnsSubnetAllocator::new(
                self.zones
                    .current_zones(BlueprintZoneFilter::ShouldBeRunning)
                    .flat_map(|(_sled_id, zone_config)| zone_config),
                self.input,
            )
        })?;
        Ok(self.internal_dns_subnets.get_mut().unwrap())
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> {
        self.zones.sled_ids_with_zones()
    }

    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        self.zones.current_sled_zones(sled_id, filter).map(|(config, _)| config)
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self) -> Blueprint {
        // Collect the Omicron zones config for all sleds, including sleds that
        // are no longer in service and need expungement work.
        let blueprint_zones = self
            .zones
            .into_zones_map(self.input.all_sled_ids(SledFilter::Commissioned));
        let (blueprint_disks, blueprint_datasets) =
            self.storage.into_blueprint_maps(
                self.input.all_sled_ids(SledFilter::InService),
            );

        // If we have the clickhouse cluster setup enabled via policy and we
        // don't yet have a `ClickhouseClusterConfiguration`, then we must create
        // one and feed it to our `ClickhouseAllocator`.
        let clickhouse_allocator = if self.input.clickhouse_cluster_enabled() {
            let parent_config = self
                .parent_blueprint
                .clickhouse_cluster_config
                .clone()
                .unwrap_or_else(|| {
                    info!(
                        self.log,
                        concat!(
                            "Clickhouse cluster enabled by policy: ",
                            "generating initial 'ClickhouseClusterConfig' ",
                            "and 'ClickhouseAllocator'"
                        )
                    );
                    ClickhouseClusterConfig::new(
                        OXIMETER_CLUSTER.to_string(),
                        self.rng.next_clickhouse().to_string(),
                    )
                });
            Some(ClickhouseAllocator::new(
                self.log.clone(),
                parent_config,
                self.collection.latest_clickhouse_keeper_membership(),
            ))
        } else {
            if self.parent_blueprint.clickhouse_cluster_config.is_some() {
                info!(
                    self.log,
                    concat!(
                        "clickhouse cluster disabled via policy ",
                        "discarding existing 'ClickhouseAllocator' and ",
                        "the resulting generated 'ClickhouseClusterConfig"
                    )
                );
            }
            None
        };

        // If we have an allocator, use it to generate a new config. If an error
        // is returned then log it and carry over the parent_config.
        let clickhouse_cluster_config = clickhouse_allocator.map(|a| {
            match a.plan(&(&blueprint_zones).into()) {
                Ok(config) => config,
                Err(e) => {
                    error!(self.log, "clickhouse allocator error: {e}");
                    a.parent_config().clone()
                }
            }
        });
        Blueprint {
            id: self.rng.next_blueprint(),
            blueprint_zones,
            blueprint_disks,
            blueprint_datasets,
            sled_state: self.sled_state,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            internal_dns_version: self.input.internal_dns_version(),
            external_dns_version: self.input.external_dns_version(),
            cockroachdb_fingerprint: self
                .input
                .cockroachdb_settings()
                .state_fingerprint
                .clone(),
            cockroachdb_setting_preserve_downgrade: self
                .cockroachdb_setting_preserve_downgrade,
            clickhouse_cluster_config,
            time_created: now_db_precision(),
            creator: self.creator,
            comment: self
                .comments
                .into_iter()
                .chain(self.operations.iter().map(|op| op.to_string()))
                .collect::<Vec<String>>()
                .join(", "),
        }
    }

    /// Set the desired state of the given sled.
    pub fn set_sled_state(
        &mut self,
        sled_id: SledUuid,
        desired_state: SledState,
    ) {
        self.sled_state.insert(sled_id, desired_state);
    }

    /// Within tests, set an RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn set_rng(&mut self, rng: PlannerRng) -> &mut Self {
        self.rng = rng;
        self
    }

    /// This is a short human-readable string summarizing the changes reflected
    /// in the blueprint. This is only intended for debugging.
    pub fn comment<S>(&mut self, comment: S)
    where
        String: From<S>,
    {
        self.comments.push(String::from(comment));
    }

    /// Records an operation to the blueprint, identifying what changes have
    /// occurred.
    ///
    /// This is currently intended only for debugging.
    pub(crate) fn record_operation(&mut self, operation: Operation) {
        self.operations.push(operation);
    }

    /// Expunges all zones requiring expungement from a sled.
    ///
    /// Returns a list of zone IDs expunged (excluding zones that were already
    /// expunged). If the list is empty, then the operation was a no-op.
    pub(crate) fn expunge_zones_for_sled(
        &mut self,
        sled_id: SledUuid,
        sled_details: &SledDetails,
    ) -> Result<BTreeMap<OmicronZoneUuid, ZoneExpungeReason>, Error> {
        let log = self.log.new(o!(
            "sled_id" => sled_id.to_string(),
        ));

        // Do any zones need to be marked expunged?
        let mut zones_to_expunge = BTreeMap::new();

        let sled_zones =
            self.zones.current_sled_zones(sled_id, BlueprintZoneFilter::All);
        for (zone_config, state) in sled_zones {
            let zone_id = zone_config.id;
            let log = log.new(o!(
                "zone_id" => zone_id.to_string()
            ));

            let Some(reason) =
                zone_needs_expungement(sled_details, zone_config, &self.input)
            else {
                continue;
            };

            let is_expunged =
                is_already_expunged(zone_config, state).map_err(|error| {
                    Error::Planner(anyhow!(error).context(format!(
                        "for sled {sled_id}, error computing zones to expunge"
                    )))
                })?;

            if !is_expunged {
                match reason {
                    ZoneExpungeReason::DiskExpunged => {
                        info!(
                            &log,
                            "expunged disk with non-expunged zone was found"
                        );
                    }
                    ZoneExpungeReason::SledDecommissioned => {
                        // A sled marked as decommissioned should have no
                        // resources allocated to it. If it does, it's an
                        // illegal state, possibly introduced by a bug elsewhere
                        // in the system -- we need to produce a loud warning
                        // (i.e. an ERROR-level log message) on this, while
                        // still removing the zones.
                        error!(
                            &log,
                            "sled has state Decommissioned, yet has zone \
                             allocated to it; will expunge it"
                        );
                    }
                    ZoneExpungeReason::SledExpunged => {
                        // This is the expected situation.
                        info!(
                            &log,
                            "expunged sled with non-expunged zone found"
                        );
                    }
                    ZoneExpungeReason::ClickhouseClusterDisabled => {
                        info!(
                            &log,
                            "clickhouse cluster disabled via policy, \
                            expunging related zone"
                        );
                    }
                    ZoneExpungeReason::ClickhouseSingleNodeDisabled => {
                        info!(
                            &log,
                            "clickhouse single-node disabled via policy, \
                            expunging related zone"
                        );
                    }
                }

                zones_to_expunge.insert(zone_id, reason);
            }
        }

        if zones_to_expunge.is_empty() {
            debug!(
                log,
                "sled has no zones that need expungement; skipping";
            );
            return Ok(zones_to_expunge);
        }

        let sled_resources = self.sled_resources(sled_id)?;
        let mut sled_storage = self.storage.sled_storage_editor(
            sled_id,
            sled_resources,
            &mut self.rng,
        )?;

        // Now expunge all the zones that need it.
        let removed_zones = {
            let change = self.zones.change_sled_zones(sled_id);
            change
                .expunge_zones(zones_to_expunge.keys().cloned().collect())
                .map_err(|error| {
                    Error::Planner(anyhow!(error).context(format!(
                        "for sled {sled_id}, error expunging zones"
                    )))
                })?
        };

        // Also expunge the datasets of all removed zones.
        for zone in removed_zones {
            sled_storage.expunge_zone_datasets(zone);
        }

        // We're done with `sled_storage`; drop it so the borrow checker is okay
        // with calling other methods on `self` below.
        mem::drop(sled_storage);

        // Finally, add comments describing what happened.
        //
        // Group the zones by their reason for expungement.
        let mut count_disk_expunged = 0;
        let mut count_sled_decommissioned = 0;
        let mut count_sled_expunged = 0;
        let mut count_clickhouse_cluster_disabled = 0;
        let mut count_clickhouse_single_node_disabled = 0;
        for reason in zones_to_expunge.values() {
            match reason {
                ZoneExpungeReason::DiskExpunged => count_disk_expunged += 1,
                ZoneExpungeReason::SledDecommissioned => {
                    count_sled_decommissioned += 1;
                }
                ZoneExpungeReason::SledExpunged => count_sled_expunged += 1,
                ZoneExpungeReason::ClickhouseClusterDisabled => {
                    count_clickhouse_cluster_disabled += 1
                }
                ZoneExpungeReason::ClickhouseSingleNodeDisabled => {
                    count_clickhouse_single_node_disabled += 1
                }
            };
        }
        let count_and_reason = [
            (count_disk_expunged, ZoneExpungeReason::DiskExpunged),
            (count_sled_decommissioned, ZoneExpungeReason::SledDecommissioned),
            (count_sled_expunged, ZoneExpungeReason::SledExpunged),
            (
                count_clickhouse_cluster_disabled,
                ZoneExpungeReason::ClickhouseClusterDisabled,
            ),
            (
                count_clickhouse_single_node_disabled,
                ZoneExpungeReason::ClickhouseSingleNodeDisabled,
            ),
        ];
        for (count, reason) in count_and_reason {
            if count > 0 {
                self.record_operation(Operation::ZoneExpunged {
                    sled_id,
                    reason,
                    count,
                });
            }
        }

        Ok(zones_to_expunge)
    }

    /// Ensures that the blueprint contains disks for a sled which already
    /// exists in the database.
    ///
    /// This operation must perform the following:
    /// - Ensure that any disks / zpools that exist in the database
    ///   are propagated into the blueprint.
    /// - Ensure that any disks that are expunged from the database are
    ///   removed from the blueprint.
    pub fn sled_ensure_disks(
        &mut self,
        sled_id: SledUuid,
        resources: &SledResources,
    ) -> Result<SledEditCounts, Error> {
        // These are the disks known to our (last?) blueprint
        let mut sled_storage = self.storage.sled_storage_editor(
            sled_id,
            resources,
            &mut self.rng,
        )?;
        let blueprint_disk_ids = sled_storage.disk_ids().collect::<Vec<_>>();

        // These are the in-service disks as we observed them in the database,
        // during the planning phase
        let database_disks = resources
            .all_disks(DiskFilter::InService)
            .map(|(zpool, disk)| (disk.disk_id, (zpool, disk)));
        let mut database_disk_ids = BTreeSet::new();

        // Ensure any disk present in the database is also present in the
        // blueprint
        for (disk_id, (zpool, disk)) in database_disks {
            database_disk_ids.insert(disk_id);
            sled_storage.ensure_disk(BlueprintPhysicalDiskConfig {
                identity: disk.disk_identity.clone(),
                id: disk_id,
                pool_id: *zpool,
            });
        }

        // Remove any disks that appear in the blueprint, but not the database
        let mut zones_to_expunge = BTreeSet::new();
        for disk_id in blueprint_disk_ids {
            if !database_disk_ids.contains(&disk_id) {
                if let Some(expunged_zpool) = sled_storage.remove_disk(&disk_id)
                {
                    zones_to_expunge.extend(
                        self.zones
                            .zones_using_zpool(
                                sled_id,
                                BlueprintZoneFilter::ShouldBeRunning,
                                &expunged_zpool,
                            )
                            .map(|zone| zone.id),
                    );
                }
            }
        }
        let mut edit_counts: SledEditCounts = sled_storage.finalize().into();

        // Expunging a zpool necessarily requires also expunging any zones that
        // depended on it.
        for zone_id in zones_to_expunge {
            edit_counts =
                edit_counts.accum(self.sled_expunge_zone(sled_id, zone_id)?);
        }

        Ok(edit_counts)
    }

    /// Ensure that a sled in the blueprint has all the datasets it needs for
    /// its running zones.
    ///
    /// In general calling this method should not be required, as adding zones
    /// also adds their datasets. This is here primarily for (a) tests and (b)
    /// backwards compatibility with blueprints that were created before
    /// datasets were added to them.
    ///
    /// Not covered by this method:
    ///
    /// * Expunging datasets associated with expunged zones (this is handled
    ///   when the zone is expunged)
    /// * Adding or removing the debug and zone root datasets (this is handled
    ///   by `sled_ensure_disks`)
    pub fn sled_ensure_zone_datasets(
        &mut self,
        sled_id: SledUuid,
        resources: &SledResources,
    ) -> Result<EnsureMultiple, Error> {
        let mut sled_storage = self.storage.sled_storage_editor(
            sled_id,
            resources,
            &mut self.rng,
        )?;

        // Ensure that datasets needed for zones exist.
        for (zone, _zone_state) in self
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
        {
            sled_storage.ensure_zone_datasets(zone);
        }

        let StorageEditCounts { disks: disk_edits, datasets: dataset_edits } =
            sled_storage.finalize();
        debug_assert_eq!(
            disk_edits,
            EditCounts::zeroes(),
            "we only edited datasets, not disks"
        );

        Ok(dataset_edits.into())
    }

    fn next_internal_dns_gz_address_index(&self, sled_id: SledUuid) -> u32 {
        let used_internal_dns_gz_address_indices = self
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|z| match z.zone_type {
                BlueprintZoneType::InternalDns(
                    blueprint_zone_type::InternalDns {
                        gz_address_index, ..
                    },
                ) => Some(gz_address_index),
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        // In production, we expect any given sled to have 0 or 1 internal DNS
        // zones. In test environments, we might have as many as 5. Either way,
        // an O(n^2) loop here to find the next unused index is probably fine.
        for i in 0.. {
            if !used_internal_dns_gz_address_indices.contains(&i) {
                return i;
            }
        }
        unreachable!("more than u32::MAX internal DNS zones on one sled");
    }

    pub fn sled_add_zone_internal_dns(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let gz_address_index = self.next_internal_dns_gz_address_index(sled_id);
        let sled_subnet = self.sled_resources(sled_id)?.subnet;
        let rack_subnet = ReservedRackSubnet::from_subnet(sled_subnet);
        let dns_subnet = self.internal_dns_subnets()?.alloc(rack_subnet)?;
        let address = dns_subnet.dns_address();
        let zpool = self.sled_select_zpool(sled_id, ZoneKind::InternalDns)?;
        let zone_type =
            BlueprintZoneType::InternalDns(blueprint_zone_type::InternalDns {
                dataset: OmicronZoneDataset { pool_name: zpool.clone() },
                dns_address: SocketAddrV6::new(address, DNS_PORT, 0, 0),
                http_address: SocketAddrV6::new(address, DNS_HTTP_PORT, 0, 0),
                gz_address: dns_subnet.gz_address(),
                gz_address_index,
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.next_zone(),
            filesystem_pool: Some(zpool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_external_dns(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let id = self.rng.next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.external_networking()?.for_new_external_dns()?;
        let nic = NetworkInterface {
            id: self.rng.next_network_interface(),
            kind: NetworkInterfaceKind::Service { id: id.into_untyped_uuid() },
            name: format!("external-dns-{id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let underlay_address = self.sled_alloc_ip(sled_id)?;
        let http_address =
            SocketAddrV6::new(underlay_address, DNS_HTTP_PORT, 0, 0);
        let dns_address = OmicronZoneExternalFloatingAddr {
            id: self.rng.next_external_ip(),
            addr: SocketAddr::new(external_ip, DNS_PORT),
        };
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::ExternalDns)?;
        let zone_type =
            BlueprintZoneType::ExternalDns(blueprint_zone_type::ExternalDns {
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
                http_address,
                dns_address,
                nic,
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: Some(pool_name),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_ensure_zone_ntp(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<Ensure, Error> {
        // If there's already an NTP zone on this sled, do nothing.
        let has_ntp = self
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .any(|(z, _)| z.zone_type.is_ntp());
        if has_ntp {
            return Ok(Ensure::NotNeeded);
        }

        let ip = self.sled_alloc_ip(sled_id)?;
        let ntp_address = SocketAddrV6::new(ip, NTP_PORT, 0, 0);

        let zone_type =
            BlueprintZoneType::InternalNtp(blueprint_zone_type::InternalNtp {
                address: ntp_address,
            });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.next_zone(),
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_crucible(
        &mut self,
        sled_id: SledUuid,
        zpool_id: ZpoolUuid,
    ) -> Result<Ensure, Error> {
        let pool_name = ZpoolName::new_external(zpool_id);

        // If this sled already has a Crucible zone on this pool, do nothing.
        let has_crucible_on_this_pool = self
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .any(|(z, _)| {
                matches!(
                    &z.zone_type,
                    BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                        dataset,
                        ..
                    })
                    if dataset.pool_name == pool_name
                )
            });
        if has_crucible_on_this_pool {
            return Ok(Ensure::NotNeeded);
        }

        let sled_info = self.sled_resources(sled_id)?;
        if !sled_info.zpools.contains_key(&zpool_id) {
            return Err(Error::Planner(anyhow!(
                "adding crucible zone for sled {:?}: \
                attempted to use unknown zpool {:?}",
                sled_id,
                pool_name
            )));
        }

        let ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::CRUCIBLE_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.next_zone(),
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    /// Return the number of zones of a given kind that would be configured to
    /// run on the given sled if this builder generated a blueprint.
    ///
    /// This value may change before a blueprint is actually generated if
    /// further changes are made to the builder.
    pub fn sled_num_running_zones_of_kind(
        &self,
        sled_id: SledUuid,
        kind: ZoneKind,
    ) -> usize {
        self.zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .filter(|(z, _)| z.zone_type.kind() == kind)
            .count()
    }

    pub fn sled_add_zone_nexus(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        // Whether Nexus should use TLS and what the external DNS servers it
        // should use are currently provided at rack-setup time, and should be
        // consistent across all Nexus instances. We'll assume we can copy them
        // from any other Nexus zone in our parent blueprint.
        //
        // TODO-correctness Once these properties can be changed by a rack
        // operator, this will need more work. At a minimum, if such a change
        // goes through the blueprint system (which seems likely), we'll need to
        // check that we're if this builder is being used to make such a change,
        // that change is also reflected here in a new zone. Perhaps these
        // settings should be part of `Policy` instead?
        let (external_tls, external_dns_servers) = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .find_map(|(_, z)| match &z.zone_type {
                BlueprintZoneType::Nexus(nexus) => Some((
                    nexus.external_tls,
                    nexus.external_dns_servers.clone(),
                )),
                _ => None,
            })
            .ok_or(Error::NoNexusZonesInParentBlueprint)?;
        self.sled_add_zone_nexus_with_config(
            sled_id,
            external_tls,
            external_dns_servers,
        )
    }

    pub fn sled_add_zone_nexus_with_config(
        &mut self,
        sled_id: SledUuid,
        external_tls: bool,
        external_dns_servers: Vec<IpAddr>,
    ) -> Result<(), Error> {
        let nexus_id = self.rng.next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.external_networking()?.for_new_nexus()?;
        let external_ip = OmicronZoneExternalFloatingIp {
            id: self.rng.next_external_ip(),
            ip: external_ip,
        };

        let nic = NetworkInterface {
            id: self.rng.next_network_interface(),
            kind: NetworkInterfaceKind::Service {
                id: nexus_id.into_untyped_uuid(),
            },
            name: format!("nexus-{nexus_id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::NEXUS_INTERNAL_PORT;
        let internal_address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type = BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
            internal_address,
            external_ip,
            nic,
            external_tls,
            external_dns_servers: external_dns_servers.clone(),
        });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: nexus_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_oximeter(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let oximeter_id = self.rng.next_zone();
        let ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::OXIMETER_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: oximeter_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_crucible_pantry(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let pantry_id = self.rng.next_zone();
        let ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::CRUCIBLE_PANTRY_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type = BlueprintZoneType::CruciblePantry(
            blueprint_zone_type::CruciblePantry { address },
        );
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: pantry_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn cockroachdb_preserve_downgrade(
        &mut self,
        version: CockroachDbPreserveDowngrade,
    ) {
        self.cockroachdb_setting_preserve_downgrade = version;
    }

    pub fn sled_add_zone_cockroachdb(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let zone_id = self.rng.next_zone();
        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::CockroachDb)?;
        let port = omicron_common::address::COCKROACH_PORT;
        let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::CockroachDb(blueprint_zone_type::CockroachDb {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_clickhouse(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let id = self.rng.next_zone();
        let underlay_address = self.sled_alloc_ip(sled_id)?;
        let address =
            SocketAddrV6::new(underlay_address, CLICKHOUSE_HTTP_PORT, 0, 0);
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::Clickhouse)?;
        let zone_type =
            BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: Some(pool_name),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_clickhouse_server(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let zone_id = self.rng.next_zone();
        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::ClickhouseServer)?;
        let address =
            SocketAddrV6::new(underlay_ip, CLICKHOUSE_HTTP_PORT, 0, 0);
        let zone_type = BlueprintZoneType::ClickhouseServer(
            blueprint_zone_type::ClickhouseServer {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            },
        );
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_clickhouse_keeper(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let zone_id = self.rng.next_zone();
        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::ClickhouseKeeper)?;
        let port = omicron_common::address::CLICKHOUSE_KEEPER_TCP_PORT;
        let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
        let zone_type = BlueprintZoneType::ClickhouseKeeper(
            blueprint_zone_type::ClickhouseKeeper {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            },
        );
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_promote_internal_ntp_to_boundary_ntp(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        // The upstream NTP/DNS servers and domain _should_ come from Nexus and
        // be modifiable by the operator, but currently can only be set at RSS.
        // We can only promote a new boundary NTP zone by copying these settings
        // from an existing one.
        let (ntp_servers, dns_servers, domain) = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .find_map(|(_, z)| match &z.zone_type {
                BlueprintZoneType::BoundaryNtp(zone_config) => Some((
                    zone_config.ntp_servers.clone(),
                    zone_config.dns_servers.clone(),
                    zone_config.domain.clone(),
                )),
                _ => None,
            })
            .ok_or(Error::NoBoundaryNtpZonesInParentBlueprint)?;

        self.sled_promote_internal_ntp_to_boundary_ntp_with_config(
            sled_id,
            ntp_servers,
            dns_servers,
            domain,
        )
    }

    pub fn sled_promote_internal_ntp_to_boundary_ntp_with_config(
        &mut self,
        sled_id: SledUuid,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let _ = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);

        // Find the internal NTP zone and expunge it.
        let mut internal_ntp_zone_id_iter = sled_zones
            .iter_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|config| {
                if matches!(
                    config.zone().zone_type,
                    BlueprintZoneType::InternalNtp(_)
                ) {
                    Some(config.zone().id)
                } else {
                    None
                }
            });

        // We should have exactly one internal NTP zone.
        let internal_ntp_zone_id =
            internal_ntp_zone_id_iter.next().ok_or_else(|| {
                Error::Planner(anyhow!(
                    "cannot promote internal NTP zone on sled {sled_id}: \
                     no internal NTP zone found"
                ))
            })?;
        if internal_ntp_zone_id_iter.next().is_some() {
            return Err(Error::Planner(anyhow!(
                "sled {sled_id} has multiple internal NTP zones"
            )));
        }
        std::mem::drop(internal_ntp_zone_id_iter);

        // Expunge the internal NTP zone.
        sled_zones.expunge_zone(internal_ntp_zone_id).map_err(|error| {
            Error::Planner(anyhow!(error).context(format!(
                "error expunging internal NTP zone from sled {sled_id}"
            )))
        })?;

        // Add the new boundary NTP zone.
        let new_zone_id = self.rng.next_zone();
        let ExternalSnatNetworkingChoice {
            snat_cfg,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.external_networking()?.for_new_boundary_ntp()?;
        let external_ip = OmicronZoneExternalSnatIp {
            id: self.rng.next_external_ip(),
            snat_cfg,
        };
        let nic = NetworkInterface {
            id: self.rng.next_network_interface(),
            kind: NetworkInterfaceKind::Service {
                id: new_zone_id.into_untyped_uuid(),
            },
            name: format!("ntp-{new_zone_id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let port = omicron_common::address::NTP_PORT;
        let zone_type =
            BlueprintZoneType::BoundaryNtp(blueprint_zone_type::BoundaryNtp {
                address: SocketAddrV6::new(underlay_ip, port, 0, 0),
                ntp_servers,
                dns_servers,
                domain,
                nic,
                external_ip,
            });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        self.sled_add_zone(
            sled_id,
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: new_zone_id,
                filesystem_pool: Some(filesystem_pool),
                zone_type,
            },
        )
    }

    pub fn sled_expunge_zone(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
    ) -> Result<SledEditCounts, Error> {
        let sled_resources = self.sled_resources(sled_id)?;

        let sled_zones = self.zones.change_sled_zones(sled_id);
        let (builder_config, did_expunge) =
            sled_zones.expunge_zone(zone_id).map_err(|error| {
                Error::Planner(
                    anyhow!(error)
                        .context("failed to expunge zone from sled {sled_id}"),
                )
            })?;
        let zone_config = builder_config.zone();

        let mut storage = self.storage.sled_storage_editor(
            sled_id,
            sled_resources,
            &mut self.rng,
        )?;
        storage.expunge_zone_datasets(zone_config);

        let mut edit_counts: SledEditCounts = storage.finalize().into();
        if did_expunge {
            edit_counts.zones.expunged += 1;
        }

        Ok(edit_counts)
    }

    fn sled_add_zone(
        &mut self,
        sled_id: SledUuid,
        zone: BlueprintZoneConfig,
    ) -> Result<(), Error> {
        // Check the sled id and return an appropriate error if it's invalid.
        let sled_resources = self.sled_resources(sled_id)?;
        let mut sled_storage = self.storage.sled_storage_editor(
            sled_id,
            sled_resources,
            &mut self.rng,
        )?;
        sled_storage.ensure_zone_datasets(&zone);

        let sled_zones = self.zones.change_sled_zones(sled_id);
        sled_zones.add_zone(zone).map_err(|error| {
            Error::Planner(
                anyhow!(error)
                    .context(format!("error adding zone to sled {sled_id}")),
            )
        })?;

        Ok(())
    }

    /// Returns a newly-allocated underlay address suitable for use by Omicron
    /// zones
    fn sled_alloc_ip(&mut self, sled_id: SledUuid) -> Result<Ipv6Addr, Error> {
        let sled_subnet = self.sled_resources(sled_id)?.subnet;
        let allocator =
            self.sled_ip_allocators.entry(sled_id).or_insert_with(|| {
                let sled_subnet_addr = sled_subnet.net().prefix();
                let minimum = sled_subnet_addr
                    .saturating_add(u128::from(SLED_RESERVED_ADDRESSES));
                let maximum = sled_subnet_addr
                    .saturating_add(u128::from(CP_SERVICES_RESERVED_ADDRESSES));
                assert!(sled_subnet.net().contains(minimum));
                assert!(sled_subnet.net().contains(maximum));
                let mut allocator = IpAllocator::new(minimum, maximum);

                // We shouldn't need to explicitly reserve the sled's global
                // zone and switch addresses because they should be out of our
                // range, but we do so just to be sure.
                let sled_gz_addr = *get_sled_address(sled_subnet).ip();
                assert!(sled_subnet.net().contains(sled_gz_addr));
                assert!(minimum > sled_gz_addr);
                assert!(maximum > sled_gz_addr);
                let switch_zone_addr = get_switch_zone_address(sled_subnet);
                assert!(sled_subnet.net().contains(switch_zone_addr));
                assert!(minimum > switch_zone_addr);
                assert!(maximum > switch_zone_addr);

                // Record each of the sled's zones' underlay IPs as
                // allocated.
                for (z, _) in self
                    .zones
                    .current_sled_zones(sled_id, BlueprintZoneFilter::All)
                {
                    allocator.reserve(z.underlay_ip());
                }

                allocator
            });

        allocator.alloc().ok_or(Error::OutOfAddresses { sled_id })
    }

    #[cfg(test)]
    pub(crate) fn sled_select_zpool_for_tests(
        &self,
        sled_id: SledUuid,
        zone_kind: ZoneKind,
    ) -> Result<ZpoolName, Error> {
        self.sled_select_zpool(sled_id, zone_kind)
    }

    /// Selects a zpool for this zone type.
    ///
    /// This zpool may be used for either durable storage or transient
    /// storage - the usage is up to the caller.
    ///
    /// If `zone_kind` already exists on `sled_id`, it is prevented
    /// from using the same zpool as existing zones with the same kind.
    fn sled_select_zpool(
        &self,
        sled_id: SledUuid,
        zone_kind: ZoneKind,
    ) -> Result<ZpoolName, Error> {
        // We'll check both the disks available to this sled per our current
        // blueprint and the list of all in-service zpools on this sled per our
        // planning input, and only pick zpools that are available in both.
        let current_sled_disks = self
            .storage
            .current_sled_disks(&sled_id)
            .ok_or(Error::NoAvailableZpool { sled_id, kind: zone_kind })?
            .values()
            .map(|disk_config| disk_config.pool_id)
            .collect::<BTreeSet<_>>();

        let all_in_service_zpools =
            self.sled_resources(sled_id)?.all_zpools(ZpoolFilter::InService);

        // We refuse to choose a zpool for a zone of a given `zone_kind` if this
        // sled already has a durable zone of that kind on the same zpool. Build
        // up a set of invalid zpools for this sled/kind pair.
        let mut skip_zpools = BTreeSet::new();
        for zone_config in self
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
        {
            if let Some(zpool) = zone_config.zone_type.durable_zpool() {
                if zone_kind == zone_config.zone_type.kind() {
                    skip_zpools.insert(zpool);
                }
            }
        }

        for &zpool_id in all_in_service_zpools {
            let zpool_name = ZpoolName::new_external(zpool_id);
            if !skip_zpools.contains(&zpool_name)
                && current_sled_disks.contains(&zpool_id)
            {
                return Ok(zpool_name);
            }
        }
        Err(Error::NoAvailableZpool { sled_id, kind: zone_kind })
    }

    /// Returns the resources for a sled that hasn't been decommissioned.
    fn sled_resources(
        &self,
        sled_id: SledUuid,
    ) -> Result<&'a SledResources, Error> {
        let details = self
            .input
            .sled_lookup(SledFilter::Commissioned, sled_id)
            .map_err(|error| {
                Error::Planner(anyhow!(error).context(format!(
                    "for sled {sled_id}, error looking up resources"
                )))
            })?;
        Ok(&details.resources)
    }

    /// Determine the number of desired external DNS zones by counting
    /// unique addresses in the parent blueprint.
    ///
    /// TODO-cleanup: Remove when external DNS addresses are in the policy.
    pub fn count_parent_external_dns_zones(&self) -> usize {
        self.parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .filter_map(|(_id, zone)| match &zone.zone_type {
                BlueprintZoneType::ExternalDns(dns) => {
                    Some(dns.dns_address.addr.ip())
                }
                _ => None,
            })
            .collect::<HashSet<IpAddr>>()
            .len()
    }

    /// Allow a test to manually add an external DNS address, which could
    /// ordinarily only come from RSS.
    ///
    /// TODO-cleanup: Remove when external DNS addresses are in the policy.
    pub(crate) fn add_external_dns_ip(
        &mut self,
        addr: IpAddr,
    ) -> Result<(), Error> {
        self.external_networking()?.add_external_dns_ip(addr)
    }
}

/// Helper for working with sets of zones on each sled
///
/// Tracking the set of zones is slightly non-trivial because we need to bump
/// the per-sled generation number iff the zones are changed.  So we need to
/// keep track of whether we've changed the zones relative to the parent
/// blueprint.  We do this by keeping a copy of any [`BlueprintZonesConfig`]
/// that we've changed and a _reference_ to the parent blueprint's zones.  This
/// struct makes it easy for callers iterate over the right set of zones.
pub(super) struct BlueprintZonesBuilder<'a> {
    changed_zones: BTreeMap<SledUuid, BuilderZonesConfig>,
    parent_zones: &'a BTreeMap<SledUuid, BlueprintZonesConfig>,
}

impl<'a> BlueprintZonesBuilder<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> BlueprintZonesBuilder {
        BlueprintZonesBuilder {
            changed_zones: BTreeMap::new(),
            parent_zones: &parent_blueprint.blueprint_zones,
        }
    }

    /// Returns a mutable reference to a sled's Omicron zones *because* we're
    /// going to change them.
    ///
    /// This updates internal data structures, and it is recommended that it be
    /// called only when the caller actually wishes to make changes to zones.
    /// But making no changes after calling this does not result in a changed
    /// blueprint. (In particular, the generation number is only updated if
    /// the state of any zones was updated.)
    pub fn change_sled_zones(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BuilderZonesConfig {
        self.changed_zones.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_zones) = self.parent_zones.get(&sled_id) {
                BuilderZonesConfig::from_parent(old_sled_zones)
            } else {
                BuilderZonesConfig::new()
            }
        })
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> {
        let mut sled_ids =
            self.changed_zones.keys().copied().collect::<BTreeSet<_>>();
        for &sled_id in self.parent_zones.keys() {
            sled_ids.insert(sled_id);
        }
        sled_ids.into_iter()
    }

    /// Iterates over the list of `current_sled_zones` for all sled IDs for
    /// which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn current_zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = (SledUuid, Vec<&BlueprintZoneConfig>)> {
        let sled_ids = self.sled_ids_with_zones();
        sled_ids.map(move |sled_id| {
            let zones = self
                .current_sled_zones(sled_id, filter)
                .map(|(zone_config, _)| zone_config)
                .collect();
            (sled_id, zones)
        })
    }

    /// Iterates over the list of Omicron zones currently configured for this
    /// sled in the blueprint that's being built, along with each zone's state
    /// in the builder.
    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
    ) -> Box<dyn Iterator<Item = (&BlueprintZoneConfig, BuilderZoneState)> + '_>
    {
        if let Some(sled_zones) = self.changed_zones.get(&sled_id) {
            Box::new(
                sled_zones.iter_zones(filter).map(|z| (z.zone(), z.state())),
            )
        } else if let Some(parent_zones) = self.parent_zones.get(&sled_id) {
            Box::new(parent_zones.zones.iter().filter_map(move |z| {
                if z.disposition.matches(filter) {
                    Some((z, BuilderZoneState::Unchanged))
                } else {
                    None
                }
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Builds a set of all zones whose filesystem or durable dataset reside on
    /// the given `zpool`.
    pub fn zones_using_zpool<'b>(
        &'b self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
        zpool: &'b ZpoolName,
    ) -> impl Iterator<Item = &'b BlueprintZoneConfig> + 'b {
        self.current_sled_zones(sled_id, filter).filter_map(
            move |(config, _state)| {
                if Some(zpool) == config.filesystem_pool.as_ref()
                    || Some(zpool) == config.zone_type.durable_zpool()
                {
                    Some(config)
                } else {
                    None
                }
            },
        )
    }

    /// Produces an owned map of zones for the sleds recorded in this blueprint
    /// plus any newly-added sleds
    pub fn into_zones_map(
        self,
        added_sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintZonesConfig> {
        // Start with self.changed_zones, which contains entries for any
        // sled whose zones config is changing in this blueprint.
        let mut zones = self
            .changed_zones
            .into_iter()
            .map(|(sled_id, zones)| (sled_id, zones.build()))
            .collect::<BTreeMap<_, _>>();

        // Carry forward any zones from our parent blueprint. This may include
        // zones for decommissioned sleds.
        for (sled_id, parent_zones) in self.parent_zones {
            zones.entry(*sled_id).or_insert_with(|| parent_zones.clone());
        }

        // Finally, insert any newly-added sleds.
        for sled_id in added_sled_ids {
            zones.entry(sled_id).or_insert_with(|| BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![],
            });
        }

        zones
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::blueprint_builder::external_networking::ExternalIpAllocator;
    use crate::example::example;
    use crate::example::ExampleSystemBuilder;
    use crate::example::SimRngState;
    use crate::system::SledBuilder;
    use expectorate::assert_contents;
    use nexus_inventory::CollectionBuilder;
    use nexus_types::deployment::BlueprintDatasetConfig;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::BlueprintDatasetFilter;
    use nexus_types::deployment::BlueprintOrCollectionZoneConfig;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::OmicronZoneNetworkResources;
    use nexus_types::external_api::views::SledPolicy;
    use omicron_common::address::IpRange;
    use omicron_common::disk::DatasetKind;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::DatasetUuid;
    use std::collections::BTreeSet;
    use std::mem;

    pub const DEFAULT_N_SLEDS: usize = 3;

    fn datasets_for_sled(
        blueprint: &Blueprint,
        sled_id: SledUuid,
    ) -> &BTreeMap<DatasetUuid, BlueprintDatasetConfig> {
        &blueprint
            .blueprint_datasets
            .get(&sled_id)
            .unwrap_or_else(|| {
                panic!("Cannot find datasets on missing sled: {sled_id}")
            })
            .datasets
    }

    fn find_dataset<'a>(
        datasets: &'a BTreeMap<DatasetUuid, BlueprintDatasetConfig>,
        zpool: &ZpoolName,
        kind: DatasetKind,
    ) -> &'a BlueprintDatasetConfig {
        datasets.values().find(|dataset| {
            &dataset.pool == zpool &&
                dataset.kind == kind
        }).unwrap_or_else(|| {
            let kinds = datasets.values().map(|d| (&d.id, &d.pool, &d.kind)).collect::<Vec<_>>();
            panic!("Cannot find dataset of type {kind}\nFound the following: {kinds:#?}")
        })
    }

    /// Checks various conditions that should be true for all blueprints
    #[track_caller]
    pub fn verify_blueprint(blueprint: &Blueprint) {
        // There should be no duplicate underlay IPs.
        let mut underlay_ips: BTreeMap<Ipv6Addr, &BlueprintZoneConfig> =
            BTreeMap::new();
        for (_, zone) in
            blueprint.all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
        {
            if let Some(previous) =
                underlay_ips.insert(zone.underlay_ip(), zone)
            {
                panic!(
                    "found duplicate underlay IP {} in zones {} and {}\
                    \n\n\
                    blueprint: {}",
                    zone.underlay_ip(),
                    zone.id,
                    previous.id,
                    blueprint.display(),
                );
            }
        }

        // There should be no duplicate external IPs.
        //
        // Checking this is slightly complicated due to SNAT IPs, so we'll
        // delegate to an `ExternalIpAllocator`, which already contains the
        // logic for dup checking. (`mark_ip_used` fails if the IP is _already_
        // marked as used.)
        //
        // We create this with an empty set of service IP pool ranges; those are
        // used for allocation, which we don't do, and aren't needed for
        // duplicate checking.
        let mut ip_allocator = ExternalIpAllocator::new(&[]);
        for (external_ip, _nic) in blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|(_, zone)| zone.zone_type.external_networking())
        {
            ip_allocator
                .mark_ip_used(&external_ip)
                .expect("no duplicate external IPs in running zones");
        }

        // On any given zpool, we should have at most one zone of any given
        // kind.
        //
        // TODO: we may want a similar check for non-durable datasets?
        let mut kinds_by_zpool: BTreeMap<
            ZpoolUuid,
            BTreeMap<ZoneKind, OmicronZoneUuid>,
        > = BTreeMap::new();
        for (_, zone) in blueprint.all_omicron_zones(BlueprintZoneFilter::All) {
            if let Some(dataset) = zone.zone_type.durable_dataset() {
                let kind = zone.zone_type.kind();
                if let Some(previous) = kinds_by_zpool
                    .entry(dataset.dataset.pool_name.id())
                    .or_default()
                    .insert(kind, zone.id)
                {
                    panic!(
                        "zpool {} has two zones of kind {kind:?}: {} and {}\
                            \n\n\
                            blueprint: {}",
                        dataset.dataset.pool_name,
                        zone.id,
                        previous,
                        blueprint.display(),
                    );
                }
            }
        }

        // All commissioned disks should have debug and zone root datasets.
        for (sled_id, disk_config) in &blueprint.blueprint_disks {
            for disk in &disk_config.disks {
                let zpool = ZpoolName::new_external(disk.pool_id);
                let datasets = datasets_for_sled(&blueprint, *sled_id);

                let dataset =
                    find_dataset(&datasets, &zpool, DatasetKind::Debug);
                assert_eq!(
                    dataset.disposition,
                    BlueprintDatasetDisposition::InService
                );
                let dataset = find_dataset(
                    &datasets,
                    &zpool,
                    DatasetKind::TransientZoneRoot,
                );
                assert_eq!(
                    dataset.disposition,
                    BlueprintDatasetDisposition::InService
                );
            }
        }

        // All zones should have dataset records.
        for (sled_id, zone_config) in
            blueprint.all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
        {
            match blueprint.sled_state.get(&sled_id) {
                // Decommissioned sleds don't keep dataset state around.
                //
                // Normally we wouldn't observe zones from decommissioned sleds
                // anyway, but that's the responsibility of the Planner, not the
                // BlueprintBuilder.
                None | Some(SledState::Decommissioned) => continue,
                Some(SledState::Active) => (),
            }
            let datasets = datasets_for_sled(&blueprint, sled_id);

            let zpool = zone_config.filesystem_pool.as_ref().unwrap();
            let kind = DatasetKind::TransientZone {
                name: storage_editor::zone_name(&zone_config),
            };
            let dataset = find_dataset(&datasets, &zpool, kind);
            assert_eq!(
                dataset.disposition,
                BlueprintDatasetDisposition::InService
            );

            if let Some(durable_dataset) =
                zone_config.zone_type.durable_dataset()
            {
                let zpool = &durable_dataset.dataset.pool_name;
                let dataset =
                    find_dataset(&datasets, &zpool, durable_dataset.kind);
                assert_eq!(
                    dataset.disposition,
                    BlueprintDatasetDisposition::InService
                );
            }
        }

        // All datasets should be on zpools that have disk records.
        for (sled_id, datasets) in &blueprint.blueprint_datasets {
            let sled_disk_zpools = blueprint
                .blueprint_disks
                .get(&sled_id)
                .expect("no disks for sled")
                .disks
                .iter()
                .map(|disk| disk.pool_id)
                .collect::<BTreeSet<_>>();

            for dataset in datasets.datasets.values().filter(|dataset| {
                dataset.disposition.matches(BlueprintDatasetFilter::InService)
            }) {
                assert!(
                    sled_disk_zpools.contains(&dataset.pool.id()),
                    "sled {sled_id} has dataset {dataset:?}, \
                     which references a zpool without an associated disk",
                );
            }
        }
    }

    #[track_caller]
    pub fn assert_planning_makes_no_changes(
        log: &Logger,
        blueprint: &Blueprint,
        input: &PlanningInput,
        test_name: &'static str,
    ) {
        let collection = CollectionBuilder::new("test").build();
        let builder = BlueprintBuilder::new_based_on(
            &log,
            &blueprint,
            &input,
            &collection,
            test_name,
        )
        .expect("failed to create builder");
        let child_blueprint = builder.build();
        verify_blueprint(&child_blueprint);
        let diff = child_blueprint.diff_since_blueprint(&blueprint);
        println!(
            "diff between blueprints (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);
    }

    #[test]
    fn test_initial() {
        // Test creating a blueprint from a collection and verifying that it
        // describes no changes.
        static TEST_NAME: &str = "blueprint_builder_test_initial";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input, blueprint_initial) =
            example(&logctx.log, TEST_NAME);
        verify_blueprint(&blueprint_initial);

        let diff = blueprint_initial.diff_since_collection(&collection);
        // There are some differences with even a no-op diff between a
        // collection and a blueprint, such as new data being added to
        // blueprints like DNS generation numbers.
        println!(
            "collection -> initial blueprint \
             (expected no non-trivial changes):\n{}",
            diff.display()
        );
        assert_contents(
            "tests/output/blueprint_builder_initial_diff.txt",
            &diff.display().to_string(),
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint_initial,
            &input,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_basic() {
        static TEST_NAME: &str = "blueprint_builder_test_basic";
        let logctx = test_setup_log(TEST_NAME);

        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut example, blueprint1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();
        verify_blueprint(&blueprint1);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &example.input,
            &example.collection,
            "test_basic",
        )
        .expect("failed to create builder");

        // The example blueprint should have internal NTP zones on all the
        // existing sleds, plus Crucible zones on all pools.  So if we ensure
        // all these zones exist, we should see no change.
        for (sled_id, sled_resources) in
            example.input.all_sled_resources(SledFilter::Commissioned)
        {
            builder.sled_ensure_disks(sled_id, sled_resources).unwrap();
            builder.sled_ensure_zone_ntp(sled_id).unwrap();
            for pool_id in sled_resources.zpools.keys() {
                builder.sled_ensure_zone_crucible(sled_id, *pool_id).unwrap();
            }
        }

        let blueprint2 = builder.build();
        verify_blueprint(&blueprint2);
        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);
        assert_eq!(diff.sleds_modified.len(), 0);

        // The next step is adding these zones to a new sled.
        let mut sled_id_rng = rng.next_sled_id_rng();
        let new_sled_id = sled_id_rng.next();

        let _ =
            example.system.sled(SledBuilder::new().id(new_sled_id)).unwrap();
        let input = example.system.to_planning_input_builder().unwrap().build();
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            &input,
            &example.collection,
            "test_basic",
        )
        .expect("failed to create builder");
        let new_sled_resources = &input
            .sled_lookup(SledFilter::Commissioned, new_sled_id)
            .unwrap()
            .resources;
        builder.sled_ensure_disks(new_sled_id, new_sled_resources).unwrap();
        builder.sled_ensure_zone_ntp(new_sled_id).unwrap();
        for pool_id in new_sled_resources.zpools.keys() {
            builder.sled_ensure_zone_crucible(new_sled_id, *pool_id).unwrap();
        }
        builder
            .sled_ensure_zone_datasets(new_sled_id, new_sled_resources)
            .unwrap();

        let blueprint3 = builder.build();
        verify_blueprint(&blueprint3);
        let diff = blueprint3.diff_since_blueprint(&blueprint2);
        println!("expecting new NTP and Crucible zones:\n{}", diff.display());

        // No sleds were changed or removed.
        assert_eq!(diff.sleds_modified.len(), 0);
        assert_eq!(diff.sleds_removed.len(), 0);

        // One sled was added.
        assert_eq!(diff.sleds_added.len(), 1);
        let sled_id = diff.sleds_added.first().unwrap();
        let new_sled_zones = diff.zones.added.get(sled_id).unwrap();
        assert_eq!(*sled_id, new_sled_id);
        // The generation number should be newer than the initial default.
        assert!(new_sled_zones.generation_after.unwrap() > Generation::new());

        // All zones' underlay addresses ought to be on the sled's subnet.
        for z in &new_sled_zones.zones {
            assert!(new_sled_resources.subnet.net().contains(z.underlay_ip()));
        }

        // Check for an NTP zone.  Its sockaddr's IP should also be on the
        // sled's subnet.
        assert!(new_sled_zones.zones.iter().any(|z| {
            if let BlueprintOrCollectionZoneConfig::Blueprint(
                BlueprintZoneConfig {
                    zone_type:
                        BlueprintZoneType::InternalNtp(
                            blueprint_zone_type::InternalNtp {
                                address, ..
                            },
                        ),
                    ..
                },
            ) = &z
            {
                assert!(new_sled_resources
                    .subnet
                    .net()
                    .contains(*address.ip()));
                true
            } else {
                false
            }
        }));
        let crucible_pool_names = new_sled_zones
            .zones
            .iter()
            .filter_map(|z| {
                if let BlueprintOrCollectionZoneConfig::Blueprint(
                    BlueprintZoneConfig {
                        zone_type:
                            BlueprintZoneType::Crucible(
                                blueprint_zone_type::Crucible {
                                    address,
                                    dataset,
                                },
                            ),
                        ..
                    },
                ) = &z
                {
                    let ip = address.ip();
                    assert!(new_sled_resources.subnet.net().contains(*ip));
                    Some(dataset.pool_name.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            crucible_pool_names,
            new_sled_resources
                .zpools
                .keys()
                .map(|id| { ZpoolName::new_external(*id) })
                .collect()
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_prune_decommissioned_sleds() {
        static TEST_NAME: &str =
            "blueprint_builder_test_prune_decommissioned_sleds";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input, mut blueprint1) =
            example(&logctx.log, TEST_NAME);
        verify_blueprint(&blueprint1);

        // Mark one sled as having a desired state of decommissioned.
        let decommision_sled_id = blueprint1
            .sled_state
            .keys()
            .copied()
            .next()
            .expect("at least one sled");
        *blueprint1.sled_state.get_mut(&decommision_sled_id).unwrap() =
            SledState::Decommissioned;

        // Change the input to note that the sled is expunged, but still active.
        let mut builder = input.into_builder();
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().policy =
            SledPolicy::Expunged;
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().state =
            SledState::Active;
        let input = builder.build();

        // Generate a new blueprint. This sled should still be included: even
        // though the desired state is decommissioned, the current state is
        // still active, so we should carry it forward.
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &input,
            &collection,
            "test_prune_decommissioned_sleds",
        )
        .expect("created builder")
        .build();
        verify_blueprint(&blueprint2);

        // We carried forward the desired state.
        assert_eq!(
            blueprint2.sled_state.get(&decommision_sled_id).copied(),
            Some(SledState::Decommissioned)
        );

        // Change the input to mark the sled decommissioned. (Normally realizing
        // blueprint2 would make this change.)
        let mut builder = input.into_builder();
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().state =
            SledState::Decommissioned;
        let input = builder.build();

        // Generate a new blueprint. This desired sled state should no longer be
        // present: it has reached the terminal decommissioned state, so there's
        // no more work to be done.
        let blueprint3 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            "test_prune_decommissioned_sleds",
        )
        .expect("created builder")
        .build();
        verify_blueprint(&blueprint3);

        // Ensure we've dropped the decommissioned sled. (We may still have
        // _zones_ for it that need cleanup work, but all state transitions for
        // it are complete.)
        assert_eq!(
            blueprint3.sled_state.get(&decommision_sled_id).copied(),
            None,
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint3,
            &input,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_physical_disks() {
        static TEST_NAME: &str = "blueprint_builder_test_add_physical_disks";
        let logctx = test_setup_log(TEST_NAME);

        // Start with an empty system (sleds with no zones). However, we leave
        // the disks around so that `sled_ensure_disks` can add them.
        let (example, parent) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .create_zones(false)
                .create_disks_in_blueprint(false)
                .build();
        let collection = example.collection;
        let input = example.input;

        {
            // We start empty, and can add a disk
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                &collection,
                "test",
            )
            .expect("failed to create builder");

            // In the map, we expect entries to be present for each sled, but
            // not have any disks in them.
            for sled_id in input.all_sled_ids(SledFilter::InService) {
                let disks = builder
                    .storage
                    .current_sled_disks(&sled_id)
                    .expect("found disks config for sled");
                assert!(
                    disks.is_empty(),
                    "expected empty disks for sled {sled_id}, got {disks:?}"
                );
            }

            for (sled_id, sled_resources) in
                input.all_sled_resources(SledFilter::InService)
            {
                let edits = builder
                    .sled_ensure_disks(sled_id, &sled_resources)
                    .unwrap();
                assert_eq!(
                    edits.disks,
                    EditCounts {
                        added: usize::from(SledBuilder::DEFAULT_NPOOLS),
                        updated: 0,
                        expunged: 0,
                        removed: 0
                    }
                );
                // Each disk addition should also result in a debug + zone root
                // dataset addition.
                assert_eq!(
                    edits.datasets,
                    EditCounts {
                        added: 2 * usize::from(SledBuilder::DEFAULT_NPOOLS),
                        updated: 0,
                        expunged: 0,
                        removed: 0
                    }
                );
            }

            let new_disks = builder
                .storage
                .into_blueprint_maps(input.all_sled_ids(SledFilter::InService))
                .0;
            // We should have disks and a generation bump for every sled.
            let parent_disk_gens = parent
                .blueprint_disks
                .iter()
                .map(|(&sled_id, config)| (sled_id, config.generation));
            for (sled_id, parent_gen) in parent_disk_gens {
                let new_sled_disks = new_disks
                    .get(&sled_id)
                    .expect("found child entry for sled present in parent");
                assert_eq!(new_sled_disks.generation, parent_gen.next());
                assert_eq!(
                    new_sled_disks.disks.len(),
                    usize::from(SledBuilder::DEFAULT_NPOOLS),
                );
            }
        }

        logctx.cleanup_successful();
    }

    // Tests that provisioning zones with durable zones co-locates their zone filesystems.
    #[test]
    fn test_zone_filesystem_zpool_colocated() {
        static TEST_NAME: &str =
            "blueprint_builder_test_zone_filesystem_zpool_colocated";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, blueprint) = example(&logctx.log, TEST_NAME);

        for (_, zone_config) in &blueprint.blueprint_zones {
            for zone in &zone_config.zones {
                // The pool should only be optional for backwards compatibility.
                let filesystem_pool = zone
                    .filesystem_pool
                    .as_ref()
                    .expect("Should have filesystem pool");

                if let Some(durable_pool) = zone.zone_type.durable_zpool() {
                    assert_eq!(durable_pool, filesystem_pool);
                }
            }
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn test_datasets_for_zpools_and_zones() {
        static TEST_NAME: &str = "test_datasets_for_zpools_and_zones";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input, blueprint) = example(&logctx.log, TEST_NAME);

        // Creating the "example" blueprint should already invoke
        // `sled_ensure_datasets`.
        //
        // Verify that it has created the datasets we expect to exist.
        verify_blueprint(&blueprint);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");

        // Before we make any modifications, there should be no work to do.
        //
        // If we haven't changed inputs, the output should be the same!
        for (sled_id, resources) in
            input.all_sled_resources(SledFilter::Commissioned)
        {
            let r =
                builder.sled_ensure_zone_datasets(sled_id, resources).unwrap();
            assert_eq!(r, EnsureMultiple::NotNeeded);
        }

        // Expunge a zone from the blueprint, observe that the dataset is
        // removed.
        let sled_id = input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled present");
        let sled_details =
            input.sled_lookup(SledFilter::Commissioned, sled_id).unwrap();
        let crucible_zone_id = builder
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .find_map(|(zone_config, _)| {
                if zone_config.zone_type.is_crucible() {
                    return Some(zone_config.id);
                }
                None
            })
            .expect("at least one crucible must be present");
        let change = builder.zones.change_sled_zones(sled_id);
        println!("Expunging crucible zone: {crucible_zone_id}");
        let expunged_zones =
            change.expunge_zones(BTreeSet::from([crucible_zone_id])).unwrap();
        assert_eq!(expunged_zones.len(), 1);

        // In the case of Crucible, we have a durable dataset and a transient
        // zone filesystem, so we expect two datasets to be expunged.
        let r = builder
            .storage
            .sled_storage_editor(
                sled_id,
                &sled_details.resources,
                &mut builder.rng,
            )
            .unwrap()
            .expunge_zone_datasets(&expunged_zones[0]);
        assert_eq!(
            r,
            EnsureMultiple::Changed {
                added: 0,
                updated: 0,
                expunged: 2,
                removed: 0
            }
        );
        // Once the datasets are expunged, no further changes will be proposed.
        let r = builder
            .sled_ensure_zone_datasets(sled_id, &sled_details.resources)
            .unwrap();
        assert_eq!(r, EnsureMultiple::NotNeeded);

        let blueprint = builder.build();
        verify_blueprint(&blueprint);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");

        // While the datasets still exist in the input (effectively, the db) we
        // cannot remove them.
        let r = builder
            .sled_ensure_zone_datasets(sled_id, &sled_details.resources)
            .unwrap();
        assert_eq!(r, EnsureMultiple::NotNeeded);

        let blueprint = builder.build();
        verify_blueprint(&blueprint);

        // Find the datasets we've expunged in the blueprint
        let expunged_datasets = blueprint
            .blueprint_datasets
            .get(&sled_id)
            .unwrap()
            .datasets
            .values()
            .filter_map(|dataset_config| {
                if dataset_config.disposition
                    == BlueprintDatasetDisposition::Expunged
                {
                    Some(dataset_config.id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // We saw two datasets being expunged earlier when we called
        // `sled_ensure_datasets` -- validate that this is true when inspecting
        // the blueprint too.
        assert_eq!(expunged_datasets.len(), 2);

        // Remove these two datasets from the input.
        let mut input_builder = input.into_builder();
        let zpools = &mut input_builder
            .sleds_mut()
            .get_mut(&sled_id)
            .unwrap()
            .resources
            .zpools;
        for (_, (_, datasets)) in zpools {
            datasets.retain(|dataset| !expunged_datasets.contains(&dataset.id));
        }
        let input = input_builder.build();

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");

        // Now, we should see the datasets "removed" from the blueprint, since
        // we no longer need to keep around records of their expungement.
        let sled_details =
            input.sled_lookup(SledFilter::Commissioned, sled_id).unwrap();
        let r = builder
            .sled_ensure_zone_datasets(sled_id, &sled_details.resources)
            .unwrap();

        // TODO(https://github.com/oxidecomputer/omicron/issues/6646):
        // Because of the workaround for #6646, we don't actually remove
        // datasets yet.
        //
        // In the future, however, we will.
        assert_eq!(r, EnsureMultiple::NotNeeded);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_nexus_with_no_existing_nexus_zones() {
        static TEST_NAME: &str =
            "blueprint_builder_test_add_nexus_with_no_existing_nexus_zones";
        let logctx = test_setup_log(TEST_NAME);

        // Start with an empty system (sleds with no zones).
        let (example, parent) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .create_zones(false)
                .build();
        let collection = example.collection;
        let input = example.input;

        // Adding a new Nexus zone currently requires copying settings from an
        // existing Nexus zone. `parent` has no zones, so we should fail if we
        // try to add a Nexus zone.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");

        let err = builder
            .sled_add_zone_nexus(
                collection
                    .sled_agents
                    .keys()
                    .next()
                    .copied()
                    .expect("no sleds present"),
            )
            .unwrap_err();

        assert!(
            matches!(err, Error::NoNexusZonesInParentBlueprint),
            "unexpected error {err}"
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_nexus_error_cases() {
        static TEST_NAME: &str = "blueprint_builder_test_add_nexus_error_cases";
        let logctx = test_setup_log(TEST_NAME);
        let (mut collection, mut input, mut parent) =
            example(&logctx.log, TEST_NAME);

        // Remove the Nexus zone from one of the sleds so that
        // `sled_ensure_zone_nexus` can attempt to add a Nexus zone to
        // `sled_id`.
        let sled_id = {
            let mut selected_sled_id = None;
            for (sled_id, sa) in &mut collection.sled_agents {
                let nzones_before_retain = sa.omicron_zones.zones.len();
                sa.omicron_zones.zones.retain(|z| !z.zone_type.is_nexus());
                if sa.omicron_zones.zones.len() < nzones_before_retain {
                    selected_sled_id = Some(*sled_id);
                    // Also remove this zone from the blueprint.
                    let mut removed_nexus = None;
                    parent
                        .blueprint_zones
                        .get_mut(sled_id)
                        .expect("missing sled")
                        .zones
                        .retain(|z| match &z.zone_type {
                            BlueprintZoneType::Nexus(z) => {
                                removed_nexus = Some(z.clone());
                                false
                            }
                            _ => true,
                        });
                    let removed_nexus =
                        removed_nexus.expect("removed Nexus from blueprint");

                    // Also remove this Nexus's external networking resources
                    // from `input`.
                    let mut builder = input.into_builder();
                    let mut new_network_resources =
                        OmicronZoneNetworkResources::new();
                    let old_network_resources = builder.network_resources_mut();
                    for ip in old_network_resources.omicron_zone_external_ips()
                    {
                        if ip.ip.id() != removed_nexus.external_ip.id {
                            new_network_resources
                                .add_external_ip(ip.zone_id, ip.ip)
                                .expect("copied IP to new input");
                        }
                    }
                    for nic in old_network_resources.omicron_zone_nics() {
                        if nic.nic.id.into_untyped_uuid()
                            != removed_nexus.nic.id
                        {
                            new_network_resources
                                .add_nic(nic.zone_id, nic.nic)
                                .expect("copied NIC to new input");
                        }
                    }
                    mem::swap(
                        old_network_resources,
                        &mut new_network_resources,
                    );
                    input = builder.build();

                    break;
                }
            }
            selected_sled_id.expect("found no sleds with Nexus zone")
        };

        {
            // Attempting to add Nexus to the sled we removed it from (with no
            // other changes to the environment) should succeed.
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                &collection,
                "test",
            )
            .expect("failed to create builder");
            builder.sled_add_zone_nexus(sled_id).expect("added nexus zone");
        }

        {
            // Attempting to add multiple Nexus zones to the sled we removed it
            // from (with no other changes to the environment) should also
            // succeed.
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                &collection,
                "test",
            )
            .expect("failed to create builder");
            for _ in 0..3 {
                builder.sled_add_zone_nexus(sled_id).expect("added nexus zone");
            }
        }

        {
            // Replace the policy's external service IP pool ranges with ranges
            // that are already in use by existing zones. Attempting to add a
            // Nexus with no remaining external IPs should fail.
            let mut used_ip_ranges = Vec::new();
            for (_, z) in parent.all_omicron_zones(BlueprintZoneFilter::All) {
                if let Some((external_ip, _)) =
                    z.zone_type.external_networking()
                {
                    used_ip_ranges.push(IpRange::from(external_ip.ip()));
                }
            }
            assert!(!used_ip_ranges.is_empty());
            let input = {
                let mut builder = input.into_builder();
                builder.policy_mut().service_ip_pool_ranges = used_ip_ranges;
                builder.build()
            };

            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                &collection,
                "test",
            )
            .expect("failed to create builder");
            let err = builder.sled_add_zone_nexus(sled_id).unwrap_err();

            assert!(
                matches!(err, Error::NoExternalServiceIpAvailable),
                "unexpected error {err}"
            );
        }

        // We're not testing the `ExhaustedNexusIps` error case (where we've run
        // out of Nexus OPTE addresses), because it's fairly diffiult to induce
        // that from outside: we would need to start from a parent blueprint
        // that contained a Nexus instance for every IP in the
        // `NEXUS_OPTE_*_SUBNET`. We could hack around that by creating the
        // `BlueprintBuilder` and mucking with its internals, but that doesn't
        // seem like a particularly useful test either.

        logctx.cleanup_successful();
    }

    #[test]
    fn test_ensure_cockroachdb() {
        static TEST_NAME: &str = "blueprint_builder_test_ensure_cockroachdb";
        let logctx = test_setup_log(TEST_NAME);

        // Start with an empty system (sleds with no zones).
        let (example, parent) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .create_zones(false)
                .build();
        let collection = example.collection;
        let input = example.input;

        // Pick an arbitrary sled.
        let (target_sled_id, sled_resources) = input
            .all_sled_resources(SledFilter::InService)
            .next()
            .expect("at least one sled");

        // It should have multiple zpools.
        let num_sled_zpools = sled_resources.zpools.len();
        assert!(
            num_sled_zpools > 1,
            "expected more than 1 zpool, got {num_sled_zpools}"
        );

        // We should be able to ask for a CRDB zone per zpool.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            &collection,
            "test",
        )
        .expect("constructed builder");
        for _ in 0..num_sled_zpools {
            builder
                .sled_add_zone_cockroachdb(target_sled_id)
                .expect("added CRDB zone");
        }
        builder
            .sled_ensure_zone_datasets(target_sled_id, sled_resources)
            .unwrap();

        let blueprint = builder.build();
        verify_blueprint(&blueprint);
        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
                .filter(|(sled_id, z)| {
                    *sled_id == target_sled_id
                        && z.zone_type.kind() == ZoneKind::CockroachDb
                })
                .count(),
            num_sled_zpools
        );

        // Test a no-op planning iteration.
        assert_planning_makes_no_changes(
            &logctx.log,
            &blueprint,
            &input,
            TEST_NAME,
        );

        // If we instead ask for one more zone than there are zpools, we should
        // get a zpool allocation error.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            &collection,
            "test",
        )
        .expect("constructed builder");
        for _ in 0..num_sled_zpools {
            builder
                .sled_add_zone_cockroachdb(target_sled_id)
                .expect("added CRDB zone");
        }
        let err = builder
            .sled_add_zone_cockroachdb(target_sled_id)
            .expect_err("failed to create too many CRDB zones");
        match err {
            Error::NoAvailableZpool { sled_id, kind } => {
                assert_eq!(target_sled_id, sled_id);
                assert_eq!(kind, ZoneKind::CockroachDb);
            }
            _ => panic!("unexpected error {err}"),
        }

        logctx.cleanup_successful();
    }
}
