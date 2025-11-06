// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::blueprint_editor::BlueprintResourceAllocator;
use crate::blueprint_editor::BlueprintResourceAllocatorInputError;
use crate::blueprint_editor::DiskExpungeDetails;
use crate::blueprint_editor::EditedSled;
use crate::blueprint_editor::ExternalNetworkingChoice;
use crate::blueprint_editor::ExternalNetworkingError;
use crate::blueprint_editor::ExternalSnatNetworkingChoice;
use crate::blueprint_editor::SledEditError;
use crate::blueprint_editor::SledEditor;
use crate::mgs_updates::PendingHostPhase2Changes;
use crate::planner::NoopConvertInfo;
use crate::planner::NoopConvertSledIneligibleReason;
use crate::planner::ZoneExpungeReason;
use crate::planner::rng::PlannerRng;
use anyhow::Context as _;
use anyhow::anyhow;
use anyhow::bail;
use clickhouse_admin_types::OXIMETER_CLUSTER;
use id_map::IdMap;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use itertools::Either;
use nexus_inventory::now_db_precision;
use nexus_sled_agent_shared::inventory::MupdateOverrideBootInventory;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintSledConfig;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::ClickhouseClusterConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
use nexus_types::deployment::OmicronZoneExternalFloatingIp;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::deployment::OximeterReadMode;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::TufRepoContentsError;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::deployment::ZpoolName;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_common::address::CLICKHOUSE_HTTP_PORT;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::DNS_PORT;
use omicron_common::address::DnsSubnet;
use omicron_common::address::NTP_PORT;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use once_cell::unsync::OnceCell;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::collections::btree_map::Entry;
use std::fmt;
use std::iter;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use swrite::SWrite;
use swrite::swriteln;
use thiserror::Error;

use super::ClickhouseZonesThatShouldBeRunning;
use super::clickhouse::ClickhouseAllocator;
use nexus_types::inventory::BaseboardId;
use std::sync::Arc;

/// Errors encountered while assembling blueprints
#[derive(Debug, Error)]
pub enum Error {
    #[error(
        "sled {sled_id}: no available zpools for additional {kind:?} zones"
    )]
    NoAvailableZpool { sled_id: SledUuid, kind: ZoneKind },
    #[error("no Nexus zones exist in parent blueprint")]
    NoNexusZonesInParentBlueprint,
    #[error("no active Nexus zones exist in blueprint currently being built")]
    NoActiveNexusZonesInBlueprint,
    #[error("conflicting values for active Nexus zones in parent blueprint")]
    ActiveNexusZoneGenerationConflictInParentBlueprint,
    #[error("no Boundary NTP zones exist in parent blueprint")]
    NoBoundaryNtpZonesInParentBlueprint,
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
    #[error("error editing sled {sled_id}")]
    SledEditError {
        sled_id: SledUuid,
        #[source]
        err: SledEditError,
    },
    #[error("error constructing resource allocator")]
    AllocatorInput(#[from] BlueprintResourceAllocatorInputError),
    #[error("no commissioned sleds - rack subnet is unknown")]
    RackSubnetUnknownNoSleds,
    #[error("no reserved subnets available for internal DNS")]
    NoAvailableDnsSubnets,
    #[error("error allocating external networking resources")]
    AllocateExternalNetworking(#[from] ExternalNetworkingError),
    #[error("zone is already up-to-date and should not be updated")]
    ZoneAlreadyUpToDate,
    #[error(
        "mismatch while setting target_release_minimum_generation, \
         expected current value is {expected} but actual value is {actual}"
    )]
    TargetReleaseMinimumGenerationMismatch {
        expected: Generation,
        actual: Generation,
    },
    #[error(
        "target release minimum generation was set to {current}, \
         but we tried to set it to the older generation {new}, indicating a \
         possible table rollback which should not happen"
    )]
    TargetReleaseMinimumGenerationRollback {
        current: Generation,
        new: Generation,
    },
    #[error(transparent)]
    TufRepoContentsError(#[from] TufRepoContentsError),
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
        /// This happens after expungement or decommissioning has completed
        /// depending upon the resource type.
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
    /// This happens after expungement or decommissioning has completed
    /// depending upon the resource type.
    pub removed: usize,
}

impl EditCounts {
    pub fn zeroes() -> Self {
        Self::default()
    }

    pub fn has_nonzero_counts(&self) -> bool {
        *self != Self::zeroes()
    }

    pub fn difference_since(self, other: Self) -> Self {
        Self {
            added: self.added - other.added,
            updated: self.updated - other.updated,
            expunged: self.expunged - other.expunged,
            removed: self.removed - other.removed,
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
    pub fn zeroes() -> Self {
        Self {
            disks: EditCounts::zeroes(),
            datasets: EditCounts::zeroes(),
            zones: EditCounts::zeroes(),
        }
    }

    fn has_nonzero_counts(&self) -> bool {
        let Self { disks, datasets, zones } = self;
        disks.has_nonzero_counts()
            || datasets.has_nonzero_counts()
            || zones.has_nonzero_counts()
    }

    fn difference_since(self, other: Self) -> Self {
        Self {
            disks: self.disks.difference_since(other.disks),
            datasets: self.datasets.difference_since(other.datasets),
            zones: self.zones.difference_since(other.zones),
        }
    }
}

impl From<StorageEditCounts> for SledEditCounts {
    fn from(value: StorageEditCounts) -> Self {
        let StorageEditCounts { disks, datasets } = value;
        Self { disks, datasets, zones: EditCounts::zeroes() }
    }
}

/// A list of scalar (primitive) values which have been edited on a sled.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct EditedSledScalarEdits {
    /// Whether the remove_mupdate_override field was modified.
    pub remove_mupdate_override: bool,
    /// Whether the debug operation to force a Sled Agent generation bump was
    /// set.
    pub debug_force_generation_bump: bool,
}

impl EditedSledScalarEdits {
    pub fn zeroes() -> Self {
        Self {
            debug_force_generation_bump: false,
            remove_mupdate_override: false,
        }
    }

    pub fn has_edits(&self) -> bool {
        self.debug_force_generation_bump || self.remove_mupdate_override
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
    DiskExpunged {
        sled_id: SledUuid,
        details: DiskExpungeDetails,
    },
    SledExpunged {
        sled_id: SledUuid,
        num_disks_expunged: usize,
        num_datasets_expunged: usize,
        num_zones_expunged: usize,
    },
    SetNexusGeneration {
        current_generation: Generation,
        new_generation: Generation,
    },
    SetTargetReleaseMinimumGeneration {
        current_generation: Generation,
        new_generation: Generation,
    },
    SledNoopZoneImageSourcesUpdated {
        sled_id: SledUuid,
        count: usize,
    },
    SledNoopHostPhase2Updated {
        sled_id: SledUuid,
        slot_a_updated: bool,
        slot_b_updated: bool,
    },
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddZone { sled_id, kind } => {
                write!(f, "sled {sled_id}: added zone: {}", kind.report_str())
            }
            Self::UpdateDisks { sled_id, added, updated, removed } => {
                write!(
                    f,
                    "sled {sled_id}: added {added} disks, updated {updated}, removed {removed} disks"
                )
            }
            Self::UpdateDatasets {
                sled_id,
                added,
                updated,
                expunged,
                removed,
            } => {
                write!(
                    f,
                    "sled {sled_id}: added {added} datasets, updated: {updated}, expunged {expunged}, removed {removed} datasets"
                )
            }
            Self::ZoneExpunged { sled_id, reason, count } => {
                let reason = match reason {
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
            Self::DiskExpunged { sled_id, details } => {
                write!(
                    f,
                    "sled {sled_id}: expunged disk {} with \
                     {} associated datasets and {} associated zones",
                    details.disk_id,
                    details.num_datasets_expunged,
                    details.num_zones_expunged,
                )
            }
            Self::SledExpunged {
                sled_id,
                num_disks_expunged,
                num_datasets_expunged,
                num_zones_expunged,
            } => {
                write!(
                    f,
                    "sled {sled_id} expunged \
                     (expunged {num_disks_expunged} disks, \
                      {num_datasets_expunged} datasets, \
                      {num_zones_expunged} zones)"
                )
            }
            Self::SledNoopZoneImageSourcesUpdated { sled_id, count } => {
                write!(
                    f,
                    "sled {sled_id}: performed {count} noop \
                     zone image source updates"
                )
            }
            Self::SledNoopHostPhase2Updated {
                sled_id,
                slot_a_updated,
                slot_b_updated,
            } => {
                let slots_updated_str = match (*slot_a_updated, *slot_b_updated)
                {
                    (true, true) => "both slot A and slot B",
                    (true, false) => "slot A",
                    (false, true) => "slot B",
                    (false, false) => "none (this shouldn't happen)",
                };
                write!(
                    f,
                    "sled {sled_id}: noop updated host phase 2 to Artifact: \
                     {slots_updated_str}"
                )
            }
            Self::SetTargetReleaseMinimumGeneration {
                current_generation,
                new_generation,
            } => {
                write!(
                    f,
                    "updated target release minimum generation from \
                     {current_generation} to {new_generation}"
                )
            }
            Self::SetNexusGeneration { current_generation, new_generation } => {
                write!(
                    f,
                    "updated nexus generation from \
                     {current_generation} to {new_generation}"
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
    log: Logger,

    /// previous blueprint, on which this one will be based
    parent_blueprint: &'a Blueprint,

    /// The latest inventory collection
    collection: &'a Collection,

    /// The ID that the completed blueprint will have
    new_blueprint_id: BlueprintUuid,

    // These fields are used to allocate resources for sleds.
    input: &'a PlanningInput,

    // `allocators` contains logic for choosing new underlay IPs, external IPs,
    // internal DNS subnets, etc. It's held in a `OnceCell` to delay its
    // creation until it's first needed; the planner expunges zones before
    // adding zones, so this delay allows us to reuse resources that just came
    // free. (This is implicit and awkward; as we rework the builder we should
    // rework this to make it more explicit.)
    //
    // Note: this is currently still a `once_cell` `OnceCell` rather than a std
    // `OnceCell`, because `get_or_try_init` isn't stable yet.
    resource_allocator: OnceCell<BlueprintResourceAllocator>,

    // These fields will become part of the final blueprint.  See the
    // corresponding fields in `Blueprint`.
    sled_editors: BTreeMap<SledUuid, SledEditor>,
    cockroachdb_setting_preserve_downgrade: CockroachDbPreserveDowngrade,
    target_release_minimum_generation: Generation,
    nexus_generation: Generation,

    creator: String,
    operations: Vec<Operation>,
    comments: Vec<String>,
    pending_mgs_updates: PendingMgsUpdates,

    /// Random number generator for new UUIDs
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
        let sleds = sled_ids
            .map(|sled_id| {
                let config = BlueprintSledConfig {
                    state: SledState::Active,
                    sled_agent_generation: Generation::new(),
                    disks: IdMap::default(),
                    datasets: IdMap::default(),
                    zones: IdMap::default(),
                    remove_mupdate_override: None,
                    host_phase_2:
                        BlueprintHostPhase2DesiredSlots::current_contents(),
                };
                (sled_id, config)
            })
            .collect::<BTreeMap<_, _>>();
        let num_sleds = sleds.len();

        let id = rng.next_blueprint();
        Blueprint {
            id,
            sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            parent_blueprint_id: None,
            internal_dns_version: Generation::new(),
            external_dns_version: Generation::new(),
            target_release_minimum_generation: Generation::new(),
            nexus_generation: Generation::new(),
            cockroachdb_fingerprint: String::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            clickhouse_cluster_config: None,
            oximeter_read_version: Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: creator.to_owned(),
            comment: format!("starting blueprint with {num_sleds} empty sleds"),
            // The only reason to create empty blueprints is tests. If that
            // changes (e.g., if RSS starts using this builder to generate its
            // blueprints), we could take a `source` argument instead.
            source: BlueprintSource::Test,
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
        mut rng: PlannerRng,
    ) -> anyhow::Result<BlueprintBuilder<'a>> {
        let log = log.new(o!(
            "component" => "BlueprintBuilder",
            "parent_id" => parent_blueprint.id.to_string(),
        ));

        // Convert our parent blueprint's sled configs into `SledEditor`s.
        let mut sled_editors = BTreeMap::new();
        for (sled_id, sled_cfg) in &parent_blueprint.sleds {
            let state = sled_cfg.state;

            let editor = match state {
                SledState::Active => {
                    let subnet = input
                        .sled_lookup(SledFilter::Commissioned, *sled_id)
                        .with_context(|| {
                            format!(
                                "failed to find sled details for \
                                 active sled in parent blueprint {sled_id}"
                            )
                        })?
                        .resources
                        .subnet;
                    SledEditor::for_existing_active(subnet, sled_cfg.clone())
                }
                SledState::Decommissioned => {
                    SledEditor::for_existing_decommissioned(sled_cfg.clone())
                }
            }
            .with_context(|| {
                format!("failed to construct SledEditor for sled {sled_id}")
            })?;

            sled_editors.insert(*sled_id, editor);
        }

        // Add new, empty `SledEditor`s for any commissioned sleds in our input
        // that weren't in the parent blueprint. (These are newly-added sleds.)
        for (sled_id, details) in input.all_sleds(SledFilter::Commissioned) {
            if let Entry::Vacant(slot) = sled_editors.entry(sled_id) {
                slot.insert(SledEditor::for_new_active(
                    details.resources.subnet,
                ));
            }
        }

        Ok(BlueprintBuilder {
            log,
            parent_blueprint,
            collection: inventory,
            new_blueprint_id: rng.next_blueprint(),
            input,
            resource_allocator: OnceCell::new(),
            sled_editors,
            cockroachdb_setting_preserve_downgrade: parent_blueprint
                .cockroachdb_setting_preserve_downgrade,
            pending_mgs_updates: parent_blueprint.pending_mgs_updates.clone(),
            target_release_minimum_generation: parent_blueprint
                .target_release_minimum_generation,
            nexus_generation: parent_blueprint.nexus_generation,
            creator: creator.to_owned(),
            operations: Vec::new(),
            comments: Vec::new(),
            rng,
        })
    }

    pub fn parent_blueprint(&self) -> &Blueprint {
        &self.parent_blueprint
    }

    pub fn new_blueprint_id(&self) -> BlueprintUuid {
        self.new_blueprint_id
    }

    pub fn available_internal_dns_subnets(
        &self,
    ) -> Result<impl Iterator<Item = DnsSubnet>, Error> {
        // TODO-multirack We need the rack subnet to know what the reserved
        // internal DNS subnets are. Pick any sled; this isn't right in
        // multirack (either DNS will be on a wider subnet or we need to pick a
        // particular rack subnet here?).
        let any_sled_subnet = self
            .input
            .all_sled_resources(SledFilter::Commissioned)
            .map(|(_sled_id, resources)| resources.subnet)
            .next()
            .ok_or(Error::RackSubnetUnknownNoSleds)?;
        let rack_subnet = ReservedRackSubnet::from_subnet(any_sled_subnet);

        // Compute the "in use" subnets; this includes all in-service internal
        // DNS zones _and_ any "expunged but not yet confirmed to be gone"
        // zones, so we use the somewhat unusual `could_be_running` filter
        // instead of the more typical `is_in_service`.
        let internal_dns_subnets_in_use = self
            .current_zones(BlueprintZoneDisposition::could_be_running)
            .filter_map(|(_sled_id, zone)| match &zone.zone_type {
                BlueprintZoneType::InternalDns(internal_dns) => {
                    Some(DnsSubnet::from_addr(*internal_dns.dns_address.ip()))
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        Ok(rack_subnet.get_dns_subnets().into_iter().filter(move |subnet| {
            !internal_dns_subnets_in_use.contains(&subnet)
        }))
    }

    pub fn planning_input(&self) -> &PlanningInput {
        &self.input
    }

    fn resource_allocator(
        &mut self,
    ) -> Result<&mut BlueprintResourceAllocator, Error> {
        self.resource_allocator.get_or_try_init(|| {
            // Check the planning input: there shouldn't be any external
            // networking resources in the database (the source of `input`)
            // that we don't know about from the parent blueprint.
            //
            // TODO-cleanup Should the planner do this instead? Move this check
            // to blippy.
            ensure_input_networking_records_appear_in_parent_blueprint(
                self.parent_blueprint,
                self.input,
            )
            .map_err(Error::Planner)?;

            let allocator = BlueprintResourceAllocator::new(
                self.sled_editors.values(),
                self.input.external_ip_policy(),
            )?;

            Ok::<_, Error>(allocator)
        })?;
        Ok(self.resource_allocator.get_mut().expect("get_or_init succeeded"))
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.sled_editors.keys().copied()
    }

    /// Iterates over all zones on a sled.
    ///
    /// This will include both zones from the parent blueprint, as well
    /// as the changes made within this builder.
    pub fn current_sled_zones<F>(
        &self,
        sled_id: SledUuid,
        filter: F,
    ) -> impl Iterator<Item = &BlueprintZoneConfig>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        let Some(editor) = self.sled_editors.get(&sled_id) else {
            return Either::Left(iter::empty());
        };
        Either::Right(editor.zones(filter))
    }

    /// Iterates over all zones on all sleds.
    ///
    /// Acts like a combination of [`Self::sled_ids_with_zones`] and
    /// [`Self::current_sled_zones`].
    ///
    /// This will include both zones from the parent blueprint, as well
    /// as the changes made within this builder.
    pub fn current_zones<F>(
        &'a self,
        filter: F,
    ) -> impl Iterator<Item = (SledUuid, &'a BlueprintZoneConfig)>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool + Clone,
    {
        self.sled_ids_with_zones().flat_map(move |sled_id| {
            self.current_sled_zones(sled_id, filter.clone())
                .map(move |config| (sled_id, config))
        })
    }

    pub fn current_sled_disks<F>(
        &self,
        sled_id: SledUuid,
        filter: F,
    ) -> impl Iterator<Item = &BlueprintPhysicalDiskConfig>
    where
        F: FnMut(BlueprintPhysicalDiskDisposition) -> bool,
    {
        let Some(editor) = self.sled_editors.get(&sled_id) else {
            return Either::Left(iter::empty());
        };
        Either::Right(editor.disks(filter))
    }

    pub fn current_sled_host_phase_2(
        &self,
        sled_id: SledUuid,
    ) -> Result<BlueprintHostPhase2DesiredSlots, Error> {
        let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to get host phase 2 for unknown sled {sled_id}"
            ))
        })?;
        Ok(editor.host_phase_2())
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self, source: BlueprintSource) -> Blueprint {
        let blueprint_id = self.new_blueprint_id();

        // Collect the Omicron zones config for all sleds, including sleds that
        // are no longer in service and need expungement work.
        let mut sleds = BTreeMap::new();
        for (sled_id, editor) in self.sled_editors {
            let EditedSled { config, edit_counts, scalar_edits } =
                editor.finalize();
            sleds.insert(sled_id, config);
            if edit_counts.has_nonzero_counts() || scalar_edits.has_edits() {
                let EditedSledScalarEdits {
                    debug_force_generation_bump,
                    remove_mupdate_override,
                } = scalar_edits;
                debug!(
                    self.log, "sled modified in new blueprint";
                    "sled_id" => %sled_id,
                    "blueprint_id" => %blueprint_id,
                    "disk_edits" => ?edit_counts.disks,
                    "dataset_edits" => ?edit_counts.datasets,
                    "zone_edits" => ?edit_counts.zones,
                    "debug_force_generation_bump" => debug_force_generation_bump,
                    "remove_mupdate_override_modified" => remove_mupdate_override,
                );
            } else {
                debug!(
                    self.log, "sled unchanged in new blueprint";
                    "sled_id" => %sled_id,
                    "blueprint_id" => %blueprint_id,
                );
            }
        }

        // If we have the clickhouse cluster setup enabled via policy and we
        // don't yet have a `ClickhouseClusterConfiguration`, then we must
        // create one and feed it to our `ClickhouseAllocator`.
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
            let should_be_running = ClickhouseZonesThatShouldBeRunning::new(
                sleds.iter().map(|(id, c)| (*id, c)),
            );
            match a.plan(&should_be_running) {
                Ok(config) => config,
                Err(e) => {
                    error!(self.log, "clickhouse allocator error: {e}");
                    a.parent_config().clone()
                }
            }
        });

        let (oximeter_read_mode, oximeter_read_version) = {
            let policy = self.input.oximeter_read_settings();
            (policy.mode.clone(), policy.version)
        };

        Blueprint {
            id: blueprint_id,
            sleds,
            pending_mgs_updates: self.pending_mgs_updates,
            parent_blueprint_id: Some(self.parent_blueprint.id),
            internal_dns_version: self.input.internal_dns_version(),
            external_dns_version: self.input.external_dns_version(),
            target_release_minimum_generation: self
                .target_release_minimum_generation,
            nexus_generation: self.nexus_generation,
            cockroachdb_fingerprint: self
                .input
                .cockroachdb_settings()
                .state_fingerprint
                .clone(),
            cockroachdb_setting_preserve_downgrade: self
                .cockroachdb_setting_preserve_downgrade,

            clickhouse_cluster_config,
            oximeter_read_version: oximeter_read_version.into(),
            oximeter_read_mode,
            time_created: now_db_precision(),
            creator: self.creator,
            comment: self
                .comments
                .into_iter()
                .chain(self.operations.iter().map(|op| op.to_string()))
                .collect::<Vec<String>>()
                .join(", "),
            source,
        }
    }

    pub fn current_sled_state(
        &self,
        sled_id: SledUuid,
    ) -> Result<SledState, Error> {
        let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to get sled state for unknown sled {sled_id}"
            ))
        })?;
        Ok(editor.state())
    }

    /// Set the desired state of the given sled.
    pub fn set_sled_decommissioned(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to set sled state for unknown sled {sled_id}"
            ))
        })?;
        editor
            .decommission()
            .map_err(|err| Error::SledEditError { sled_id, err })
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

    /// Expunges a disk from a sled.
    pub(crate) fn expunge_disk(
        &mut self,
        sled_id: SledUuid,
        disk_id: PhysicalDiskUuid,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to expunge disk for unknown sled {sled_id}"
            ))
        })?;
        let details = editor
            .expunge_disk(&disk_id)
            .map_err(|err| Error::SledEditError { sled_id, err })?;
        if details.did_expunge_disk {
            self.record_operation(Operation::DiskExpunged { sled_id, details });
        }
        Ok(())
    }

    /// Expunge everything on a sled.
    pub(crate) fn expunge_sled(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!("tried to expunge unknown sled {sled_id}"))
        })?;

        // We don't have an explicit "sled disposition" in the blueprint, but we
        // can expunge all the disks, datasets, and zones on the sled. It is
        // sufficient to just expunge the disks: expunging all disks will
        // expunge all datasets and zones that depend on those disks, which
        // should include all datasets and zones on the sled. (We'll
        // double-check this below and fail if this is wrong.)
        let mut num_disks_expunged = 0;
        let mut num_datasets_expunged = 0;
        let mut num_zones_expunged = 0;

        let mut disks_to_expunge = Vec::new();
        for disk in
            editor.disks(BlueprintPhysicalDiskDisposition::is_in_service)
        {
            disks_to_expunge.push(disk.id);
        }
        for disk_id in disks_to_expunge {
            let details = editor
                .expunge_disk(&disk_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
            if details.did_expunge_disk {
                num_disks_expunged += 1;
            }
            num_datasets_expunged += details.num_datasets_expunged;
            num_zones_expunged += details.num_zones_expunged;

            // When we expunge a disk on an expunged sled, we can decommission
            // it immediately because the sled is already gone. There is no sled
            // agent to notify about the  disk expungement.
            editor
                .decommission_disk(&disk_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
        }

        // Expunging a disk expunges any datasets and zones that depend on it,
        // so expunging all in-service disks should have also expunged all
        // datasets and zones. Double-check that that's true.
        let mut zones_ready_for_cleanup = Vec::new();
        for zone in editor.zones(BlueprintZoneDisposition::any) {
            match zone.disposition {
                BlueprintZoneDisposition::Expunged { .. } => {
                    // Since this is a full sled expungement, we'll never see an
                    // inventory collection indicating the zones are shut down,
                    // nor do we need to: go ahead and mark any expunged zones
                    // as ready for cleanup.
                    zones_ready_for_cleanup.push(zone.id);
                }
                BlueprintZoneDisposition::InService => {
                    return Err(Error::Planner(anyhow!(
                        "expunged all disks but a zone \
                         is still in service: {zone:?}"
                    )));
                }
            }
        }
        if let Some(dataset) =
            editor.datasets(BlueprintDatasetDisposition::is_in_service).next()
        {
            return Err(Error::Planner(anyhow!(
                "expunged all disks but a dataset \
                 is still in service: {dataset:?}"
            )));
        }
        for zone_id in zones_ready_for_cleanup {
            editor
                .mark_expunged_zone_ready_for_cleanup(&zone_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
        }

        // If we didn't expunge anything, this sled was presumably expunged in a
        // prior planning run. Only note the operation if we did anything.
        if num_disks_expunged > 0
            || num_datasets_expunged > 0
            || num_zones_expunged > 0
        {
            self.record_operation(Operation::SledExpunged {
                sled_id,
                num_disks_expunged,
                num_datasets_expunged,
                num_zones_expunged,
            });
        }

        Ok(())
    }

    /// Mark expunged zones as ready for cleanup.
    pub(crate) fn mark_expunged_zones_ready_for_cleanup(
        &mut self,
        sled_id: SledUuid,
        zone_ids: &[OmicronZoneUuid],
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!("tried to expunge unknown sled {sled_id}"))
        })?;
        for zone_id in zone_ids {
            editor
                .mark_expunged_zone_ready_for_cleanup(zone_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
        }
        Ok(())
    }

    pub(crate) fn expunge_all_multinode_clickhouse(
        &mut self,
        sled_id: SledUuid,
        reason: ZoneExpungeReason,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to expunge multinode clickhouse zones for unknown \
                 sled {sled_id}"
            ))
        })?;

        let mut zones_to_expunge = Vec::new();

        for zone in editor.zones(BlueprintZoneDisposition::is_in_service) {
            if zone.zone_type.is_clickhouse_keeper()
                || zone.zone_type.is_clickhouse_server()
            {
                zones_to_expunge.push(zone.id);
            }
        }

        let mut nexpunged = 0;
        for zone_id in zones_to_expunge {
            if editor
                .expunge_zone(&zone_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?
            {
                nexpunged += 1;
            }
        }

        if nexpunged > 0 {
            self.record_operation(Operation::ZoneExpunged {
                sled_id,
                reason,
                count: nexpunged,
            });
        }

        Ok(())
    }

    pub(crate) fn expunge_all_singlenode_clickhouse(
        &mut self,
        sled_id: SledUuid,
        reason: ZoneExpungeReason,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to expunge singlenode clickhouse zones for unknown \
                 sled {sled_id}"
            ))
        })?;

        let mut zones_to_expunge = Vec::new();

        for zone in editor.zones(BlueprintZoneDisposition::is_in_service) {
            if zone.zone_type.is_clickhouse() {
                zones_to_expunge.push(zone.id);
            }
        }

        let mut nexpunged = 0;
        for zone_id in zones_to_expunge {
            if editor
                .expunge_zone(&zone_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?
            {
                nexpunged += 1;
            }
        }

        if nexpunged > 0 {
            self.record_operation(Operation::ZoneExpunged {
                sled_id,
                reason,
                count: nexpunged,
            });
        }

        Ok(())
    }

    /// Add any disks to the blueprint
    /// Called by the planner in the `do_plan_add()` stage of planning
    pub fn sled_add_disks(
        &mut self,
        sled_id: SledUuid,
        sled_resources: &SledResources,
    ) -> Result<SledEditCounts, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to add disks for unknown sled {sled_id}"
            ))
        })?;
        let initial_counts = editor.edit_counts();

        // These are the in-service disks as we observed them in the database,
        // during the planning phase
        let database_disks = sled_resources
            .all_disks(DiskFilter::InService)
            .map(|(zpool, disk)| (disk.disk_id, (zpool, disk)));
        let mut database_disk_ids = BTreeSet::new();

        // Ensure any disk present in the database is also present in the
        // blueprint
        for (disk_id, (zpool, disk)) in database_disks {
            database_disk_ids.insert(disk_id);
            editor
                .ensure_disk(
                    BlueprintPhysicalDiskConfig {
                        disposition:
                            BlueprintPhysicalDiskDisposition::InService,
                        identity: disk.disk_identity.clone(),
                        id: disk_id,
                        pool_id: *zpool,
                    },
                    self.rng.sled_rng(sled_id),
                )
                .map_err(|err| Error::SledEditError { sled_id, err })?;
        }

        let final_counts = editor.edit_counts();
        Ok(final_counts.difference_since(initial_counts))
    }

    /// Decommission any expunged disks.
    ///
    /// Note: This method is called by the planner only for `InService` sleds.
    /// `Self::expunge_sled` expunges and decommissions disks immediately since
    /// the sled is already gone by that point.
    ///
    /// Called by the planner in
    /// `do_plan_decommission_expunged_disks_for_in_service_sled()`.
    pub fn sled_decommission_disks(
        &mut self,
        sled_id: SledUuid,
        disk_ids: Vec<PhysicalDiskUuid>,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to decommission disks for unknown sled {sled_id}"
            ))
        })?;

        for disk_id in disk_ids {
            editor
                .decommission_disk(&disk_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
        }

        Ok(())
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
    ) -> Result<EnsureMultiple, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to ensure zone datasets for unknown sled {sled_id}"
            ))
        })?;

        let initial_counts = editor.edit_counts();
        editor
            .ensure_datasets_for_running_zones(self.rng.sled_rng(sled_id))
            .map_err(|err| Error::SledEditError { sled_id, err })?;
        let final_counts = editor.edit_counts();

        let SledEditCounts { disks, datasets, zones } =
            final_counts.difference_since(initial_counts);
        debug_assert_eq!(
            disks,
            EditCounts::zeroes(),
            "we only edited datasets"
        );
        debug_assert_eq!(
            zones,
            EditCounts::zeroes(),
            "we only edited datasets"
        );
        Ok(datasets.into())
    }

    /// Returns the remove_mupdate_override field for a sled.
    pub(crate) fn sled_get_remove_mupdate_override(
        &self,
        sled_id: SledUuid,
    ) -> Result<Option<MupdateOverrideUuid>, Error> {
        let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to get remove_mupdate_override for \
                 unknown sled {sled_id}"
            ))
        })?;
        Ok(editor.get_remove_mupdate_override())
    }

    /// Updates a sled's mupdate override field based on the mupdate override
    /// provided by inventory.
    pub(crate) fn sled_ensure_mupdate_override(
        &mut self,
        sled_id: SledUuid,
        // inv_mupdate_override_info has a weird type (not Option<&T>, not &str)
        // because this is what `Result::as_ref` returns.
        inv_mupdate_override_info: Result<
            &Option<MupdateOverrideBootInventory>,
            &String,
        >,
        noop_info: &mut NoopConvertInfo,
    ) -> Result<EnsureMupdateOverrideAction, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to ensure mupdate override for unknown sled {sled_id}"
            ))
        })?;

        // Also map the editor to the corresponding PendingMgsUpdates.
        let sled_details = self
            .input
            .sled_lookup(SledFilter::InService, sled_id)
            .map_err(|error| Error::Planner(anyhow!(error)))?;
        let pending_mgs_update =
            self.pending_mgs_updates.entry(&sled_details.baseboard_id);
        let noop_sled_info = noop_info.sled_info_mut(sled_id)?;

        editor
            .ensure_mupdate_override(
                inv_mupdate_override_info,
                pending_mgs_update,
                noop_sled_info,
            )
            .map_err(|err| Error::SledEditError { sled_id, err })
    }

    fn next_internal_dns_gz_address_index(&self, sled_id: SledUuid) -> u32 {
        let used_internal_dns_gz_address_indices = self
            .current_sled_zones(
                sled_id,
                BlueprintZoneDisposition::is_in_service,
            )
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
        image_source: BlueprintZoneImageSource,
        dns_subnet: DnsSubnet,
    ) -> Result<(), Error> {
        let gz_address_index = self.next_internal_dns_gz_address_index(sled_id);
        let address = dns_subnet.dns_address();
        let zpool = self.sled_select_zpool(sled_id, ZoneKind::InternalDns)?;
        let zone_type =
            BlueprintZoneType::InternalDns(blueprint_zone_type::InternalDns {
                dataset: OmicronZoneDataset { pool_name: zpool },
                dns_address: SocketAddrV6::new(address, DNS_PORT, 0, 0),
                http_address: SocketAddrV6::new(address, DNS_HTTP_PORT, 0, 0),
                gz_address: dns_subnet.gz_address(),
                gz_address_index,
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.sled_rng(sled_id).next_zone(),
            filesystem_pool: zpool,
            zone_type,
            image_source,
        };

        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_external_dns(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let id = self.rng.sled_rng(sled_id).next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.resource_allocator()?.next_external_ip_external_dns()?;
        let nic = NetworkInterface {
            id: self.rng.sled_rng(sled_id).next_network_interface(),
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
            id: self.rng.sled_rng(sled_id).next_external_ip(),
            addr: SocketAddr::new(external_ip, DNS_PORT),
        };
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::ExternalDns)?;
        let zone_type =
            BlueprintZoneType::ExternalDns(blueprint_zone_type::ExternalDns {
                dataset: OmicronZoneDataset { pool_name },
                http_address,
                dns_address,
                nic,
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: pool_name,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_ensure_zone_ntp(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<Ensure, Error> {
        // If there's already an NTP zone on this sled, do nothing.
        let has_ntp = {
            let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
                Error::Planner(anyhow!(
                    "tried to ensure NTP zone for unknown sled {sled_id}"
                ))
            })?;
            editor
                .zones(BlueprintZoneDisposition::is_in_service)
                .any(|z| z.zone_type.is_ntp())
        };
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
            id: self.rng.sled_rng(sled_id).next_zone(),
            filesystem_pool,
            zone_type,
            image_source,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    pub fn sled_ensure_zone_crucible(
        &mut self,
        sled_id: SledUuid,
        zpool_id: ZpoolUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<Ensure, Error> {
        let pool_name = ZpoolName::new_external(zpool_id);

        // If this sled already has a Crucible zone on this pool, do nothing.
        let has_crucible_on_this_pool = {
            let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
                Error::Planner(anyhow!(
                    "tried to ensure crucible zone for unknown sled {sled_id}"
                ))
            })?;
            editor.zones(BlueprintZoneDisposition::is_in_service).any(|z| {
                matches!(
                    &z.zone_type,
                    BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                        dataset,
                        ..
                    })
                    if dataset.pool_name == pool_name
                )
            })
        };
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
                dataset: OmicronZoneDataset { pool_name },
            });
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.sled_rng(sled_id).next_zone(),
            filesystem_pool,
            zone_type,
            image_source,
        };

        self.sled_add_zone(sled_id, zone)?;
        Ok(Ensure::Added)
    }

    /// Adds a nexus zone on this sled.
    pub fn sled_add_zone_nexus(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
        nexus_generation: Generation,
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
            .all_omicron_zones(BlueprintZoneDisposition::any)
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
            image_source,
            nexus_generation,
        )
    }

    pub fn sled_add_zone_nexus_with_config(
        &mut self,
        sled_id: SledUuid,
        external_tls: bool,
        external_dns_servers: Vec<IpAddr>,
        image_source: BlueprintZoneImageSource,
        nexus_generation: Generation,
    ) -> Result<(), Error> {
        let nexus_id = self.rng.sled_rng(sled_id).next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.resource_allocator()?.next_external_ip_nexus()?;
        let external_ip = OmicronZoneExternalFloatingIp {
            id: self.rng.sled_rng(sled_id).next_external_ip(),
            ip: external_ip,
        };

        let nic = NetworkInterface {
            id: self.rng.sled_rng(sled_id).next_network_interface(),
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
            lockstep_port: omicron_common::address::NEXUS_LOCKSTEP_PORT,
            external_ip,
            nic,
            external_tls,
            external_dns_servers: external_dns_servers.clone(),
            nexus_generation,
        });
        let filesystem_pool =
            self.sled_select_zpool(sled_id, zone_type.kind())?;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: nexus_id,
            filesystem_pool,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_oximeter(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let oximeter_id = self.rng.sled_rng(sled_id).next_zone();
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
            filesystem_pool,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_crucible_pantry(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let pantry_id = self.rng.sled_rng(sled_id).next_zone();
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
            filesystem_pool,
            zone_type,
            image_source,
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
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let zone_id = self.rng.sled_rng(sled_id).next_zone();
        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::CockroachDb)?;
        let port = omicron_common::address::COCKROACH_PORT;
        let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::CockroachDb(blueprint_zone_type::CockroachDb {
                address,
                dataset: OmicronZoneDataset { pool_name },
            });
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_clickhouse(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let id = self.rng.sled_rng(sled_id).next_zone();
        let underlay_address = self.sled_alloc_ip(sled_id)?;
        let address =
            SocketAddrV6::new(underlay_address, CLICKHOUSE_HTTP_PORT, 0, 0);
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::Clickhouse)?;
        let zone_type =
            BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                address,
                dataset: OmicronZoneDataset { pool_name },
            });

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: pool_name,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_clickhouse_server(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let zone_id = self.rng.sled_rng(sled_id).next_zone();
        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::ClickhouseServer)?;
        let address =
            SocketAddrV6::new(underlay_ip, CLICKHOUSE_HTTP_PORT, 0, 0);
        let zone_type = BlueprintZoneType::ClickhouseServer(
            blueprint_zone_type::ClickhouseServer {
                address,
                dataset: OmicronZoneDataset { pool_name },
            },
        );
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_clickhouse_keeper(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let zone_id = self.rng.sled_rng(sled_id).next_zone();
        let underlay_ip = self.sled_alloc_ip(sled_id)?;
        let pool_name =
            self.sled_select_zpool(sled_id, ZoneKind::ClickhouseKeeper)?;
        let port = omicron_common::address::CLICKHOUSE_KEEPER_TCP_PORT;
        let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
        let zone_type = BlueprintZoneType::ClickhouseKeeper(
            blueprint_zone_type::ClickhouseKeeper {
                address,
                dataset: OmicronZoneDataset { pool_name },
            },
        );
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool,
            zone_type,
            image_source,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_promote_internal_ntp_to_boundary_ntp(
        &mut self,
        sled_id: SledUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        // The upstream NTP/DNS servers and domain _should_ come from Nexus and
        // be modifiable by the operator, but currently can only be set at RSS.
        // We can only promote a new boundary NTP zone by copying these settings
        // from an existing one.
        let (ntp_servers, dns_servers, domain) = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::any)
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
            image_source,
        )
    }

    pub fn sled_promote_internal_ntp_to_boundary_ntp_with_config(
        &mut self,
        sled_id: SledUuid,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
        image_source: BlueprintZoneImageSource,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to promote NTP zone on unknown sled {sled_id}"
            ))
        })?;

        // Find the internal NTP zone and expunge it.
        let mut internal_ntp_zone_id_iter = editor
            .zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|zone| {
                if matches!(zone.zone_type, BlueprintZoneType::InternalNtp(_)) {
                    Some(zone.id)
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
        editor.expunge_zone(&internal_ntp_zone_id).map_err(|error| {
            Error::Planner(anyhow!(error).context(format!(
                "error expunging internal NTP zone from sled {sled_id}"
            )))
        })?;

        // Add the new boundary NTP zone.
        let new_zone_id = self.rng.sled_rng(sled_id).next_zone();
        let ExternalSnatNetworkingChoice {
            snat_cfg,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.resource_allocator()?.next_external_ip_boundary_ntp()?;
        let external_ip = OmicronZoneExternalSnatIp {
            id: self.rng.sled_rng(sled_id).next_external_ip(),
            snat_cfg,
        };
        let nic = NetworkInterface {
            id: self.rng.sled_rng(sled_id).next_network_interface(),
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
                filesystem_pool,
                zone_type,
                image_source,
            },
        )
    }

    pub fn sled_expunge_zone(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
    ) -> Result<SledEditCounts, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to expunge zone on unknown sled {sled_id}"
            ))
        })?;
        let initial_counts = editor.edit_counts();
        editor
            .expunge_zone(&zone_id)
            .map_err(|err| Error::SledEditError { sled_id, err })?;
        let final_counts = editor.edit_counts();

        Ok(final_counts.difference_since(initial_counts))
    }

    pub fn sled_mark_expunged_zone_ready_for_cleanup(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
    ) -> Result<SledEditCounts, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to mark expunged zone ready for cleanup on unknown sled {sled_id}"
            ))
        })?;
        let initial_counts = editor.edit_counts();
        editor
            .mark_expunged_zone_ready_for_cleanup(&zone_id)
            .map_err(|err| Error::SledEditError { sled_id, err })?;
        let final_counts = editor.edit_counts();

        Ok(final_counts.difference_since(initial_counts))
    }

    pub fn sled_set_zone_source(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
        source: BlueprintZoneImageSource,
    ) -> Result<SledEditCounts, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to change image of zone on unknown sled {sled_id}"
            ))
        })?;
        let initial_counts = editor.edit_counts();
        editor
            .set_zone_image_source(&zone_id, source)
            .map_err(|err| Error::SledEditError { sled_id, err })?;
        let final_counts = editor.edit_counts();
        Ok(final_counts.difference_since(initial_counts))
    }

    pub(crate) fn apply_pending_host_phase_2_changes(
        &mut self,
        changes: PendingHostPhase2Changes,
    ) -> Result<(), Error> {
        for (sled_id, slot, contents) in changes.into_iter() {
            self.sled_set_host_phase_2_slot(sled_id, slot, contents)?;
        }
        Ok(())
    }

    pub fn sled_set_host_phase_2(
        &mut self,
        sled_id: SledUuid,
        host_phase_2: BlueprintHostPhase2DesiredSlots,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to change image of zone on unknown sled {sled_id}"
            ))
        })?;
        editor
            .set_host_phase_2(host_phase_2)
            .map_err(|err| Error::SledEditError { sled_id, err })
    }

    pub fn sled_set_host_phase_2_slot(
        &mut self,
        sled_id: SledUuid,
        slot: M2Slot,
        host_phase_2: BlueprintHostPhase2DesiredContents,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to change image of zone on unknown sled {sled_id}"
            ))
        })?;
        editor
            .set_host_phase_2_slot(slot, host_phase_2)
            .map_err(|err| Error::SledEditError { sled_id, err })
    }

    /// Set the `remove_mupdate_override` field of the given sled.
    pub fn sled_set_remove_mupdate_override(
        &mut self,
        sled_id: SledUuid,
        remove_mupdate_override: Option<MupdateOverrideUuid>,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to set sled state for unknown sled {sled_id}"
            ))
        })?;
        editor
            .set_remove_mupdate_override(remove_mupdate_override)
            .map_err(|err| Error::SledEditError { sled_id, err })
    }

    fn sled_add_zone(
        &mut self,
        sled_id: SledUuid,
        zone: BlueprintZoneConfig,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to add zone on unknown sled {sled_id}"
            ))
        })?;
        editor
            .add_zone(zone, self.rng.sled_rng(sled_id))
            .map_err(|err| Error::SledEditError { sled_id, err })
    }

    /// Returns a newly-allocated underlay address suitable for use by Omicron
    /// zones
    fn sled_alloc_ip(&mut self, sled_id: SledUuid) -> Result<Ipv6Addr, Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to allocate underlay IP on unknown sled {sled_id}"
            ))
        })?;
        editor
            .alloc_underlay_ip()
            .map_err(|err| Error::SledEditError { sled_id, err })
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
        let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to select zpool for unknown sled {sled_id}"
            ))
        })?;

        // We'll check both the disks available to this sled per our current
        // blueprint and the list of all in-service zpools on this sled per our
        // planning input, and only pick zpools that are available in both.
        let current_sled_disks = editor
            .disks(BlueprintPhysicalDiskDisposition::is_in_service)
            .map(|disk_config| disk_config.pool_id)
            .collect::<BTreeSet<_>>();

        let all_in_service_zpools =
            self.sled_resources(sled_id)?.all_zpools(ZpoolFilter::InService);

        // We refuse to choose a zpool for a zone of a given `zone_kind` if this
        // sled already has a durable zone of that kind on the same zpool. Build
        // up a set of invalid zpools for this sled/kind pair.
        let mut skip_zpools = BTreeSet::new();
        for zone_config in self
            .current_sled_zones(
                sled_id,
                BlueprintZoneDisposition::is_in_service,
            )
            .filter(|z| z.zone_type.kind() == zone_kind)
        {
            if let Some(zpool) = zone_config.zone_type.durable_zpool() {
                skip_zpools.insert(zpool);
            }
            skip_zpools.insert(&zone_config.filesystem_pool);
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
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .filter_map(|(_id, zone)| match &zone.zone_type {
                BlueprintZoneType::ExternalDns(dns) => {
                    Some(dns.dns_address.addr.ip())
                }
                _ => None,
            })
            .collect::<HashSet<IpAddr>>()
            .len()
    }

    /// Get the value of `target_release_minimum_generation`.
    pub fn target_release_minimum_generation(&self) -> Generation {
        self.target_release_minimum_generation
    }

    /// Given the current value of `target_release_minimum_generation`, set the
    /// new value for this blueprint.
    pub fn set_target_release_minimum_generation(
        &mut self,
        current_generation: Generation,
        new_generation: Generation,
    ) -> Result<(), Error> {
        if self.target_release_minimum_generation != current_generation {
            return Err(Error::TargetReleaseMinimumGenerationMismatch {
                expected: current_generation,
                actual: self.target_release_minimum_generation,
            });
        }
        self.target_release_minimum_generation = new_generation;
        self.record_operation(Operation::SetTargetReleaseMinimumGeneration {
            current_generation,
            new_generation,
        });
        Ok(())
    }

    /// Get the value of `nexus_generation`.
    pub fn nexus_generation(&self) -> Generation {
        self.nexus_generation
    }

    /// Given the current value of `nexus_generation`, set the new value for
    /// this blueprint.
    pub fn set_nexus_generation(&mut self, new_generation: Generation) {
        let current_generation = self.nexus_generation;
        self.nexus_generation = new_generation;
        self.record_operation(Operation::SetNexusGeneration {
            current_generation,
            new_generation,
        });
    }

    pub fn pending_mgs_updates_replace_all(
        &mut self,
        updates: nexus_types::deployment::PendingMgsUpdates,
    ) {
        self.pending_mgs_updates = updates;
    }

    pub fn pending_mgs_update_insert(
        &mut self,
        update: nexus_types::deployment::PendingMgsUpdate,
    ) {
        self.pending_mgs_updates.insert(update);
    }

    pub fn pending_mgs_update_delete(
        &mut self,
        baseboard_id: &Arc<BaseboardId>,
    ) {
        self.pending_mgs_updates.remove(baseboard_id);
    }

    /// Debug method to remove a sled from a blueprint entirely.
    ///
    /// Bypasses all expungement checks. Do not use in production.
    pub fn debug_sled_remove(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        if self.sled_editors.remove(&sled_id).is_none() {
            return Err(Error::Planner(anyhow!(
                "for sled {sled_id}, error looking up resources"
            )));
        }
        Ok(())
    }

    /// Debug method to force a sled agent generation number to be bumped, even
    /// if there are no changes to the sled.
    ///
    /// Do not use in production. Instead, update the logic that decides if the
    /// generation number should be bumped.
    pub fn debug_sled_force_generation_bump(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to force generation bump for unknown sled {sled_id}"
            ))
        })?;
        editor
            .debug_force_generation_bump()
            .map_err(|err| Error::SledEditError { sled_id, err })
    }
}

// Helper to validate that the system hasn't gone off the rails. There should
// never be any external networking resources in the planning input (which is
// derived from the contents of CRDB) that we don't know about from the parent
// blueprint. It's possible a given planning iteration could see such a state
// there have been intermediate changes made by other Nexus instances; e.g.,
//
// 1. Nexus A generates a `PlanningInput` by reading from CRDB
// 2. Nexus B executes on a target blueprint that removes IPs/NICs from
//    CRDB
// 3. Nexus B regenerates a new blueprint and prunes the zone(s) associated
//    with the IPs/NICs from step 2
// 4. Nexus B makes this new blueprint the target
// 5. Nexus A attempts to run planning with its `PlanningInput` from step 1 but
//    the target blueprint from step 4; this will fail the following checks
//    because the input contains records that were removed in step 3
//
// We do not need to handle this class of error; it's a transient failure that
// will clear itself up when Nexus A repeats its planning loop from the top and
// generates a new `PlanningInput`.
//
// There may still be database records corresponding to _expunged_ zones, but
// that's okay: it just means we haven't yet realized a blueprint where those
// zones are expunged. And those should should still be in the blueprint (not
// pruned) until their database records are cleaned up.
//
// It's also possible that there may be networking records in the database
// assigned to zones that have been expunged, and our parent blueprint uses
// those same records for new zones. This is also fine and expected, and is a
// similar case to the previous paragraph: a zone with networking resources was
// expunged, the database doesn't realize it yet, but can still move forward and
// make planning decisions that reuse those resources for new zones.
pub(super) fn ensure_input_networking_records_appear_in_parent_blueprint(
    parent_blueprint: &Blueprint,
    input: &PlanningInput,
) -> anyhow::Result<()> {
    use nexus_types::deployment::OmicronZoneExternalIp;
    use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
    use omicron_common::address::DNS_OPTE_IPV6_SUBNET;
    use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
    use omicron_common::address::NEXUS_OPTE_IPV6_SUBNET;
    use omicron_common::address::NTP_OPTE_IPV4_SUBNET;
    use omicron_common::address::NTP_OPTE_IPV6_SUBNET;
    use omicron_common::api::external::MacAddr;

    let mut all_macs: HashSet<MacAddr> = HashSet::new();
    let mut all_nexus_nic_ips: HashSet<IpAddr> = HashSet::new();
    let mut all_boundary_ntp_nic_ips: HashSet<IpAddr> = HashSet::new();
    let mut all_external_dns_nic_ips: HashSet<IpAddr> = HashSet::new();
    let mut all_external_ips: HashSet<OmicronZoneExternalIp> = HashSet::new();

    // Unlike the construction of the external IP allocator and existing IPs
    // constructed above in `BuilderExternalNetworking::new()`, we do not
    // check for duplicates here: we could very well see reuse of IPs
    // between expunged zones or between expunged -> running zones.
    for (_, z) in
        parent_blueprint.all_omicron_zones(BlueprintZoneDisposition::any)
    {
        let zone_type = &z.zone_type;
        match zone_type {
            BlueprintZoneType::BoundaryNtp(ntp) => {
                all_boundary_ntp_nic_ips.insert(ntp.nic.ip);
            }
            BlueprintZoneType::Nexus(nexus) => {
                all_nexus_nic_ips.insert(nexus.nic.ip);
            }
            BlueprintZoneType::ExternalDns(dns) => {
                all_external_dns_nic_ips.insert(dns.nic.ip);
            }
            _ => (),
        }

        if let Some((external_ip, nic)) = zone_type.external_networking() {
            // As above, ignore localhost (used by the test suite).
            if !external_ip.ip().is_loopback() {
                all_external_ips.insert(external_ip);
            }
            all_macs.insert(nic.mac);
        }
    }
    for external_ip_entry in
        input.network_resources().omicron_zone_external_ips()
    {
        // As above, ignore localhost (used by the test suite).
        if external_ip_entry.ip.ip().is_loopback() {
            continue;
        }
        if !all_external_ips.contains(&external_ip_entry.ip) {
            bail!(
                "planning input contains unexpected external IP \
                 (IP not found in parent blueprint): {external_ip_entry:?}"
            );
        }
    }
    for nic_entry in input.network_resources().omicron_zone_nics() {
        if !all_macs.contains(&nic_entry.nic.mac) {
            bail!(
                "planning input contains unexpected NIC \
                 (MAC not found in parent blueprint): {nic_entry:?}"
            );
        }
        match nic_entry.nic.ip {
            IpAddr::V4(ip) if NEXUS_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_nexus_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V4(ip) if NTP_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_boundary_ntp_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V4(ip) if DNS_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_external_dns_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V6(ip) if NEXUS_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_nexus_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V6(ip) if NTP_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_boundary_ntp_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V6(ip) if DNS_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_external_dns_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            _ => {
                bail!(
                    "planning input contains unexpected NIC \
                    (IP not contained in known OPTE subnet): {nic_entry:?}"
                )
            }
        }
    }
    Ok(())
}

/// The result of an `ensure_mupdate_override` call for a particular sled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EnsureMupdateOverrideAction {
    /// The inventory and blueprint overrides are consistent, so no action was
    /// taken.
    NoAction {
        /// The mupdate override currently in place.
        mupdate_override_id: Option<MupdateOverrideUuid>,
    },
    /// Inventory had an override that didn't match what was in the blueprint,
    /// so the blueprint was updated to match the inventory.
    BpSetOverride {
        /// The override ID that was set.
        inv_override: MupdateOverrideUuid,
        /// The previous blueprint override that was removed.
        prev_bp_override: Option<MupdateOverrideUuid>,
        /// The zones which were updated to the install dataset, along with
        /// their old values.
        zones: IdOrdMap<EnsureMupdateOverrideUpdatedZone>,
        /// The pending MGS update that was cleared, if any.
        prev_mgs_update: Option<Box<PendingMgsUpdate>>,
        /// The previous host phase 2 contents.
        prev_host_phase_2: BlueprintHostPhase2DesiredSlots,
    },
    /// The inventory did not have an override but the blueprint did, and other
    /// conditions were met, so the blueprint's override was cleared.
    BpClearOverride {
        /// The previous blueprint override that was removed.
        prev_bp_override: MupdateOverrideUuid,
    },
    /// The inventory did not have an override but the blueprint did, but some
    /// zones' image sources can't be converted over to Artifact, so the
    /// blueprint's override was left in place.
    BpOverrideNotCleared {
        /// The blueprint override that was not removed.
        bp_override: MupdateOverrideUuid,
        /// The reason the blueprint override was not cleared.
        reason: BpMupdateOverrideNotClearedReason,
    },
    /// Sled Agent encountered an error retrieving the mupdate override from the
    /// inventory.
    ///
    /// In this case, the blueprint's `remove_mupdate_override` field and zone
    /// image sources are not altered, but pending MGS and host phase 2 updates
    /// are cleared.
    GetOverrideError {
        /// An error message.
        message: String,
        /// The current blueprint override value, left unchanged.
        bp_override: Option<MupdateOverrideUuid>,
        /// The pending MGS update that was cleared, if any.
        prev_mgs_update: Option<Box<PendingMgsUpdate>>,
        /// The previous host phase 2 contents.
        prev_host_phase_2: BlueprintHostPhase2DesiredSlots,
    },
}

impl EnsureMupdateOverrideAction {
    pub fn log_to(&self, log: &slog::Logger) {
        match self {
            EnsureMupdateOverrideAction::NoAction {
                mupdate_override_id: mupdate_override,
            } => {
                debug!(
                    log,
                    "no mupdate override action taken, current value left unchanged";
                    "mupdate_override" => ?mupdate_override,
                );
            }
            EnsureMupdateOverrideAction::BpSetOverride {
                inv_override,
                prev_bp_override,
                zones,
                prev_mgs_update,
                prev_host_phase_2,
            } => {
                let zones_desc = zones_desc(zones);
                let host_phase_2_desc =
                    host_phase_2_to_current_contents_desc(prev_host_phase_2);

                info!(
                    log,
                    "blueprint mupdate override updated to match inventory";
                    "new_bp_override" => %inv_override,
                    "prev_bp_override" => ?prev_bp_override,
                    "zones" => zones_desc,
                    "host_phase_2" => host_phase_2_desc,
                );
                if let Some(prev_mgs_update) = prev_mgs_update {
                    info!(
                        log,
                        "previous MGS update cleared as part of updating \
                         blueprint mupdate override to match inventory";
                        prev_mgs_update,
                    );
                } else {
                    info!(
                        log,
                        "no previous MGS update found as part of updating \
                         blueprint mupdate override to match inventory",
                    );
                }
            }
            EnsureMupdateOverrideAction::BpClearOverride {
                prev_bp_override,
            } => {
                info!(
                    log,
                    "inventory override no longer exists, blueprint override \
                     cleared";
                    "prev_bp_override" => %prev_bp_override,
                )
            }
            EnsureMupdateOverrideAction::BpOverrideNotCleared {
                bp_override,
                reason,
            } => {
                info!(
                    log,
                    "inventory override no longer exists, but blueprint \
                     override could not be cleared";
                    "bp_override" => %bp_override,
                    "reason" => %reason,
                );
            }
            EnsureMupdateOverrideAction::GetOverrideError {
                message,
                bp_override,
                prev_mgs_update,
                prev_host_phase_2,
            } => {
                let host_phase_2_desc =
                    host_phase_2_to_current_contents_desc(prev_host_phase_2);
                error!(
                    log,
                    "error getting mupdate override info for sled, \
                     not altering blueprint override, but cleared \
                     pending host phase 2 updates if any";
                    "message" => %message,
                    "bp_override" => ?bp_override,
                    "prev_host_phase_2" => %host_phase_2_desc,
                );
                if let Some(prev_mgs_update) = prev_mgs_update {
                    info!(
                        log,
                        "previous MGS update cleared because there was an \
                         error obtaining mupdate override info";
                        prev_mgs_update,
                    );
                } else {
                    info!(
                        log,
                        "no previous MGS update found, so no action taken \
                         as part of updating blueprint because there was an \
                         error obtaining mupdate override info",
                    );
                }
            }
        }
    }
}

fn zones_desc(zones: &IdOrdMap<EnsureMupdateOverrideUpdatedZone>) -> String {
    let mut zones_desc = String::new();
    if zones.is_empty() {
        zones_desc.push_str("(none)");
    } else {
        // Add a newline before the first zone -- it makes it easier
        // to read in log output.
        zones_desc.push('\n');
        for zone in zones {
            swriteln!(zones_desc, "  - {}", zone);
        }
    }
    zones_desc
}

/// Return a description string that represents changing the host phase 2
/// contents from the provided value to `CurrentContents`.
fn host_phase_2_to_current_contents_desc(
    host_phase_2: &BlueprintHostPhase2DesiredSlots,
) -> String {
    let mut host_phase_2_desc = String::from("\n");
    let BlueprintHostPhase2DesiredSlots { slot_a, slot_b } = host_phase_2;
    match slot_a {
        BlueprintHostPhase2DesiredContents::CurrentContents => {
            swriteln!(
                host_phase_2_desc,
                "  - host phase 2 slot A: current contents (unchanged)"
            );
        }
        BlueprintHostPhase2DesiredContents::Artifact { version, hash } => {
            swriteln!(
                host_phase_2_desc,
                "  - host phase 2 slot A: updated from artifact \
                 (version {}, hash {}) to preserving current contents",
                version,
                hash
            );
        }
    }
    match slot_b {
        BlueprintHostPhase2DesiredContents::CurrentContents => {
            swriteln!(
                host_phase_2_desc,
                "  - host phase 2 slot B: current contents (unchanged)"
            );
        }
        BlueprintHostPhase2DesiredContents::Artifact { version, hash } => {
            swriteln!(
                host_phase_2_desc,
                "  - host phase 2 slot B: updated from artifact \
                 (version {}, hash {}) to preserving current contents",
                version,
                hash
            );
        }
    }

    host_phase_2_desc
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureMupdateOverrideUpdatedZone {
    /// The ID of the zone.
    pub zone_id: OmicronZoneUuid,

    /// The Omicron zone kind.
    pub kind: ZoneKind,

    /// The previous image source.
    pub old_image_source: BlueprintZoneImageSource,

    /// The new image source.
    pub new_image_source: BlueprintZoneImageSource,
}

impl fmt::Display for EnsureMupdateOverrideUpdatedZone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.old_image_source == self.new_image_source {
            write!(
                f,
                "zone {} ({:?}) left unchanged, image source: {}",
                self.zone_id, self.kind, self.old_image_source,
            )
        } else {
            write!(
                f,
                "zone {} ({:?}) updated from {} to {}",
                self.zone_id,
                self.kind,
                self.old_image_source,
                self.new_image_source,
            )
        }
    }
}

impl IdOrdItem for EnsureMupdateOverrideUpdatedZone {
    type Key<'a> = OmicronZoneUuid;
    fn key(&self) -> Self::Key<'_> {
        self.zone_id
    }
    id_upcast!();
}

/// The reason a blueprint's mupdate override for a sled was not cleared, even
/// though inventory no longer has the sled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BpMupdateOverrideNotClearedReason {
    /// There is a sled-specific reason noop conversions are not possible.
    NoopSledIneligible(NoopConvertSledIneligibleReason),
}

impl fmt::Display for BpMupdateOverrideNotClearedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BpMupdateOverrideNotClearedReason::NoopSledIneligible(reason) => {
                write!(
                    f,
                    "this sled cannot be noop-converted to Artifact: {reason}",
                )
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::example::ExampleSystemBuilder;
    use crate::example::SimRngState;
    use crate::example::example;
    use crate::planner::test::assert_planning_makes_no_changes;
    use crate::system::SledBuilder;
    use expectorate::assert_contents;
    use nexus_reconfigurator_blippy::Blippy;
    use nexus_reconfigurator_blippy::BlippyReportSortKey;
    use nexus_types::deployment::BlueprintArtifactVersion;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::ExternalIpPolicy;
    use nexus_types::deployment::OmicronZoneNetworkResources;
    use nexus_types::external_api::views::SledPolicy;
    use omicron_common::address::IpRange;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeSet;
    use std::mem;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    pub const DEFAULT_N_SLEDS: usize = 3;

    /// Checks various conditions that should be true for all blueprints
    #[track_caller]
    pub fn verify_blueprint(
        blueprint: &Blueprint,
        planning_input: &PlanningInput,
    ) {
        let blippy_report = Blippy::new(blueprint)
            .check_against_planning_input(planning_input)
            .into_report(BlippyReportSortKey::Kind);
        if !blippy_report.notes().is_empty() {
            eprintln!("{}", blueprint.display());
            eprintln!("---");
            eprintln!("{}", blippy_report.display());
            panic!("expected blippy report for blueprint to have no notes");
        }
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
        verify_blueprint(&blueprint1, &example.input);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &example.input,
            &example.collection,
            "test_basic",
            rng.next_planner_rng(),
        )
        .expect("failed to create builder");

        // The example blueprint should have internal NTP zones on all the
        // existing sleds, plus Crucible zones on all pools.  So if we ensure
        // all these zones exist, we should see no change.
        for (sled_id, sled_resources) in
            example.input.all_sled_resources(SledFilter::Commissioned)
        {
            builder.sled_add_disks(sled_id, sled_resources).unwrap();
            builder
                .sled_ensure_zone_ntp(
                    sled_id,
                    BlueprintZoneImageSource::InstallDataset,
                )
                .unwrap();
            for pool_id in sled_resources.zpools.keys() {
                builder
                    .sled_ensure_zone_crucible(
                        sled_id,
                        *pool_id,
                        BlueprintZoneImageSource::InstallDataset,
                    )
                    .unwrap();
            }
        }

        let blueprint2 = builder.build(BlueprintSource::Test);
        verify_blueprint(&blueprint2, &example.input);
        let summary = blueprint2.diff_since_blueprint(&blueprint1);
        println!(
            "initial blueprint -> next blueprint (expected no changes):\n{}",
            summary.display()
        );
        assert_eq!(summary.diff.sleds.added.len(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);
        assert_eq!(summary.diff.sleds.modified().count(), 0);

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
            rng.next_planner_rng(),
        )
        .expect("failed to create builder");
        let new_sled_resources = &input
            .sled_lookup(SledFilter::Commissioned, new_sled_id)
            .unwrap()
            .resources;
        builder.sled_add_disks(new_sled_id, &new_sled_resources).unwrap();
        builder
            .sled_ensure_zone_ntp(
                new_sled_id,
                BlueprintZoneImageSource::InstallDataset,
            )
            .unwrap();
        for pool_id in new_sled_resources.zpools.keys() {
            builder
                .sled_ensure_zone_crucible(
                    new_sled_id,
                    *pool_id,
                    BlueprintZoneImageSource::InstallDataset,
                )
                .unwrap();
        }
        builder.sled_ensure_zone_datasets(new_sled_id).unwrap();

        let blueprint3 = builder.build(BlueprintSource::Test);
        verify_blueprint(&blueprint3, &input);
        let summary = blueprint3.diff_since_blueprint(&blueprint2);
        println!(
            "expecting new NTP and Crucible zones:\n{}",
            summary.display()
        );

        // No sleds were changed or removed.
        assert_eq!(summary.diff.sleds.modified().count(), 0);
        assert_eq!(summary.diff.sleds.removed.len(), 0);

        // One sled was added.
        assert_eq!(summary.diff.sleds.added.len(), 1);
        let (&sled_id, new_sled) =
            summary.diff.sleds.added.first_key_value().unwrap();
        assert_eq!(*sled_id, new_sled_id);
        // The generation number should be newer than the initial default.
        assert!(new_sled.sled_agent_generation > Generation::new());

        // All zones' underlay addresses ought to be on the sled's subnet.
        for z in &new_sled.zones {
            assert!(new_sled_resources.subnet.net().contains(z.underlay_ip()));
        }

        // Check for an NTP zone.  Its sockaddr's IP should also be on the
        // sled's subnet.
        assert!(new_sled.zones.iter().any(|z| {
            if let BlueprintZoneConfig {
                zone_type:
                    BlueprintZoneType::InternalNtp(
                        blueprint_zone_type::InternalNtp { address, .. },
                    ),
                ..
            } = &z
            {
                assert!(
                    new_sled_resources.subnet.net().contains(*address.ip())
                );
                true
            } else {
                false
            }
        }));
        let crucible_pool_names =
            new_sled
                .zones
                .iter()
                .filter_map(|z| {
                    if let BlueprintZoneConfig {
                        zone_type:
                            BlueprintZoneType::Crucible(
                                blueprint_zone_type::Crucible {
                                    address,
                                    dataset,
                                },
                            ),
                        ..
                    } = &z
                    {
                        let ip = address.ip();
                        assert!(new_sled_resources.subnet.net().contains(*ip));
                        Some(dataset.pool_name)
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
            &example.collection,
            TEST_NAME,
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_decommissioned_sleds() {
        static TEST_NAME: &str = "blueprint_builder_test_decommissioned_sleds";
        let logctx = test_setup_log(TEST_NAME);
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (collection, input, mut blueprint1) =
            example(&logctx.log, TEST_NAME);
        verify_blueprint(&blueprint1, &input);

        // Mark one sled as having a desired state of decommissioned.
        let decommision_sled_id =
            blueprint1.sleds.keys().copied().next().expect("at least one sled");

        // We're going under the hood of the blueprint here; a sled can only get
        // to the decommissioned state if all its disks/datasets/zones have been
        // expunged, so do that too.
        {
            let sled_config =
                blueprint1.sleds.get_mut(&decommision_sled_id).unwrap();
            sled_config.state = SledState::Decommissioned;

            for mut zone in &mut sled_config.zones {
                zone.disposition = BlueprintZoneDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: false,
                };
            }
            for mut dataset in &mut sled_config.datasets {
                dataset.disposition = BlueprintDatasetDisposition::Expunged;
            }
            for mut disk in &mut sled_config.disks {
                disk.disposition = BlueprintPhysicalDiskDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: false,
                };
            }
        }

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
        let mut blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &input,
            &collection,
            "test_decommissioned_sleds",
            rng.next_planner_rng(),
        )
        .expect("created builder")
        .build(BlueprintSource::Test);
        verify_blueprint(&blueprint2, &input);

        // We carried forward the desired state.
        assert_eq!(
            blueprint2.sleds.get(&decommision_sled_id).map(|c| c.state),
            Some(SledState::Decommissioned)
        );

        // Change the input to mark the sled decommissioned. (Normally realizing
        // blueprint2 would make this change.) We must also mark all its zones
        // expunged to avoid tripping over an invalid state check in
        // `new_based_on()`.
        let mut builder = input.into_builder();
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().state =
            SledState::Decommissioned;
        let input = builder.build();
        for mut z in
            &mut blueprint2.sleds.get_mut(&decommision_sled_id).unwrap().zones
        {
            z.disposition = BlueprintZoneDisposition::Expunged {
                as_of_generation: Generation::new(),
                ready_for_cleanup: false,
            };
        }

        // Generate a new blueprint. This desired sled state should still be
        // decommissioned, because we haven't implemented all the cleanup
        // necessary to prune sleds from the blueprint.
        let blueprint3 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            &input,
            &collection,
            "test_decommissioned_sleds",
            rng.next_planner_rng(),
        )
        .expect("created builder")
        .build(BlueprintSource::Test);
        verify_blueprint(&blueprint3, &input);
        assert_eq!(
            blueprint3.sleds.get(&decommision_sled_id).map(|c| c.state),
            Some(SledState::Decommissioned),
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_add_physical_disks() {
        static TEST_NAME: &str = "blueprint_builder_test_add_physical_disks";
        let logctx = test_setup_log(TEST_NAME);
        let mut rng = SimRngState::from_seed(TEST_NAME);

        // Start with an empty system (sleds with no zones). However, we leave
        // the disks around so that `sled_add_disks` can add them.
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
                rng.next_planner_rng(),
            )
            .expect("failed to create builder");

            // In the map, we expect entries to be present for each sled, but
            // not have any disks in them.
            for sled_id in input.all_sled_ids(SledFilter::InService) {
                let disks = builder
                    .sled_editors
                    .get(&sled_id)
                    .unwrap()
                    .disks(BlueprintPhysicalDiskDisposition::any)
                    .collect::<Vec<_>>();
                assert!(
                    disks.is_empty(),
                    "expected empty disks for sled {sled_id}, got {disks:?}"
                );
            }

            for (sled_id, sled_resources) in
                input.all_sled_resources(SledFilter::InService)
            {
                let edits =
                    builder.sled_add_disks(sled_id, &sled_resources).unwrap();
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

            // We should have disks and a generation bump for every sled.
            let parent_gens = parent.sleds.iter().map(|(&sled_id, config)| {
                (sled_id, config.sled_agent_generation)
            });
            for (sled_id, parent_gen) in parent_gens {
                let EditedSled { config, .. } =
                    builder.sled_editors.remove(&sled_id).unwrap().finalize();
                assert_eq!(config.sled_agent_generation, parent_gen.next());
                assert_eq!(
                    config.disks.len(),
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

        for (_, sled_config) in &blueprint.sleds {
            for zone in &sled_config.zones {
                if let Some(durable_pool) = zone.zone_type.durable_zpool() {
                    assert_eq!(*durable_pool, zone.filesystem_pool);
                }
            }
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn test_datasets_for_zpools_and_zones() {
        static TEST_NAME: &str = "test_datasets_for_zpools_and_zones";
        let logctx = test_setup_log(TEST_NAME);
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (collection, input, blueprint) = example(&logctx.log, TEST_NAME);

        // Creating the "example" blueprint should already invoke
        // `sled_ensure_datasets`.
        //
        // Verify that it has created the datasets we expect to exist.
        verify_blueprint(&blueprint, &input);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
            rng.next_planner_rng(),
        )
        .expect("failed to create builder");

        // Before we make any modifications, there should be no work to do.
        //
        // If we haven't changed inputs, the output should be the same!
        for sled_id in input.all_sled_ids(SledFilter::Commissioned) {
            let r = builder.sled_ensure_zone_datasets(sled_id).unwrap();
            assert_eq!(r, EnsureMultiple::NotNeeded);
        }

        // Expunge a zone from the blueprint, observe that the dataset is
        // removed.
        let sled_id = input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled present");
        let editor =
            builder.sled_editors.get_mut(&sled_id).expect("found sled");
        let crucible_zone_id = editor
            .zones(BlueprintZoneDisposition::is_in_service)
            .find_map(|zone_config| {
                if zone_config.zone_type.is_crucible() {
                    return Some(zone_config.id);
                }
                None
            })
            .expect("at least one crucible must be present");
        println!("Expunging crucible zone: {crucible_zone_id}");

        let initial_counts = editor.edit_counts();
        editor.expunge_zone(&crucible_zone_id).expect("expunged crucible");
        let changed_counts =
            editor.edit_counts().difference_since(initial_counts);

        // In the case of Crucible, we have a durable dataset and a transient
        // zone filesystem, so we expect two datasets to be expunged.
        assert_eq!(
            changed_counts.datasets,
            EditCounts { added: 0, updated: 0, expunged: 2, removed: 0 }
        );
        // Once the datasets are expunged, no further changes will be proposed.
        let r = builder.sled_ensure_zone_datasets(sled_id).unwrap();
        assert_eq!(r, EnsureMultiple::NotNeeded);

        let blueprint = builder.build(BlueprintSource::Test);
        verify_blueprint(&blueprint, &input);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
            rng.next_planner_rng(),
        )
        .expect("failed to create builder");

        // While the datasets still exist in the input (effectively, the db) we
        // cannot remove them.
        let r = builder.sled_ensure_zone_datasets(sled_id).unwrap();
        assert_eq!(r, EnsureMultiple::NotNeeded);

        let blueprint = builder.build(BlueprintSource::Test);
        verify_blueprint(&blueprint, &input);

        // Find the datasets we've expunged in the blueprint
        let expunged_datasets = blueprint
            .sleds
            .get(&sled_id)
            .unwrap()
            .datasets
            .iter()
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

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
            rng.next_planner_rng(),
        )
        .expect("failed to create builder");

        // Now, we should see the datasets "removed" from the blueprint, since
        // we no longer need to keep around records of their expungement.
        let r = builder.sled_ensure_zone_datasets(sled_id).unwrap();

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
        let mut rng = SimRngState::from_seed(TEST_NAME);

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
            rng.next_planner_rng(),
        )
        .expect("failed to create builder");

        let err = builder
            .sled_add_zone_nexus(
                collection
                    .sled_agents
                    .iter()
                    .next()
                    .map(|sa| sa.sled_id)
                    .expect("no sleds present"),
                BlueprintZoneImageSource::InstallDataset,
                parent.nexus_generation,
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
        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (mut collection, mut input, mut parent) =
            example(&logctx.log, TEST_NAME);

        // Remove the Nexus zone from one of the sleds so that
        // `sled_ensure_zone_nexus` can attempt to add a Nexus zone to
        // `sled_id`.
        let sled_id = {
            let mut selected_sled_id = None;
            for mut sa in &mut collection.sled_agents {
                let sa_zones = &mut sa
                    .ledgered_sled_config
                    .as_mut()
                    .expect("example should have a sled config")
                    .zones;
                let nzones_before_retain = sa_zones.len();
                sa_zones.retain(|z| !z.zone_type.is_nexus());
                if sa_zones.len() < nzones_before_retain {
                    selected_sled_id = Some(sa.sled_id);
                    // Also remove this zone from the blueprint.
                    let mut removed_nexus = None;
                    parent
                        .sleds
                        .get_mut(&sa.sled_id)
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
                rng.next_planner_rng(),
            )
            .expect("failed to create builder");
            builder
                .sled_add_zone_nexus(
                    sled_id,
                    BlueprintZoneImageSource::InstallDataset,
                    parent.nexus_generation,
                )
                .expect("added nexus zone");
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
                rng.next_planner_rng(),
            )
            .expect("failed to create builder");
            for _ in 0..3 {
                builder
                    .sled_add_zone_nexus(
                        sled_id,
                        BlueprintZoneImageSource::InstallDataset,
                        parent.nexus_generation,
                    )
                    .expect("added nexus zone");
            }
        }

        {
            // Replace the policy's external service IP pool ranges with ranges
            // that are already in use by existing zones. Attempting to add a
            // Nexus with no remaining external IPs should fail.
            let mut used_ip_ranges = Vec::new();
            for (_, z) in
                parent.all_omicron_zones(BlueprintZoneDisposition::any)
            {
                if let Some((external_ip, _)) =
                    z.zone_type.external_networking()
                {
                    used_ip_ranges.push(IpRange::from(external_ip.ip()));
                }
            }
            assert!(!used_ip_ranges.is_empty());
            let input = {
                let mut builder = input.into_builder();
                builder.policy_mut().external_ips = {
                    let mut ip_policy = ExternalIpPolicy::builder();
                    for r in used_ip_ranges {
                        ip_policy.push_service_pool_range(r).unwrap();
                    }
                    ip_policy.build()
                };
                builder.build()
            };

            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &parent,
                &input,
                &collection,
                "test",
                rng.next_planner_rng(),
            )
            .expect("failed to create builder");
            let err = builder
                .sled_add_zone_nexus(
                    sled_id,
                    BlueprintZoneImageSource::InstallDataset,
                    parent.nexus_generation,
                )
                .unwrap_err();

            assert!(
                matches!(
                    err,
                    Error::AllocateExternalNetworking(
                        ExternalNetworkingError::NoExternalServiceIpAvailable
                    )
                ),
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
        let mut rng = SimRngState::from_seed(TEST_NAME);

        // Start with an example system (no CRDB zones).
        let (example, parent) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).build();
        let collection = example.collection;
        let input = example.input;

        // Ensure no CRDB zones (currently `ExampleSystemBuilder` never
        // provisions CRDB; this check makes sure we update our use of it if
        // that changes).
        for (_, z) in
            parent.all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        {
            assert!(
                !z.zone_type.is_cockroach(),
                "unexpected cockroach zone \
                 (update use of ExampleSystemBuilder?): {z:?}"
            );
        }

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
            rng.next_planner_rng(),
        )
        .expect("constructed builder");
        for _ in 0..num_sled_zpools {
            builder
                .sled_add_zone_cockroachdb(
                    target_sled_id,
                    BlueprintZoneImageSource::InstallDataset,
                )
                .expect("added CRDB zone");
        }
        builder.sled_ensure_zone_datasets(target_sled_id).unwrap();

        let blueprint = builder.build(BlueprintSource::Test);
        verify_blueprint(&blueprint, &input);
        assert_eq!(
            blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
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
            &collection,
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
            rng.next_planner_rng(),
        )
        .expect("constructed builder");
        for _ in 0..num_sled_zpools {
            builder
                .sled_add_zone_cockroachdb(
                    target_sled_id,
                    BlueprintZoneImageSource::InstallDataset,
                )
                .expect("added CRDB zone");
        }
        let err = builder
            .sled_add_zone_cockroachdb(
                target_sled_id,
                BlueprintZoneImageSource::InstallDataset,
            )
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

    /// Test that if an Omicron zone's image source changes, the diff reflects the change.
    #[test]
    fn test_zone_image_source_change_diff() {
        static TEST_NAME: &str = "builder_zone_image_source_change_diff";
        let logctx = test_setup_log(TEST_NAME);
        let log = logctx.log.clone();
        let mut rng = SimRngState::from_seed(TEST_NAME);

        // Use our example system.
        let (system, blueprint1) =
            ExampleSystemBuilder::new(&log, TEST_NAME).nsleds(1).build();

        // Find a zone and change its image source.
        let mut blueprint_builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &system.input,
            &system.collection,
            TEST_NAME,
            rng.next_planner_rng(),
        )
        .expect("built blueprint builder");

        let sled_id = system
            .input
            .all_sled_ids(SledFilter::All)
            .next()
            .expect("system has one sled");
        {
            let editor = blueprint_builder
                .sled_editors
                .get_mut(&sled_id)
                .expect("sled editor exists");
            let mut zones = blueprint1
                .sleds
                .get(&sled_id)
                .expect("sled exists")
                .zones
                .iter();
            let zone_id = zones.next().expect("zone exists").id;
            // Set the zone's image source to an artifact.
            const ARTIFACT_VERSION: ArtifactVersion =
                ArtifactVersion::new_const("1.2.3");
            editor
                .set_zone_image_source(
                    &zone_id,
                    BlueprintZoneImageSource::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION,
                        },
                        // The hash is not displayed in the diff -- only the
                        // version is.
                        hash: ArtifactHash([0x12; 32]),
                    },
                )
                .expect("set zone image source successfully");
        }

        let blueprint2 = blueprint_builder.build(BlueprintSource::Test);
        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        let display = diff.display();
        assert_contents(
            "tests/output/zone_image_source_change_1.txt",
            &display.to_string(),
        );

        logctx.cleanup_successful();
    }
}
