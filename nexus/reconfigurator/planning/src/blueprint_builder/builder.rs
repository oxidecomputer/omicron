// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level facility for generating Blueprints

use crate::blueprint_editor::DatasetIdsBackfillFromDb;
use crate::blueprint_editor::EditedSled;
use crate::blueprint_editor::SledEditError;
use crate::blueprint_editor::SledEditor;
use crate::ip_allocator::IpAllocator;
use crate::planner::rng::PlannerRng;
use crate::planner::zone_needs_expungement;
use crate::planner::ZoneExpungeReason;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context as _;
use clickhouse_admin_types::OXIMETER_CLUSTER;
use ipnet::IpAdd;
use nexus_inventory::now_db_precision;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
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
use nexus_types::deployment::SledLookupErrorKind;
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
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::iter;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use thiserror::Error;

use super::clickhouse::ClickhouseAllocator;
use super::external_networking::ensure_input_networking_records_appear_in_parent_blueprint;
use super::external_networking::BuilderExternalNetworking;
use super::external_networking::ExternalNetworkingChoice;
use super::external_networking::ExternalSnatNetworkingChoice;
use super::internal_dns::DnsSubnetAllocator;

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
    #[error("error editing sled {sled_id}")]
    SledEditError {
        sled_id: SledUuid,
        #[source]
        err: SledEditError,
    },
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
    sled_editors: BTreeMap<SledUuid, SledEditor>,
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
                    zones: BTreeMap::new(),
                };
                (sled_id, config)
            })
            .collect::<BTreeMap<_, _>>();
        let blueprint_disks = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| {
                let config = BlueprintPhysicalDisksConfig {
                    generation: Generation::new(),
                    disks: Vec::new(),
                };
                (sled_id, config)
            })
            .collect();
        let blueprint_datasets = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| {
                let config = BlueprintDatasetsConfig {
                    generation: Generation::new(),
                    datasets: BTreeMap::new(),
                };
                (sled_id, config)
            })
            .collect();
        let num_sleds = blueprint_zones.len();
        let sled_state = blueprint_zones
            .keys()
            .copied()
            .map(|sled_id| (sled_id, SledState::Active))
            .collect();

        Blueprint {
            id: rng.next_blueprint(),
            blueprint_zones,
            blueprint_disks,
            blueprint_datasets,
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

        // Helper to build a `PreexistingDatasetIds` for a given sled. This will
        // go away with https://github.com/oxidecomputer/omicron/issues/6645.
        let build_preexisting_dataset_ids =
            |sled_id| -> anyhow::Result<DatasetIdsBackfillFromDb> {
                match input.sled_lookup(SledFilter::All, sled_id) {
                    Ok(details) => {
                        DatasetIdsBackfillFromDb::build(&details.resources)
                            .with_context(|| {
                                format!(
                                    "failed building map of preexisting \
                             dataset IDs for sled {sled_id}"
                                )
                            })
                    }
                    Err(err) => match err.kind() {
                        SledLookupErrorKind::Missing => {
                            Ok(DatasetIdsBackfillFromDb::empty())
                        }
                        SledLookupErrorKind::Filtered { .. } => unreachable!(
                            "SledFilter::All should not filter anything out"
                        ),
                    },
                }
            };

        // Squish the disparate maps in our parent blueprint into one map of
        // `SledEditor`s.
        let mut sled_editors = BTreeMap::new();
        for (sled_id, zones) in &parent_blueprint.blueprint_zones {
            // Prefer the sled state from our parent blueprint for sleds
            // that were in it.
            let state = match parent_blueprint.sled_state.get(sled_id).copied()
            {
                Some(state) => state,
                None => {
                    // If we have zones but no state for a sled, we assume
                    // it was removed by an earlier version of the planner
                    // (which pruned decommissioned sleds from
                    // `sled_state`). Check that all of its zones are
                    // expunged, which is a prerequisite for
                    // decommissioning. If any zones aren't, then we don't
                    // know what to do: the state is missing but we can't
                    // assume "decommissioned", so fail.
                    if zones.are_all_zones_expunged() {
                        SledState::Decommissioned
                    } else {
                        bail!(
                            "sled {sled_id} is missing in parent blueprint \
                             sled_state map, but has non-expunged zones"
                        );
                    }
                }
            };

            // If we don't have disks/datasets entries, we'll start with an
            // empty config and rely on `sled_ensure_{disks,datasets}` calls to
            // populate it. It's also possible our parent blueprint removed
            // entries because our sled has been expunged, in which case we
            // won't do any further editing and what we fill in here is
            // irrelevant.
            let disks = parent_blueprint
                .blueprint_disks
                .get(sled_id)
                .cloned()
                .unwrap_or_else(|| BlueprintPhysicalDisksConfig {
                    generation: Generation::new(),
                    disks: Vec::new(),
                });
            let datasets = parent_blueprint
                .blueprint_datasets
                .get(sled_id)
                .cloned()
                .unwrap_or_else(|| BlueprintDatasetsConfig {
                    generation: Generation::new(),
                    datasets: BTreeMap::new(),
                });
            let editor = SledEditor::for_existing(
                state,
                zones.clone(),
                disks,
                datasets.clone(),
                build_preexisting_dataset_ids(*sled_id)?,
            )
            .with_context(|| {
                format!("failed to construct SledEditor for sled {sled_id}")
            })?;
            sled_editors.insert(*sled_id, editor);
        }

        // Add new, empty `SledEditor`s for any commissioned sleds in our input
        // that weren't in the parent blueprint. (These are newly-added sleds.)
        for sled_id in input.all_sled_ids(SledFilter::Commissioned) {
            if let Entry::Vacant(slot) = sled_editors.entry(sled_id) {
                slot.insert(SledEditor::for_new_active(
                    build_preexisting_dataset_ids(sled_id)?,
                ));
            }
        }

        Ok(BlueprintBuilder {
            log,
            parent_blueprint,
            collection: inventory,
            input,
            sled_ip_allocators: BTreeMap::new(),
            external_networking: OnceCell::new(),
            internal_dns_subnets: OnceCell::new(),
            sled_editors,
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
                    self.sled_editors.values().flat_map(|editor| {
                        editor.zones(BlueprintZoneFilter::ShouldBeRunning)
                    }),
                    self.sled_editors.values().flat_map(|editor| {
                        editor.zones(BlueprintZoneFilter::Expunged)
                    }),
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
                self.sled_editors.values().flat_map(|editor| {
                    editor.zones(BlueprintZoneFilter::ShouldBeRunning)
                }),
                self.input,
            )
        })?;
        Ok(self.internal_dns_subnets.get_mut().unwrap())
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.sled_editors.keys().copied()
    }

    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        let Some(editor) = self.sled_editors.get(&sled_id) else {
            return Box::new(iter::empty())
                as Box<dyn Iterator<Item = &BlueprintZoneConfig>>;
        };
        Box::new(editor.zones(filter))
    }

    /// Assemble a final [`Blueprint`] based on the contents of the builder
    pub fn build(mut self) -> Blueprint {
        let blueprint_id = self.rng.next_blueprint();

        // Collect the Omicron zones config for all sleds, including sleds that
        // are no longer in service and need expungement work.
        let mut sled_state = BTreeMap::new();
        let mut blueprint_zones = BTreeMap::new();
        let mut blueprint_disks = BTreeMap::new();
        let mut blueprint_datasets = BTreeMap::new();
        for (sled_id, editor) in self.sled_editors {
            let EditedSled { zones, disks, datasets, state, edit_counts } =
                editor.finalize();
            sled_state.insert(sled_id, state);
            blueprint_disks.insert(sled_id, disks);
            blueprint_datasets.insert(sled_id, datasets);
            blueprint_zones.insert(sled_id, zones);
            if edit_counts.has_nonzero_counts() {
                debug!(
                    self.log, "sled modified in new blueprint";
                    "sled_id" => %sled_id,
                    "blueprint_id" => %blueprint_id,
                    "disk_edits" => ?edit_counts.disks,
                    "dataset_edits" => ?edit_counts.datasets,
                    "zone_edits" => ?edit_counts.zones,
                );
            } else {
                debug!(
                    self.log, "sled unchanged in new blueprint";
                    "sled_id" => %sled_id,
                    "blueprint_id" => %blueprint_id,
                );
            }
        }
        // Preserving backwards compatibility, for now: prune sled_state of any
        // fully decommissioned sleds, which we determine by the state being
        // `Decommissioned` _and_ the sled is no longer in our PlanningInput's
        // list of commissioned sleds.
        let commissioned_sled_ids = self
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .collect::<BTreeSet<_>>();
        sled_state.retain(|sled_id, state| {
            *state != SledState::Decommissioned
                || commissioned_sled_ids.contains(sled_id)
        });
        // Preserving backwards compatibility, for now: disks should only
        // have entries for in-service sleds, and expunged disks should be
        // removed entirely.
        let in_service_sled_ids = self
            .input
            .all_sled_ids(SledFilter::InService)
            .collect::<BTreeSet<_>>();
        blueprint_disks.retain(|sled_id, disks_config| {
            if !in_service_sled_ids.contains(sled_id) {
                return false;
            }

            disks_config.disks.retain(|config| match config.disposition {
                BlueprintPhysicalDiskDisposition::InService => true,
                BlueprintPhysicalDiskDisposition::Expunged => false,
            });

            true
        });
        // Preserving backwards compatibility, for now: datasets should only
        // have entries for in-service sleds.
        blueprint_datasets
            .retain(|sled_id, _| in_service_sled_ids.contains(sled_id));

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
            match a.plan(&(&blueprint_zones).into()) {
                Ok(config) => config,
                Err(e) => {
                    error!(self.log, "clickhouse allocator error: {e}");
                    a.parent_config().clone()
                }
            }
        });
        Blueprint {
            id: blueprint_id,
            blueprint_zones,
            blueprint_disks,
            blueprint_datasets,
            sled_state,
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

        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to expunge zones for unknown sled {sled_id}"
            ))
        })?;

        // Do any zones need to be marked expunged?
        let mut zones_to_expunge = BTreeMap::new();

        for zone_config in editor.zones(BlueprintZoneFilter::All) {
            let zone_id = zone_config.id;
            let log = log.new(o!(
                "zone_id" => zone_id.to_string()
            ));

            let Some(reason) =
                zone_needs_expungement(sled_details, zone_config, &self.input)
            else {
                continue;
            };

            // TODO-john we lost the check for "are we expunging a zone we
            // modified in this planner iteration" - do we need that?
            let is_expunged = match zone_config.disposition {
                BlueprintZoneDisposition::InService
                | BlueprintZoneDisposition::Quiesced => false,
                BlueprintZoneDisposition::Expunged => true,
            };

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

        // Now expunge all the zones that need it.
        for zone_id in zones_to_expunge.keys() {
            editor
                .expunge_zone(&zone_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
        }

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
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to ensure disks for unknown sled {sled_id}"
            ))
        })?;
        let initial_counts = editor.edit_counts();

        let blueprint_disk_ids = editor
            .disks(DiskFilter::InService)
            .map(|config| config.id)
            .collect::<Vec<_>>();

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

        // Remove any disks that appear in the blueprint, but not the database
        for disk_id in blueprint_disk_ids {
            if !database_disk_ids.contains(&disk_id) {
                editor
                    .expunge_disk(&disk_id)
                    .map_err(|err| Error::SledEditError { sled_id, err })?;
            }
        }
        let final_counts = editor.edit_counts();

        Ok(final_counts.difference_since(initial_counts))
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
            id: self.rng.sled_rng(sled_id).next_zone(),
            filesystem_pool: Some(zpool),
            zone_type,
        };

        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_external_dns(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let id = self.rng.sled_rng(sled_id).next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.external_networking()?.for_new_external_dns()?;
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
        let has_ntp = {
            let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
                Error::Planner(anyhow!(
                    "tried to ensure NTP zone for unknown sled {sled_id}"
                ))
            })?;
            editor
                .zones(BlueprintZoneFilter::ShouldBeRunning)
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
        let has_crucible_on_this_pool = {
            let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
                Error::Planner(anyhow!(
                    "tried to ensure crucible zone for unknown sled {sled_id}"
                ))
            })?;
            editor.zones(BlueprintZoneFilter::ShouldBeRunning).any(|z| {
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
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });
        let filesystem_pool = pool_name;

        let zone = BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.sled_rng(sled_id).next_zone(),
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
        let Some(editor) = self.sled_editors.get(&sled_id) else {
            return 0;
        };
        editor
            .zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter(|z| z.zone_type.kind() == kind)
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
        let nexus_id = self.rng.sled_rng(sled_id).next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self.external_networking()?.for_new_nexus()?;
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
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        };
        self.sled_add_zone(sled_id, zone)
    }

    pub fn sled_add_zone_crucible_pantry(
        &mut self,
        sled_id: SledUuid,
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
        let zone_id = self.rng.sled_rng(sled_id).next_zone();
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
        let id = self.rng.sled_rng(sled_id).next_zone();
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
        let zone_id = self.rng.sled_rng(sled_id).next_zone();
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
        let zone_id = self.rng.sled_rng(sled_id).next_zone();
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
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to promote NTP zone on unknown sled {sled_id}"
            ))
        })?;

        // Find the internal NTP zone and expunge it.
        let mut internal_ntp_zone_id_iter = editor
            .zones(BlueprintZoneFilter::ShouldBeRunning)
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
        } = self.external_networking()?.for_new_boundary_ntp()?;
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
        let sled_subnet = self.sled_resources(sled_id)?.subnet;
        let editor = self.sled_editors.get(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to allocate underlay IP for unknown sled {sled_id}"
            ))
        })?;
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
                for z in editor.zones(BlueprintZoneFilter::All) {
                    allocator.reserve(z.underlay_ip());
                }

                allocator
            });

        allocator.alloc().ok_or(Error::OutOfAddresses { sled_id })
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
            .disks(DiskFilter::InService)
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
            .filter(|z| z.zone_type.kind() == zone_kind)
        {
            if let Some(zpool) = zone_config.zone_type.durable_zpool() {
                skip_zpools.insert(zpool);
            }
            if let Some(zpool) = &zone_config.filesystem_pool {
                skip_zpools.insert(zpool);
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

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::example::example;
    use crate::example::ExampleSystemBuilder;
    use crate::example::SimRngState;
    use crate::system::SledBuilder;
    use nexus_inventory::CollectionBuilder;
    use nexus_reconfigurator_blippy::Blippy;
    use nexus_reconfigurator_blippy::BlippyReportSortKey;
    use nexus_types::deployment::BlueprintDatasetDisposition;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::OmicronZoneNetworkResources;
    use nexus_types::external_api::views::SledPolicy;
    use omicron_common::address::IpRange;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeSet;
    use std::mem;

    pub const DEFAULT_N_SLEDS: usize = 3;

    /// Checks various conditions that should be true for all blueprints
    #[track_caller]
    pub fn verify_blueprint(blueprint: &Blueprint) {
        let blippy_report =
            Blippy::new(blueprint).into_report(BlippyReportSortKey::Kind);
        if !blippy_report.notes().is_empty() {
            eprintln!("{}", blueprint.display());
            eprintln!("---");
            eprintln!("{}", blippy_report.display());
            panic!("expected blippy report for blueprint to have no notes");
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
        builder.sled_ensure_zone_datasets(new_sled_id).unwrap();

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
            if let BlueprintZoneConfig {
                zone_type:
                    BlueprintZoneType::InternalNtp(
                        blueprint_zone_type::InternalNtp { address, .. },
                    ),
                ..
            } = &z
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
        let crucible_pool_names =
            new_sled_zones
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

        // We're going under the hood of the blueprint here; a sled can only get
        // to the decommissioned state if all its disks/datasets/zones have been
        // expunged, so do that too.
        for (_, zone) in &mut blueprint1
            .blueprint_zones
            .get_mut(&decommision_sled_id)
            .expect("has zones")
            .zones
        {
            zone.disposition = BlueprintZoneDisposition::Expunged;
        }
        blueprint1.blueprint_datasets.remove(&decommision_sled_id);
        blueprint1.blueprint_disks.remove(&decommision_sled_id);

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
        // blueprint2 would make this change.) We must also mark all its zones
        // expunged to avoid tripping over an invalid state check in
        // `new_based_on()`.
        let mut builder = input.into_builder();
        builder.sleds_mut().get_mut(&decommision_sled_id).unwrap().state =
            SledState::Decommissioned;
        let input = builder.build();
        for (_, z) in &mut blueprint2
            .blueprint_zones
            .get_mut(&decommision_sled_id)
            .unwrap()
            .zones
        {
            z.disposition = BlueprintZoneDisposition::Expunged;
        }

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
                    .sled_editors
                    .get(&sled_id)
                    .unwrap()
                    .disks(DiskFilter::All)
                    .collect::<Vec<_>>();
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

            // We should have disks and a generation bump for every sled.
            let parent_disk_gens = parent
                .blueprint_disks
                .iter()
                .map(|(&sled_id, config)| (sled_id, config.generation));
            for (sled_id, parent_gen) in parent_disk_gens {
                let EditedSled { disks: new_sled_disks, .. } =
                    builder.sled_editors.remove(&sled_id).unwrap().finalize();
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
            for (_, zone) in &zone_config.zones {
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
            .zones(BlueprintZoneFilter::ShouldBeRunning)
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
        let r = builder.sled_ensure_zone_datasets(sled_id).unwrap();
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
                        .retain(|_, z| match &z.zone_type {
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
        builder.sled_ensure_zone_datasets(target_sled_id).unwrap();

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

    // This test can go away with
    // https://github.com/oxidecomputer/omicron/issues/6645; for now, it
    // confirms we maintain the compatibility layer it needs.
    #[test]
    fn test_backcompat_reuse_existing_database_dataset_ids() {
        static TEST_NAME: &str =
            "backcompat_reuse_existing_database_dataset_ids";
        let logctx = test_setup_log(TEST_NAME);

        // Start with the standard example blueprint.
        let (collection, input, mut parent) = example(&logctx.log, TEST_NAME);

        // `parent` was not created prior to the addition of disks and datasets,
        // so it should have datasets for all the disks and zones, and the
        // dataset IDs should match the input.
        let mut input_dataset_ids = BTreeMap::new();
        let mut input_ndatasets = 0;
        for (_, resources) in input.all_sled_resources(SledFilter::All) {
            for (zpool_id, dataset_configs) in
                resources.all_datasets(ZpoolFilter::All)
            {
                for dataset in dataset_configs {
                    let id = dataset.id;
                    let kind = dataset.name.kind();
                    let by_kind: &mut BTreeMap<_, _> =
                        input_dataset_ids.entry(*zpool_id).or_default();
                    let prev = by_kind.insert(kind.clone(), id);
                    input_ndatasets += 1;
                    assert!(prev.is_none());
                }
            }
        }
        // We should have 3 datasets per disk (debug + zone root + crucible),
        // plus some number of datasets for discretionary zones. We'll just
        // check that we have more than 3 per disk.
        assert!(
            input_ndatasets
                > 3 * usize::from(SledBuilder::DEFAULT_NPOOLS)
                    * ExampleSystemBuilder::DEFAULT_N_SLEDS,
            "too few datasets: {input_ndatasets}"
        );

        // Now _remove_ the blueprint datasets entirely, to emulate a
        // pre-dataset-addition blueprint.
        parent.blueprint_datasets = BTreeMap::new();

        // Build a new blueprint.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &parent,
            &input,
            &collection,
            TEST_NAME,
        )
        .expect("failed to create builder");

        // Ensure disks and datasets. This should repopulate the datasets.
        for (sled_id, resources) in input.all_sled_resources(SledFilter::All) {
            builder
                .sled_ensure_disks(sled_id, resources)
                .expect("ensured disks");
            builder
                .sled_ensure_zone_datasets(sled_id)
                .expect("ensured zone datasets");
        }
        let output = builder.build();

        // Repeat the logic above on our new blueprint; it should have the same
        // number of datasets, and they should all have identical IDs.
        let mut output_dataset_ids = BTreeMap::new();
        let mut output_ndatasets = 0;
        for datasets in output.blueprint_datasets.values() {
            for (id, dataset) in &datasets.datasets {
                let zpool_id = dataset.pool.id();
                let kind = dataset.kind.clone();
                let by_kind: &mut BTreeMap<_, _> =
                    output_dataset_ids.entry(zpool_id).or_default();
                let prev = by_kind.insert(kind, *id);
                output_ndatasets += 1;
                assert!(prev.is_none());
            }
        }
        assert_eq!(input_ndatasets, output_ndatasets);
        assert_eq!(input_dataset_ids, output_dataset_ids);

        logctx.cleanup_successful();
    }
}
