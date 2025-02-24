// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for editing the blueprint details of a single sled.

use crate::blueprint_builder::SledEditCounts;
use crate::planner::SledPlannerRng;
use illumos_utils::zpool::ZpoolName;
use itertools::Either;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetFilter;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Dataset;
use nexus_types::inventory::Zpool;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog::info;
use slog::warn;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::iter;
use std::mem;
use std::net::Ipv6Addr;
use underlay_ip_allocator::SledUnderlayIpAllocator;

mod datasets;
mod disks;
mod underlay_ip_allocator;
mod zones;

pub use self::datasets::DatasetsEditError;
pub use self::datasets::MultipleDatasetsOfKind;
pub use self::disks::DisksEditError;
pub use self::disks::DuplicateDiskId;
pub use self::zones::DuplicateZoneId;
pub use self::zones::ZonesEditError;

use self::datasets::DatasetsEditor;
use self::datasets::PartialDatasetConfig;
use self::disks::DisksEditor;
use self::zones::ZonesEditor;

#[derive(Debug, thiserror::Error)]
pub enum SledInputError {
    #[error(transparent)]
    DuplicateZoneId(#[from] DuplicateZoneId),
    #[error(transparent)]
    DuplicateDiskId(#[from] DuplicateDiskId),
    #[error(transparent)]
    MultipleDatasetsOfKind(#[from] MultipleDatasetsOfKind),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiskExpungeDetails {
    pub disk_id: PhysicalDiskUuid,
    pub did_expunge_disk: bool,
    pub num_datasets_expunged: usize,
    pub num_zones_expunged: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum SledEditError {
    #[error("editing a decommissioned sled is not allowed")]
    EditDecommissioned,
    #[error(
        "sled is not decommissionable: \
         disk {disk_id} (zpool {zpool_id}) is in service"
    )]
    NonDecommissionableDiskInService {
        disk_id: PhysicalDiskUuid,
        zpool_id: ZpoolUuid,
    },
    #[error(
        "sled is not decommissionable: \
         dataset {dataset_id} (kind {kind:?}) is in service"
    )]
    NonDecommissionableDatasetInService {
        dataset_id: DatasetUuid,
        kind: DatasetKind,
    },
    #[error(
        "sled is not decommissionable: \
         zone {zone_id} (kind {kind:?}) is not expunged"
    )]
    NonDecommissionableZoneNotExpunged {
        zone_id: OmicronZoneUuid,
        kind: ZoneKind,
    },
    #[error("failed to edit disks")]
    EditDisks(#[from] DisksEditError),
    #[error("failed to edit datasets")]
    EditDatasetsError(#[from] DatasetsEditError),
    #[error("failed to edit zones")]
    EditZones(#[from] ZonesEditError),
    #[error(
        "invalid configuration for zone {zone_id}: \
         filesystem root zpool ({fs_zpool}) and durable dataset zpool \
         ({dur_zpool}) should be the same"
    )]
    ZoneInvalidZpoolCombination {
        zone_id: OmicronZoneUuid,
        fs_zpool: ZpoolName,
        dur_zpool: ZpoolName,
    },
    #[error(
        "invalid configuration for zone {zone_id}: \
         zpool ({zpool}) is not present in this sled's disks"
    )]
    ZoneOnNonexistentZpool { zone_id: OmicronZoneUuid, zpool: ZpoolName },
    #[error("ran out of underlay IP addresses")]
    OutOfUnderlayIps,
}

#[derive(Debug)]
pub(crate) struct SledEditor(InnerSledEditor);

#[derive(Debug)]
enum InnerSledEditor {
    // Internally, `SledEditor` has a variant for each variant of `SledState`,
    // as the operations allowed in different states are substantially different
    // (i.e., an active sled allows any edit; a decommissioned sled allows
    // none).
    Active(ActiveSledEditor),
    Decommissioned(EditedSled),
}

impl SledEditor {
    pub fn for_existing_active(
        subnet: Ipv6Subnet<SLED_PREFIX>,
        zones: BlueprintZonesConfig,
        disks: BlueprintPhysicalDisksConfig,
        datasets: BlueprintDatasetsConfig,
    ) -> Result<Self, SledInputError> {
        let inner = ActiveSledEditor::new(subnet, zones, disks, datasets)?;
        Ok(Self(InnerSledEditor::Active(inner)))
    }

    pub fn for_existing_decommissioned(
        zones: BlueprintZonesConfig,
        disks: BlueprintPhysicalDisksConfig,
        datasets: BlueprintDatasetsConfig,
    ) -> Result<Self, SledInputError> {
        let inner = EditedSled {
            state: SledState::Decommissioned,
            zones,
            disks,
            datasets,
            edit_counts: SledEditCounts::zeroes(),
        };
        Ok(Self(InnerSledEditor::Decommissioned(inner)))
    }

    pub fn for_new_active(subnet: Ipv6Subnet<SLED_PREFIX>) -> Self {
        Self(InnerSledEditor::Active(ActiveSledEditor::new_empty(subnet)))
    }

    pub fn finalize(self) -> EditedSled {
        match self.0 {
            InnerSledEditor::Active(editor) => editor.finalize(),
            InnerSledEditor::Decommissioned(edited) => edited,
        }
    }

    pub fn state(&self) -> SledState {
        match &self.0 {
            InnerSledEditor::Active(_) => SledState::Active,
            InnerSledEditor::Decommissioned(edited_sled) => edited_sled.state,
        }
    }

    pub fn edit_counts(&self) -> SledEditCounts {
        match &self.0 {
            InnerSledEditor::Active(editor) => editor.edit_counts(),
            InnerSledEditor::Decommissioned(edited) => edited.edit_counts,
        }
    }

    pub fn decommission(&mut self) -> Result<(), SledEditError> {
        match &mut self.0 {
            InnerSledEditor::Active(editor) => {
                // Decommissioning a sled is a one-way trip that has many
                // preconditions. We can't check all of them here (e.g., we
                // should kick the sled out of trust quorum before
                // decommissioning, which is entirely outside the realm of
                // `SledEditor`. But we can do some basic checks: all of the
                // disks, datasets, and zones for this sled should be expunged.
                editor.validate_decommisionable()?;

                // We can't take ownership of `editor` from the `&mut self`
                // reference we have, and we need ownership to call
                // `finalize()`. Steal the contents via `mem::swap()` with an
                // empty editor. This isn't panic safe (i.e., if we panic
                // between the `mem::swap()` and the reassignment to `self.0`
                // below, we'll be left in the active state with an empty sled
                // editor), but omicron in general is not panic safe and aborts
                // on panic. Plus `finalize()` should never panic.
                let mut stolen = ActiveSledEditor::new_empty(Ipv6Subnet::new(
                    Ipv6Addr::LOCALHOST,
                ));
                mem::swap(editor, &mut stolen);

                let mut finalized = stolen.finalize();
                finalized.state = SledState::Decommissioned;
                self.0 = InnerSledEditor::Decommissioned(finalized);
            }
            // If we're already decommissioned, there's nothing to do.
            InnerSledEditor::Decommissioned(_) => (),
        }
        Ok(())
    }

    pub fn alloc_underlay_ip(&mut self) -> Result<Ipv6Addr, SledEditError> {
        self.as_active_mut()?
            .alloc_underlay_ip()
            .ok_or(SledEditError::OutOfUnderlayIps)
    }

    pub fn disks<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = &BlueprintPhysicalDiskConfig>
    where
        F: FnMut(BlueprintPhysicalDiskDisposition) -> bool,
    {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                Either::Left(editor.disks(filter))
            }
            InnerSledEditor::Decommissioned(edited) => Either::Right(
                edited
                    .disks
                    .disks
                    .iter()
                    .filter(move |disk| filter(disk.disposition)),
            ),
        }
    }

    pub fn datasets(
        &self,
        filter: BlueprintDatasetFilter,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig> {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                Either::Left(editor.datasets(filter))
            }
            InnerSledEditor::Decommissioned(edited) => Either::Right(
                edited
                    .datasets
                    .datasets
                    .iter()
                    .filter(move |disk| disk.disposition.matches(filter)),
            ),
        }
    }

    pub fn zones<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = &BlueprintZoneConfig>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                Either::Left(editor.zones(filter))
            }
            InnerSledEditor::Decommissioned(edited) => Either::Right(
                edited
                    .zones
                    .zones
                    .iter()
                    .filter(move |zone| filter(zone.disposition)),
            ),
        }
    }

    fn as_active_mut(
        &mut self,
    ) -> Result<&mut ActiveSledEditor, SledEditError> {
        match &mut self.0 {
            InnerSledEditor::Active(editor) => Ok(editor),
            InnerSledEditor::Decommissioned(_) => {
                Err(SledEditError::EditDecommissioned)
            }
        }
    }

    pub fn ensure_disk(
        &mut self,
        disk: BlueprintPhysicalDiskConfig,
        rng: &mut SledPlannerRng,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.ensure_disk(disk, rng)
    }

    pub fn expunge_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<DiskExpungeDetails, SledEditError> {
        self.as_active_mut()?.expunge_disk(disk_id)
    }

    pub fn decommission_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.decommission_disk(disk_id)?;
        Ok(())
    }

    pub fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
        rng: &mut SledPlannerRng,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.add_zone(zone, rng)
    }

    pub fn expunge_zone(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<bool, SledEditError> {
        self.as_active_mut()?.expunge_zone(zone_id)
    }

    pub fn mark_expunged_zone_ready_for_cleanup(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<bool, SledEditError> {
        self.as_active_mut()?.mark_expunged_zone_ready_for_cleanup(zone_id)
    }

    /// Backwards compatibility / test helper: If we're given a blueprint that
    /// has zones but wasn't created via `SledEditor`, it might not have
    /// datasets for all its zones. This method backfills them.
    pub fn ensure_datasets_for_running_zones(
        &mut self,
        rng: &mut SledPlannerRng,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.ensure_datasets_for_running_zones(rng)
    }

    // Apply fixes for #7229: If any zones have a missing or incorrect
    // `filesystem_pool` property, correct it based on the inventory pools and
    // datasets.
    pub fn backfill_zone_filesystem_pools(
        &mut self,
        inventory_zpools: &[Zpool],
        inventory_datasets: &[Dataset],
        log: &Logger,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.backfill_zone_filesystem_pools(
            inventory_zpools,
            inventory_datasets,
            log,
        );
        Ok(())
    }
}

#[derive(Debug)]
struct ActiveSledEditor {
    underlay_ip_allocator: SledUnderlayIpAllocator,
    zones: ZonesEditor,
    disks: DisksEditor,
    datasets: DatasetsEditor,
}

#[derive(Debug)]
pub(crate) struct EditedSled {
    pub state: SledState,
    pub zones: BlueprintZonesConfig,
    pub disks: BlueprintPhysicalDisksConfig,
    pub datasets: BlueprintDatasetsConfig,
    pub edit_counts: SledEditCounts,
}

impl ActiveSledEditor {
    pub fn new(
        subnet: Ipv6Subnet<SLED_PREFIX>,
        zones: BlueprintZonesConfig,
        disks: BlueprintPhysicalDisksConfig,
        datasets: BlueprintDatasetsConfig,
    ) -> Result<Self, SledInputError> {
        let zones = ZonesEditor::from(zones);

        // We never reuse underlay IPs within a sled, regardless of zone
        // dispositions. If a zone has been fully removed from the blueprint
        // some time after expungement, we may reuse its IP; reconfigurator must
        // know that's safe prior to pruning the expunged zone.
        let zone_ips =
            zones.zones(BlueprintZoneDisposition::any).map(|z| z.underlay_ip());

        Ok(Self {
            underlay_ip_allocator: SledUnderlayIpAllocator::new(
                subnet, zone_ips,
            ),
            zones,
            disks: disks.try_into()?,
            datasets: DatasetsEditor::new(datasets)?,
        })
    }

    pub fn new_empty(subnet: Ipv6Subnet<SLED_PREFIX>) -> Self {
        // Creating the underlay IP allocator can only fail if we have a zone
        // with an IP outside the sled subnet, but we don't have any zones at
        // all, so this can't fail. Match explicitly to guard against this error
        // turning into an enum and getting new variants we'd need to check.
        let underlay_ip_allocator =
            SledUnderlayIpAllocator::new(subnet, iter::empty());

        Self {
            underlay_ip_allocator,
            zones: ZonesEditor::empty(),
            disks: DisksEditor::empty(),
            datasets: DatasetsEditor::empty(),
        }
    }

    pub fn finalize(self) -> EditedSled {
        let (disks, disks_counts) = self.disks.finalize();
        let (datasets, datasets_counts) = self.datasets.finalize();
        let (zones, zones_counts) = self.zones.finalize();
        EditedSled {
            state: SledState::Active,
            zones,
            disks,
            datasets,
            edit_counts: SledEditCounts {
                disks: disks_counts,
                datasets: datasets_counts,
                zones: zones_counts,
            },
        }
    }

    fn validate_decommisionable(&self) -> Result<(), SledEditError> {
        // A sled is only decommissionable if all its zones have been expunged
        // (i.e., there are no zones left with an in-service disposition).
        if let Some(zone) =
            self.zones(BlueprintZoneDisposition::is_in_service).next()
        {
            return Err(SledEditError::NonDecommissionableZoneNotExpunged {
                zone_id: zone.id,
                kind: zone.zone_type.kind(),
            });
        }

        Ok(())
    }

    pub fn edit_counts(&self) -> SledEditCounts {
        SledEditCounts {
            disks: self.disks.edit_counts(),
            datasets: self.datasets.edit_counts(),
            zones: self.zones.edit_counts(),
        }
    }

    pub fn alloc_underlay_ip(&mut self) -> Option<Ipv6Addr> {
        self.underlay_ip_allocator.alloc()
    }

    pub fn disks<F>(
        &self,
        filter: F,
    ) -> impl Iterator<Item = &BlueprintPhysicalDiskConfig>
    where
        F: FnMut(BlueprintPhysicalDiskDisposition) -> bool,
    {
        self.disks.disks(filter)
    }

    pub fn datasets(
        &self,
        filter: BlueprintDatasetFilter,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig> {
        self.datasets.datasets(filter)
    }

    pub fn zones<F>(
        &self,
        filter: F,
    ) -> impl Iterator<Item = &BlueprintZoneConfig>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        self.zones.zones(filter)
    }

    pub fn ensure_disk(
        &mut self,
        disk: BlueprintPhysicalDiskConfig,
        rng: &mut SledPlannerRng,
    ) -> Result<(), SledEditError> {
        let zpool = ZpoolName::new_external(disk.pool_id);

        self.disks.ensure(disk)?;

        // Every disk also gets a Debug and Transient Zone Root dataset; ensure
        // both of those exist as well.
        let debug = PartialDatasetConfig::for_debug(zpool.clone());
        let zone_root = PartialDatasetConfig::for_transient_zone_root(zpool);

        self.datasets.ensure_in_service(debug, rng);
        self.datasets.ensure_in_service(zone_root, rng);

        Ok(())
    }

    pub fn expunge_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<DiskExpungeDetails, SledEditError> {
        let (did_expunge_disk, zpool_id) = self.disks.expunge(disk_id)?;

        // When we expunge a disk, we must also expunge any datasets on it, and
        // any zones that relied on those datasets.
        let num_datasets_expunged =
            self.datasets.expunge_all_on_zpool(&zpool_id);
        let num_zones_expunged = self.zones.expunge_all_on_zpool(&zpool_id);

        if !did_expunge_disk {
            // If we didn't expunge the disk, it was already expunged, so there
            // shouldn't have been any datasets or zones to expunge.
            debug_assert_eq!(num_datasets_expunged, 0);
            debug_assert_eq!(num_zones_expunged, 0);
        }

        Ok(DiskExpungeDetails {
            disk_id: *disk_id,
            did_expunge_disk,
            num_datasets_expunged,
            num_zones_expunged,
        })
    }

    pub fn decommission_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<(), SledEditError> {
        // TODO: report decommissioning
        let _ = self.disks.decommission(disk_id)?;
        Ok(())
    }

    pub fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
        rng: &mut SledPlannerRng,
    ) -> Result<(), SledEditError> {
        // Ensure we can construct the configs for the datasets for this zone.
        let datasets = ZoneDatasetConfigs::new(&self.disks, &zone)?;

        // This zone's underlay IP should have come from us (via
        // `alloc_underlay_ip()`), but in case it wasn't, ensure we don't hand
        // out this IP again later.
        self.underlay_ip_allocator.mark_as_allocated(zone.underlay_ip());

        // Actually add the zone and its datasets.
        self.zones.add_zone(zone)?;
        datasets.ensure_in_service(&mut self.datasets, rng);

        Ok(())
    }

    pub fn expunge_zone(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<bool, SledEditError> {
        let (did_expunge, config) = self.zones.expunge(zone_id)?;

        // If we didn't actually expunge the zone in this edit, we don't
        // move on and expunge its datasets. This is to guard against
        // accidentally exposing a different zone's datasets (if that zone has
        // happens to have the same dataset kind as us and is running on the
        // same zpool as us, which is only possible if we were previously
        // expunged).
        //
        // This wouldn't be necessary if `config` tracked its dataset IDs
        // explicitly instead of only recording its zpool; once we fix that we
        // should be able to remove this check.
        if !did_expunge {
            return Ok(did_expunge);
        }

        if let Some(dataset) = config.filesystem_dataset() {
            self.datasets.expunge(&dataset.pool().id(), dataset.kind())?;
        }
        if let Some(dataset) = config.zone_type.durable_dataset() {
            self.datasets
                .expunge(&dataset.dataset.pool_name.id(), &dataset.kind)?;
        }

        Ok(did_expunge)
    }

    pub fn mark_expunged_zone_ready_for_cleanup(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<bool, SledEditError> {
        let did_mark_ready =
            self.zones.mark_expunged_zone_ready_for_cleanup(zone_id)?;
        Ok(did_mark_ready)
    }

    /// Backwards compatibility / test helper: If we're given a blueprint that
    /// has zones but wasn't created via `SledEditor`, it might not have
    /// datasets for all its zones. This method backfills them.
    pub fn ensure_datasets_for_running_zones(
        &mut self,
        rng: &mut SledPlannerRng,
    ) -> Result<(), SledEditError> {
        for zone in self.zones.zones(BlueprintZoneDisposition::is_in_service) {
            ZoneDatasetConfigs::new(&self.disks, zone)?
                .ensure_in_service(&mut self.datasets, rng);
        }
        Ok(())
    }

    pub fn backfill_zone_filesystem_pools(
        &mut self,
        inventory_zpools: &[Zpool],
        inventory_datasets: &[Dataset],
        log: &Logger,
    ) {
        let mut zones_to_edit: BTreeMap<OmicronZoneUuid, ZpoolName> =
            BTreeMap::new();

        for zone in self.zones.zones(BlueprintZoneDisposition::is_in_service) {
            let expected_filesystem_pool = if let Some(pool) =
                zone.zone_type.durable_zpool()
            {
                // Easy case: if this zone type has a durable dataset, its
                // filesystem_pool must be on the same zpool.
                Cow::Borrowed(pool)
            } else {
                // Hard case: this zone type has no durable dataset, so if
                // its `filesystem_pool` is `None` in sled-agent's ledger,
                // sled-agent chooses a random zpool each time it launches
                // the zone. Look at the provided inventory collection and
                // attempt to find that zpool. This could be fail in two
                // ways:
                //
                // 1. We might not have an inventory collection for this
                //    sled (in which case, we just skip this zone; we'll
                //    have to backfill it during some future planning run
                //    where we do have one)
                // 2. sled-agent might have restarted the zone since the
                //    inventory collection was taken and chosen a
                //    _different_ zpool. We have no way of detecting this
                //    now, but must be willing to overwrite a non-`None`
                //    `filesystem_pool` value to correct ourselves if we've
                //    hit this case (without knowing it!) in a past planner
                //    run.
                //
                // We have a list of dataset names from inventory; we could
                // try to parse those back into `DatasetName`s, but it's
                // more straightforward to construct what this zone's
                // `DatasetName` _would_ be (for any given zpool on the
                // sled) and see if it's present in the inventory list. This
                // is certainly less efficient than parsing the dataset name
                // string, but we only have 10 zpools per sled, so shouldn't
                // be too bad in practice.
                let mut found_zpool = None;
                let kind = DatasetKind::TransientZone {
                    name: illumos_utils::zone::zone_name(
                        zone.zone_type.kind().zone_prefix(),
                        Some(zone.id),
                    ),
                };

                for inv_zpool in inventory_zpools {
                    let zpool = ZpoolName::new_external(inv_zpool.id);
                    let dataset_name = DatasetName::new(zpool, kind.clone());
                    let dataset_name_string = dataset_name.full_name();
                    if inventory_datasets
                        .iter()
                        .any(|d| d.name == dataset_name_string)
                    {
                        found_zpool = Some(dataset_name.pool().clone());
                        break;
                    }
                }

                match found_zpool {
                    Some(zpool) => Cow::Owned(zpool),
                    None => {
                        warn!(
                            log,
                            "could not determine expected \
                             `filesystem_pool` for zone";
                            "zone_id" => %zone.id,
                            "zone_kind" => ?zone.zone_type.kind(),
                            "current_filesystem_pool" => ?zone.filesystem_pool,
                        );
                        continue;
                    }
                }
            };

            // If the pool is already correct, we have nothing to do.
            if zone.filesystem_pool.as_ref() == Some(&*expected_filesystem_pool)
            {
                continue;
            }

            info!(
                log,
                "updating filesystem_pool for zone";
                "zone_id" => %zone.id,
                "zone_kind" => ?zone.zone_type.kind(),
                "current_filesystem_pool" => ?zone.filesystem_pool,
                "new_filesystem_zpool" => %expected_filesystem_pool,
            );

            // If we're _correcting_ a filesystem_pool rather than just
            // filling it in, we also need to expunge the dataset from the
            // incorrect value.
            if let Some(old_filesystem) = zone.filesystem_dataset() {
                let (pool, kind) = old_filesystem.into_parts();
                match self.datasets.expunge(&pool.id(), &kind) {
                    Ok(()) => (),
                    // We're trying to get rid of a potentially-orphaned
                    // dataset; it not existing is okay but unexpected! Log
                    // a warning but don't fail.
                    Err(
                        err @ DatasetsEditError::ExpungeNonexistentDataset {
                            ..
                        },
                    ) => {
                        warn!(
                            log,
                            "unexpected failure trying to expunge dataset";
                            InlineErrorChain::new(&err),
                        );
                    }
                }
            }

            zones_to_edit
                .insert(zone.id, expected_filesystem_pool.into_owned());
        }

        for (zone_id, new_filesystem_zpool) in zones_to_edit {
            self.zones.backfill_filesystem_pool(zone_id, new_filesystem_zpool);
        }
    }
}

#[derive(Debug)]
struct ZoneDatasetConfigs {
    filesystem: Option<PartialDatasetConfig>,
    durable: Option<PartialDatasetConfig>,
}

impl ZoneDatasetConfigs {
    fn new(
        disks: &DisksEditor,
        zone: &BlueprintZoneConfig,
    ) -> Result<Self, SledEditError> {
        let filesystem_dataset = zone
            .filesystem_dataset()
            .map(|dataset| PartialDatasetConfig::for_transient_zone(dataset));
        let durable_dataset = zone.zone_type.durable_dataset().map(|dataset| {
            // `dataset` records include an optional socket address, which is
            // only applicable for durable datasets backing crucible. This this
            // is a little fishy and might go away with
            // https://github.com/oxidecomputer/omicron/issues/6998.
            let address = match &zone.zone_type {
                BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible { address, .. },
                ) => Some(*address),
                _ => None,
            };
            PartialDatasetConfig::for_durable_zone(
                dataset.dataset.pool_name.clone(),
                dataset.kind,
                address,
            )
        });

        // Ensure that if this zone has both kinds of datasets, they reside on
        // the same zpool.
        if let (Some(fs), Some(dur)) = (&filesystem_dataset, &durable_dataset) {
            if fs.zpool() != dur.zpool() {
                return Err(SledEditError::ZoneInvalidZpoolCombination {
                    zone_id: zone.id,
                    fs_zpool: fs.zpool().clone(),
                    dur_zpool: dur.zpool().clone(),
                });
            }
        }

        // Ensure that if we have a zpool, we have a matching disk (i.e., a zone
        // can't be added if it has a dataset on a zpool that we don't have)
        if let Some(dataset) =
            filesystem_dataset.as_ref().or(durable_dataset.as_ref())
        {
            if !disks.contains_zpool(&dataset.zpool().id()) {
                return Err(SledEditError::ZoneOnNonexistentZpool {
                    zone_id: zone.id,
                    zpool: dataset.zpool().clone(),
                });
            }
        }

        Ok(Self { filesystem: filesystem_dataset, durable: durable_dataset })
    }

    fn ensure_in_service(
        self,
        datasets: &mut DatasetsEditor,
        rng: &mut SledPlannerRng,
    ) {
        if let Some(dataset) = self.filesystem {
            datasets.ensure_in_service(dataset, rng);
        }
        if let Some(dataset) = self.durable {
            datasets.ensure_in_service(dataset, rng);
        }
    }
}
