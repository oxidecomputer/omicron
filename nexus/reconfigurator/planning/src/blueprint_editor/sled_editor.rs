// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for editing the blueprint details of a single sled.

use crate::blueprint_builder::SledEditCounts;
use crate::planner::SledPlannerRng;
use illumos_utils::zpool::ZpoolName;
use itertools::Either;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintSledConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::external_api::views::SledState;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use scalar::ScalarEditor;
use std::iter;
use std::mem;
use std::net::Ipv6Addr;
use underlay_ip_allocator::SledUnderlayIpAllocator;

mod datasets;
mod disks;
mod scalar;
mod underlay_ip_allocator;
mod zones;

pub use self::datasets::DatasetsEditError;
pub use self::datasets::MultipleDatasetsOfKind;
pub use self::disks::DisksEditError;
pub use self::zones::ZonesEditError;

use self::datasets::DatasetsEditor;
use self::datasets::PartialDatasetConfig;
use self::disks::DisksEditor;
use self::zones::ZonesEditor;

#[derive(Debug, thiserror::Error)]
pub enum SledInputError {
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
        config: BlueprintSledConfig,
    ) -> Result<Self, SledInputError> {
        assert_eq!(
            config.state,
            SledState::Active,
            "for_existing_active called on non-active sled"
        );
        let inner = ActiveSledEditor::new(subnet, config)?;
        Ok(Self(InnerSledEditor::Active(inner)))
    }

    pub fn for_existing_decommissioned(
        config: BlueprintSledConfig,
    ) -> Result<Self, SledInputError> {
        assert_eq!(
            config.state,
            SledState::Decommissioned,
            "for_existing_decommissioned called on non-decommissioned sled"
        );
        let inner =
            EditedSled { config, edit_counts: SledEditCounts::zeroes() };
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
            InnerSledEditor::Decommissioned(_) => SledState::Decommissioned,
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
                finalized.config.state = SledState::Decommissioned;
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
                    .config
                    .disks
                    .iter()
                    .filter(move |disk| filter(disk.disposition)),
            ),
        }
    }

    pub fn datasets<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig>
    where
        F: FnMut(BlueprintDatasetDisposition) -> bool,
    {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                Either::Left(editor.datasets(filter))
            }
            InnerSledEditor::Decommissioned(edited) => Either::Right(
                edited
                    .config
                    .datasets
                    .iter()
                    .filter(move |disk| filter(disk.disposition)),
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
                    .config
                    .zones
                    .iter()
                    .filter(move |zone| filter(zone.disposition)),
            ),
        }
    }

    /// Returns the remove_mupdate_override field for this sled.
    pub fn get_remove_mupdate_override(&self) -> Option<MupdateOverrideUuid> {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                *editor.remove_mupdate_override.value()
            }
            InnerSledEditor::Decommissioned(sled) => {
                sled.config.remove_mupdate_override
            }
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

    /// Sets the image source for a zone.
    pub fn set_zone_image_source(
        &mut self,
        zone_id: &OmicronZoneUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<BlueprintZoneImageSource, SledEditError> {
        self.as_active_mut()?.set_zone_image_source(zone_id, image_source)
    }

    /// Sets remove-mupdate-override configuration for this sled.
    ///
    /// Currently only used in test code.
    pub fn set_remove_mupdate_override(
        &mut self,
        remove_mupdate_override: Option<MupdateOverrideUuid>,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?
            .set_remove_mupdate_override(remove_mupdate_override);
        Ok(())
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

    /// Debug method to force a sled agent generation number to be bumped, even
    /// if there are no changes to the sled.
    ///
    /// Do not use in production. Instead, update the logic that decides if the
    /// generation number should be bumped.
    pub fn debug_force_generation_bump(&mut self) -> Result<(), SledEditError> {
        self.as_active_mut()?.debug_force_generation_bump();
        Ok(())
    }
}

#[derive(Debug)]
struct ActiveSledEditor {
    underlay_ip_allocator: SledUnderlayIpAllocator,
    incoming_sled_agent_generation: Generation,
    zones: ZonesEditor,
    disks: DisksEditor,
    datasets: DatasetsEditor,
    remove_mupdate_override: ScalarEditor<Option<MupdateOverrideUuid>>,
    debug_force_generation_bump: bool,
}

#[derive(Debug)]
pub(crate) struct EditedSled {
    pub config: BlueprintSledConfig,
    pub edit_counts: SledEditCounts,
}

impl ActiveSledEditor {
    pub fn new(
        subnet: Ipv6Subnet<SLED_PREFIX>,
        config: BlueprintSledConfig,
    ) -> Result<Self, SledInputError> {
        let zones =
            ZonesEditor::new(config.sled_agent_generation, config.zones);

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
            incoming_sled_agent_generation: config.sled_agent_generation,
            zones,
            disks: DisksEditor::new(config.sled_agent_generation, config.disks),
            datasets: DatasetsEditor::new(config.datasets)?,
            remove_mupdate_override: ScalarEditor::new(
                config.remove_mupdate_override,
            ),
            debug_force_generation_bump: false,
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
            incoming_sled_agent_generation: Generation::new(),
            zones: ZonesEditor::empty(),
            disks: DisksEditor::empty(),
            datasets: DatasetsEditor::empty(),
            remove_mupdate_override: ScalarEditor::new(None),
            debug_force_generation_bump: false,
        }
    }

    pub fn finalize(self) -> EditedSled {
        let (disks, disks_counts) = self.disks.finalize();
        let (datasets, datasets_counts) = self.datasets.finalize();
        let (zones, zones_counts) = self.zones.finalize();
        let remove_mupdate_override_is_modified =
            self.remove_mupdate_override.is_modified();
        let mut sled_agent_generation = self.incoming_sled_agent_generation;

        // Bump the generation if we made any changes of concern to sled-agent.
        if self.debug_force_generation_bump
            || disks_counts.has_nonzero_counts()
            || datasets_counts.has_nonzero_counts()
            || zones_counts.has_nonzero_counts()
            || remove_mupdate_override_is_modified
        {
            sled_agent_generation = sled_agent_generation.next();
        }

        EditedSled {
            config: BlueprintSledConfig {
                state: SledState::Active,
                sled_agent_generation,
                disks,
                datasets,
                zones,
                remove_mupdate_override: self
                    .remove_mupdate_override
                    .finalize(),
            },
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

    pub fn datasets<F>(
        &self,
        filter: F,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig>
    where
        F: FnMut(BlueprintDatasetDisposition) -> bool,
    {
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
        let debug = PartialDatasetConfig::for_debug(zpool);
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

        {
            let dataset = config.filesystem_dataset();
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

    /// Set the image source for a zone, returning the old image source.
    pub fn set_zone_image_source(
        &mut self,
        zone_id: &OmicronZoneUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<BlueprintZoneImageSource, SledEditError> {
        Ok(self.zones.set_zone_image_source(zone_id, image_source)?)
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

    /// Set remove-mupdate-override configuration for this sled.
    pub fn set_remove_mupdate_override(
        &mut self,
        remove_mupdate_override: Option<MupdateOverrideUuid>,
    ) {
        self.remove_mupdate_override.set_value(remove_mupdate_override);
    }

    /// Debug method to force a sled agent generation number to be bumped, even
    /// if there are no changes to the sled.
    ///
    /// Do not use in production. Instead, update the logic that decides if the
    /// generation number should be bumped.
    pub fn debug_force_generation_bump(&mut self) {
        self.debug_force_generation_bump = true;
    }
}

#[derive(Debug)]
struct ZoneDatasetConfigs {
    filesystem: PartialDatasetConfig,
    durable: Option<PartialDatasetConfig>,
}

impl ZoneDatasetConfigs {
    fn new(
        disks: &DisksEditor,
        zone: &BlueprintZoneConfig,
    ) -> Result<Self, SledEditError> {
        let filesystem_dataset =
            PartialDatasetConfig::for_transient_zone(zone.filesystem_dataset());
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
                dataset.dataset.pool_name,
                dataset.kind,
                address,
            )
        });

        // Ensure that if this zone has both kinds of datasets, they reside on
        // the same zpool.
        if let Some(dur) = &durable_dataset {
            if filesystem_dataset.zpool() != dur.zpool() {
                return Err(SledEditError::ZoneInvalidZpoolCombination {
                    zone_id: zone.id,
                    fs_zpool: *filesystem_dataset.zpool(),
                    dur_zpool: *dur.zpool(),
                });
            }
        }

        // Ensure that we have a matching disk (i.e., a zone can't be added if
        // it has a dataset on a zpool that we don't have)
        if !disks.contains_zpool(&filesystem_dataset.zpool().id()) {
            return Err(SledEditError::ZoneOnNonexistentZpool {
                zone_id: zone.id,
                zpool: *filesystem_dataset.zpool(),
            });
        }

        Ok(Self { filesystem: filesystem_dataset, durable: durable_dataset })
    }

    fn ensure_in_service(
        self,
        datasets: &mut DatasetsEditor,
        rng: &mut SledPlannerRng,
    ) {
        datasets.ensure_in_service(self.filesystem, rng);
        if let Some(dataset) = self.durable {
            datasets.ensure_in_service(dataset, rng);
        }
    }
}
