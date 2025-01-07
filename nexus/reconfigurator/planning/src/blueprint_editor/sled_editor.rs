// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for editing the blueprint details of a single sled.

use crate::blueprint_builder::SledEditCounts;
use crate::planner::PlannerRng;
use illumos_utils::zpool::ZpoolName;
use itertools::Either;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::DiskFilter;
use nexus_types::external_api::views::SledState;
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::mem;

mod datasets;
mod disks;
mod zones;

pub(crate) use self::datasets::DatasetIdsBackfillFromDb;

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
    pub fn for_existing(
        state: SledState,
        zones: BlueprintZonesConfig,
        disks: BlueprintPhysicalDisksConfig,
        datasets: BlueprintDatasetsConfig,
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Result<Self, SledInputError> {
        match state {
            SledState::Active => {
                let inner = ActiveSledEditor::new(
                    zones,
                    disks,
                    datasets,
                    preexisting_dataset_ids,
                )?;
                Ok(Self(InnerSledEditor::Active(inner)))
            }
            SledState::Decommissioned => {
                let inner = EditedSled {
                    state,
                    zones,
                    disks,
                    datasets,
                    edit_counts: SledEditCounts::zeroes(),
                };
                Ok(Self(InnerSledEditor::Decommissioned(inner)))
            }
        }
    }

    pub fn for_new_active(
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Self {
        Self(InnerSledEditor::Active(ActiveSledEditor::new_empty(
            preexisting_dataset_ids,
        )))
    }

    pub fn finalize(self) -> EditedSled {
        match self.0 {
            InnerSledEditor::Active(editor) => editor.finalize(),
            InnerSledEditor::Decommissioned(edited) => edited,
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
                let mut stolen = ActiveSledEditor::new_empty(
                    DatasetIdsBackfillFromDb::empty(),
                );
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

    pub fn disks(
        &self,
        filter: DiskFilter,
    ) -> impl Iterator<Item = &BlueprintPhysicalDiskConfig> {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                Either::Left(editor.disks(filter))
            }
            InnerSledEditor::Decommissioned(edited) => Either::Right(
                edited
                    .disks
                    .disks
                    .iter()
                    .filter(move |disk| disk.disposition.matches(filter)),
            ),
        }
    }

    pub fn zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        match &self.0 {
            InnerSledEditor::Active(editor) => {
                Either::Left(editor.zones(filter))
            }
            InnerSledEditor::Decommissioned(edited) => Either::Right(
                edited
                    .zones
                    .zones
                    .iter()
                    .filter(move |zone| zone.disposition.matches(filter)),
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
        rng: &mut PlannerRng,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.ensure_disk(disk, rng);
        Ok(())
    }

    pub fn expunge_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.expunge_disk(disk_id)
    }

    pub fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
        rng: &mut PlannerRng,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.add_zone(zone, rng)
    }

    pub fn expunge_zone(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.expunge_zone(zone_id)
    }

    /// Backwards compatibility / test helper: If we're given a blueprint that
    /// has zones but wasn't created via `SledEditor`, it might not have
    /// datasets for all its zones. This method backfills them.
    pub fn ensure_datasets_for_running_zones(
        &mut self,
        rng: &mut PlannerRng,
    ) -> Result<(), SledEditError> {
        self.as_active_mut()?.ensure_datasets_for_running_zones(rng)
    }
}

#[derive(Debug)]
struct ActiveSledEditor {
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
        zones: BlueprintZonesConfig,
        disks: BlueprintPhysicalDisksConfig,
        datasets: BlueprintDatasetsConfig,
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Result<Self, SledInputError> {
        Ok(Self {
            zones: zones.try_into()?,
            disks: disks.try_into()?,
            datasets: DatasetsEditor::new(datasets, preexisting_dataset_ids)?,
        })
    }

    pub fn new_empty(
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Self {
        Self {
            zones: ZonesEditor::empty(),
            disks: DisksEditor::empty(),
            datasets: DatasetsEditor::empty(preexisting_dataset_ids),
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
        // ... and all zones are expunged.
        if let Some(zone) = self.zones(BlueprintZoneFilter::All).find(|zone| {
            match zone.disposition {
                BlueprintZoneDisposition::InService
                | BlueprintZoneDisposition::Quiesced => true,
                BlueprintZoneDisposition::Expunged => false,
            }
        }) {
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

    pub fn disks(
        &self,
        filter: DiskFilter,
    ) -> impl Iterator<Item = &BlueprintPhysicalDiskConfig> {
        self.disks.disks(filter)
    }

    pub fn zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        self.zones.zones(filter)
    }

    pub fn ensure_disk(
        &mut self,
        disk: BlueprintPhysicalDiskConfig,
        rng: &mut PlannerRng,
    ) {
        let zpool = ZpoolName::new_external(disk.pool_id);

        self.disks.ensure(disk);

        // Every disk also gets a Debug and Transient Zone Root dataset; ensure
        // both of those exist as well.
        let debug = PartialDatasetConfig::for_debug(zpool.clone());
        let zone_root = PartialDatasetConfig::for_transient_zone_root(zpool);

        self.datasets.ensure_in_service(debug, rng);
        self.datasets.ensure_in_service(zone_root, rng);
    }

    pub fn expunge_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<(), SledEditError> {
        let zpool_id = self.disks.expunge(disk_id)?;

        // When we expunge a disk, we must also expunge any datasets on it, and
        // any zones that relied on those datasets.
        self.datasets.expunge_all_on_zpool(&zpool_id);
        self.zones.expunge_all_on_zpool(&zpool_id);

        Ok(())
    }

    pub fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
        rng: &mut PlannerRng,
    ) -> Result<(), SledEditError> {
        // Ensure we can construct the configs for the datasets for this zone.
        let datasets = ZoneDatasetConfigs::new(&self.disks, &zone)?;

        // Actually add the zone and its datasets.
        self.zones.add_zone(zone)?;
        datasets.ensure_in_service(&mut self.datasets, rng);

        Ok(())
    }

    pub fn expunge_zone(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<(), SledEditError> {
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
            return Ok(());
        }

        if let Some(dataset) = config.filesystem_dataset() {
            self.datasets.expunge(&dataset.pool().id(), dataset.kind())?;
        }
        if let Some(dataset) = config.zone_type.durable_dataset() {
            self.datasets
                .expunge(&dataset.dataset.pool_name.id(), &dataset.kind)?;
        }

        Ok(())
    }

    /// Backwards compatibility / test helper: If we're given a blueprint that
    /// has zones but wasn't created via `SledEditor`, it might not have
    /// datasets for all its zones. This method backfills them.
    pub fn ensure_datasets_for_running_zones(
        &mut self,
        rng: &mut PlannerRng,
    ) -> Result<(), SledEditError> {
        for zone in self.zones.zones(BlueprintZoneFilter::ShouldBeRunning) {
            ZoneDatasetConfigs::new(&self.disks, zone)?
                .ensure_in_service(&mut self.datasets, rng);
        }
        Ok(())
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
        rng: &mut PlannerRng,
    ) {
        if let Some(dataset) = self.filesystem {
            datasets.ensure_in_service(dataset, rng);
        }
        if let Some(dataset) = self.durable {
            datasets.ensure_in_service(dataset, rng);
        }
    }
}
