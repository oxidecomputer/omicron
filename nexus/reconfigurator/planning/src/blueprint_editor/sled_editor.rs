// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for editing the blueprint details of a single sled.

use crate::blueprint_builder::SledEditCounts;
use crate::planner::PlannerRng;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::DiskFilter;
use nexus_types::external_api::views::SledState;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;

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
pub(crate) struct SledEditor {
    state: SledState,
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

impl SledEditor {
    pub fn new(
        state: SledState,
        zones: BlueprintZonesConfig,
        disks: BlueprintPhysicalDisksConfig,
        datasets: BlueprintDatasetsConfig,
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Result<Self, SledInputError> {
        Ok(Self {
            state,
            zones: zones.try_into()?,
            disks: disks.try_into()?,
            datasets: DatasetsEditor::new(datasets, preexisting_dataset_ids)?,
        })
    }

    pub fn new_empty(
        state: SledState,
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Self {
        Self {
            state,
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
            state: self.state,
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

    pub fn edit_counts(&self) -> SledEditCounts {
        SledEditCounts {
            disks: self.disks.edit_counts(),
            datasets: self.datasets.edit_counts(),
            zones: self.zones.edit_counts(),
        }
    }

    pub fn set_state(&mut self, new_state: SledState) {
        self.state = new_state;
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
        let config = self.zones.expunge(zone_id)?;

        // When expunging a zone, we also expunge its datasets.
        if let Some(dataset) = config.filesystem_dataset() {
            match self.datasets.expunge(&dataset.pool().id(), dataset.dataset())
            {
                Ok(()) => (),
                Err(DatasetsEditError::ExpungeNonexistentDataset {
                    ..
                }) => {
                    // It would be strange to not find the dataset here; it
                    // would mean we have a zone (because we found `config`)
                    // that has a filesystem dataset, but that dataset isn't
                    // present. It _could_ happen if the zone and its dataset
                    // had previously been expunged, and the dataset has since
                    // been cleaned up and removed from the blueprint entirely,
                    // but the (expunged) zone is still around, so we ignore
                    // this error.
                }
            }
        }
        if let Some(dataset) = config.zone_type.durable_dataset() {
            match self
                .datasets
                .expunge(&dataset.dataset.pool_name.id(), &dataset.kind)
            {
                Ok(()) => (),
                Err(DatasetsEditError::ExpungeNonexistentDataset {
                    ..
                }) => {
                    // See note above about ignoring this error.
                }
            }
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
