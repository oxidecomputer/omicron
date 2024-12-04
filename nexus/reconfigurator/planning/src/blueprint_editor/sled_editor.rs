// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for editing the blueprint details of a single sled.

use crate::blueprint_builder::SledEditCounts;
use crate::planner::PlannerRng;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintDatasetConfig;
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
    ) -> Result<Self, SledInputError> {
        Ok(Self {
            state,
            zones: zones.try_into()?,
            disks: disks.try_into()?,
            datasets: datasets.try_into()?,
        })
    }

    pub fn new_empty(state: SledState) -> Self {
        Self {
            state,
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
        let debug = self.dataset_config(
            PartialDatasetConfig::for_debug(zpool.clone()),
            rng,
        );
        let zone_root = self.dataset_config(
            PartialDatasetConfig::for_transient_zone_root(zpool),
            rng,
        );

        self.datasets.ensure(debug);
        self.datasets.ensure(zone_root);
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

    // Helper to generate a full `BlueprintDatasetConfig` from a partial config;
    // we either look up the ID (if we're updating an existing dataset) or
    // generate a new one via our RNG.
    //
    // TODO-cleanup It seems awkward we don't know whether we're updating or
    // adding at this point. For zones, should we store the dataset ID
    // explicitly so we don't need to do this lookup for updates? Less sure what
    // we'd do with extra datasets like Debug and ZoneRoot.
    fn dataset_config(
        &mut self,
        config: PartialDatasetConfig,
        rng: &mut PlannerRng,
    ) -> BlueprintDatasetConfig {
        let id = self
            .datasets
            .get_id(&config.zpool().id(), config.kind())
            .unwrap_or_else(|| rng.next_dataset());
        config.build(id)
    }

    pub fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
        rng: &mut PlannerRng,
    ) -> Result<(), SledEditError> {
        // Ensure we can construct the configs for the datasets for this zone.
        let datasets = ZoneDatasetConfigs::new(self, &zone, rng)?;

        // Actually add the zone and its datasets.
        self.zones.add_zone(zone)?;
        datasets.ensure(&mut self.datasets);

        Ok(())
    }

    pub fn expunge_zone(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<(), SledEditError> {
        let config = self.zones.expunge(zone_id)?;

        // When expunging a zone, we also expunge its datasets.
        if let Some(dataset) = config.filesystem_dataset() {
            // It would be strange to not find the dataset ID here; it would
            // mean we have a zone (because we found `config`) that has a
            // filesystem dataset, but that dataset isn't present. It _could_
            // happen if the zone and its dataset had previously been expunged,
            // and the dataset has since been cleaned up and removed from the
            // blueprint entirely, but the (expunged) zone is still around.
            if let Some(dataset_id) =
                self.datasets.get_id(&dataset.pool().id(), dataset.dataset())
            {
                match self.datasets.expunge(&dataset_id) {
                    Ok(()) => (),
                    Err(DatasetsEditError::ExpungeNonexistentDataset {
                        ..
                    }) => unreachable!("we just looked up the dataset ID"),
                }
            }
        }
        if let Some(dataset) = config.zone_type.durable_dataset() {
            // See note above about failing to find the dataset ID.
            if let Some(dataset_id) = self
                .datasets
                .get_id(&dataset.dataset.pool_name.id(), &dataset.kind)
            {
                match self.datasets.expunge(&dataset_id) {
                    Ok(()) => (),
                    Err(DatasetsEditError::ExpungeNonexistentDataset {
                        ..
                    }) => unreachable!("we just looked up the dataset ID"),
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
        for zone in self.zones(BlueprintZoneFilter::ShouldBeRunning) {
            let datasets = ZoneDatasetConfigs::new(self, zone, rng)?;
            datasets.ensure(&mut self.datasets);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ZoneDatasetConfigs {
    filesystem: Option<BlueprintDatasetConfig>,
    durable: Option<BlueprintDatasetConfig>,
}

impl ZoneDatasetConfigs {
    fn new(
        editor: &mut SledEditor,
        zone: &BlueprintZoneConfig,
        rng: &mut PlannerRng,
    ) -> Result<Self, SledEditError> {
        let filesystem_dataset = zone.filesystem_dataset().map(|dataset| {
            editor.dataset_config(
                PartialDatasetConfig::for_transient_zone(dataset),
                rng,
            )
        });
        let durable_dataset = zone.zone_type.durable_dataset().map(|dataset| {
            let address = match &zone.zone_type {
                BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible { address, .. },
                ) => Some(*address),
                _ => None,
            };
            editor.dataset_config(
                PartialDatasetConfig::for_durable_zone(
                    dataset.dataset.pool_name.clone(),
                    dataset.kind,
                    address,
                ),
                rng,
            )
        });

        // Ensure that if this zone has both kinds of datasets, they reside on
        // the same zpool.
        if let (Some(fs), Some(dur)) = (&filesystem_dataset, &durable_dataset) {
            if fs.pool != dur.pool {
                return Err(SledEditError::ZoneInvalidZpoolCombination {
                    zone_id: zone.id,
                    fs_zpool: fs.pool.clone(),
                    dur_zpool: dur.pool.clone(),
                });
            }
        }

        // Ensure that if we have a zpool, we have a matching disk (i.e., a zone
        // can't be added if it has a dataset on a zpool that we don't have)
        if let Some(dataset) =
            filesystem_dataset.as_ref().or(durable_dataset.as_ref())
        {
            if !editor.disks.contains_zpool(&dataset.pool.id()) {
                return Err(SledEditError::ZoneOnNonexistentZpool {
                    zone_id: zone.id,
                    zpool: dataset.pool.clone(),
                });
            }
        }

        Ok(Self {
            filesystem: filesystem_dataset,
            durable: durable_dataset,
        })
    }

    fn ensure(self, datasets: &mut DatasetsEditor) {
        if let Some(dataset) = self.filesystem {
            datasets.ensure(dataset);
        }
        if let Some(dataset) = self.durable {
            datasets.ensure(dataset);
        }
    }
}
