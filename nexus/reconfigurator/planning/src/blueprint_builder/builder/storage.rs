// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for editing the storage (disks and datasets) of a Blueprint

use crate::planner::PlannerRng;

use super::datasets_editor::BlueprintDatasetsEditError;
use super::datasets_editor::BlueprintDatasetsEditor;
use super::datasets_editor::SledDatasetsEditor;
use super::disks_editor::BlueprintDisksEditor;
use super::disks_editor::SledDisksEditor;
use super::Ensure;
use super::EnsureMultiple;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::SledResources;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;

#[derive(Debug)]
pub(super) struct BlueprintStorageEditor {
    disks: BlueprintDisksEditor,
    datasets: BlueprintDatasetsEditor,
}

impl BlueprintStorageEditor {
    pub fn new(
        disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
        datasets: BTreeMap<SledUuid, BlueprintDatasetsConfig>,
    ) -> Self {
        Self {
            disks: BlueprintDisksEditor::new(disks),
            datasets: BlueprintDatasetsEditor::new(datasets),
        }
    }

    pub fn sled_storage_editor<'a>(
        &'a mut self,
        sled_id: SledUuid,
        sled_resources: &SledResources,
        rng: &'a mut PlannerRng,
    ) -> Result<SledStorageEditor<'a>, BlueprintDatasetsEditError> {
        let disks = self.disks.sled_disks_editor(sled_id);
        let datasets =
            self.datasets.sled_datasets_editor(sled_id, sled_resources, rng)?;
        Ok(SledStorageEditor { disks, datasets })
    }

    #[cfg(test)]
    pub fn current_sled_disks(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&BTreeMap<PhysicalDiskUuid, BlueprintPhysicalDiskConfig>> {
        self.disks.current_sled_disks(sled_id)
    }

    pub fn into_builders(
        self,
    ) -> (BlueprintDisksBuilder, BlueprintDatasetsBuilder) {
        (
            BlueprintDisksBuilder(self.disks),
            BlueprintDatasetsBuilder(self.datasets),
        )
    }
}

#[derive(Debug)]
pub(super) struct BlueprintDisksBuilder(BlueprintDisksEditor);

impl BlueprintDisksBuilder {
    pub fn build(
        self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintPhysicalDisksConfig> {
        self.0.build(sled_ids)
    }
}

#[derive(Debug)]
pub(super) struct BlueprintDatasetsBuilder(BlueprintDatasetsEditor);

impl BlueprintDatasetsBuilder {
    pub fn build(
        self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintDatasetsConfig> {
        self.0.build(sled_ids)
    }
}

#[derive(Debug)]
pub(super) struct SledStorageEditor<'a> {
    disks: SledDisksEditor<'a>,
    datasets: SledDatasetsEditor<'a>,
}

impl SledStorageEditor<'_> {
    pub fn disk_ids(&self) -> impl Iterator<Item = PhysicalDiskUuid> + '_ {
        self.disks.disk_ids()
    }

    pub fn ensure_disk(&mut self, disk: BlueprintPhysicalDiskConfig) -> Ensure {
        let zpool = ZpoolName::new_external(disk.pool_id);

        let result = self.disks.ensure_disk(disk);

        // We ignore the result of possibly adding or updating the disk's
        // dataset. Do we care to log if they've changed even if the disk
        // doesn't?
        self.datasets.ensure_debug_dataset(zpool.clone());
        self.datasets.ensure_zone_root_dataset(zpool);

        result
    }

    pub fn remove_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Option<ZpoolName> {
        let Some(disk) = self.disks.remove_disk(disk_id) else {
            return None;
        };
        self.datasets
            .expunge_datasets_if(|dataset| dataset.pool.id() == disk.pool_id);
        Some(ZpoolName::new_external(disk.pool_id))
    }

    pub fn ensure_zone_datasets(
        &mut self,
        zone: &BlueprintZoneConfig,
    ) -> EnsureMultiple {
        // TODO check that zpools are on valid disks?

        let mut added = 0;
        let mut updated = 0;

        // Dataset for transient zone filesystem
        if let Some(fs_zpool) = &zone.filesystem_pool {
            let name = zone_name(&zone);
            let address = None;
            let quota = None;
            let reservation = None;
            match self.datasets.ensure_dataset(
                DatasetName::new(
                    fs_zpool.clone(),
                    DatasetKind::TransientZone { name },
                ),
                address,
                quota,
                reservation,
                CompressionAlgorithm::Off,
            ) {
                Ensure::Added => added += 1,
                Ensure::Updated => updated += 1,
                Ensure::NotNeeded => (),
            }
        }

        // Dataset for durable dataset co-located with zone
        if let Some(dataset) = zone.zone_type.durable_dataset() {
            let zpool = &dataset.dataset.pool_name;

            if let Some(fs_zpool) = &zone.filesystem_pool {
                debug_assert_eq!(
                    zpool, fs_zpool,
                    "zone has durable dataset and transient root \
                     on different zpools"
                );
            }

            let address = match zone.zone_type {
                BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible { address, .. },
                ) => Some(address),
                _ => None,
            };
            let quota = None;
            let reservation = None;
            match self.datasets.ensure_dataset(
                DatasetName::new(zpool.clone(), dataset.kind),
                address,
                quota,
                reservation,
                CompressionAlgorithm::Off,
            ) {
                Ensure::Added => added += 1,
                Ensure::Updated => updated += 1,
                Ensure::NotNeeded => (),
            }
        }

        if added == 0 && updated == 0 {
            EnsureMultiple::NotNeeded
        } else {
            EnsureMultiple::Changed { added, updated, expunged: 0, removed: 0 }
        }
    }

    pub fn expunge_zone_datasets(
        &mut self,
        zone: &BlueprintZoneConfig,
    ) -> EnsureMultiple {
        let mut expunged = 0;

        if zone.filesystem_pool.is_some() {
            let name = zone_name(&zone);
            let kind = DatasetKind::TransientZone { name };
            expunged += self.datasets.expunge_datasets_if(|dataset_config| {
                dataset_config.kind == kind
            });
        }

        if let Some(dataset) = zone.zone_type.durable_dataset() {
            expunged += self.datasets.expunge_datasets_if(|dataset_config| {
                dataset_config.pool == dataset.dataset.pool_name
                    && dataset_config.kind == dataset.kind
            });
        }

        if expunged == 0 {
            EnsureMultiple::NotNeeded
        } else {
            EnsureMultiple::Changed {
                added: 0,
                updated: 0,
                expunged,
                removed: 0,
            }
        }
    }
}

pub(super) fn zone_name(zone: &BlueprintZoneConfig) -> String {
    illumos_utils::zone::zone_name(
        zone.zone_type.kind().zone_prefix(),
        Some(zone.id),
    )
}
