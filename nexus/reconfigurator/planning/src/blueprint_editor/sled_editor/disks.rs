// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum DisksEditError {
    #[error("tried to expunge nonexistent disk {id}")]
    ExpungeNonexistentDisk { id: PhysicalDiskUuid },
}

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid blueprint input: duplicate disk ID {id} \
     (zpools: {zpool1:?}, {zpool2:?})"
)]
pub struct DuplicateDiskId {
    pub id: PhysicalDiskUuid,
    pub zpool1: ZpoolUuid,
    pub zpool2: ZpoolUuid,
}

#[derive(Debug)]
pub(super) struct DisksEditor {
    generation: Generation,
    disks: BTreeMap<PhysicalDiskUuid, BlueprintPhysicalDiskConfig>,
    counts: EditCounts,
}

impl DisksEditor {
    pub fn finalize(self) -> (BlueprintPhysicalDisksConfig, EditCounts) {
        let mut generation = self.generation;
        if self.counts.has_nonzero_counts() {
            generation = generation.next();
        }

        (
            BlueprintPhysicalDisksConfig {
                generation,
                disks: self.disks.into_values().collect(),
            },
            self.counts,
        )
    }

    pub fn contains_zpool(&self, zpool_id: &ZpoolUuid) -> bool {
        self.disks.values().any(|disk| disk.pool_id == *zpool_id)
    }

    pub fn ensure(&mut self, disk: BlueprintPhysicalDiskConfig) {
        match self.disks.entry(disk.id) {
            Entry::Vacant(slot) => {
                slot.insert(disk);
                self.counts.added += 1;
            }
            Entry::Occupied(mut slot) => {
                if *slot.get() != disk {
                    slot.insert(disk);
                    self.counts.updated += 1;
                }
            }
        }
    }

    pub fn expunge(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<ZpoolUuid, DisksEditError> {
        let config = self.disks.get_mut(disk_id).ok_or_else(|| {
            DisksEditError::ExpungeNonexistentDisk { id: *disk_id }
        })?;

        match config.disposition {
            BlueprintPhysicalDiskDisposition::InService => {
                config.disposition = BlueprintPhysicalDiskDisposition::Expunged;
                self.counts.expunged += 1;
            }
            BlueprintPhysicalDiskDisposition::Expunged => {
                // expunge is idempotent; do nothing
            }
        }

        Ok(config.pool_id)
    }
}

impl TryFrom<BlueprintPhysicalDisksConfig> for DisksEditor {
    type Error = DuplicateDiskId;

    fn try_from(
        config: BlueprintPhysicalDisksConfig,
    ) -> Result<Self, Self::Error> {
        let mut disks = BTreeMap::new();
        for disk in config.disks {
            match disks.entry(disk.id) {
                Entry::Vacant(slot) => {
                    slot.insert(disk);
                }
                Entry::Occupied(prev) => {
                    return Err(DuplicateDiskId {
                        id: disk.id,
                        zpool1: disk.pool_id,
                        zpool2: prev.get().pool_id,
                    });
                }
            }
        }
        Ok(Self {
            generation: config.generation,
            disks,
            counts: EditCounts::zeroes(),
        })
    }
}
