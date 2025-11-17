// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use iddqd::IdOrdMap;
use iddqd::id_ord_map::Entry;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;

#[derive(Debug, thiserror::Error)]
pub enum DisksEditError {
    #[error("tried to expunge nonexistent disk {id}")]
    ExpungeNonexistentDisk { id: PhysicalDiskUuid },
    #[error("tried to decommission nonexistent disk {id}")]
    DecommissionNonexistentDisk { id: PhysicalDiskUuid },
    #[error("tried to mark an expunged disk as in service {id}")]
    AddExpungedDisk { id: PhysicalDiskUuid },
    #[error("tried to decommission an in service disk {id}")]
    DecommissionInServiceDisk { id: PhysicalDiskUuid },
}

#[derive(Debug)]
pub(super) struct DisksEditor {
    incoming_sled_agent_generation: Generation,
    disks: IdOrdMap<BlueprintPhysicalDiskConfig>,
    counts: EditCounts,
}

impl DisksEditor {
    pub fn new(
        incoming_sled_agent_generation: Generation,
        disks: IdOrdMap<BlueprintPhysicalDiskConfig>,
    ) -> Self {
        Self {
            incoming_sled_agent_generation,
            disks,
            counts: EditCounts::zeroes(),
        }
    }

    pub fn empty() -> Self {
        Self {
            incoming_sled_agent_generation: Generation::new(),
            disks: IdOrdMap::new(),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn finalize(
        self,
    ) -> (IdOrdMap<BlueprintPhysicalDiskConfig>, EditCounts) {
        (self.disks, self.counts)
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    pub fn disks<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = &BlueprintPhysicalDiskConfig>
    where
        F: FnMut(BlueprintPhysicalDiskDisposition) -> bool,
    {
        self.disks.iter().filter(move |config| filter(config.disposition))
    }

    pub fn contains_zpool(&self, zpool_id: &ZpoolUuid) -> bool {
        self.disks.iter().any(|disk| disk.pool_id == *zpool_id)
    }

    pub fn ensure(
        &mut self,
        disk: BlueprintPhysicalDiskConfig,
    ) -> Result<(), DisksEditError> {
        match self.disks.entry(disk.id) {
            Entry::Vacant(slot) => {
                slot.insert(disk);
                self.counts.added += 1;
            }
            Entry::Occupied(mut slot) => {
                let existing = slot.get();
                if *existing != disk {
                    match (existing.disposition, disk.disposition) {
                        // All other combinations are valid
                        (
                            BlueprintPhysicalDiskDisposition::Expunged {
                                ..
                            },
                            BlueprintPhysicalDiskDisposition::InService,
                        ) => {
                            return Err(DisksEditError::AddExpungedDisk {
                                id: disk.id,
                            });
                        }
                        // All following combinations are valid
                        (
                            BlueprintPhysicalDiskDisposition::Expunged {
                                ..
                            },
                            BlueprintPhysicalDiskDisposition::Expunged {
                                ..
                            },
                        ) => (),
                        (
                            BlueprintPhysicalDiskDisposition::InService,
                            BlueprintPhysicalDiskDisposition::InService,
                        ) => (),
                        (
                            BlueprintPhysicalDiskDisposition::InService,
                            BlueprintPhysicalDiskDisposition::Expunged {
                                ..
                            },
                        ) => (),
                    }

                    slot.insert(disk);
                    self.counts.updated += 1;
                }
            }
        }
        Ok(())
    }

    pub fn expunge(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<(bool, ZpoolUuid), DisksEditError> {
        let mut config = self.disks.get_mut(disk_id).ok_or_else(|| {
            DisksEditError::ExpungeNonexistentDisk { id: *disk_id }
        })?;

        let did_expunge: bool;
        match config.disposition {
            BlueprintPhysicalDiskDisposition::InService => {
                config.disposition =
                    BlueprintPhysicalDiskDisposition::Expunged {
                        // Our parent is responsible for bumping the sled-agent
                        // generation; record that bumped value here as the
                        // generation in which this disk was expunged.
                        as_of_generation: self
                            .incoming_sled_agent_generation
                            .next(),
                        ready_for_cleanup: false,
                    };
                self.counts.expunged += 1;
                did_expunge = true;
            }
            BlueprintPhysicalDiskDisposition::Expunged { .. } => {
                // expunge is idempotent; do nothing
                did_expunge = false;
            }
        }

        Ok((did_expunge, config.pool_id))
    }

    pub fn decommission(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Result<bool, DisksEditError> {
        let mut config = self.disks.get_mut(disk_id).ok_or_else(|| {
            DisksEditError::DecommissionNonexistentDisk { id: *disk_id }
        })?;

        match config.disposition {
            BlueprintPhysicalDiskDisposition::InService => {
                return Err(DisksEditError::DecommissionInServiceDisk {
                    id: *disk_id,
                });
            }
            BlueprintPhysicalDiskDisposition::Expunged {
                ready_for_cleanup: false,
                as_of_generation,
            } => {
                config.disposition =
                    BlueprintPhysicalDiskDisposition::Expunged {
                        ready_for_cleanup: true,
                        as_of_generation,
                    };
                let did_decommission = true;
                Ok(did_decommission)
            }
            // We've already decommissioned this disk
            BlueprintPhysicalDiskDisposition::Expunged {
                ready_for_cleanup: true,
                ..
            } => {
                let did_decommision = false;
                Ok(did_decommision)
            }
        }
    }
}
