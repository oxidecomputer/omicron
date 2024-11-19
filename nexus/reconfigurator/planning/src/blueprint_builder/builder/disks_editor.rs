// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for editing the disks of a Blueprint

use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::Ensure;

/// Helper for working with sets of disks on each sled
///
/// Tracking the set of disks is slightly non-trivial because we need to
/// bump the per-sled generation number iff the disks are changed.  So
/// we need to keep track of whether we've changed the disks relative
/// to the parent blueprint.
#[derive(Debug)]
pub(super) struct BlueprintDisksEditor {
    current: BTreeMap<SledUuid, DisksConfig>,
    changed: BTreeSet<SledUuid>,
}

impl BlueprintDisksEditor {
    pub fn new(
        current: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
    ) -> Self {
        let current = current
            .into_iter()
            .map(|(sled_id, config)| (sled_id, config.into()))
            .collect();
        Self { current, changed: BTreeSet::new() }
    }

    /// Get a helper to edit the disks of a specific sled.
    ///
    /// If any changes are made via the returned editor, the sled will be
    /// recorded as needing a generation bump in its disk config when the editor
    /// is dropped.
    pub fn sled_disks_editor(
        &mut self,
        sled_id: SledUuid,
    ) -> SledDisksEditor<'_> {
        let config =
            self.current.entry(sled_id).or_insert_with(DisksConfig::empty);
        SledDisksEditor::new(sled_id, config, &mut self.changed)
    }

    #[cfg(test)]
    pub fn current_sled_disks(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&BTreeMap<PhysicalDiskUuid, BlueprintPhysicalDiskConfig>> {
        let config = self.current.get(sled_id)?;
        Some(&config.disks)
    }

    pub fn build(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintPhysicalDisksConfig> {
        sled_ids
            .map(|sled_id| {
                let config = match self.current.remove(&sled_id) {
                    Some(mut config) => {
                        // Bump generation number for any sled whose DisksConfig
                        // changed
                        if self.changed.contains(&sled_id) {
                            config.generation = config.generation.next()
                        }
                        config.into()
                    }
                    None => DisksConfig::empty().into(),
                };
                (sled_id, config)
            })
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct SledDisksEditor<'a> {
    config: &'a mut DisksConfig,
    changed: bool,
    sled_id: SledUuid,
    parent_changed_set: &'a mut BTreeSet<SledUuid>,
}

impl Drop for SledDisksEditor<'_> {
    fn drop(&mut self) {
        if self.changed {
            self.parent_changed_set.insert(self.sled_id);
        }
    }
}

impl<'a> SledDisksEditor<'a> {
    fn new(
        sled_id: SledUuid,
        config: &'a mut DisksConfig,
        parent_changed_set: &'a mut BTreeSet<SledUuid>,
    ) -> Self {
        Self { config, changed: false, sled_id, parent_changed_set }
    }

    pub fn disk_ids(&self) -> impl Iterator<Item = PhysicalDiskUuid> + '_ {
        self.config.disks.keys().copied()
    }

    pub fn ensure_disk(&mut self, disk: BlueprintPhysicalDiskConfig) -> Ensure {
        let disk_id = PhysicalDiskUuid::from_untyped_uuid(disk.id);
        match self.config.disks.entry(disk_id) {
            Entry::Vacant(slot) => {
                slot.insert(disk);
                self.changed = true;
                Ensure::Added
            }
            Entry::Occupied(mut slot) => {
                if *slot.get() != disk {
                    slot.insert(disk);
                    self.changed = true;
                    Ensure::Updated
                } else {
                    Ensure::NotNeeded
                }
            }
        }
    }

    pub fn remove_disk(
        &mut self,
        disk_id: &PhysicalDiskUuid,
    ) -> Option<BlueprintPhysicalDiskConfig> {
        self.changed = true;
        self.config.disks.remove(disk_id)
    }
}

// We want add and remove to be cheap and easy to check whether they performed
// the requested operation, so we'll internally convert from the vec of disks to
// a map of disks keyed by disk ID.
#[derive(Debug)]
struct DisksConfig {
    generation: Generation,
    disks: BTreeMap<PhysicalDiskUuid, BlueprintPhysicalDiskConfig>,
}

impl DisksConfig {
    fn empty() -> Self {
        Self { generation: Generation::new(), disks: BTreeMap::new() }
    }
}

impl From<DisksConfig> for BlueprintPhysicalDisksConfig {
    fn from(config: DisksConfig) -> Self {
        BlueprintPhysicalDisksConfig {
            generation: config.generation,
            disks: config.disks.into_values().collect(),
        }
    }
}

impl From<BlueprintPhysicalDisksConfig> for DisksConfig {
    fn from(config: BlueprintPhysicalDisksConfig) -> Self {
        Self {
            generation: config.generation,
            disks: config
                .disks
                .into_iter()
                .map(|disk| {
                    (PhysicalDiskUuid::from_untyped_uuid(disk.id), disk)
                })
                .collect(),
        }
    }
}
