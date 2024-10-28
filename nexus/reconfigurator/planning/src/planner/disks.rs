// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EnsureMultiple;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::SledResources;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::collections::HashSet;

/// Helper for working with sets of disks on each sled
///
/// Tracking the set of disks is slightly non-trivial because we need to
/// bump the per-sled generation number iff the disks are changed.  So
/// we need to keep track of whether we've changed the disks relative
/// to the parent blueprint.  We do this by keeping a copy of any
/// [`BlueprintPhysicalDisksConfig`] that we've changed and a _reference_ to
/// the parent blueprint's disks.  This struct makes it easy for callers iterate
/// over the right set of disks.
pub(super) struct BlueprintDisksBuilder<'a> {
    changed_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
    parent_disks: &'a BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
}

impl<'a> BlueprintDisksBuilder<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> Self {
        Self {
            changed_disks: BTreeMap::new(),
            parent_disks: &parent_blueprint.blueprint_disks,
        }
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
    ) -> EnsureMultiple {
        let (mut additions, removals) = {
            // These are the disks known to our (last?) blueprint
            let blueprint_disks: BTreeMap<_, _> = self
                .current_sled_disks(sled_id)
                .map(|disk| {
                    (PhysicalDiskUuid::from_untyped_uuid(disk.id), disk)
                })
                .collect();

            // These are the in-service disks as we observed them in the database,
            // during the planning phase
            let database_disks: BTreeMap<_, _> = resources
                .all_disks(DiskFilter::InService)
                .map(|(zpool, disk)| (disk.disk_id, (zpool, disk)))
                .collect();

            // Add any disks that appear in the database, but not the blueprint
            let additions = database_disks
                .iter()
                .filter_map(|(disk_id, (zpool, disk))| {
                    if !blueprint_disks.contains_key(disk_id) {
                        Some(BlueprintPhysicalDiskConfig {
                            identity: disk.disk_identity.clone(),
                            id: disk_id.into_untyped_uuid(),
                            pool_id: **zpool,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<BlueprintPhysicalDiskConfig>>();

            // Remove any disks that appear in the blueprint, but not the database
            let removals: HashSet<PhysicalDiskUuid> = blueprint_disks
                .keys()
                .filter_map(|disk_id| {
                    if !database_disks.contains_key(disk_id) {
                        Some(*disk_id)
                    } else {
                        None
                    }
                })
                .collect();

            (additions, removals)
        };

        if additions.is_empty() && removals.is_empty() {
            return EnsureMultiple::NotNeeded;
        }
        let added = additions.len();
        let removed = removals.len();

        let disks = &mut self.change_sled_disks(sled_id).disks;

        disks.append(&mut additions);
        disks.retain(|config| {
            !removals.contains(&PhysicalDiskUuid::from_untyped_uuid(config.id))
        });

        EnsureMultiple::Changed { added, updated: 0, expunged: 0, removed }
    }

    /// Returns a mutable reference to a sled's Omicron disks *because* we're
    /// going to change them.
    ///
    /// Unlike [`BlueprintZonesBuilder::change_sled_zones`], it is essential
    /// that the caller _does_ change them, because constructing this bumps the
    /// generation number unconditionally.
    fn change_sled_disks(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BlueprintPhysicalDisksConfig {
        self.changed_disks.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_disks) = self.parent_disks.get(&sled_id) {
                BlueprintPhysicalDisksConfig {
                    generation: old_sled_disks.generation.next(),
                    disks: old_sled_disks.disks.clone(),
                }
            } else {
                // No requests have been sent to the disk previously,
                // we should be able to use the first generation.
                BlueprintPhysicalDisksConfig {
                    generation: Generation::new(),
                    disks: vec![],
                }
            }
        })
    }

    /// Iterates over the list of Omicron disks currently configured for this
    /// sled in the blueprint that's being built
    pub(crate) fn current_sled_disks(
        &self,
        sled_id: SledUuid,
    ) -> Box<dyn Iterator<Item = &BlueprintPhysicalDiskConfig> + '_> {
        if let Some(sled_disks) = self
            .changed_disks
            .get(&sled_id)
            .or_else(|| self.parent_disks.get(&sled_id))
        {
            Box::new(sled_disks.disks.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of disks for the requested sleds
    pub fn into_disks_map(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintPhysicalDisksConfig> {
        sled_ids
            .map(|sled_id| {
                // Start with self.changed_disks, which contains entries for any
                // sled whose disks config is changing in this blueprint.
                let mut disks = self
                    .changed_disks
                    .remove(&sled_id)
                    // If it's not there, use the config from the parent
                    // blueprint.
                    .or_else(|| self.parent_disks.get(&sled_id).cloned())
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any disks to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| BlueprintPhysicalDisksConfig {
                        generation: Generation::new(),
                        disks: vec![],
                    });
                disks.disks.sort_unstable_by_key(|d| d.id);

                (sled_id, disks)
            })
            .collect()
    }
}
