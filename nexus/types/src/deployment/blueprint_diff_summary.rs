// TODO: Move this all to blueprint_diff.rs

use super::blueprint_diff::{
    BpDiffZoneDetails, BpDiffZoneErrors, BpDiffZonesModified,
};
use super::blueprint_display::{
    constants::*, linear_table_modified, linear_table_unchanged,
    BpClickhouseServersTableSchema, BpDatasetsTableSchema, BpDiffState,
    BpGeneration, BpOmicronZonesTableSchema, BpPhysicalDisksTableSchema,
    BpTable, BpTableColumn, BpTableData, BpTableRow, KvListWithHeading, KvPair,
};
use super::{
    BlueprintDatasetsConfig, BlueprintDatasetsConfigDiff, BlueprintDiff,
    BlueprintPhysicalDisksConfig, BlueprintPhysicalDisksConfigDiff,
    BlueprintZoneConfig, BlueprintZoneConfigDiff, BlueprintZonesConfig,
    BlueprintZonesConfigDiff,
};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeSet;

// A wrapper type around a `daft` generated `BlueprintDiff that provides summary
// data and direct access to the underlying diff.
#[derive(Debug, Clone)]
pub struct BlueprintDiffSummary<'a> {
    pub diff: &'a BlueprintDiff<'a>,
    pub all_sleds: BTreeSet<SledUuid>,
    pub sleds_added: BTreeSet<SledUuid>,
    pub sleds_removed: BTreeSet<SledUuid>,
    pub sleds_modified: BTreeSet<SledUuid>,
    pub sleds_unchanged: BTreeSet<SledUuid>,
}

impl<'a> BlueprintDiffSummary<'a> {
    pub fn new(diff: &'a BlueprintDiff<'a>) -> Self {
        // We assume for now that sled_state additions represent sled additions.
        // Once we collapse the 4 blueprint maps this will be unambiguously
        // true.
        let sleds_added: BTreeSet<SledUuid> =
            diff.sled_state.added.keys().map(|k| **k).collect();

        // We can't do the same for removals unfortunately. We prematurely
        // prune decommissioned sleds, but there may still be zones, disks,
        // or datasets that have not yet been removed. We must check for this.
        //
        // Removed sleds are the intersection of sleds removed in
        // `blueprint_zones`, `blueprint_disks`, and `blueprint_datasets`
        let zone_sleds_removed: BTreeSet<_> =
            diff.blueprint_zones.removed.keys().map(|k| **k).collect();
        let disk_sleds_removed: BTreeSet<_> =
            diff.blueprint_disks.removed.keys().map(|k| **k).collect();
        let dataset_sleds_removed: BTreeSet<_> =
            diff.blueprint_datasets.removed.keys().map(|k| **k).collect();
        let sleds_removed: BTreeSet<_> = zone_sleds_removed
            .intersection(&disk_sleds_removed)
            .cloned()
            .collect();
        let sleds_removed: BTreeSet<_> = sleds_removed
            .intersection(&dataset_sleds_removed)
            .cloned()
            .collect();

        // Sleds unchanged are the intersection of all unchanged sets.
        let zone_sleds_unchanged: BTreeSet<_> =
            diff.blueprint_zones.unchanged.iter().map(|(id, _)| **id).collect();
        let disk_sleds_unchanged: BTreeSet<_> =
            diff.blueprint_disks.unchanged.iter().map(|(id, _)| **id).collect();
        let dataset_sleds_unchanged: BTreeSet<_> = diff
            .blueprint_datasets
            .unchanged
            .iter()
            .map(|(id, _)| **id)
            .collect();
        let sleds_unchanged: BTreeSet<_> = zone_sleds_unchanged
            .intersection(&disk_sleds_unchanged)
            .cloned()
            .collect();
        let sleds_unchanged: BTreeSet<_> = sleds_unchanged
            .intersection(&dataset_sleds_unchanged)
            .cloned()
            .collect();

        // Modifieds sleds are the union of sleds modified in `sled_state`,
        // `blueprint_zones`, `blueprint_disks`, and `blueprint_datasets`.
        //
        // Because of backwards compatibility, disks and datasets get removed
        // when expunged. Because of this we must also check for removed disks
        // and datasets that are not in removed sleds.
        let mut sleds_modified: BTreeSet<_> = diff
            .sled_state
            .modified
            .keys()
            .chain(diff.blueprint_zones.modified.keys())
            .chain(diff.blueprint_disks.modified.keys())
            .chain(diff.blueprint_datasets.modified.keys())
            .map(|k| **k)
            .collect();
        for sled_id in diff
            .blueprint_disks
            .removed
            .keys()
            .chain(diff.blueprint_datasets.removed.keys())
        {
            if !sleds_removed.contains(*sled_id) {
                sleds_modified.insert(**sled_id);
            }
        }

        let all_sleds = sleds_added
            .iter()
            .chain(sleds_removed.iter())
            .chain(sleds_modified.iter())
            .chain(sleds_unchanged.iter())
            .cloned()
            .collect();

        BlueprintDiffSummary {
            diff,
            all_sleds,
            sleds_added,
            sleds_removed,
            sleds_modified,
            sleds_unchanged,
        }
    }

    ///  The number of zones added across all sleds
    pub fn total_zones_added(&self) -> usize {
        self.diff
            .blueprint_zones
            .added
            .values()
            .fold(0, |acc, c| acc + c.zones.len())
            + self
                .diff
                .blueprint_zones
                .modified
                .values()
                .fold(0, |acc, c| acc + c.zones.added.len())
    }

    ///  The number of zones removed across all sleds
    pub fn total_zones_removed(&self) -> usize {
        self.diff
            .blueprint_zones
            .removed
            .values()
            .fold(0, |acc, c| acc + c.zones.len())
            + self
                .diff
                .blueprint_zones
                .modified
                .values()
                .fold(0, |acc, c| acc + c.zones.removed.len())
    }
    ///  The number of zones modified across all sleds
    pub fn total_zones_modified(&self) -> usize {
        self.diff
            .blueprint_zones
            .modified
            .values()
            .fold(0, |acc, c| acc + c.zones.modified.len())
    }

    ///  The number of disks added across all sleds
    pub fn total_disks_added(&self) -> usize {
        self.diff
            .blueprint_disks
            .added
            .values()
            .fold(0, |acc, c| acc + c.disks.len())
            + self
                .diff
                .blueprint_disks
                .modified
                .values()
                .fold(0, |acc, c| acc + c.disks.added.len())
    }

    ///  The number of disks removed across all sleds
    pub fn total_disks_removed(&self) -> usize {
        self.diff
            .blueprint_disks
            .removed
            .values()
            .fold(0, |acc, c| acc + c.disks.len())
            + self
                .diff
                .blueprint_disks
                .modified
                .values()
                .fold(0, |acc, c| acc + c.disks.removed.len())
    }
    ///  The number of disks modified across all sleds
    pub fn total_disks_modified(&self) -> usize {
        self.diff
            .blueprint_disks
            .modified
            .values()
            .fold(0, |acc, c| acc + c.disks.modified.len())
    }

    ///  The number of datasets added across all sleds
    pub fn total_datasets_added(&self) -> usize {
        self.diff
            .blueprint_datasets
            .added
            .values()
            .fold(0, |acc, c| acc + c.datasets.len())
            + self
                .diff
                .blueprint_datasets
                .modified
                .values()
                .fold(0, |acc, c| acc + c.datasets.added.len())
    }

    ///  The number of datasets removed across all sleds
    pub fn total_datasets_removed(&self) -> usize {
        self.diff
            .blueprint_datasets
            .removed
            .values()
            .fold(0, |acc, c| acc + c.datasets.len())
            + self
                .diff
                .blueprint_datasets
                .modified
                .values()
                .fold(0, |acc, c| acc + c.datasets.removed.len())
    }
    ///  The number of datasets modified across all sleds
    pub fn total_datasets_modified(&self) -> usize {
        self.diff
            .blueprint_datasets
            .modified
            .values()
            .fold(0, |acc, c| acc + c.datasets.modified.len())
    }

    /// Return the `BlueprintZonesConfig` for a newly added sled
    pub fn zones_on_added_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintZonesConfig> {
        self.diff.blueprint_zones.added.get(sled_id).cloned()
    }

    /// Return the `BlueprintZonesConfig` for a removed sled
    pub fn zones_on_removed_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintZonesConfig> {
        self.diff.blueprint_zones.removed.get(sled_id).cloned()
    }

    /// Return the `BlueprintZonesConfigDiff` for a modified sled
    pub fn zones_on_modified_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintZonesConfigDiff> {
        self.diff.blueprint_zones.modified.get(sled_id)
    }

    /// Iterate over all added zones on a sled
    pub fn added_zones(&self, sled_id: &SledUuid) -> Option<BpDiffZoneDetails> {
        // First check if the sled is added
        if let Some(&zones_cfg) = self.diff.blueprint_zones.added.get(sled_id) {
            return Some(BpDiffZoneDetails::new(
                Some(zones_cfg.generation),
                None,
                zones_cfg.zones.iter(),
            ));
        }

        // Then check if the sled is modified and there are any removed zones
        self.diff.blueprint_zones.modified.get(sled_id).map(|zones_cfg_diff| {
            BpDiffZoneDetails::new(
                Some(*zones_cfg_diff.generation.before),
                Some(*zones_cfg_diff.generation.after),
                zones_cfg_diff.zones.added.values().map(|z| *z),
            )
        })
    }

    /// Iterate over all removed zones on a sled
    pub fn removed_zones(
        &self,
        sled_id: &SledUuid,
    ) -> Option<BpDiffZoneDetails> {
        // First check if the sled is removed
        if let Some(&zones_cfg) = self.diff.blueprint_zones.removed.get(sled_id)
        {
            return Some(BpDiffZoneDetails::new(
                Some(zones_cfg.generation),
                None,
                zones_cfg.zones.iter(),
            ));
        }

        // Then check if the sled is modified and there are any removed zones
        self.diff.blueprint_zones.modified.get(sled_id).map(|zones_cfg_diff| {
            BpDiffZoneDetails::new(
                Some(*zones_cfg_diff.generation.before),
                Some(*zones_cfg_diff.generation.after),
                zones_cfg_diff.zones.removed.values().map(|z| *z),
            )
        })
    }

    /// Iterate over all modified zones on a sled
    pub fn modified_zones(
        &self,
        sled_id: &SledUuid,
    ) -> Option<(BpDiffZonesModified, BpDiffZoneErrors)> {
        // Then check if the sled is modified and there are any added zones
        self.diff.blueprint_zones.modified.get(sled_id).map(|zones_cfg_diff| {
            BpDiffZonesModified::new(
                *zones_cfg_diff.generation.before,
                *zones_cfg_diff.generation.after,
                zones_cfg_diff.zones.modified.values(),
            )
        })
    }

    /// Iterate over all unchanged zones on a sled
    pub fn unchanged_zones(
        &self,
        sled_id: &SledUuid,
    ) -> Option<BpDiffZoneDetails> {
        // First check if the sled is unchanged
        if let Some(&zones_cfg) =
            self.diff.blueprint_zones.unchanged.get(sled_id)
        {
            return Some(BpDiffZoneDetails::new(
                Some(zones_cfg.generation),
                None,
                zones_cfg.zones.iter(),
            ));
        }

        // Then check if the sled is modified and there are any unchanged zones
        self.diff.blueprint_zones.modified.get(sled_id).map(|zones_cfg_diff| {
            BpDiffZoneDetails::new(
                Some(*zones_cfg_diff.generation.before),
                Some(*zones_cfg_diff.generation.after),
                zones_cfg_diff.zones.unchanged.values().map(|z| *z),
            )
        })
    }

    /// Return the `BlueprintDisksConfig` for a newly added sled
    pub fn disks_on_added_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintPhysicalDisksConfig> {
        self.diff.blueprint_disks.added.get(sled_id).cloned()
    }

    /// Return the `BlueprintDisksConfig` for a removed sled
    pub fn disks_on_removed_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintPhysicalDisksConfig> {
        self.diff.blueprint_disks.removed.get(sled_id).cloned()
    }

    /// Return the `BlueprintDisksConfigDiff` for a modified sled
    pub fn disks_on_modified_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintPhysicalDisksConfigDiff> {
        self.diff.blueprint_disks.modified.get(sled_id)
    }

    /// Return the `BlueprintDatasetsConfig` for a newly added sled
    pub fn datasets_on_added_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintDatasetsConfig> {
        self.diff.blueprint_datasets.added.get(sled_id).cloned()
    }

    /// Return the `BlueprintDatasetsConfig` for a removed sled
    pub fn datasets_on_removed_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintDatasetsConfig> {
        self.diff.blueprint_datasets.removed.get(sled_id).cloned()
    }

    /// Return the `BlueprintDatasetsConfigDiff` for a modified sled
    pub fn datasets_on_modified_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&'a BlueprintDatasetsConfigDiff> {
        self.diff.blueprint_datasets.modified.get(sled_id)
    }
}

/// Wrapper to allow a [`BlueprintDiff`] to be displayed.
///
/// Returned by [`BlueprintDiff::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintDiffDisplay<'diff> {
    pub diff: &'diff BlueprintDiffSummary<'diff>,
}

impl<'diff> BlueprintDiffDisplay<'diff> {
    #[inline]
    fn new(diff: &'diff BlueprintDiffSummary<'diff>) -> Self {
        Self { diff }
    }
}
