// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types helpful for diffing blueprints.

use super::blueprint_display::{
    constants::*, linear_table_modified, linear_table_unchanged,
    BpClickhouseServersTableSchema, BpDatasetsTableSchema, BpDiffState,
    BpGeneration, BpOmicronZonesTableSchema, BpPhysicalDisksTableSchema,
    BpTable, BpTableColumn, BpTableData, BpTableRow, KvListWithHeading, KvPair,
};
use super::{
    unwrap_or_none, zone_sort_key, BlueprintDatasetConfigDiff,
    BlueprintDatasetDisposition, BlueprintDatasetsConfigDiff, BlueprintDiff,
    BlueprintMetadata, BlueprintPhysicalDiskConfig,
    BlueprintPhysicalDiskConfigDiff, BlueprintPhysicalDisksConfigDiff,
    BlueprintSledConfigDiff, BlueprintZoneConfigDiff, BlueprintZonesConfigDiff,
    ClickhouseClusterConfig, CockroachDbPreserveDowngrade,
};
use daft::Diffable;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::external::{ByteCount, Generation};
use omicron_common::disk::{CompressionAlgorithm, DatasetName};
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::{DatasetUuid, OmicronZoneUuid, PhysicalDiskUuid};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};

use crate::deployment::blueprint_display::BpClickhouseKeepersTableSchema;
use crate::deployment::{
    Blueprint, BlueprintDatasetConfig, BlueprintZoneConfig,
    BlueprintZoneDisposition, CollectionDatasetIdentifier, ZoneSortKey,
};

// A wrapper type around a `daft` generated `BlueprintDiff that provides summary
// data and direct access to the underlying diff.
#[derive(Debug)]
pub struct BlueprintDiffSummary<'a> {
    pub before: &'a Blueprint,
    pub after: &'a Blueprint,
    pub diff: BlueprintDiff<'a>,
    pub modified_sleds_diff: BTreeMap<SledUuid, BlueprintSledConfigDiff<'a>>,
    // TODO-john do we need these sets still?
    pub all_sleds: BTreeSet<SledUuid>,
    pub sleds_added: BTreeSet<SledUuid>,
    pub sleds_removed: BTreeSet<SledUuid>,
    pub sleds_modified: BTreeSet<SledUuid>,
    pub sleds_unchanged: BTreeSet<SledUuid>,
}

impl<'a> BlueprintDiffSummary<'a> {
    pub fn new(before: &'a Blueprint, after: &'a Blueprint) -> Self {
        let diff = before.diff(after);

        let modified_sleds_diff =
            diff.sleds.modified_diff().map(|(k, v)| (*k, v)).collect();

        // Collect sets of sled IDs of various categories.
        //
        // TODO-john do we still need these or can we use `diff.sleds` directly?
        let sleds_added: BTreeSet<_> =
            diff.sleds.added.keys().map(|k| **k).collect();
        let sleds_removed: BTreeSet<_> =
            diff.sleds.removed.keys().map(|k| **k).collect();
        let sleds_unchanged: BTreeSet<_> =
            diff.sleds.unchanged_keys().copied().collect();
        let sleds_modified: BTreeSet<_> =
            diff.sleds.modified_keys().copied().collect();

        let all_sleds = sleds_added
            .iter()
            .chain(sleds_removed.iter())
            .chain(sleds_modified.iter())
            .chain(sleds_unchanged.iter())
            .cloned()
            .collect();

        BlueprintDiffSummary {
            before,
            after,
            diff,
            modified_sleds_diff,
            all_sleds,
            sleds_added,
            sleds_removed,
            sleds_modified,
            sleds_unchanged,
        }
    }

    /// Return a struct that can be used to display the diff.
    pub fn display(&'a self) -> BlueprintDiffDisplay<'a> {
        BlueprintDiffDisplay::new(self)
    }

    /// Returns whether the diff reflects any changes or if the blueprints are
    /// equivalent.
    pub fn has_changes(&self) -> bool {
        // Any changes to physical disks, datasets, or zones would be reflected
        // in `self.sleds_modified`, `self.sleds_added`, or
        // `self.sleds_removed`.
        if !self.sleds_modified.is_empty()
            || !self.sleds_added.is_empty()
            || !self.sleds_removed.is_empty()
        {
            return true;
        }

        self.diff.clickhouse_cluster_config.before
            != self.diff.clickhouse_cluster_config.after
    }

    ///  The number of zones added across all sleds
    pub fn total_zones_added(&self) -> usize {
        self.diff
            .sleds
            .added
            .values()
            .fold(0, |acc, c| acc + c.zones_config.zones.len())
            + self
                .modified_sleds_diff
                .values()
                .fold(0, |acc, c| acc + c.zones_config.zones.added.len())
    }

    ///  The number of zones removed across all sleds
    pub fn total_zones_removed(&self) -> usize {
        self.diff
            .sleds
            .removed
            .values()
            .fold(0, |acc, c| acc + c.zones_config.zones.len())
            + self
                .modified_sleds_diff
                .values()
                .fold(0, |acc, c| acc + c.zones_config.zones.removed.len())
    }
    ///  The number of zones modified across all sleds
    pub fn total_zones_modified(&self) -> usize {
        self.modified_sleds_diff
            .values()
            .fold(0, |acc, c| acc + c.zones_config.zones.modified().count())
    }

    ///  The number of disks added across all sleds
    pub fn total_disks_added(&self) -> usize {
        self.diff
            .sleds
            .added
            .values()
            .fold(0, |acc, c| acc + c.disks_config.disks.len())
            + self
                .modified_sleds_diff
                .values()
                .fold(0, |acc, c| acc + c.disks_config.disks.added.len())
    }

    ///  The number of disks removed across all sleds
    pub fn total_disks_removed(&self) -> usize {
        self.diff
            .sleds
            .removed
            .values()
            .fold(0, |acc, c| acc + c.disks_config.disks.len())
            + self
                .modified_sleds_diff
                .values()
                .fold(0, |acc, c| acc + c.disks_config.disks.removed.len())
    }
    ///  The number of disks modified across all sleds
    pub fn total_disks_modified(&self) -> usize {
        self.modified_sleds_diff
            .values()
            .fold(0, |acc, c| acc + c.disks_config.disks.modified().count())
    }

    ///  The number of datasets added across all sleds
    pub fn total_datasets_added(&self) -> usize {
        self.diff
            .sleds
            .added
            .values()
            .fold(0, |acc, c| acc + c.datasets_config.datasets.len())
            + self
                .modified_sleds_diff
                .values()
                .fold(0, |acc, c| acc + c.datasets_config.datasets.added.len())
    }

    ///  The number of datasets removed across all sleds
    pub fn total_datasets_removed(&self) -> usize {
        self.diff
            .sleds
            .removed
            .values()
            .fold(0, |acc, c| acc + c.datasets_config.datasets.len())
            + self.modified_sleds_diff.values().fold(0, |acc, c| {
                acc + c.datasets_config.datasets.removed.len()
            })
    }
    ///  The number of datasets modified across all sleds
    pub fn total_datasets_modified(&self) -> usize {
        self.modified_sleds_diff.values().fold(0, |acc, c| {
            acc + c.datasets_config.datasets.modified().count()
        })
    }

    /// Return the `BlueprintZonesConfigDiff` for a modified sled
    pub fn zones_on_modified_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&BlueprintZonesConfigDiff<'a>> {
        self.modified_sleds_diff.get(sled_id).map(|c| &c.zones_config)
    }

    /// Return the `BlueprintDisksConfigDiff` for a modified sled
    pub fn disks_on_modified_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&BlueprintPhysicalDisksConfigDiff<'a>> {
        self.modified_sleds_diff.get(sled_id).map(|c| &c.disks_config)
    }

    /// Return the `BlueprintDatasetsConfigDiff` for a modified sled
    pub fn datasets_on_modified_sled(
        &self,
        sled_id: &SledUuid,
    ) -> Option<&BlueprintDatasetsConfigDiff<'a>> {
        self.modified_sleds_diff.get(sled_id).map(|c| &c.datasets_config)
    }

    /// Iterate over all added zones on a sled
    pub fn added_zones(&self, sled_id: &SledUuid) -> Option<BpDiffZoneDetails> {
        // First check if the sled is added
        if let Some(&sled_cfg) = self.diff.sleds.added.get(sled_id) {
            if sled_cfg.zones_config.zones.is_empty() {
                return None;
            }
            return Some(BpDiffZoneDetails::new(
                None,
                Some(sled_cfg.zones_config.generation),
                sled_cfg.zones_config.zones.iter(),
            ));
        }

        // Then check if the sled is modified and there are any added zones
        let zones_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.zones_config;
        if zones_cfg_diff.zones.added.is_empty() {
            return None;
        }
        Some(BpDiffZoneDetails::new(
            Some(*zones_cfg_diff.generation.before),
            Some(*zones_cfg_diff.generation.after),
            zones_cfg_diff.zones.added.values().map(|z| *z),
        ))
    }

    /// Iterate over all removed zones on a sled
    pub fn removed_zones(
        &self,
        sled_id: &SledUuid,
    ) -> Option<BpDiffZoneDetails> {
        // First check if the sled is removed
        if let Some(&sled_cfg) = self.diff.sleds.removed.get(sled_id) {
            if sled_cfg.zones_config.zones.is_empty() {
                return None;
            }
            return Some(BpDiffZoneDetails::new(
                Some(sled_cfg.zones_config.generation),
                None,
                sled_cfg.zones_config.zones.iter(),
            ));
        }

        // Then check if the sled is modified and there are any removed zones
        let zones_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.zones_config;
        if zones_cfg_diff.zones.removed.is_empty() {
            return None;
        }
        Some(BpDiffZoneDetails::new(
            Some(*zones_cfg_diff.generation.before),
            Some(*zones_cfg_diff.generation.after),
            zones_cfg_diff.zones.removed.values().map(|z| *z),
        ))
    }

    /// Iterate over all modified zones on a sled
    pub fn modified_zones(
        &'a self,
        sled_id: &SledUuid,
    ) -> Option<(BpDiffZonesModified, BpDiffZoneErrors)> {
        // Then check if the sled is modified and there are any modified zones
        let zones_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.zones_config;
        let mut modified_zones =
            zones_cfg_diff.zones.modified_values_diff().peekable();
        if modified_zones.peek().is_none() {
            return None;
        }
        Some(BpDiffZonesModified::new(
            *zones_cfg_diff.generation.before,
            *zones_cfg_diff.generation.after,
            modified_zones,
        ))
    }

    /// Iterate over all unchanged zones on a sled
    pub fn unchanged_zones(
        &self,
        sled_id: &SledUuid,
    ) -> Option<BpDiffZoneDetails> {
        // First check if the sled is unchanged
        if let Some(sled_cfg) = self.diff.sleds.get_unchanged(sled_id) {
            if sled_cfg.zones_config.zones.is_empty() {
                return None;
            }
            return Some(BpDiffZoneDetails::new(
                None,
                Some(sled_cfg.zones_config.generation),
                sled_cfg.zones_config.zones.iter(),
            ));
        }

        // Then check if the sled is modified and there are any unchanged zones
        let zones_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.zones_config;
        let mut unchanged_zones =
            zones_cfg_diff.zones.unchanged_values().peekable();
        if unchanged_zones.peek().is_none() {
            return None;
        }
        Some(BpDiffZoneDetails::new(
            Some(*zones_cfg_diff.generation.before),
            Some(*zones_cfg_diff.generation.after),
            unchanged_zones,
        ))
    }

    /// Iterate over all added disks on a sled
    pub fn added_disks(
        &self,
        sled_id: &SledUuid,
    ) -> Option<DiffPhysicalDisksDetails> {
        // First check if the sled is added
        if let Some(&sled_cfg) = self.diff.sleds.added.get(sled_id) {
            if sled_cfg.disks_config.disks.is_empty() {
                return None;
            }
            return Some(DiffPhysicalDisksDetails::new(
                None,
                Some(sled_cfg.disks_config.generation),
                sled_cfg.disks_config.disks.iter(),
            ));
        }

        // Then check if the sled is modified and there are any added disks
        let disks_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.disks_config;
        if disks_cfg_diff.disks.added.is_empty() {
            return None;
        }
        Some(DiffPhysicalDisksDetails::new(
            Some(*disks_cfg_diff.generation.before),
            Some(*disks_cfg_diff.generation.after),
            disks_cfg_diff.disks.added.values().map(|z| *z),
        ))
    }

    /// Iterate over all removed disks on a sled
    pub fn removed_disks(
        &self,
        sled_id: &SledUuid,
    ) -> Option<DiffPhysicalDisksDetails> {
        // First check if the sled is removed
        if let Some(&sled_cfg) = self.diff.sleds.removed.get(sled_id) {
            if sled_cfg.disks_config.disks.is_empty() {
                return None;
            }
            return Some(DiffPhysicalDisksDetails::new(
                Some(sled_cfg.disks_config.generation),
                None,
                sled_cfg.disks_config.disks.iter(),
            ));
        }

        // Then check if the sled is modified and there are any removed disks
        let disks_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.disks_config;
        if disks_cfg_diff.disks.removed.is_empty() {
            return None;
        }
        Some(DiffPhysicalDisksDetails::new(
            Some(*disks_cfg_diff.generation.before),
            Some(*disks_cfg_diff.generation.after),
            disks_cfg_diff.disks.removed.values().map(|z| *z),
        ))
    }

    /// Iterate over all unchanged disks on a sled
    pub fn unchanged_disks(
        &self,
        sled_id: &SledUuid,
    ) -> Option<DiffPhysicalDisksDetails> {
        // First check if the sled is unchanged
        if let Some(sled_cfg) = self.diff.sleds.get_unchanged(sled_id) {
            if sled_cfg.disks_config.disks.is_empty() {
                return None;
            }
            return Some(DiffPhysicalDisksDetails::new(
                None,
                Some(sled_cfg.disks_config.generation),
                sled_cfg.disks_config.disks.iter(),
            ));
        }

        // Then check if the sled is modified and there are any unchanged disks
        let disks_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.disks_config;
        let mut unchanged_disks =
            disks_cfg_diff.disks.unchanged_values().peekable();
        if unchanged_disks.peek().is_none() {
            return None;
        }
        Some(DiffPhysicalDisksDetails::new(
            Some(*disks_cfg_diff.generation.before),
            Some(*disks_cfg_diff.generation.after),
            unchanged_disks,
        ))
    }

    /// Iterate over all modified disks on a sled
    pub fn modified_disks(
        &'a self,
        sled_id: &SledUuid,
    ) -> Option<(BpDiffPhysicalDisksModified<'a>, BpDiffPhysicalDiskErrors)>
    {
        // Check if the sled is modified and there are any modified disks
        let disks_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.disks_config;
        let mut modified_disks =
            disks_cfg_diff.disks.modified_values_diff().peekable();
        if modified_disks.peek().is_none() {
            return None;
        }
        Some(BpDiffPhysicalDisksModified::new(
            *disks_cfg_diff.generation.before,
            *disks_cfg_diff.generation.after,
            modified_disks,
        ))
    }

    /// Iterate over all added datasets on a sled
    pub fn added_datasets(
        &self,
        sled_id: &SledUuid,
    ) -> Option<DiffDatasetsDetails> {
        // First check if the sled is added
        if let Some(&sled_cfg) = self.diff.sleds.added.get(sled_id) {
            if sled_cfg.datasets_config.datasets.is_empty() {
                return None;
            }
            return Some(DiffDatasetsDetails::new(
                None,
                Some(sled_cfg.datasets_config.generation),
                sled_cfg.datasets_config.datasets.iter(),
            ));
        }

        // Then check if the sled is modified and there are any added datasets
        let datasets_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.datasets_config;
        if datasets_cfg_diff.datasets.added.is_empty() {
            return None;
        }
        Some(DiffDatasetsDetails::new(
            Some(*datasets_cfg_diff.generation.before),
            Some(*datasets_cfg_diff.generation.after),
            datasets_cfg_diff.datasets.added.values().map(|z| *z),
        ))
    }

    /// Iterate over all removed datasets on a sled
    pub fn removed_datasets(
        &self,
        sled_id: &SledUuid,
    ) -> Option<DiffDatasetsDetails> {
        // First check if the sled is removed
        if let Some(&sled_cfg) = self.diff.sleds.removed.get(sled_id) {
            if sled_cfg.datasets_config.datasets.is_empty() {
                return None;
            }
            return Some(DiffDatasetsDetails::new(
                Some(sled_cfg.datasets_config.generation),
                None,
                sled_cfg.datasets_config.datasets.iter(),
            ));
        }

        // Then check if the sled is modified and there are any removed datasets
        let datasets_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.datasets_config;
        if datasets_cfg_diff.datasets.removed.is_empty() {
            return None;
        }
        Some(DiffDatasetsDetails::new(
            Some(*datasets_cfg_diff.generation.before),
            Some(*datasets_cfg_diff.generation.after),
            datasets_cfg_diff.datasets.removed.values().map(|z| *z),
        ))
    }

    /// Iterate over all unchanged datasets on a sled
    pub fn unchanged_datasets(
        &self,
        sled_id: &SledUuid,
    ) -> Option<DiffDatasetsDetails> {
        // First check if the sled is unchanged
        if let Some(sled_cfg) = self.diff.sleds.get_unchanged(sled_id) {
            if sled_cfg.datasets_config.datasets.is_empty() {
                return None;
            }
            return Some(DiffDatasetsDetails::new(
                None,
                Some(sled_cfg.datasets_config.generation),
                sled_cfg.datasets_config.datasets.iter(),
            ));
        }

        // Then check if the sled is modified and there are any unchanged datasets
        let datasets_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.datasets_config;
        let mut unchanged_datasets =
            datasets_cfg_diff.datasets.unchanged_values().peekable();
        if unchanged_datasets.peek().is_none() {
            return None;
        }
        Some(DiffDatasetsDetails::new(
            Some(*datasets_cfg_diff.generation.before),
            Some(*datasets_cfg_diff.generation.after),
            unchanged_datasets,
        ))
    }

    /// Iterate over all modified datasets on a sled
    pub fn modified_datasets(
        &'a self,
        sled_id: &SledUuid,
    ) -> Option<(BpDiffDatasetsModified, BpDiffDatasetErrors)> {
        // Check if the sled is modified and there are any modified datasets
        let datasets_cfg_diff =
            &self.modified_sleds_diff.get(sled_id)?.datasets_config;
        let mut modified_datasets =
            datasets_cfg_diff.datasets.modified_values_diff().peekable();
        if modified_datasets.peek().is_none() {
            return None;
        }
        Some(BpDiffDatasetsModified::new(
            *datasets_cfg_diff.generation.before,
            *datasets_cfg_diff.generation.after,
            modified_datasets,
        ))
    }
}

/// Diffs for omicron zones on a given sled with a given `BpDiffState`
#[derive(Debug)]
pub struct BpDiffZoneDetails {
    pub generation_before: Option<Generation>,
    pub generation_after: Option<Generation>,
    pub zones: Vec<BlueprintZoneConfig>,
}

impl BpDiffZoneDetails {
    pub fn new<'a>(
        generation_before: Option<Generation>,
        generation_after: Option<Generation>,
        zones_iter: impl Iterator<Item = &'a BlueprintZoneConfig>,
    ) -> Self {
        let mut zones: Vec<_> = zones_iter.cloned().collect();
        zones.sort_unstable_by_key(zone_sort_key);
        BpDiffZoneDetails { generation_before, generation_after, zones }
    }
}

impl BpTableData for BpDiffZoneDetails {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Diff {
            before: self.generation_before,
            after: self.generation_after,
        }
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.zones.iter().map(move |zone| {
            BpTableRow::from_strings(
                state,
                vec![
                    zone.kind().report_str().to_string(),
                    zone.id().to_string(),
                    zone.disposition.to_string(),
                    zone.underlay_ip().to_string(),
                ],
            )
        })
    }
}

/// A modified omicron zone
///
/// A zone is considered modified if its `disposition` changes. All
/// modifications to other fields are considered errors, and will be recorded
/// as such.
#[derive(Debug)]
pub struct ModifiedZone {
    pub prior_disposition: BlueprintZoneDisposition,
    pub zone: BlueprintZoneConfig,
}

impl ZoneSortKey for ModifiedZone {
    fn kind(&self) -> ZoneKind {
        self.zone.kind()
    }

    fn id(&self) -> OmicronZoneUuid {
        self.zone.id()
    }
}

impl ModifiedZone {
    pub fn from_diff(
        diff: &BlueprintZoneConfigDiff,
    ) -> Result<ModifiedZone, BpDiffZoneError> {
        // Do we have any errors? If so, create a "reason" string.
        let mut reason = String::new();
        // These first two checks are only for backwards compatibility. They are
        // all included in the zone_type comparison below.
        if diff.zone_type.before.kind() != diff.zone_type.after.kind() {
            let msg = format!(
                "mismatched zone kind: before: {}, after: {}\n",
                diff.zone_type.before.kind().report_str(),
                diff.zone_type.after.kind().report_str(),
            );
            reason.push_str(&msg);
        }
        if diff.zone_type.before.underlay_ip()
            != diff.zone_type.after.underlay_ip()
        {
            let msg = format!(
                "mismatched underlay IP: before: {}, after: {}\n",
                diff.zone_type.before.underlay_ip(),
                diff.zone_type.after.underlay_ip()
            );
            reason.push_str(&msg);
        }
        if diff.zone_type.before != diff.zone_type.after {
            let msg = format!(
                "mismatched zone type: after: {:#?}\n",
                diff.zone_type.after
            );
            reason.push_str(&msg);
        }
        if reason.is_empty() {
            Ok(ModifiedZone {
                prior_disposition: *diff.disposition.before,
                zone: BlueprintZoneConfig {
                    disposition: *diff.disposition.after,
                    id: *diff.id.after,
                    filesystem_pool: diff.filesystem_pool.after.cloned(),
                    zone_type: diff.zone_type.after.clone(),
                },
            })
        } else {
            Err(BpDiffZoneError {
                zone_before_id: *diff.id.before,
                zone_after_id: *diff.id.after,
                reason,
            })
        }
    }
}

/// Details of modified zones on a given sled
#[derive(Debug)]
pub struct BpDiffZonesModified {
    pub generation_before: Generation,
    pub generation_after: Generation,
    pub zones: Vec<ModifiedZone>,
}

impl BpDiffZonesModified {
    pub fn new<'a>(
        generation_before: Generation,
        generation_after: Generation,
        zone_diffs: impl Iterator<Item = BlueprintZoneConfigDiff<'a>>,
    ) -> (BpDiffZonesModified, BpDiffZoneErrors) {
        let mut zones = vec![];
        let mut errors = vec![];
        for diff in zone_diffs {
            match ModifiedZone::from_diff(&diff) {
                Ok(modified_zone) => zones.push(modified_zone),
                Err(error) => errors.push(error),
            }
        }
        zones.sort_unstable_by_key(zone_sort_key);
        (
            BpDiffZonesModified { generation_before, generation_after, zones },
            BpDiffZoneErrors { generation_before, generation_after, errors },
        )
    }
}

impl BpTableData for BpDiffZonesModified {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Diff {
            before: Some(self.generation_before),
            after: Some(self.generation_after),
        }
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.zones.iter().map(move |zone| {
            BpTableRow::new(
                state,
                vec![
                    BpTableColumn::value(
                        zone.zone.kind().report_str().to_string(),
                    ),
                    BpTableColumn::value(zone.zone.id().to_string()),
                    BpTableColumn::diff(
                        zone.prior_disposition.to_string(),
                        zone.zone.disposition.to_string(),
                    ),
                    BpTableColumn::value(zone.zone.underlay_ip().to_string()),
                ],
            )
        })
    }
}

/// Errors arising from illegally modified zone fields
#[derive(Debug)]
pub struct BpDiffZoneErrors {
    pub generation_before: Generation,
    pub generation_after: Generation,
    pub errors: Vec<BpDiffZoneError>,
}

#[derive(Debug)]
pub struct BpDiffZoneError {
    pub zone_before_id: OmicronZoneUuid,
    pub zone_after_id: OmicronZoneUuid,
    pub reason: String,
}

/// All known zones across all known sleds, their various states, and errors
#[derive(Debug, Default)]
pub struct BpDiffZones {
    pub added: BTreeMap<SledUuid, BpDiffZoneDetails>,
    pub removed: BTreeMap<SledUuid, BpDiffZoneDetails>,
    pub unchanged: BTreeMap<SledUuid, BpDiffZoneDetails>,
    pub modified: BTreeMap<SledUuid, BpDiffZonesModified>,
    pub errors: BTreeMap<SledUuid, BpDiffZoneErrors>,
}

impl BpDiffZones {
    /// Convert from our diff summary to our display compatibility layer
    /// from the prior version of code.
    pub fn from_diff_summary<'a>(
        summary: &'a BlueprintDiffSummary<'a>,
    ) -> Self {
        let mut diffs = BpDiffZones::default();
        for sled_id in &summary.all_sleds {
            if let Some(added) = summary.added_zones(sled_id) {
                diffs.added.insert(*sled_id, added);
            }
            if let Some(removed) = summary.removed_zones(sled_id) {
                diffs.removed.insert(*sled_id, removed);
            }
            if let Some(unchanged) = summary.unchanged_zones(sled_id) {
                diffs.unchanged.insert(*sled_id, unchanged);
            }
            if let Some((modified, errors)) = summary.modified_zones(sled_id) {
                diffs.modified.insert(*sled_id, modified);
                if !errors.errors.is_empty() {
                    diffs.errors.insert(*sled_id, errors);
                }
            }
        }
        diffs
    }

    /// Return a [`BpTable`] for the given `sled_id`
    ///
    /// We collate all the data from each category to produce a single table.
    /// The order is:
    ///
    /// 1. Unchanged
    /// 2. Removed
    /// 3. Modified
    /// 4. Added
    ///
    /// The idea behind the order is to (a) group all changes together
    /// and (b) put changes towards the bottom, so people have to scroll
    /// back less.
    ///
    /// Errors are printed in a more freeform manner after the table is
    /// displayed.
    pub fn to_bp_sled_subtable(&self, sled_id: &SledUuid) -> Option<BpTable> {
        let mut generation = BpGeneration::Diff { before: None, after: None };
        let mut rows = vec![];
        if let Some(diff) = self.unchanged.get(sled_id) {
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Unchanged));
        }
        if let Some(diff) = self.removed.get(sled_id) {
            // Generations never vary for the same sled, so this is harmless
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Removed));
        }

        if let Some(diff) = self.modified.get(sled_id) {
            // Generations never vary for the same sled, so this is harmless
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Modified));
        }

        if let Some(diff) = self.added.get(sled_id) {
            // Generations never vary for the same sled, so this is harmless
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Added));
        }

        if rows.is_empty() {
            None
        } else {
            Some(BpTable::new(BpOmicronZonesTableSchema {}, generation, rows))
        }
    }
}

#[derive(Debug)]
pub struct DiffPhysicalDisksDetails {
    // Newly added sleds don't have "before" disks
    pub before_generation: Option<Generation>,

    // Disks that are removed don't have "after" generation numbers
    pub after_generation: Option<Generation>,

    // Disks added, removed, or unmodified
    pub disks: Vec<BlueprintPhysicalDiskConfig>,
}

impl DiffPhysicalDisksDetails {
    pub fn new<'a>(
        before_generation: Option<Generation>,
        after_generation: Option<Generation>,
        disks_iter: impl Iterator<Item = &'a BlueprintPhysicalDiskConfig>,
    ) -> Self {
        let mut disks: Vec<_> = disks_iter.cloned().collect();
        disks.sort_unstable_by_key(|d| d.identity.clone());
        DiffPhysicalDisksDetails { before_generation, after_generation, disks }
    }
}

impl BpTableData for DiffPhysicalDisksDetails {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Diff {
            before: self.before_generation,
            after: self.after_generation,
        }
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.disks.iter().map(move |d| {
            BpTableRow::from_strings(
                state,
                vec![
                    d.identity.vendor.clone(),
                    d.identity.model.clone(),
                    d.identity.serial.clone(),
                    d.disposition.to_string(),
                ],
            )
        })
    }
}

/// Errors arising from illegally modified physical disk fields
#[derive(Debug)]
pub struct BpDiffPhysicalDiskErrors {
    pub generation_before: Generation,
    pub generation_after: Generation,
    pub errors: Vec<BpDiffPhysicalDiskError>,
}

#[derive(Debug)]
pub struct BpDiffPhysicalDiskError {
    pub disk_id: PhysicalDiskUuid,
    pub reason: String,
}

/// This is just an error parsed diff (Parse don't validate)
///
/// We still just want the underlying diff representation for printing
#[derive(Debug)]
pub struct ModifiedPhysicalDisk<'a> {
    pub diff: BlueprintPhysicalDiskConfigDiff<'a>,
}

impl<'a> ModifiedPhysicalDisk<'a> {
    pub fn from_diff(
        diff: BlueprintPhysicalDiskConfigDiff<'a>,
    ) -> Result<Self, BpDiffPhysicalDiskError> {
        // Do we have any errors? If so, create a "reason" string.
        let mut reason = String::new();

        let BlueprintPhysicalDiskConfigDiff {
            disposition: _,
            identity,
            id,
            pool_id,
        } = diff;

        // If we're a "modified" disk, we must have the same ID before and
        // after. (Otherwise our "before" or "after" should've been recorded as
        // removed/added.)
        debug_assert_eq!(id.before, id.after);

        if identity.is_modified() {
            writeln!(
                &mut reason,
                "mismatched identity: before: {:?}, after: {:?}",
                identity.before, identity.after
            )
            .expect("write to String is infallible");
        }

        if pool_id.is_modified() {
            writeln!(
                &mut reason,
                "mismatched zpool: before: {}, after: {}",
                pool_id.before, pool_id.after
            )
            .expect("write to String is infallible");
        }

        if reason.is_empty() {
            Ok(ModifiedPhysicalDisk { diff })
        } else {
            Err(BpDiffPhysicalDiskError { disk_id: *id.before, reason })
        }
    }
}

#[derive(Debug)]
pub struct BpDiffPhysicalDisksModified<'a> {
    pub generation_before: Generation,
    pub generation_after: Generation,
    pub disks: Vec<ModifiedPhysicalDisk<'a>>,
}

impl<'a> BpDiffPhysicalDisksModified<'a> {
    pub fn new(
        generation_before: Generation,
        generation_after: Generation,
        disk_diffs: impl Iterator<Item = BlueprintPhysicalDiskConfigDiff<'a>>,
    ) -> (BpDiffPhysicalDisksModified<'a>, BpDiffPhysicalDiskErrors) {
        let mut disks = vec![];
        let mut errors = vec![];
        for diff in disk_diffs {
            match ModifiedPhysicalDisk::from_diff(diff) {
                Ok(modified_disk) => disks.push(modified_disk),
                Err(error) => errors.push(error),
            }
        }
        disks.sort_unstable_by_key(|d| d.diff.identity.before.clone());
        (
            BpDiffPhysicalDisksModified {
                generation_before,
                generation_after,
                disks,
            },
            BpDiffPhysicalDiskErrors {
                generation_before,
                generation_after,
                errors,
            },
        )
    }
}

impl BpTableData for BpDiffPhysicalDisksModified<'_> {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Diff {
            before: Some(self.generation_before),
            after: Some(self.generation_after),
        }
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.disks.iter().map(move |disk| {
            let identity = disk.diff.identity.before;
            let disposition = disk.diff.disposition;
            BpTableRow::new(
                state,
                vec![
                    BpTableColumn::value(identity.vendor.clone()),
                    BpTableColumn::value(identity.model.clone()),
                    BpTableColumn::value(identity.serial.clone()),
                    BpTableColumn::new(
                        disposition.before.to_string(),
                        disposition.after.to_string(),
                    ),
                ],
            )
        })
    }
}

#[derive(Debug, Default)]
pub struct BpDiffPhysicalDisks<'a> {
    pub added: BTreeMap<SledUuid, DiffPhysicalDisksDetails>,
    pub removed: BTreeMap<SledUuid, DiffPhysicalDisksDetails>,
    pub unchanged: BTreeMap<SledUuid, DiffPhysicalDisksDetails>,
    pub modified: BTreeMap<SledUuid, BpDiffPhysicalDisksModified<'a>>,
    pub errors: BTreeMap<SledUuid, BpDiffPhysicalDiskErrors>,
}

impl<'a> BpDiffPhysicalDisks<'a> {
    pub fn from_diff_summary(summary: &'a BlueprintDiffSummary<'a>) -> Self {
        let mut diffs = BpDiffPhysicalDisks::default();
        for sled_id in &summary.all_sleds {
            if let Some(added) = summary.added_disks(sled_id) {
                diffs.added.insert(*sled_id, added);
            }
            if let Some(removed) = summary.removed_disks(sled_id) {
                diffs.removed.insert(*sled_id, removed);
            }
            if let Some(unchanged) = summary.unchanged_disks(sled_id) {
                diffs.unchanged.insert(*sled_id, unchanged);
            }
            if let Some((modified, errors)) = summary.modified_disks(sled_id) {
                diffs.modified.insert(*sled_id, modified);
                if !errors.errors.is_empty() {
                    diffs.errors.insert(*sled_id, errors);
                }
            }
        }
        diffs
    }

    /// Return a [`BpTable`] for the given `sled_id`
    pub fn to_bp_sled_subtable(&self, sled_id: &SledUuid) -> Option<BpTable> {
        let mut generation = BpGeneration::Diff { before: None, after: None };
        let mut rows = vec![];
        if let Some(diff) = self.unchanged.get(sled_id) {
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Unchanged));
        }
        if let Some(diff) = self.removed.get(sled_id) {
            // Generations never vary for the same sled, so this is harmless
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Removed));
        }

        if let Some(diff) = self.modified.get(sled_id) {
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Modified));
        }

        if let Some(diff) = self.added.get(sled_id) {
            // Generations never vary for the same sled, so this is harmless
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Added));
        }

        if rows.is_empty() {
            None
        } else {
            Some(BpTable::new(BpPhysicalDisksTableSchema {}, generation, rows))
        }
    }
}

#[derive(Debug)]
pub struct DiffDatasetsDetails {
    // Datasets that come from disks on newly added sleds don't have "before"
    // generation numbers
    pub before_generation: Option<Generation>,

    // Datasets that are removed don't have "after" generation numbers
    pub after_generation: Option<Generation>,

    // Datasets added, removed, modified, or unmodified
    pub datasets: BTreeMap<CollectionDatasetIdentifier, BlueprintDatasetConfig>,
}

impl DiffDatasetsDetails {
    pub fn new<'a>(
        before_generation: Option<Generation>,
        after_generation: Option<Generation>,
        datasets_iter: impl Iterator<Item = &'a BlueprintDatasetConfig>,
    ) -> Self {
        DiffDatasetsDetails {
            before_generation,
            after_generation,
            datasets: datasets_iter
                .map(|dataset_config| {
                    (dataset_config.into(), dataset_config.clone())
                })
                .collect(),
        }
    }
}

impl BpTableData for DiffDatasetsDetails {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Diff {
            before: self.before_generation,
            after: self.after_generation,
        }
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        // `self.datasets` is naturally ordered by ID, but that doesn't play
        // well with expectorate-based tests: We end up sorted by (random)
        // UUIDs. We redact the UUIDs, but that still results in test-to-test
        // variance in the _order_ of the rows. We can work around this for now
        // by sorting by dataset kind: after UUID redaction, that produces
        // a stable table ordering for datasets.
        let mut rows = self.datasets.values().collect::<Vec<_>>();
        rows.sort_unstable_by_key(|d| (&d.kind, &d.pool));
        rows.into_iter().map(move |dataset| {
            BpTableRow::from_strings(state, dataset.as_strings())
        })
    }
}

/// Properties of a dataset that may change from one blueprint to another.
///
/// Other properties of a dataset (e.g., its `pool`, `kind`, and `address`) are
/// not expected to change, and we'll record an error if they do.
#[derive(Debug)]
pub struct ModifiableDatasetProperties {
    pub disposition: BlueprintDatasetDisposition,
    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: CompressionAlgorithm,
}

/// Errors arising from illegally modified dataset fields
#[derive(Debug)]
pub struct BpDiffDatasetErrors {
    pub generation_before: Generation,
    pub generation_after: Generation,
    pub errors: Vec<BpDiffDatasetError>,
}

#[derive(Debug)]
pub struct BpDiffDatasetError {
    pub dataset_id: DatasetUuid,
    pub reason: String,
}

#[derive(Debug)]
pub struct ModifiedDataset {
    pub prior_properties: ModifiableDatasetProperties,
    pub dataset: BlueprintDatasetConfig,
}

impl ModifiedDataset {
    pub fn from_diff(
        diff: &BlueprintDatasetConfigDiff,
    ) -> Result<Self, BpDiffDatasetError> {
        // Do we have any errors? If so, create a "reason" string.
        let mut reason = String::new();
        let BlueprintDatasetConfigDiff {
            disposition,
            id,
            pool,
            kind,
            address,
            quota,
            reservation,
            compression,
        } = diff;

        // If we're a "modified" dataset, we must have the same ID before and
        // after. (Otherwise our "before" or "after" should've been recorded as
        // removed/added.)
        debug_assert_eq!(id.before, id.after);

        let prior_properties = ModifiableDatasetProperties {
            disposition: *disposition.before,
            quota: quota.before.copied(),
            reservation: reservation.before.copied(),
            compression: *compression.before,
        };
        if pool.before != pool.after {
            writeln!(
                &mut reason,
                "mismatched zpool: before: {}, after: {}",
                pool.before, pool.after
            )
            .expect("write to String is infallible");
        }
        if kind.before != kind.after {
            writeln!(
                &mut reason,
                "mismatched kind: before: {}, after: {}",
                kind.before, kind.after
            )
            .expect("write to String is infallible");
        }
        if address.before != address.after {
            writeln!(
                &mut reason,
                "mismatched address: before: {:?}, after: {:?}",
                address.before, address.after
            )
            .expect("write to String is infallible");
        }

        if reason.is_empty() {
            Ok(Self {
                prior_properties,
                dataset: BlueprintDatasetConfig {
                    disposition: *disposition.after,
                    id: *id.after,
                    pool: pool.after.clone(),
                    kind: kind.after.clone(),
                    address: address.after.copied(),
                    quota: quota.after.copied(),
                    reservation: reservation.after.copied(),
                    compression: *compression.after,
                },
            })
        } else {
            Err(BpDiffDatasetError { dataset_id: *id.after, reason })
        }
    }
}

#[derive(Debug)]
pub struct BpDiffDatasetsModified {
    pub generation_before: Generation,
    pub generation_after: Generation,
    pub datasets: Vec<ModifiedDataset>,
}

impl BpDiffDatasetsModified {
    pub fn new<'a>(
        generation_before: Generation,
        generation_after: Generation,
        dataset_diffs: impl Iterator<Item = BlueprintDatasetConfigDiff<'a>>,
    ) -> (BpDiffDatasetsModified, BpDiffDatasetErrors) {
        let mut datasets = vec![];
        let mut errors = vec![];
        for diff in dataset_diffs {
            match ModifiedDataset::from_diff(&diff) {
                Ok(modified_zone) => datasets.push(modified_zone),
                Err(error) => errors.push(error),
            }
        }
        datasets.sort_unstable_by_key(|d| {
            (d.dataset.kind.clone(), d.dataset.pool.clone())
        });
        (
            BpDiffDatasetsModified {
                generation_before,
                generation_after,
                datasets,
            },
            BpDiffDatasetErrors { generation_before, generation_after, errors },
        )
    }
}

impl BpTableData for BpDiffDatasetsModified {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Diff {
            before: Some(self.generation_before),
            after: Some(self.generation_after),
        }
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.datasets.iter().map(move |dataset| {
            let ModifiableDatasetProperties {
                disposition: before_disposition,
                quota: before_quota,
                reservation: before_reservation,
                compression: before_compression,
            } = &dataset.prior_properties;

            BpTableRow::new(
                state,
                vec![
                    BpTableColumn::value(
                        DatasetName::new(
                            dataset.dataset.pool.clone(),
                            dataset.dataset.kind.clone(),
                        )
                        .full_name(),
                    ),
                    BpTableColumn::value(dataset.dataset.id.to_string()),
                    BpTableColumn::new(
                        before_disposition.to_string(),
                        dataset.dataset.disposition.to_string(),
                    ),
                    BpTableColumn::new(
                        unwrap_or_none(&before_quota),
                        unwrap_or_none(&dataset.dataset.quota),
                    ),
                    BpTableColumn::new(
                        unwrap_or_none(&before_reservation),
                        unwrap_or_none(&dataset.dataset.reservation),
                    ),
                    BpTableColumn::new(
                        before_compression.to_string(),
                        dataset.dataset.compression.to_string(),
                    ),
                ],
            )
        })
    }
}

#[derive(Debug, Default)]
pub struct BpDiffDatasets {
    pub added: BTreeMap<SledUuid, DiffDatasetsDetails>,
    pub removed: BTreeMap<SledUuid, DiffDatasetsDetails>,
    pub modified: BTreeMap<SledUuid, BpDiffDatasetsModified>,
    pub unchanged: BTreeMap<SledUuid, DiffDatasetsDetails>,
    pub errors: BTreeMap<SledUuid, BpDiffDatasetErrors>,
}

impl BpDiffDatasets {
    pub fn from_diff_summary<'a>(
        summary: &'a BlueprintDiffSummary<'a>,
    ) -> Self {
        let mut diffs = BpDiffDatasets::default();
        for sled_id in &summary.all_sleds {
            if let Some(added) = summary.added_datasets(sled_id) {
                diffs.added.insert(*sled_id, added);
            }
            if let Some(removed) = summary.removed_datasets(sled_id) {
                diffs.removed.insert(*sled_id, removed);
            }
            if let Some(unchanged) = summary.unchanged_datasets(sled_id) {
                diffs.unchanged.insert(*sled_id, unchanged);
            }
            if let Some((modified, errors)) = summary.modified_datasets(sled_id)
            {
                diffs.modified.insert(*sled_id, modified);
                if !errors.errors.is_empty() {
                    diffs.errors.insert(*sled_id, errors);
                }
            }
        }
        diffs
    }
    /// Return a [`BpTable`] for the given `sled_id`
    pub fn to_bp_sled_subtable(&self, sled_id: &SledUuid) -> Option<BpTable> {
        let mut generation = BpGeneration::Diff { before: None, after: None };
        let mut rows = vec![];
        if let Some(diff) = self.unchanged.get(sled_id) {
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Unchanged));
        }
        if let Some(diff) = self.removed.get(sled_id) {
            // Generations never vary for the same sled, so this is harmless
            //
            // (Same below, where we overwrite the "generation")
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Removed));
        }
        if let Some(diff) = self.modified.get(sled_id) {
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Modified));
        }
        if let Some(diff) = self.added.get(sled_id) {
            generation = diff.bp_generation();
            rows.extend(diff.rows(BpDiffState::Added));
        }

        if rows.is_empty() {
            None
        } else {
            Some(BpTable::new(BpDatasetsTableSchema {}, generation, rows))
        }
    }
}

/// A printable representation of `ClickhouseClusterConfig` diff tables where
/// there is only a single known blueprint with no before or after collection or
/// bluerpint to compare to.
pub struct ClickhouseClusterConfigDiffTablesForSingleBlueprint {
    pub metadata: KvListWithHeading,
    pub keepers: BpTable,
    pub servers: BpTable,
}

impl ClickhouseClusterConfigDiffTablesForSingleBlueprint {
    pub fn new(
        diff_state: BpDiffState,
        config: &ClickhouseClusterConfig,
    ) -> Self {
        let rows: Vec<_> = [
            (GENERATION, config.generation.to_string()),
            (
                CLICKHOUSE_MAX_USED_SERVER_ID,
                config.max_used_server_id.to_string(),
            ),
            (
                CLICKHOUSE_MAX_USED_KEEPER_ID,
                config.max_used_keeper_id.to_string(),
            ),
            (CLICKHOUSE_CLUSTER_NAME, config.cluster_name.clone()),
            (CLICKHOUSE_CLUSTER_SECRET, config.cluster_secret.clone()),
            (
                CLICKHOUSE_HIGHEST_SEEN_KEEPER_LEADER_COMMITTED_LOG_INDEX,
                config
                    .highest_seen_keeper_leader_committed_log_index
                    .to_string(),
            ),
        ]
        .into_iter()
        .map(|(key, val)| KvPair::new(diff_state, key, val))
        .collect();

        let metadata =
            KvListWithHeading::new(CLICKHOUSE_CLUSTER_CONFIG_HEADING, rows);

        let keepers = BpTable::new(
            BpClickhouseKeepersTableSchema {},
            BpGeneration::Value(config.generation),
            (config.generation, &config.keepers).rows(diff_state).collect(),
        );
        let servers = BpTable::new(
            BpClickhouseServersTableSchema {},
            BpGeneration::Value(config.generation),
            (config.generation, &config.servers).rows(diff_state).collect(),
        );

        ClickhouseClusterConfigDiffTablesForSingleBlueprint {
            metadata,
            keepers,
            servers,
        }
    }
}

impl From<ClickhouseClusterConfigDiffTablesForSingleBlueprint>
    for ClickhouseClusterConfigDiffTables
{
    fn from(
        value: ClickhouseClusterConfigDiffTablesForSingleBlueprint,
    ) -> Self {
        ClickhouseClusterConfigDiffTables {
            metadata: value.metadata,
            keepers: value.keepers,
            servers: Some(value.servers),
        }
    }
}

/// A printable representation of the difference between two
/// `ClickhouseClusterConfig` tables or a `ClickhouseClusterConfig` table and
/// its inventory representation.
pub struct ClickhouseClusterConfigDiffTables {
    pub metadata: KvListWithHeading,
    pub keepers: BpTable,
    pub servers: Option<BpTable>,
}

impl ClickhouseClusterConfigDiffTables {
    pub fn diff_collection_and_blueprint(
        before: &clickhouse_admin_types::ClickhouseKeeperClusterMembership,
        after: &ClickhouseClusterConfig,
    ) -> Self {
        let leader_committed_log_index = if before.leader_committed_log_index
            == after.highest_seen_keeper_leader_committed_log_index
        {
            KvPair::new(
                BpDiffState::Unchanged,
                CLICKHOUSE_HIGHEST_SEEN_KEEPER_LEADER_COMMITTED_LOG_INDEX,
                linear_table_unchanged(
                    &after.highest_seen_keeper_leader_committed_log_index,
                ),
            )
        } else {
            KvPair::new(
                BpDiffState::Modified,
                CLICKHOUSE_HIGHEST_SEEN_KEEPER_LEADER_COMMITTED_LOG_INDEX,
                linear_table_modified(
                    &before.leader_committed_log_index,
                    &after.highest_seen_keeper_leader_committed_log_index,
                ),
            )
        };
        let metadata = KvListWithHeading::new(
            CLICKHOUSE_CLUSTER_CONFIG_HEADING,
            vec![
                KvPair::new(
                    BpDiffState::Added,
                    GENERATION,
                    linear_table_modified(
                        &NOT_PRESENT_IN_COLLECTION_PARENS,
                        &after.generation,
                    ),
                ),
                KvPair::new(
                    BpDiffState::Added,
                    CLICKHOUSE_MAX_USED_SERVER_ID,
                    linear_table_modified(
                        &NOT_PRESENT_IN_COLLECTION_PARENS,
                        &after.max_used_server_id,
                    ),
                ),
                KvPair::new(
                    BpDiffState::Added,
                    CLICKHOUSE_MAX_USED_KEEPER_ID,
                    linear_table_modified(
                        &NOT_PRESENT_IN_COLLECTION_PARENS,
                        &after.max_used_keeper_id,
                    ),
                ),
                KvPair::new(
                    BpDiffState::Added,
                    CLICKHOUSE_CLUSTER_NAME,
                    linear_table_modified(
                        &NOT_PRESENT_IN_COLLECTION_PARENS,
                        &after.cluster_name,
                    ),
                ),
                KvPair::new(
                    BpDiffState::Added,
                    CLICKHOUSE_CLUSTER_SECRET,
                    linear_table_modified(
                        &NOT_PRESENT_IN_COLLECTION_PARENS,
                        &after.cluster_secret,
                    ),
                ),
                leader_committed_log_index,
            ],
        );

        // Build up our keeper table
        let mut keeper_rows = vec![];
        for (zone_id, keeper_id) in &after.keepers {
            if before.raft_config.contains(keeper_id) {
                // Unchanged keepers
                keeper_rows.push(BpTableRow::new(
                    BpDiffState::Unchanged,
                    vec![
                        BpTableColumn::Value(zone_id.to_string()),
                        BpTableColumn::Value(keeper_id.to_string()),
                    ],
                ));
            } else {
                // Added keepers
                keeper_rows.push(BpTableRow::new(
                    BpDiffState::Added,
                    vec![
                        BpTableColumn::Value(zone_id.to_string()),
                        BpTableColumn::Value(keeper_id.to_string()),
                    ],
                ));
            }
        }

        let after_ids: BTreeSet<_> = after.keepers.values().clone().collect();
        for keeper_id in &before.raft_config {
            if !after_ids.contains(keeper_id) {
                // Removed keepers
                keeper_rows.push(BpTableRow::new(
                    BpDiffState::Removed,
                    vec![
                        BpTableColumn::Value(
                            NOT_PRESENT_IN_COLLECTION_PARENS.to_string(),
                        ),
                        BpTableColumn::Value(keeper_id.to_string()),
                    ],
                ));
            }
        }

        let keepers = BpTable::new(
            BpClickhouseKeepersTableSchema {},
            BpGeneration::Diff { before: None, after: Some(after.generation) },
            keeper_rows,
        );

        // Build up our server table
        let server_rows: Vec<BpTableRow> = after
            .servers
            .iter()
            .map(|(zone_id, server_id)| {
                BpTableRow::new(
                    BpDiffState::Added,
                    vec![
                        BpTableColumn::Value(zone_id.to_string()),
                        BpTableColumn::Value(server_id.to_string()),
                    ],
                )
            })
            .collect();

        let servers = Some(BpTable::new(
            BpClickhouseServersTableSchema {},
            BpGeneration::Diff { before: None, after: Some(after.generation) },
            server_rows,
        ));

        ClickhouseClusterConfigDiffTables { metadata, keepers, servers }
    }

    pub fn diff_blueprints(
        before: &ClickhouseClusterConfig,
        after: &ClickhouseClusterConfig,
    ) -> Self {
        macro_rules! diff_row {
            ($member:ident, $label:expr) => {
                if before.$member == after.$member {
                    KvPair::new(
                        BpDiffState::Unchanged,
                        $label,
                        linear_table_unchanged(&after.$member),
                    )
                } else {
                    KvPair::new(
                        BpDiffState::Modified,
                        $label,
                        linear_table_modified(&before.$member, &after.$member),
                    )
                }
            };
        }

        let metadata = KvListWithHeading::new(
            CLICKHOUSE_CLUSTER_CONFIG_HEADING,
            vec![
                diff_row!(generation, GENERATION),
                diff_row!(max_used_server_id, CLICKHOUSE_MAX_USED_SERVER_ID),
                diff_row!(max_used_keeper_id, CLICKHOUSE_MAX_USED_KEEPER_ID),
                diff_row!(cluster_name, CLICKHOUSE_CLUSTER_NAME),
                diff_row!(cluster_secret, CLICKHOUSE_CLUSTER_SECRET),
                diff_row!(
                    highest_seen_keeper_leader_committed_log_index,
                    CLICKHOUSE_HIGHEST_SEEN_KEEPER_LEADER_COMMITTED_LOG_INDEX
                ),
            ],
        );

        // Macro used to construct keeper and server tables
        macro_rules! diff_table_rows {
            ($rows:ident, $collection:ident) => {
                for (zone_id, id) in &after.$collection {
                    if before.$collection.contains_key(zone_id) {
                        // Unchanged
                        $rows.push(BpTableRow::new(
                            BpDiffState::Unchanged,
                            vec![
                                BpTableColumn::Value(zone_id.to_string()),
                                BpTableColumn::Value(id.to_string()),
                            ],
                        ));
                    } else {
                        // Added
                        $rows.push(BpTableRow::new(
                            BpDiffState::Added,
                            vec![
                                BpTableColumn::Value(zone_id.to_string()),
                                BpTableColumn::Value(id.to_string()),
                            ],
                        ));
                    }
                }

                for (zone_id, id) in &before.$collection {
                    if !after.$collection.contains_key(zone_id) {
                        // Removed
                        $rows.push(BpTableRow::new(
                            BpDiffState::Removed,
                            vec![
                                BpTableColumn::Value(zone_id.to_string()),
                                BpTableColumn::Value(id.to_string()),
                            ],
                        ));
                    }
                }
            };
        }

        // Construct our keeper table
        let mut keeper_rows = vec![];
        diff_table_rows!(keeper_rows, keepers);
        let keepers = BpTable::new(
            BpClickhouseKeepersTableSchema {},
            BpGeneration::Diff {
                before: Some(before.generation),
                after: Some(after.generation),
            },
            keeper_rows,
        );

        // Construct our server table
        let mut server_rows = vec![];
        diff_table_rows!(server_rows, servers);

        let servers = Some(BpTable::new(
            BpClickhouseServersTableSchema {},
            BpGeneration::Diff {
                before: Some(before.generation),
                after: Some(after.generation),
            },
            server_rows,
        ));

        ClickhouseClusterConfigDiffTables { metadata, keepers, servers }
    }

    /// We are diffing a `Collection` and `Blueprint` but  the latest blueprint
    /// does not have a ClickhouseClusterConfig.
    pub fn removed_from_collection(
        before: &clickhouse_admin_types::ClickhouseKeeperClusterMembership,
    ) -> Self {
        // There's only so much information in a collection. Show what we can.
        let metadata = KvListWithHeading::new(
            CLICKHOUSE_CLUSTER_CONFIG_HEADING,
            vec![KvPair::new(
                BpDiffState::Removed,
                CLICKHOUSE_HIGHEST_SEEN_KEEPER_LEADER_COMMITTED_LOG_INDEX,
                before.leader_committed_log_index.to_string(),
            )],
        );

        let keeper_rows: Vec<BpTableRow> = before
            .raft_config
            .iter()
            .map(|keeper_id| {
                BpTableRow::new(
                    BpDiffState::Removed,
                    vec![
                        BpTableColumn::Value(
                            NOT_PRESENT_IN_COLLECTION_PARENS.to_string(),
                        ),
                        BpTableColumn::Value(keeper_id.to_string()),
                    ],
                )
            })
            .collect();

        let keepers = BpTable::new(
            BpClickhouseKeepersTableSchema {},
            BpGeneration::unknown(),
            keeper_rows,
        );

        ClickhouseClusterConfigDiffTables { metadata, keepers, servers: None }
    }

    /// The "before" inventory collection or blueprint does not have a relevant
    /// keeper configuration.
    pub fn added_to_blueprint(after: &ClickhouseClusterConfig) -> Self {
        ClickhouseClusterConfigDiffTablesForSingleBlueprint::new(
            BpDiffState::Added,
            after,
        )
        .into()
    }

    /// We are diffing two `Blueprint`s, but The latest bluerprint does not have
    /// a `ClickhouseClusterConfig`.
    pub fn removed_from_blueprint(before: &ClickhouseClusterConfig) -> Self {
        ClickhouseClusterConfigDiffTablesForSingleBlueprint::new(
            BpDiffState::Removed,
            before,
        )
        .into()
    }
}

/// Wrapper to allow a [`BlueprintDiff`] to be displayed.
///
/// Returned by [`BlueprintDiffSummary::display()`].
#[derive(Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintDiffDisplay<'diff> {
    summary: &'diff BlueprintDiffSummary<'diff>,
    // These structures are intermediate structures that we generate displayable
    // tables from.
    before_meta: BlueprintMetadata,
    after_meta: BlueprintMetadata,
    zones: BpDiffZones,
    disks: BpDiffPhysicalDisks<'diff>,
    datasets: BpDiffDatasets,
}

impl<'diff> BlueprintDiffDisplay<'diff> {
    #[inline]
    fn new(summary: &'diff BlueprintDiffSummary<'diff>) -> Self {
        let before_meta = summary.before.metadata();
        let after_meta = summary.after.metadata();
        let zones = BpDiffZones::from_diff_summary(summary);
        let disks = BpDiffPhysicalDisks::from_diff_summary(summary);
        let datasets = BpDiffDatasets::from_diff_summary(summary);
        Self { summary, before_meta, after_meta, zones, disks, datasets }
    }

    pub fn make_metadata_diff_tables(
        &self,
    ) -> impl IntoIterator<Item = KvListWithHeading> {
        macro_rules! diff_row {
            ($member:ident, $label:expr) => {
                diff_row!($member, $label, std::convert::identity)
            };

            ($member:ident, $label:expr, $display:expr) => {
                if self.before_meta.$member == self.after_meta.$member {
                    KvPair::new(
                        BpDiffState::Unchanged,
                        $label,
                        linear_table_unchanged(&$display(
                            &self.after_meta.$member,
                        )),
                    )
                } else {
                    KvPair::new(
                        BpDiffState::Modified,
                        $label,
                        linear_table_modified(
                            &$display(&self.before_meta.$member),
                            &$display(&self.after_meta.$member),
                        ),
                    )
                }
            };
        }

        [
            KvListWithHeading::new(
                COCKROACHDB_HEADING,
                vec![
                    diff_row!(
                        cockroachdb_fingerprint,
                        COCKROACHDB_FINGERPRINT,
                        display_none_if_empty
                    ),
                    diff_row!(
                        cockroachdb_setting_preserve_downgrade,
                        COCKROACHDB_PRESERVE_DOWNGRADE,
                        display_optional_preserve_downgrade
                    ),
                ],
            ),
            KvListWithHeading::new(
                METADATA_HEADING,
                vec![
                    diff_row!(internal_dns_version, INTERNAL_DNS_VERSION),
                    diff_row!(external_dns_version, EXTERNAL_DNS_VERSION),
                ],
            ),
        ]
    }

    pub fn make_clickhouse_cluster_config_diff_tables(
        &self,
    ) -> Option<ClickhouseClusterConfigDiffTables> {
        match (
            &self.summary.diff.clickhouse_cluster_config.before,
            &self.summary.diff.clickhouse_cluster_config.after,
        ) {
            // Before blueprint + after blueprint
            (Some(before), Some(after)) => {
                Some(ClickhouseClusterConfigDiffTables::diff_blueprints(
                    before, after,
                ))
            }

            // Before blueprint only
            (Some(before), None) => {
                Some(ClickhouseClusterConfigDiffTables::removed_from_blueprint(
                    before,
                ))
            }

            // After blueprint only
            (None, Some(after)) => Some(
                ClickhouseClusterConfigDiffTables::added_to_blueprint(after),
            ),

            // No before or after
            (None, None) => None,
        }
    }

    /// Write out disk, dataset, and zone tables for a given `sled_id`
    fn write_tables(
        &self,
        f: &mut fmt::Formatter<'_>,
        sled_id: &SledUuid,
    ) -> fmt::Result {
        // Write the physical disks table if needed
        if let Some(table) = self.disks.to_bp_sled_subtable(sled_id) {
            writeln!(f, "{table}\n")?;
        }

        // Write the datasets table if it exists
        if let Some(table) = self.datasets.to_bp_sled_subtable(sled_id) {
            writeln!(f, "{table}\n")?;
        }

        // Write the zones table if it exists
        if let Some(table) = self.zones.to_bp_sled_subtable(sled_id) {
            writeln!(f, "{table}\n")?;
        }

        Ok(())
    }

    /// Helper methods to stringify sled states. These are separated by
    /// diff section because each section has different expectations for what
    /// before and after should be that can only be wrong if we have a bug
    /// constructing the diff.
    fn sled_state_unchanged(&self, sled_id: &SledUuid) -> String {
        self.summary
            .diff
            .sleds
            .get_unchanged(sled_id)
            .unwrap()
            .state
            .to_string()
    }
    fn sled_state_added(&self, sled_id: &SledUuid) -> String {
        let after = self.summary.diff.sleds.added.get(sled_id).unwrap().state;
        format!("{after}")
    }
    fn sled_state_removed(&self, sled_id: &SledUuid) -> String {
        let before =
            self.summary.diff.sleds.removed.get(sled_id).unwrap().state;
        format!("was {before}")
    }
    fn sled_state_modified(&self, sled_id: &SledUuid) -> String {
        let modified_sled =
            self.summary.diff.sleds.get_modified(sled_id).unwrap();
        let before = modified_sled.before.state;
        let after = modified_sled.after.state;
        if before != after {
            format!("{before} -> {after}")
        } else {
            format!("{before}")
        }
    }
}

impl fmt::Display for BlueprintDiffDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let summary = self.summary;
        let before_metadata = self.summary.before.metadata();
        let after_metadata = self.summary.after.metadata();

        writeln!(
            f,
            "from: blueprint {}\n\
             to:   blueprint {}\n",
            before_metadata.id, after_metadata.id
        )?;

        // Write out sled information
        //
        // The order is:
        //
        // 1. Unchanged
        // 2. Removed
        // 3. Modified
        // 4. Added
        // 5. Errors
        //
        // The idea behind the order is to (a) group all changes together
        // and (b) put changes towards the bottom, so people have to scroll
        // back less.
        //
        // We put errors at the bottom to ensure they are seen immediately.

        // Write out tables for unchanged sleds
        if !summary.sleds_unchanged.is_empty() {
            writeln!(f, " UNCHANGED SLEDS:\n")?;
            for sled_id in &summary.sleds_unchanged {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_unchanged(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out tables for removed sleds
        if !summary.sleds_removed.is_empty() {
            writeln!(f, " REMOVED SLEDS:\n")?;
            for sled_id in &summary.sleds_removed {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_removed(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out tables for modified sleds
        if !summary.sleds_modified.is_empty() {
            writeln!(f, " MODIFIED SLEDS:\n")?;
            for sled_id in &summary.sleds_modified {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_modified(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out tables for added sleds
        if !summary.sleds_added.is_empty() {
            writeln!(f, " ADDED SLEDS:\n")?;
            for sled_id in &summary.sleds_added {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_added(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out zone errors.
        if !self.zones.errors.is_empty() {
            writeln!(f, "ZONE ERRORS:")?;
            for (sled_id, errors) in &self.zones.errors {
                writeln!(f, "\n  sled {sled_id}\n")?;
                writeln!(
                    f,
                    "    zone diff errors: before gen {}, after gen {}\n",
                    errors.generation_before, errors.generation_after
                )?;

                for err in &errors.errors {
                    writeln!(f, "      zone id: {}", err.zone_before_id)?;
                    writeln!(f, "      reason: {}", err.reason)?;
                }
            }
        }

        // Write out disk errors.
        if !self.disks.errors.is_empty() {
            writeln!(f, "DISK ERRORS:")?;
            for (sled_id, errors) in &self.disks.errors {
                writeln!(f, "\n  sled {sled_id}\n")?;
                writeln!(
                    f,
                    "    disk diff errors: before gen {}, after gen {}\n",
                    errors.generation_before, errors.generation_after
                )?;

                for err in &errors.errors {
                    writeln!(f, "      disk id: {}", err.disk_id)?;
                    writeln!(f, "      reason: {}", err.reason)?;
                }
            }
        }

        // Write out dataset errors.
        if !self.datasets.errors.is_empty() {
            writeln!(f, "DATASET ERRORS:")?;
            for (sled_id, errors) in &self.datasets.errors {
                writeln!(f, "\n  sled {sled_id}\n")?;
                writeln!(
                    f,
                    "    dataset diff errors: before gen {}, after gen {}\n",
                    errors.generation_before, errors.generation_after
                )?;

                for err in &errors.errors {
                    writeln!(f, "      dataset id: {}", err.dataset_id)?;
                    writeln!(f, "      reason: {}", err.reason)?;
                }
            }
        }

        // Write out metadata diff table
        for table in self.make_metadata_diff_tables() {
            writeln!(f, "{}", table)?;
        }

        // Write out clickhouse cluster diff tables
        if let Some(tables) = self.make_clickhouse_cluster_config_diff_tables()
        {
            writeln!(f, "{}", tables.metadata)?;
            writeln!(f, "{}", tables.keepers)?;
            if let Some(servers) = &tables.servers {
                writeln!(f, "{}", servers)?;
            }
        }

        Ok(())
    }
}

fn display_none_if_empty(value: &str) -> &str {
    if value.is_empty() {
        NONE_PARENS
    } else {
        value
    }
}

fn display_optional_preserve_downgrade(
    value: &Option<CockroachDbPreserveDowngrade>,
) -> String {
    match value {
        Some(v) => v.to_string(),
        None => INVALID_VALUE_PARENS.to_string(),
    }
}
