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
use super::diff_visitors::visit_blueprint::{SledInsert, SledRemove};
use super::diff_visitors::Change;
use super::id_map::IdMap;
use super::{
    Blueprint, BlueprintDatasetDisposition, BlueprintPhysicalDiskDisposition,
    ClickhouseClusterConfig, CockroachDbPreserveDowngrade,
    DiffBeforeClickhouseClusterConfig,
};
use crate::deployment::blueprint_display::BpClickhouseKeepersTableSchema;
use crate::deployment::{
    unwrap_or_none, BlueprintDatasetConfig, BlueprintDatasetsConfig,
    BlueprintPhysicalDiskConfig, BlueprintPhysicalDisksConfig,
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, DiffBeforeMetadata, ZoneSortKey, ZpoolName,
};
use crate::external_api::views::SledState;
use diffus::Diffable;
use omicron_common::api::external::{ByteCount, Generation};
use omicron_common::api::internal::shared::DatasetKind;
use omicron_common::disk::{CompressionAlgorithm, DatasetName};
use omicron_uuid_kinds::{BlueprintUuid, DatasetUuid, SledUuid};
use omicron_uuid_kinds::{OmicronZoneUuid, PhysicalDiskUuid};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Accumulated state about diffs inside a `BlueprintDiffer`.
///
/// Tied to the lifetime of a diffus diff.
#[derive(Debug)]
pub struct BlueprintDiff<'e> {
    pub before: &'e Blueprint,
    pub after: &'e Blueprint,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub added_sleds: BTreeMap<SledUuid, SledInsert<'e>>,
    pub removed_sleds: BTreeMap<SledUuid, SledRemove<'e>>,
    pub unchanged_sleds: BTreeSet<SledUuid>,
    pub modified_sleds: BTreeMap<SledUuid, ModifiedSled<'e>>,
    pub metadata: MetadataDiff<'e>,

    // TODO: Change once we have a visitor for `ClickhouseClusterConfig`
    pub clickhouse_cluster_config:
        DiffValue<'e, Option<ClickhouseClusterConfig>>,
}

impl<'e> BlueprintDiff<'e> {
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> BlueprintDiff<'e> {
        BlueprintDiff {
            before,
            after,
            errors: vec![],
            warnings: vec![],
            added_sleds: BTreeMap::new(),
            removed_sleds: BTreeMap::new(),
            unchanged_sleds: BTreeSet::new(),
            modified_sleds: BTreeMap::new(),
            metadata: MetadataDiff::new(before, after),
            clickhouse_cluster_config: DiffValue::Unchanged(
                &before.clickhouse_cluster_config,
            ),
        }
    }

    /// Build a diff with the provided contents, verifying that the provided
    /// Return a struct that can be used to display the diff.
    pub fn display(&self) -> BlueprintDiffDisplay<'_> {
        BlueprintDiffDisplay::new(self)
    }

    /// Returns whether the diff reflects any changes or if the blueprints are
    /// equivalent.
    pub fn has_changes(&self) -> bool {
        // Any changes to physical disks, datasets, or zones would be reflected
        // in `self.sleds_modified`, `self.sleds_added`, or
        // `self.sleds_removed`.
        if !self.modified_sleds.is_empty()
            || !self.added_sleds.is_empty()
            || !self.removed_sleds.is_empty()
        {
            return true;
        }

        self.metadata.has_semantic_changes()
            || self.clickhouse_cluster_config.is_changed()
    }
}

/// A single value in a diff.
#[derive(Debug)]
pub enum DiffValue<'e, T> {
    Unchanged(&'e T),
    Changed(Change<'e, T>),
}

impl<'e, T> DiffValue<'e, T> {
    fn is_unchanged(&self) -> bool {
        if let DiffValue::Unchanged(_) = self {
            true
        } else {
            false
        }
    }

    fn is_changed(&self) -> bool {
        !self.is_unchanged()
    }
}

#[derive(Debug)]
pub struct ModifiedZone<'e> {
    pub disposition: DiffValue<'e, BlueprintZoneDisposition>,
    pub filesystem_pool: DiffValue<'e, Option<ZpoolName>>,
    // zone_type is not allowed to change
    pub zone_type: &'e BlueprintZoneType,
}

impl<'e> ModifiedZone<'e> {
    /// Initialize a `ModifiedZone`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(before: &'e BlueprintZoneConfig) -> ModifiedZone<'e> {
        ModifiedZone {
            disposition: DiffValue::Unchanged(&before.disposition),
            filesystem_pool: DiffValue::Unchanged(&before.filesystem_pool),
            zone_type: &before.zone_type,
        }
    }
}

#[derive(Debug)]
pub struct ModifiedDisk<'e> {
    pub disposition: DiffValue<'e, BlueprintPhysicalDiskDisposition>,
}

impl<'e> ModifiedDisk<'e> {
    /// Initialize a `ModifiedDisk`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(before: &'e BlueprintPhysicalDiskConfig) -> ModifiedDisk<'e> {
        ModifiedDisk { disposition: DiffValue::Unchanged(&before.disposition) }
    }
}

#[derive(Debug)]
pub struct ModifiedDataset<'e> {
    // The pool is not allowed to change
    pub pool: &'e ZpoolName,
    // The kind is not allowed to change
    pub kind: &'e DatasetKind,
    pub disposition: DiffValue<'e, BlueprintDatasetDisposition>,
    pub quota: DiffValue<'e, Option<ByteCount>>,
    pub reservation: DiffValue<'e, Option<ByteCount>>,
    pub compression: DiffValue<'e, CompressionAlgorithm>,
}

impl<'e> ModifiedDataset<'e> {
    /// Initialize a `ModifiedDataset`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(before: &'e BlueprintDatasetConfig) -> ModifiedDataset<'e> {
        ModifiedDataset {
            pool: &before.pool,
            kind: &before.kind,
            disposition: DiffValue::Unchanged(&before.disposition),
            quota: DiffValue::Unchanged(&before.quota),
            reservation: DiffValue::Unchanged(&before.reservation),
            compression: DiffValue::Unchanged(&before.compression),
        }
    }
}

/// All modifications of a sled that we track for purposes of diff display output
#[derive(Debug)]
pub struct ModifiedSled<'e> {
    pub sled_state: DiffValue<'e, SledState>,
    pub zones_generation: DiffValue<'e, Generation>,
    pub zones_inserted: IdMap<BlueprintZoneConfig>,
    pub zones_removed: IdMap<BlueprintZoneConfig>,
    pub zones_unchanged: IdMap<BlueprintZoneConfig>,
    pub zones_modified: BTreeMap<OmicronZoneUuid, ModifiedZone<'e>>,
    pub disks_generation: DiffValue<'e, Generation>,
    pub disks_inserted: IdMap<BlueprintPhysicalDiskConfig>,
    pub disks_removed: IdMap<BlueprintPhysicalDiskConfig>,
    pub disks_unchanged: IdMap<BlueprintPhysicalDiskConfig>,
    pub disks_modified: BTreeMap<PhysicalDiskUuid, ModifiedDisk<'e>>,
    pub datasets_generation: DiffValue<'e, Generation>,
    pub datasets_inserted: IdMap<BlueprintDatasetConfig>,
    pub datasets_removed: IdMap<BlueprintDatasetConfig>,
    pub datasets_unchanged: IdMap<BlueprintDatasetConfig>,
    pub datasets_modified: BTreeMap<DatasetUuid, ModifiedDataset<'e>>,
}

impl<'e> ModifiedSled<'e> {
    pub fn tables(&self, show_unchanged: bool) -> SledTables {
        SledTables {
            disks: self.disks_table(show_unchanged),
            datasets: self.datasets_table(show_unchanged),
            zones: self.zones_table(show_unchanged),
        }
    }

    /// Collate all data from each category to produce a single table.
    ///
    /// The order is:
    ///
    /// 1. Unchanged (if `show_unchanged` flag is set)
    /// 2. Removed
    /// 3. Added
    ///
    /// The idea behind the order is to (a) group all changes together
    /// and (b) put changes towards the bottom, so people have to scroll
    /// back less.
    ///
    /// Note that we don't currently show modified disks for backwards
    /// compatibility, but that is easily added.
    fn disks_table(&self, show_unchanged: bool) -> Option<BpTable> {
        let DiffValue::Changed(generation) = self.disks_generation else {
            return None;
        };
        let generation = BpGeneration::Diff {
            before: Some(*generation.before),
            after: Some(*generation.after),
        };

        let mut rows = vec![];

        // Unchanged
        if show_unchanged {
            rows.extend(
                self.disks_unchanged
                    .iter()
                    .map(|disk| disks_row(BpDiffState::Unchanged, disk)),
            );
        }

        // Removed
        rows.extend(
            self.disks_removed
                .iter()
                .map(|disk| disks_row(BpDiffState::Removed, disk)),
        );

        // Added
        rows.extend(
            self.zones_inserted
                .iter()
                .map(|zone| zones_row(BpDiffState::Added, zone)),
        );

        if rows.is_empty() {
            return None;
        }

        Some(BpTable::new(BpPhysicalDisksTableSchema {}, generation, rows))
    }

    /// Collate all data from each category to produce a single table.
    ///
    /// The order is:
    ///
    /// 1. Unchanged (if `show_unchanged` flag is set)
    /// 2. Removed
    /// 3. Modified
    /// 4. Added
    ///
    /// The idea behind the order is to (a) group all changes together
    /// and (b) put changes towards the bottom, so people have to scroll
    /// back less.
    fn datasets_table(&self, show_unchanged: bool) -> Option<BpTable> {
        let DiffValue::Changed(generation) = self.datasets_generation else {
            return None;
        };
        let generation = BpGeneration::Diff {
            before: Some(*generation.before),
            after: Some(*generation.after),
        };

        let mut rows = vec![];

        // Unchanged
        if show_unchanged {
            rows.extend(self.datasets_unchanged.iter().map(|dataset| {
                BpTableRow::from_strings(
                    BpDiffState::Unchanged,
                    dataset.as_strings(),
                )
            }));
        }

        // Modified
        rows.extend(self.datasets_modified.iter().map(
            |(dataset_id, modified_dataset)| {
                let mut columns = vec![];

                columns.push(BpTableColumn::Value(
                    DatasetName::new(
                        modified_dataset.pool.clone(),
                        modified_dataset.kind.clone(),
                    )
                    .full_name(),
                ));

                columns.push(BpTableColumn::Value(dataset_id.to_string()));

                match &modified_dataset.quota {
                    DiffValue::Unchanged(quota) => columns
                        .push(BpTableColumn::Value(unwrap_or_none(quota))),
                    DiffValue::Changed(change) => {
                        columns.push(BpTableColumn::Diff {
                            before: unwrap_or_none(change.before),
                            after: unwrap_or_none(change.after),
                        })
                    }
                }

                match &modified_dataset.reservation {
                    DiffValue::Unchanged(reservation) => columns.push(
                        BpTableColumn::Value(unwrap_or_none(reservation)),
                    ),
                    DiffValue::Changed(change) => {
                        columns.push(BpTableColumn::Diff {
                            before: unwrap_or_none(change.before),
                            after: unwrap_or_none(change.after),
                        })
                    }
                }

                match &modified_dataset.compression {
                    DiffValue::Unchanged(compression) => columns
                        .push(BpTableColumn::Value(compression.to_string())),
                    DiffValue::Changed(change) => {
                        columns.push(BpTableColumn::Diff {
                            before: change.before.to_string(),
                            after: change.after.to_string(),
                        })
                    }
                }

                todo!()
            },
        ));

        // Removed
        rows.extend(self.datasets_removed.iter().map(|dataset| {
            BpTableRow::from_strings(BpDiffState::Removed, dataset.as_strings())
        }));

        // Added
        rows.extend(self.datasets_inserted.iter().map(|dataset| {
            BpTableRow::from_strings(BpDiffState::Added, dataset.as_strings())
        }));

        if rows.is_empty() {
            return None;
        }

        Some(BpTable::new(BpDatasetsTableSchema {}, generation, rows))
    }

    /// Collate all data from each category to produce a single table.
    ///
    /// The order is:
    ///
    /// 1. Unchanged (if `show_unchanged` flag is set)
    /// 2. Removed
    /// 3. Modified
    /// 4. Added
    ///
    /// The idea behind the order is to (a) group all changes together
    /// and (b) put changes towards the bottom, so people have to scroll
    /// back less.
    fn zones_table(&self, show_unchanged: bool) -> Option<BpTable> {
        let DiffValue::Changed(generation) = self.zones_generation else {
            return None;
        };
        let generation = BpGeneration::Diff {
            before: Some(*generation.before),
            after: Some(*generation.after),
        };

        let mut rows = vec![];

        // Unchanged
        if show_unchanged {
            rows.extend(
                self.zones_unchanged
                    .iter()
                    .map(|zone| zones_row(BpDiffState::Unchanged, zone)),
            );
        }

        // Removed
        rows.extend(
            self.zones_removed
                .iter()
                .map(|zone| zones_row(BpDiffState::Removed, zone)),
        );

        // Modified
        rows.extend(self.zones_modified.iter().map(|(zone_id, fields)| {
            let mut columns = vec![];

            // kind
            columns.push(BpTableColumn::Value(
                fields.zone_type.kind().report_str().to_string(),
            ));

            // zone_id
            columns.push(BpTableColumn::Value(zone_id.to_string()));

            // disposition
            match &fields.disposition {
                DiffValue::Unchanged(val) => {
                    columns.push(BpTableColumn::Value(val.to_string()));
                }
                DiffValue::Changed(change) => {
                    columns.push(BpTableColumn::Diff {
                        before: change.before.to_string(),
                        after: change.after.to_string(),
                    });
                }
            }

            // underlay IP
            columns.push(BpTableColumn::Value(
                fields.zone_type.underlay_ip().to_string(),
            ));

            BpTableRow::new(BpDiffState::Modified, columns)
        }));

        // Added
        rows.extend(
            self.zones_inserted
                .iter()
                .map(|zone| zones_row(BpDiffState::Added, zone)),
        );

        if rows.is_empty() {
            return None;
        }

        Some(BpTable::new(BpOmicronZonesTableSchema {}, generation, rows))
    }
}

impl<'e> ModifiedSled<'e> {
    /// Initialize a `ModifiedSled`.
    ///
    /// We always initialized `DiffValue`s to the `before` state as if they are
    /// unchanged. If a change callback fires, then we'll update the state. We
    /// do the same for unchanged zones, disks, and datasets.
    pub fn new(before: &'e Blueprint, sled_id: SledUuid) -> ModifiedSled<'e> {
        let zones_cfg = before.blueprint_zones.get(&sled_id).unwrap();
        let disks_cfg = before.blueprint_disks.get(&sled_id).unwrap();
        let datasets_cfg = before.blueprint_datasets.get(&sled_id).unwrap();
        ModifiedSled {
            sled_state: DiffValue::Unchanged(
                before
                    .sled_state
                    .get(&sled_id)
                    .unwrap_or(&SledState::Decommissioned),
            ),
            zones_generation: DiffValue::Unchanged(&zones_cfg.generation),
            zones_inserted: IdMap::new(),
            zones_removed: IdMap::new(),
            zones_unchanged: zones_cfg.zones.clone(),
            zones_modified: BTreeMap::new(),
            disks_generation: DiffValue::Unchanged(&disks_cfg.generation),
            disks_inserted: IdMap::new(),
            disks_removed: IdMap::new(),
            disks_unchanged: disks_cfg.disks.clone(),
            disks_modified: BTreeMap::new(),
            datasets_generation: DiffValue::Unchanged(&datasets_cfg.generation),
            datasets_inserted: IdMap::new(),
            datasets_removed: IdMap::new(),
            datasets_unchanged: datasets_cfg.datasets.clone(),
            datasets_modified: BTreeMap::new(),
        }
    }
}

/// All possible modifications to `BlueprintMetadata`
#[derive(Debug)]
pub struct MetadataDiff<'e> {
    pub blueprint_id: DiffValue<'e, BlueprintUuid>,
    pub parent_blueprint_id: DiffValue<'e, Option<BlueprintUuid>>,
    pub internal_dns_version: DiffValue<'e, Generation>,
    pub external_dns_version: DiffValue<'e, Generation>,
    pub cockroachdb_fingerprint: DiffValue<'e, String>,
    pub cockroachdb_setting_preserve_downgrade:
        DiffValue<'e, CockroachDbPreserveDowngrade>,
    pub creator: DiffValue<'e, String>,
    pub comment: DiffValue<'e, String>,
}

impl<'e> MetadataDiff<'e> {
    /// Initialize a `MetadataDiff`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> MetadataDiff<'e> {
        // We don't get a callback for `id`, so just fill it in here.
        let blueprint_id = if before.id == after.id {
            DiffValue::Unchanged(&before.id)
        } else {
            DiffValue::Changed(Change { before: &before.id, after: &after.id })
        };
        MetadataDiff {
            blueprint_id,
            parent_blueprint_id: DiffValue::Unchanged(
                &before.parent_blueprint_id,
            ),
            internal_dns_version: DiffValue::Unchanged(
                &before.internal_dns_version,
            ),
            external_dns_version: DiffValue::Unchanged(
                &before.external_dns_version,
            ),
            cockroachdb_fingerprint: DiffValue::Unchanged(
                &before.cockroachdb_fingerprint,
            ),
            cockroachdb_setting_preserve_downgrade: DiffValue::Unchanged(
                &before.cockroachdb_setting_preserve_downgrade,
            ),
            creator: DiffValue::Unchanged(&before.creator),
            comment: DiffValue::Unchanged(&before.comment),
        }
    }

    /// Does this diff have changes that are not superficial or likely to be
    /// present in all diffs between two blueprints?
    pub fn has_semantic_changes(&self) -> bool {
        self.internal_dns_version.is_changed()
            || self.external_dns_version.is_changed()
            || self.cockroachdb_fingerprint.is_changed()
            || self.cockroachdb_setting_preserve_downgrade.is_changed()
    }

    /// Return displayable tables
    //
    // The current format is backwards compatible for now
    pub fn tables(&self, show_unchanged: bool) -> Vec<KvListWithHeading> {
        let mut tables = vec![];

        // Cockroach related data
        let mut crdb_rows = vec![];
        match &self.cockroachdb_fingerprint {
            DiffValue::Unchanged(val) => {
                if show_unchanged {
                    crdb_rows.push(KvPair::new(
                        BpDiffState::Unchanged,
                        COCKROACHDB_FINGERPRINT,
                        linear_table_unchanged(&display_none_if_empty(val)),
                    ))
                }
            }
            DiffValue::Changed(change) => crdb_rows.push(KvPair::new(
                BpDiffState::Modified,
                COCKROACHDB_FINGERPRINT,
                linear_table_modified(
                    &display_none_if_empty(change.before),
                    &display_none_if_empty(change.after),
                ),
            )),
        }

        match &self.cockroachdb_setting_preserve_downgrade {
            DiffValue::Unchanged(val) => {
                if show_unchanged {
                    crdb_rows.push(KvPair::new(
                        BpDiffState::Unchanged,
                        COCKROACHDB_PRESERVE_DOWNGRADE,
                        linear_table_unchanged(
                            &display_optional_preserve_downgrade(&Some(**val)),
                        ),
                    ))
                }
            }
            DiffValue::Changed(change) => crdb_rows.push(KvPair::new(
                BpDiffState::Modified,
                COCKROACHDB_PRESERVE_DOWNGRADE,
                linear_table_modified(
                    &display_optional_preserve_downgrade(&Some(*change.before)),
                    &display_optional_preserve_downgrade(&Some(*change.after)),
                ),
            )),
        }

        if !crdb_rows.is_empty() {
            tables.push(KvListWithHeading::new(COCKROACHDB_HEADING, crdb_rows));
        }

        // Rest of metadata (Just DNS for backwards compatibility for now)
        let mut metadata_rows = vec![];

        match &self.internal_dns_version {
            DiffValue::Unchanged(val) => {
                if show_unchanged {
                    metadata_rows.push(KvPair::new(
                        BpDiffState::Unchanged,
                        INTERNAL_DNS_VERSION,
                        linear_table_unchanged(val),
                    ))
                }
            }
            DiffValue::Changed(change) => metadata_rows.push(KvPair::new(
                BpDiffState::Modified,
                INTERNAL_DNS_VERSION,
                linear_table_modified(change.before, change.after),
            )),
        }

        match &self.external_dns_version {
            DiffValue::Unchanged(val) => {
                if show_unchanged {
                    metadata_rows.push(KvPair::new(
                        BpDiffState::Unchanged,
                        EXTERNAL_DNS_VERSION,
                        linear_table_unchanged(val),
                    ))
                }
            }
            DiffValue::Changed(change) => metadata_rows.push(KvPair::new(
                BpDiffState::Modified,
                EXTERNAL_DNS_VERSION,
                linear_table_modified(change.before, change.after),
            )),
        }

        if !metadata_rows.is_empty() {
            tables
                .push(KvListWithHeading::new(METADATA_HEADING, metadata_rows));
        }

        tables
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
/// Tables for added sleds in a blueprint diff
pub struct SledTables {
    pub disks: Option<BpTable>,
    pub datasets: Option<BpTable>,
    pub zones: Option<BpTable>,
}

/// Printable output derived from a `BlueprintDiff`
#[derive(Default)]
pub struct BpDiffOutput {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub added_sleds: BTreeMap<SledUuid, SledTables>,
    pub removed_sleds: BTreeMap<SledUuid, SledTables>,
    pub unchanged_sleds: BTreeMap<SledUuid, SledTables>,
    pub modified_sleds: BTreeMap<SledUuid, SledTables>,
    pub metadata: Vec<KvListWithHeading>,
    pub clickhouse_cluster_config: Option<ClickhouseClusterConfigDiffTables>,
}

impl BpDiffOutput {
    pub fn new(acc: &BlueprintDiff, show_unchanged: bool) -> BpDiffOutput {
        BpDiffOutput {
            errors: acc.errors.clone(),
            warnings: acc.warnings.clone(),
            added_sleds: acc
                .added_sleds
                .iter()
                .map(|(sled_id, insert)| {
                    (
                        *sled_id,
                        SledTables {
                            disks: disks_table(
                                BpDiffState::Added,
                                insert.disks,
                            ),
                            datasets: datasets_table(
                                BpDiffState::Added,
                                insert.datasets,
                            ),
                            zones: zones_table(
                                BpDiffState::Added,
                                insert.zones,
                            ),
                        },
                    )
                })
                .collect(),
            removed_sleds: acc
                .removed_sleds
                .iter()
                .map(|(sled_id, remove)| {
                    (
                        *sled_id,
                        SledTables {
                            disks: disks_table(
                                BpDiffState::Removed,
                                remove.disks,
                            ),
                            datasets: datasets_table(
                                BpDiffState::Removed,
                                remove.datasets,
                            ),
                            zones: zones_table(
                                BpDiffState::Removed,
                                remove.zones,
                            ),
                        },
                    )
                })
                .collect(),
            unchanged_sleds: {
                if show_unchanged {
                    acc.unchanged_sleds
                        .iter()
                        .map(|sled_id| {
                            (
                                *sled_id,
                                SledTables {
                                    disks: disks_table(
                                        BpDiffState::Unchanged,
                                        acc.before.blueprint_disks.get(sled_id),
                                    ),
                                    datasets: datasets_table(
                                        BpDiffState::Unchanged,
                                        acc.before
                                            .blueprint_datasets
                                            .get(sled_id),
                                    ),
                                    zones: zones_table(
                                        BpDiffState::Unchanged,
                                        acc.before.blueprint_zones.get(sled_id),
                                    ),
                                },
                            )
                        })
                        .collect()
                } else {
                    BTreeMap::new()
                }
            },
            modified_sleds: acc
                .modified_sleds
                .iter()
                .map(|(sled_id, modified)| {
                    (*sled_id, modified.tables(show_unchanged))
                })
                .collect(),

            metadata: acc.metadata.tables(show_unchanged),
            clickhouse_cluster_config: {
                match &acc.clickhouse_cluster_config {
                    DiffValue::Unchanged(val) => {
                        make_clickhouse_cluster_config_diff_tables(*val, *val)
                    }
                    DiffValue::Changed(Change { before, after }) => {
                        make_clickhouse_cluster_config_diff_tables(
                            *before, *after,
                        )
                    }
                }
            },
        }
    }
}

// Diff's are still done manually for `ClickhouseClusterConfig`. It's relatively
// simple compared to other diffs, but we could and probably should change to a
// visitor/accumulator model for consistency with the rest of the code.
pub fn make_clickhouse_cluster_config_diff_tables(
    before: &Option<ClickhouseClusterConfig>,
    after: &Option<ClickhouseClusterConfig>,
) -> Option<ClickhouseClusterConfigDiffTables> {
    match (before, after) {
        // Before blueprint + after blueprint
        (Some(before), Some(after)) => Some(
            ClickhouseClusterConfigDiffTables::diff_blueprints(before, after),
        ),

        // Before blueprint only
        (Some(before), None) => Some(
            ClickhouseClusterConfigDiffTables::removed_from_blueprint(before),
        ),

        // After blueprint only
        (None, Some(after)) => {
            Some(ClickhouseClusterConfigDiffTables::added_to_blueprint(after))
        }

        // No before or after
        (None, None) => None,
    }
}

/// Create a `BpTable` from a `BlueprintPhysicalDisksConfig`.
fn disks_table(
    state: BpDiffState,
    disks: Option<&BlueprintPhysicalDisksConfig>,
) -> Option<BpTable> {
    disks.map(|disks_config| {
        let rows = disks_config
            .disks
            .iter()
            .map(|disk| disks_row(state, disk))
            .collect();
        BpTable::new(
            BpPhysicalDisksTableSchema {},
            disks_config.generation.into(),
            rows,
        )
    })
}

fn disks_row(
    state: BpDiffState,
    disk: &BlueprintPhysicalDiskConfig,
) -> BpTableRow {
    BpTableRow::from_strings(
        state,
        vec![
            disk.identity.vendor.clone(),
            disk.identity.model.clone(),
            disk.identity.serial.clone(),
        ],
    )
}

/// Create a `BpTable` from a `BlueprintDatasetsConfig`
fn datasets_table(
    state: BpDiffState,
    datasets: Option<&BlueprintDatasetsConfig>,
) -> Option<BpTable> {
    datasets.map(|datasets_config| {
        // `self.datasets` is naturally ordered by ID, but that doesn't play
        // well with expectorate-based tests: We end up sorted by (random)
        // UUIDs. We redact the UUIDs, but that still results in test-to-test
        // variance in the _order_ of the rows. We can work around this for now
        // by sorting by dataset kind: after UUID redaction, that produces
        // a stable table ordering for datasets.
        let mut rows = datasets_config.datasets.iter().collect::<Vec<_>>();
        rows.sort_unstable_by_key(|d| (&d.kind, &d.pool));
        let rows = rows
            .into_iter()
            .map(move |dataset| {
                BpTableRow::from_strings(state, dataset.as_strings())
            })
            .collect();
        BpTable::new(
            BpDatasetsTableSchema {},
            datasets_config.generation.into(),
            rows,
        )
    })
}

/// Create a `BpTable` from a `BlueprintZonesConfig`
fn zones_table(
    state: BpDiffState,
    zones: Option<&BlueprintZonesConfig>,
) -> Option<BpTable> {
    zones.map(|zones_config| {
        let rows = zones_config
            .zones
            .iter()
            .map(|zone| zones_row(state, zone))
            .collect();
        BpTable::new(
            BpOmicronZonesTableSchema {},
            zones_config.generation.into(),
            rows,
        )
    })
}

fn zones_row(state: BpDiffState, zone: &BlueprintZoneConfig) -> BpTableRow {
    BpTableRow::from_strings(
        state,
        vec![
            zone.kind().report_str().to_string(),
            zone.id().to_string(),
            zone.disposition.to_string(),
            zone.underlay_ip().to_string(),
        ],
    )
}

/// Wrapper to allow a [`BlueprintDiff`] to be displayed.
///
/// Returned by [`BlueprintDiff::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintDiffDisplay<'diff> {
    pub diff: &'diff BlueprintDiff<'diff>,
    // TODO: add colorization with a stylesheet
}

impl<'diff> BlueprintDiffDisplay<'diff> {
    #[inline]
    fn new(diff: &'diff BlueprintDiff) -> Self {
        Self { diff }
    }

    /// Write out physical disk and zone tables for a given `sled_id`
    fn write_tables(
        &self,
        f: &mut fmt::Formatter<'_>,
        sled_id: &SledUuid,
    ) -> fmt::Result {
        // Write the physical disks table if it exists
        if let Some(table) =
            self.diff.physical_disks.to_bp_sled_subtable(sled_id)
        {
            writeln!(f, "{table}\n")?;
        }

        // Write the datasets table if it exists
        if let Some(table) = self.diff.datasets.to_bp_sled_subtable(sled_id) {
            writeln!(f, "{table}\n")?;
        }

        // Write the zones table if it exists
        if let Some(table) = self.diff.zones.to_bp_sled_subtable(sled_id) {
            writeln!(f, "{table}\n")?;
        }

        Ok(())
    }

    /// Helper methods to stringify sled states. These are separated by
    /// diff section because each section has different expectations for what
    /// before and after should be that can only be wrong if we have a bug
    /// constructing the diff.
    fn sled_state_unchanged(&self, sled_id: &SledUuid) -> String {
        let before = self.diff.before_state.get(sled_id);
        let after = self.diff.after_state.get(sled_id);
        if before == after {
            after
                .map(|s| s.to_string())
                .unwrap_or_else(|| "state unknown".to_string())
        } else {
            format!(
                "blueprint diff error: unchanged sled changed state from \
                 {before:?} to {after:?}"
            )
        }
    }
    fn sled_state_added(&self, sled_id: &SledUuid) -> String {
        let before = self.diff.before_state.get(sled_id);
        let after = self.diff.after_state.get(sled_id);
        if before.is_none() {
            after
                .map(|s| format!("{s}"))
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            format!(
                "blueprint diff error: added sled has old state \
                 {before:?} (now {after:?})"
            )
        }
    }
    fn sled_state_removed(&self, sled_id: &SledUuid) -> String {
        let before = self.diff.before_state.get(sled_id);
        let after = self.diff.after_state.get(sled_id);
        if after.is_none() {
            before
                .map(|s| format!("was {s}"))
                .unwrap_or_else(|| "state was unknown".to_string())
        } else {
            format!(
                "blueprint diff error: removed sled has new state \
                 {after:?} (was {before:?})"
            )
        }
    }
    fn sled_state_modified(&self, sled_id: &SledUuid) -> String {
        let before = self.diff.before_state.get(sled_id);
        let after = self.diff.after_state.get(sled_id);
        match (before, after) {
            (Some(before), Some(after)) if before != after => {
                format!("{before} -> {after}")
            }
            (Some(state), Some(_)) => {
                // states are equal; the sled was presumably modified some other
                // way
                format!("{state}")
            }
            (None, Some(after)) => format!("unknown -> {after}"),
            (Some(before), None) => format!("{before} -> unknown"),
            (None, None) => "unknown".to_string(),
        }
    }
}

impl fmt::Display for BlueprintDiffDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let diff = self.diff;

        // Print things differently based on whether the diff is between a
        // collection and a blueprint, or a blueprint and a blueprint.
        match &diff.before_meta {
            DiffBeforeMetadata::Collection { id } => {
                writeln!(
                    f,
                    "from: collection {}\n\
                     to:   blueprint  {}",
                    id, diff.after_meta.id,
                )?;
            }
            DiffBeforeMetadata::Blueprint(before) => {
                writeln!(
                    f,
                    "from: blueprint {}\n\
                     to:   blueprint {}\n",
                    before.id, diff.after_meta.id
                )?;
            }
        }

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
        if !diff.sleds_unchanged.is_empty() {
            writeln!(f, " UNCHANGED SLEDS:\n")?;
            for sled_id in &diff.sleds_unchanged {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_unchanged(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out tables for removed sleds
        if !diff.sleds_removed.is_empty() {
            writeln!(f, " REMOVED SLEDS:\n")?;
            for sled_id in &diff.sleds_removed {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_removed(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out tables for modified sleds
        if !diff.sleds_modified.is_empty() {
            writeln!(f, " MODIFIED SLEDS:\n")?;
            for sled_id in &diff.sleds_modified {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_modified(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out tables for added sleds
        if !diff.sleds_added.is_empty() {
            writeln!(f, " ADDED SLEDS:\n")?;
            for sled_id in &diff.sleds_added {
                writeln!(
                    f,
                    "  sled {sled_id} ({}):\n",
                    self.sled_state_added(sled_id)
                )?;
                self.write_tables(f, sled_id)?;
            }
        }

        // Write out zone errors.
        if !diff.zones.errors.is_empty() {
            writeln!(f, "ERRORS:")?;
            for (sled_id, errors) in &diff.zones.errors {
                writeln!(f, "\n  sled {sled_id}\n")?;
                writeln!(
                    f,
                    "    zone diff errors: before gen {}, after gen {}\n",
                    errors.generation_before, errors.generation_after
                )?;

                for err in &errors.errors {
                    writeln!(f, "      zone id: {}", err.zone_before.id())?;
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
