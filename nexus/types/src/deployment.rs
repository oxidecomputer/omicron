// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing deployed software and configuration
//!
//! For more on this, see the crate-level documentation for
//! `nexus/reconfigurator/planning`.
//!
//! This lives in nexus/types because it's used by both nexus/db-model and
//! nexus/reconfigurator/planning.  (It could as well just live in
//! nexus/db-model, but nexus/reconfigurator/planning does not currently know
//! about nexus/db-model and it's convenient to separate these concerns.)

use crate::external_api::views::SledState;
use crate::internal_api::params::DnsConfigParams;
use crate::inventory::Collection;
pub use crate::inventory::SourceNatConfig;
pub use crate::inventory::ZpoolName;
use blueprint_diff::ClickhouseClusterConfigDiffTablesForSingleBlueprint;
use blueprint_display::BpDatasetsTableSchema;
use blueprint_display::BpHostPhase2TableSchema;
use blueprint_display::BpTableColumn;
use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_ord_map::Entry;
use iddqd::id_ord_map::RefMut;
use iddqd::id_upcast;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredContents;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::external::TufArtifactMeta;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::M2Slot;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use slog::Key;
use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use strum::EnumIter;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::ArtifactVersionError;

mod blueprint_diff;
mod blueprint_display;
mod chicken_switches;
mod clickhouse;
pub mod execution;
mod network_resources;
mod planning_input;
mod zone_type;

use crate::inventory::BaseboardId;
pub use blueprint_diff::BlueprintDiffSummary;
use blueprint_display::BpPendingMgsUpdates;
pub use chicken_switches::PlannerChickenSwitches;
pub use chicken_switches::PlannerChickenSwitchesDiff;
pub use chicken_switches::PlannerChickenSwitchesDisplay;
pub use chicken_switches::ReconfiguratorChickenSwitches;
pub use chicken_switches::ReconfiguratorChickenSwitchesDiff;
pub use chicken_switches::ReconfiguratorChickenSwitchesDiffDisplay;
pub use chicken_switches::ReconfiguratorChickenSwitchesDisplay;
pub use chicken_switches::ReconfiguratorChickenSwitchesParam;
pub use chicken_switches::ReconfiguratorChickenSwitchesView;
pub use chicken_switches::ReconfiguratorChickenSwitchesViewDisplay;
pub use clickhouse::ClickhouseClusterConfig;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
pub use network_resources::AddNetworkResourceError;
pub use network_resources::OmicronZoneExternalFloatingAddr;
pub use network_resources::OmicronZoneExternalFloatingIp;
pub use network_resources::OmicronZoneExternalIp;
pub use network_resources::OmicronZoneExternalIpEntry;
pub use network_resources::OmicronZoneExternalIpKey;
pub use network_resources::OmicronZoneExternalSnatIp;
pub use network_resources::OmicronZoneNetworkResources;
pub use network_resources::OmicronZoneNic;
pub use network_resources::OmicronZoneNicEntry;
pub use planning_input::ClickhouseMode;
pub use planning_input::ClickhousePolicy;
pub use planning_input::CockroachDbClusterVersion;
pub use planning_input::CockroachDbPreserveDowngrade;
pub use planning_input::CockroachDbSettings;
pub use planning_input::DiskFilter;
pub use planning_input::OximeterReadMode;
pub use planning_input::OximeterReadPolicy;
pub use planning_input::PlanningInput;
pub use planning_input::PlanningInputBuildError;
pub use planning_input::PlanningInputBuilder;
pub use planning_input::Policy;
pub use planning_input::SledDetails;
pub use planning_input::SledDisk;
pub use planning_input::SledFilter;
pub use planning_input::SledLookupError;
pub use planning_input::SledLookupErrorKind;
pub use planning_input::SledResources;
pub use planning_input::TargetReleaseDescription;
pub use planning_input::TufRepoContentsError;
pub use planning_input::TufRepoPolicy;
pub use planning_input::ZpoolFilter;
pub use zone_type::BlueprintZoneType;
pub use zone_type::DurableDataset;
pub use zone_type::blueprint_zone_type;

use blueprint_display::{
    BpDiffState, BpOmicronZonesTableSchema, BpPhysicalDisksTableSchema,
    BpTable, BpTableData, BpTableRow, KvList, constants::*,
};
use id_map::{IdMap, IdMappable};
use std::str::FromStr;

/// Describes a complete set of software and configuration for the system
// Blueprints are a fundamental part of how the system modifies itself.  Each
// blueprint completely describes all of the software and configuration
// that the control plane manages.  See the nexus/reconfigurator/planning
// crate-level documentation for details.
//
// Blueprints are different from policy.  Policy describes the things that an
// operator would generally want to control.  The blueprint describes the
// details of implementing that policy that an operator shouldn't have to deal
// with.  For example, the operator might write policy that says "I want
// 5 external DNS zones".  The system could then generate a blueprint that
// _has_ 5 external DNS zones on 5 specific sleds.  The blueprint includes all
// the details needed to achieve that, including which image these zones should
// run, which zpools their persistent data should be stored on, their public and
// private IP addresses, their internal DNS names, etc.
//
// It must be possible for multiple Nexus instances to execute the same
// blueprint concurrently and converge to the same thing.  Thus, these _cannot_
// be how a blueprint works:
//
// - "add a Nexus zone" -- two Nexus instances operating concurrently would
//   add _two_ Nexus zones (which is wrong)
// - "ensure that there is a Nexus zone on this sled with this id" -- the IP
//   addresses and images are left unspecified.  Two Nexus instances could pick
//   different IPs or images for the zone.
//
// This is why blueprints must be so detailed.  The key principle here is that
// **all the work of ensuring that the system do the right thing happens in one
// process (the update planner in one Nexus instance).  Once a blueprint has
// been committed, everyone is on the same page about how to execute it.**  The
// intent is that this makes both planning and executing a lot easier.  In
// particular, by the time we get to execution, all the hard choices have
// already been made.
//
// Currently, blueprints are limited to describing only the set of Omicron
// zones deployed on each host and some supporting configuration (e.g., DNS).
// This is aimed at supporting add/remove sleds.  The plan is to grow this to
// include more of the system as we support more use cases.
#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
pub struct Blueprint {
    /// unique identifier for this blueprint
    pub id: BlueprintUuid,

    /// A map of sled id -> desired configuration of the sled.
    pub sleds: BTreeMap<SledUuid, BlueprintSledConfig>,

    /// List of pending MGS-mediated updates
    pub pending_mgs_updates: PendingMgsUpdates,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<BlueprintUuid>,

    /// internal DNS version when this blueprint was created
    // See blueprint execution for more on this.
    pub internal_dns_version: Generation,

    /// external DNS version when this blueprint was created
    // See blueprint execution for more on this.
    pub external_dns_version: Generation,

    /// The minimum release generation to accept for target release
    /// configuration. Target release configuration with a generation less than
    /// this number will be ignored.
    ///
    /// For example, let's say that the current target release generation is 5.
    /// Then, when reconfigurator detects a MUPdate:
    ///
    /// * the target release is ignored in favor of the install dataset
    /// * this field is set to 6
    ///
    /// Once an operator sets a new target release, its generation will be 6 or
    /// higher. Reconfigurator will then know that it is back in charge of
    /// driving the system to the target release.
    pub target_release_minimum_generation: Generation,

    /// CockroachDB state fingerprint when this blueprint was created
    // See `nexus/db-queries/src/db/datastore/cockroachdb_settings.rs` for more
    // on this.
    pub cockroachdb_fingerprint: String,

    /// Whether to set `cluster.preserve_downgrade_option` and what to set it to
    pub cockroachdb_setting_preserve_downgrade: CockroachDbPreserveDowngrade,

    /// Allocation of Clickhouse Servers and Keepers for replicated clickhouse
    /// setups. This is set to `None` if replicated clickhouse is not in use.
    //
    // We already have some manual diff code for this, and don't expect the
    // structure to change any time soon.
    #[daft(leaf)]
    pub clickhouse_cluster_config: Option<ClickhouseClusterConfig>,

    /// Oximeter read policy version when this blueprint was created
    pub oximeter_read_version: Generation,

    /// Whether oximeter should read from a single node or a cluster
    pub oximeter_read_mode: OximeterReadMode,

    /// when this blueprint was generated (for debugging)
    #[daft(ignore)]
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// identity of the component that generated the blueprint (for debugging)
    /// This would generally be the Uuid of a Nexus instance.
    pub creator: String,
    /// human-readable string describing why this blueprint was created
    /// (for debugging)
    pub comment: String,
}

impl Blueprint {
    /// Return metadata for this blueprint.
    pub fn metadata(&self) -> BlueprintMetadata {
        BlueprintMetadata {
            id: self.id,
            parent_blueprint_id: self.parent_blueprint_id,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
            target_release_minimum_generation: self
                .target_release_minimum_generation,
            cockroachdb_fingerprint: self.cockroachdb_fingerprint.clone(),
            cockroachdb_setting_preserve_downgrade: Some(
                self.cockroachdb_setting_preserve_downgrade,
            ),
            time_created: self.time_created,
            creator: self.creator.clone(),
            comment: self.comment.clone(),
        }
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances in the blueprint
    /// that match the provided filter, along with the associated sled id.
    pub fn all_omicron_zones<F>(
        &self,
        filter: F,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        Blueprint::filtered_zones(
            self.sleds.iter().map(|(sled_id, config)| (*sled_id, config)),
            filter,
        )
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances that match the
    /// provided filter, along with the associated sled id.
    //
    // This is a scoped function so that it can be used in the
    // `BlueprintBuilder` during planning as well as in the `Blueprint`.
    pub fn filtered_zones<'a, I, F>(
        zones_by_sled_id: I,
        mut filter: F,
    ) -> impl Iterator<Item = (SledUuid, &'a BlueprintZoneConfig)>
    where
        I: Iterator<Item = (SledUuid, &'a BlueprintSledConfig)>,
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        zones_by_sled_id
            .flat_map(move |(sled_id, config)| {
                config.zones.iter().map(move |z| (sled_id, z))
            })
            .filter(move |(_, z)| filter(z.disposition))
    }

    /// Iterate over the [`BlueprintPhysicalDiskConfig`] instances in the
    /// blueprint that match the provided filter, along with the associated
    /// sled id.
    pub fn all_omicron_disks<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintPhysicalDiskConfig)>
    where
        F: FnMut(BlueprintPhysicalDiskDisposition) -> bool,
    {
        self.sleds
            .iter()
            .flat_map(move |(sled_id, config)| {
                config.disks.iter().map(|disk| (*sled_id, disk))
            })
            .filter(move |(_, d)| filter(d.disposition))
    }

    /// Iterate over the [`BlueprintDatasetConfig`] instances in the blueprint.
    pub fn all_omicron_datasets<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintDatasetConfig)>
    where
        F: FnMut(BlueprintDatasetDisposition) -> bool,
    {
        self.sleds
            .iter()
            .flat_map(move |(sled_id, config)| {
                config.datasets.iter().map(|dataset| (*sled_id, dataset))
            })
            .filter(move |(_, d)| filter(d.disposition))
    }

    /// Iterate over the ids of all sleds in the blueprint
    pub fn sleds(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.sleds.keys().copied()
    }

    /// Summarize the difference between two blueprints.
    ///
    /// The argument provided is the "before" side, and `self` is the "after"
    /// side.
    pub fn diff_since_blueprint<'a>(
        &'a self,
        before: &'a Blueprint,
    ) -> BlueprintDiffSummary<'a> {
        BlueprintDiffSummary::new(before, self)
    }

    /// Return a struct that can be displayed to present information about the
    /// blueprint.
    pub fn display(&self) -> BlueprintDisplay<'_> {
        BlueprintDisplay { blueprint: self }
    }
}

/// Wrapper to display a table of a `BlueprintSledConfig`'s host phase 2
/// contents.
#[derive(Clone, Debug)]
struct BlueprintHostPhase2TableData<'a> {
    host_phase_2: &'a BlueprintHostPhase2DesiredSlots,
}

impl<'a> BlueprintHostPhase2TableData<'a> {
    fn new(host_phase_2: &'a BlueprintHostPhase2DesiredSlots) -> Self {
        Self { host_phase_2 }
    }

    fn diff_rows<'b>(
        diffs: &'b BlueprintHostPhase2DesiredSlotsDiff<'_>,
    ) -> impl Iterator<Item = BpTableRow> + 'b {
        [(M2Slot::A, &diffs.slot_a), (M2Slot::B, &diffs.slot_b)]
            .into_iter()
            .map(|(slot, diff)| {
                let mut cols = vec![BpTableColumn::value(format!("{slot:?}"))];
                let state;
                if diff.is_modified() {
                    state = BpDiffState::Modified;
                    cols.push(BpTableColumn::new(
                        diff.before.to_string(),
                        diff.after.to_string(),
                    ));
                } else {
                    state = BpDiffState::Unchanged;
                    cols.push(BpTableColumn::value(diff.before.to_string()));
                }
                BpTableRow::new(state, cols)
            })
    }
}

impl BpTableData for BlueprintHostPhase2TableData<'_> {
    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        [
            (M2Slot::A, &self.host_phase_2.slot_a),
            (M2Slot::B, &self.host_phase_2.slot_b),
        ]
        .into_iter()
        .map(move |(slot, contents)| {
            BpTableRow::from_strings(
                state,
                vec![format!("{slot:?}"), contents.to_string()],
            )
        })
    }
}

/// Wrapper to display a table of a `BlueprintSledConfig`'s disks.
#[derive(Clone, Debug)]
struct BlueprintPhysicalDisksTableData<'a> {
    disks: &'a IdMap<BlueprintPhysicalDiskConfig>,
}

impl<'a> BlueprintPhysicalDisksTableData<'a> {
    fn new(disks: &'a IdMap<BlueprintPhysicalDiskConfig>) -> Self {
        Self { disks }
    }
}

impl BpTableData for BlueprintPhysicalDisksTableData<'_> {
    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        let mut disks: Vec<_> = self.disks.iter().cloned().collect();
        disks.sort_unstable_by_key(|d| d.identity.clone());

        disks.into_iter().map(move |d| {
            BpTableRow::from_strings(
                state,
                vec![
                    d.identity.vendor,
                    d.identity.model,
                    d.identity.serial,
                    d.disposition.to_string(),
                ],
            )
        })
    }
}

/// Wrapper to display a table of a `BlueprintSledConfig`'s disks.
#[derive(Clone, Debug)]
struct BlueprintDatasetsTableData<'a> {
    datasets: &'a IdMap<BlueprintDatasetConfig>,
}

impl<'a> BlueprintDatasetsTableData<'a> {
    fn new(datasets: &'a IdMap<BlueprintDatasetConfig>) -> Self {
        Self { datasets }
    }
}

impl BpTableData for BlueprintDatasetsTableData<'_> {
    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        // We want to sort by (kind, pool)
        let mut datasets: Vec<_> = self.datasets.iter().collect();
        datasets.sort_unstable_by_key(|d| (&d.kind, &d.pool));
        datasets.into_iter().map(move |dataset| {
            BpTableRow::from_strings(state, dataset.as_strings())
        })
    }
}

/// Wrapper to display a table of a `BlueprintSledConfig`'s zones.
#[derive(Clone, Debug)]
struct BlueprintZonesTableData<'a> {
    zones: &'a IdMap<BlueprintZoneConfig>,
}

impl<'a> BlueprintZonesTableData<'a> {
    fn new(zones: &'a IdMap<BlueprintZoneConfig>) -> Self {
        Self { zones }
    }
}

impl BpTableData for BlueprintZonesTableData<'_> {
    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        // We want to sort by (kind, id)
        let mut zones: Vec<_> = self.zones.iter().cloned().collect();
        zones.sort_unstable_by_key(zone_sort_key);
        zones.into_iter().map(move |zone| {
            BpTableRow::from_strings(
                state,
                vec![
                    zone.kind().report_str().to_string(),
                    ZoneSortKey::id(&zone).to_string(),
                    zone.image_source.to_string(),
                    zone.disposition.to_string(),
                    zone.underlay_ip().to_string(),
                ],
            )
        })
    }
}

// Useful implementation for printing two column tables stored in a `BTreeMap`, where
// each key and value impl `Display`.
impl<S1, S2> BpTableData for BTreeMap<S1, S2>
where
    S1: fmt::Display,
    S2: fmt::Display,
{
    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.iter().map(move |(s1, s2)| {
            BpTableRow::from_strings(
                state,
                vec![s1.to_string(), s2.to_string()],
            )
        })
    }
}

/// Wrapper to allow a [`Blueprint`] to be displayed with information.
///
/// Returned by [`Blueprint::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintDisplay<'a> {
    blueprint: &'a Blueprint,
    // TODO: add colorization with a stylesheet
}

impl BlueprintDisplay<'_> {
    fn make_cockroachdb_table(&self) -> KvList {
        let fingerprint = if self.blueprint.cockroachdb_fingerprint.is_empty() {
            NONE_PARENS.to_string()
        } else {
            self.blueprint.cockroachdb_fingerprint.clone()
        };

        KvList::new_unchanged(
            Some(COCKROACHDB_HEADING),
            vec![
                (COCKROACHDB_FINGERPRINT, fingerprint),
                (
                    COCKROACHDB_PRESERVE_DOWNGRADE,
                    self.blueprint
                        .cockroachdb_setting_preserve_downgrade
                        .to_string(),
                ),
            ],
        )
    }

    fn make_oximeter_table(&self) -> KvList {
        KvList::new_unchanged(
            Some(OXIMETER_HEADING),
            vec![
                (GENERATION, self.blueprint.oximeter_read_version.to_string()),
                (
                    OXIMETER_READ_FROM,
                    self.blueprint.oximeter_read_mode.to_string(),
                ),
            ],
        )
    }

    fn make_metadata_table(&self) -> KvList {
        let comment = if self.blueprint.comment.is_empty() {
            NONE_PARENS.to_string()
        } else {
            self.blueprint.comment.clone()
        };

        KvList::new_unchanged(
            Some(METADATA_HEADING),
            vec![
                (CREATED_BY, self.blueprint.creator.clone()),
                (
                    CREATED_AT,
                    humantime::format_rfc3339_millis(
                        self.blueprint.time_created.into(),
                    )
                    .to_string(),
                ),
                (COMMENT, comment),
                (
                    INTERNAL_DNS_VERSION,
                    self.blueprint.internal_dns_version.to_string(),
                ),
                (
                    EXTERNAL_DNS_VERSION,
                    self.blueprint.external_dns_version.to_string(),
                ),
                (
                    TARGET_RELEASE_MIN_GEN,
                    self.blueprint
                        .target_release_minimum_generation
                        .to_string(),
                ),
            ],
        )
    }

    // Return tables representing a [`ClickhouseClusterConfig`] in a given blueprint
    fn make_clickhouse_cluster_config_tables(
        &self,
    ) -> Option<(KvList, BpTable, BpTable)> {
        let config = &self.blueprint.clickhouse_cluster_config.as_ref()?;

        let diff_table =
            ClickhouseClusterConfigDiffTablesForSingleBlueprint::new(
                BpDiffState::Unchanged,
                config,
            );

        Some((diff_table.metadata, diff_table.keepers, diff_table.servers))
    }
}

impl fmt::Display for BlueprintDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Unpack the blueprint so any future field changes require updating
        // this method to update the display.
        let Blueprint {
            id,
            sleds,
            pending_mgs_updates,
            parent_blueprint_id,
            // These two cockroachdb_* fields are handled by
            // `make_cockroachdb_table()`, called below.
            cockroachdb_fingerprint: _,
            cockroachdb_setting_preserve_downgrade: _,
            // Handled by `make_clickhouse_cluster_config_tables()`, called
            // below.
            clickhouse_cluster_config: _,
            // Handled by `make_oximeter_table`, called below.
            oximeter_read_version: _,
            oximeter_read_mode: _,
            // These six fields are handled by `make_metadata_table()`, called
            // below.
            target_release_minimum_generation: _,
            internal_dns_version: _,
            external_dns_version: _,
            time_created: _,
            creator: _,
            comment: _,
        } = self.blueprint;

        writeln!(f, "blueprint  {}", id)?;
        writeln!(
            f,
            "parent:    {}",
            parent_blueprint_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| String::from("<none>"))
        )?;

        // Loop through all sleds and print details of their configs.
        for (sled_id, config) in sleds {
            let BlueprintSledConfig {
                state,
                sled_agent_generation,
                disks,
                datasets,
                zones,
                remove_mupdate_override,
                host_phase_2,
            } = config;

            // Report the sled state
            writeln!(
                f,
                "\n  sled: {sled_id} ({state}, config generation \
                 {sled_agent_generation})",
            )?;

            let mut rows = Vec::new();
            if let Some(id) = remove_mupdate_override {
                rows.push((WILL_REMOVE_MUPDATE_OVERRIDE, id.to_string()));
            }
            let list = KvList::new_unchanged(None, rows);
            writeln!(f, "{list}")?;

            // Construct the desired host phase 2 contents table
            let host_phase_2_table = BpTable::new(
                BpHostPhase2TableSchema {},
                None,
                BlueprintHostPhase2TableData::new(host_phase_2)
                    .rows(BpDiffState::Unchanged)
                    .collect(),
            );
            writeln!(f, "{host_phase_2_table}\n")?;

            // Construct the disks subtable
            let disks_table = BpTable::new(
                BpPhysicalDisksTableSchema {},
                None,
                BlueprintPhysicalDisksTableData::new(&disks)
                    .rows(BpDiffState::Unchanged)
                    .collect(),
            );
            writeln!(f, "{disks_table}\n")?;

            // Construct the datasets subtable
            let datasets_tab = BpTable::new(
                BpDatasetsTableSchema {},
                None,
                BlueprintDatasetsTableData::new(&datasets)
                    .rows(BpDiffState::Unchanged)
                    .collect(),
            );
            writeln!(f, "{datasets_tab}\n")?;

            // Construct the zones subtable
            let zones_tab = BpTable::new(
                BpOmicronZonesTableSchema {},
                None,
                BlueprintZonesTableData::new(&zones)
                    .rows(BpDiffState::Unchanged)
                    .collect(),
            );
            writeln!(f, "{zones_tab}\n")?;
        }

        if let Some((t1, t2, t3)) = self.make_clickhouse_cluster_config_tables()
        {
            writeln!(f, "{t1}\n{t2}\n{t3}\n")?;
        }

        writeln!(f, "{}", self.make_cockroachdb_table())?;
        writeln!(f, "{}", self.make_oximeter_table())?;
        writeln!(f, "{}", self.make_metadata_table())?;

        writeln!(
            f,
            " PENDING MGS-MANAGED UPDATES: {}",
            pending_mgs_updates.len()
        )?;
        if !pending_mgs_updates.is_empty() {
            writeln!(
                f,
                "{}",
                BpTable::new(
                    BpPendingMgsUpdates {},
                    None,
                    pending_mgs_updates
                        .iter()
                        .map(|pu| {
                            BpTableRow::from_strings(
                                BpDiffState::Unchanged,
                                pu.to_bp_table_values(),
                            )
                        })
                        .collect()
                )
            )?;
        }

        Ok(())
    }
}

/// Information about the configuration of a sled as recorded in a blueprint.
///
/// Part of [`Blueprint`].
#[derive(
    Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
pub struct BlueprintSledConfig {
    pub state: SledState,

    /// Generation number used when this type is converted into an
    /// `OmicronSledConfig` for use by sled-agent.
    ///
    /// This field is explicitly named `sled_agent_generation` to indicate that
    /// it is only required to cover information that changes what
    /// Reconfigurator sends to sled agent. For example, changing the sled
    /// `state` from `Active` to `Decommissioned` would not require a bump to
    /// `sled_agent_generation`, because a `Decommissioned` sled will never be
    /// sent an `OmicronSledConfig`.
    pub sled_agent_generation: Generation,

    pub disks: IdMap<BlueprintPhysicalDiskConfig>,
    pub datasets: IdMap<BlueprintDatasetConfig>,
    pub zones: IdMap<BlueprintZoneConfig>,
    pub remove_mupdate_override: Option<MupdateOverrideUuid>,
    pub host_phase_2: BlueprintHostPhase2DesiredSlots,
}

impl BlueprintSledConfig {
    /// Converts self into [`OmicronSledConfig`].
    ///
    /// This function is effectively a `From` implementation, but
    /// is named slightly more explicitly, as it filters the blueprint
    /// configuration to only consider components that should be in-service.
    pub fn into_in_service_sled_config(self) -> OmicronSledConfig {
        OmicronSledConfig {
            generation: self.sled_agent_generation,
            disks: self
                .disks
                .into_iter()
                .filter_map(|disk| {
                    if disk.disposition.is_in_service() {
                        Some(disk.into())
                    } else {
                        None
                    }
                })
                .collect(),
            datasets: self
                .datasets
                .into_iter()
                .filter_map(|dataset| {
                    if dataset.disposition.is_in_service() {
                        Some(dataset.into())
                    } else {
                        None
                    }
                })
                .collect(),
            zones: self
                .zones
                .into_iter()
                .filter_map(|zone| {
                    if zone.disposition.is_in_service() {
                        Some(zone.into())
                    } else {
                        None
                    }
                })
                .collect(),
            remove_mupdate_override: self.remove_mupdate_override,
            host_phase_2: self.host_phase_2.into(),
        }
    }

    /// Returns true if all disks, datasets, and zones in the blueprint have a
    /// disposition of `Expunged`, false otherwise.
    pub fn are_all_items_expunged(&self) -> bool {
        self.are_all_disks_expunged()
            && self.are_all_datasets_expunged()
            && self.are_all_zones_expunged()
    }

    /// Returns true if all disks in the blueprint have a disposition of
    /// `Expunged`, false otherwise.
    pub fn are_all_disks_expunged(&self) -> bool {
        self.disks.iter().all(|c| c.disposition.is_expunged())
    }

    /// Returns true if all datasets in the blueprint have a disposition of
    /// `Expunged`, false otherwise.
    pub fn are_all_datasets_expunged(&self) -> bool {
        self.datasets.iter().all(|c| c.disposition.is_expunged())
    }

    /// Returns true if all zones in the blueprint have a disposition of
    /// `Expunged`, false otherwise.
    pub fn are_all_zones_expunged(&self) -> bool {
        self.zones.iter().all(|c| c.disposition.is_expunged())
    }
}

trait ZoneSortKey {
    fn kind(&self) -> ZoneKind;
    fn id(&self) -> OmicronZoneUuid;
}

impl ZoneSortKey for BlueprintZoneConfig {
    fn kind(&self) -> ZoneKind {
        self.zone_type.kind()
    }

    fn id(&self) -> OmicronZoneUuid {
        self.id
    }
}

impl ZoneSortKey for OmicronZoneConfig {
    fn kind(&self) -> ZoneKind {
        self.zone_type.kind()
    }

    fn id(&self) -> OmicronZoneUuid {
        self.id
    }
}

fn zone_sort_key<T: ZoneSortKey>(z: &T) -> impl Ord {
    // First sort by kind, then by ID. This makes it so that zones of the same
    // kind (e.g. Crucible zones) are grouped together.
    (z.kind(), z.id())
}

/// Describes one Omicron-managed zone in a blueprint.
///
/// Part of [`BlueprintSledConfig`].
#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
pub struct BlueprintZoneConfig {
    /// The disposition (desired state) of this zone recorded in the blueprint.
    pub disposition: BlueprintZoneDisposition,

    pub id: OmicronZoneUuid,
    /// zpool used for the zone's (transient) root filesystem
    pub filesystem_pool: ZpoolName,
    pub zone_type: BlueprintZoneType,
    pub image_source: BlueprintZoneImageSource,
}

impl IdMappable for BlueprintZoneConfig {
    type Id = OmicronZoneUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

impl BlueprintZoneConfig {
    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    pub fn underlay_ip(&self) -> Ipv6Addr {
        self.zone_type.underlay_ip()
    }

    /// Returns the dataset used for the the zone's (transient) root filesystem.
    pub fn filesystem_dataset(&self) -> DatasetName {
        let name = illumos_utils::zone::zone_name(
            self.zone_type.kind().zone_prefix(),
            Some(self.id),
        );
        let kind = DatasetKind::TransientZone { name };
        DatasetName::new(self.filesystem_pool, kind)
    }

    pub fn kind(&self) -> ZoneKind {
        self.zone_type.kind()
    }
}

impl From<BlueprintZoneConfig> for OmicronZoneConfig {
    fn from(z: BlueprintZoneConfig) -> Self {
        let BlueprintZoneConfig {
            id,
            filesystem_pool,
            zone_type,
            disposition: _disposition,
            image_source,
        } = z;
        Self {
            id,
            filesystem_pool: Some(filesystem_pool),
            zone_type: zone_type.into(),
            image_source: image_source.into(),
        }
    }
}

/// The desired state of an Omicron-managed zone in a blueprint.
///
/// Part of [`BlueprintZoneConfig`].
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlueprintZoneDisposition {
    /// The zone is in-service.
    InService,

    /// The zone is permanently gone.
    Expunged {
        /// Generation of the parent config in which this zone became expunged.
        as_of_generation: Generation,

        /// True if Reconfiguration knows that this zone has been shut down and
        /// will not be restarted.
        ///
        /// In the current implementation, this means the planner has observed
        /// an inventory collection where the sled on which this zone was
        /// running (a) is no longer running the zone and (b) has a config
        /// generation at least as high as `as_of_generation`, indicating it
        /// will not try to start the zone on a cold boot based on an older
        /// config.
        ready_for_cleanup: bool,
    },
}

impl BlueprintZoneDisposition {
    /// Always returns true.
    ///
    /// This is intended for use with methods that take a filtering closure
    /// operating on a `BlueprintZoneDisposition` (e.g.,
    /// `Blueprint::all_omicron_zones()`), allowing callers to make it clear
    /// they accept any disposition via
    ///
    /// ```rust,ignore
    /// blueprint.all_omicron_zones(BlueprintZoneDisposition::any)
    /// ```
    pub fn any(self) -> bool {
        true
    }

    /// Returns true if `self` is `BlueprintZoneDisposition::InService`.
    pub fn is_in_service(self) -> bool {
        matches!(self, Self::InService)
    }

    /// Returns true if `self` is `BlueprintZoneDisposition::Expunged { .. }`,
    /// regardless of the details contained within that variant.
    pub fn is_expunged(self) -> bool {
        matches!(self, Self::Expunged { .. })
    }

    /// Returns true if it's possible a zone with this disposition could be
    /// running.
    ///
    /// This is true for both in-service zones and zones that have been expunged
    /// but are not yet ready for cleanup (e.g., immediately after the planner
    /// expunges a zone, there is a period of time where the zone continues to
    /// run before execution notifies the sled to shut it down).
    pub fn could_be_running(self) -> bool {
        match self {
            BlueprintZoneDisposition::InService => true,
            BlueprintZoneDisposition::Expunged {
                ready_for_cleanup, ..
            } => !ready_for_cleanup,
        }
    }

    /// Returns true if `self` indicates the zone is expunged and ready for
    /// cleanup.
    pub fn is_ready_for_cleanup(self) -> bool {
        matches!(self, Self::Expunged { ready_for_cleanup: true, .. })
    }
}

impl fmt::Display for BlueprintZoneDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Neither `write!(f, "...")` nor `f.write_str("...")` obey fill
            // and alignment (used above), but this does.
            BlueprintZoneDisposition::InService => "in service".fmt(f),
            BlueprintZoneDisposition::Expunged {
                ready_for_cleanup, ..
            } => {
                if *ready_for_cleanup {
                    "expunged ✓".fmt(f)
                } else {
                    "expunged ⏳".fmt(f)
                }
            }
        }
    }
}

/// Where the zone's image source is located.
///
/// This is the blueprint version of [`OmicronZoneImageSource`].
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlueprintZoneImageSource {
    /// This zone's image source is whatever happens to be on the sled's
    /// "install" dataset.
    ///
    /// This is whatever was put in place at the factory or by the latest
    /// MUPdate. The image used here can vary by sled and even over time (if the
    /// sled gets MUPdated again).
    ///
    /// Historically, this was the only source for zone images. In an system
    /// with automated control-plane-driven update we expect to only use this
    /// variant in emergencies where the system had to be recovered via MUPdate.
    InstallDataset,

    /// This zone's image source is the artifact matching this hash from the TUF
    /// artifact store (aka "TUF repo depot").
    ///
    /// This originates from TUF repos uploaded to Nexus which are then
    /// replicated out to all sleds.
    #[serde(rename_all = "snake_case")]
    Artifact { version: BlueprintArtifactVersion, hash: ArtifactHash },
}

impl BlueprintZoneImageSource {
    pub fn from_available_artifact(artifact: &TufArtifactMeta) -> Self {
        BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: artifact.id.version.clone(),
            },
            hash: artifact.hash,
        }
    }
}

impl From<BlueprintZoneImageSource> for OmicronZoneImageSource {
    fn from(source: BlueprintZoneImageSource) -> Self {
        match source {
            BlueprintZoneImageSource::InstallDataset => {
                OmicronZoneImageSource::InstallDataset
            }
            BlueprintZoneImageSource::Artifact { version: _, hash } => {
                OmicronZoneImageSource::Artifact { hash }
            }
        }
    }
}

impl fmt::Display for BlueprintZoneImageSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlueprintZoneImageSource::InstallDataset => {
                write!(f, "install dataset")
            }
            BlueprintZoneImageSource::Artifact { version, hash: _ } => {
                write!(f, "artifact: {version}")
            }
        }
    }
}

/// The version of an artifact in a blueprint.
///
/// This is used for debugging output.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
#[serde(tag = "artifact_version", rename_all = "snake_case")]
pub enum BlueprintArtifactVersion {
    /// A specific version of the image is available.
    Available { version: ArtifactVersion },

    /// The version could not be determined. This is non-fatal.
    Unknown,
}

impl fmt::Display for BlueprintArtifactVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlueprintArtifactVersion::Available { version } => {
                write!(f, "version {version}")
            }
            BlueprintArtifactVersion::Unknown => {
                write!(f, "(unknown version)")
            }
        }
    }
}

/// Describes the desired contents for both host phase 2 slots.
///
/// This is the blueprint version of [`HostPhase2DesiredSlots`].
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Diffable,
)]
#[serde(rename_all = "snake_case")]
pub struct BlueprintHostPhase2DesiredSlots {
    pub slot_a: BlueprintHostPhase2DesiredContents,
    pub slot_b: BlueprintHostPhase2DesiredContents,
}

impl From<BlueprintHostPhase2DesiredSlots> for HostPhase2DesiredSlots {
    fn from(value: BlueprintHostPhase2DesiredSlots) -> Self {
        Self { slot_a: value.slot_a.into(), slot_b: value.slot_b.into() }
    }
}

impl BlueprintHostPhase2DesiredSlots {
    /// Return a `BlueprintHostPhase2DesiredSlots` with both slots set to
    /// [`BlueprintHostPhase2DesiredContents::CurrentContents`]; i.e., "make no
    /// changes to the current contents of either slot".
    pub const fn current_contents() -> Self {
        Self {
            slot_a: BlueprintHostPhase2DesiredContents::CurrentContents,
            slot_b: BlueprintHostPhase2DesiredContents::CurrentContents,
        }
    }
}

/// Describes the desired contents of a host phase 2 slot (i.e., the boot
/// partition on one of the internal M.2 drives).
///
/// This is the blueprint version of [`HostPhase2DesiredContents`].
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlueprintHostPhase2DesiredContents {
    /// Do not change the current contents.
    ///
    /// We use this value when we've detected a sled has been mupdated (and we
    /// don't want to overwrite phase 2 images until we understand how to
    /// recover from that mupdate) and as the default value when reading a
    /// blueprint that was ledgered before this concept existed.
    CurrentContents,

    /// Set the phase 2 slot to the given artifact.
    ///
    /// The artifact will come from an unpacked and distributed TUF repo.
    Artifact { version: BlueprintArtifactVersion, hash: ArtifactHash },
}

impl From<BlueprintHostPhase2DesiredContents> for HostPhase2DesiredContents {
    fn from(value: BlueprintHostPhase2DesiredContents) -> Self {
        match value {
            BlueprintHostPhase2DesiredContents::CurrentContents => {
                Self::CurrentContents
            }
            BlueprintHostPhase2DesiredContents::Artifact { hash, .. } => {
                Self::Artifact { hash }
            }
        }
    }
}

impl BlueprintHostPhase2DesiredContents {
    pub fn artifact_hash(&self) -> Option<ArtifactHash> {
        match self {
            Self::CurrentContents => None,
            Self::Artifact { hash, .. } => Some(*hash),
        }
    }
}

impl fmt::Display for BlueprintHostPhase2DesiredContents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CurrentContents => {
                write!(f, "current contents")
            }
            Self::Artifact { version, hash: _ } => {
                write!(f, "artifact: {version}")
            }
        }
    }
}

#[derive(
    Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema, Diffable,
)]
pub struct PendingMgsUpdates {
    // The IdOrdMap key is the baseboard_id. Only one outstanding MGS-managed
    // update is allowed for a given baseboard.
    //
    // Note that keys aren't strings so this can't be serialized as a JSON map,
    // but IdOrdMap serializes as an array.
    by_baseboard: IdOrdMap<PendingMgsUpdate>,
}

impl PendingMgsUpdates {
    pub fn new() -> PendingMgsUpdates {
        PendingMgsUpdates { by_baseboard: IdOrdMap::new() }
    }

    pub fn iter(&self) -> impl Iterator<Item = &PendingMgsUpdate> {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.by_baseboard.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_baseboard.is_empty()
    }

    pub fn contains_key(&self, key: &BaseboardId) -> bool {
        self.by_baseboard.contains_key(key)
    }

    pub fn get(&self, baseboard_id: &BaseboardId) -> Option<&PendingMgsUpdate> {
        self.by_baseboard.get(baseboard_id)
    }

    pub fn get_mut(
        &mut self,
        baseboard_id: &BaseboardId,
    ) -> Option<RefMut<'_, PendingMgsUpdate>> {
        self.by_baseboard.get_mut(baseboard_id)
    }

    pub fn entry(
        &mut self,
        baseboard_id: &BaseboardId,
    ) -> Entry<'_, PendingMgsUpdate> {
        self.by_baseboard.entry(baseboard_id)
    }

    pub fn remove(
        &mut self,
        baseboard_id: &BaseboardId,
    ) -> Option<PendingMgsUpdate> {
        self.by_baseboard.remove(baseboard_id)
    }

    pub fn insert(
        &mut self,
        update: PendingMgsUpdate,
    ) -> Option<PendingMgsUpdate> {
        self.by_baseboard.insert_overwrite(update)
    }
}

impl<'a> IntoIterator for &'a PendingMgsUpdates {
    type Item = &'a PendingMgsUpdate;
    type IntoIter = iddqd::id_ord_map::Iter<'a, PendingMgsUpdate>;
    fn into_iter(self) -> Self::IntoIter {
        self.by_baseboard.iter()
    }
}

#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
pub struct PendingMgsUpdate {
    // identity of the baseboard
    /// id of the baseboard that we're going to update
    pub baseboard_id: Arc<BaseboardId>,

    // location of the baseboard (that we'd pass to MGS)
    /// what type of baseboard this is
    pub sp_type: SpType,
    /// last known MGS slot (cubby number) of the baseboard
    pub slot_id: u16,

    /// component-specific details of the pending update
    pub details: PendingMgsUpdateDetails,

    /// which artifact to apply to this device
    pub artifact_hash: ArtifactHash,
    pub artifact_version: ArtifactVersion,
}

impl slog::KV for PendingMgsUpdate {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::KV::serialize(&self.baseboard_id, record, serializer)?;
        serializer
            .emit_str(Key::from("sp_type"), &format!("{:?}", self.sp_type))?;
        serializer.emit_u16(Key::from("sp_slot"), self.slot_id)?;
        slog::KV::serialize(&self.details, record, serializer)?;
        serializer.emit_str(
            Key::from("artifact_hash"),
            &self.artifact_hash.to_string(),
        )?;
        serializer.emit_str(
            Key::from("artifact_version"),
            &self.artifact_version.to_string(),
        )
    }
}

impl IdOrdItem for PendingMgsUpdate {
    type Key<'a> = &'a BaseboardId;
    fn key(&self) -> Self::Key<'_> {
        &*self.baseboard_id
    }
    id_upcast!();
}

impl PendingMgsUpdate {
    fn to_bp_table_values(&self) -> Vec<String> {
        vec![
            self.sp_type.to_string(),
            self.slot_id.to_string(),
            self.baseboard_id.part_number.clone(),
            self.baseboard_id.serial_number.clone(),
            self.artifact_hash.to_string(),
            self.artifact_version.to_string(),
            format!("{:?}", self.details),
        ]
    }
}

/// Describes the component-specific details of a PendingMgsUpdate
// This needs to specify:
//
// - the "component" (that we provide to the SP)
// - the slot that needs to be updated
// - any preconditions we expect to be true.  These generally specify what we
//   think is in each slot.  This is intended to reduce the chance that an
//   update operation using outdated configuration winds up rolling back the
//   deployed version.
//
// Much of this may be implicit.  See comments below.
#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
#[serde(tag = "component", rename_all = "snake_case")]
pub enum PendingMgsUpdateDetails {
    /// the SP itself is being updated
    Sp {
        // implicit: component = SP_ITSELF
        // implicit: firmware slot id = 0 (always 0 for SP itself)
        /// expected contents of the active slot
        expected_active_version: ArtifactVersion,
        /// expected contents of the inactive slot
        expected_inactive_version: ExpectedVersion,
    },
    /// the RoT is being updated
    Rot {
        // implicit: component = ROT
        // implicit: firmware slot id will be the inactive slot
        // whether we expect the "A" or "B" slot to be active
        // and its expected version
        expected_active_slot: ExpectedActiveRotSlot,
        // expected version of the "A" or "B" slot (opposite to
        // the active slot as specified above)
        expected_inactive_version: ExpectedVersion,
        // under normal operation, this should always match the active slot.
        // if this field changed without the active slot changing, that might
        // reflect a bad update.
        //
        /// the persistent boot preference written into the current authoritative
        /// CFPA page (ping or pong)
        expected_persistent_boot_preference: RotSlot,
        // if this value changed, but not any of this other information, that could
        // reflect an attempt to switch to the other slot.
        //
        /// the persistent boot preference written into the CFPA scratch page that
        /// will become the persistent boot preference in the authoritative CFPA
        /// page upon reboot, unless CFPA update of the authoritative page fails
        /// for some reason.
        expected_pending_persistent_boot_preference: Option<RotSlot>,
        // this field is not in use yet.
        //
        /// override persistent preference selection for a single boot
        expected_transient_boot_preference: Option<RotSlot>,
    },
    RotBootloader {
        // implicit: component = STAGE0
        // implicit: firmware slot id = 1 (always 1 (Stage0Next) for RoT bootloader)
        /// expected contents of the stage 0
        expected_stage0_version: ArtifactVersion,
        /// expected contents of the stage 0 next
        expected_stage0_next_version: ExpectedVersion,
    },
    /// the host OS is being updated
    ///
    /// We write the phase 1 via MGS, and have a precheck condition that
    /// sled-agent has already written the matching phase 2.
    HostPhase1(PendingMgsUpdateHostPhase1Details),
}

impl slog::KV for PendingMgsUpdateDetails {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            } => {
                serializer.emit_str(Key::from("component"), "sp")?;
                serializer.emit_str(
                    Key::from("expected_active_version"),
                    &expected_active_version.to_string(),
                )?;
                serializer.emit_str(
                    Key::from("expected_inactive_version"),
                    &format!("{:?}", expected_inactive_version),
                )
            }
            PendingMgsUpdateDetails::Rot {
                expected_active_slot,
                expected_inactive_version,
                expected_persistent_boot_preference,
                expected_pending_persistent_boot_preference,
                expected_transient_boot_preference,
            } => {
                serializer.emit_str(Key::from("component"), "rot")?;
                serializer.emit_str(
                    Key::from("expected_inactive_version"),
                    &format!("{:?}", expected_inactive_version),
                )?;
                serializer.emit_str(
                    Key::from("expected_active_slot"),
                    &format!("{:?}", expected_active_slot),
                )?;
                serializer.emit_str(
                    Key::from("expected_persistent_boot_preference"),
                    &format!("{:?}", expected_persistent_boot_preference),
                )?;
                serializer.emit_str(
                    Key::from("expected_pending_persistent_boot_preference"),
                    &format!(
                        "{:?}",
                        expected_pending_persistent_boot_preference
                    ),
                )?;
                serializer.emit_str(
                    Key::from("expected_transient_boot_preference"),
                    &format!("{:?}", expected_transient_boot_preference),
                )
            }
            PendingMgsUpdateDetails::RotBootloader {
                expected_stage0_version,
                expected_stage0_next_version,
            } => {
                serializer
                    .emit_str(Key::from("component"), "rot_bootloader")?;
                serializer.emit_str(
                    Key::from("expected_stage0_version"),
                    &expected_stage0_version.to_string(),
                )?;
                serializer.emit_str(
                    Key::from("expected_stage0_next_version"),
                    &format!("{:?}", expected_stage0_next_version),
                )
            }
            PendingMgsUpdateDetails::HostPhase1(details) => {
                serializer.emit_str(Key::from("component"), "host_phase_1")?;
                slog::KV::serialize(details, record, serializer)
            }
        }
    }
}

/// Describes the host-phase-1-specific details of a PendingMgsUpdate
#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
#[serde(rename_all = "snake_case")]
pub struct PendingMgsUpdateHostPhase1Details {
    pub expected_active_slot: ExpectedActiveHostOsSlot,
    pub expected_inactive_artifact: ExpectedInactiveHostOsArtifact,
    pub sled_agent_address: SocketAddrV6,
}

impl slog::KV for PendingMgsUpdateHostPhase1Details {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self {
            expected_active_slot,
            expected_inactive_artifact,
            sled_agent_address,
        } = self;
        serializer.emit_str(
            Key::from("sled_agent_address"),
            &sled_agent_address.to_string(),
        )?;

        slog::KV::serialize(expected_active_slot, record, serializer)?;
        slog::KV::serialize(expected_inactive_artifact, record, serializer)
    }
}

#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
#[serde(rename_all = "snake_case")]
pub struct ExpectedActiveHostOsSlot {
    /// Which slot is currently active according to the SP.
    ///
    /// This controls which slot will be used the next time the sled boots; it
    /// will _usually_ match `boot_disk`, but differs in the window of time
    /// between telling the SP to change which slot to use and the host OS
    /// rebooting to actually use that slot.
    pub phase_1_slot: M2Slot,
    /// Which slot the host OS most recently booted from.
    pub boot_disk: M2Slot,
    /// The hash of the currently-active phase 1 artifact.
    ///
    /// We should always be able to fetch this. Even if the phase 1 contents
    /// themselves have been corrupted (very scary for the active slot!), the SP
    /// can still hash those contents.
    pub phase_1: ArtifactHash,
    /// The hash of the currently-active phase 2 artifact.
    ///
    /// It's possible sled-agent won't be able to report this value, but that
    /// would indicate that we don't know the version currently running. The
    /// planner wouldn't stage an update without knowing the current version, so
    /// if something has gone wrong in the meantime we won't proceede either.
    pub phase_2: ArtifactHash,
}

impl slog::KV for ExpectedActiveHostOsSlot {
    fn serialize(
        &self,
        _record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { phase_1_slot, boot_disk, phase_1, phase_2 } = self;
        serializer.emit_str(
            Key::from("active_phase_1_slot"),
            &format!("{phase_1_slot:?}"),
        )?;
        serializer
            .emit_str(Key::from("boot_disk"), &format!("{boot_disk:?}"))?;
        serializer.emit_str(
            Key::from("active_phase_1_artifact"),
            &phase_1.to_string(),
        )?;
        serializer.emit_str(
            Key::from("active_phase_2_artifact"),
            &phase_2.to_string(),
        )?;
        Ok(())
    }
}

#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
#[serde(rename_all = "snake_case")]
pub struct ExpectedInactiveHostOsArtifact {
    /// The hash of the currently-inactive phase 1 artifact.
    ///
    /// We should always be able to fetch this. Even if the phase 1 contents
    /// of the inactive slot are entirely bogus, the SP can still hash those
    /// contents.
    /// can still hash those contents.
    pub phase_1: ArtifactHash,
    /// The hash of the currently-inactive phase 2 artifact.
    ///
    /// It's entirely possible that a sled needing a host OS update has no valid
    /// artifact in its inactive slot. However, a precondition for us performing
    /// a phase 1 update is that `sled-agent` on the target sled has already
    /// written the paired phase 2 artifact to the inactive slot; therefore, we
    /// don't need to be able to represent an invalid inactive slot.
    pub phase_2: ArtifactHash,
}

impl slog::KV for ExpectedInactiveHostOsArtifact {
    fn serialize(
        &self,
        _record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { phase_1, phase_2 } = self;
        serializer.emit_str(
            Key::from("inactive_phase_1_artifact"),
            &phase_1.to_string(),
        )?;
        serializer.emit_str(
            Key::from("inactive_phase_2_artifact"),
            &phase_2.to_string(),
        )?;
        Ok(())
    }
}

/// Describes the version that we expect to find in some firmware slot
#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
#[serde(tag = "kind", content = "version", rename_all = "snake_case")]
pub enum ExpectedVersion {
    /// We expect to find _no_ valid caboose in this slot
    NoValidVersion,
    /// We expect to find the specified version in this slot
    Version(ArtifactVersion),
}

impl FromStr for ExpectedVersion {
    type Err = ArtifactVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "invalid" {
            Ok(ExpectedVersion::NoValidVersion)
        } else {
            Ok(ExpectedVersion::Version(s.parse()?))
        }
    }
}

impl fmt::Display for ExpectedVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedVersion::NoValidVersion => f.write_str("invalid"),
            ExpectedVersion::Version(v) => v.fmt(f),
        }
    }
}

/// Describes the expected active RoT slot, and the version we expect to find for it
#[derive(
    Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
pub struct ExpectedActiveRotSlot {
    pub slot: RotSlot,
    pub version: ArtifactVersion,
}

impl ExpectedActiveRotSlot {
    pub fn slot(&self) -> &RotSlot {
        &self.slot
    }

    pub fn version(&self) -> ArtifactVersion {
        self.version.clone()
    }
}

/// The desired state of an Omicron-managed physical disk in a blueprint.
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlueprintPhysicalDiskDisposition {
    /// The physical disk is in-service.
    InService,

    /// The physical disk is permanently gone.
    Expunged {
        /// Generation of the parent config in which this disk became expunged.
        as_of_generation: Generation,

        /// True if Reconfiguration knows that this disk has been expunged.
        ///
        /// In the current implementation, this means either:
        ///
        /// a) the sled where the disk was residing has been expunged.
        ///
        /// b) the planner has observed an inventory collection where the
        /// disk expungement was seen by the sled agent on the sled where the
        /// disk was previously in service. This is indicated by the inventory
        /// reporting a disk generation at least as high as `as_of_generation`.
        ready_for_cleanup: bool,
    },
}

impl BlueprintPhysicalDiskDisposition {
    /// Always returns true.
    ///
    /// This is intended for use with methods that take a filtering
    /// closure operating on a `BlueprintPhysicalDiskDisposition` (e.g.,
    /// `Blueprint::all_omicron_disks()`), allowing callers to make it clear
    /// they accept any disposition via
    ///
    /// ```rust,ignore
    /// blueprint.all_omicron_disks(BlueprintPhysicalDiskDisposition::any)
    /// ```
    pub fn any(self) -> bool {
        true
    }

    /// Returns true if `self` is `BlueprintZoneDisposition::InService`
    pub fn is_in_service(self) -> bool {
        matches!(self, Self::InService)
    }

    /// Returns true if `self` is `BlueprintPhysicalDiskDisposition::Expunged
    /// { .. }`, regardless of the details contained within that variant.
    pub fn is_expunged(self) -> bool {
        matches!(self, Self::Expunged { .. })
    }

    /// Returns true if `self` is `BlueprintPhysicalDiskDisposition::Expunged
    /// {ready_for_cleanup: true, ..}`
    pub fn is_ready_for_cleanup(self) -> bool {
        matches!(self, Self::Expunged { ready_for_cleanup: true, .. })
    }

    /// Return the generation when a disk was expunged or `None` if the disk
    /// was not expunged.
    pub fn expunged_as_of_generation(&self) -> Option<Generation> {
        match self {
            BlueprintPhysicalDiskDisposition::Expunged {
                as_of_generation,
                ..
            } => Some(*as_of_generation),
            _ => None,
        }
    }
}

impl fmt::Display for BlueprintPhysicalDiskDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Neither `write!(f, "...")` nor `f.write_str("...")` obey fill
            // and alignment (used above), but this does.
            BlueprintPhysicalDiskDisposition::InService => "in service".fmt(f),
            BlueprintPhysicalDiskDisposition::Expunged {
                ready_for_cleanup: true,
                ..
            } => "expunged ✓".fmt(f),
            BlueprintPhysicalDiskDisposition::Expunged {
                ready_for_cleanup: false,
                ..
            } => "expunged ⏳".fmt(f),
        }
    }
}

/// Information about an Omicron physical disk as recorded in a bluerprint.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Diffable,
)]
pub struct BlueprintPhysicalDiskConfig {
    pub disposition: BlueprintPhysicalDiskDisposition,
    #[daft(leaf)]
    pub identity: DiskIdentity,
    pub id: PhysicalDiskUuid,
    pub pool_id: ZpoolUuid,
}

impl IdMappable for BlueprintPhysicalDiskConfig {
    type Id = PhysicalDiskUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

impl From<BlueprintPhysicalDiskConfig> for OmicronPhysicalDiskConfig {
    fn from(value: BlueprintPhysicalDiskConfig) -> Self {
        OmicronPhysicalDiskConfig {
            identity: value.identity,
            id: value.id,
            pool_id: value.pool_id,
        }
    }
}

impl IdMappable for BlueprintDatasetConfig {
    type Id = DatasetUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

/// The desired state of an Omicron-managed dataset in a blueprint.
///
/// Part of [`BlueprintDatasetConfig`].
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    EnumIter,
    Diffable,
)]
#[serde(rename_all = "snake_case")]
pub enum BlueprintDatasetDisposition {
    /// The dataset is in-service.
    InService,

    /// The dataset is permanently gone.
    Expunged,
}

impl BlueprintDatasetDisposition {
    /// Always returns true.
    ///
    /// This is intended for use with methods that take a filtering
    /// closure operating on a `BlueprintDatasetDisposition` (e.g.,
    /// `Blueprint::all_omicron_datasets()`), allowing callers to make it clear
    /// they accept any disposition via
    ///
    /// ```rust,ignore
    /// blueprint.all_omicron_datasets(BlueprintDatasetDisposition::any)
    /// ```
    pub fn any(self) -> bool {
        true
    }

    /// Returns true if `self` is `BlueprintDatasetDisposition::InService`
    pub fn is_in_service(self) -> bool {
        matches!(self, Self::InService)
    }

    /// Returns true if `self` is `BlueprintDatasetDisposition::Expunged`,
    /// regardless of any details contained within that variant.
    pub fn is_expunged(self) -> bool {
        matches!(self, Self::Expunged)
    }
}

impl fmt::Display for BlueprintDatasetDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Neither `write!(f, "...")` nor `f.write_str("...")` obey fill
            // and alignment (used above), but this does.
            BlueprintDatasetDisposition::InService => "in service".fmt(f),
            BlueprintDatasetDisposition::Expunged => "expunged".fmt(f),
        }
    }
}

/// Information about a dataset as recorded in a blueprint
#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
pub struct BlueprintDatasetConfig {
    pub disposition: BlueprintDatasetDisposition,
    pub id: DatasetUuid,
    pub pool: ZpoolName,
    pub kind: DatasetKind,
    pub address: Option<SocketAddrV6>,
    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: CompressionAlgorithm,
}

impl From<BlueprintDatasetConfig> for DatasetConfig {
    fn from(config: BlueprintDatasetConfig) -> Self {
        Self {
            id: config.id,
            name: DatasetName::new(config.pool, config.kind),
            inner: SharedDatasetConfig {
                quota: config.quota,
                reservation: config.reservation,
                compression: config.compression,
            },
        }
    }
}

fn unwrap_or_none<T: ToString>(opt: &Option<T>) -> String {
    opt.as_ref().map(|v| v.to_string()).unwrap_or_else(|| "none".to_string())
}

impl BlueprintDatasetConfig {
    fn as_strings(&self) -> Vec<String> {
        vec![
            DatasetName::new(self.pool, self.kind.clone()).full_name(),
            self.id.to_string(),
            self.disposition.to_string(),
            unwrap_or_none(&self.quota),
            unwrap_or_none(&self.reservation),
            self.compression.to_string(),
        ]
    }
}

/// Describe high-level metadata about a blueprint
// These fields are a subset of [`Blueprint`], and include only the data we can
// quickly fetch from the main blueprint table (e.g., when listing all
// blueprints).
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Serialize)]
pub struct BlueprintMetadata {
    /// unique identifier for this blueprint
    pub id: BlueprintUuid,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<BlueprintUuid>,
    /// internal DNS version when this blueprint was created
    pub internal_dns_version: Generation,
    /// external DNS version when this blueprint was created
    pub external_dns_version: Generation,
    /// The minimum generation for the target release.
    ///
    /// See [`Blueprint::target_release_minimum_generation`].
    pub target_release_minimum_generation: Generation,
    /// CockroachDB state fingerprint when this blueprint was created
    pub cockroachdb_fingerprint: String,
    /// Whether to set `cluster.preserve_downgrade_option` and what to set it to
    /// (`None` if this value was retrieved from the database and was invalid)
    pub cockroachdb_setting_preserve_downgrade:
        Option<CockroachDbPreserveDowngrade>,

    /// when this blueprint was generated (for debugging)
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// identity of the component that generated the blueprint (for debugging)
    /// This would generally be the Uuid of a Nexus instance.
    pub creator: String,
    /// human-readable string describing why this blueprint was created
    /// (for debugging)
    pub comment: String,
}

/// Describes what blueprint, if any, the system is currently working toward
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct BlueprintTarget {
    /// id of the blueprint that the system is trying to make real
    pub target_id: BlueprintUuid,
    /// policy: should the system actively work towards this blueprint
    ///
    /// This should generally be left enabled.
    pub enabled: bool,
    /// when this blueprint was made the target
    pub time_made_target: chrono::DateTime<chrono::Utc>,
}

/// Specifies what blueprint, if any, the system should be working toward
#[derive(Deserialize, JsonSchema)]
pub struct BlueprintTargetSet {
    pub target_id: BlueprintUuid,
    pub enabled: bool,
}

/// A unique identifier for a dataset within a collection.
/// TODO: Should we use just the `DatasetUuid` and re-organize the tables to put the `DatasetUuid` first?
/// This was kept for backwards compatibility, even though IDs are not optional
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct CollectionDatasetIdentifier {
    name: String,
    id: Option<DatasetUuid>,
}

impl From<&BlueprintDatasetConfig> for CollectionDatasetIdentifier {
    fn from(d: &BlueprintDatasetConfig) -> Self {
        Self {
            id: Some(d.id),
            name: DatasetName::new(d.pool, d.kind.clone()).full_name(),
        }
    }
}

impl From<&crate::inventory::Dataset> for CollectionDatasetIdentifier {
    fn from(d: &crate::inventory::Dataset) -> Self {
        Self { id: d.id, name: d.name.clone() }
    }
}

/// Encapsulates Reconfigurator state
///
/// This serialized from is intended for saving state from hand-constructed or
/// real, deployed systems and loading it back into a simulator or test suite
///
/// **This format is not stable.  It may change at any time without
/// backwards-compatibility guarantees.**
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnstableReconfiguratorState {
    pub planning_input: PlanningInput,
    pub collections: Vec<Collection>,
    // When collected from a deployed system, `target_blueprint` will always be
    // `Some(_)`. `UnstableReconfiguratorState` is also used by
    // `reconfigurator-cli`, which allows construction of states that do not
    // represent a fully-deployed system (and maybe no blueprints at all, hence
    // no target blueprint).
    pub target_blueprint: Option<BlueprintTarget>,
    pub blueprints: Vec<Blueprint>,
    pub internal_dns: BTreeMap<Generation, DnsConfigParams>,
    pub external_dns: BTreeMap<Generation, DnsConfigParams>,
    pub silo_names: Vec<omicron_common::api::external::Name>,
    pub external_dns_zone_names: Vec<String>,
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::ExpectedVersion;
    use super::PendingMgsUpdate;
    use super::PendingMgsUpdateDetails;
    use super::PendingMgsUpdates;
    use crate::inventory::BaseboardId;
    use gateway_client::types::SpType;

    #[test]
    fn test_serialize_pending_mgs_updates() {
        // Trivial case: empty map
        let empty = PendingMgsUpdates::new();
        let empty_serialized = serde_json::to_string(&empty).unwrap();
        assert_eq!(empty_serialized, r#"{"by_baseboard":[]}"#);
        let empty_deserialized: PendingMgsUpdates =
            serde_json::from_str(&empty_serialized).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty, empty_deserialized);

        // Non-trivial case: contains an element.
        let mut pending_mgs_updates = PendingMgsUpdates::new();
        let update = PendingMgsUpdate {
            baseboard_id: Arc::new(BaseboardId {
                part_number: String::from("913-0000019"),
                serial_number: String::from("BRM27230037"),
            }),
            sp_type: SpType::Sled,
            slot_id: 15,
            details: PendingMgsUpdateDetails::Sp {
                expected_active_version: "1.0.36".parse().unwrap(),
                expected_inactive_version: ExpectedVersion::Version(
                    "1.0.36".parse().unwrap(),
                ),
            },
            artifact_hash: "47266ede81e13f5f1e36623ea8dd963842606b783397e4809a9a5f0bda0f8170".parse().unwrap(),
            artifact_version: "1.0.34".parse().unwrap(),
        };
        pending_mgs_updates.insert(update);
        let serialized = serde_json::to_string(&pending_mgs_updates).unwrap();
        let deserialized: PendingMgsUpdates =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, pending_mgs_updates);
        assert!(!deserialized.is_empty());
    }
}
