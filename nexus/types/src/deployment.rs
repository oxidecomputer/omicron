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
use daft::Diffable;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::OmicronZonesConfig;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use strum::EnumIter;

mod blueprint_diff;
mod blueprint_display;
mod clickhouse;
pub mod execution;
pub mod id_map;
mod network_resources;
mod planning_input;
mod tri_map;
mod zone_type;

pub use clickhouse::ClickhouseClusterConfig;
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
pub use planning_input::ZpoolFilter;
pub use zone_type::blueprint_zone_type;
pub use zone_type::BlueprintZoneType;
pub use zone_type::DurableDataset;

use blueprint_display::{
    constants::*, BpDiffState, BpGeneration, BpOmicronZonesTableSchema,
    BpPhysicalDisksTableSchema, BpTable, BpTableData, BpTableRow,
    KvListWithHeading,
};
use id_map::{IdMap, IdMappable};

pub use blueprint_diff::BlueprintDiffSummary;

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

    /// A map of sled id -> desired state of the sled.
    ///
    /// A sled is considered part of the control plane cluster iff it has an
    /// entry in this map.
    pub sled_state: BTreeMap<SledUuid, SledState>,

    /// A map of sled id -> zones deployed on each sled, along with the
    /// [`BlueprintZoneDisposition`] for each zone.
    ///
    /// Unlike `sled_state`, this map may contain entries for sleds that are no
    /// longer a part of the control plane cluster (e.g., sleds that have been
    /// decommissioned, but still have expunged zones where cleanup has not yet
    /// completed).
    pub blueprint_zones: BTreeMap<SledUuid, BlueprintZonesConfig>,

    /// A map of sled id -> disks in use on each sled.
    pub blueprint_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,

    /// A map of sled id -> datasets in use on each sled
    pub blueprint_datasets: BTreeMap<SledUuid, BlueprintDatasetsConfig>,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<BlueprintUuid>,

    /// internal DNS version when this blueprint was created
    // See blueprint execution for more on this.
    pub internal_dns_version: Generation,

    /// external DNS version when thi blueprint was created
    // See blueprint execution for more on this.
    pub external_dns_version: Generation,

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
        Blueprint::filtered_zones(&self.blueprint_zones, filter)
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances that match the
    /// provided filter, along with the associated sled id.
    //
    // This is a scoped function so that it can be used in the
    // `BlueprintBuilder` during planning as well as in the `Blueprint`.
    pub fn filtered_zones<F>(
        zones_by_sled_id: &BTreeMap<SledUuid, BlueprintZonesConfig>,
        mut filter: F,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        zones_by_sled_id
            .iter()
            .flat_map(move |(sled_id, z)| {
                z.zones.iter().map(move |z| (*sled_id, z))
            })
            .filter(move |(_, z)| filter(z.disposition))
    }

    /// Iterate over the [`BlueprintDatasetsConfig`] instances in the blueprint.
    pub fn all_omicron_datasets(
        &self,
        filter: BlueprintDatasetFilter,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintDatasetConfig)> {
        self.blueprint_datasets
            .iter()
            .flat_map(move |(sled_id, datasets)| {
                datasets.datasets.iter().map(|dataset| (*sled_id, dataset))
            })
            .filter(move |(_, d)| d.disposition.matches(filter))
    }

    /// Iterate over the ids of all sleds in the blueprint
    pub fn sleds(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.blueprint_zones.keys().copied()
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

impl BpTableData for &BlueprintPhysicalDisksConfig {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Value(self.generation)
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        let sorted_disk_ids: BTreeSet<DiskIdentity> =
            self.disks.iter().map(|d| d.identity.clone()).collect();

        sorted_disk_ids.into_iter().map(move |d| {
            BpTableRow::from_strings(state, vec![d.vendor, d.model, d.serial])
        })
    }
}

impl BpTableData for BlueprintDatasetsConfig {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Value(self.generation)
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        // We want to sort by (kind, pool)
        let mut datasets: Vec<_> = self.datasets.iter().collect();
        datasets.sort_unstable_by_key(|d| (&d.kind, &d.pool));
        datasets.into_iter().map(move |dataset| {
            BpTableRow::from_strings(state, dataset.as_strings())
        })
    }
}

impl BpTableData for BlueprintZonesConfig {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Value(self.generation)
    }

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
                    zone.disposition.to_string(),
                    zone.underlay_ip().to_string(),
                ],
            )
        })
    }
}

// Useful implementation for printing two column tables stored in a `BTreeMap`, where
// each key and value impl `Display`.
impl<S1, S2> BpTableData for (Generation, &BTreeMap<S1, S2>)
where
    S1: fmt::Display,
    S2: fmt::Display,
{
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Value(self.0)
    }

    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow> {
        self.1.iter().map(move |(s1, s2)| {
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
    fn make_cockroachdb_table(&self) -> KvListWithHeading {
        let fingerprint = if self.blueprint.cockroachdb_fingerprint.is_empty() {
            NONE_PARENS.to_string()
        } else {
            self.blueprint.cockroachdb_fingerprint.clone()
        };

        KvListWithHeading::new_unchanged(
            COCKROACHDB_HEADING,
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

    fn make_metadata_table(&self) -> KvListWithHeading {
        let comment = if self.blueprint.comment.is_empty() {
            NONE_PARENS.to_string()
        } else {
            self.blueprint.comment.clone()
        };

        KvListWithHeading::new_unchanged(
            METADATA_HEADING,
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
            ],
        )
    }

    // Return tables representing a [`ClickhouseClusterConfig`] in a given blueprint
    fn make_clickhouse_cluster_config_tables(
        &self,
    ) -> Option<(KvListWithHeading, BpTable, BpTable)> {
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
            sled_state,
            blueprint_zones,
            blueprint_disks,
            blueprint_datasets,
            parent_blueprint_id,
            // These two cockroachdb_* fields are handled by
            // `make_cockroachdb_table()`, called below.
            cockroachdb_fingerprint: _,
            cockroachdb_setting_preserve_downgrade: _,
            // Handled by `make_clickhouse_cluster_config_tables()`, called
            // below.
            clickhouse_cluster_config: _,
            // These five fields are handled by `make_metadata_table()`, called
            // below.
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

        // Keep track of any sled_ids that have been seen in the first loop.
        let mut seen_sleds = BTreeSet::new();

        // Loop through all sleds that have physical disks and print a table of
        // those physical disks.
        //
        // If there are corresponding zones, print those as well.
        for (sled_id, disks) in blueprint_disks {
            // Construct the disks subtable
            let disks_table = BpTable::new(
                BpPhysicalDisksTableSchema {},
                disks.bp_generation(),
                disks.rows(BpDiffState::Unchanged).collect(),
            );

            // Look up the sled state
            let sled_state = sled_state
                .get(sled_id)
                .map(|state| state.to_string())
                .unwrap_or_else(|| {
                    "blueprint error: unknown sled state".to_string()
                });
            writeln!(
                f,
                "\n  sled: {sled_id} ({sled_state})\n\n{disks_table}\n",
            )?;

            // Construct the datasets subtable
            if let Some(datasets) = blueprint_datasets.get(sled_id) {
                let datasets_tab = BpTable::new(
                    BpDatasetsTableSchema {},
                    datasets.bp_generation(),
                    datasets.rows(BpDiffState::Unchanged).collect(),
                );
                writeln!(f, "{datasets_tab}\n")?;
            }

            // Construct the zones subtable
            if let Some(zones) = blueprint_zones.get(sled_id) {
                let zones_tab = BpTable::new(
                    BpOmicronZonesTableSchema {},
                    zones.bp_generation(),
                    zones.rows(BpDiffState::Unchanged).collect(),
                );
                writeln!(f, "{zones_tab}\n")?;
            }
            seen_sleds.insert(sled_id);
        }

        // Now create and display a table of zones/datasets on sleds that don't
        // have physical disks.
        //
        // This should basically be impossible, so we warn if it occurs.
        for (sled_id, zones) in blueprint_zones {
            if !seen_sleds.contains(sled_id) && !zones.zones.is_empty() {
                writeln!(
                    f,
                    "!{sled_id}\n{}\n{}\n\n",
                    "WARNING: Zones exist without physical disks!",
                    BpTable::new(
                        BpOmicronZonesTableSchema {},
                        zones.bp_generation(),
                        zones.rows(BpDiffState::Unchanged).collect()
                    )
                )?;
                if let Some(datasets) = blueprint_datasets.get(sled_id) {
                    writeln!(
                        f,
                        "{}\n{}\n",
                        "WARNING: Datasets also exist without physical disks!",
                        BpTable::new(
                            BpDatasetsTableSchema {},
                            datasets.bp_generation(),
                            datasets.rows(BpDiffState::Unchanged).collect(),
                        )
                    )?;
                }
                seen_sleds.insert(sled_id);
            }
        }

        // Finally, create and display a table of datasets on sleds that don't
        // have disks or zones.
        //
        // This should basically be impossible, so we warn if it occurs.
        for (sled_id, datasets) in blueprint_datasets {
            if !seen_sleds.contains(sled_id) && !datasets.datasets.is_empty() {
                writeln!(
                    f,
                    "!{sled_id}\n{}\n{}\n\n",
                    "WARNING: Datasets exist without physical disks or zones!",
                    BpTable::new(
                        BpDatasetsTableSchema {},
                        datasets.bp_generation(),
                        datasets.rows(BpDiffState::Unchanged).collect(),
                    )
                )?;
            }
        }

        if let Some((t1, t2, t3)) = self.make_clickhouse_cluster_config_tables()
        {
            writeln!(f, "{t1}\n{t2}\n{t3}\n")?;
        }

        writeln!(f, "{}", self.make_cockroachdb_table())?;
        writeln!(f, "{}", self.make_metadata_table())?;

        Ok(())
    }
}

/// Information about an Omicron zone as recorded in a blueprint.
///
/// Currently, this is similar to [`OmicronZonesConfig`], but also contains a
/// per-zone [`BlueprintZoneDisposition`].
///
/// Part of [`Blueprint`].
#[derive(
    Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
pub struct BlueprintZonesConfig {
    /// Generation number of this configuration.
    ///
    /// This generation number is owned by the control plane. See
    /// [`OmicronZonesConfig::generation`] for more details.
    pub generation: Generation,

    /// The set of running zones.
    pub zones: IdMap<BlueprintZoneConfig>,
}

impl BlueprintZonesConfig {
    /// Converts self into [`OmicronZonesConfig`].
    ///
    /// [`OmicronZonesConfig`] is a format of the zones configuration that can
    /// be passed to Sled Agents for deployment.
    ///
    /// This function is effectively a `From` implementation, but
    /// is named slightly more explicitly, as it filters the blueprint
    /// configuration to only consider zones that should be running.
    pub fn into_running_omicron_zones_config(self) -> OmicronZonesConfig {
        OmicronZonesConfig {
            generation: self.generation,
            zones: self
                .zones
                .into_iter()
                .filter_map(|z| {
                    if z.disposition.is_in_service() {
                        Some(z.into())
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }

    /// Returns true if all zones in the blueprint have a disposition of
    /// `Expunged`, false otherwise.
    pub fn are_all_zones_expunged(&self) -> bool {
        self.zones.iter().all(|c| {
            matches!(c.disposition, BlueprintZoneDisposition::Expunged { .. })
        })
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
/// Part of [`BlueprintZonesConfig`].
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
    pub filesystem_pool: Option<ZpoolName>,
    pub zone_type: BlueprintZoneType,
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
    pub fn filesystem_dataset(&self) -> Option<DatasetName> {
        let pool_name = self.filesystem_pool.clone()?;
        let name = illumos_utils::zone::zone_name(
            self.zone_type.kind().zone_prefix(),
            Some(self.id),
        );
        let kind = DatasetKind::TransientZone { name };
        Some(DatasetName::new(pool_name, kind))
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
        } = z;
        Self {
            id,
            filesystem_pool,
            zone_type: zone_type.into(),
            image_source: OmicronZoneImageSource::InstallDataset,
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

/// Filters that apply to blueprint datasets.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BlueprintDatasetFilter {
    // ---
    // Prefer to keep this list in alphabetical order.
    // ---
    /// All datasets
    All,

    /// Datasets that have been expunged.
    Expunged,

    /// Datasets that are in-service.
    InService,
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
    EnumIter,
    Diffable,
)]
#[serde(rename_all = "snake_case")]
pub enum BlueprintPhysicalDiskDisposition {
    /// The physical disk is in-service.
    InService,

    /// The physical disk is permanently gone.
    Expunged,
}

impl BlueprintPhysicalDiskDisposition {
    /// Returns true if the disk disposition matches this filter.
    pub fn matches(self, filter: DiskFilter) -> bool {
        match self {
            Self::InService => match filter {
                DiskFilter::All => true,
                DiskFilter::InService => true,
                // TODO remove this variant?
                DiskFilter::ExpungedButActive => false,
            },
            Self::Expunged => match filter {
                DiskFilter::All => true,
                DiskFilter::InService => false,
                // TODO remove this variant?
                DiskFilter::ExpungedButActive => true,
            },
        }
    }
}

/// Information about an Omicron physical disk as recorded in a bluerprint.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Diffable,
)]
pub struct BlueprintPhysicalDiskConfig {
    pub disposition: BlueprintPhysicalDiskDisposition,
    pub identity: DiskIdentity,
    pub id: PhysicalDiskUuid,
    pub pool_id: ZpoolUuid,
}

/// Information about Omicron physical disks as recorded in a blueprint.
///
/// Part of [`Blueprint`].
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Diffable,
)]
pub struct BlueprintPhysicalDisksConfig {
    pub generation: Generation,
    pub disks: IdMap<BlueprintPhysicalDiskConfig>,
}

impl BlueprintPhysicalDisksConfig {
    /// Converts self into [`OmicronPhysicalDisksConfig`].
    ///
    /// [`OmicronPhysicalDisksConfig`] is a format of the disks configuration
    /// that can be passed to Sled Agents for deployment.
    ///
    /// This function is effectively a `From` implementation, but
    /// is named slightly more explicitly, as it filters the blueprint
    /// configuration to only consider in-service disks.
    pub fn into_in_service_disks(self) -> OmicronPhysicalDisksConfig {
        OmicronPhysicalDisksConfig {
            generation: self.generation,
            disks: self
                .disks
                .into_iter()
                .filter_map(|d| {
                    if d.disposition.matches(DiskFilter::InService) {
                        Some(d.into())
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }
}

impl IdMappable for BlueprintPhysicalDiskConfig {
    type Id = PhysicalDiskUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

// Required by RSS
impl Default for BlueprintPhysicalDisksConfig {
    fn default() -> Self {
        BlueprintPhysicalDisksConfig {
            generation: Generation::new(),
            disks: IdMap::new(),
        }
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

/// Information about Omicron datasets as recorded in a blueprint.
#[derive(
    Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize, Diffable,
)]
pub struct BlueprintDatasetsConfig {
    pub generation: Generation,
    pub datasets: IdMap<BlueprintDatasetConfig>,
}

impl BlueprintDatasetsConfig {
    /// Converts self into [DatasetsConfig].
    ///
    /// [DatasetsConfig] is a format of the dataset configuration that can be
    /// passed to Sled Agents for deployment.
    ///
    /// This function is effectively a [std::convert::From] implementation, but
    /// is named slightly more explicitly, as it filters the blueprint
    /// configuration to only consider in-service datasets.
    pub fn into_in_service_datasets(self) -> DatasetsConfig {
        DatasetsConfig {
            generation: self.generation,
            datasets: self
                .datasets
                .into_iter()
                .filter_map(|d| {
                    if d.disposition.matches(BlueprintDatasetFilter::InService)
                    {
                        Some((d.id, d.into()))
                    } else {
                        None
                    }
                })
                .collect(),
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
    pub fn matches(self, filter: BlueprintDatasetFilter) -> bool {
        match self {
            Self::InService => match filter {
                BlueprintDatasetFilter::All => true,
                BlueprintDatasetFilter::Expunged => false,
                BlueprintDatasetFilter::InService => true,
            },
            Self::Expunged => match filter {
                BlueprintDatasetFilter::All => true,
                BlueprintDatasetFilter::Expunged => true,
                BlueprintDatasetFilter::InService => false,
            },
        }
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
            DatasetName::new(self.pool.clone(), self.kind.clone()).full_name(),
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
            name: DatasetName::new(d.pool.clone(), d.kind.clone()).full_name(),
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
