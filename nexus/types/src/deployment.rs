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
use derive_more::From;
use newtype_uuid::GenericUuid;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
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
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use strum::EnumIter;
use strum::IntoEnumIterator;
use uuid::Uuid;

mod blueprint_diff;
mod blueprint_display;
mod network_resources;
mod planning_input;
mod tri_map;
mod zone_type;

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
pub use planning_input::SledResources;
pub use planning_input::ZpoolFilter;
pub use zone_type::blueprint_zone_type;
pub use zone_type::BlueprintZoneType;
pub use zone_type::DurableDataset;

use blueprint_display::{
    constants::*, BpDiffState, BpGeneration, BpOmicronZonesSubtableSchema,
    BpPhysicalDisksSubtableSchema, BpSledSubtable, BpSledSubtableData,
    BpSledSubtableRow, KvListWithHeading,
};

pub use blueprint_diff::BlueprintDiff;

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
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct Blueprint {
    /// unique identifier for this blueprint
    pub id: Uuid,

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
    pub parent_blueprint_id: Option<Uuid>,

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

    /// when this blueprint was generated (for debugging)
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
    pub fn all_omicron_zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)> {
        self.blueprint_zones.iter().flat_map(move |(sled_id, z)| {
            z.zones
                .iter()
                .filter(move |z| z.disposition.matches(filter))
                .map(|z| (*sled_id, z))
        })
    }

    /// Iterate over the [`BlueprintDatasetsConfig`] instances in the blueprint.
    pub fn all_omicron_datasets(
        &self,
        filter: BlueprintDatasetFilter,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig> {
        self.blueprint_datasets
            .iter()
            .flat_map(move |(_, datasets)| datasets.datasets.values())
            .filter(move |d| d.disposition.matches(filter))
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances in the blueprint
    /// that do not match the provided filter, along with the associated sled
    /// id.
    pub fn all_omicron_zones_not_in(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)> {
        self.blueprint_zones.iter().flat_map(move |(sled_id, z)| {
            z.zones
                .iter()
                .filter(move |z| !z.disposition.matches(filter))
                .map(|z| (*sled_id, z))
        })
    }

    /// Iterate over the ids of all sleds in the blueprint
    pub fn sleds(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.blueprint_zones.keys().copied()
    }

    /// Summarize the difference between sleds and zones between two
    /// blueprints.
    ///
    /// The argument provided is the "before" side, and `self` is the "after"
    /// side. This matches the order of arguments to
    /// [`Blueprint::diff_since_collection`].
    pub fn diff_since_blueprint(&self, before: &Blueprint) -> BlueprintDiff {
        BlueprintDiff::new(
            DiffBeforeMetadata::Blueprint(Box::new(before.metadata())),
            before
                .blueprint_zones
                .iter()
                .map(|(sled_id, zones)| (*sled_id, zones.clone().into()))
                .collect(),
            self.metadata(),
            self.blueprint_zones.clone(),
            before
                .blueprint_disks
                .iter()
                .map(|(sled_id, disks)| (*sled_id, disks.clone().into()))
                .collect(),
            self.blueprint_disks.clone(),
        )
    }

    /// Summarize the differences in sleds and zones between a collection and a
    /// blueprint.
    ///
    /// This gives an idea about what would change about a running system if
    /// one were to execute the blueprint.
    ///
    /// Note that collections do not include information about zone
    /// disposition, so it is assumed that all zones in the collection have the
    /// [`InService`](BlueprintZoneDisposition::InService) disposition.
    pub fn diff_since_collection(&self, before: &Collection) -> BlueprintDiff {
        let before_zones = before
            .omicron_zones
            .iter()
            .map(|(sled_id, zones_found)| {
                (*sled_id, zones_found.zones.clone().into())
            })
            .collect();

        let before_disks = before
            .sled_agents
            .iter()
            .map(|(sled_id, sa)| {
                (
                    *sled_id,
                    CollectionPhysicalDisksConfig {
                        disks: sa
                            .disks
                            .iter()
                            .map(|d| d.identity.clone())
                            .collect::<BTreeSet<_>>(),
                    }
                    .into(),
                )
            })
            .collect();

        BlueprintDiff::new(
            DiffBeforeMetadata::Collection { id: before.id },
            before_zones,
            self.metadata(),
            self.blueprint_zones.clone(),
            before_disks,
            self.blueprint_disks.clone(),
        )
    }

    /// Return a struct that can be displayed to present information about the
    /// blueprint.
    pub fn display(&self) -> BlueprintDisplay<'_> {
        BlueprintDisplay { blueprint: self }
    }
}

impl BpSledSubtableData for &OmicronPhysicalDisksConfig {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Value(self.generation)
    }

    fn rows(
        &self,
        state: BpDiffState,
    ) -> impl Iterator<Item = BpSledSubtableRow> {
        let sorted_disk_ids: BTreeSet<DiskIdentity> =
            self.disks.iter().map(|d| d.identity.clone()).collect();

        sorted_disk_ids.into_iter().map(move |d| {
            BpSledSubtableRow::from_strings(
                state,
                vec![d.vendor, d.model, d.serial],
            )
        })
    }
}

impl BpSledSubtableData for BlueprintOrCollectionZonesConfig {
    fn bp_generation(&self) -> BpGeneration {
        BpGeneration::Value(self.generation())
    }

    fn rows(
        &self,
        state: BpDiffState,
    ) -> impl Iterator<Item = BpSledSubtableRow> {
        self.zones().map(move |zone| {
            BpSledSubtableRow::from_strings(
                state,
                vec![
                    zone.kind().report_str().to_string(),
                    zone.id().to_string(),
                    zone.disposition().to_string(),
                    zone.underlay_address().to_string(),
                ],
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

impl<'a> BlueprintDisplay<'a> {
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
}

impl<'a> fmt::Display for BlueprintDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.blueprint;
        writeln!(f, "blueprint  {}", b.id)?;
        writeln!(
            f,
            "parent:    {}",
            b.parent_blueprint_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| String::from("<none>"))
        )?;

        // Keep track of any sled_ids that have been seen in the first loop.
        let mut seen_sleds = BTreeSet::new();

        // Loop through all sleds that have physical disks and print a table of
        // those physical disks.
        //
        // If there are corresponding zones, print those as well.
        for (sled_id, disks) in &self.blueprint.blueprint_disks {
            // Construct the disks subtable
            let disks_table = BpSledSubtable::new(
                BpPhysicalDisksSubtableSchema {},
                disks.bp_generation(),
                disks.rows(BpDiffState::Unchanged).collect(),
            );

            // Construct the zones subtable
            match self.blueprint.blueprint_zones.get(sled_id) {
                Some(zones) => {
                    let zones =
                        BlueprintOrCollectionZonesConfig::from(zones.clone());
                    let zones_tab = BpSledSubtable::new(
                        BpOmicronZonesSubtableSchema {},
                        zones.bp_generation(),
                        zones.rows(BpDiffState::Unchanged).collect(),
                    );
                    writeln!(
                        f,
                        "\n  sled: {sled_id}\n\n{disks_table}\n\n{zones_tab}\n"
                    )?;
                }
                None => writeln!(f, "\n  sled: {sled_id}\n\n{disks_table}\n")?,
            }
            seen_sleds.insert(sled_id);
        }

        // Now create and display a table of zones on sleds that don't
        // yet have physical disks.
        //
        // This should basically be impossible, so we warn if it occurs.
        for (sled_id, zones) in &self.blueprint.blueprint_zones {
            if !seen_sleds.contains(sled_id) && !zones.zones.is_empty() {
                let zones =
                    BlueprintOrCollectionZonesConfig::from(zones.clone());
                writeln!(
                    f,
                    "\n!{sled_id}\n{}\n{}\n\n",
                    "WARNING: Zones exist without physical disks!",
                    BpSledSubtable::new(
                        BpOmicronZonesSubtableSchema {},
                        zones.bp_generation(),
                        zones.rows(BpDiffState::Unchanged).collect()
                    )
                )?;
            }
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
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct BlueprintZonesConfig {
    /// Generation number of this configuration.
    ///
    /// This generation number is owned by the control plane. See
    /// [`OmicronZonesConfig::generation`] for more details.
    pub generation: Generation,

    /// The list of running zones.
    pub zones: Vec<BlueprintZoneConfig>,
}

impl From<BlueprintZonesConfig> for OmicronZonesConfig {
    fn from(config: BlueprintZonesConfig) -> Self {
        Self {
            generation: config.generation,
            zones: config.zones.into_iter().map(From::from).collect(),
        }
    }
}

impl BlueprintZonesConfig {
    /// Sorts the list of zones stored in this configuration.
    ///
    /// This is not strictly necessary. But for testing (particularly snapshot
    /// testing), it's helpful for zones to be in sorted order.
    pub fn sort(&mut self) {
        self.zones.sort_unstable_by_key(zone_sort_key);
    }

    /// Converts self to an [`OmicronZonesConfig`], applying the provided
    /// [`BlueprintZoneFilter`].
    ///
    /// The filter controls which zones should be exported into the resulting
    /// [`OmicronZonesConfig`].
    pub fn to_omicron_zones_config(
        &self,
        filter: BlueprintZoneFilter,
    ) -> OmicronZonesConfig {
        OmicronZonesConfig {
            generation: self.generation,
            zones: self
                .zones
                .iter()
                .filter(|z| z.disposition.matches(filter))
                .cloned()
                .map(OmicronZoneConfig::from)
                .collect(),
        }
    }

    /// Returns true if all zones in the blueprint have a disposition of
    /// `Expunged`, false otherwise.
    pub fn are_all_zones_expunged(&self) -> bool {
        self.zones
            .iter()
            .all(|c| c.disposition == BlueprintZoneDisposition::Expunged)
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
        OmicronZoneUuid::from_untyped_uuid(self.id)
    }
}

impl ZoneSortKey for BlueprintOrCollectionZoneConfig {
    fn kind(&self) -> ZoneKind {
        BlueprintOrCollectionZoneConfig::kind(self)
    }

    fn id(&self) -> OmicronZoneUuid {
        BlueprintOrCollectionZoneConfig::id(self)
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
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct BlueprintZoneConfig {
    /// The disposition (desired state) of this zone recorded in the blueprint.
    pub disposition: BlueprintZoneDisposition,

    pub id: OmicronZoneUuid,
    pub underlay_address: Ipv6Addr,
    pub filesystem_pool: Option<ZpoolName>,
    pub zone_type: BlueprintZoneType,
}

impl From<BlueprintZoneConfig> for OmicronZoneConfig {
    fn from(z: BlueprintZoneConfig) -> Self {
        Self {
            id: z.id.into_untyped_uuid(),
            underlay_address: z.underlay_address,
            filesystem_pool: z.filesystem_pool,
            zone_type: z.zone_type.into(),
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
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum BlueprintZoneDisposition {
    /// The zone is in-service.
    InService,

    /// The zone is not in service.
    Quiesced,

    /// The zone is permanently gone.
    Expunged,
}

impl BlueprintZoneDisposition {
    /// Returns true if the zone disposition matches this filter.
    pub fn matches(self, filter: BlueprintZoneFilter) -> bool {
        // This code could be written in three ways:
        //
        // 1. match self { match filter { ... } }
        // 2. match filter { match self { ... } }
        // 3. match (self, filter) { ... }
        //
        // We choose 1 here because we expect many filters and just a few
        // dispositions, and 1 is the easiest form to represent that.
        match self {
            Self::InService => match filter {
                BlueprintZoneFilter::All => true,
                BlueprintZoneFilter::Expunged => false,
                BlueprintZoneFilter::ShouldBeRunning => true,
                BlueprintZoneFilter::ShouldBeExternallyReachable => true,
                BlueprintZoneFilter::ShouldBeInInternalDns => true,
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules => true,
            },
            Self::Quiesced => match filter {
                BlueprintZoneFilter::All => true,
                BlueprintZoneFilter::Expunged => false,

                // Quiesced zones are still running.
                BlueprintZoneFilter::ShouldBeRunning => true,

                // Quiesced zones should not have external resources -- we do
                // not want traffic to be directed to them.
                BlueprintZoneFilter::ShouldBeExternallyReachable => false,

                // Quiesced zones should not be exposed in DNS.
                BlueprintZoneFilter::ShouldBeInInternalDns => false,

                // Quiesced zones should get firewall rules.
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules => true,
            },
            Self::Expunged => match filter {
                BlueprintZoneFilter::All => true,
                BlueprintZoneFilter::Expunged => true,
                BlueprintZoneFilter::ShouldBeRunning => false,
                BlueprintZoneFilter::ShouldBeExternallyReachable => false,
                BlueprintZoneFilter::ShouldBeInInternalDns => false,
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules => false,
            },
        }
    }

    /// Returns all zone dispositions that match the given filter.
    pub fn all_matching(
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = Self> {
        BlueprintZoneDisposition::iter().filter(move |&d| d.matches(filter))
    }
}

impl fmt::Display for BlueprintZoneDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Neither `write!(f, "...")` nor `f.write_str("...")` obey fill
            // and alignment (used above), but this does.
            BlueprintZoneDisposition::InService => "in service".fmt(f),
            BlueprintZoneDisposition::Quiesced => "quiesced".fmt(f),
            BlueprintZoneDisposition::Expunged => "expunged".fmt(f),
        }
    }
}

/// Filters that apply to blueprint zones.
///
/// This logic lives here rather than within the individual components making
/// decisions, so that this is easier to read.
///
/// The meaning of a particular filter should not be overloaded -- each time a
/// new use case wants to make a decision based on the zone disposition, a new
/// variant should be added to this enum.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BlueprintZoneFilter {
    // ---
    // Prefer to keep this list in alphabetical order.
    // ---
    /// All zones.
    All,

    /// Zones that have been expunged.
    Expunged,

    /// Zones that are desired to be in the RUNNING state
    ShouldBeRunning,

    /// Filter by zones that should have external IP and DNS resources.
    ShouldBeExternallyReachable,

    /// Filter by zones that should be in internal DNS.
    ShouldBeInInternalDns,

    /// Filter by zones that should be sent VPC firewall rules.
    ShouldDeployVpcFirewallRules,
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

/// Information about an Omicron physical disk as recorded in a blueprint.
///
/// Part of [`Blueprint`].
pub type BlueprintPhysicalDisksConfig =
    omicron_common::disk::OmicronPhysicalDisksConfig;

pub type BlueprintPhysicalDiskConfig =
    omicron_common::disk::OmicronPhysicalDiskConfig;

/// Information about Omicron datasets as recorded in a blueprint.
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct BlueprintDatasetsConfig {
    pub generation: Generation,
    pub datasets: BTreeMap<DatasetUuid, BlueprintDatasetConfig>,
}

impl From<BlueprintDatasetsConfig> for DatasetsConfig {
    fn from(config: BlueprintDatasetsConfig) -> Self {
        Self {
            generation: config.generation,
            datasets: config
                .datasets
                .into_iter()
                .map(|(id, d)| (id, d.into()))
                .collect(),
        }
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

/// Information about a dataset as recorded in a blueprint
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
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
            quota: config.quota,
            reservation: config.reservation,
            compression: config.compression,
        }
    }
}

/// Describe high-level metadata about a blueprint
// These fields are a subset of [`Blueprint`], and include only the data we can
// quickly fetch from the main blueprint table (e.g., when listing all
// blueprints).
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Serialize)]
pub struct BlueprintMetadata {
    /// unique identifier for this blueprint
    pub id: Uuid,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<Uuid>,
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

impl BlueprintMetadata {
    pub fn display_id(&self) -> String {
        format!("blueprint {}", self.id)
    }
}

/// Describes what blueprint, if any, the system is currently working toward
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct BlueprintTarget {
    /// id of the blueprint that the system is trying to make real
    pub target_id: Uuid,
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
    pub target_id: Uuid,
    pub enabled: bool,
}

/// Data about the "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug)]
pub enum DiffBeforeMetadata {
    /// The diff was made from a collection.
    Collection { id: CollectionUuid },
    /// The diff was made from a blueprint.
    Blueprint(Box<BlueprintMetadata>),
}

impl DiffBeforeMetadata {
    pub fn display_id(&self) -> String {
        match self {
            DiffBeforeMetadata::Collection { id } => format!("collection {id}"),
            DiffBeforeMetadata::Blueprint(b) => b.display_id(),
        }
    }
}

/// Single sled's zones config for "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug)]
pub enum BlueprintOrCollectionZonesConfig {
    /// The diff was made from a collection.
    Collection(OmicronZonesConfig),
    /// The diff was made from a blueprint.
    Blueprint(BlueprintZonesConfig),
}

impl BlueprintOrCollectionZonesConfig {
    pub fn sort(&mut self) {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(z) => {
                z.zones.sort_unstable_by_key(zone_sort_key)
            }
            BlueprintOrCollectionZonesConfig::Blueprint(z) => z.sort(),
        }
    }

    pub fn generation(&self) -> Generation {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(z) => z.generation,
            BlueprintOrCollectionZonesConfig::Blueprint(z) => z.generation,
        }
    }

    pub fn zones(
        &self,
    ) -> Box<dyn Iterator<Item = BlueprintOrCollectionZoneConfig> + '_> {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(zc) => {
                Box::new(zc.zones.iter().map(|z| z.clone().into()))
            }
            BlueprintOrCollectionZonesConfig::Blueprint(zc) => {
                Box::new(zc.zones.iter().map(|z| z.clone().into()))
            }
        }
    }
}

impl From<OmicronZonesConfig> for BlueprintOrCollectionZonesConfig {
    fn from(zc: OmicronZonesConfig) -> Self {
        Self::Collection(zc)
    }
}

impl From<BlueprintZonesConfig> for BlueprintOrCollectionZonesConfig {
    fn from(zc: BlueprintZonesConfig) -> Self {
        Self::Blueprint(zc)
    }
}

impl PartialEq<BlueprintZonesConfig> for BlueprintOrCollectionZonesConfig {
    fn eq(&self, other: &BlueprintZonesConfig) -> bool {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(z) => {
                // BlueprintZonesConfig contains more information than
                // OmicronZonesConfig. We compare them by lowering the
                // BlueprintZonesConfig into an OmicronZonesConfig.
                let lowered = OmicronZonesConfig::from(other.clone());
                z.eq(&lowered)
            }
            BlueprintOrCollectionZonesConfig::Blueprint(z) => z.eq(other),
        }
    }
}

/// Single zone config for "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug)]
pub enum BlueprintOrCollectionZoneConfig {
    /// The diff was made from a collection.
    Collection(OmicronZoneConfig),
    /// The diff was made from a blueprint.
    Blueprint(BlueprintZoneConfig),
}

impl From<OmicronZoneConfig> for BlueprintOrCollectionZoneConfig {
    fn from(zc: OmicronZoneConfig) -> Self {
        Self::Collection(zc)
    }
}

impl From<BlueprintZoneConfig> for BlueprintOrCollectionZoneConfig {
    fn from(zc: BlueprintZoneConfig) -> Self {
        Self::Blueprint(zc)
    }
}

impl PartialEq<BlueprintZoneConfig> for BlueprintOrCollectionZoneConfig {
    fn eq(&self, other: &BlueprintZoneConfig) -> bool {
        self.kind() == other.kind()
            && self.disposition() == other.disposition
            && self.underlay_address() == other.underlay_address
            && self.is_zone_type_equal(&other.zone_type)
    }
}

impl BlueprintOrCollectionZoneConfig {
    pub fn id(&self) -> OmicronZoneUuid {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => z.id(),
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.id(),
        }
    }

    pub fn kind(&self) -> ZoneKind {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => z.kind(),
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.kind(),
        }
    }

    pub fn disposition(&self) -> BlueprintZoneDisposition {
        match self {
            // All zones from inventory collection are assumed to be in-service.
            BlueprintOrCollectionZoneConfig::Collection(_) => {
                BlueprintZoneDisposition::InService
            }
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.disposition,
        }
    }

    pub fn underlay_address(&self) -> Ipv6Addr {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => {
                z.underlay_address
            }
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.underlay_address,
        }
    }

    pub fn is_zone_type_equal(&self, other: &BlueprintZoneType) -> bool {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => {
                // BlueprintZoneType contains more information than
                // OmicronZoneType. We compare them by lowering the
                // BlueprintZoneType into an OmicronZoneType.
                let lowered = OmicronZoneType::from(other.clone());
                z.zone_type == lowered
            }
            BlueprintOrCollectionZoneConfig::Blueprint(z) => {
                z.zone_type == *other
            }
        }
    }
}

/// Single sled's disks config for "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug, From)]
pub enum BlueprintOrCollectionDisksConfig {
    /// The diff was made from a collection.
    Collection(CollectionPhysicalDisksConfig),
    /// The diff was made from a blueprint.
    Blueprint(BlueprintPhysicalDisksConfig),
}

impl BlueprintOrCollectionDisksConfig {
    pub fn generation(&self) -> Option<Generation> {
        match self {
            BlueprintOrCollectionDisksConfig::Collection(_) => None,
            BlueprintOrCollectionDisksConfig::Blueprint(c) => {
                Some(c.generation)
            }
        }
    }

    pub fn disks(&self) -> BTreeSet<DiskIdentity> {
        match self {
            BlueprintOrCollectionDisksConfig::Collection(c) => c.disks.clone(),
            BlueprintOrCollectionDisksConfig::Blueprint(c) => {
                c.disks.iter().map(|d| d.identity.clone()).collect()
            }
        }
    }
}

/// Single sled's disk config for "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug, From)]
pub struct CollectionPhysicalDisksConfig {
    disks: BTreeSet<DiskIdentity>,
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
    pub blueprints: Vec<Blueprint>,
    pub internal_dns: BTreeMap<Generation, DnsConfigParams>,
    pub external_dns: BTreeMap<Generation, DnsConfigParams>,
    pub silo_names: Vec<omicron_common::api::external::Name>,
    pub external_dns_zone_names: Vec<String>,
}
