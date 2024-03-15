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

use crate::external_api::views::SledPolicy;
use crate::external_api::views::SledState;
use crate::inventory::Collection;
pub use crate::inventory::OmicronZoneConfig;
pub use crate::inventory::OmicronZoneDataset;
pub use crate::inventory::OmicronZoneType;
pub use crate::inventory::OmicronZonesConfig;
pub use crate::inventory::SourceNatConfig;
pub use crate::inventory::ZpoolName;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use uuid::Uuid;

/// Fleet-wide deployment policy
///
/// The **policy** represents the deployment controls that people (operators and
/// support engineers) can modify directly under normal operation.  In the
/// limit, this would include things like: which sleds are supposed to be part
/// of the system, how many CockroachDB nodes should be part of the cluster,
/// what system version the system should be running, etc.  It would _not_
/// include things like which services should be running on which sleds or which
/// host OS version should be on each sled because that's up to the control
/// plane to decide.  (To be clear, the intent is that for extenuating
/// circumstances, people could exercise control over such things, but that
/// would not be part of normal operation.)
///
/// The current policy is pretty limited.  It's aimed primarily at supporting
/// the add/remove sled use case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// set of sleds that are supposed to be part of the control plane, along
    /// with information about resources available to the planner
    pub sleds: BTreeMap<Uuid, SledResources>,

    /// ranges specified by the IP pool for externally-visible control plane
    /// services (e.g., external DNS, Nexus, boundary NTP)
    pub service_ip_pool_ranges: Vec<IpRange>,

    /// desired total number of deployed Nexus zones
    pub target_nexus_zone_count: usize,
}

/// Describes the resources available on each sled for the planner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledResources {
    /// current sled policy
    pub policy: SledPolicy,

    /// current sled state
    pub state: SledState,

    /// zpools on this sled
    ///
    /// (used to allocate storage for control plane zones with persistent
    /// storage)
    pub zpools: BTreeSet<ZpoolName>,

    /// the IPv6 subnet of this sled on the underlay network
    ///
    /// (implicitly specifies the whole range of addresses that the planner can
    /// use for control plane components)
    pub subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl SledResources {
    /// Returns true if the sled can have services provisioned on it that
    /// aren't required to be on every sled.
    ///
    /// For example, NTP must exist on every sled, but Nexus does not have to.
    pub fn is_eligible_for_discretionary_services(&self) -> bool {
        self.policy.is_provisionable()
            && self.state.is_eligible_for_discretionary_services()
    }
}

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

    /// A map of sled id -> zones deployed on each sled, along with the
    /// [`BlueprintZonePolicy`] for each zone.
    ///
    /// A sled is considered part of the control plane cluster iff it has an
    /// entry in this map.
    pub blueprint_zones: BTreeMap<Uuid, BlueprintZonesConfig>,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<Uuid>,

    /// internal DNS version when this blueprint was created
    // See blueprint execution for more on this.
    pub internal_dns_version: Generation,

    /// external DNS version when thi blueprint was created
    // See blueprint execution for more on this.
    pub external_dns_version: Generation,

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
    /// Iterate over all the [`BlueprintZoneConfig`] instances in the
    /// blueprint, along with the associated sled id.
    pub fn all_blueprint_zones(
        &self,
    ) -> impl Iterator<Item = (Uuid, &BlueprintZoneConfig)> {
        self.blueprint_zones
            .iter()
            .flat_map(|(sled_id, z)| z.zones.iter().map(|z| (*sled_id, z)))
    }

    /// Iterate over all the [`OmicronZoneConfig`] instances in the blueprint,
    /// along with the associated sled id.
    pub fn all_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (Uuid, &OmicronZoneConfig)> {
        self.blueprint_zones.iter().flat_map(|(sled_id, z)| {
            z.zones.iter().map(|z| (*sled_id, &z.config))
        })
    }

    /// Iterate over the ids of all sleds in the blueprint
    pub fn sleds(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.blueprint_zones.keys().copied()
    }

    /// Summarize the difference between sleds and zones between two blueprints
    pub fn diff_sleds<'a>(
        &'a self,
        other: &'a Blueprint,
    ) -> OmicronZonesDiff<'a> {
        OmicronZonesDiff {
            before_meta: DiffBeforeMetadata::Blueprint(self.diff_metadata()),
            before_zones: self.blueprint_zones.clone(),
            after_meta: other.diff_metadata(),
            after_zones: &other.blueprint_zones,
        }
    }

    /// Summarize the differences in sleds and zones between a collection and a
    /// blueprint
    ///
    /// This gives an idea about what would change about a running system if
    /// one were to execute the blueprint.
    ///
    /// Note that collections do not currently include information about what
    /// zones are in-service, so it is assumed that all zones in the collection
    /// are in-service. (This is the same assumption made by
    /// [`BlueprintZonesConfig::initial_from_collection`]. The logic here may
    /// also be expanded to handle cases where not all zones in the collection
    /// are in-service.)
    pub fn diff_sleds_from_collection(
        &self,
        collection: &Collection,
    ) -> OmicronZonesDiff<'_> {
        let before_zones = collection
            .omicron_zones
            .iter()
            .map(|(sled_id, zones_found)| {
                let zones = zones_found
                    .zones
                    .zones
                    .iter()
                    .map(|z| BlueprintZoneConfig {
                        config: z.clone(),
                        zone_policy: BlueprintZonePolicy::InService,
                    })
                    .collect();
                let zones = BlueprintZonesConfig {
                    generation: zones_found.zones.generation,
                    zones,
                };
                (*sled_id, zones)
            })
            .collect();
        OmicronZonesDiff {
            before_meta: DiffBeforeMetadata::Collection { id: collection.id },
            before_zones,
            after_meta: self.diff_metadata(),
            after_zones: &self.blueprint_zones,
        }
    }

    /// Return a struct that can be displayed to present information about the
    /// blueprint.
    pub fn display(&self) -> BlueprintDisplay<'_> {
        BlueprintDisplay { blueprint: self }
    }

    // ---
    // Helper methods
    // ---

    fn diff_metadata(&self) -> BlueprintDiffMetadata {
        BlueprintDiffMetadata {
            id: self.id,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
        }
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
        writeln!(
            f,
            "created by {}{}",
            b.creator,
            if b.creator.parse::<Uuid>().is_ok() {
                " (likely a Nexus instance)"
            } else {
                ""
            }
        )?;
        writeln!(
            f,
            "created at {}",
            humantime::format_rfc3339_millis(b.time_created.into(),)
        )?;
        writeln!(f, "internal DNS version: {}", b.internal_dns_version)?;
        writeln!(f, "comment: {}", b.comment)?;
        writeln!(f, "zones:\n")?;

        writeln!(f, "{}", self.make_zones_table())?;

        Ok(())
    }
}

/// Information about an Omicron zone as recorded in a blueprint.
///
/// Currently, this is similar to [`OmicronZonesConfig`], but also contains a
/// per-zone [`BlueprintZonePolicy`].
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

impl BlueprintZonesConfig {
    /// Constructs a new [`BlueprintZonesConfig`] from a collection's zones.
    ///
    /// For the initial blueprint, all zones within a collection are assumed to
    /// be in-service.
    pub fn initial_from_collection(collection: &OmicronZonesConfig) -> Self {
        let zones = collection
            .zones
            .iter()
            .map(|z| BlueprintZoneConfig {
                config: z.clone(),
                zone_policy: BlueprintZonePolicy::InService,
            })
            .collect();

        let mut ret = Self {
            // An initial `BlueprintZonesConfig` reuses the generation from
            // `OmicronZonesConfig`.
            generation: collection.generation,
            zones,
        };
        ret.sort();

        ret
    }

    /// Sorts the list of zones stored in this configuration.
    ///
    /// This is not strictly necessary. But for testing, it's helpful for
    /// things to be in sorted order.
    pub fn sort(&mut self) {
        self.zones.sort_unstable_by_key(|z| z.config.id);
    }

    /// Converts self to an [`OmicronZonesConfig`].
    pub fn to_omicron_zones_config(&self) -> OmicronZonesConfig {
        OmicronZonesConfig {
            generation: self.generation,
            zones: self.zones.iter().map(|z| z.config.clone()).collect(),
        }
    }
}

/// Describes one Omicron-managed zone in a blueprint.
///
/// This is a wrapper around an [`OmicronZoneConfig`] that also includes a
/// [`BlueprintZonePolicy`].
///
/// Part of [`BlueprintZonesConfig`].
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct BlueprintZoneConfig {
    /// The underlying zone configuration.
    pub config: OmicronZoneConfig,

    /// The policy for this zone.
    pub zone_policy: BlueprintZonePolicy,
}

impl BlueprintZoneConfig {
    /// Return a struct that can be displayed to present information about the
    /// zone.
    pub fn display(&self) -> BlueprintZoneConfigDisplay<'_> {
        BlueprintZoneConfigDisplay { zone: self }
    }
}

/// A wrapper to allow a [`BlueprintZoneConfig`] to be displayed with
/// information.
///
/// Returned by [`BlueprintZoneConfig::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintZoneConfigDisplay<'a> {
    zone: &'a BlueprintZoneConfig,
}

impl<'a> fmt::Display for BlueprintZoneConfigDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let z = self.zone;
        write!(
            f,
            "{} {:<width$} {} [underlay IP {}]",
            z.config.id,
            z.zone_policy,
            z.config.zone_type.tag(),
            z.config.underlay_address,
            width = BlueprintZonePolicy::DISPLAY_WIDTH,
        )
    }
}

/// The policy for an Omicron-managed zone in a blueprint.
///
/// Part of [`BlueprintZoneConfig`].
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BlueprintZonePolicy {
    InService,
    NotInService,
}

impl BlueprintZonePolicy {
    /// The maximum width of `Display` output.
    const DISPLAY_WIDTH: usize = 14;
}

impl fmt::Display for BlueprintZonePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Neither `write!(f, "...")` nor `f.write_str("...")` obey fill
            // and alignment (used above), but this does.
            BlueprintZonePolicy::InService => "in service".fmt(f),
            BlueprintZonePolicy::NotInService => "not in service".fmt(f),
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

/// Summarizes the differences between two blueprints
#[derive(Debug)]
pub struct OmicronZonesDiff<'a> {
    before_meta: DiffBeforeMetadata,
    // We store an owned copy of "before_zones" to make it easier to support
    // collections here, where we need to assemble this map ourselves.
    before_zones: BTreeMap<Uuid, BlueprintZonesConfig>,
    after_meta: BlueprintDiffMetadata,
    after_zones: &'a BTreeMap<Uuid, BlueprintZonesConfig>,
}

#[derive(Debug)]
enum DiffBeforeMetadata {
    Collection { id: Uuid },
    Blueprint(BlueprintDiffMetadata),
}

#[derive(Debug)]
struct BlueprintDiffMetadata {
    id: Uuid,
    internal_dns_version: Generation,
    external_dns_version: Generation,
}

/// Describes a sled that appeared on both sides of a diff (possibly changed)
#[derive(Debug)]
pub struct DiffSledCommon<'a> {
    /// id of the sled
    pub sled_id: Uuid,
    /// generation of the "zones" configuration on the left side
    pub generation_before: Generation,
    /// generation of the "zones" configuration on the right side
    pub generation_after: Generation,
    zones_added: Vec<&'a BlueprintZoneConfig>,
    zones_removed: Vec<&'a BlueprintZoneConfig>,
    zones_common: Vec<DiffZoneCommon<'a>>,
}

impl<'a> DiffSledCommon<'a> {
    /// Iterate over zones added between the blueprints
    pub fn zones_added(
        &self,
    ) -> impl Iterator<Item = &'a BlueprintZoneConfig> + '_ {
        self.zones_added.iter().copied()
    }

    /// Iterate over zones removed between the blueprints
    pub fn zones_removed(
        &self,
    ) -> impl Iterator<Item = &'a BlueprintZoneConfig> + '_ {
        self.zones_removed.iter().copied()
    }

    /// Iterate over zones that are common to both blueprints
    pub fn zones_in_common(
        &self,
    ) -> impl Iterator<Item = DiffZoneCommon<'a>> + '_ {
        self.zones_common.iter().copied()
    }

    /// Iterate over zones that changed between the blueprints
    pub fn zones_changed(
        &self,
    ) -> impl Iterator<Item = DiffZoneCommon<'a>> + '_ {
        self.zones_in_common().filter(|z| z.is_changed())
    }

    /// Iterate over zones that did not change between the blueprints
    pub fn zones_unchanged(
        &self,
    ) -> impl Iterator<Item = DiffZoneCommon<'a>> + '_ {
        self.zones_in_common().filter(|z| !z.is_changed())
    }
}

/// Describes a zone that was common to both sides of a diff
#[derive(Debug, Copy, Clone)]
pub struct DiffZoneCommon<'a> {
    /// full zone configuration before
    pub zone_before: &'a BlueprintZoneConfig,
    /// full zone configuration after
    pub zone_after: &'a BlueprintZoneConfig,
}

impl<'a> DiffZoneCommon<'a> {
    /// Returns true if there are any differences between `zone_before` and
    /// `zone_after`.
    ///
    /// This is equivalent to `config_changed() || policy_changed()`.
    #[inline]
    pub fn is_changed(&self) -> bool {
        // policy is smaller and easier to compare than config.
        self.policy_changed() || self.config_changed()
    }

    /// Returns true if the zone configuration (excluding the policy) changed.
    #[inline]
    pub fn config_changed(&self) -> bool {
        self.zone_before.config != self.zone_after.config
    }

    /// Returns true if the policy for the zone changed.
    #[inline]
    pub fn policy_changed(&self) -> bool {
        self.zone_before.zone_policy != self.zone_after.zone_policy
    }
}

impl<'a> OmicronZonesDiff<'a> {
    fn sleds_before(&self) -> BTreeSet<Uuid> {
        self.before_zones.keys().copied().collect()
    }

    fn sleds_after(&self) -> BTreeSet<Uuid> {
        self.after_zones.keys().copied().collect()
    }

    /// Iterate over sleds only present in the second blueprint of a diff
    pub fn sleds_added(
        &self,
    ) -> impl Iterator<Item = (Uuid, &BlueprintZonesConfig)> + '_ {
        let sled_ids = self
            .sleds_after()
            .difference(&self.sleds_before())
            .copied()
            .collect::<BTreeSet<_>>();

        sled_ids
            .into_iter()
            .map(|sled_id| (sled_id, self.after_zones.get(&sled_id).unwrap()))
    }

    /// Iterate over sleds only present in the first blueprint of a diff
    pub fn sleds_removed(
        &self,
    ) -> impl Iterator<Item = (Uuid, &BlueprintZonesConfig)> + '_ {
        let sled_ids = self
            .sleds_before()
            .difference(&self.sleds_after())
            .copied()
            .collect::<BTreeSet<_>>();
        sled_ids
            .into_iter()
            .map(|sled_id| (sled_id, self.before_zones.get(&sled_id).unwrap()))
    }

    /// Iterate over sleds present in both blueprints in a diff
    pub fn sleds_in_common(
        &'a self,
    ) -> impl Iterator<Item = (Uuid, DiffSledCommon<'a>)> + '_ {
        let sled_ids = self
            .sleds_before()
            .intersection(&self.sleds_after())
            .copied()
            .collect::<BTreeSet<_>>();
        sled_ids.into_iter().map(|sled_id| {
            let b1sledzones = self.before_zones.get(&sled_id).unwrap();
            let b2sledzones = self.after_zones.get(&sled_id).unwrap();

            // Assemble separate summaries of the zones, indexed by zone id.
            let b1_zones: BTreeMap<Uuid, &'a BlueprintZoneConfig> = b1sledzones
                .zones
                .iter()
                .map(|zone| (zone.config.id, zone))
                .collect();
            let mut b2_zones: BTreeMap<Uuid, &'a BlueprintZoneConfig> =
                b2sledzones
                    .zones
                    .iter()
                    .map(|zone| (zone.config.id, zone))
                    .collect();
            let mut zones_removed = vec![];
            let mut zones_common = vec![];

            // Now go through each zone and compare them.
            for (zone_id, zone_before) in &b1_zones {
                if let Some(zone_after) = b2_zones.remove(zone_id) {
                    zones_common
                        .push(DiffZoneCommon { zone_before, zone_after });
                } else {
                    zones_removed.push(*zone_before);
                }
            }

            // Since we removed common zones above, anything else exists only in
            // b2 and was therefore added.
            let zones_added = b2_zones.into_values().collect();

            (
                sled_id,
                DiffSledCommon {
                    sled_id,
                    generation_before: b1sledzones.generation,
                    generation_after: b2sledzones.generation,
                    zones_added,
                    zones_removed,
                    zones_common,
                },
            )
        })
    }

    pub fn sleds_changed(
        &'a self,
    ) -> impl Iterator<Item = (Uuid, DiffSledCommon<'a>)> + '_ {
        self.sleds_in_common().filter(|(_, sled_changes)| {
            sled_changes.zones_added().next().is_some()
                || sled_changes.zones_removed().next().is_some()
                || sled_changes.zones_changed().next().is_some()
        })
    }

    /// Return a struct that can be used to display the diff.
    pub fn display(&self) -> OmicronZonesDiffDisplay<'_, 'a> {
        OmicronZonesDiffDisplay::new(self)
    }
}

/// Wrapper to allow a [`OmicronZonesDiff`] to be displayed.
///
/// Returned by [`OmicronZonesDiff::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct OmicronZonesDiffDisplay<'diff, 'a> {
    diff: &'diff OmicronZonesDiff<'a>,
    // TODO: add colorization with a stylesheet
}

impl<'diff, 'a> OmicronZonesDiffDisplay<'diff, 'a> {
    #[inline]
    fn new(diff: &'diff OmicronZonesDiff<'a>) -> Self {
        Self { diff }
    }
}

impl<'diff, 'a> fmt::Display for OmicronZonesDiffDisplay<'diff, 'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let diff = self.diff;

        // Print things differently based on whether the diff is between a
        // collection and a blueprint, or a blueprint and a blueprint.
        match &diff.before_meta {
            DiffBeforeMetadata::Collection { id } => {
                writeln!(
                    f,
                    "from: collection {}\n\
                     to:   blueprint  {}\n\
                     +       external DNS version: {}\n\
                     +       internal DNS version: {}",
                    id,
                    diff.after_meta.id,
                    diff.after_meta.external_dns_version,
                    diff.after_meta.internal_dns_version,
                )?;
            }
            DiffBeforeMetadata::Blueprint(before) => {
                writeln!(
                    f,
                    "from: blueprint {}\n\
                     -       external DNS version: {}\n\
                     -       internal DNS version: {}",
                    before.id,
                    before.external_dns_version,
                    before.internal_dns_version,
                )?;

                writeln!(
                    f,
                    "to:   blueprint {}\n\
                     +       external DNS version: {}\n\
                     +       internal DNS version: {}",
                    diff.after_meta.id,
                    diff.after_meta.external_dns_version,
                    diff.after_meta.internal_dns_version,
                )?;
            }
        }

        writeln!(f, "\nzones:\n{}", self.make_zones_diff_table())?;

        Ok(())
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
    pub policy: Policy,
    pub collections: Vec<Collection>,
    pub blueprints: Vec<Blueprint>,
}

/// Code to generate tables.
///
/// This is here because `tabled` has a number of generically-named types, and
/// we'd like to avoid name collisions with other types.
mod table_display {
    use super::*;

    use tabled::builder::Builder;
    use tabled::settings::object::Columns;
    use tabled::settings::span::ColumnSpan;
    use tabled::settings::Alignment;
    use tabled::settings::Border;
    use tabled::settings::Modify;
    use tabled::settings::Padding;
    use tabled::settings::Style;
    use tabled::Table;

    const SLED_INDENT: &str = "    ";
    const SLED_SUBHEAD_INDENT: &str = "      ";
    // Due to somewhat mysterious reasons with how padding works with tabled,
    // this needs to be 7 columns wide rather than 8.
    const ZONE_INDENT: &str = "       ";
    const ADDED_PREFIX: char = '+';
    const REMOVED_PREFIX: char = '-';
    const CHANGED_PREFIX: char = '~';
    const UNCHANGED_PREFIX: char = ' ';
    const ARROW: &str = "->";
    const ARROW_UNCHANGED: &str = "  ";
    const ZONE_ID_HEADER: &str = "zone ID";
    const POLICY_HEADER: &str = "policy";
    const ZONE_TYPE_HEADER: &str = "zone type";
    const UNDERLAY_IP_HEADER: &str = "underlay IP";
    const UNCHANGED: &str = "(unchanged)";
    const CONFIG_CHANGED: &str = "(config changed)";

    impl<'a> super::BlueprintDisplay<'a> {
        pub(super) fn make_zones_table(&self) -> Table {
            let blueprint_zones = &self.blueprint.blueprint_zones;
            let mut builder = ZoneTableBuilder::new(ZONE_INDENT.to_string());

            for (sled_id, sled_zones) in blueprint_zones {
                // Add a heading row.
                builder.push_heading(format!(
                    "{SLED_INDENT}sled {sled_id}: zones at generation {}",
                    sled_zones.generation
                ));

                // Add a row for each zone.
                for zone in &sled_zones.zones {
                    builder.push_zone_record(ZONE_INDENT.to_string(), zone);
                }
            }

            builder.build()
        }
    }

    impl<'diff, 'a> OmicronZonesDiffDisplay<'diff, 'a> {
        pub(super) fn make_zones_diff_table(&self) -> Table {
            let diff = self.diff;

            // Add the unchanged prefix to the zone indent since the first
            // column will be used as the prefix.
            let mut builder = ZoneTableBuilder::new(format!(
                "{UNCHANGED_PREFIX}{ZONE_INDENT}"
            ));

            // Print out the whole sled for removed sleds.
            for (sled_id, sled_zones) in diff.sleds_removed() {
                self.add_whole_sled_records(
                    sled_id,
                    sled_zones,
                    WholeSledKind::Removed,
                    &mut builder,
                );
            }

            // For sleds that are in common:
            for (sled_id, sled_changes) in diff.sleds_in_common() {
                // Print a line about the sled itself and zone config
                // generation, regardless of whether anything has changed.
                if sled_changes.generation_before
                    != sled_changes.generation_after
                {
                    builder.push_heading(format!(
                        "{CHANGED_PREFIX}{SLED_INDENT}sled {sled_id}: \
                         zones at generation {} -> {}",
                        sled_changes.generation_before,
                        sled_changes.generation_after,
                    ));
                } else {
                    builder.push_heading(format!(
                        "{UNCHANGED_PREFIX}{SLED_INDENT}sled {sled_id}: zones at generation {}",
                        sled_changes.generation_before,
                    ));
                }

                for (i, zone) in sled_changes.zones_removed().enumerate() {
                    if i == 0 {
                        builder.push_subheading(format!(
                            "{REMOVED_PREFIX}{SLED_SUBHEAD_INDENT}removed zones:"
                        ));
                    }
                    builder.push_zone_record(
                        format!("{REMOVED_PREFIX}{ZONE_INDENT}"),
                        zone,
                    );
                }

                // First print out all changed zones.
                for (i, zone_changed) in
                    sled_changes.zones_changed().enumerate()
                {
                    if i == 0 {
                        builder.push_subheading(format!(
                            "{CHANGED_PREFIX}{SLED_SUBHEAD_INDENT}changed zones:"
                        ));
                    }
                    builder.push_change_table(
                        &zone_changed.zone_before,
                        &zone_changed.zone_after,
                    );
                }

                // Then, all unchanged zones.
                for (i, zone_unchanged) in
                    sled_changes.zones_unchanged().enumerate()
                {
                    if i == 0 {
                        builder.push_subheading(format!(
                            "{UNCHANGED_PREFIX}{SLED_SUBHEAD_INDENT}unchanged zones:"
                        ));
                    }
                    builder.push_zone_record(
                        format!("{UNCHANGED_PREFIX}{ZONE_INDENT}"),
                        &zone_unchanged.zone_before,
                    );
                }

                for (i, zone) in sled_changes.zones_added().enumerate() {
                    if i == 0 {
                        builder.push_subheading(format!(
                            "{ADDED_PREFIX}{SLED_SUBHEAD_INDENT}added zones:"
                        ));
                    }
                    builder.push_zone_record(
                        format!("{ADDED_PREFIX}{ZONE_INDENT}"),
                        zone,
                    );
                }
            }

            // Print out the whole sled for added sleds.
            for (sled_id, sled_zones) in diff.sleds_added() {
                self.add_whole_sled_records(
                    sled_id,
                    sled_zones,
                    WholeSledKind::Added,
                    &mut builder,
                );
            }

            builder.build()
        }

        fn add_whole_sled_records(
            &self,
            sled_id: Uuid,
            sled_zones: &BlueprintZonesConfig,
            kind: WholeSledKind,
            builder: &mut ZoneTableBuilder,
        ) {
            builder.push_heading(format!(
                "{}{SLED_INDENT}sled {sled_id}: zones at generation {} ({})",
                kind.prefix(),
                sled_zones.generation,
                kind.label(),
            ));

            for zone in &sled_zones.zones {
                builder.push_zone_record(
                    format!("{}{ZONE_INDENT}", kind.prefix()),
                    zone,
                );
            }
        }
    }

    /// Builder for a zone table.
    ///
    /// A zone table consists of all rows across all possible sleds,
    /// interspersed with special rows that break the standard table
    /// conventions.
    ///
    /// There are three kinds of special rows:
    ///
    /// 1. Headings: these are rows that span all columns, and have padding
    ///    above and below them.
    /// 2. Subheadings: these are rows that span all columns, but do not have
    ///    any padding above or below them.
    /// 3. Change tables: these are rows that contain a table of changes
    ///    between two zone records. They are presented with columns from top
    ///    to bottom rather than left to right, and are rendered in a visually
    ///    distinctive manner.
    #[derive(Debug)]
    struct ZoneTableBuilder {
        builder: Builder,
        headings: Vec<usize>,
        subheadings: Vec<usize>,
        change_tables: Vec<usize>,
    }

    impl ZoneTableBuilder {
        fn new(zone_indent: String) -> Self {
            let mut builder = Builder::new();
            builder.push_record(vec![
                zone_indent,
                ZONE_ID_HEADER.to_string(),
                POLICY_HEADER.to_string(),
                ZONE_TYPE_HEADER.to_string(),
                UNDERLAY_IP_HEADER.to_string(),
            ]);

            Self {
                builder,
                headings: Vec::new(),
                subheadings: Vec::new(),
                change_tables: Vec::new(),
            }
        }

        /// Push a heading row onto the table.
        ///
        /// A heading row spans all columns, and has padding above and below it.
        fn push_heading(&mut self, heading: String) {
            self.headings.push(self.builder.count_records());
            self.builder.push_record(vec![heading]);
        }

        /// Push a subheading row onto the table.
        ///
        /// A subheading row spans all columns, but does not have any padding
        /// above or below it.
        fn push_subheading(&mut self, subheading: String) {
            self.subheadings.push(self.builder.count_records());
            self.builder.push_record(vec![subheading]);
        }

        /// Push a zone record onto the table.
        ///
        /// The first column is the indent and prefix column, and the rest of
        /// the columns are the zone record.
        fn push_zone_record(
            &mut self,
            first_column: String,
            zone: &BlueprintZoneConfig,
        ) {
            self.builder.push_record(vec![
                first_column,
                zone.config.id.to_string(),
                zone.zone_policy.to_string(),
                zone.config.zone_type.tag().to_string(),
                zone.config.underlay_address.to_string(),
            ]);
        }

        /// Push a change table onto the table.
        ///
        /// For diffs, this contains a table of changes between two zone
        /// records.
        fn push_change_table(
            &mut self,
            before: &BlueprintZoneConfig,
            after: &BlueprintZoneConfig,
        ) {
            self.change_tables.push(self.builder.count_records());
            let table = make_zone_change_table(before, after);
            self.builder.push_record(vec![
                format!("{CHANGED_PREFIX}{ZONE_INDENT}"),
                before.config.id.to_string(),
                table.to_string(),
            ]);
        }

        fn build(self) -> Table {
            let mut table = self.builder.build();
            apply_general_settings(&mut table);
            apply_heading_settings(&mut table, &self.headings);
            apply_subheading_settings(&mut table, &self.subheadings);
            apply_change_table_settings(&mut table, &self.change_tables);

            table
        }
    }

    fn make_zone_change_table(
        before: &BlueprintZoneConfig,
        after: &BlueprintZoneConfig,
    ) -> Table {
        let mut builder = Builder::new();

        if before.zone_policy != after.zone_policy {
            builder.push_record(vec![
                POLICY_HEADER.to_string(),
                before.zone_policy.to_string(),
                ARROW.to_string(),
                after.zone_policy.to_string(),
            ]);
        } else {
            builder.push_record(vec![
                POLICY_HEADER.to_string(),
                before.zone_policy.to_string(),
                ARROW_UNCHANGED.to_string(),
                UNCHANGED.to_string(),
            ]);
        }

        if before.config.zone_type != after.config.zone_type {
            // There are two cases here:
            // 1. The zone type changed (we don't expect this to happen)
            // 2. The zone type did not change, but some of the configuration
            //    parameters inside changed. We need to add additional text to
            //    make that clearer.
            //
            // TODO: also print out the exact bits of configuration that
            // changed.
            if before.config.zone_type.tag() != after.config.zone_type.tag() {
                // Case 1
                builder.push_record(vec![
                    ZONE_TYPE_HEADER.to_string(),
                    before.config.zone_type.tag().to_string(),
                    ARROW.to_string(),
                    after.config.zone_type.tag().to_string(),
                ]);
            } else {
                // Case 2
                builder.push_record(vec![
                    ZONE_TYPE_HEADER.to_string(),
                    before.config.zone_type.tag().to_string(),
                    ARROW_UNCHANGED.to_string(),
                    CONFIG_CHANGED.to_string(),
                ]);
            }
        } else {
            builder.push_record(vec![
                ZONE_TYPE_HEADER.to_string(),
                before.config.zone_type.tag().to_string(),
                ARROW_UNCHANGED.to_string(),
                UNCHANGED.to_string(),
            ]);
        }

        if before.config.underlay_address != after.config.underlay_address {
            builder.push_record(vec![
                UNDERLAY_IP_HEADER.to_string(),
                before.config.underlay_address.to_string(),
                ARROW.to_string(),
                after.config.underlay_address.to_string(),
            ]);
        } else {
            builder.push_record(vec![
                UNDERLAY_IP_HEADER.to_string(),
                before.config.underlay_address.to_string(),
                "".to_string(),
                UNCHANGED.to_string(),
            ]);
        }

        let mut table = builder.build();
        // Style::blank + Padding::zero makes it so that there's a gap of one
        // space between the most constrained columns.
        table
            .with(Style::blank())
            .with(Alignment::left())
            .with(Padding::zero())
            .with(
                Modify::new(Columns::single(0))
                    // Add two spaces of padding to the right of the first
                    // column (header), so it looks a bit separated from the
                    // rest of the table. This could also be a '|' or other
                    // border character, but that looks busier.
                    .with(Padding::new(0, 2, 0, 0)),
            );

        table
    }

    fn apply_general_settings(table: &mut Table) {
        table
            .with(tabled::settings::Style::blank())
            .with(Alignment::center())
            // Column 0 is the indent and prefix column, and it should not
            // have any padding.
            .with(
                Modify::new(Columns::first())
                    .with(Padding::zero())
                    .with(Border::empty()),
            )
            .with(
                Modify::new(Columns::single(1))
                    // Column 1 (sled ID) gets some extra padding and a border
                    // on the right to separate it from the other columns.
                    .with(Padding::new(0, 2, 0, 0))
                    .with(Border::new().set_right('|')),
            )
            .with(
                // Column 2 gets extra padding on the left to be
                // symmetrical to column 1.
                Modify::new(Columns::single(2)).with(Padding::new(2, 0, 0, 0)),
            );
    }

    fn apply_heading_settings(table: &mut Table, headings: &[usize]) {
        for h in headings {
            table.with(
                Modify::new((*h, 0))
                    // Adjust each heading row to span the whole column.
                    .with(ColumnSpan::new(5))
                    .with(Alignment::left())
                    // Add a row of padding at the top and bottom of each
                    // heading.
                    .with(Padding::new(0, 0, 1, 1)),
            );
        }
    }

    fn apply_subheading_settings(table: &mut Table, subheadings: &[usize]) {
        for sh in subheadings {
            table.with(
                Modify::new((*sh, 0))
                    // Adjust each subheading row to span the whole column.
                    .with(ColumnSpan::new(5))
                    .with(Alignment::left()),
            );
        }
    }

    fn apply_change_table_settings(table: &mut Table, change_tables: &[usize]) {
        for ct in change_tables {
            table
                .with(
                    Modify::new((*ct, 1))
                        // For the first column, use a different prefix to make
                        // change tables stand out (since they don't obey the rules
                        // of the rest of the table).
                        .with(Border::new().set_right(':')),
                )
                .with(
                    // The change table is stored in column 2.
                    Modify::new((*ct, 2))
                        // Adjust each change table cell to span the rest of the
                        // table.
                        .with(ColumnSpan::new(3))
                        .with(Alignment::left()),
                );
        }
    }

    #[derive(Copy, Clone, Debug)]
    enum WholeSledKind {
        Removed,
        Added,
    }

    impl WholeSledKind {
        fn prefix(self) -> char {
            match self {
                WholeSledKind::Removed => REMOVED_PREFIX,
                WholeSledKind::Added => ADDED_PREFIX,
            }
        }

        fn label(self) -> &'static str {
            match self {
                WholeSledKind::Removed => "removed",
                WholeSledKind::Added => "added",
            }
        }
    }
}
