// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing deployed software and configuration
//!
//! For more on this, see the crate-level documentation for `nexus/deployment`.
//!
//! This lives in nexus/types because it's used by both nexus/db-model and
//! nexus/deployment.  (It could as well just live in nexus/db-model, but
//! nexus/deployment does not currently know about nexus/db-model and it's
//! convenient to separate these concerns.)

pub use crate::inventory::OmicronZoneConfig;
pub use crate::inventory::OmicronZoneDataset;
pub use crate::inventory::OmicronZoneType;
pub use crate::inventory::OmicronZonesConfig;
pub use crate::inventory::ZpoolName;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
pub struct Policy {
    /// set of sleds that are supposed to be part of the control plane, along
    /// with information about resources available to the planner
    pub sleds: BTreeMap<Uuid, SledResources>,
}

/// Describes the resources available on each sled for the planner
pub struct SledResources {
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

/// Describes a complete set of software and configuration for the system
// Blueprints are a fundamental part of how the system modifies itself.  Each
// blueprint completely describes all of the software and configuration
// that the control plane manages.  See the nexus/deployment crate-level
// documentation for details.
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
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct Blueprint {
    /// unique identifier for this blueprint
    pub id: Uuid,

    /// mapping: sled id -> zones deployed on each sled
    /// A sled is considered part of the control plane cluster iff it has an
    /// entry in this map.
    pub omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,

    /// Omicron zones considered in-service (which generally means that they
    /// should appear in DNS)
    pub zones_in_service: BTreeSet<Uuid>,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<Uuid>,

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
    /// Iterate over all the Omicron zones in the blueprint, along with
    /// associated sled id
    pub fn all_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (Uuid, &OmicronZoneConfig)> {
        self.omicron_zones
            .iter()
            .flat_map(|(sled_id, z)| z.zones.iter().map(|z| (*sled_id, z)))
    }

    /// Iterate over the ids of all sleds in the blueprint
    pub fn sleds(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.omicron_zones.keys().copied()
    }

    /// Summarize the difference between two blueprints
    pub fn diff<'a>(&'a self, other: &'a Blueprint) -> BlueprintDiff<'a> {
        BlueprintDiff { b1: self, b2: other }
    }
}

/// Describes which blueprint the system is currently trying to make real
// This is analogous to the db model type until we have that.
#[derive(Debug, Clone)]
pub struct BlueprintTarget {
    pub target_id: Option<Uuid>,
    pub enabled: bool,
    pub time_set: chrono::DateTime<chrono::Utc>,
}

/// Specifies what blueprint, if any, the system should be working toward
#[derive(Deserialize, JsonSchema)]
pub struct BlueprintTargetSet {
    pub target_id: Uuid,
    pub enabled: bool,
}

/// Summarizes the differences between two blueprints
pub struct BlueprintDiff<'a> {
    b1: &'a Blueprint,
    b2: &'a Blueprint,
}

/// Describes a sled that appeared on both sides of a diff (possibly changed)
pub struct DiffSledCommon<'a> {
    /// id of the sled
    pub sled_id: Uuid,
    /// generation of the "zones" configuration on the left side
    pub generation_before: Generation,
    /// generation of the "zones" configuration on the right side
    pub generation_after: Generation,
    zones_added: Vec<&'a OmicronZoneConfig>,
    zones_removed: Vec<&'a OmicronZoneConfig>,
    zones_common: Vec<DiffZoneCommon<'a>>,
}

impl<'a> DiffSledCommon<'a> {
    /// Iterate over zones added between the blueprints
    pub fn zones_added(
        &self,
    ) -> impl Iterator<Item = &'a OmicronZoneConfig> + '_ {
        self.zones_added.iter().copied()
    }

    /// Iterate over zones removed between the blueprints
    pub fn zones_removed(
        &self,
    ) -> impl Iterator<Item = &'a OmicronZoneConfig> + '_ {
        self.zones_removed.iter().copied()
    }

    /// Iterate over zones that are common to both blueprints
    pub fn zones_in_common(
        &self,
    ) -> impl Iterator<Item = DiffZoneCommon<'a>> + '_ {
        self.zones_common.iter().copied()
    }

    /// Iterate over zones that changed between the blue prints
    pub fn zones_changed(
        &self,
    ) -> impl Iterator<Item = DiffZoneCommon<'a>> + '_ {
        self.zones_in_common()
            .filter(|z| z.changed_how != DiffZoneChangedHow::NoChanges)
    }
}

/// Describes a zone that was common to both sides of a diff
#[derive(Debug, Copy, Clone)]
pub struct DiffZoneCommon<'a> {
    /// full zone configuration before
    pub zone_before: &'a OmicronZoneConfig,
    /// full zone configuration after
    pub zone_after: &'a OmicronZoneConfig,
    /// summary of what changed, if anything
    pub changed_how: DiffZoneChangedHow,
}

/// Describes how a zone changed across two blueprints, if at all
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DiffZoneChangedHow {
    /// the zone did not change between these two blueprints
    NoChanges,
    /// the zone details are the same, but it was brought into service
    AddedToService,
    /// the zone details are the same, but it was removed from service
    RemovedFromService,
    /// the zone's details (i.e., configuration) changed
    DetailsChanged,
}

impl<'a> BlueprintDiff<'a> {
    /// Iterate over sleds only present in the second blueprint of a diff
    pub fn sleds_added(
        &self,
    ) -> impl Iterator<Item = (Uuid, &OmicronZonesConfig)> + '_ {
        let sled_ids = self
            .b2
            .sleds()
            .collect::<BTreeSet<_>>()
            .difference(&self.b1.sleds().collect::<BTreeSet<_>>())
            .copied()
            .collect::<BTreeSet<_>>();

        sled_ids.into_iter().map(|sled_id| {
            (sled_id, self.b2.omicron_zones.get(&sled_id).unwrap())
        })
    }

    /// Iterate over sleds only present in the first blueprint of a diff
    pub fn sleds_removed(
        &self,
    ) -> impl Iterator<Item = (Uuid, &OmicronZonesConfig)> + '_ {
        let sled_ids = self
            .b1
            .sleds()
            .collect::<BTreeSet<_>>()
            .difference(&self.b2.sleds().collect::<BTreeSet<_>>())
            .copied()
            .collect::<BTreeSet<_>>();
        sled_ids.into_iter().map(|sled_id| {
            (sled_id, self.b1.omicron_zones.get(&sled_id).unwrap())
        })
    }

    /// Iterate over sleds present in both blueprints in a diff
    pub fn sleds_in_common(
        &self,
    ) -> impl Iterator<Item = (Uuid, DiffSledCommon<'a>)> + '_ {
        let sled_ids = self
            .b1
            .sleds()
            .collect::<BTreeSet<_>>()
            .intersection(&self.b2.sleds().collect::<BTreeSet<_>>())
            .copied()
            .collect::<BTreeSet<_>>();
        sled_ids.into_iter().map(|sled_id| {
            let b1sledzones = self.b1.omicron_zones.get(&sled_id).unwrap();
            let b2sledzones = self.b2.omicron_zones.get(&sled_id).unwrap();

            // Assemble separate summaries of the zones, indexed by zone id.
            #[derive(Debug)]
            struct ZoneInfo<'a> {
                zone: &'a OmicronZoneConfig,
                in_service: bool,
            }

            let b1zones: BTreeMap<Uuid, ZoneInfo> = b1sledzones
                .zones
                .iter()
                .map(|zone| {
                    (
                        zone.id,
                        ZoneInfo {
                            zone,
                            in_service: self
                                .b1
                                .zones_in_service
                                .contains(&zone.id),
                        },
                    )
                })
                .collect();
            let b2zones: BTreeMap<Uuid, ZoneInfo> = b2sledzones
                .zones
                .iter()
                .map(|zone| {
                    (
                        zone.id,
                        ZoneInfo {
                            zone,
                            in_service: self
                                .b2
                                .zones_in_service
                                .contains(&zone.id),
                        },
                    )
                })
                .collect();
            let mut zones_added = vec![];
            let mut zones_removed = vec![];
            let mut zones_changed = vec![];

            // Now go through each zone and compare them.
            for (zone_id, b1z_info) in &b1zones {
                if let Some(b2z_info) = b2zones.get(zone_id) {
                    let changed_how = if b1z_info.zone != b2z_info.zone {
                        DiffZoneChangedHow::DetailsChanged
                    } else if b1z_info.in_service && !b2z_info.in_service {
                        DiffZoneChangedHow::RemovedFromService
                    } else if !b1z_info.in_service && b2z_info.in_service {
                        DiffZoneChangedHow::AddedToService
                    } else {
                        DiffZoneChangedHow::NoChanges
                    };
                    zones_changed.push(DiffZoneCommon {
                        zone_before: b1z_info.zone,
                        zone_after: b2z_info.zone,
                        changed_how,
                    });
                } else {
                    zones_removed.push(b1z_info.zone);
                }
            }

            for (zone_id, b2z_info) in &b2zones {
                if b1zones.contains_key(&zone_id) {
                    // We handled this zone above.
                    continue;
                }

                zones_added.push(b2z_info.zone)
            }

            (
                sled_id,
                DiffSledCommon {
                    sled_id,
                    generation_before: b1sledzones.generation,
                    generation_after: b2sledzones.generation,
                    zones_added,
                    zones_removed,
                    zones_common: zones_changed,
                },
            )
        })
    }

    pub fn sleds_changed(
        &self,
    ) -> impl Iterator<Item = (Uuid, DiffSledCommon<'a>)> + '_ {
        self.sleds_in_common().filter(|(_, sled_changes)| {
            sled_changes.zones_added().next().is_some()
                || sled_changes.zones_removed().next().is_some()
                || sled_changes.zones_changed().next().is_some()
        })
    }

    fn print_whole_sled(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        prefix: char,
        label: &str,
        bbsledzones: &OmicronZonesConfig,
        sled_id: Uuid,
    ) -> std::fmt::Result {
        writeln!(f, "{} sled {} ({})", prefix, sled_id, label)?;
        writeln!(
            f,
            "{}     zone config generation {}",
            prefix, bbsledzones.generation
        )?;
        for z in &bbsledzones.zones {
            writeln!(
                f,
                "{}         zone {} type {} ({})",
                prefix,
                z.id,
                z.zone_type.label(),
                label
            )?;
        }

        Ok(())
    }
}

/// Implements diff(1)-like output for diff'ing two blueprints
impl<'a> std::fmt::Display for BlueprintDiff<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "diff blueprints {} {}", self.b1.id, self.b2.id)?;
        writeln!(f, "--- {}", self.b1.id)?;
        writeln!(f, "+++ {}", self.b2.id)?;

        for (sled_id, sled_zones) in self.sleds_removed() {
            self.print_whole_sled(f, '-', "removed", sled_zones, sled_id)?;
        }

        for (sled_id, sled_changes) in self.sleds_in_common() {
            // Print a line about the sled itself and zone config generation,
            // regardless of whether anything has changed.
            writeln!(f, "  sled {}", sled_id)?;
            if sled_changes.generation_before != sled_changes.generation_after {
                writeln!(
                    f,
                    "-     zone config generation {}",
                    sled_changes.generation_before
                )?;
                writeln!(
                    f,
                    "+     zone config generation {}",
                    sled_changes.generation_after
                )?;
            } else {
                writeln!(
                    f,
                    "      zone config generation {}",
                    sled_changes.generation_before
                )?;
            }

            for zone in sled_changes.zones_removed() {
                writeln!(
                    f,
                    "-        zone {} type {} (removed)",
                    zone.id,
                    zone.zone_type.label(),
                )?;
            }

            for zone_changes in sled_changes.zones_in_common() {
                let zone_id = zone_changes.zone_before.id;
                let zone_type = zone_changes.zone_before.zone_type.label();
                let zone2_type = zone_changes.zone_after.zone_type.label();
                match zone_changes.changed_how {
                    DiffZoneChangedHow::DetailsChanged => {
                        writeln!(
                            f,
                            "-         zone {} type {} (changed)",
                            zone_id, zone_type,
                        )?;
                        writeln!(
                            f,
                            "+         zone {} type {} (changed)",
                            zone_id, zone2_type,
                        )?;
                    }
                    DiffZoneChangedHow::RemovedFromService => {
                        writeln!(
                            f,
                            "-         zone {} type {} (in service)",
                            zone_id, zone_type,
                        )?;
                        writeln!(
                            f,
                            "+         zone {} type {} (removed from service)",
                            zone_id, zone2_type,
                        )?;
                    }
                    DiffZoneChangedHow::AddedToService => {
                        writeln!(
                            f,
                            "-         zone {} type {} (not in service)",
                            zone_id, zone_type,
                        )?;
                        writeln!(
                            f,
                            "+         zone {} type {} (added to service)",
                            zone_id, zone2_type,
                        )?;
                    }
                    DiffZoneChangedHow::NoChanges => {
                        writeln!(
                            f,
                            "         zone {} type {} (unchanged)",
                            zone_id, zone_type,
                        )?;
                    }
                }
            }

            for zone in sled_changes.zones_added() {
                writeln!(
                    f,
                    "+        zone {} type {} (added)",
                    zone.id,
                    zone.zone_type.label(),
                )?;
            }
        }

        for (sled_id, sled_zones) in self.sleds_added() {
            self.print_whole_sled(f, '+', "added", sled_zones, sled_id)?;
        }

        Ok(())
    }
}
