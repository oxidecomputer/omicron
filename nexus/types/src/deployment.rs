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
///
/// Blueprints are a fundamental part of how the system modifies itself.  Each
/// blueprint completely describes all of the software and configuration
/// that the control plane manages.  See the nexus/deployment crate-level
/// documentation for details.
///
/// Blueprints are different from policy.  Policy describes the things that an
/// operator would generally want to control.  The blueprint describes the
/// details of implementing that policy that an operator shouldn't have to deal
/// with.  For example, the operator might write policy that says "I want
/// 5 external DNS zones".  The system could then generate a blueprint that
/// _has_ 5 external DNS zones on 5 specific sleds.  The blueprint includes all
/// the details needed to achieve that, including which image these zones should
/// run, which zpools their persistent data should be stored on, their public and
/// private IP addresses, their internal DNS names, etc.
///
/// It must be possible for multiple Nexus instances to execute the same
/// blueprint concurrently and converge to the same thing.  Thus, these _cannot_
/// be how a blueprint works:
///
/// - "add a Nexus zone" -- two Nexus instances operating concurrently would
///   add _two_ Nexus zones (which is wrong)
/// - "ensure that there is a Nexus zone on this sled with this id" -- the IP
///   addresses and images are left unspecified.  Two Nexus instances could pick
///   different IPs or images for the zone.
///
/// This is why blueprints must be so detailed.  The key principle here is that
/// **all the work of ensuring that the system do the right thing happens in one
/// process (the update planner in one Nexus instance).  Once a blueprint has
/// been committed, everyone is on the same page about how to execute it.**  The
/// intent is that this makes both planning and executing a lot easier.  In
/// particular, by the time we get to execution, all the hard choices have
/// already been made.
///
/// Currently, blueprints are limited to describing only the set of Omicron
/// zones deployed on each host and some supporting configuration (e.g., DNS).
/// This is aimed at supporting add/remove sleds.  The plan is to grow this to
/// include more of the system as we support more use cases.
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
    /// This would generally be the uuid of a Nexus instance.
    pub creator: String,
    /// human-readable string describing why this blueprint was created
    /// (for debugging)
    pub comment: String,
}

impl Blueprint {
    pub fn all_omicron_zones(
        &self,
    ) -> impl Iterator<Item = (Uuid, &OmicronZoneConfig)> {
        self.omicron_zones
            .iter()
            .flat_map(|(sled_id, z)| z.zones.iter().map(|z| (*sled_id, z)))
    }

    pub fn sleds(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.omicron_zones.keys().copied()
    }

    pub fn diff<'a>(&'a self, other: &'a Blueprint) -> BlueprintDiff<'a> {
        BlueprintDiff { b1: self, b2: other }
    }
}

pub struct BlueprintDiff<'a> {
    b1: &'a Blueprint,
    b2: &'a Blueprint,
}

impl<'a> BlueprintDiff<'a> {
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

impl<'a> std::fmt::Display for BlueprintDiff<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "diff blueprints {} {}", self.b1.id, self.b2.id)?;
        writeln!(f, "--- {}", self.b1.id)?;
        writeln!(f, "+++ {}", self.b2.id)?;

        let b1 = &self.b1;
        let b2 = &self.b2;

        let all_sleds: BTreeSet<Uuid> = b1
            .sleds()
            .collect::<BTreeSet<_>>()
            .union(&b2.sleds().collect::<BTreeSet<_>>())
            .copied()
            .collect();

        for sled_id in all_sleds {
            // First, figure out if this sled is only in one blueprint or common
            // to both.
            let maybe_b1sled = b1.omicron_zones.get(&sled_id);
            let maybe_b2sled = b1.omicron_zones.get(&sled_id);
            let (b1sledzones, b2sledzones) = match (maybe_b1sled, maybe_b2sled)
            {
                (None, None) => unreachable!(),
                (Some(zones), None) => {
                    // This sled was removed altogether.  Print it and move on.
                    self.print_whole_sled(f, '-', "removed", zones, sled_id)?;
                    continue;
                }
                (None, Some(zones)) => {
                    // This sled was added.  Print it and move on.
                    self.print_whole_sled(f, '+', "added", zones, sled_id)?;
                    continue;
                }
                (Some(b1zones), Some(b2zones)) => (b1zones, b2zones),
            };

            // At this point, we're looking at a sled that's in both blueprints.
            // Print a line about the sled regardless of whether it's changed.
            writeln!(f, "  sled {}", sled_id)?;

            // Print a line about the zone config regardless of whether it's
            // changed.
            if b1sledzones.generation != b2sledzones.generation {
                writeln!(
                    f,
                    "-     zone config generation {}",
                    b1sledzones.generation
                )?;
                writeln!(
                    f,
                    "+     zone config generation {}",
                    b2sledzones.generation
                )?;
            } else {
                writeln!(
                    f,
                    "      zone config generation {}",
                    b1sledzones.generation
                )?;
            }

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
                            in_service: b1.zones_in_service.contains(&zone.id),
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
                            in_service: b2.zones_in_service.contains(&zone.id),
                        },
                    )
                })
                .collect();

            // Now go through each zone and compare them.
            for (zone_id, b1z_info) in &b1zones {
                let zone_type = b1z_info.zone.zone_type.label();
                if let Some(b2z_info) = b2zones.get(zone_id) {
                    let zone2_type = b2z_info.zone.zone_type.label();
                    if b1z_info.zone != b2z_info.zone {
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
                    } else if b1z_info.in_service && !b2z_info.in_service {
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
                    } else if !b1z_info.in_service && b2z_info.in_service {
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
                    } else {
                        writeln!(
                            f,
                            "         zone {} type {} (unchanged)",
                            zone_id, zone_type,
                        )?;
                    }
                } else {
                    writeln!(
                        f,
                        "-        zone {} type {} (removed)",
                        zone_id, zone_type,
                    )?;
                }
            }

            for (zone_id, b2z_info) in &b2zones {
                if b1zones.contains_key(&zone_id) {
                    // We handled this zone above.
                    continue;
                }

                writeln!(
                    f,
                    "+        zone {} type {} (added)",
                    zone_id,
                    b2z_info.zone.zone_type.label(),
                )?;
            }
        }

        Ok(())
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

pub mod views {
    use schemars::JsonSchema;
    use serde::Serialize;
    use thiserror::Error;
    use uuid::Uuid;

    /// Describes what blueprint, if any, the system is currently working toward
    #[derive(Debug, Serialize, JsonSchema)]
    pub struct BlueprintTarget {
        /// id of the blueprint that the system is trying to make real
        pub target_id: Uuid,
        /// policy: should the system actively work towards this blueprint
        ///
        /// This should generally be left enabled.
        pub enabled: bool,
        /// when this blueprint was made the target
        pub time_set: chrono::DateTime<chrono::Utc>,
    }

    #[derive(Debug, Error)]
    #[error("no target blueprint has been configured")]
    pub struct NoTargetBlueprint;

    impl TryFrom<super::BlueprintTarget> for BlueprintTarget {
        type Error = NoTargetBlueprint;

        fn try_from(
            value: super::BlueprintTarget,
        ) -> Result<Self, NoTargetBlueprint> {
            Ok(BlueprintTarget {
                target_id: value.target_id.ok_or(NoTargetBlueprint)?,
                enabled: value.enabled,
                time_set: value.time_set,
            })
        }
    }
}

pub mod params {
    use schemars::JsonSchema;
    use serde::Deserialize;
    use uuid::Uuid;

    /// Specifies what blueprint, if any, the system should be working toward
    #[derive(Deserialize, JsonSchema)]
    pub struct BlueprintTargetSet {
        pub target_id: Uuid,
        pub enabled: bool,
    }
}
