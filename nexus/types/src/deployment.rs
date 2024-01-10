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
    /// set of sleds that are supposed to be part of the system
    // XXX-dap If we want this mechanism to be used for the process of creating
    // a "sled" for a baseboard, then we might need this to be set of something
    // else -- baseboard ids?  But how would that work for cases that don't have
    // baseboard ids? (testing, PCs)
    pub sleds: BTreeSet<Uuid>,
    // XXX-dap zpools might need to be here, along with whatever else RSS uses
    // while it's allocating zones.  (zpools could be part of inventory if we
    // think of it as "stuff that's out there and has no associated policy".
    // But if we might want to have controls on them (e.g., "don't use this
    // zpool"), then at least some part of it needs to be here.  And right now
    // they exist in the database (I think?) so it seems reasonable to assemble
    // them into the policy.)
}

/// Describes a complete set of software and configuration for the system
///
/// Blueprints are a fundamental part of how the system updates itself.  Each
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
#[derive(Debug, Clone)]
pub struct Blueprint {
    /// unique identifier for this blueprint
    pub id: Uuid,

    /// set of sleds that we consider part of the control plane
    pub sleds: BTreeSet<Uuid>,

    /// mapping: sled id -> zones deployed on each sled
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
    pub reason: String,
}

impl Blueprint {
    pub fn all_omicron_zones(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneConfig> {
        self.omicron_zones.values().flat_map(|z| z.zones.iter())
    }
}

// This is analogous to the db model type until we have that.
#[derive(Debug, Clone)]
pub struct BlueprintTarget {
    pub target_id: Option<Uuid>,
    pub enabled: bool,
    pub time_set: chrono::DateTime<chrono::Utc>,
}

pub mod views {
    use super::OmicronZonesConfig;
    use schemars::JsonSchema;
    use serde::Serialize;
    use std::collections::{BTreeMap, BTreeSet};
    use uuid::Uuid;

    // XXX-dap
    #[derive(Serialize, JsonSchema)]
    pub struct Blueprint {
        pub id: Uuid,
        pub parent_blueprint_id: Option<Uuid>,
        pub time_created: chrono::DateTime<chrono::Utc>,
        pub creator: String,
        pub reason: String,

        pub sleds: BTreeSet<Uuid>,
        pub omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,
        pub zones_in_service: BTreeSet<Uuid>,
    }
    impl From<super::Blueprint> for Blueprint {
        fn from(generic: super::Blueprint) -> Self {
            Blueprint {
                id: generic.id,
                parent_blueprint_id: generic.parent_blueprint_id,
                time_created: generic.time_created,
                creator: generic.creator,
                reason: generic.reason,
                sleds: generic.sleds,
                omicron_zones: generic.omicron_zones,
                zones_in_service: generic.zones_in_service,
            }
        }
    }

    /// Describes what blueprint, if any, the system is currently working toward
    // XXX-dap should we just make this non-optional and produce an error (404?
    // 400? 500?) if there's no target yet?
    #[derive(Debug, Serialize, JsonSchema)]
    pub struct BlueprintTarget {
        pub target_id: Option<Uuid>,
        pub enabled: bool,
        pub set_at: chrono::DateTime<chrono::Utc>,
    }

    impl From<super::BlueprintTarget> for BlueprintTarget {
        fn from(value: super::BlueprintTarget) -> Self {
            BlueprintTarget {
                target_id: value.target_id,
                enabled: value.enabled,
                set_at: value.time_set,
            }
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
