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

pub use crate::inventory::Generation;
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
/// details of impleenting that policy that an operator shouldn't have to deal
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
// XXX-dap Consider whether this should be an enum like in #4732.  My going-in
// assumption was that this would be a _complete_ state of the world.  This came
// from the determination that it needs to _not_ be an incremental description
// of a change.  (Why not incremental?  Because the state of the world can
// change while we're trying to execute a blueprint.  If the blueprint itself is
// defined in terms of how it differs from the state of the world (e.g., "add
// one Nexus zone"), then its _meaning_ changes as the underlying state changes.
// That's not what we want.)  Having decided that blueprints must not be
// incremental, I concluded that they must describe the _whole_ state -- i.e.,
// each blueprint should describe all zones, all DNS names, all sleds
// in-service, and any other configuration managed by this system.
// A consequence of this is that some _sequences_ of blueprints are not valid.
// For example: it would be problematic to have a blueprint that (relative to
// the current state) moves all CockroachDB nodes from one set of sleds to
// another set of sleds because executing that could take down the CockroachDB
// cluster or even remove all the data, even though both the initial and final
// states are valid.
//
// If we instead view this as an enum of possible _changes_, we could make some
// of these invalid transitions unrepresentable.  For example, if this were an
// enum where one variant described a change to deployed zones and another
// described a change to DNS, then it would be impossible to try to change both
// at once.  That's probably good.  But it wouldn't address the example above,
// since this invalid change could be represented in the enum version, too.
//
// With the enum option, we're implicitly saying that the rest of the system is
// held constant -- but relative to what?  Should the blueprint have a reference
// to what inventory it was generated against?  That makes other things trickier
// (like deleting old inventory collections or even just seeing what the full
// state of the world is supposed to be when a blueprint has been executed).
pub struct Blueprint {
    /// mapping: sled id -> zones deployed on each sled
    pub omicron_zones: BTreeMap<Uuid, OmicronZonesConfig>,

    /// Omicron zones considered in-service (which generally means that they
    /// should appear in DNS)
    pub zones_in_service: BTreeSet<Uuid>,

    /// which collection this was generated from (for debugging)
    pub built_from_collection: Uuid,
    /// when this blueprint was generated (for debugging)
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// identity of the component that generated the blueprint (for debugging)
    /// This would generally be the uuid of a Nexus instance.
    pub creator: String,
    /// human-readable string describing why this blueprint was created
    /// (for debugging)
    pub reason: String,
}
