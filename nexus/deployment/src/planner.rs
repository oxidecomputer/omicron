// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic deployment planner types and implementation
//!
//! See crate-level docs for details.

use crate::policy::Policy;
use nexus_types::inventory::Collection;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Pluggable planner implementation
///
/// This is a trait so that we can provide various impls that cover various
/// parts of the plan.  For example, one impl might only know about MGS-managed
/// software (the SP, RoT, and host OS software).  Another impl might only know
/// about Omicron zones.
pub trait Planner {
    /// Returns whether the given plan is consistent with the given policy
    fn plan_acceptable(
        &self,
        policy: &Policy,
        latest_collection: Option<&Collection>,
        plan: &Plan,
    ) -> PlanAcceptability;

    /// Uses `builder` to generate a new plan consistent with the latest policy
    /// and collection
    fn plan_generate(
        &self,
        policy: &Policy,
        latest_collection: Option<&Collection>,
        builder: &mut PlanBuilder,
    ) -> anyhow::Result<()>;
}

pub enum PlanAcceptability {
    /// the plan is consistent with the policy
    Acceptable,
    /// the plan is not consistent with the policy
    Unacceptable {
        /// reasons why the plan is not consistent with the policy
        reasons: Vec<String>,
    },
}

// XXX-dap fill it in
// XXX-dap TODO-doc
pub struct Plan {
    /// Omicron zones
    zones: BTreeMap<Uuid, OmicronZone>,
    /// Sleds used
    sleds: BTreeSet<Uuid>,
}

impl Plan {
    pub fn sled_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.sleds.iter().cloned()
    }

    // XXX-dap TODO-doc expected to be useful during execution to assemble PUT
    // /services request
    pub fn zones_on_sled(
        &self,
        sled_id: Uuid,
    ) -> impl Iterator<Item = &OmicronZone> {
        self.zones.values().filter(move |zone| zone.sled_id == sled_id)
    }

    // XXX-dap zones-by-kind will be useful during plan validation to ensure we
    // have the right number of everything.  But we might need it by version,
    // too?
}

// XXX-dap the specific data and types here are not super clear.  On the one
// hand, it basically needs to include everything that's needed to provision
// this zone, which means it basically could be a
// `sled_agent_client::types::ServiceZoneRequest`.  Of course, the sled agent
// API could evolve over time (XXX-dap possibly its own huge problem, since we
// need to talk to multiples of them).  Should we have our own copy of these
// types?  We kind of already do in the form of the Nexus internal API
// ServiceKind.
// For now, this is modeled on `ServiceZoneRequest` but re-implements much of
// it.  **It's also much more constrained than ServiceZoneRequest (see the
// comments below) but I'm hopeful that we can generate a ServiceZoneRequest
// _from_ it.**  For example, we don't store the dataset pool name's "kind"
// because it's derivable from the other information in the struct.
// XXX-dap there's a lot I still don't understand here
// - why does the dataset have its own id?  service_address?  name.kind?
// - why is there more than one IPv6 address for the zone?
// - do we still use the idea of more than one service in each zone?  if so, is
//   this where service-specific configuration lives?  (seems like it)
// - what's the deal with the overlap between "zone_type" and "services" (i.e.,
//   each type of zone always has the same services, right?)
pub struct OmicronZone {
    sled_id: Uuid,
    id: Uuid,
    // XXX-dap why is there more than one?
    addresses: Vec<Ipv6Addr>,
    dataset: Option<OmicronZoneDataset>,
    // XXX-dap service-specific enum and associated config goes here
    // maybe better to do the inventory side of this before this part.
}

struct OmicronZoneDataset {
    pool_name: String, // XXX-dap newtype
}

// XXX-dap fill in
// XXX-dap TODO-doc
pub struct PlanBuilder {
    zones: BTreeMap<Uuid, OmicronZone>,
    sleds: BTreeSet<Uuid>,
}

impl PlanBuilder {
    pub fn new() -> PlanBuilder {
        PlanBuilder { zones: BTreeMap::new(), sleds: BTreeSet::new() }
    }

    pub fn existing_zone(&mut self) {
        // XXX-dap this should accept the inventory version of OmicronZone and
        // generate the plan OmicronZone from that
    }

    pub fn new_zone(&mut self, zone: OmicronZone) {
        // XXX-dap-deep uh oh where does the allocation of IPs happen?  During
        // execution?  What if we change plans mid-stream?  How do we
        // de-allocate them?
        self.sleds.insert(zone.sled_id);
        // XXX-dap panic or return error if the zone id already exists
        self.zones.insert(zone.id, zone);
    }

    pub fn build(self) -> Plan {
        Plan { zones: self.zones, sleds: self.sleds }
    }
}
