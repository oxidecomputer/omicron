// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Deployment details crated by the update planner

pub use sled_agent_client::types::OmicronZonesConfig;
use std::{collections::BTreeMap, net::SocketAddr};
use uuid::Uuid;

/// An individual decision made by the update planner in order to drive the
/// state of the system forward and align policy and inventory.
///
/// The planner is constantly operating as a background task and reconciling
/// the state of the system as denoted by the inventory with the policy imposed
/// by the operator and oxide internal system requirements. In this scenario
/// the policy represents an abstract desired state of the system and the
/// planner is constantly looking at the inventory until it reflects a concrete
/// implementation of the abstract policy. Additionally, the planner can decide
/// at any point to *cancel* the current plan and make a new plan active if the
/// current plan no longer serves to drive the concrete representation of the
/// system towards the desired policy.
///
/// A `Blueprint` represents the current plan, although it should not be viewed
/// as the desired state of the entire system, but rather as the desired state
/// of  a small part of the system that will serve to align the entire concrete
/// system over time with the current policy. In other words, each `Blueprint`
/// represents an incremental state transition from the current state of the
/// system towards a new state of the system that is closer to realizing the
/// policy than before. When the state of the system as represented by the
/// inventory matches the policy, there is no more work for the planner to do.
/// At this point, in our analogy, if you were to take all blueprints and stack
/// them on top of each other, they would generate a complete blueprint of the
/// entire system as it exists.
///
/// Each `Blueprint` represents an incremental desired state of a subset of
/// the system in order to minimize the decisions made by the plan executor
/// background task. Each executor will attempt to apply the current `Blueprint`
/// without the need to record runtime information in CRDB, since enough
/// state will exist in the `Blueprint` to make this unnecessary. A minimal
/// `Blueprint` also allows multiple executors to operate concurrently with
/// minimal explicit support for this concurrency. And by making the steps small
/// and incremental, it allows easier correction of mistaken plan directions
/// based on inventory changes, while also enabling the encapsulation of each
/// `Blueprint` operation.
#[derive(Debug, PartialEq, Eq)]
pub enum Blueprint {
    // Mapping from Sled UUID to (sled-agent address, zone config) pair
    OmicronZones(BTreeMap<Uuid, (SocketAddr, OmicronZonesConfig)>),
    //Dns(SomeDnsMapping)
}

impl Blueprint {
    /// Return a description of the specific blueprint variant
    pub fn description(&self) -> &'static str {
        match self {
            Blueprint::OmicronZones(_) => "deploying zones",
        }
    }
}
