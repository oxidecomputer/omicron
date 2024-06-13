// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Defines database model types for the Vmm table.
//!
//! A row in the Vmm table stores information about a single Propolis VMM
//! running on a specific sled that incarnates a specific instance. A VMM's
//! instance ID, sled assignment, and Propolis server IP are all fixed for the
//! lifetime of the VMM. As with instances, the VMM's lifecycle-related state is
//! broken out into a separate type that allows sled agent and Nexus to send VMM
//! state updates to each other without sending parameters that are useless to
//! sled agent or that sled agent will never update (like the sled ID).

use super::{Generation, VmmState};
use crate::schema::vmm;
use crate::SqlU16;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An individual VMM process that incarnates a specific instance.
#[derive(
    Clone,
    Queryable,
    Debug,
    Selectable,
    Serialize,
    Deserialize,
    Insertable,
    PartialEq,
)]
#[diesel(table_name = vmm)]
pub struct Vmm {
    /// This VMM's primary ID, referred to by an `Instance`'s `propolis_id` or
    /// `target_propolis_id` fields.
    pub id: Uuid,

    /// The time this VMM record was created.
    pub time_created: DateTime<Utc>,

    /// The time this VMM was destroyed.
    pub time_deleted: Option<DateTime<Utc>>,

    /// The ID of the `Instance` that owns this VMM.
    pub instance_id: Uuid,

    /// The sled assigned to the care and feeding of this VMM.
    pub sled_id: Uuid,

    /// The IP address at which this VMM is serving the Propolis server API.
    pub propolis_ip: ipnetwork::IpNetwork,

    /// The socket port on which this VMM is serving the Propolis server API.
    pub propolis_port: SqlU16,

    /// Runtime state for the VMM.
    #[diesel(embed)]
    pub runtime: VmmRuntimeState,
}

/// The set of states that a VMM can have when it is created.
pub enum VmmInitialState {
    Starting,
    Migrating,
}

impl Vmm {
    /// Creates a new VMM record.
    pub fn new(
        id: Uuid,
        instance_id: Uuid,
        sled_id: Uuid,
        propolis_ip: ipnetwork::IpNetwork,
        propolis_port: u16,
        initial_state: VmmInitialState,
    ) -> Self {
        let now = Utc::now();
        let state = match initial_state {
            VmmInitialState::Starting => VmmState::Starting,
            VmmInitialState::Migrating => VmmState::Migrating,
        };

        Self {
            id,
            time_created: now,
            time_deleted: None,
            instance_id,
            sled_id,
            propolis_ip,
            propolis_port: SqlU16(propolis_port),
            runtime: VmmRuntimeState {
                state,
                time_state_updated: now,
                gen: Generation::new(),
            },
        }
    }
}

/// Runtime state for a VMM, owned by the sled where that VMM is running.
#[derive(
    Clone,
    Debug,
    AsChangeset,
    Selectable,
    Insertable,
    Queryable,
    Serialize,
    Deserialize,
    PartialEq,
)]
#[diesel(table_name = vmm)]
pub struct VmmRuntimeState {
    /// The time at which this state was most recently updated.
    pub time_state_updated: DateTime<Utc>,

    /// The generation number protecting this VMM's state and update time.
    #[diesel(column_name = state_generation)]
    pub gen: Generation,

    /// The state of this VMM. If this VMM is the active VMM for a given
    /// instance, this state is the instance's logical state.
    pub state: VmmState,
}

impl From<omicron_common::api::internal::nexus::VmmRuntimeState>
    for VmmRuntimeState
{
    fn from(
        value: omicron_common::api::internal::nexus::VmmRuntimeState,
    ) -> Self {
        Self {
            state: value.state.into(),
            time_state_updated: value.time_updated,
            gen: value.gen.into(),
        }
    }
}

impl From<Vmm> for sled_agent_client::types::VmmRuntimeState {
    fn from(s: Vmm) -> Self {
        Self {
            gen: s.runtime.gen.into(),
            state: s.runtime.state.into(),
            time_updated: s.runtime.time_state_updated,
        }
    }
}
