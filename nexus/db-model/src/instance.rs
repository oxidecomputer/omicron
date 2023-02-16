// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ByteCount, Disk, Generation, InstanceCpuCount, InstanceState};
use crate::collection::DatastoreAttachTargetConfig;
use crate::schema::{disk, instance};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::address::PROPOLIS_PORT;
use omicron_common::api::external;
use omicron_common::api::internal;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use uuid::Uuid;

/// An Instance (VM).
#[derive(
    Queryable, Insertable, Debug, Selectable, Resource, Serialize, Deserialize,
)]
#[diesel(table_name = instance)]
pub struct Instance {
    #[diesel(embed)]
    identity: InstanceIdentity,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// user data for instance initialization systems (e.g. cloud-init)
    pub user_data: Vec<u8>,

    /// runtime state of the Instance
    #[diesel(embed)]
    pub runtime_state: InstanceRuntimeState,
}

impl Instance {
    pub fn new(
        instance_id: Uuid,
        project_id: Uuid,
        params: &params::InstanceCreate,
        runtime: InstanceRuntimeState,
    ) -> Self {
        let identity =
            InstanceIdentity::new(instance_id, params.identity.clone());
        Self {
            identity,
            project_id,
            user_data: params.user_data.clone(),
            runtime_state: runtime,
        }
    }

    pub fn runtime(&self) -> &InstanceRuntimeState {
        &self.runtime_state
    }
}

/// Conversion to the external API type.
impl Into<external::Instance> for Instance {
    fn into(self) -> external::Instance {
        external::Instance {
            identity: self.identity(),
            project_id: self.project_id,
            ncpus: self.runtime().ncpus.into(),
            memory: self.runtime().memory.into(),
            hostname: self.runtime().hostname.clone(),
            runtime: self.runtime().clone().into(),
        }
    }
}

impl DatastoreAttachTargetConfig<Disk> for Instance {
    type Id = Uuid;

    type CollectionIdColumn = instance::dsl::id;
    type CollectionTimeDeletedColumn = instance::dsl::time_deleted;

    type ResourceIdColumn = disk::dsl::id;
    type ResourceCollectionIdColumn = disk::dsl::attach_instance_id;
    type ResourceTimeDeletedColumn = disk::dsl::time_deleted;
}

/// Runtime state of the Instance, including the actual running state and minimal
/// metadata
///
/// This state is owned by the sled agent running that Instance.
#[derive(
    Clone,
    Debug,
    AsChangeset,
    Selectable,
    Insertable,
    Queryable,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = instance)]
pub struct InstanceRuntimeState {
    /// runtime state of the Instance
    #[diesel(column_name = state)]
    pub state: InstanceState,
    /// timestamp for this information
    // TODO: Is this redundant with "time_modified"?
    #[diesel(column_name = time_state_updated)]
    pub time_updated: DateTime<Utc>,
    /// generation number for this state
    #[diesel(column_name = state_generation)]
    pub gen: Generation,
    /// which sled is running this Instance
    // TODO: should this be optional?
    #[diesel(column_name = active_server_id)]
    pub sled_id: Uuid,
    #[diesel(column_name = active_propolis_id)]
    pub propolis_id: Uuid,
    #[diesel(column_name = active_propolis_ip)]
    pub propolis_ip: Option<ipnetwork::IpNetwork>,
    #[diesel(column_name = target_propolis_id)]
    pub dst_propolis_id: Option<Uuid>,
    #[diesel(column_name = migration_id)]
    pub migration_id: Option<Uuid>,
    #[diesel(column_name = ncpus)]
    pub ncpus: InstanceCpuCount,
    #[diesel(column_name = memory)]
    pub memory: ByteCount,
    // TODO-cleanup: Different type?
    #[diesel(column_name = hostname)]
    pub hostname: String,
}

impl From<InstanceRuntimeState>
    for sled_agent_client::types::InstanceRuntimeState
{
    fn from(s: InstanceRuntimeState) -> Self {
        Self {
            run_state: s.state.into(),
            sled_id: s.sled_id,
            propolis_id: s.propolis_id,
            dst_propolis_id: s.dst_propolis_id,
            propolis_addr: s
                .propolis_ip
                .map(|ip| SocketAddr::new(ip.ip(), PROPOLIS_PORT).to_string()),
            migration_id: s.migration_id,
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

/// Conversion to the external API type.
impl Into<external::InstanceRuntimeState> for InstanceRuntimeState {
    fn into(self) -> external::InstanceRuntimeState {
        external::InstanceRuntimeState {
            run_state: *self.state.state(),
            time_run_state_updated: self.time_updated,
        }
    }
}

/// Conversion from the internal API type.
impl From<internal::nexus::InstanceRuntimeState> for InstanceRuntimeState {
    fn from(state: internal::nexus::InstanceRuntimeState) -> Self {
        Self {
            state: InstanceState::new(state.run_state),
            sled_id: state.sled_id,
            propolis_id: state.propolis_id,
            dst_propolis_id: state.dst_propolis_id,
            propolis_ip: state.propolis_addr.map(|addr| addr.ip().into()),
            migration_id: state.migration_id,
            ncpus: state.ncpus.into(),
            memory: state.memory.into(),
            hostname: state.hostname,
            gen: state.gen.into(),
            time_updated: state.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::InstanceRuntimeState> for InstanceRuntimeState {
    fn into(self) -> internal::nexus::InstanceRuntimeState {
        internal::nexus::InstanceRuntimeState {
            run_state: *self.state.state(),
            sled_id: self.sled_id,
            propolis_id: self.propolis_id,
            dst_propolis_id: self.dst_propolis_id,
            propolis_addr: self
                .propolis_ip
                .map(|ip| SocketAddr::new(ip.ip(), PROPOLIS_PORT)),
            migration_id: self.migration_id,
            ncpus: self.ncpus.into(),
            memory: self.memory.into(),
            hostname: self.hostname,
            gen: self.gen.into(),
            time_updated: self.time_updated,
        }
    }
}
