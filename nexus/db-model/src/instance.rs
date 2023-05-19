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
// N.B. Setting `treat_none_as_null` is required for these fields to be cleared
//      properly during live migrations. See the documentation for
//      `diesel::prelude::AsChangeset`.
#[diesel(table_name = instance, treat_none_as_null = true)]
pub struct InstanceRuntimeState {
    /// The instance's current user-visible instance state.
    ///
    /// This field is guarded by the instance's `gen` field.
    #[diesel(column_name = state)]
    pub state: InstanceState,
    /// The time at which the runtime state was last updated. This is distinct
    /// from the time the record was last modified, because some updates don't
    /// modify the runtime state.
    #[diesel(column_name = time_state_updated)]
    pub time_updated: DateTime<Utc>,
    /// The generation number for the instance's user-visible state. Each
    /// successive state update from a single incarnation of an instance must
    /// bear a new generation number.
    #[diesel(column_name = state_generation)]
    pub gen: Generation,
    /// The ID of the sled hosting the current incarnation of this instance.
    ///
    /// This field is guarded by the instance's `propolis_gen`.
    //
    // TODO(#2315): This should be optional so that it can be cleared when the
    // instance is not active.
    #[diesel(column_name = active_sled_id)]
    pub sled_id: Uuid,
    /// The ID of the Propolis server hosting the current incarnation of this
    /// instance.
    ///
    /// This field is guarded by the instance's `propolis_gen`.
    #[diesel(column_name = active_propolis_id)]
    pub propolis_id: Uuid,
    /// The IP of the instance's current Propolis server.
    ///
    /// This field is guarded by the instance's `propolis_gen`.
    #[diesel(column_name = active_propolis_ip)]
    pub propolis_ip: Option<ipnetwork::IpNetwork>,
    /// If a migration is in progress, the ID of the Propolis server that is
    /// the migration target. Note that the target's sled agent will have a
    /// runtime state where `propolis_id` and `dst_propolis_id` are equal.
    ///
    /// This field is guarded by the instance's `propolis_gen`.
    #[diesel(column_name = target_propolis_id)]
    pub dst_propolis_id: Option<Uuid>,
    /// If a migration is in progress, a UUID identifying that migration. This
    /// can be used to provide mutual exclusion between multiple attempts to
    /// migrate and between an attempt to migrate an attempt to mutate an
    /// instance in a way that's incompatible with migration.
    ///
    /// This field is guarded by the instance's `propolis_gen`.
    #[diesel(column_name = migration_id)]
    pub migration_id: Option<Uuid>,
    /// A generation number protecting the instance's "location" information:
    /// its sled ID, Propolis ID and IP, and migration information. Each state
    /// update that updates one or more of these fields must bear a new
    /// Propolis generation.
    ///
    /// Records with new Propolis generations supersede records with older
    /// generations irrespective of their state generations. That is, a record
    /// with Propolis generation 4 and state generation 1 is "newer" than
    /// a record with Propolis generation 3 and state generation 5.
    #[diesel(column_name = propolis_generation)]
    pub propolis_gen: Generation,
    /// The number of vCPUs (i.e., virtual logical processors) to allocate for
    /// this instance.
    #[diesel(column_name = ncpus)]
    pub ncpus: InstanceCpuCount,
    /// The amount of guest memory to allocate for this instance.
    #[diesel(column_name = memory)]
    pub memory: ByteCount,
    /// The instance's hostname.
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
            propolis_gen: s.propolis_gen.into(),
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
            propolis_gen: state.propolis_gen.into(),
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
            propolis_gen: self.propolis_gen.into(),
            migration_id: self.migration_id,
            ncpus: self.ncpus.into(),
            memory: self.memory.into(),
            hostname: self.hostname,
            gen: self.gen.into(),
            time_updated: self.time_updated,
        }
    }
}
