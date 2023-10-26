// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ByteCount, Disk, Generation, InstanceCpuCount, InstanceState};
use crate::collection::DatastoreAttachTargetConfig;
use crate::schema::{disk, instance};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::params;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// An Instance (VM).
#[derive(
    Clone,
    Debug,
    Queryable,
    Insertable,
    Selectable,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = instance)]
pub struct Instance {
    #[diesel(embed)]
    identity: InstanceIdentity,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// user data for instance initialization systems (e.g. cloud-init)
    pub user_data: Vec<u8>,

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

    #[diesel(column_name = boot_on_fault)]
    pub boot_on_fault: bool,

    #[diesel(embed)]
    pub runtime_state: InstanceRuntimeState,
}

impl Instance {
    /// Constructs a new instance record with no VMM that will initially appear
    /// to be in the Creating state.
    pub fn new(
        instance_id: Uuid,
        project_id: Uuid,
        params: &params::InstanceCreate,
    ) -> Self {
        let identity =
            InstanceIdentity::new(instance_id, params.identity.clone());

        let runtime_state = InstanceRuntimeState::new(
            InstanceState::new(
                omicron_common::api::external::InstanceState::Creating,
            ),
            identity.time_modified,
        );

        Self {
            identity,
            project_id,
            user_data: params.user_data.clone(),
            ncpus: params.ncpus.into(),
            memory: params.memory.into(),
            hostname: params.hostname.clone(),
            boot_on_fault: false,
            runtime_state,
        }
    }

    pub fn runtime(&self) -> &InstanceRuntimeState {
        &self.runtime_state
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
    /// The instance state to fall back on if asked to compute this instance's
    /// state while it has no active VMM.
    ///
    /// This field is guarded by the instance's `gen` field.
    #[diesel(column_name = state)]
    pub nexus_state: InstanceState,

    /// The time at which the runtime state was last updated. This is distinct
    /// from the time the record was last modified, because some updates don't
    /// modify the runtime state.
    #[diesel(column_name = time_state_updated)]
    pub time_updated: DateTime<Utc>,

    /// The generation number for the information stored in this structure,
    /// including the fallback state, the instance's active Propolis ID, and its
    /// migration IDs.
    #[diesel(column_name = state_generation)]
    pub gen: Generation,

    /// The ID of the Propolis server hosting the current incarnation of this
    /// instance, or None if the instance has no active VMM.
    ///
    /// This field is guarded by the instance's `gen`.
    #[diesel(column_name = active_propolis_id)]
    pub propolis_id: Option<Uuid>,

    /// If a migration is in progress, the ID of the Propolis server that is
    /// the migration target.
    ///
    /// This field is guarded by the instance's `gen`.
    #[diesel(column_name = target_propolis_id)]
    pub dst_propolis_id: Option<Uuid>,

    /// If a migration is in progress, a UUID identifying that migration. This
    /// can be used to provide mutual exclusion between multiple attempts to
    /// migrate and between an attempt to migrate an attempt to mutate an
    /// instance in a way that's incompatible with migration.
    ///
    /// This field is guarded by the instance's `gen`.
    #[diesel(column_name = migration_id)]
    pub migration_id: Option<Uuid>,
}

impl InstanceRuntimeState {
    fn new(initial_state: InstanceState, creation_time: DateTime<Utc>) -> Self {
        Self {
            nexus_state: initial_state,
            time_updated: creation_time,
            propolis_id: None,
            dst_propolis_id: None,
            migration_id: None,
            gen: Generation::new(),
        }
    }
}

impl From<omicron_common::api::internal::nexus::InstanceRuntimeState>
    for InstanceRuntimeState
{
    fn from(
        state: omicron_common::api::internal::nexus::InstanceRuntimeState,
    ) -> Self {
        let nexus_state = if state.propolis_id.is_some() {
            omicron_common::api::external::InstanceState::Running
        } else {
            omicron_common::api::external::InstanceState::Stopped
        };

        Self {
            nexus_state: InstanceState::new(nexus_state),
            time_updated: state.time_updated,
            gen: state.gen.into(),
            propolis_id: state.propolis_id,
            dst_propolis_id: state.dst_propolis_id,
            migration_id: state.migration_id,
        }
    }
}

impl From<InstanceRuntimeState>
    for sled_agent_client::types::InstanceRuntimeState
{
    fn from(state: InstanceRuntimeState) -> Self {
        Self {
            dst_propolis_id: state.dst_propolis_id,
            gen: state.gen.into(),
            migration_id: state.migration_id,
            propolis_id: state.propolis_id,
            time_updated: state.time_updated,
        }
    }
}
