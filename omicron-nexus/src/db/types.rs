//! Structures stored to the database.

use chrono::{DateTime, Utc};
use omicron_common::api::external::{
    self, ByteCount, Error, Generation, IdentityMetadata, InstanceCpuCount,
};
use omicron_common::api::internal;
use omicron_common::db::sql_row_value;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use uuid::Uuid;

use super::sql::SqlSerialize;
use super::sql::SqlValueSet;

/// Serialization to DB.
impl SqlSerialize for external::IdentityMetadata {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("id", &self.id);
        output.set("name", &self.name);
        output.set("description", &self.description);
        output.set("time_created", &self.time_created);
        output.set("time_modified", &self.time_modified);

        // TODO: Is this right? When should this be set?
        output.set("time_deleted", &(None as Option<DateTime<Utc>>));
    }
}

/// Describes a project within the database.
pub struct Project(internal::nexus::Project);

impl Project {
    /// Creates a new database Project object.
    pub fn new(params: &external::ProjectCreateParams) -> Self {
        let id = Uuid::new_v4();
        let now = Utc::now();
        Self(internal::nexus::Project {
            identity: external::IdentityMetadata {
                id,
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now,
                time_modified: now,
            },
        })
    }

    pub fn name(&self) -> &str {
        self.0.identity.name.as_str()
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::Project> for Project {
    fn into(self) -> internal::nexus::Project {
        self.0
    }
}

/// Conversion from the internal API type.
impl From<internal::nexus::Project> for Project {
    fn from(project: internal::nexus::Project) -> Self {
        Self(project)
    }
}

/// Serialization to DB.
impl SqlSerialize for Project {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.0.identity.sql_serialize(output);
    }
}

/// Deserialization from DB.
impl TryFrom<&tokio_postgres::Row> for Project {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Project(internal::nexus::Project {
            identity: IdentityMetadata::try_from(value)?,
        }))
    }
}

/// An Instance (VM).
#[derive(Clone, Debug)]
pub struct Instance {
    /// common identifying metadata
    pub identity: IdentityMetadata,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// state owned by the data plane
    pub runtime: InstanceRuntimeState,
    // TODO-completeness: add disks, network, tags, metrics
    /// number of CPUs allocated for this Instance
    pub ncpus: InstanceCpuCount,
    /// memory allocated for this Instance
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the Instance.
    // TODO-cleanup different type?
    pub hostname: String,
}

impl Instance {
    pub fn new(
        instance_id: Uuid,
        project_id: Uuid,
        params: &external::InstanceCreateParams,
        runtime: InstanceRuntimeState,
    ) -> Self {
        let now = Utc::now();
        Self {
            identity: external::IdentityMetadata {
                id: instance_id,
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now,
                time_modified: now,
            },
            project_id,
            ncpus: params.ncpus,
            memory: params.memory,
            hostname: params.hostname.clone(),
            runtime,
        }
    }
}

/// Conversion to the external API type.
impl Into<external::InstanceView> for Instance {
    fn into(self) -> external::InstanceView {
        external::InstanceView {
            identity: self.identity.clone(),
            project_id: self.project_id,
            ncpus: self.ncpus,
            memory: self.memory,
            hostname: self.hostname.clone(),
            runtime: self.runtime.into(),
        }
    }
}

/// Serialization to DB.
impl SqlSerialize for Instance {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("project_id", &self.project_id);
        self.runtime.sql_serialize(output);
        output.set("ncpus", &self.ncpus);
        output.set("memory", &self.memory);
        output.set("hostname", &self.hostname);
    }
}

/// Deserialization from DB.
impl TryFrom<&tokio_postgres::Row> for Instance {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Instance {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            ncpus: sql_row_value(value, "ncpus")?,
            memory: sql_row_value(value, "memory")?,
            hostname: sql_row_value(value, "hostname")?,
            runtime: InstanceRuntimeState::try_from(value)?,
        })
    }
}

/// Runtime state of the Instance, including the actual running state and minimal
/// metadata
///
/// This state is owned by the sled agent running that Instance.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InstanceRuntimeState {
    /// runtime state of the Instance
    pub run_state: InstanceState,
    /// which sled is running this Instance
    pub sled_uuid: Uuid,
    /// generation number for this state
    pub gen: Generation,
    /// timestamp for this information
    pub time_updated: DateTime<Utc>,
}

/// Conversion to the external API type.
impl Into<external::InstanceRuntimeStateView> for InstanceRuntimeState {
    fn into(self) -> external::InstanceRuntimeStateView {
        external::InstanceRuntimeStateView {
            run_state: self.run_state.0,
            time_run_state_updated: self.time_updated,
        }
    }
}

/// Conversion from the internal API type.
impl From<internal::nexus::InstanceRuntimeState> for InstanceRuntimeState {
    fn from(state: internal::nexus::InstanceRuntimeState) -> Self {
        Self {
            run_state: InstanceState(state.run_state),
            sled_uuid: state.sled_uuid,
            gen: state.gen,
            time_updated: state.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::InstanceRuntimeState> for InstanceRuntimeState {
    fn into(self) -> internal::nexus::InstanceRuntimeState {
        internal::sled_agent::InstanceRuntimeState {
            run_state: self.run_state.0,
            sled_uuid: self.sled_uuid,
            gen: self.gen,
            time_updated: self.time_updated,
        }
    }
}

/// Serialization to the database.
impl SqlSerialize for InstanceRuntimeState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.run_state.sql_serialize(output);
        output.set("active_server_id", &self.sled_uuid);
        output.set("state_generation", &self.gen);
        output.set("time_state_updated", &self.time_updated);
    }
}

/// Deserialization from the database.
impl TryFrom<&tokio_postgres::Row> for InstanceRuntimeState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(InstanceRuntimeState {
            run_state: InstanceState::try_from(value)?,
            sled_uuid: sql_row_value(value, "active_server_id")?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// A wrapper around the external "InstanceState" object,
/// which may be stored to disk.
#[derive(
    Copy, Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct InstanceState(external::InstanceState);

impl InstanceState {
    pub fn new(state: external::InstanceState) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::InstanceState {
        &self.0
    }
}

/// Serialization to the database.
impl SqlSerialize for InstanceState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("instance_state", &self.0.label());
    }
}

/// Deserialization from the database.
impl TryFrom<&tokio_postgres::Row> for InstanceState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let variant: &str = sql_row_value(value, "instance_state")?;
        Ok(InstanceState(
            external::InstanceState::try_from(variant)
                .map_err(|err| Error::InternalError { message: err })?,
        ))
    }
}
