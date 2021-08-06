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

/// Deserialization from the DB.
// TODO: delete from model_db, move here when everything else has migrated.
/*
impl TryFrom<&tokio_postgres::Row> for IdentityMetadata {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let time_deleted: Option<DateTime<Utc>> =
            sql_row_value(value, "time_deleted")?;

        // We could support representing deleted objects, but we would want to
        // think about how to do that.  For example, we might want to use
        // separate types so that the control plane can't accidentally do things
        // like attach a disk to a deleted Instance.  We haven't figured any of
        // this out, and there's no need yet.
        if time_deleted.is_none() {
            return Err(external::Error::internal_error("model does not support objects that have been deleted"));
        }
        Ok(IdentityMetadata {
            id: sql_row_value(value, "id")?,
            name: sql_row_value(value, "name")?,
            description: sql_row_value(value, "description")?,
            time_created: sql_row_value(value, "time_created")?,
            time_modified: sql_row_value(value, "time_modified")?,
        })
    }
}
*/

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

/// A Disk (network block device).
#[derive(Clone, Debug)]
pub struct Disk {
    /// common identifying metadata.
    pub identity: IdentityMetadata,
    /// id for the project containing this Disk
    pub project_id: Uuid,
    /// id for the snapshot from which this Disk was created (None means a blank
    /// disk)
    pub create_snapshot_id: Option<Uuid>,
    /// size of the Disk
    pub size: ByteCount,
    /// runtime state of the Disk
    pub runtime: DiskRuntimeState,
}

impl Disk {
    pub fn new(
        disk_id: Uuid,
        project_id: Uuid,
        params: external::DiskCreateParams,
        runtime_initial: DiskRuntimeState
    ) -> Self {
        let now = Utc::now();
        Self {
            identity: external::IdentityMetadata {
                id: disk_id,
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now,
                time_modified: now,
            },
            project_id,
            create_snapshot_id: params.snapshot_id,
            size: params.size,
            runtime: runtime_initial,
        }
    }
}

/// Conversion to the external API type.
impl Into<external::DiskView> for Disk {
    fn into(self) -> external::DiskView {
        let device_path = format!("/mnt/{}", self.identity.name.as_str());
        external::DiskView {
            identity: self.identity.clone(),
            project_id: self.project_id,
            snapshot_id: self.create_snapshot_id,
            size: self.size,
            state: self.runtime.disk_state.into(),
            device_path,
        }
    }
}

/// Serialization to the DB.
impl SqlSerialize for Disk {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("project_id", &self.project_id);
        self.runtime.sql_serialize(output);
        output.set("size_bytes", &self.size);
        output.set("origin_snapshot", &self.create_snapshot_id);
    }
}

/// Deserialization from the DB.
impl TryFrom<&tokio_postgres::Row> for Disk {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Disk {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            create_snapshot_id: sql_row_value(value, "origin_snapshot")?,
            size: sql_row_value(value, "size_bytes")?,
            runtime: DiskRuntimeState::try_from(value)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DiskRuntimeState {
    /// runtime state of the Disk
    pub disk_state: DiskState,
    /// generation number for this state
    pub gen: Generation,
    /// timestamp for this information
    pub time_updated: DateTime<Utc>,
}

/// Conversion from the internal API type.
impl From<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn from(runtime: internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: runtime.disk_state.into(),
            gen: runtime.gen,
            time_updated: runtime.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn into(self) -> internal::nexus::DiskRuntimeState {
        internal::nexus::DiskRuntimeState {
            disk_state: self.disk_state.into(),
            gen: self.gen,
            time_updated: self.time_updated,
        }
    }
}

/// Serialization to the DB.
impl SqlSerialize for DiskRuntimeState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.disk_state.sql_serialize(output);
        output.set("state_generation", &self.gen);
        output.set("time_state_updated", &self.time_updated);
    }
}

/// Deserialization from the DB.
impl TryFrom<&tokio_postgres::Row> for DiskRuntimeState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(DiskRuntimeState {
            disk_state: DiskState::try_from(value)?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DiskState(external::DiskState);

impl DiskState {
    pub fn new(state: external::DiskState) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::DiskState {
        &self.0
    }

    pub fn is_attached(&self) -> bool {
        self.0.is_attached()
    }

    pub fn attached_instance_id(&self) -> Option<&Uuid> {
        self.0.attached_instance_id()
    }
}

/// Conversion from the external API type.
impl From<external::DiskState> for DiskState {
    fn from(state: external::DiskState) -> Self {
        Self(state)
    }
}
/// Conversion to the external API type.
impl Into<external::DiskState> for DiskState {
    fn into(self) -> external::DiskState {
        self.0
    }
}

/// Serialization to the DB.
impl SqlSerialize for DiskState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        let attach_id = &self.0.attached_instance_id().map(|id| *id);
        output.set("attach_instance_id", attach_id);
        output.set("disk_state", &self.0.label());
    }
}

/// Deserialization from the DB.
impl TryFrom<&tokio_postgres::Row> for DiskState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let disk_state_str: &str = sql_row_value(value, "disk_state")?;
        let instance_uuid: Option<Uuid> =
            sql_row_value(value, "attach_instance_id")?;
        Ok(
            DiskState(
            external::DiskState::try_from((disk_state_str, instance_uuid))
                .map_err(|e| Error::internal_error(&e))?
            )
        )
    }
}

#[derive(Clone, Debug)]
pub struct DiskAttachment {
    pub instance_id: Uuid,
    pub disk_id: Uuid,
    pub disk_name: external::Name,
    pub disk_state: DiskState,
}

impl Into<external::DiskAttachment> for DiskAttachment {
    fn into(self) -> external::DiskAttachment {
        external::DiskAttachment {
            instance_id: self.instance_id,
            disk_id: self.disk_id,
            disk_name: self.disk_name,
            disk_state: self.disk_state.into(),
        }
    }
}

impl TryFrom<&tokio_postgres::Row> for DiskAttachment {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(DiskAttachment {
            instance_id: sql_row_value(value, "attach_instance_id")?,
            disk_id: sql_row_value(value, "id")?,
            disk_name: sql_row_value(value, "name")?,
            disk_state: DiskState::try_from(value)?,
        })
    }
}
