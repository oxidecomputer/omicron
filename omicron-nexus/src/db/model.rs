//! Structures stored to the database.

use super::diesel_schema::{disk, instance, project, metricproducer, oximeter, oximeterassignment};
use chrono::{DateTime, Utc};
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external::{
    self, ByteCount, Error, Generation, InstanceCpuCount,
};
use omicron_common::api::internal;
use omicron_common::db::sql_row_value;
use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};
use uuid::Uuid;

use super::sql::SqlSerialize;
use super::sql::SqlValueSet;

// TODO: Break up types into multiple files

// NOTE: This object is not currently stored in the database.
//
// However, it likely will be in the future - for the single-rack
// case, however, it is synthesized.
pub struct Rack {
    pub identity: IdentityMetadata,
}

impl Into<external::Rack> for Rack {
    fn into(self) -> external::Rack {
        external::Rack { identity: self.identity.into() }
    }
}

// NOTE: This object is not currently stored in the database.
//
// However, it likely will be in the future. At the moment,
// Nexus simply reports all the live connections it knows about.
pub struct Sled {
    pub identity: IdentityMetadata,
    pub service_address: SocketAddr,
}

impl Into<external::Sled> for Sled {
    fn into(self) -> external::Sled {
        external::Sled {
            identity: self.identity.into(),
            service_address: self.service_address,
        }
    }
}

// TODO: As Diesel needs to flatten things out, this structure
// may become unused.
#[derive(Clone, Debug)]
pub struct IdentityMetadata {
    pub id: Uuid,
    pub name: external::Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl IdentityMetadata {
    fn new(id: Uuid, params: external::IdentityMetadataCreateParams) -> Self {
        let now = Utc::now();
        Self {
            id,
            name: params.name,
            description: params.description,
            time_created: now,
            time_modified: now,
            time_deleted: None,
        }
    }
}

impl Into<external::IdentityMetadata> for IdentityMetadata {
    fn into(self) -> external::IdentityMetadata {
        external::IdentityMetadata {
            id: self.id,
            name: self.name,
            description: self.description,
            time_created: self.time_created,
            time_modified: self.time_modified,
        }
    }
}

impl From<external::IdentityMetadata> for IdentityMetadata {
    fn from(metadata: external::IdentityMetadata) -> Self {
        Self {
            id: metadata.id,
            name: metadata.name,
            description: metadata.description,
            time_created: metadata.time_created,
            time_modified: metadata.time_modified,
            time_deleted: None,
        }
    }
}

/// Serialization to DB.
impl SqlSerialize for IdentityMetadata {
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
        if time_deleted.is_some() {
            return Err(external::Error::internal_error(
                "model does not support objects that have been deleted",
            ));
        }
        Ok(IdentityMetadata {
            id: sql_row_value(value, "id")?,
            name: sql_row_value(value, "name")?,
            description: sql_row_value(value, "description")?,
            time_created: sql_row_value(value, "time_created")?,
            time_modified: sql_row_value(value, "time_modified")?,
            time_deleted: sql_row_value(value, "time_deleted")?,
        })
    }
}

/// Describes a project within the database.
#[derive(Queryable, Identifiable, Insertable, Debug)]
#[table_name = "project"]
pub struct Project {
    pub id: Uuid,
    pub name: external::Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl Project {
    /// Creates a new database Project object.
    pub fn new(params: external::ProjectCreateParams) -> Self {
        let id = Uuid::new_v4();
        let now = Utc::now();
        Self {
            id,
            name: params.identity.name,
            description: params.identity.description,
            time_created: now,
            time_modified: now,
            time_deleted: None,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }
}

impl Into<external::Project> for Project {
    fn into(self) -> external::Project {
        external::Project {
            identity: external::IdentityMetadata {
                id: self.id,
                name: self.name,
                description: self.description,
                time_created: self.time_created,
                time_modified: self.time_modified,
            },
        }
    }
}

/// Conversion from the internal API type.
impl From<external::Project> for Project {
    fn from(project: external::Project) -> Self {
        Self {
            id: project.identity.id,
            name: project.identity.name,
            description: project.identity.description,
            time_created: project.identity.time_created,
            time_modified: project.identity.time_modified,
            time_deleted: None,
        }
    }
}

/// Describes a set of updates for the [`Project`] model.
#[derive(AsChangeset)]
#[table_name = "project"]
pub struct ProjectUpdate {
    pub name: Option<external::Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<external::ProjectUpdateParams> for ProjectUpdate {
    fn from(params: external::ProjectUpdateParams) -> Self {
        Self {
            name: params.identity.name,
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

/// An Instance (VM).
#[derive(Queryable, Identifiable, Insertable, Debug)]
#[table_name = "instance"]
pub struct Instance {
    pub id: Uuid,
    pub name: external::Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// runtime state of the Instance
    pub instance_state: InstanceState,

    /// timestamp for this information
    pub time_state_updated: DateTime<Utc>,

    /// generation number for this state
    pub state_generation: Generation,

    /// which sled is running this Instance
    pub active_server_id: Uuid,

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
        let identity =
            IdentityMetadata::new(instance_id, params.identity.clone());

        Self {
            id: identity.id,
            name: identity.name,
            description: identity.description,
            time_created: identity.time_created,
            time_modified: identity.time_modified,
            time_deleted: identity.time_deleted,

            project_id,

            // TODO: Align these names pls
            instance_state: runtime.run_state,
            time_state_updated: runtime.time_updated,
            state_generation: runtime.gen,
            active_server_id: runtime.sled_uuid,

            ncpus: params.ncpus,
            memory: params.memory,
            hostname: params.hostname.clone(),
        }
    }

    // TODO: Here and for InstanceRuntimeState - if we are flattening
    // the versions stored in the DB, do we *need* the intermediate "model"
    // objects?
    //
    // Theoretically we could just jump straight to the view.

    // TODO: We could definitely derive this.
    // We could actually derive any "into subset" struct with
    // identically named fields.
    pub fn identity(&self) -> IdentityMetadata {
        IdentityMetadata {
            id: self.id,
            name: self.name.clone(),
            description: self.description.clone(),
            time_created: self.time_created,
            time_modified: self.time_modified,
            time_deleted: self.time_deleted,
        }
    }

    pub fn runtime(&self) -> InstanceRuntimeState {
        InstanceRuntimeState {
            run_state: self.instance_state,
            sled_uuid: self.active_server_id,
            gen: self.state_generation,
            time_updated: self.time_state_updated,
        }
    }
}

/// Conversion to the external API type.
impl Into<external::Instance> for Instance {
    fn into(self) -> external::Instance {
        external::Instance {
            identity: self.identity().into(),
            project_id: self.project_id,
            ncpus: self.ncpus,
            memory: self.memory,
            hostname: self.hostname.clone(),
            runtime: self.runtime().into(),
        }
    }
}

/// Runtime state of the Instance, including the actual running state and minimal
/// metadata
///
/// This state is owned by the sled agent running that Instance.
#[derive(Clone, Debug, AsChangeset)]
#[table_name = "instance"]
pub struct InstanceRuntimeState {
    /// runtime state of the Instance
    #[column_name = "instance_state"]
    pub run_state: InstanceState,
    /// which sled is running this Instance
    // TODO: should this be optional?
    #[column_name = "active_server_id"]
    pub sled_uuid: Uuid,
    /// generation number for this state
    #[column_name = "state_generation"]
    pub gen: Generation,
    /// timestamp for this information
    #[column_name = "time_state_updated"]
    pub time_updated: DateTime<Utc>,
}

/// Conversion to the external API type.
impl Into<external::InstanceRuntimeState> for InstanceRuntimeState {
    fn into(self) -> external::InstanceRuntimeState {
        external::InstanceRuntimeState {
            run_state: *self.run_state.state(),
            time_run_state_updated: self.time_updated,
        }
    }
}

/// Conversion from the internal API type.
impl From<internal::nexus::InstanceRuntimeState> for InstanceRuntimeState {
    fn from(state: internal::nexus::InstanceRuntimeState) -> Self {
        Self {
            run_state: InstanceState::new(state.run_state),
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
            run_state: *self.run_state.state(),
            sled_uuid: self.sled_uuid,
            gen: self.gen,
            time_updated: self.time_updated,
        }
    }
}

/// A wrapper around the external "InstanceState" object,
/// which may be stored to disk.
#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
pub struct InstanceState(external::InstanceState);

impl InstanceState {
    pub fn new(state: external::InstanceState) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::InstanceState {
        &self.0
    }
}

impl<DB> ToSql<sql_types::Text, DB> for InstanceState
where
    DB: Backend,
    String: ToSql<sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        (&self.0.label().to_string() as &String).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Text, DB> for InstanceState
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        let s = String::from_sql(bytes)?;
        let state = external::InstanceState::try_from(s.as_str())?;
        Ok(InstanceState::new(state))
    }
}

/// A Disk (network block device).
#[derive(Queryable, Identifiable, Insertable, Clone, Debug)]
#[table_name = "disk"]
pub struct Disk {
    // IdentityMetadata
    pub id: Uuid,
    pub name: external::Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    /// id for the project containing this Disk
    pub project_id: Uuid,

    // DiskRuntimeState
    /// runtime state of the Disk
    pub disk_state: String,
    pub attach_instance_id: Option<Uuid>,
    /// generation number for this state
    #[column_name = "state_generation"]
    pub gen: Generation,
    /// timestamp for this information
    #[column_name = "time_state_updated"]
    pub time_updated: DateTime<Utc>,

    /// size of the Disk
    #[column_name = "size_bytes"]
    pub size: ByteCount,
    /// id for the snapshot from which this Disk was created (None means a blank
    /// disk)
    #[column_name = "origin_snapshot"]
    pub create_snapshot_id: Option<Uuid>,
}

impl Disk {
    pub fn new(
        disk_id: Uuid,
        project_id: Uuid,
        params: external::DiskCreateParams,
        runtime_initial: DiskRuntimeState,
    ) -> Self {
        let identity = IdentityMetadata::new(disk_id, params.identity);
        Self {
            id: identity.id,
            name: identity.name,
            description: identity.description,
            time_created: identity.time_created,
            time_modified: identity.time_modified,
            time_deleted: identity.time_deleted,

            project_id,

            disk_state: runtime_initial.disk_state,
            attach_instance_id: runtime_initial.attach_instance_id,
            gen: runtime_initial.gen,
            time_updated: runtime_initial.time_updated,

            size: params.size,
            create_snapshot_id: params.snapshot_id,
        }
    }

    pub fn identity(&self) -> IdentityMetadata {
        IdentityMetadata {
            id: self.id,
            name: self.name.clone(),
            description: self.description.clone(),
            time_created: self.time_created,
            time_modified: self.time_modified,
            time_deleted: self.time_deleted,
        }
    }

    pub fn state(&self) -> DiskState {
        // TODO: If we could store disk state in-line, we could avoid the
        // unwrap. Would prefer to parse it as such.
        //
        // TODO: also impl'd for DiskRuntimeState
        DiskState::new(
            external::DiskState::try_from((
                self.disk_state.as_str(),
                self.attach_instance_id,
            ))
            .unwrap(),
        )
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        DiskRuntimeState {
            disk_state: self.disk_state.clone(),
            attach_instance_id: self.attach_instance_id,
            gen: self.gen,
            time_updated: self.time_updated,
        }
    }

    pub fn attachment(&self) -> Option<external::DiskAttachment> {
        if let Some(instance_id) = self.attach_instance_id {
            Some(
                external::DiskAttachment {
                    instance_id,
                    disk_id: self.id,
                    disk_name: self.name.clone(),
                    disk_state: self.state().into(),
                }
            )
        } else {
            None
        }
    }
}

/// Conversion to the external API type.
impl Into<external::Disk> for Disk {
    fn into(self) -> external::Disk {
        let device_path = format!("/mnt/{}", self.name.as_str());
        external::Disk {
            identity: self.identity().into(),
            project_id: self.project_id,
            snapshot_id: self.create_snapshot_id,
            size: self.size,
            state: self.state().into(),
            device_path,
        }
    }
}

#[derive(AsChangeset, Clone, Debug)]
#[table_name = "disk"]
// When "attach_instance_id" is set to None, we'd like to
// clear it from the DB, rather than ignore the update.
#[changeset_options(treat_none_as_null = "true")]
pub struct DiskRuntimeState {
    /// runtime state of the Disk
    pub disk_state: String,
    pub attach_instance_id: Option<Uuid>,
    /// generation number for this state
    #[column_name = "state_generation"]
    pub gen: Generation,
    /// timestamp for this information
    #[column_name = "time_state_updated"]
    pub time_updated: DateTime<Utc>,
}

impl DiskRuntimeState {
    pub fn new() -> Self {
        Self {
            disk_state: external::DiskState::Creating.label().to_string(),
            attach_instance_id: None,
            gen: Generation::new(),
            time_updated: Utc::now(),
        }
    }

    pub fn detach(self) -> Self {
        Self {
            disk_state: external::DiskState::Detached.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next(),
            time_updated: Utc::now(),
        }
    }

    pub fn state(&self) -> DiskState {
        // TODO: If we could store disk state in-line, we could avoid the
        // unwrap. Would prefer to parse it as such.
        DiskState::new(
            external::DiskState::try_from((
                self.disk_state.as_str(),
                self.attach_instance_id,
            ))
            .unwrap(),
        )
    }
}

/// Conversion from the internal API type.
impl From<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn from(runtime: internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: runtime.disk_state.label().to_string(),
            attach_instance_id: runtime
                .disk_state
                .attached_instance_id()
                .map(|id| *id),
            gen: runtime.gen,
            time_updated: runtime.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn into(self) -> internal::nexus::DiskRuntimeState {
        internal::nexus::DiskRuntimeState {
            disk_state: self.state().into(),
            gen: self.gen,
            time_updated: self.time_updated,
        }
    }
}

// TODO: What to do with this type???

#[derive(Clone, Debug, AsExpression)]
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

pub type DiskAttachment = external::DiskAttachment;

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Queryable, Identifiable, Insertable, Debug, Clone)]
#[table_name = "metricproducer"]
pub struct ProducerEndpoint {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub ip: ipnetwork::IpNetwork,
    pub port: i32,
    pub interval: f64,
    pub base_route: String,
}

impl ProducerEndpoint {
    pub fn new(endpoint: &internal::nexus::ProducerEndpoint) -> Self {
        let now = Utc::now();
        Self {
            id: endpoint.id,
            time_created: now,
            time_modified: now,
            ip: endpoint.address.ip().into(),
            port: endpoint.address.port().into(),
            base_route: endpoint.base_route.clone(),
            interval: endpoint.interval.as_secs_f64(),
        }
    }

    /// Return the route that can be used to request metric data.
    pub fn collection_route(&self) -> String {
        format!("{}/{}", &self.base_route, &self.id)
    }
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Queryable, Identifiable, Insertable, Debug, Clone, Copy)]
#[table_name = "oximeter"]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub id: Uuid,
    /// When this resource was created.
    pub time_created: DateTime<Utc>,
    /// When this resource was last modified.
    pub time_modified: DateTime<Utc>,
    /// The address on which this oximeter instance listens for requests
    pub ip: ipnetwork::IpNetwork,
    pub port: i32,
}

impl OximeterInfo {
    pub fn new(info: &internal::nexus::OximeterInfo) -> Self {
        let now = Utc::now();
        Self {
            id: info.collector_id,
            time_created: now,
            time_modified: now,
            ip: info.address.ip().into(),
            port: info.address.port().into(),
        }
    }
}

/// An assignment of an Oximeter instance to a metric producer for collection.
#[derive(Queryable, Insertable, Debug, Clone, Copy)]
#[table_name = "oximeterassignment"]
pub struct OximeterAssignment {
    pub oximeter_id: Uuid,
    pub producer_id: Uuid,
    pub time_created: DateTime<Utc>,
}

impl OximeterAssignment {
    pub fn new(oximeter_id: Uuid, producer_id: Uuid) -> Self {
        Self {
            oximeter_id,
            producer_id,
            time_created: Utc::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Vpc {
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub dns_name: external::Name,
}

impl Vpc {
    pub fn new(
        vpc_id: Uuid,
        project_id: Uuid,
        params: external::VpcCreateParams,
    ) -> Self {
        Self {
            identity: IdentityMetadata::new(vpc_id, params.identity),
            project_id,
            dns_name: params.dns_name,
        }
    }
}

impl Into<external::Vpc> for Vpc {
    fn into(self) -> external::Vpc {
        external::Vpc {
            identity: self.identity.into(),
            project_id: self.project_id,
            dns_name: self.dns_name,
            // VPC subnets are accessed through a separate row lookup.
            vpc_subnets: vec![],
        }
    }
}

impl TryFrom<&tokio_postgres::Row> for Vpc {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            dns_name: sql_row_value(value, "dns_name")?,
        })
    }
}

impl SqlSerialize for Vpc {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("project_id", &self.project_id);
        output.set("dns_name", &self.dns_name);
    }
}

#[derive(Clone, Debug)]
pub struct VpcSubnet {
    pub identity: IdentityMetadata,
    pub vpc_id: Uuid,
    pub ipv4_block: Option<external::Ipv4Net>,
    pub ipv6_block: Option<external::Ipv6Net>,
}

impl TryFrom<&tokio_postgres::Row> for VpcSubnet {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: IdentityMetadata::try_from(value)?,
            vpc_id: sql_row_value(value, "vpc_id")?,
            ipv4_block: sql_row_value(value, "ipv4_block")?,
            ipv6_block: sql_row_value(value, "ipv6_block")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct NetworkInterface {
    pub identity: IdentityMetadata,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: external::MacAddr,
    pub ip: IpAddr,
}

impl TryFrom<&tokio_postgres::Row> for NetworkInterface {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: IdentityMetadata::try_from(value)?,
            vpc_id: sql_row_value(value, "vpc_id")?,
            subnet_id: sql_row_value(value, "subnet_id")?,
            mac: sql_row_value(value, "mac")?,
            ip: sql_row_value(value, "ip")?,
        })
    }
}
