//! Structures stored to the database.

use super::schema::{
    disk, instance, metricproducer, networkinterface, oximeter, project, sled,
    vpc, vpcsubnet,
};
use chrono::{DateTime, Utc};
use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use omicron_common::api::internal;
use ref_cast::RefCast;
use std::convert::TryFrom;
use std::net::SocketAddr;
use uuid::Uuid;

// TODO: Break up types into multiple files

/// Newtype wrapper around [external::Name].
#[derive(
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    RefCast,
)]
#[sql_type = "sql_types::Text"]
#[repr(transparent)]
pub struct Name(pub external::Name);

NewtypeFrom! { () pub struct Name(external::Name); }
NewtypeDeref! { () pub struct Name(external::Name); }

impl<DB> ToSql<sql_types::Text, DB> for Name
where
    DB: Backend,
    str: ToSql<sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        self.as_str().to_sql(out)
    }
}

// Deserialize the "Name" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for Name
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        external::Name::try_from(String::from_sql(bytes)?)
            .map(Name)
            .map_err(|e| e.into())
    }
}

#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::BigInt"]
pub struct ByteCount(pub external::ByteCount);

NewtypeFrom! { () pub struct ByteCount(external::ByteCount); }
NewtypeDeref! { () pub struct ByteCount(external::ByteCount); }

impl<DB> ToSql<sql_types::BigInt, DB> for ByteCount
where
    DB: Backend,
    i64: ToSql<sql_types::BigInt, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        i64::from(&self.0).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for ByteCount
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        external::ByteCount::try_from(i64::from_sql(bytes)?)
            .map(ByteCount)
            .map_err(|e| e.into())
    }
}

#[derive(
    Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, AsExpression, FromSqlRow,
)]
#[sql_type = "sql_types::BigInt"]
pub struct Generation(pub external::Generation);

NewtypeFrom! { () pub struct Generation(external::Generation); }
NewtypeDeref! { () pub struct Generation(external::Generation); }

impl<DB> ToSql<sql_types::BigInt, DB> for Generation
where
    DB: Backend,
    i64: ToSql<sql_types::BigInt, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        i64::from(&self.0).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for Generation
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        external::Generation::try_from(i64::from_sql(bytes)?)
            .map(Generation)
            .map_err(|e| e.into())
    }
}

#[derive(Copy, Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::BigInt"]
pub struct InstanceCpuCount(pub external::InstanceCpuCount);

NewtypeFrom! { () pub struct InstanceCpuCount(external::InstanceCpuCount); }
NewtypeDeref! { () pub struct InstanceCpuCount(external::InstanceCpuCount); }

impl<DB> ToSql<sql_types::BigInt, DB> for InstanceCpuCount
where
    DB: Backend,
    i64: ToSql<sql_types::BigInt, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        i64::from(&self.0).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for InstanceCpuCount
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        external::InstanceCpuCount::try_from(i64::from_sql(bytes)?)
            .map(InstanceCpuCount)
            .map_err(|e| e.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Inet"]
pub struct Ipv4Net(pub external::Ipv4Net);

NewtypeFrom! { () pub struct Ipv4Net(external::Ipv4Net); }
NewtypeDeref! { () pub struct Ipv4Net(external::Ipv4Net); }

impl<DB> ToSql<sql_types::Inet, DB> for Ipv4Net
where
    DB: Backend,
    IpNetwork: ToSql<sql_types::Inet, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        IpNetwork::V4(*self.0).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Inet, DB> for Ipv4Net
where
    DB: Backend,
    IpNetwork: FromSql<sql_types::Inet, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        let inet = IpNetwork::from_sql(bytes)?;
        match inet {
            IpNetwork::V4(net) => Ok(Ipv4Net(external::Ipv4Net(net))),
            _ => Err("Expected IPV4".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Inet"]
pub struct Ipv6Net(pub external::Ipv6Net);

NewtypeFrom! { () pub struct Ipv6Net(external::Ipv6Net); }
NewtypeDeref! { () pub struct Ipv6Net(external::Ipv6Net); }

impl<DB> ToSql<sql_types::Inet, DB> for Ipv6Net
where
    DB: Backend,
    IpNetwork: ToSql<sql_types::Inet, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        IpNetwork::V6(self.0 .0).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Inet, DB> for Ipv6Net
where
    DB: Backend,
    IpNetwork: FromSql<sql_types::Inet, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        let inet = IpNetwork::from_sql(bytes)?;
        match inet {
            IpNetwork::V6(net) => Ok(Ipv6Net(external::Ipv6Net(net))),
            _ => Err("Expected IPV6".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
pub struct MacAddr(pub external::MacAddr);

NewtypeFrom! { () pub struct MacAddr(external::MacAddr); }
NewtypeDeref! { () pub struct MacAddr(external::MacAddr); }

impl<DB> ToSql<sql_types::Text, DB> for MacAddr
where
    DB: Backend,
    String: ToSql<sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        self.0.to_string().to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Text, DB> for MacAddr
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        external::MacAddr::try_from(String::from_sql(bytes)?)
            .map(MacAddr)
            .map_err(|e| e.into())
    }
}

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

/// Database representation of a Sled.
#[derive(Queryable, Identifiable, Insertable, Debug, Clone)]
#[table_name = "sled"]
pub struct Sled {
    // IdentityMetadata
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    // ServiceAddress (Sled Agent).
    pub ip: ipnetwork::IpNetwork,
    pub port: i32,
}

impl Sled {
    pub fn new(
        id: Uuid,
        addr: SocketAddr,
        params: external::IdentityMetadataCreateParams,
    ) -> Self {
        let identity = IdentityMetadata::new(id, params);
        Self {
            id,
            time_created: identity.time_created,
            time_modified: identity.time_modified,
            time_deleted: identity.time_deleted,
            ip: addr.ip().into(),
            port: addr.port().into(),
        }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn address(&self) -> SocketAddr {
        // TODO: avoid this unwrap
        SocketAddr::new(self.ip.ip(), u16::try_from(self.port).unwrap())
    }
}

impl Into<external::Sled> for Sled {
    fn into(self) -> external::Sled {
        let service_address = self.address();
        external::Sled {
            identity: external::IdentityMetadata {
                id: self.id,
                name: external::Name::try_from("sled").unwrap(),
                description: "sled description".to_string(),
                time_created: self.time_created,
                time_modified: self.time_modified,
            },
            service_address,
        }
    }
}

// TODO: As Diesel needs to flatten things out, this structure
// may become unused.
#[derive(Clone, Debug)]
pub struct IdentityMetadata {
    pub id: Uuid,
    pub name: Name,
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
            name: Name(params.name),
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
            name: self.name.0,
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
            name: Name(metadata.name),
            description: metadata.description,
            time_created: metadata.time_created,
            time_modified: metadata.time_modified,
            time_deleted: None,
        }
    }
}

/// Describes a project within the database.
#[derive(Queryable, Identifiable, Insertable, Debug)]
#[table_name = "project"]
pub struct Project {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl Project {
    /// Creates a new database Project object.
    pub fn new(params: external::ProjectCreateParams) -> Self {
        let id = Uuid::new_v4();
        let identity = IdentityMetadata::new(id, params.identity);
        Self {
            id: identity.id,
            name: identity.name,
            description: identity.description,
            time_created: identity.time_created,
            time_modified: identity.time_modified,
            time_deleted: identity.time_deleted,
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
                name: self.name.0,
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
            name: Name(project.identity.name),
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
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<external::ProjectUpdateParams> for ProjectUpdate {
    fn from(params: external::ProjectUpdateParams) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

/// An Instance (VM).
#[derive(Queryable, Identifiable, Insertable, Debug, Selectable)]
#[table_name = "instance"]
pub struct Instance {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// runtime state of the Instance
    #[diesel(embed)]
    pub runtime_state: InstanceRuntimeState,
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

            runtime_state: runtime,
        }
    }

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

    pub fn runtime(&self) -> &InstanceRuntimeState {
        &self.runtime_state
    }
}

/// Conversion to the external API type.
impl Into<external::Instance> for Instance {
    fn into(self) -> external::Instance {
        external::Instance {
            identity: self.identity().into(),
            project_id: self.project_id,
            ncpus: self.runtime().ncpus.into(),
            memory: self.runtime().memory.into(),
            hostname: self.runtime().hostname.clone(),
            runtime: self.runtime().clone().into(),
        }
    }
}

/// Runtime state of the Instance, including the actual running state and minimal
/// metadata
///
/// This state is owned by the sled agent running that Instance.
#[derive(Clone, Debug, AsChangeset, Selectable, Insertable, Queryable)]
#[table_name = "instance"]
pub struct InstanceRuntimeState {
    /// runtime state of the Instance
    #[column_name = "state"]
    pub state: InstanceState,
    /// timestamp for this information
    // TODO: Is this redundant with "time_modified"?
    #[column_name = "time_state_updated"]
    pub time_updated: DateTime<Utc>,
    /// generation number for this state
    #[column_name = "state_generation"]
    pub gen: Generation,
    /// which sled is running this Instance
    // TODO: should this be optional?
    #[column_name = "active_server_id"]
    pub sled_uuid: Uuid,
    #[column_name = "ncpus"]
    pub ncpus: InstanceCpuCount,
    #[column_name = "memory"]
    pub memory: ByteCount,
    // TODO-cleanup: Different type?
    #[column_name = "hostname"]
    pub hostname: String,
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
            sled_uuid: state.sled_uuid,
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
            sled_uuid: self.sled_uuid,
            ncpus: self.ncpus.into(),
            memory: self.memory.into(),
            hostname: self.hostname,
            gen: self.gen.into(),
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
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        let s = String::from_sql(bytes)?;
        let state = external::InstanceState::try_from(s.as_str())?;
        Ok(InstanceState::new(state))
    }
}

/// A Disk (network block device).
#[derive(Queryable, Identifiable, Insertable, Clone, Debug, Selectable)]
#[table_name = "disk"]
pub struct Disk {
    // IdentityMetadata
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    /// id for the project containing this Disk
    pub project_id: Uuid,

    /// runtime state of the Disk
    #[diesel(embed)]
    pub runtime_state: DiskRuntimeState,

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

            runtime_state: DiskRuntimeState {
                disk_state: runtime_initial.disk_state,
                attach_instance_id: runtime_initial.attach_instance_id,
                gen: runtime_initial.gen,
                time_updated: runtime_initial.time_updated,
            },

            size: params.size.into(),
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
        self.runtime_state.state()
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        self.runtime_state.clone()
    }

    pub fn attachment(&self) -> Option<DiskAttachment> {
        if let Some(instance_id) = self.runtime_state.attach_instance_id {
            Some(DiskAttachment {
                instance_id,
                disk_id: self.id,
                disk_name: self.name.clone(),
                disk_state: self.state(),
            })
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
            size: self.size.into(),
            state: self.state().into(),
            device_path,
        }
    }
}

#[derive(AsChangeset, Clone, Debug, Queryable, Insertable, Selectable)]
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
            gen: external::Generation::new().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn detach(self) -> Self {
        Self {
            disk_state: external::DiskState::Detached.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
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
            gen: runtime.gen.into(),
            time_updated: runtime.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn into(self) -> internal::nexus::DiskRuntimeState {
        internal::nexus::DiskRuntimeState {
            disk_state: self.state().into(),
            gen: self.gen.into(),
            time_updated: self.time_updated,
        }
    }
}

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

/// Type which describes the attachment status of a disk.
///
/// This happens to be the same as the type in the external API,
/// but it is not required to be.
#[derive(Clone, Debug)]
pub struct DiskAttachment {
    pub instance_id: Uuid,
    pub disk_id: Uuid,
    pub disk_name: Name,
    pub disk_state: DiskState,
}

impl Into<external::DiskAttachment> for DiskAttachment {
    fn into(self) -> external::DiskAttachment {
        external::DiskAttachment {
            instance_id: self.instance_id,
            disk_id: self.disk_id,
            disk_name: self.disk_name.0,
            disk_state: self.disk_state.0,
        }
    }
}

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Queryable, Identifiable, Insertable, Debug, Clone, Selectable)]
#[table_name = "metricproducer"]
pub struct ProducerEndpoint {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub ip: ipnetwork::IpNetwork,
    pub port: i32,
    pub interval: f64,
    pub base_route: String,
    pub oximeter_id: Uuid,
}

impl ProducerEndpoint {
    /// Create a new endpoint, with the data announced by the producer and a chosen Oximeter
    /// instance to act as its collector.
    pub fn new(
        endpoint: &internal::nexus::ProducerEndpoint,
        oximeter_id: Uuid,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: endpoint.id,
            time_created: now,
            time_modified: now,
            ip: endpoint.address.ip().into(),
            port: endpoint.address.port().into(),
            base_route: endpoint.base_route.clone(),
            interval: endpoint.interval.as_secs_f64(),
            oximeter_id,
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

#[derive(Queryable, Identifiable, Insertable, Clone, Debug)]
#[table_name = "vpc"]
pub struct Vpc {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    pub project_id: Uuid,
    pub dns_name: Name,
}

impl Vpc {
    pub fn new(
        vpc_id: Uuid,
        project_id: Uuid,
        params: external::VpcCreateParams,
    ) -> Self {
        let identity = IdentityMetadata::new(vpc_id, params.identity);
        Self {
            id: identity.id,
            name: identity.name,
            description: identity.description,
            time_created: identity.time_created,
            time_modified: identity.time_modified,
            time_deleted: identity.time_deleted,

            project_id,
            dns_name: Name(params.dns_name),
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
}

impl Into<external::Vpc> for Vpc {
    fn into(self) -> external::Vpc {
        external::Vpc {
            identity: self.identity().into(),
            project_id: self.project_id,
            dns_name: self.dns_name.0,
        }
    }
}

#[derive(AsChangeset)]
#[table_name = "vpc"]
pub struct VpcUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub dns_name: Option<Name>,
}

impl From<external::VpcUpdateParams> for VpcUpdate {
    fn from(params: external::VpcUpdateParams) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
            dns_name: params.dns_name.map(Name),
        }
    }
}

#[derive(Queryable, Identifiable, Insertable, Clone, Debug)]
#[table_name = "vpcsubnet"]
pub struct VpcSubnet {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    pub vpc_id: Uuid,
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

impl VpcSubnet {
    pub fn new(
        subnet_id: Uuid,
        vpc_id: Uuid,
        params: external::VpcSubnetCreateParams,
    ) -> Self {
        let identity = IdentityMetadata::new(subnet_id, params.identity);
        Self {
            id: identity.id,
            name: identity.name,
            description: identity.description,
            time_created: identity.time_created,
            time_modified: identity.time_modified,
            time_deleted: identity.time_deleted,

            vpc_id,

            ipv4_block: params.ipv4_block.map(Ipv4Net),
            ipv6_block: params.ipv6_block.map(Ipv6Net),
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
}

impl Into<external::VpcSubnet> for VpcSubnet {
    fn into(self) -> external::VpcSubnet {
        external::VpcSubnet {
            identity: self.identity().into(),
            vpc_id: self.vpc_id,
            ipv4_block: self.ipv4_block.map(|ip| ip.into()),
            ipv6_block: self.ipv6_block.map(|ip| ip.into()),
        }
    }
}

#[derive(AsChangeset)]
#[table_name = "vpcsubnet"]
pub struct VpcSubnetUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

impl From<external::VpcSubnetUpdateParams> for VpcSubnetUpdate {
    fn from(params: external::VpcSubnetUpdateParams) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
            ipv4_block: params.ipv4_block.map(Ipv4Net),
            ipv6_block: params.ipv6_block.map(Ipv6Net),
        }
    }
}

#[derive(Queryable, Identifiable, Insertable, Clone, Debug)]
#[table_name = "networkinterface"]
pub struct NetworkInterface {
    pub id: Uuid,
    pub name: Name,
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    pub ip: ipnetwork::IpNetwork,
}

impl NetworkInterface {
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
}
