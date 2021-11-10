//! Structures stored to the database.

use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::{Asset, Resource};
use crate::db::schema::{
    console_session, disk, instance, metric_producer, network_interface,
    organization, oximeter, project, rack, sled, vpc, vpc_router, vpc_subnet,
};
use chrono::{DateTime, Utc};
use db_macros::{Asset, Resource};
use diesel::backend::{Backend, BinaryRawValue, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, ToSql};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use omicron_common::api::internal;
use ref_cast::RefCast;
use schemars::JsonSchema;
use serde::Deserialize;
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
    Deserialize,
    JsonSchema,
)]
#[sql_type = "sql_types::Text"]
#[serde(transparent)]
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
        String::from_sql(bytes)?.parse().map(Name).map_err(|e| e.into())
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
#[repr(transparent)]
pub struct Generation(pub external::Generation);

NewtypeFrom! { () pub struct Generation(external::Generation); }
NewtypeDeref! { () pub struct Generation(external::Generation); }

impl Generation {
    pub fn new() -> Self {
        Self(external::Generation::new())
    }
}

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
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[table_name = "rack"]
pub struct Rack {
    #[diesel(embed)]
    pub identity: RackIdentity,
}

impl Into<external::Rack> for Rack {
    fn into(self) -> external::Rack {
        external::Rack { identity: self.identity() }
    }
}

/// Database representation of a Sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[table_name = "sled"]
pub struct Sled {
    #[diesel(embed)]
    identity: SledIdentity,

    // ServiceAddress (Sled Agent).
    pub ip: ipnetwork::IpNetwork,
    pub port: i32,
}

impl Sled {
    pub fn new(id: Uuid, addr: SocketAddr) -> Self {
        Self {
            identity: SledIdentity::new(id),
            ip: addr.ip().into(),
            port: addr.port().into(),
        }
    }

    pub fn address(&self) -> SocketAddr {
        // TODO: avoid this unwrap
        SocketAddr::new(self.ip.ip(), u16::try_from(self.port).unwrap())
    }
}

impl Into<external::Sled> for Sled {
    fn into(self) -> external::Sled {
        let service_address = self.address();
        external::Sled { identity: self.identity(), service_address }
    }
}

/// Describes an organization within the database.
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[table_name = "organization"]
pub struct Organization {
    #[diesel(embed)]
    identity: OrganizationIdentity,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl Organization {
    /// Creates a new database Organization object.
    pub fn new(params: external::OrganizationCreateParams) -> Self {
        let id = Uuid::new_v4();
        Self {
            identity: OrganizationIdentity::new(id, params.identity),
            rcgen: Generation::new(),
        }
    }
}

impl DatastoreCollection<Project> for Organization {
    type CollectionId = Uuid;
    type GenerationNumberColumn = organization::dsl::rcgen;
    type CollectionTimeDeletedColumn = organization::dsl::time_deleted;
    type CollectionIdColumn = project::dsl::organization_id;
}

impl Into<external::Organization> for Organization {
    fn into(self) -> external::Organization {
        external::Organization { identity: self.identity() }
    }
}

/// Describes a set of updates for the [`Organization`] model.
#[derive(AsChangeset)]
#[table_name = "organization"]
pub struct OrganizationUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<external::OrganizationUpdateParams> for OrganizationUpdate {
    fn from(params: external::OrganizationUpdateParams) -> Self {
        Self {
            name: params.identity.name.map(|n| n.into()),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

/// Describes a project within the database.
#[derive(Selectable, Queryable, Insertable, Debug, Resource)]
#[table_name = "project"]
pub struct Project {
    #[diesel(embed)]
    identity: ProjectIdentity,

    pub organization_id: Uuid,
}

impl Project {
    /// Creates a new database Project object.
    pub fn new(
        organization_id: Uuid,
        params: external::ProjectCreateParams,
    ) -> Self {
        Self {
            identity: ProjectIdentity::new(Uuid::new_v4(), params.identity),
            organization_id: organization_id,
        }
    }
}

impl Into<external::Project> for Project {
    fn into(self) -> external::Project {
        external::Project {
            identity: self.identity(),
            organization_id: self.organization_id,
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
#[derive(Queryable, Insertable, Debug, Selectable, Resource)]
#[table_name = "instance"]
pub struct Instance {
    #[diesel(embed)]
    identity: InstanceIdentity,

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
            InstanceIdentity::new(instance_id, params.identity.clone());
        Self { identity, project_id, runtime_state: runtime }
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
#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[table_name = "disk"]
pub struct Disk {
    #[diesel(embed)]
    identity: DiskIdentity,

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
        let identity = DiskIdentity::new(disk_id, params.identity);
        Self {
            identity,
            project_id,
            runtime_state: runtime_initial,
            size: params.size.into(),
            create_snapshot_id: params.snapshot_id,
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
                disk_id: self.id(),
                disk_name: self.name().clone(),
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
        let device_path = format!("/mnt/{}", self.name().as_str());
        external::Disk {
            identity: self.identity(),
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
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[table_name = "metric_producer"]
pub struct ProducerEndpoint {
    #[diesel(embed)]
    identity: ProducerEndpointIdentity,

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
        Self {
            identity: ProducerEndpointIdentity::new(endpoint.id),
            ip: endpoint.address.ip().into(),
            port: endpoint.address.port().into(),
            base_route: endpoint.base_route.clone(),
            interval: endpoint.interval.as_secs_f64(),
            oximeter_id,
        }
    }

    /// Return the route that can be used to request metric data.
    pub fn collection_route(&self) -> String {
        format!("{}/{}", &self.base_route, self.id())
    }
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Queryable, Insertable, Debug, Clone, Copy)]
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

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[table_name = "vpc"]
pub struct Vpc {
    #[diesel(embed)]
    identity: VpcIdentity,

    pub project_id: Uuid,
    pub system_router_id: Uuid,
    pub dns_name: Name,
}

impl Vpc {
    pub fn new(
        vpc_id: Uuid,
        project_id: Uuid,
        system_router_id: Uuid,
        params: external::VpcCreateParams,
    ) -> Self {
        let identity = VpcIdentity::new(vpc_id, params.identity);
        Self {
            identity,
            project_id,
            system_router_id,
            dns_name: params.dns_name.into(),
        }
    }
}

impl Into<external::Vpc> for Vpc {
    fn into(self) -> external::Vpc {
        external::Vpc {
            identity: self.identity(),
            project_id: self.project_id,
            system_router_id: self.system_router_id,
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

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[table_name = "vpc_subnet"]
pub struct VpcSubnet {
    #[diesel(embed)]
    identity: VpcSubnetIdentity,

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
        let identity = VpcSubnetIdentity::new(subnet_id, params.identity);
        Self {
            identity,
            vpc_id,
            ipv4_block: params.ipv4_block.map(Ipv4Net),
            ipv6_block: params.ipv6_block.map(Ipv6Net),
        }
    }
}

impl Into<external::VpcSubnet> for VpcSubnet {
    fn into(self) -> external::VpcSubnet {
        external::VpcSubnet {
            identity: self.identity(),
            vpc_id: self.vpc_id,
            ipv4_block: self.ipv4_block.map(|ip| ip.into()),
            ipv6_block: self.ipv6_block.map(|ip| ip.into()),
        }
    }
}

#[derive(AsChangeset)]
#[table_name = "vpc_subnet"]
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

/// This macro implements serialization and deserialization of an enum type
/// from our database into our model types.
/// See VpcRouterKindEnum and VpcRouterKind for a sample usage
macro_rules! impl_enum_type {
    (
        $(#[$enum_meta:meta])*
        pub struct $diesel_type:ident;

        $(#[$model_meta:meta])*
        pub struct $model_type:ident(pub $ext_type:ty);
        $($enum_item:ident => $sql_value:literal)+
    ) => {

        $(#[$enum_meta])*
        pub struct $diesel_type;

        $(#[$model_meta])*
        pub struct $model_type(pub $ext_type);

        impl $model_type {
            pub fn new(state: $ext_type) -> Self {
                Self(state)
            }

            pub fn state(&self) -> &$ext_type {
                &self.0
            }
        }

        impl<DB> ToSql<$diesel_type, DB> for $model_type
        where
            DB: Backend,
        {
            fn to_sql<W: std::io::Write>(
                &self,
                out: &mut serialize::Output<W, DB>,
            ) -> serialize::Result {
                match *self.state() {
                    $(
                    <$ext_type>::$enum_item => {
                        out.write_all($sql_value)?
                    }
                    )*
                }
                Ok(IsNull::No)
            }
        }

        impl<DB> FromSql<$diesel_type, DB> for $model_type
        where
            DB: Backend + for<'a> BinaryRawValue<'a>,
        {
            fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
                match DB::as_bytes(bytes) {
                    $(
                    $sql_value => {
                        Ok($model_type::new(<$ext_type>::$enum_item))
                    }
                    )*
                    _ => {
                        Err(concat!("Unrecognized enum variant for ",
                                stringify!{$model_type})
                            .into())
                    }
                }
            }
        }
    }
}

impl_enum_type!(
    /// Note that the type_name _must_ be all lowercase or it'll fail.
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_router_kind", type_schema = "public")]
    pub struct VpcRouterKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcRouterKindEnum"]
    pub struct VpcRouterKind(pub external::VpcRouterKind);

    // Enum values
    System => b"system"
    Custom => b"custom"
);

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[table_name = "vpc_router"]
pub struct VpcRouter {
    #[diesel(embed)]
    identity: VpcRouterIdentity,

    pub vpc_id: Uuid,
    pub kind: VpcRouterKind,
}

impl VpcRouter {
    pub fn new(
        router_id: Uuid,
        vpc_id: Uuid,
        kind: external::VpcRouterKind,
        params: external::VpcRouterCreateParams,
    ) -> Self {
        let identity = VpcRouterIdentity::new(router_id, params.identity);
        Self { identity, vpc_id, kind: VpcRouterKind::new(kind) }
    }
}

impl Into<external::VpcRouter> for VpcRouter {
    fn into(self) -> external::VpcRouter {
        external::VpcRouter {
            identity: self.identity(),
            vpc_id: self.vpc_id,
            kind: *self.kind.state(),
        }
    }
}

#[derive(AsChangeset)]
#[table_name = "vpc_router"]
pub struct VpcRouterUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<external::VpcRouterUpdateParams> for VpcRouterUpdate {
    fn from(params: external::VpcRouterUpdateParams) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_status", type_schema = "public")]
    pub struct VpcFirewallStatusEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallStatusEnum"]
    pub struct VpcFirewallStatus(pub external::VpcFirewallStatus);

    Disabled => b"disabled"
    Enabled => b"enabled"
);

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_direction", type_schema = "public")]
    pub struct VpcFirewallDirectionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallDirectionEnum"]
    pub struct VpcFirewallDirection(pub external::VpcFirewallDirection);

    Incoming => b"incoming"
    Outgoing => b"outgoing"
);

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_action", type_schema = "public")]
    pub struct VpcFirewallActionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallActionEnum"]
    pub struct VpcFirewallAction(pub external::VpcFirewallAction);

    Allow => b"allow"
    Drop => b"drop"
);

#[derive(Queryable, Insertable, Clone, Debug, Resource)]
#[table_name = "network_interface"]
pub struct NetworkInterface {
    #[diesel(embed)]
    pub identity: NetworkInterfaceIdentity,

    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    pub ip: ipnetwork::IpNetwork,
}

// TODO: `struct SessionToken(String)` for session token

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[table_name = "console_session"]
pub struct ConsoleSession {
    pub token: String,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
    pub user_id: Uuid,
}

impl ConsoleSession {
    pub fn new(token: String, user_id: Uuid) -> Self {
        let now = Utc::now();
        Self { token, user_id, time_last_used: now, time_created: now }
    }
}
