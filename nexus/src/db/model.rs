// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures stored to the database.

use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::{Asset, Resource};
use crate::db::schema::{
    console_session, dataset, disk, instance, metric_producer,
    network_interface, organization, oximeter, project, rack, region,
    role_assignment_builtin, role_builtin, router_route, sled,
    update_available_artifact, user_builtin, vpc, vpc_firewall_rule,
    vpc_router, vpc_subnet, zpool,
};
use crate::defaults;
use crate::external_api::params;
use crate::internal_api;
use chrono::{DateTime, Utc};
use db_macros::{Asset, Resource};
use diesel::backend::{Backend, BinaryRawValue, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, ToSql};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use omicron_common::api::internal;
use omicron_sled_agent::common::instance::PROPOLIS_PORT;
use parse_display::Display;
use rand::{rngs::StdRng, SeedableRng};
use ref_cast::RefCast;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::net::IpAddr;
use std::net::SocketAddr;
use uuid::Uuid;

// TODO: Break up types into multiple files

/// This macro implements serialization and deserialization of an enum type
/// from our database into our model types.
/// See [`VpcRouterKindEnum`] and [`VpcRouterKind`] for a sample usage
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

        impl<DB> ToSql<$diesel_type, DB> for $model_type
        where
            DB: Backend,
        {
            fn to_sql<W: std::io::Write>(
                &self,
                out: &mut serialize::Output<W, DB>,
            ) -> serialize::Result {
                match self.0 {
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
                        Ok($model_type(<$ext_type>::$enum_item))
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

/// Newtype wrapper around [external::Name].
#[derive(
    Clone,
    Debug,
    Display,
    AsExpression,
    FromSqlRow,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    RefCast,
    JsonSchema,
    Serialize,
    Deserialize,
)]
#[sql_type = "sql_types::Text"]
#[serde(transparent)]
#[repr(transparent)]
#[display("{0}")]
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

#[derive(
    Copy,
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    PartialEq,
)]
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
        i64::from(self.0).to_sql(out)
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
    Copy,
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
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

/// Representation of a [`u16`] in the database.
/// We need this because the database does not support unsigned types.
/// This handles converting from the database's INT4 to the actual u16.
#[derive(
    Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, AsExpression, FromSqlRow,
)]
#[sql_type = "sql_types::Int4"]
#[repr(transparent)]
pub struct SqlU16(pub u16);

NewtypeFrom! { () pub struct SqlU16(u16); }
NewtypeDeref! { () pub struct SqlU16(u16); }

impl SqlU16 {
    pub fn new(port: u16) -> Self {
        Self(port)
    }
}

impl<DB> ToSql<sql_types::Int4, DB> for SqlU16
where
    DB: Backend,
    i32: ToSql<sql_types::Int4, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        i32::from(self.0).to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Int4, DB> for SqlU16
where
    DB: Backend,
    i32: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        u16::try_from(i32::from_sql(bytes)?).map(SqlU16).map_err(|e| e.into())
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

impl Ipv6Net {
    /// Generate a random subnetwork from this one, of the given prefix length.
    ///
    /// `None` is returned if:
    ///
    ///  - `prefix` is less than this address's prefix
    ///  - `prefix` is greater than 128
    ///
    /// Note that if the prefix is the same as this address's prefix, a copy of
    /// `self` is returned.
    pub fn random_subnet(&self, prefix: u8) -> Option<Self> {
        use rand::RngCore;

        const MAX_IPV6_SUBNET_PREFIX: u8 = 128;
        if prefix < self.prefix() || prefix > MAX_IPV6_SUBNET_PREFIX {
            return None;
        }
        if prefix == self.prefix() {
            return Some(*self);
        }

        // Generate a random address
        let mut rng = if cfg!(test) {
            StdRng::seed_from_u64(0)
        } else {
            StdRng::from_entropy()
        };
        let random =
            u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64());

        // Generate a mask for the new address.
        //
        // We're operating on the big-endian byte representation of the address.
        // So shift down by the prefix, and then invert, so that we have 1's
        // on the leading bits up to the prefix.
        let full_mask = !(u128::MAX >> prefix);

        // Get the existing network address and mask.
        let network = u128::from_be_bytes(self.network().octets());
        let network_mask = u128::from_be_bytes(self.mask().octets());

        // Take random bits _only_ where the new mask is set.
        let random_mask = full_mask ^ network_mask;

        let out = (network & network_mask) | (random & random_mask);
        let addr = std::net::Ipv6Addr::from(out.to_be_bytes());
        let net = ipnetwork::Ipv6Network::new(addr, prefix)
            .expect("Failed to create random subnet");
        Some(Self(external::Ipv6Net(net)))
    }
}

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

impl MacAddr {
    /**
     * Generate a unique MAC address for an interface
     */
    pub fn new() -> Result<Self, external::Error> {
        use rand::Fill;
        // Use the Oxide OUI A8 40 25
        let mut addr = [0xA8, 0x40, 0x25, 0x00, 0x00, 0x00];
        addr[3..].try_fill(&mut StdRng::from_entropy()).map_err(|_| {
            external::Error::internal_error("failed to generate MAC")
        })?;
        // From RFD 174, Oxide virtual MACs are constrained to have these bits
        // set.
        addr[3] |= 0xF0;
        // TODO-correctness: We should use an explicit allocator for the MACs
        // given the small address space. Right now creation requests may fail
        // due to MAC collision, especially given the 20-bit space.
        Ok(Self(external::MacAddr(macaddr::MacAddr6::from(addr))))
    }
}

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

    pub tuf_base_url: Option<String>,
}

/// Database representation of a Sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[table_name = "sled"]
pub struct Sled {
    #[diesel(embed)]
    identity: SledIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    // ServiceAddress (Sled Agent).
    pub ip: ipnetwork::IpNetwork,
    // TODO: Make use of SqlU16
    pub port: i32,
}

impl Sled {
    pub fn new(id: Uuid, addr: SocketAddr) -> Self {
        Self {
            identity: SledIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            ip: addr.ip().into(),
            port: addr.port().into(),
        }
    }

    pub fn address(&self) -> SocketAddr {
        // TODO: avoid this unwrap
        SocketAddr::new(self.ip.ip(), u16::try_from(self.port).unwrap())
    }
}

impl DatastoreCollection<Zpool> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = zpool::dsl::sled_id;
}

/// Database representation of a Pool.
///
/// A zpool represents a ZFS storage pool, allocated on a single
/// physical sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[table_name = "zpool"]
pub struct Zpool {
    #[diesel(embed)]
    identity: ZpoolIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    // Sled to which this Zpool belongs.
    pub sled_id: Uuid,

    // TODO: In the future, we may expand this structure to include
    // size, allocation, and health information.
    pub total_size: ByteCount,
}

impl Zpool {
    pub fn new(
        id: Uuid,
        sled_id: Uuid,
        info: &internal_api::params::ZpoolPutRequest,
    ) -> Self {
        Self {
            identity: ZpoolIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            sled_id,
            total_size: info.size.into(),
        }
    }
}

impl DatastoreCollection<Dataset> for Zpool {
    type CollectionId = Uuid;
    type GenerationNumberColumn = zpool::dsl::rcgen;
    type CollectionTimeDeletedColumn = zpool::dsl::time_deleted;
    type CollectionIdColumn = dataset::dsl::pool_id;
}

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[postgres(type_name = "dataset_kind", type_schema = "public")]
    pub struct DatasetKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[sql_type = "DatasetKindEnum"]
    pub struct DatasetKind(pub internal_api::params::DatasetKind);

    // Enum values
    Crucible => b"crucible"
    Cockroach => b"cockroach"
    Clickhouse => b"clickhouse"
);

impl From<internal_api::params::DatasetKind> for DatasetKind {
    fn from(k: internal_api::params::DatasetKind) -> Self {
        Self(k)
    }
}

/// Database representation of a Dataset.
///
/// A dataset represents a portion of a Zpool, which is then made
/// available to a service on the Sled.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Asset,
    Deserialize,
    Serialize,
    PartialEq,
)]
#[table_name = "dataset"]
pub struct Dataset {
    #[diesel(embed)]
    identity: DatasetIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub pool_id: Uuid,

    ip: ipnetwork::IpNetwork,
    port: i32,

    kind: DatasetKind,
    pub size_used: Option<i64>,
}

impl Dataset {
    pub fn new(
        id: Uuid,
        pool_id: Uuid,
        addr: SocketAddr,
        kind: DatasetKind,
    ) -> Self {
        let size_used = match kind {
            DatasetKind(internal_api::params::DatasetKind::Crucible) => Some(0),
            _ => None,
        };
        Self {
            identity: DatasetIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            pool_id,
            ip: addr.ip().into(),
            port: addr.port().into(),
            kind,
            size_used,
        }
    }

    pub fn address(&self) -> SocketAddr {
        // TODO: avoid this unwrap
        SocketAddr::new(self.ip.ip(), u16::try_from(self.port).unwrap())
    }
}

// Datasets contain regions
impl DatastoreCollection<Region> for Dataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}

// Virtual disks contain regions
impl DatastoreCollection<Region> for Disk {
    type CollectionId = Uuid;
    type GenerationNumberColumn = disk::dsl::rcgen;
    type CollectionTimeDeletedColumn = disk::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::disk_id;
}

/// Database representation of a Region.
///
/// A region represents a portion of a Crucible Downstairs dataset
/// allocated within a volume.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Asset,
    Serialize,
    Deserialize,
    PartialEq,
)]
#[table_name = "region"]
pub struct Region {
    #[diesel(embed)]
    identity: RegionIdentity,

    dataset_id: Uuid,
    disk_id: Uuid,

    block_size: ByteCount,
    blocks_per_extent: i64,
    extent_count: i64,
}

impl Region {
    pub fn new(
        dataset_id: Uuid,
        disk_id: Uuid,
        block_size: ByteCount,
        blocks_per_extent: i64,
        extent_count: i64,
    ) -> Self {
        Self {
            identity: RegionIdentity::new(Uuid::new_v4()),
            dataset_id,
            disk_id,
            block_size,
            blocks_per_extent,
            extent_count,
        }
    }

    pub fn disk_id(&self) -> Uuid {
        self.disk_id
    }
    pub fn dataset_id(&self) -> Uuid {
        self.dataset_id
    }
    pub fn block_size(&self) -> external::ByteCount {
        self.block_size.0
    }
    pub fn blocks_per_extent(&self) -> i64 {
        self.blocks_per_extent
    }
    pub fn extent_count(&self) -> i64 {
        self.extent_count
    }
    pub fn encrypted(&self) -> bool {
        // Per RFD 29, data is always encrypted at rest, and support for
        // external, customer-supplied keys is a non-requirement.
        true
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
    pub fn new(params: params::OrganizationCreate) -> Self {
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

/// Describes a set of updates for the [`Organization`] model.
#[derive(AsChangeset)]
#[table_name = "organization"]
pub struct OrganizationUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::OrganizationUpdate> for OrganizationUpdate {
    fn from(params: params::OrganizationUpdate) -> Self {
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
    pub fn new(organization_id: Uuid, params: params::ProjectCreate) -> Self {
        Self {
            identity: ProjectIdentity::new(Uuid::new_v4(), params.identity),
            organization_id,
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

impl From<params::ProjectUpdate> for ProjectUpdate {
    fn from(params: params::ProjectUpdate) -> Self {
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
        params: &params::InstanceCreate,
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
    #[column_name = "active_propolis_id"]
    pub propolis_uuid: Uuid,
    #[column_name = "target_propolis_id"]
    pub dst_propolis_uuid: Option<Uuid>,
    #[column_name = "active_propolis_ip"]
    pub propolis_ip: Option<ipnetwork::IpNetwork>,
    #[column_name = "migration_id"]
    pub migration_uuid: Option<Uuid>,
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
            propolis_uuid: state.propolis_uuid,
            dst_propolis_uuid: state.dst_propolis_uuid,
            propolis_ip: state.propolis_addr.map(|addr| addr.ip().into()),
            migration_uuid: state.migration_uuid,
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
            propolis_uuid: self.propolis_uuid,
            dst_propolis_uuid: self.dst_propolis_uuid,
            propolis_addr: self
                .propolis_ip
                .map(|ip| SocketAddr::new(ip.ip(), PROPOLIS_PORT)),
            migration_uuid: self.migration_uuid,
            ncpus: self.ncpus.into(),
            memory: self.memory.into(),
            hostname: self.hostname,
            gen: self.gen.into(),
            time_updated: self.time_updated,
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "instance_state", type_schema = "public")]
    pub struct InstanceStateEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "InstanceStateEnum"]
    pub struct InstanceState(pub external::InstanceState);

    // Enum values
    Creating => b"creating"
    Starting => b"starting"
    Running => b"running"
    Stopping => b"stopping"
    Stopped => b"stopped"
    Rebooting => b"rebooting"
    Migrating => b"migrating"
    Repairing => b"repairing"
    Failed => b"failed"
    Destroyed => b"destroyed"
);

impl InstanceState {
    pub fn new(state: external::InstanceState) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::InstanceState {
        &self.0
    }
}

/// A Disk (network block device).
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Resource,
    Serialize,
    Deserialize,
)]
#[table_name = "disk"]
pub struct Disk {
    #[diesel(embed)]
    identity: DiskIdentity,

    /// child resource generation number, per RFD 192
    rcgen: Generation,

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
        params: params::DiskCreate,
        runtime_initial: DiskRuntimeState,
    ) -> Self {
        let identity = DiskIdentity::new(disk_id, params.identity);
        Self {
            identity,
            rcgen: external::Generation::new().into(),
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

#[derive(
    AsChangeset,
    Clone,
    Debug,
    Queryable,
    Insertable,
    Selectable,
    Serialize,
    Deserialize,
)]
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

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[table_name = "metric_producer"]
pub struct ProducerEndpoint {
    #[diesel(embed)]
    identity: ProducerEndpointIdentity,

    pub ip: ipnetwork::IpNetwork,
    // TODO: Make use of SqlU16
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
    // TODO: Make use of SqlU16
    pub port: i32,
}

impl OximeterInfo {
    pub fn new(info: &internal_api::params::OximeterInfo) -> Self {
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
    pub ipv6_prefix: Ipv6Net,
    pub dns_name: Name,

    /// firewall generation number, used as a child resource generation number
    /// per RFD 192
    pub firewall_gen: Generation,
}

impl Vpc {
    pub fn new(
        vpc_id: Uuid,
        project_id: Uuid,
        system_router_id: Uuid,
        params: params::VpcCreate,
    ) -> Result<Self, external::Error> {
        let identity = VpcIdentity::new(vpc_id, params.identity);
        let ipv6_prefix = match params.ipv6_prefix {
            None => defaults::random_vpc_ipv6_prefix(),
            Some(prefix) => {
                if prefix.is_vpc_prefix() {
                    Ok(prefix)
                } else {
                    Err(external::Error::invalid_request(
                        "VPC IPv6 address prefixes must be in the 
                            Unique Local Address range `fd00::/48` (RFD 4193)",
                    ))
                }
            }
        }?
        .into();
        Ok(Self {
            identity,
            project_id,
            system_router_id,
            ipv6_prefix,
            dns_name: params.dns_name.into(),
            firewall_gen: Generation::new(),
        })
    }
}

impl DatastoreCollection<VpcFirewallRule> for Vpc {
    type CollectionId = Uuid;
    type GenerationNumberColumn = vpc::dsl::firewall_gen;
    type CollectionTimeDeletedColumn = vpc::dsl::time_deleted;
    type CollectionIdColumn = vpc_firewall_rule::dsl::vpc_id;
}

#[derive(AsChangeset)]
#[table_name = "vpc"]
pub struct VpcUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub dns_name: Option<Name>,
}

impl From<params::VpcUpdate> for VpcUpdate {
    fn from(params: params::VpcUpdate) -> Self {
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
    pub ipv4_block: Ipv4Net,
    pub ipv6_block: Ipv6Net,
}

impl VpcSubnet {
    /// Create a new VPC Subnet.
    ///
    /// NOTE: This assumes that the IP address ranges provided in `params` are
    /// valid for the VPC, and does not do any further validation.
    pub fn new(
        subnet_id: Uuid,
        vpc_id: Uuid,
        identity: external::IdentityMetadataCreateParams,
        ipv4_block: external::Ipv4Net,
        ipv6_block: external::Ipv6Net,
    ) -> Self {
        let identity = VpcSubnetIdentity::new(subnet_id, identity);
        Self {
            identity,
            vpc_id,
            ipv4_block: Ipv4Net(ipv4_block),
            ipv6_block: Ipv6Net(ipv6_block),
        }
    }

    /// Verify that the provided IP address is contained in the VPC Subnet.
    ///
    /// This checks:
    ///
    /// - The subnet has an allocated block of the same version as the address
    /// - The allocated block contains the address.
    pub fn contains(&self, addr: IpAddr) -> Result<(), external::Error> {
        match addr {
            IpAddr::V4(addr) => {
                if self.ipv4_block.contains(addr) {
                    Ok(())
                } else {
                    Err(external::Error::invalid_request(&format!(
                        "Address '{}' not in IPv4 subnet '{}'",
                        addr, self.ipv4_block.0,
                    )))
                }
            }
            IpAddr::V6(addr) => {
                if self.ipv6_block.contains(addr) {
                    Ok(())
                } else {
                    Err(external::Error::invalid_request(&format!(
                        "Address '{}' not in IPv6 subnet '{}'",
                        addr, self.ipv6_block.0,
                    )))
                }
            }
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

impl From<params::VpcSubnetUpdate> for VpcSubnetUpdate {
    fn from(params: params::VpcSubnetUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
            ipv4_block: params.ipv4_block.map(Ipv4Net),
            ipv6_block: params.ipv6_block.map(Ipv6Net),
        }
    }
}

impl_enum_type!(
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
    pub rcgen: Generation,
}

impl VpcRouter {
    pub fn new(
        router_id: Uuid,
        vpc_id: Uuid,
        kind: external::VpcRouterKind,
        params: params::VpcRouterCreate,
    ) -> Self {
        let identity = VpcRouterIdentity::new(router_id, params.identity);
        Self {
            identity,
            vpc_id,
            kind: VpcRouterKind(kind),
            rcgen: Generation::new(),
        }
    }
}

impl DatastoreCollection<RouterRoute> for VpcRouter {
    type CollectionId = Uuid;
    type GenerationNumberColumn = vpc_router::dsl::rcgen;
    type CollectionTimeDeletedColumn = vpc_router::dsl::time_deleted;
    type CollectionIdColumn = router_route::dsl::router_id;
}

impl Into<external::VpcRouter> for VpcRouter {
    fn into(self) -> external::VpcRouter {
        external::VpcRouter {
            identity: self.identity(),
            vpc_id: self.vpc_id,
            kind: self.kind.0,
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

impl From<params::VpcRouterUpdate> for VpcRouterUpdate {
    fn from(params: params::VpcRouterUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "router_route_kind", type_schema = "public")]
    pub struct RouterRouteKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "RouterRouteKindEnum"]
    pub struct RouterRouteKind(pub external::RouterRouteKind);

    // Enum values
    Default => b"default"
    VpcSubnet => b"vpc_subnet"
    VpcPeering => b"vpc_peering"
    Custom => b"custom"
);

#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
pub struct RouteTarget(pub external::RouteTarget);

impl<DB> ToSql<sql_types::Text, DB> for RouteTarget
where
    DB: Backend,
    str: ToSql<sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        self.0.to_string().as_str().to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Text, DB> for RouteTarget
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        Ok(RouteTarget(
            String::from_sql(bytes)?.parse::<external::RouteTarget>()?,
        ))
    }
}

#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
pub struct RouteDestination(pub external::RouteDestination);

impl RouteDestination {
    pub fn new(state: external::RouteDestination) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::RouteDestination {
        &self.0
    }
}

impl<DB> ToSql<sql_types::Text, DB> for RouteDestination
where
    DB: Backend,
    str: ToSql<sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        self.0.to_string().as_str().to_sql(out)
    }
}

impl<DB> FromSql<sql_types::Text, DB> for RouteDestination
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        Ok(RouteDestination::new(
            String::from_sql(bytes)?.parse::<external::RouteDestination>()?,
        ))
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[table_name = "router_route"]
pub struct RouterRoute {
    #[diesel(embed)]
    identity: RouterRouteIdentity,

    pub kind: RouterRouteKind,
    pub router_id: Uuid,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

impl RouterRoute {
    pub fn new(
        route_id: Uuid,
        router_id: Uuid,
        kind: external::RouterRouteKind,
        params: external::RouterRouteCreateParams,
    ) -> Self {
        let identity = RouterRouteIdentity::new(route_id, params.identity);
        Self {
            identity,
            router_id,
            kind: RouterRouteKind(kind),
            target: RouteTarget(params.target),
            destination: RouteDestination::new(params.destination),
        }
    }
}

impl Into<external::RouterRoute> for RouterRoute {
    fn into(self) -> external::RouterRoute {
        external::RouterRoute {
            identity: self.identity(),
            router_id: self.router_id,
            kind: self.kind.0,
            target: self.target.0.clone(),
            destination: self.destination.state().clone(),
        }
    }
}

#[derive(AsChangeset)]
#[table_name = "router_route"]
pub struct RouterRouteUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

impl From<external::RouterRouteUpdateParams> for RouterRouteUpdate {
    fn from(params: external::RouterRouteUpdateParams) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
            target: RouteTarget(params.target),
            destination: RouteDestination::new(params.destination),
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_rule_status", type_schema = "public")]
    pub struct VpcFirewallRuleStatusEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallRuleStatusEnum"]
    pub struct VpcFirewallRuleStatus(pub external::VpcFirewallRuleStatus);

    Disabled => b"disabled"
    Enabled => b"enabled"
);
NewtypeFrom! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }
NewtypeDeref! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_rule_direction", type_schema = "public")]
    pub struct VpcFirewallRuleDirectionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallRuleDirectionEnum"]
    pub struct VpcFirewallRuleDirection(pub external::VpcFirewallRuleDirection);

    Inbound => b"inbound"
    Outbound => b"outbound"
);
NewtypeFrom! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }
NewtypeDeref! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_rule_action", type_schema = "public")]
    pub struct VpcFirewallRuleActionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallRuleActionEnum"]
    pub struct VpcFirewallRuleAction(pub external::VpcFirewallRuleAction);

    Allow => b"allow"
    Deny => b"deny"
);
NewtypeFrom! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }
NewtypeDeref! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[postgres(type_name = "vpc_firewall_rule_protocol", type_schema = "public")]
    pub struct VpcFirewallRuleProtocolEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[sql_type = "VpcFirewallRuleProtocolEnum"]
    pub struct VpcFirewallRuleProtocol(pub external::VpcFirewallRuleProtocol);

    Tcp => b"TCP"
    Udp => b"UDP"
    Icmp => b"ICMP"
);
NewtypeFrom! { () pub struct VpcFirewallRuleProtocol(external::VpcFirewallRuleProtocol); }
NewtypeDeref! { () pub struct VpcFirewallRuleProtocol(external::VpcFirewallRuleProtocol); }

/// Newtype wrapper around [`external::VpcFirewallRuleTarget`] so we can derive
/// diesel traits for it
#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
#[repr(transparent)]
pub struct VpcFirewallRuleTarget(pub external::VpcFirewallRuleTarget);
NewtypeFrom! { () pub struct VpcFirewallRuleTarget(external::VpcFirewallRuleTarget); }
NewtypeDeref! { () pub struct VpcFirewallRuleTarget(external::VpcFirewallRuleTarget); }

impl<DB> ToSql<sql_types::Text, DB> for VpcFirewallRuleTarget
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

// Deserialize the "VpcFirewallRuleTarget" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for VpcFirewallRuleTarget
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        Ok(VpcFirewallRuleTarget(
            String::from_sql(bytes)?
                .parse::<external::VpcFirewallRuleTarget>()?,
        ))
    }
}

/// Newtype wrapper around [`external::VpcFirewallRuleHostFilter`] so we can derive
/// diesel traits for it
#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
#[repr(transparent)]
pub struct VpcFirewallRuleHostFilter(pub external::VpcFirewallRuleHostFilter);
NewtypeFrom! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }
NewtypeDeref! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }

impl<DB> ToSql<sql_types::Text, DB> for VpcFirewallRuleHostFilter
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

// Deserialize the "VpcFirewallRuleHostFilter" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for VpcFirewallRuleHostFilter
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        Ok(VpcFirewallRuleHostFilter(
            String::from_sql(bytes)?
                .parse::<external::VpcFirewallRuleHostFilter>()?,
        ))
    }
}

/// Newtype wrapper around [`external::L4PortRange`] so we can derive
/// diesel traits for it
#[derive(Clone, Copy, Debug, AsExpression, FromSqlRow)]
#[sql_type = "sql_types::Text"]
#[repr(transparent)]
pub struct L4PortRange(pub external::L4PortRange);
NewtypeFrom! { () pub struct L4PortRange(external::L4PortRange); }
NewtypeDeref! { () pub struct L4PortRange(external::L4PortRange); }

impl<DB> ToSql<sql_types::Text, DB> for L4PortRange
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

// Deserialize the "L4PortRange" object from SQL INT4.
impl<DB> FromSql<sql_types::Text, DB> for L4PortRange
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        String::from_sql(bytes)?.parse().map(L4PortRange).map_err(|e| e.into())
    }
}

/// Newtype wrapper around [`external::VpcFirewallRulePriority`] so we can derive
/// diesel traits for it
#[derive(Clone, Copy, Debug, AsExpression, FromSqlRow)]
#[repr(transparent)]
#[sql_type = "sql_types::Int4"]
pub struct VpcFirewallRulePriority(pub external::VpcFirewallRulePriority);
NewtypeFrom! { () pub struct VpcFirewallRulePriority(external::VpcFirewallRulePriority); }
NewtypeDeref! { () pub struct VpcFirewallRulePriority(external::VpcFirewallRulePriority); }

impl<DB> ToSql<sql_types::Int4, DB> for VpcFirewallRulePriority
where
    DB: Backend,
    SqlU16: ToSql<sql_types::Int4, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut serialize::Output<W, DB>,
    ) -> serialize::Result {
        SqlU16(self.0 .0).to_sql(out)
    }
}

// Deserialize the "VpcFirewallRulePriority" object from SQL TEXT.
impl<DB> FromSql<sql_types::Int4, DB> for VpcFirewallRulePriority
where
    DB: Backend,
    SqlU16: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        Ok(VpcFirewallRulePriority(external::VpcFirewallRulePriority(
            *SqlU16::from_sql(bytes)?,
        )))
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[table_name = "vpc_firewall_rule"]
pub struct VpcFirewallRule {
    #[diesel(embed)]
    pub identity: VpcFirewallRuleIdentity,

    pub vpc_id: Uuid,
    pub status: VpcFirewallRuleStatus,
    pub direction: VpcFirewallRuleDirection,
    pub targets: Vec<VpcFirewallRuleTarget>,
    pub filter_hosts: Option<Vec<VpcFirewallRuleHostFilter>>,
    pub filter_ports: Option<Vec<L4PortRange>>,
    pub filter_protocols: Option<Vec<VpcFirewallRuleProtocol>>,
    pub action: VpcFirewallRuleAction,
    pub priority: VpcFirewallRulePriority,
}

impl VpcFirewallRule {
    pub fn new(
        rule_id: Uuid,
        vpc_id: Uuid,
        rule: &external::VpcFirewallRuleUpdate,
    ) -> Self {
        let identity = VpcFirewallRuleIdentity::new(
            rule_id,
            external::IdentityMetadataCreateParams {
                name: rule.name.clone(),
                description: rule.description.clone(),
            },
        );
        Self {
            identity,
            vpc_id,
            status: rule.status.into(),
            direction: rule.direction.into(),
            targets: rule
                .targets
                .iter()
                .map(|target| target.clone().into())
                .collect(),
            filter_hosts: rule.filters.hosts.as_ref().map(|hosts| {
                hosts
                    .iter()
                    .map(|target| VpcFirewallRuleHostFilter(target.clone()))
                    .collect()
            }),
            filter_ports: rule.filters.ports.as_ref().map(|ports| {
                ports.iter().map(|range| L4PortRange(*range)).collect()
            }),
            filter_protocols: rule.filters.protocols.as_ref().map(|protos| {
                protos.iter().map(|proto| (*proto).into()).collect()
            }),
            action: rule.action.into(),
            priority: rule.priority.into(),
        }
    }

    pub fn vec_from_params(
        vpc_id: Uuid,
        params: external::VpcFirewallRuleUpdateParams,
    ) -> Vec<VpcFirewallRule> {
        params
            .rules
            .iter()
            .map(|rule| VpcFirewallRule::new(Uuid::new_v4(), vpc_id, rule))
            .collect()
    }
}

impl Into<external::VpcFirewallRule> for VpcFirewallRule {
    fn into(self) -> external::VpcFirewallRule {
        external::VpcFirewallRule {
            identity: self.identity(),
            status: self.status.into(),
            direction: self.direction.into(),
            targets: self
                .targets
                .iter()
                .map(|target| target.clone().into())
                .collect(),
            filters: external::VpcFirewallRuleFilter {
                hosts: self.filter_hosts.map(|hosts| {
                    hosts.iter().map(|host| host.0.clone()).collect()
                }),
                ports: self
                    .filter_ports
                    .map(|ports| ports.iter().map(|range| range.0).collect()),
                protocols: self.filter_protocols.map(|protocols| {
                    protocols.iter().map(|protocol| protocol.0).collect()
                }),
            },
            action: self.action.into(),
            priority: self.priority.into(),
            vpc_id: self.vpc_id,
        }
    }
}

/// A not fully constructed NetworkInterface. It may not yet have an IP
/// address allocated.
#[derive(Clone, Debug)]
pub struct IncompleteNetworkInterface {
    pub identity: NetworkInterfaceIdentity,

    pub instance_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet: VpcSubnet,
    pub mac: MacAddr,
    pub ip: Option<std::net::IpAddr>,
}

impl IncompleteNetworkInterface {
    pub fn new(
        interface_id: Uuid,
        instance_id: Uuid,
        vpc_id: Uuid,
        subnet: VpcSubnet,
        mac: MacAddr,
        ip: Option<std::net::IpAddr>,
        params: params::NetworkInterfaceCreate,
    ) -> Self {
        let identity =
            NetworkInterfaceIdentity::new(interface_id, params.identity);
        Self { identity, instance_id, subnet, vpc_id, mac, ip }
    }
}

#[derive(Selectable, Queryable, Insertable, Clone, Debug, Resource)]
#[table_name = "network_interface"]
pub struct NetworkInterface {
    #[diesel(embed)]
    pub identity: NetworkInterfaceIdentity,

    pub instance_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    pub ip: ipnetwork::IpNetwork,
}

impl From<NetworkInterface> for external::NetworkInterface {
    fn from(iface: NetworkInterface) -> Self {
        Self {
            identity: iface.identity(),
            instance_id: iface.instance_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            ip: iface.ip.ip(),
            mac: *iface.mac,
        }
    }
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

/// Describes a built-in user, as stored in the database
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[table_name = "user_builtin"]
pub struct UserBuiltin {
    #[diesel(embed)]
    pub identity: UserBuiltinIdentity,
}

impl UserBuiltin {
    /// Creates a new database UserBuiltin object.
    pub fn new(id: Uuid, params: params::UserBuiltinCreate) -> Self {
        Self { identity: UserBuiltinIdentity::new(id, params.identity) }
    }
}

/// Describes a built-in role, as stored in the database
#[derive(Queryable, Insertable, Debug, Selectable)]
#[table_name = "role_builtin"]
pub struct RoleBuiltin {
    pub resource_type: String,
    pub role_name: String,
    pub description: String,
}

impl RoleBuiltin {
    /// Creates a new database UserBuiltin object.
    pub fn new(
        resource_type: omicron_common::api::external::ResourceType,
        role_name: &str,
        description: &str,
    ) -> Self {
        Self {
            resource_type: resource_type.to_string(),
            role_name: String::from(role_name),
            description: String::from(description),
        }
    }
}

/// Describes an assignment of a built-in role for a built-in user
#[derive(Queryable, Insertable, Debug, Selectable)]
#[table_name = "role_assignment_builtin"]
pub struct RoleAssignmentBuiltin {
    pub user_builtin_id: Uuid,
    pub resource_type: String,
    pub resource_id: Uuid,
    pub role_name: String,
}

impl RoleAssignmentBuiltin {
    /// Creates a new database RoleAssignmentBuiltin object.
    pub fn new(
        user_builtin_id: Uuid,
        resource_type: omicron_common::api::external::ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) -> Self {
        Self {
            user_builtin_id,
            resource_type: resource_type.to_string(),
            resource_id,
            role_name: String::from(role_name),
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[postgres(type_name = "update_artifact_kind", type_schema = "public")]
    pub struct UpdateArtifactKindEnum;

    #[derive(Clone, Debug, Display, AsExpression, FromSqlRow)]
    #[display("{0}")]
    #[sql_type = "UpdateArtifactKindEnum"]
    pub struct UpdateArtifactKind(pub internal::nexus::UpdateArtifactKind);

    // Enum values
    Zone => b"zone"
);

#[derive(
    Queryable, Insertable, Clone, Debug, Display, Selectable, AsChangeset,
)]
#[table_name = "update_available_artifact"]
#[display("{kind} \"{name}\" v{version}")]
pub struct UpdateAvailableArtifact {
    pub name: String,
    /// Version of the artifact itself
    pub version: i64,
    pub kind: UpdateArtifactKind,
    /// `version` field of targets.json from the repository
    // FIXME this *should* be a NonZeroU64
    pub targets_role_version: i64,
    pub valid_until: DateTime<Utc>,
    pub target_name: String,
    // FIXME should this be [u8; 32]?
    pub target_sha256: String,
    // FIXME this *should* be a u64
    pub target_length: i64,
}

#[cfg(test)]
mod tests {
    use super::Uuid;
    use super::VpcSubnet;
    use ipnetwork::Ipv4Network;
    use ipnetwork::Ipv6Network;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Ipv4Net;
    use omicron_common::api::external::Ipv6Net;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    #[test]
    fn test_vpc_subnet_contains() {
        let ipv4_block =
            Ipv4Net("192.168.0.0/16".parse::<Ipv4Network>().unwrap());
        let ipv6_block = Ipv6Net("fd00::/48".parse::<Ipv6Network>().unwrap());
        let identity = IdentityMetadataCreateParams {
            name: "net-test-vpc".parse().unwrap(),
            description: "A test VPC".parse().unwrap(),
        };
        let vpc = VpcSubnet::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            identity,
            ipv4_block,
            ipv6_block,
        );
        assert!(vpc
            .contains(IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)))
            .is_ok());
        assert!(vpc
            .contains(IpAddr::from(Ipv4Addr::new(192, 160, 1, 1)))
            .is_err());
        assert!(vpc
            .contains(IpAddr::from(Ipv6Addr::new(
                0xfd00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
            )))
            .is_ok());
        assert!(vpc
            .contains(IpAddr::from(Ipv6Addr::new(
                0xfc00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
            )))
            .is_err());
    }

    #[test]
    fn test_ipv6_net_random_subnet() {
        let base = super::Ipv6Net(Ipv6Net(
            "fd00::/48".parse::<Ipv6Network>().unwrap(),
        ));
        assert!(
            base.random_subnet(8).is_none(),
            "random_subnet() should fail when prefix is less than the base prefix"
        );
        assert!(
            base.random_subnet(130).is_none(),
            "random_subnet() should fail when prefix is greater than 128"
        );
        let subnet = base.random_subnet(64).unwrap();
        assert_eq!(
            subnet.prefix(),
            64,
            "random_subnet() returned an incorrect prefix"
        );
        let octets = subnet.network().octets();
        const EXPECTED_RANDOM_BYTES: [u8; 8] = [253, 0, 0, 0, 0, 0, 111, 127];
        assert_eq!(octets[..8], EXPECTED_RANDOM_BYTES);
        assert!(
            octets[8..].iter().all(|x| *x == 0),
            "Host address portion should be 0"
        );
        assert!(
            base.is_supernet_of(subnet.0 .0),
            "random_subnet should generate an actual subnet"
        );
        assert_eq!(base.random_subnet(base.prefix()), Some(base));
    }
}
