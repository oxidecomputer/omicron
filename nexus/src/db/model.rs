// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures stored to the database.

use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::{Asset, Resource};
use crate::db::ipv6;
use crate::db::schema::{
    console_session, dataset, disk, global_image, image, instance,
    metric_producer, network_interface, organization, oximeter, project, rack,
    region, role_assignment, role_builtin, router_route, silo, silo_user, sled,
    snapshot, ssh_key, update_available_artifact, user_builtin, volume, vpc,
    vpc_firewall_rule, vpc_router, vpc_subnet, zpool,
};
use crate::defaults;
use crate::external_api::views;
use crate::external_api::{params, shared};
use crate::internal_api;
use chrono::{DateTime, Utc};
use db_macros::{Asset, Resource};
use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
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
use std::io::Write;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use uuid::Uuid;

// TODO: Break up types into multiple files

// TODO: The existence of both impl_enum_type and impl_enum_wrapper is a
// temporary state of affairs while we do the work of converting uses of
// impl_enum_wrapper to impl_enum_type. This is part of a broader initiative to
// move types out of the common crate into Nexus where possible. See
// https://github.com/oxidecomputer/omicron/issues/388

/// This macro implements serialization and deserialization of an enum type from
/// our database into our model types. This version wraps an enum imported from
/// the common crate in a struct so we can implement DB traits on it. We are
/// moving those enum definitions into this file and using impl_enum_type
/// instead, so eventually this macro will go away. See [`InstanceState`] for a
/// sample usage.
macro_rules! impl_enum_wrapper {
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

        impl ToSql<$diesel_type, Pg> for $model_type {
            fn to_sql<'a>(
                &'a self,
                out: &mut serialize::Output<'a, '_, Pg>,
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

        impl FromSql<$diesel_type, Pg> for $model_type {
            fn from_sql(bytes: RawValue<Pg>) -> deserialize::Result<Self> {
                match RawValue::<Pg>::as_bytes(&bytes) {
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

/// This macro implements serialization and deserialization of an enum type from
/// our database into our model types. See [`VpcRouterKindEnum`] and
/// [`VpcRouterKind`] for a sample usage
macro_rules! impl_enum_type {
    (
        $(#[$enum_meta:meta])*
        pub struct $diesel_type:ident;

        $(#[$model_meta:meta])*
        pub enum $model_type:ident;

        $($enum_item:ident => $sql_value:literal)+
    ) => {
        $(#[$enum_meta])*
        pub struct $diesel_type;

        $(#[$model_meta])*
        pub enum $model_type {
            $(
                $enum_item,
            )*
        }

        impl ToSql<$diesel_type, Pg> for $model_type {
            fn to_sql<'a>(
                &'a self,
                out: &mut serialize::Output<'a, '_, Pg>,
            ) -> serialize::Result {
                match self {
                    $(
                    $model_type::$enum_item => {
                        out.write_all($sql_value)?
                    }
                    )*
                }
                Ok(IsNull::No)
            }
        }

        impl FromSql<$diesel_type, Pg> for $model_type {
            fn from_sql(bytes: RawValue<Pg>) -> deserialize::Result<Self> {
                match RawValue::<Pg>::as_bytes(&bytes) {
                    $(
                    $sql_value => {
                        Ok($model_type::$enum_item)
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
#[diesel(sql_type = sql_types::Text)]
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
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, DB>,
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
#[diesel(sql_type = sql_types::BigInt)]
pub struct ByteCount(pub external::ByteCount);

NewtypeFrom! { () pub struct ByteCount(external::ByteCount); }
NewtypeDeref! { () pub struct ByteCount(external::ByteCount); }

impl ToSql<sql_types::BigInt, Pg> for ByteCount {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::from(self.0),
            &mut out.reborrow(),
        )
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

impl From<ByteCount> for sled_agent_client::types::ByteCount {
    fn from(b: ByteCount) -> Self {
        Self(b.to_bytes())
    }
}

impl From<BlockSize> for ByteCount {
    fn from(bs: BlockSize) -> Self {
        Self(bs.to_bytes().into())
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
#[diesel(sql_type = sql_types::BigInt)]
#[repr(transparent)]
pub struct Generation(pub external::Generation);

NewtypeFrom! { () pub struct Generation(external::Generation); }
NewtypeDeref! { () pub struct Generation(external::Generation); }

impl Generation {
    pub fn new() -> Self {
        Self(external::Generation::new())
    }
}

impl ToSql<sql_types::BigInt, Pg> for Generation {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::from(&self.0),
            &mut out.reborrow(),
        )
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

impl From<Generation> for sled_agent_client::types::Generation {
    fn from(g: Generation) -> Self {
        Self(i64::from(&g.0) as u64)
    }
}

/// Representation of a [`u16`] in the database.
/// We need this because the database does not support unsigned types.
/// This handles converting from the database's INT4 to the actual u16.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, FromSqlRow)]
#[diesel(sql_type = sql_types::Int4)]
#[repr(transparent)]
pub struct SqlU16(pub u16);

NewtypeFrom! { () pub struct SqlU16(u16); }
NewtypeDeref! { () pub struct SqlU16(u16); }

impl SqlU16 {
    pub fn new(port: u16) -> Self {
        Self(port)
    }
}

impl ToSql<sql_types::Int4, Pg> for SqlU16 {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i32 as ToSql<sql_types::Int4, Pg>>::to_sql(
            &i32::from(self.0),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::BigInt)]
pub struct InstanceCpuCount(pub external::InstanceCpuCount);

NewtypeFrom! { () pub struct InstanceCpuCount(external::InstanceCpuCount); }
NewtypeDeref! { () pub struct InstanceCpuCount(external::InstanceCpuCount); }

impl ToSql<sql_types::BigInt, Pg> for InstanceCpuCount {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::from(&self.0),
            &mut out.reborrow(),
        )
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

impl From<InstanceCpuCount> for sled_agent_client::types::InstanceCpuCount {
    fn from(i: InstanceCpuCount) -> Self {
        Self(i.0 .0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Inet)]
pub struct Ipv4Net(pub external::Ipv4Net);

NewtypeFrom! { () pub struct Ipv4Net(external::Ipv4Net); }
NewtypeDeref! { () pub struct Ipv4Net(external::Ipv4Net); }

impl Ipv4Net {
    /// Check if an address is a valid user-requestable address for this subnet
    pub fn check_requestable_addr(&self, addr: Ipv4Addr) -> bool {
        self.contains(addr)
            && (
                // First N addresses are reserved
                self.iter()
                    .take(defaults::NUM_INITIAL_RESERVED_IP_ADDRESSES)
                    .all(|this| this != addr)
            )
            && (
                // Last address in the subnet is reserved
                addr != self.broadcast()
            )
    }
}

impl ToSql<sql_types::Inet, Pg> for Ipv4Net {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <IpNetwork as ToSql<sql_types::Inet, Pg>>::to_sql(
            &IpNetwork::V4(*self.0),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::Inet)]
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

    /// Check if an address is a valid user-requestable address for this subnet
    pub fn check_requestable_addr(&self, addr: Ipv6Addr) -> bool {
        // Only the first N addresses are reserved
        self.contains(addr)
            && self
                .iter()
                .take(defaults::NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .all(|this| this != addr)
    }
}

impl ToSql<sql_types::Inet, Pg> for Ipv6Net {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <IpNetwork as ToSql<sql_types::Inet, Pg>>::to_sql(
            &IpNetwork::V6(self.0 .0),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::Text)]
pub struct MacAddr(pub external::MacAddr);

impl MacAddr {
    /// Generate a unique MAC address for an interface
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

impl ToSql<sql_types::Text, Pg> for MacAddr {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
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
#[diesel(table_name = rack)]
pub struct Rack {
    #[diesel(embed)]
    pub identity: RackIdentity,

    pub tuf_base_url: Option<String>,
}

/// Database representation of a Sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = sled)]
pub struct Sled {
    #[diesel(embed)]
    identity: SledIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    // TODO: Make use of SqlU16
    pub port: i32,

    /// The last IP address provided to an Oxide service on this sled
    pub last_used_address: ipv6::Ipv6Addr,
}

// TODO-correctness: We need a small offset here, while services and
// their addresses are still hardcoded in the mock RSS config file at
// `./smf/sled-agent/config-rss.toml`. This avoids conflicts with those
// addresses, but should be removed when they are entirely under the
// control of Nexus or RSS.
//
// See https://github.com/oxidecomputer/omicron/issues/732 for tracking issue.
pub(crate) const STATIC_IPV6_ADDRESS_OFFSET: u16 = 20;
impl Sled {
    pub fn new(id: Uuid, addr: SocketAddrV6) -> Self {
        let last_used_address = {
            let mut segments = addr.ip().segments();
            segments[7] += STATIC_IPV6_ADDRESS_OFFSET;
            ipv6::Ipv6Addr::from(Ipv6Addr::from(segments))
        };
        Self {
            identity: SledIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            ip: ipv6::Ipv6Addr::from(addr.ip()),
            port: addr.port().into(),
            last_used_address,
        }
    }

    pub fn ip(&self) -> Ipv6Addr {
        self.ip.into()
    }

    pub fn address(&self) -> SocketAddrV6 {
        // TODO: avoid this unwrap
        self.address_with_port(u16::try_from(self.port).unwrap())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip(), port, 0, 0)
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
#[diesel(table_name = zpool)]
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
    #[diesel(postgres_type(name = "dataset_kind"))]
    pub struct DatasetKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DatasetKindEnum)]
    pub enum DatasetKind;

    // Enum values
    Crucible => b"crucible"
    Cockroach => b"cockroach"
    Clickhouse => b"clickhouse"
);

impl From<internal_api::params::DatasetKind> for DatasetKind {
    fn from(k: internal_api::params::DatasetKind) -> Self {
        match k {
            internal_api::params::DatasetKind::Crucible => {
                DatasetKind::Crucible
            }
            internal_api::params::DatasetKind::Cockroach => {
                DatasetKind::Cockroach
            }
            internal_api::params::DatasetKind::Clickhouse => {
                DatasetKind::Clickhouse
            }
        }
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
#[diesel(table_name = dataset)]
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
            DatasetKind::Crucible => Some(0),
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
        self.address_with_port(u16::try_from(self.port).unwrap())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddr {
        SocketAddr::new(self.ip.ip(), port)
    }
}

// Datasets contain regions
impl DatastoreCollection<Region> for Dataset {
    type CollectionId = Uuid;
    type GenerationNumberColumn = dataset::dsl::rcgen;
    type CollectionTimeDeletedColumn = dataset::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::dataset_id;
}

// Volumes contain regions
impl DatastoreCollection<Region> for Volume {
    type CollectionId = Uuid;
    type GenerationNumberColumn = volume::dsl::rcgen;
    type CollectionTimeDeletedColumn = volume::dsl::time_deleted;
    type CollectionIdColumn = region::dsl::volume_id;
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
#[diesel(table_name = region)]
pub struct Region {
    #[diesel(embed)]
    identity: RegionIdentity,

    dataset_id: Uuid,
    volume_id: Uuid,

    block_size: ByteCount,
    blocks_per_extent: i64,
    extent_count: i64,
}

impl Region {
    pub fn new(
        dataset_id: Uuid,
        volume_id: Uuid,
        block_size: ByteCount,
        blocks_per_extent: i64,
        extent_count: i64,
    ) -> Self {
        Self {
            identity: RegionIdentity::new(Uuid::new_v4()),
            dataset_id,
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
        }
    }

    pub fn volume_id(&self) -> Uuid {
        self.volume_id
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

#[derive(
    Asset,
    Queryable,
    Insertable,
    Debug,
    Selectable,
    Serialize,
    Deserialize,
    Clone,
)]
#[diesel(table_name = volume)]
pub struct Volume {
    #[diesel(embed)]
    identity: VolumeIdentity,
    time_deleted: Option<DateTime<Utc>>,

    rcgen: Generation,

    data: String,
}

impl Volume {
    pub fn new(id: Uuid, data: String) -> Self {
        Self {
            identity: VolumeIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            data,
        }
    }

    pub fn data(&self) -> &str {
        &self.data
    }
}

/// Describes a silo within the database.
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = silo)]
pub struct Silo {
    #[diesel(embed)]
    identity: SiloIdentity,

    pub discoverable: bool,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl Silo {
    /// Creates a new database Silo object.
    pub fn new(params: params::SiloCreate) -> Self {
        Self::new_with_id(Uuid::new_v4(), params)
    }

    pub fn new_with_id(id: Uuid, params: params::SiloCreate) -> Self {
        Self {
            identity: SiloIdentity::new(id, params.identity),
            discoverable: params.discoverable,
            rcgen: Generation::new(),
        }
    }
}

impl DatastoreCollection<Organization> for Silo {
    type CollectionId = Uuid;
    type GenerationNumberColumn = silo::dsl::rcgen;
    type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
    type CollectionIdColumn = organization::dsl::silo_id;
}

/// Describes a silo user within the database.
#[derive(Asset, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_user)]
pub struct SiloUser {
    #[diesel(embed)]
    identity: SiloUserIdentity,
    pub silo_id: Uuid,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl SiloUser {
    pub fn new(silo_id: Uuid, user_id: Uuid) -> Self {
        Self {
            identity: SiloUserIdentity::new(user_id),
            silo_id,
            time_deleted: None,
        }
    }
}

/// Describes a user's public SSH key within the database.
#[derive(Clone, Debug, Insertable, Queryable, Resource, Selectable)]
#[diesel(table_name = ssh_key)]
pub struct SshKey {
    #[diesel(embed)]
    identity: SshKeyIdentity,

    pub silo_user_id: Uuid,
    pub public_key: String,
}

impl SshKey {
    pub fn new(silo_user_id: Uuid, params: params::SshKeyCreate) -> Self {
        Self::new_with_id(Uuid::new_v4(), silo_user_id, params)
    }

    pub fn new_with_id(
        id: Uuid,
        silo_user_id: Uuid,
        params: params::SshKeyCreate,
    ) -> Self {
        Self {
            identity: SshKeyIdentity::new(id, params.identity),
            silo_user_id,
            public_key: params.public_key,
        }
    }
}

/// Describes an organization within the database.
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = organization)]
pub struct Organization {
    #[diesel(embed)]
    identity: OrganizationIdentity,

    pub silo_id: Uuid,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl Organization {
    /// Creates a new database Organization object.
    pub fn new(params: params::OrganizationCreate, silo_id: Uuid) -> Self {
        let id = Uuid::new_v4();
        Self {
            identity: OrganizationIdentity::new(id, params.identity),
            silo_id,
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
#[diesel(table_name = organization)]
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
#[diesel(table_name = project)]
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
#[diesel(table_name = project)]
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

/// Runtime state of the Instance, including the actual running state and minimal
/// metadata
///
/// This state is owned by the sled agent running that Instance.
#[derive(Clone, Debug, AsChangeset, Selectable, Insertable, Queryable)]
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
    pub sled_uuid: Uuid,
    #[diesel(column_name = active_propolis_id)]
    pub propolis_uuid: Uuid,
    #[diesel(column_name = active_propolis_ip)]
    pub propolis_ip: Option<ipnetwork::IpNetwork>,
    #[diesel(column_name = target_propolis_id)]
    pub dst_propolis_uuid: Option<Uuid>,
    #[diesel(column_name = migration_id)]
    pub migration_uuid: Option<Uuid>,
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
            sled_uuid: s.sled_uuid,
            propolis_uuid: s.propolis_uuid,
            dst_propolis_uuid: s.dst_propolis_uuid,
            propolis_addr: s
                .propolis_ip
                .map(|ip| SocketAddr::new(ip.ip(), PROPOLIS_PORT).to_string()),
            migration_uuid: s.migration_uuid,
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

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "instance_state"))]
    pub struct InstanceStateEnum;

    #[derive(Clone, Debug, PartialEq, AsExpression, FromSqlRow)]
    #[diesel(sql_type = InstanceStateEnum)]
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

impl From<InstanceState> for sled_agent_client::types::InstanceState {
    fn from(s: InstanceState) -> Self {
        use external::InstanceState::*;
        use sled_agent_client::types::InstanceState as Output;
        match s.0 {
            Creating => Output::Creating,
            Starting => Output::Starting,
            Running => Output::Running,
            Stopping => Output::Stopping,
            Stopped => Output::Stopped,
            Rebooting => Output::Rebooting,
            Migrating => Output::Migrating,
            Repairing => Output::Repairing,
            Failed => Output::Failed,
            Destroyed => Output::Destroyed,
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "block_size"))]
    pub struct BlockSizeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = BlockSizeEnum)]
    pub enum BlockSize;

    // Enum values
    Traditional => b"512"
    Iso => b"2048"
    AdvancedFormat => b"4096"
);

impl BlockSize {
    pub fn to_bytes(&self) -> u32 {
        match self {
            BlockSize::Traditional => 512,
            BlockSize::Iso => 2048,
            BlockSize::AdvancedFormat => 4096,
        }
    }
}

impl Into<external::ByteCount> for BlockSize {
    fn into(self) -> external::ByteCount {
        external::ByteCount::from(self.to_bytes())
    }
}

impl TryFrom<params::BlockSize> for BlockSize {
    type Error = anyhow::Error;
    fn try_from(block_size: params::BlockSize) -> Result<Self, Self::Error> {
        match block_size.0 {
            512 => Ok(BlockSize::Traditional),
            2048 => Ok(BlockSize::Iso),
            4096 => Ok(BlockSize::AdvancedFormat),
            _ => anyhow::bail!("invalid block size {}", block_size.0),
        }
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
#[diesel(table_name = disk)]
pub struct Disk {
    #[diesel(embed)]
    identity: DiskIdentity,

    /// child resource generation number, per RFD 192
    rcgen: Generation,

    /// id for the project containing this Disk
    pub project_id: Uuid,

    /// Root volume of the disk
    pub volume_id: Uuid,

    /// runtime state of the Disk
    #[diesel(embed)]
    pub runtime_state: DiskRuntimeState,

    /// size of the Disk
    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,

    /// size of blocks (512, 2048, or 4096)
    pub block_size: BlockSize,

    /// id for the snapshot from which this Disk was created (None means a blank
    /// disk)
    #[diesel(column_name = origin_snapshot)]
    pub create_snapshot_id: Option<Uuid>,

    /// id for the image from which this Disk was created (None means a blank
    /// disk)
    #[diesel(column_name = origin_image)]
    pub create_image_id: Option<Uuid>,
}

impl Disk {
    pub fn new(
        disk_id: Uuid,
        project_id: Uuid,
        volume_id: Uuid,
        params: params::DiskCreate,
        block_size: BlockSize,
        runtime_initial: DiskRuntimeState,
    ) -> Result<Self, anyhow::Error> {
        let identity = DiskIdentity::new(disk_id, params.identity);

        let create_snapshot_id = match params.disk_source {
            params::DiskSource::Snapshot { snapshot_id } => Some(snapshot_id),
            _ => None,
        };

        // XXX further enum here for different image types?
        let create_image_id = match params.disk_source {
            params::DiskSource::Image { image_id } => Some(image_id),
            params::DiskSource::GlobalImage { image_id } => Some(image_id),
            _ => None,
        };

        Ok(Self {
            identity,
            rcgen: external::Generation::new().into(),
            project_id,
            volume_id,
            runtime_state: runtime_initial,
            size: params.size.into(),
            block_size,
            create_snapshot_id,
            create_image_id,
        })
    }

    pub fn state(&self) -> DiskState {
        self.runtime_state.state()
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        self.runtime_state.clone()
    }

    pub fn id(&self) -> Uuid {
        self.identity.id
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
            image_id: self.create_image_id,
            size: self.size.into(),
            block_size: self.block_size.into(),
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
#[diesel(table_name = disk)]
// When "attach_instance_id" is set to None, we'd like to
// clear it from the DB, rather than ignore the update.
#[diesel(treat_none_as_null = true)]
pub struct DiskRuntimeState {
    /// runtime state of the Disk
    pub disk_state: String,
    pub attach_instance_id: Option<Uuid>,
    /// generation number for this state
    #[diesel(column_name = state_generation)]
    pub gen: Generation,
    /// timestamp for this information
    #[diesel(column_name = time_state_updated)]
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

    pub fn attach(self, instance_id: Uuid) -> Self {
        Self {
            disk_state: external::DiskState::Attached(instance_id)
                .label()
                .to_string(),
            attach_instance_id: Some(instance_id),
            gen: self.gen.next().into(),
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

    pub fn faulted(self) -> Self {
        Self {
            disk_state: external::DiskState::Faulted.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
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
#[diesel(sql_type = sql_types::Text)]
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

/// Newtype wrapper around [external::Digest]
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
#[diesel(sql_type = sql_types::Text)]
#[serde(transparent)]
#[repr(transparent)]
#[display("{0}")]
pub struct Digest(pub external::Digest);

NewtypeFrom! { () pub struct Digest(external::Digest); }
NewtypeDeref! { () pub struct Digest(external::Digest); }

impl ToSql<sql_types::Text, Pg> for Digest {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Text, DB> for Digest
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        let digest: external::Digest = String::from_sql(bytes)?.parse()?;
        Ok(Digest(digest))
    }
}

// Project images

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = image)]
pub struct Image {
    #[diesel(embed)]
    pub identity: ImageIdentity,

    pub project_id: Uuid,
    pub volume_id: Uuid,
    pub url: Option<String>,
    pub version: Option<String>,
    pub digest: Option<Digest>,

    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl From<Image> for views::Image {
    fn from(image: Image) -> Self {
        Self {
            identity: image.identity(),
            project_id: image.project_id,
            url: image.url,
            version: image.version,
            digest: image.digest.map(|x| x.into()),
            block_size: image.block_size.into(),
            size: image.size.into(),
        }
    }
}

// Global images

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = global_image)]
pub struct GlobalImage {
    #[diesel(embed)]
    pub identity: GlobalImageIdentity,

    pub volume_id: Uuid,
    pub url: Option<String>,
    pub version: Option<String>,
    pub digest: Option<Digest>,

    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl From<GlobalImage> for views::GlobalImage {
    fn from(image: GlobalImage) -> Self {
        Self {
            identity: image.identity(),
            url: image.url,
            version: image.version,
            digest: image.digest.map(|x| x.into()),
            block_size: image.block_size.into(),
            size: image.size.into(),
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = snapshot)]
pub struct Snapshot {
    #[diesel(embed)]
    identity: SnapshotIdentity,

    project_id: Uuid,
    disk_id: Uuid,
    volume_id: Uuid,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl From<Snapshot> for views::Snapshot {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            identity: snapshot.identity(),
            project_id: snapshot.project_id,
            disk_id: snapshot.disk_id,
            size: snapshot.size.into(),
        }
    }
}

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = metric_producer)]
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
#[diesel(table_name = oximeter)]
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
#[diesel(table_name = vpc)]
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
#[diesel(table_name = vpc)]
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
#[diesel(table_name = vpc_subnet)]
pub struct VpcSubnet {
    #[diesel(embed)]
    pub identity: VpcSubnetIdentity,

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
    /// - The address is not reserved.
    pub fn check_requestable_addr(
        &self,
        addr: IpAddr,
    ) -> Result<(), external::Error> {
        let subnet = match addr {
            IpAddr::V4(addr) => {
                if self.ipv4_block.check_requestable_addr(addr) {
                    return Ok(());
                }
                ipnetwork::IpNetwork::V4(self.ipv4_block.0 .0)
            }
            IpAddr::V6(addr) => {
                if self.ipv6_block.check_requestable_addr(addr) {
                    return Ok(());
                }
                ipnetwork::IpNetwork::V6(self.ipv6_block.0 .0)
            }
        };
        Err(external::Error::invalid_request(&format!(
            "Address '{}' not in subnet '{}' or is reserved for rack services",
            addr, subnet,
        )))
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = vpc_subnet)]
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
    #[diesel(postgres_type(name = "vpc_router_kind"))]
    pub struct VpcRouterKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = VpcRouterKindEnum)]
    pub enum VpcRouterKind;

    // Enum values
    System => b"system"
    Custom => b"custom"
);

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = vpc_router)]
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
        kind: VpcRouterKind,
        params: params::VpcRouterCreate,
    ) -> Self {
        let identity = VpcRouterIdentity::new(router_id, params.identity);
        Self { identity, vpc_id, kind, rcgen: Generation::new() }
    }
}

impl DatastoreCollection<RouterRoute> for VpcRouter {
    type CollectionId = Uuid;
    type GenerationNumberColumn = vpc_router::dsl::rcgen;
    type CollectionTimeDeletedColumn = vpc_router::dsl::time_deleted;
    type CollectionIdColumn = router_route::dsl::vpc_router_id;
}

#[derive(AsChangeset)]
#[diesel(table_name = vpc_router)]
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

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "router_route_kind"))]
    pub struct RouterRouteKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = RouterRouteKindEnum)]
    pub struct RouterRouteKind(pub external::RouterRouteKind);

    // Enum values
    Default => b"default"
    VpcSubnet => b"vpc_subnet"
    VpcPeering => b"vpc_peering"
    Custom => b"custom"
);

#[derive(Clone, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Text)]
pub struct RouteTarget(pub external::RouteTarget);

impl ToSql<sql_types::Text, Pg> for RouteTarget {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::Text)]
pub struct RouteDestination(pub external::RouteDestination);

impl RouteDestination {
    pub fn new(state: external::RouteDestination) -> Self {
        Self(state)
    }

    pub fn state(&self) -> &external::RouteDestination {
        &self.0
    }
}

impl ToSql<sql_types::Text, Pg> for RouteDestination {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
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
#[diesel(table_name = router_route)]
pub struct RouterRoute {
    #[diesel(embed)]
    identity: RouterRouteIdentity,

    pub kind: RouterRouteKind,
    pub vpc_router_id: Uuid,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

impl RouterRoute {
    pub fn new(
        route_id: Uuid,
        vpc_router_id: Uuid,
        kind: external::RouterRouteKind,
        params: external::RouterRouteCreateParams,
    ) -> Self {
        let identity = RouterRouteIdentity::new(route_id, params.identity);
        Self {
            identity,
            vpc_router_id,
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
            vpc_router_id: self.vpc_router_id,
            kind: self.kind.0,
            target: self.target.0.clone(),
            destination: self.destination.state().clone(),
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = router_route)]
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

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_status"))]
    pub struct VpcFirewallRuleStatusEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = VpcFirewallRuleStatusEnum)]
    pub struct VpcFirewallRuleStatus(pub external::VpcFirewallRuleStatus);

    Disabled => b"disabled"
    Enabled => b"enabled"
);
NewtypeFrom! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }
NewtypeDeref! { () pub struct VpcFirewallRuleStatus(external::VpcFirewallRuleStatus); }

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_direction"))]
    pub struct VpcFirewallRuleDirectionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = VpcFirewallRuleDirectionEnum)]
    pub struct VpcFirewallRuleDirection(pub external::VpcFirewallRuleDirection);

    Inbound => b"inbound"
    Outbound => b"outbound"
);
NewtypeFrom! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }
NewtypeDeref! { () pub struct VpcFirewallRuleDirection(external::VpcFirewallRuleDirection); }

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_action"))]
    pub struct VpcFirewallRuleActionEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = VpcFirewallRuleActionEnum)]
    pub struct VpcFirewallRuleAction(pub external::VpcFirewallRuleAction);

    Allow => b"allow"
    Deny => b"deny"
);
NewtypeFrom! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }
NewtypeDeref! { () pub struct VpcFirewallRuleAction(external::VpcFirewallRuleAction); }

impl_enum_wrapper!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vpc_firewall_rule_protocol"))]
    pub struct VpcFirewallRuleProtocolEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = VpcFirewallRuleProtocolEnum)]
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
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct VpcFirewallRuleTarget(pub external::VpcFirewallRuleTarget);
NewtypeFrom! { () pub struct VpcFirewallRuleTarget(external::VpcFirewallRuleTarget); }
NewtypeDeref! { () pub struct VpcFirewallRuleTarget(external::VpcFirewallRuleTarget); }

impl ToSql<sql_types::Text, Pg> for VpcFirewallRuleTarget {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct VpcFirewallRuleHostFilter(pub external::VpcFirewallRuleHostFilter);
NewtypeFrom! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }
NewtypeDeref! { () pub struct VpcFirewallRuleHostFilter(external::VpcFirewallRuleHostFilter); }

impl ToSql<sql_types::Text, Pg> for VpcFirewallRuleHostFilter {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct L4PortRange(pub external::L4PortRange);
NewtypeFrom! { () pub struct L4PortRange(external::L4PortRange); }
NewtypeDeref! { () pub struct L4PortRange(external::L4PortRange); }

impl ToSql<sql_types::Text, Pg> for L4PortRange {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
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
#[diesel(sql_type = sql_types::Int4)]
pub struct VpcFirewallRulePriority(pub external::VpcFirewallRulePriority);
NewtypeFrom! { () pub struct VpcFirewallRulePriority(external::VpcFirewallRulePriority); }
NewtypeDeref! { () pub struct VpcFirewallRulePriority(external::VpcFirewallRulePriority); }

impl ToSql<sql_types::Int4, Pg> for VpcFirewallRulePriority {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        SqlU16(self.0 .0).to_sql(&mut out.reborrow())
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
#[diesel(table_name = vpc_firewall_rule)]
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
        identity: external::IdentityMetadataCreateParams,
        ip: Option<std::net::IpAddr>,
    ) -> Result<Self, external::Error> {
        if let Some(ip) = ip {
            subnet.check_requestable_addr(ip)?;
        };
        let identity = NetworkInterfaceIdentity::new(interface_id, identity);

        Ok(Self { identity, instance_id, subnet, vpc_id, mac, ip })
    }
}

#[derive(Selectable, Queryable, Insertable, Clone, Debug, Resource)]
#[diesel(table_name = network_interface)]
pub struct NetworkInterface {
    #[diesel(embed)]
    pub identity: NetworkInterfaceIdentity,

    pub instance_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    // TODO-correctness: We need to split this into an optional V4 and optional V6 address, at
    // least one of which will always be specified.
    //
    // If user requests an address of either kind, give exactly that and not the other.
    // If neither is specified, auto-assign one of each?
    pub ip: ipnetwork::IpNetwork,
    pub slot: i16,
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
#[diesel(table_name = console_session)]
pub struct ConsoleSession {
    pub token: String,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
    pub silo_user_id: Uuid,
}

impl ConsoleSession {
    pub fn new(token: String, silo_user_id: Uuid) -> Self {
        let now = Utc::now();
        Self { token, silo_user_id, time_last_used: now, time_created: now }
    }

    pub fn id(&self) -> String {
        self.token.clone()
    }
}

/// Describes a built-in user, as stored in the database
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = user_builtin)]
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
#[diesel(table_name = role_builtin)]
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

    pub fn id(&self) -> (String, String) {
        (self.resource_type.clone(), self.role_name.clone())
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "identity_type"))]
    pub struct IdentityTypeEnum;

    #[derive(
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        PartialEq
    )]
    #[diesel(sql_type = IdentityTypeEnum)]
    pub enum IdentityType;

    // Enum values
    UserBuiltin => b"user_builtin"
    SiloUser => b"silo_user"
);

impl From<shared::IdentityType> for IdentityType {
    fn from(other: shared::IdentityType) -> Self {
        match other {
            shared::IdentityType::UserBuiltin => IdentityType::UserBuiltin,
            shared::IdentityType::SiloUser => IdentityType::SiloUser,
        }
    }
}

/// Describes an assignment of a built-in role for a user
#[derive(Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = role_assignment)]
pub struct RoleAssignment {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub resource_type: String,
    pub resource_id: Uuid,
    pub role_name: String,
}

impl RoleAssignment {
    /// Creates a new database RoleAssignment object.
    pub fn new(
        identity_type: IdentityType,
        identity_id: Uuid,
        resource_type: omicron_common::api::external::ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) -> Self {
        Self {
            identity_type,
            identity_id,
            resource_type: resource_type.to_string(),
            resource_id,
            role_name: String::from(role_name),
        }
    }
}

impl_enum_wrapper!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "update_artifact_kind"))]
    pub struct UpdateArtifactKindEnum;

    #[derive(Clone, Copy, Debug, Display, AsExpression, FromSqlRow, PartialEq, Eq)]
    #[display("{0}")]
    #[diesel(sql_type = UpdateArtifactKindEnum)]
    pub struct UpdateArtifactKind(pub internal::nexus::UpdateArtifactKind);

    // Enum values
    Zone => b"zone"
);

#[derive(
    Queryable, Insertable, Clone, Debug, Display, Selectable, AsChangeset,
)]
#[diesel(table_name = update_available_artifact)]
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

impl UpdateAvailableArtifact {
    pub fn id(&self) -> (String, i64, UpdateArtifactKind) {
        (self.name.clone(), self.version, self.kind)
    }
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
    fn test_vpc_subnet_check_requestable_addr() {
        let ipv4_block =
            Ipv4Net("192.168.0.0/16".parse::<Ipv4Network>().unwrap());
        let ipv6_block = Ipv6Net("fd00::/48".parse::<Ipv6Network>().unwrap());
        let identity = IdentityMetadataCreateParams {
            name: "net-test-vpc".parse().unwrap(),
            description: "A test VPC".parse().unwrap(),
        };
        let subnet = VpcSubnet::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            identity,
            ipv4_block,
            ipv6_block,
        );
        // Within subnet
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv4Addr::new(
                192, 168, 1, 10
            )))
            .is_ok());
        // Network address is reserved
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv4Addr::new(192, 168, 0, 0)))
            .is_err());
        // Broadcast address is reserved
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv4Addr::new(
                192, 168, 255, 255
            )))
            .is_err());
        // Within subnet, but reserved
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv4Addr::new(192, 168, 0, 1)))
            .is_err());
        // Not within subnet
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv4Addr::new(192, 160, 1, 1)))
            .is_err());

        // Within subnet
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv6Addr::new(
                0xfd00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10
            )))
            .is_ok());
        // Within subnet, but reserved
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv6Addr::new(
                0xfd00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
            )))
            .is_err());
        // Not within subnet
        assert!(subnet
            .check_requestable_addr(IpAddr::from(Ipv6Addr::new(
                0xfc00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10
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

    #[test]
    fn test_ip_subnet_check_requestable_address() {
        let subnet = super::Ipv4Net(Ipv4Net("192.168.0.0/16".parse().unwrap()));
        assert!(subnet.check_requestable_addr("192.168.0.10".parse().unwrap()));
        assert!(subnet.check_requestable_addr("192.168.1.0".parse().unwrap()));
        assert!(!subnet.check_requestable_addr("192.168.0.0".parse().unwrap()));
        assert!(subnet.check_requestable_addr("192.168.0.255".parse().unwrap()));
        assert!(
            !subnet.check_requestable_addr("192.168.255.255".parse().unwrap())
        );

        let subnet = super::Ipv6Net(Ipv6Net("fd00::/64".parse().unwrap()));
        assert!(subnet.check_requestable_addr("fd00::a".parse().unwrap()));
        assert!(!subnet.check_requestable_addr("fd00::1".parse().unwrap()));
        assert!(subnet.check_requestable_addr("fd00::1:1".parse().unwrap()));
    }
}
