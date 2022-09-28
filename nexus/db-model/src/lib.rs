// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures stored to the database.

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate newtype_derive;

mod block_size;
mod bytecount;
mod collection;
mod console_session;
mod dataset;
mod dataset_kind;
mod device_auth;
mod digest;
mod disk;
mod disk_state;
mod external_ip;
mod generation;
mod global_image;
mod identity_provider;
mod image;
mod instance;
mod instance_cpu_count;
mod instance_state;
mod ip_pool;
mod ipv4net;
mod ipv6;
mod ipv6net;
mod l4_port_range;
mod macaddr;
mod name;
mod network_interface;
mod organization;
mod oximeter_info;
mod producer_endpoint;
mod project;
// These actually represent subqueries, not real table.
// However, they must be defined in the same crate as our tables
// for join-based marker trait generation.
pub mod queries;
mod rack;
mod region;
mod role_assignment;
mod role_builtin;
pub mod saga_types;
pub mod schema;
mod service;
mod service_kind;
mod silo;
mod silo_group;
mod silo_user;
mod sled;
mod snapshot;
mod ssh_key;
mod u16;
mod update_artifact;
mod user_builtin;
mod vni;
mod volume;
mod vpc;
mod vpc_firewall_rule;
mod vpc_route;
mod vpc_router;
mod vpc_subnet;
mod zpool;

// This module namespacing is a quirk to allow `db-macros` to refer to
// `crate::db::model::Name` in both this crate and `nexus` proper.
mod db {
    pub(crate) mod model {
        pub(crate) use crate::Name;
    }
}

pub use self::macaddr::*;
pub use self::u16::*;
pub use block_size::*;
pub use bytecount::*;
pub use collection::*;
pub use console_session::*;
pub use dataset::*;
pub use dataset_kind::*;
pub use device_auth::*;
pub use digest::*;
pub use disk::*;
pub use disk_state::*;
pub use external_ip::*;
pub use generation::*;
pub use global_image::*;
pub use identity_provider::*;
pub use image::*;
pub use instance::*;
pub use instance_cpu_count::*;
pub use instance_state::*;
pub use ip_pool::*;
pub use ipv4net::*;
pub use ipv6::*;
pub use ipv6net::*;
pub use l4_port_range::*;
pub use name::*;
pub use network_interface::*;
pub use organization::*;
pub use oximeter_info::*;
pub use producer_endpoint::*;
pub use project::*;
pub use rack::*;
pub use region::*;
pub use role_assignment::*;
pub use role_builtin::*;
pub use service::*;
pub use service_kind::*;
pub use silo::*;
pub use silo_group::*;
pub use silo_user::*;
pub use sled::*;
pub use snapshot::*;
pub use ssh_key::*;
pub use update_artifact::*;
pub use user_builtin::*;
pub use vni::*;
pub use volume::*;
pub use vpc::*;
pub use vpc_firewall_rule::*;
pub use vpc_route::*;
pub use vpc_router::*;
pub use vpc_subnet::*;
pub use zpool::*;

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

        impl ::diesel::serialize::ToSql<$diesel_type, ::diesel::pg::Pg> for $model_type {
            fn to_sql<'a>(
                &'a self,
                out: &mut ::diesel::serialize::Output<'a, '_, ::diesel::pg::Pg>,
            ) -> ::diesel::serialize::Result {
                match self.0 {
                    $(
                    <$ext_type>::$enum_item => {
                        out.write_all($sql_value)?
                    }
                    )*
                }
                Ok(::diesel::serialize::IsNull::No)
            }
        }

        impl ::diesel::deserialize::FromSql<$diesel_type, ::diesel::pg::Pg> for $model_type {
            fn from_sql(bytes: ::diesel::backend::RawValue<::diesel::pg::Pg>) -> ::diesel::deserialize::Result<Self> {
                match ::diesel::backend::RawValue::<::diesel::pg::Pg>::as_bytes(&bytes) {
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

pub(crate) use impl_enum_wrapper;

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

        impl ::diesel::serialize::ToSql<$diesel_type, ::diesel::pg::Pg> for $model_type {
            fn to_sql<'a>(
                &'a self,
                out: &mut ::diesel::serialize::Output<'a, '_, ::diesel::pg::Pg>,
            ) -> ::diesel::serialize::Result {
                use ::std::io::Write;
                match self {
                    $(
                    $model_type::$enum_item => {
                        out.write_all($sql_value)?
                    }
                    )*
                }
                Ok(::diesel::serialize::IsNull::No)
            }
        }

        impl ::diesel::deserialize::FromSql<$diesel_type, ::diesel::pg::Pg> for $model_type {
            fn from_sql(bytes: ::diesel::backend::RawValue<::diesel::pg::Pg>) -> ::diesel::deserialize::Result<Self> {
                match ::diesel::backend::RawValue::<::diesel::pg::Pg>::as_bytes(&bytes) {
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

pub(crate) use impl_enum_type;

/// Describes a type that's represented in the database using a String
///
/// If you're reaching for this type, consider whether it'd be better to use an
/// enum in the database, along with `impl_enum_wrapper!` to define a
/// corresponding Rust type.  This trait is really intended for cases where
/// that's not possible (e.g., because the set of allowed values isn't the same
/// for all rows in the table).
///
/// For a given type, the impl of `DatabaseString` is potentially different than
/// the impl of `Serialize` (which is usually how the type appears in an API)
/// and the impl of `Display` (if the type even has one).  We could piggy-back
/// on `Display`, but it'd be a footgun in that someone might change `Display`
/// thinking it wouldn't have much impact, not realizing that it'd be breaking
/// an on-disk interface.
pub trait DatabaseString: Sized {
    type Error: std::fmt::Display;

    fn to_database_string(&self) -> &str;
    fn from_database_string(s: &str) -> Result<Self, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::VpcSubnet;
    use ipnetwork::Ipv4Network;
    use ipnetwork::Ipv6Network;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Ipv4Net;
    use omicron_common::api::external::Ipv6Net;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use uuid::Uuid;

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
