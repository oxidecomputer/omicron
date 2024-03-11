// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures stored to the database.

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate newtype_derive;

mod address_lot;
mod bfd;
mod bgp;
mod block_size;
mod bootstore;
mod bytecount;
mod certificate;
mod collection;
mod console_session;
mod dataset;
mod dataset_kind;
mod db_metadata;
mod device_auth;
mod digest;
mod disk;
mod disk_state;
mod dns;
mod external_ip;
mod generation;
mod identity_provider;
mod image;
mod instance;
mod instance_cpu_count;
mod instance_state;
mod inventory;
mod ip_pool;
mod ipv4net;
pub mod ipv6;
mod ipv6net;
mod l4_port_range;
mod macaddr;
mod name;
mod network_interface;
mod oximeter_info;
mod physical_disk;
mod physical_disk_kind;
mod probe;
mod producer_endpoint;
mod project;
mod semver_version;
mod switch_interface;
mod switch_port;
// These actually represent subqueries, not real table.
// However, they must be defined in the same crate as our tables
// for join-based marker trait generation.
mod deployment;
mod ipv4_nat_entry;
mod omicron_zone_config;
pub mod queries;
mod quota;
mod rack;
mod region;
mod region_snapshot;
mod role_assignment;
mod role_builtin;
pub mod saga_types;
pub mod schema;
mod service;
mod service_kind;
mod silo;
mod silo_group;
mod silo_user;
mod silo_user_password_hash;
mod sled;
mod sled_instance;
mod sled_policy;
mod sled_resource;
mod sled_resource_kind;
mod sled_state;
mod sled_underlay_subnet_allocation;
mod snapshot;
mod ssh_key;
mod switch;
mod tuf_repo;
mod typed_uuid;
mod unsigned;
mod user_builtin;
mod utilization;
mod virtual_provisioning_collection;
mod virtual_provisioning_resource;
mod vmm;
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
pub use self::unsigned::*;
pub use address_lot::*;
pub use bfd::*;
pub use bgp::*;
pub use block_size::*;
pub use bootstore::*;
pub use bytecount::*;
pub use certificate::*;
pub use collection::*;
pub use console_session::*;
pub use dataset::*;
pub use dataset_kind::*;
pub use db_metadata::*;
pub use deployment::*;
pub use device_auth::*;
pub use digest::*;
pub use disk::*;
pub use disk_state::*;
pub use dns::*;
pub use external_ip::*;
pub use generation::*;
pub use identity_provider::*;
pub use image::*;
pub use instance::*;
pub use instance_cpu_count::*;
pub use instance_state::*;
pub use inventory::*;
pub use ip_pool::*;
pub use ipv4_nat_entry::*;
pub use ipv4net::*;
pub use ipv6::*;
pub use ipv6net::*;
pub use l4_port_range::*;
pub use name::*;
pub use network_interface::*;
pub use oximeter_info::*;
pub use physical_disk::*;
pub use physical_disk_kind::*;
pub use probe::*;
pub use producer_endpoint::*;
pub use project::*;
pub use quota::*;
pub use rack::*;
pub use region::*;
pub use region_snapshot::*;
pub use role_assignment::*;
pub use role_builtin::*;
pub use semver_version::*;
pub use service::*;
pub use service_kind::*;
pub use silo::*;
pub use silo_group::*;
pub use silo_user::*;
pub use silo_user_password_hash::*;
pub use sled::*;
pub use sled_instance::*;
pub use sled_policy::to_db_sled_policy; // Do not expose DbSledPolicy
pub use sled_resource::*;
pub use sled_resource_kind::*;
pub use sled_state::*;
pub use sled_underlay_subnet_allocation::*;
pub use snapshot::*;
pub use ssh_key::*;
pub use switch::*;
pub use switch_interface::*;
pub use switch_port::*;
pub use tuf_repo::*;
pub use typed_uuid::to_db_typed_uuid;
pub use user_builtin::*;
pub use utilization::*;
pub use virtual_provisioning_collection::*;
pub use virtual_provisioning_resource::*;
pub use vmm::*;
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
            fn from_sql(bytes: <::diesel::pg::Pg as ::diesel::backend::Backend>::RawValue<'_>) -> ::diesel::deserialize::Result<Self> {
                match <::diesel::pg::Pg as ::diesel::backend::Backend>::RawValue::<'_>::as_bytes(&bytes) {
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
            fn from_sql(bytes: <::diesel::pg::Pg as ::diesel::backend::Backend>::RawValue<'_>) -> ::diesel::deserialize::Result<Self> {
                match <::diesel::pg::Pg as ::diesel::backend::Backend>::RawValue::<'_>::as_bytes(&bytes) {
                    $(
                    $sql_value => {
                        Ok($model_type::$enum_item)
                    }
                    )*
                    other => {
                        let s = concat!("Unrecognized enum variant for ", stringify!{$model_type});
                        Err(format!("{}: (raw bytes: {:?})", s, other).into())
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

use anyhow::anyhow;
use nexus_types::external_api::shared::FleetRole;
use nexus_types::external_api::shared::ProjectRole;
use nexus_types::external_api::shared::SiloRole;

impl DatabaseString for FleetRole {
    type Error = anyhow::Error;

    fn to_database_string(&self) -> &str {
        match self {
            FleetRole::Admin => "admin",
            FleetRole::Collaborator => "collaborator",
            FleetRole::Viewer => "viewer",
        }
    }

    // WARNING: if you're considering changing this (including removing
    // variants), be sure you've considered how Nexus will handle rows written
    // previous to your change.
    fn from_database_string(s: &str) -> Result<Self, Self::Error> {
        match s {
            "admin" => Ok(FleetRole::Admin),
            "collaborator" => Ok(FleetRole::Collaborator),
            "viewer" => Ok(FleetRole::Viewer),
            _ => Err(anyhow!("unsupported Fleet role from database: {:?}", s)),
        }
    }
}

impl DatabaseString for SiloRole {
    type Error = anyhow::Error;

    fn to_database_string(&self) -> &str {
        match self {
            SiloRole::Admin => "admin",
            SiloRole::Collaborator => "collaborator",
            SiloRole::Viewer => "viewer",
        }
    }

    // WARNING: if you're considering changing this (including removing
    // variants), be sure you've considered how Nexus will handle rows written
    // previous to your change.
    fn from_database_string(s: &str) -> Result<Self, Self::Error> {
        match s {
            "admin" => Ok(SiloRole::Admin),
            "collaborator" => Ok(SiloRole::Collaborator),
            "viewer" => Ok(SiloRole::Viewer),
            _ => Err(anyhow!("unsupported Silo role from database: {:?}", s)),
        }
    }
}

impl DatabaseString for ProjectRole {
    type Error = anyhow::Error;

    fn to_database_string(&self) -> &str {
        match self {
            ProjectRole::Admin => "admin",
            ProjectRole::Collaborator => "collaborator",
            ProjectRole::Viewer => "viewer",
        }
    }

    // WARNING: if you're considering changing this (including removing
    // variants), be sure you've considered how Nexus will handle rows written
    // previous to your change.
    fn from_database_string(s: &str) -> Result<Self, Self::Error> {
        match s {
            "admin" => Ok(ProjectRole::Admin),
            "collaborator" => Ok(ProjectRole::Collaborator),
            "viewer" => Ok(ProjectRole::Viewer),
            _ => {
                Err(anyhow!("unsupported Project role from database: {:?}", s))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RequestAddressError;

    use super::VpcSubnet;
    use ipnetwork::Ipv4Network;
    use ipnetwork::Ipv6Network;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::IpNet;
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
        subnet.check_requestable_addr("192.168.0.10".parse().unwrap()).unwrap();
        subnet.check_requestable_addr("192.168.1.0".parse().unwrap()).unwrap();
        let addr = "192.178.0.10".parse().unwrap();
        assert_eq!(
            subnet.check_requestable_addr(addr),
            Err(RequestAddressError::OutsideSubnet(
                addr.into(),
                IpNet::from(subnet.0).into()
            ))
        );
        assert_eq!(
            subnet.check_requestable_addr("192.168.0.0".parse().unwrap()),
            Err(RequestAddressError::Reserved)
        );

        subnet
            .check_requestable_addr("192.168.0.255".parse().unwrap())
            .unwrap();

        assert_eq!(
            subnet.check_requestable_addr("192.168.255.255".parse().unwrap()),
            Err(RequestAddressError::Broadcast)
        );

        let subnet = super::Ipv6Net(Ipv6Net("fd00::/64".parse().unwrap()));
        subnet.check_requestable_addr("fd00::a".parse().unwrap()).unwrap();
        assert_eq!(
            subnet.check_requestable_addr("fd00::1".parse().unwrap()),
            Err(RequestAddressError::Reserved)
        );
        subnet.check_requestable_addr("fd00::1:1".parse().unwrap()).unwrap();
    }

    /// Does some basic smoke checks on an impl of `DatabaseString`
    ///
    /// This tests:
    ///
    /// - that for every variant, if we serialize it and deserialize the result,
    ///   we get back the original variant
    /// - that if we attempt to deserialize some _other_ input, we get back an
    ///   error
    /// - that the serialized form for each variant matches what's found in
    ///   `expected_output_file`.  This output file is generated by this test
    ///   using expectorate.
    ///
    /// This cannot completely test the correctness of the implementation, but it
    /// can catch some basic copy/paste errors and accidental compatibility
    /// breakage.
    #[cfg(test)]
    pub fn test_database_string_impl<T, P>(expected_output_file: P)
    where
        T: std::fmt::Debug
            + PartialEq
            + super::DatabaseString
            + strum::IntoEnumIterator,
        P: std::convert::AsRef<std::path::Path>,
    {
        let mut output = String::new();
        let mut maxlen: Option<usize> = None;

        for variant in T::iter() {
            // Serialize the variant.  Verify that we can deserialize the thing
            // we just got back.
            let serialized = variant.to_database_string();
            let deserialized = T::from_database_string(serialized)
                .unwrap_or_else(|_| {
                    panic!(
                        "failed to deserialize the string {:?}, which we \
                    got by serializing {:?}",
                        serialized, variant
                    )
                });
            assert_eq!(variant, deserialized);

            // Put the serialized form into "output".  At the end, we'll compare
            // this to the expected output.  This will fail if somebody has
            // incompatibly changed things.
            output.push_str(&format!(
                "variant {:?}: serialized form = {}\n",
                variant, serialized
            ));

            // Keep track of the maximum length of the serialized strings.
            // We'll use this to construct an input to `from_database_string()`
            // that was not emitted by any of the variants'
            // `to_database_string()` functions.
            maxlen = maxlen.max(Some(serialized.len()));
        }

        // Check that `from_database_string()` fails when given input that
        // doesn't match any of the variants' serialized forms.  We construct
        // this input by providing a string that's longer than all strings
        // emitted by `to_database_string()`.
        if let Some(maxlen) = maxlen {
            let input = String::from_utf8(vec![b'-'; maxlen + 1]).unwrap();
            T::from_database_string(&input)
                .expect_err("expected failure to deserialize unknown string");
        }

        expectorate::assert_contents(expected_output_file, &output);
    }

    #[test]
    fn test_roles_database_strings() {
        test_database_string_impl::<super::FleetRole, _>(
            "tests/output/authz-roles-fleet.txt",
        );
        test_database_string_impl::<super::SiloRole, _>(
            "tests/output/authz-roles-silo.txt",
        );
        test_database_string_impl::<super::ProjectRole, _>(
            "tests/output/authz-roles-project.txt",
        );
    }
}
