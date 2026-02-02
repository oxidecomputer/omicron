// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for Subnet Pools and Members and External Subnets.

use crate::DbTypedUuid;
use crate::Generation;
use crate::IpAttachState;
use crate::IpNet;
use crate::IpVersion;
use crate::Name;
use crate::SqlU8;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::AsChangeset;
use diesel::Insertable;
use diesel::Queryable;
use diesel::Selectable;
use nexus_db_schema::schema::external_subnet;
use nexus_db_schema::schema::subnet_pool;
use nexus_db_schema::schema::subnet_pool_member;
use nexus_db_schema::schema::subnet_pool_silo_link;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceKind;
use omicron_uuid_kinds::SubnetPoolKind;
use omicron_uuid_kinds::SubnetPoolMemberKind;
use omicron_uuid_kinds::SubnetPoolMemberUuid;
use omicron_uuid_kinds::SubnetPoolUuid;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// A pool of subnets that can be attached to instances.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Insertable,
    PartialEq,
    Queryable,
    Resource,
    Selectable,
    Serialize,
)]
#[diesel(table_name = subnet_pool)]
#[resource(uuid_kind = SubnetPoolKind)]
pub struct SubnetPool {
    #[diesel(embed)]
    pub identity: SubnetPoolIdentity,
    pub rcgen: Generation,
    pub ip_version: IpVersion,
}

impl SubnetPool {
    /// Create a new Subnet Pool.
    pub fn new(
        identity: external::IdentityMetadataCreateParams,
        ip_version: IpVersion,
    ) -> Self {
        Self {
            identity: SubnetPoolIdentity::new(
                SubnetPoolUuid::new_v4(),
                identity,
            ),
            rcgen: Generation::new(),
            ip_version,
        }
    }
}

impl From<SubnetPool> for views::SubnetPool {
    fn from(value: SubnetPool) -> Self {
        Self { identity: value.identity(), ip_version: value.ip_version.into() }
    }
}

#[derive(AsChangeset, Clone, Debug)]
#[diesel(table_name = subnet_pool)]
pub struct SubnetPoolUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::SubnetPoolUpdate> for SubnetPoolUpdate {
    fn from(value: params::SubnetPoolUpdate) -> Self {
        Self {
            name: value.identity.name.map(Into::into),
            description: value.identity.description,
            time_modified: Utc::now(),
        }
    }
}

/// A member of a Subnet Pool.
#[derive(
    Clone, Debug, Deserialize, PartialEq, Selectable, Serialize, Queryable,
)]
#[diesel(table_name = subnet_pool_member)]
pub struct SubnetPoolMember {
    pub id: DbTypedUuid<SubnetPoolMemberKind>,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub subnet_pool_id: DbTypedUuid<SubnetPoolKind>,
    pub subnet: IpNet,
    min_prefix_length: SqlU8,
    max_prefix_length: SqlU8,
    rcgen: Generation,
}

impl From<SubnetPoolMember> for views::SubnetPoolMember {
    fn from(value: SubnetPoolMember) -> Self {
        Self {
            id: value.id.into_untyped_uuid(),
            time_created: value.time_created,
            subnet_pool_id: value.subnet_pool_id.into_untyped_uuid(),
            subnet: value.subnet.into(),
            min_prefix_length: value.min_prefix_length.into(),
            max_prefix_length: value.max_prefix_length.into(),
        }
    }
}

impl SubnetPoolMember {
    pub fn new(
        params: &params::SubnetPoolMemberAdd,
        pool_id: SubnetPoolUuid,
    ) -> Result<Self, Error> {
        // Require that the subnet is actually a network, i.e.,
        // "192.168.0.0/24", rather than "192.168.0.100/24".
        if !params.subnet.is_network_address() {
            return Err(Error::invalid_request(
                "IP subnet must be a network address, with a host \
                ID of zero.",
            ));
        }
        let (version, max_for_version) = if params.subnet.is_ipv4() {
            (IpVersion::V4, std::net::Ipv4Addr::BITS)
        } else {
            // NOTE: Depending on which RFC you read or what the context is, a
            // /64 is the smallest IPv6 subnet you can create. Still, we don't
            // really control how this is used, the guest does, so we should try
            // to be permissive here.
            (IpVersion::V6, std::net::Ipv6Addr::BITS)
        };
        let min_prefix_length =
            params.min_prefix_length.unwrap_or_else(|| params.subnet.width());
        let max_prefix_length = params
            .max_prefix_length
            .unwrap_or_else(|| u8::try_from(max_for_version).unwrap());

        // Sanity checks on the prefix lengths.
        //
        // - Min has to be <= max.
        // - Min and max <= 32 or 64, depending on IP version.
        // - Min has to be >= subnet prefix itself.
        // - Max has to be >= subnet prefix itself.
        if min_prefix_length > max_prefix_length {
            return Err(Error::invalid_request(
                "The minimum prefix length must be no greater than the maximum",
            ));
        }
        if u32::from(max_prefix_length) > max_for_version {
            return Err(Error::invalid_request(&format!(
                "Cannot create an IP{} subnet with a max prefix length \
                greater than {}",
                version, max_for_version,
            )));
        };
        if min_prefix_length < params.subnet.width()
            || max_prefix_length < params.subnet.width()
        {
            return Err(Error::invalid_request(&format!(
                "Min and max prefix length must be no less than \
                the IP subnet prefix length (/{})",
                params.subnet.width(),
            )));
        }
        let now = Utc::now();
        Ok(Self {
            id: DbTypedUuid(SubnetPoolMemberUuid::new_v4()),
            time_created: now,
            time_modified: now,
            time_deleted: None,
            subnet_pool_id: pool_id.into(),
            subnet: params.subnet.into(),
            min_prefix_length: SqlU8::new(min_prefix_length),
            max_prefix_length: SqlU8::new(max_prefix_length),
            rcgen: Generation::new(),
        })
    }

    pub fn rcgen(&self) -> Generation {
        self.rcgen
    }

    pub fn min_prefix_length(&self) -> SqlU8 {
        self.min_prefix_length
    }

    pub fn max_prefix_length(&self) -> SqlU8 {
        self.max_prefix_length
    }
}

/// A Project-scoped external subnet, which may be attached to an instance.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    PartialEq,
    Queryable,
    Resource,
    Selectable,
    Serialize,
)]
#[diesel(table_name = external_subnet)]
#[resource(uuid_kind = ExternalSubnetKind)]
pub struct ExternalSubnet {
    #[diesel(embed)]
    pub identity: ExternalSubnetIdentity,
    pub subnet_pool_id: DbTypedUuid<SubnetPoolKind>,
    pub subnet_pool_member_id: DbTypedUuid<SubnetPoolMemberKind>,
    pub project_id: Uuid,
    pub subnet: IpNet,
    pub attach_state: IpAttachState,
    pub instance_id: Option<DbTypedUuid<InstanceKind>>,
}

impl From<ExternalSubnet> for views::ExternalSubnet {
    fn from(value: ExternalSubnet) -> Self {
        Self {
            identity: value.identity(),
            subnet: value.subnet.into(),
            project_id: value.project_id,
            subnet_pool_id: value.subnet_pool_id.into_untyped_uuid(),
            subnet_pool_member_id: value
                .subnet_pool_member_id
                .into_untyped_uuid(),
            instance_id: value.instance_id.map(|id| id.into_untyped_uuid()),
        }
    }
}

#[derive(AsChangeset, Clone, Debug)]
#[diesel(table_name = external_subnet)]
pub struct ExternalSubnetUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::ExternalSubnetUpdate> for ExternalSubnetUpdate {
    fn from(value: params::ExternalSubnetUpdate) -> Self {
        Self {
            name: value.identity.name.map(Into::into),
            description: value.identity.description,
            time_modified: Utc::now(),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, PartialEq, Queryable, Selectable, Serialize,
)]
#[diesel(table_name = subnet_pool_silo_link)]
pub struct SubnetPoolSiloLink {
    pub subnet_pool_id: DbTypedUuid<SubnetPoolKind>,
    pub silo_id: Uuid,
    pub ip_version: IpVersion,
    pub is_default: bool,
}

impl From<SubnetPoolSiloLink> for views::SubnetPoolSiloLink {
    fn from(value: SubnetPoolSiloLink) -> Self {
        Self {
            subnet_pool_id: value.subnet_pool_id.into_untyped_uuid(),
            silo_id: value.silo_id.into_untyped_uuid(),
            is_default: value.is_default,
        }
    }
}
