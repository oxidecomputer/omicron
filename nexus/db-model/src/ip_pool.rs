// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for IP Pools and the CIDR blocks therein.

use crate::Name;
use crate::SqlU16;
use crate::collection::DatastoreCollectionConfig;
use crate::impl_enum_type;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::Selectable;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::ip_pool;
use nexus_db_schema::schema::ip_pool_range;
use nexus_db_schema::schema::ip_pool_resource;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::vlan::VlanID;
use std::net::IpAddr;
use uuid::Uuid;

impl_enum_type!(
    IpVersionEnum:

    #[derive(
        AsExpression,
        Clone,
        Copy,
        Debug,
        serde::Deserialize,
        Eq,
        FromSqlRow,
        schemars::JsonSchema,
        PartialEq,
        serde::Serialize,
    )]
    pub enum IpVersion;

    V4 => b"v4"
    V6 => b"v6"
);

impl ::std::fmt::Display for IpVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IpVersion::V4 => "v4",
            IpVersion::V6 => "v6",
        })
    }
}

impl From<shared::IpVersion> for IpVersion {
    fn from(value: shared::IpVersion) -> Self {
        match value {
            shared::IpVersion::V4 => Self::V4,
            shared::IpVersion::V6 => Self::V6,
        }
    }
}

impl From<IpVersion> for shared::IpVersion {
    fn from(value: IpVersion) -> Self {
        match value {
            IpVersion::V4 => Self::V4,
            IpVersion::V6 => Self::V6,
        }
    }
}

impl From<shared::IpPoolType> for IpPoolType {
    fn from(value: shared::IpPoolType) -> Self {
        match value {
            shared::IpPoolType::Unicast => Self::Unicast,
            shared::IpPoolType::Multicast => Self::Multicast,
        }
    }
}

impl From<IpPoolType> for shared::IpPoolType {
    fn from(value: IpPoolType) -> Self {
        match value {
            IpPoolType::Unicast => Self::Unicast,
            IpPoolType::Multicast => Self::Multicast,
        }
    }
}

/// An IP Pool is a collection of IP addresses external to the rack.
///
/// IP pools can be external or internal. External IP pools can be associated
/// with a silo or project so that instance IP allocation draws from that pool
/// instead of a system pool.
#[derive(Queryable, Insertable, Selectable, Clone, Debug, Resource)]
#[diesel(table_name = ip_pool)]
pub struct IpPool {
    #[diesel(embed)]
    pub identity: IpPoolIdentity,
    /// The IP version of the pool.
    pub ip_version: IpVersion,
    /// Pool type for unicast (default) vs multicast pools.
    pub pool_type: IpPoolType,
    /// Switch port uplinks for multicast pools (array of switch port UUIDs).
    /// Only applies to multicast pools, None for unicast pools.
    pub switch_port_uplinks: Option<Vec<Uuid>>,
    /// MVLAN ID for multicast pools.
    /// Only applies to multicast pools, None for unicast pools.
    pub mvlan: Option<SqlU16>,
    /// Child resource generation number, for optimistic concurrency control of
    /// the contained ranges.
    pub rcgen: i64,
}

impl IpPool {
    /// Creates a new unicast (default) IP pool.
    pub fn new(
        pool_identity: &external::IdentityMetadataCreateParams,
        ip_version: IpVersion,
    ) -> Self {
        Self {
            identity: IpPoolIdentity::new(
                Uuid::new_v4(),
                pool_identity.clone(),
            ),
            ip_version,
            pool_type: IpPoolType::Unicast,
            switch_port_uplinks: None,
            mvlan: None,
            rcgen: 0,
        }
    }

    /// Creates a new multicast IP pool.
    pub fn new_multicast(
        pool_identity: &external::IdentityMetadataCreateParams,
        ip_version: IpVersion,
        switch_port_uplinks: Option<Vec<Uuid>>,
        mvlan: Option<VlanID>,
    ) -> Self {
        Self {
            identity: IpPoolIdentity::new(
                Uuid::new_v4(),
                pool_identity.clone(),
            ),
            ip_version,
            pool_type: IpPoolType::Multicast,
            switch_port_uplinks,
            mvlan: mvlan.map(|vid| u16::from(vid).into()),
            rcgen: 0,
        }
    }

    pub fn new_v4(
        pool_identity: &external::IdentityMetadataCreateParams,
    ) -> Self {
        Self::new(pool_identity, IpVersion::V4)
    }

    pub fn new_v6(
        pool_identity: &external::IdentityMetadataCreateParams,
    ) -> Self {
        Self::new(pool_identity, IpVersion::V6)
    }
}

impl From<IpPool> for views::IpPool {
    fn from(pool: IpPool) -> Self {
        let identity = pool.identity();
        let pool_type = pool.pool_type;

        // Note: UUIDs expected to be converted to "switch.port" format in app
        // layer, upon retrieval.
        let switch_port_uplinks = match pool.switch_port_uplinks {
            Some(uuid_list) => Some(
                uuid_list.into_iter().map(|uuid| uuid.to_string()).collect(),
            ),
            None => None,
        };

        let mvlan = pool.mvlan.map(|vlan| vlan.into());

        Self {
            identity,
            pool_type: pool_type.into(),
            ip_version: pool.ip_version.into(),
            switch_port_uplinks,
            mvlan,
        }
    }
}

/// A set of updates to an IP Pool.
///
/// We do not modify the pool type after creation (e.g. unicast -> multicast or
/// vice versa), as that would require a migration of all associated resources.
#[derive(AsChangeset)]
#[diesel(table_name = ip_pool)]
pub struct IpPoolUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    /// Switch port uplinks for multicast pools (array of switch port UUIDs),
    /// used for multicast traffic outbound from the rack to external networks.
    pub switch_port_uplinks: Option<Vec<Uuid>>,
    /// MVLAN ID for multicast pools.
    pub mvlan: Option<SqlU16>,
    pub time_modified: DateTime<Utc>,
}

// Used for unicast updates.
impl From<params::IpPoolUpdate> for IpPoolUpdate {
    fn from(params: params::IpPoolUpdate) -> Self {
        Self {
            name: params.identity.name.map(|n| n.into()),
            description: params.identity.description,
            switch_port_uplinks: None, // no change
            mvlan: None,               // no change
            time_modified: Utc::now(),
        }
    }
}

impl_enum_type!(
    IpPoolResourceTypeEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum IpPoolResourceType;

    Silo => b"silo"
);

impl_enum_type!(
    IpPoolTypeEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum IpPoolType;

    Unicast => b"unicast"
    Multicast => b"multicast"
);

impl std::fmt::Display for IpPoolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IpPoolType::Unicast => write!(f, "unicast"),
            IpPoolType::Multicast => write!(f, "multicast"),
        }
    }
}

#[derive(Queryable, Insertable, Selectable, Clone, Copy, Debug, PartialEq)]
#[diesel(table_name = ip_pool_resource)]
pub struct IpPoolResource {
    pub ip_pool_id: Uuid,
    pub resource_type: IpPoolResourceType,
    pub resource_id: Uuid,
    pub is_default: bool,
}

impl From<IpPoolResource> for views::IpPoolSiloLink {
    fn from(assoc: IpPoolResource) -> Self {
        Self {
            ip_pool_id: assoc.ip_pool_id,
            silo_id: assoc.resource_id,
            is_default: assoc.is_default,
        }
    }
}

/// A range of IP addresses for an IP Pool.
#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = ip_pool_range)]
pub struct IpPoolRange {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    /// First (lowest) address in the range, inclusive.
    pub first_address: IpNetwork,
    /// Last (highest) address in the range, inclusive.
    pub last_address: IpNetwork,
    /// Foreign-key to the `ip_pool` table with the parent pool for this range
    pub ip_pool_id: Uuid,
    /// The child resource generation number, tracking IP addresses allocated or
    /// used from this range.
    pub rcgen: i64,
}

impl IpPoolRange {
    pub fn new(range: &shared::IpRange, ip_pool_id: Uuid) -> Self {
        let now = Utc::now();
        let first_address = range.first_address();
        let last_address = range.last_address();
        // `range` has already been validated to have first address no greater
        // than last address.
        assert!(
            last_address >= first_address,
            "Address ranges must be non-decreasing"
        );
        Self {
            id: Uuid::new_v4(),
            time_created: now,
            time_modified: now,
            time_deleted: None,
            first_address: IpNetwork::from(range.first_address()),
            last_address: IpNetwork::from(range.last_address()),
            ip_pool_id,
            rcgen: 0,
        }
    }
}

impl From<IpPoolRange> for views::IpPoolRange {
    fn from(range: IpPoolRange) -> Self {
        Self {
            id: range.id,
            ip_pool_id: range.ip_pool_id,
            time_created: range.time_created,
            range: shared::IpRange::from(&range),
        }
    }
}

impl From<&IpPoolRange> for shared::IpRange {
    fn from(range: &IpPoolRange) -> Self {
        let maybe_range =
            match (range.first_address.ip(), range.last_address.ip()) {
                (IpAddr::V4(first), IpAddr::V4(last)) => {
                    shared::IpRange::try_from((first, last))
                }
                (IpAddr::V6(first), IpAddr::V6(last)) => {
                    shared::IpRange::try_from((first, last))
                }
                (first, last) => {
                    unreachable!(
                        "Expected first/last address of an IP range to \
                    both be of the same protocol version, but first = {:?} \
                    and last = {:?}",
                        first, last,
                    );
                }
            };
        maybe_range
            .expect("Retrieved an out-of-order IP range pair from the database")
    }
}

impl DatastoreCollectionConfig<IpPoolRange> for IpPool {
    type CollectionId = uuid::Uuid;
    type GenerationNumberColumn = ip_pool::dsl::rcgen;
    type CollectionTimeDeletedColumn = ip_pool::dsl::time_deleted;
    type CollectionIdColumn = ip_pool_range::dsl::ip_pool_id;
}
