// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for IP Pools and the CIDR blocks therein.

use crate::collection::DatastoreCollectionConfig;
use crate::schema::ip_pool;
use crate::schema::ip_pool_range;
use crate::Name;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::Selectable;
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use std::net::IpAddr;
use uuid::Uuid;

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

    /// Child resource generation number, for optimistic concurrency control of
    /// the contained ranges.
    pub rcgen: i64,

    /// Silo, if IP pool is associated with a particular silo. One special use
    /// for this is  associating a pool with the internal silo oxide-internal,
    /// which is used for internal services. If there is no silo ID, the
    /// pool is considered a fleet-wide pool and will be used for allocating
    /// instance IPs in silos that don't have their own pool.
    pub silo_id: Option<Uuid>,

    pub is_default: bool,
}

impl IpPool {
    pub fn new(
        pool_identity: &external::IdentityMetadataCreateParams,
        silo_id: Option<Uuid>,
        is_default: bool,
    ) -> Self {
        Self {
            identity: IpPoolIdentity::new(
                Uuid::new_v4(),
                pool_identity.clone(),
            ),
            rcgen: 0,
            silo_id,
            is_default,
        }
    }
}

impl From<IpPool> for views::IpPool {
    fn from(pool: IpPool) -> Self {
        Self {
            identity: pool.identity(),
            silo_id: pool.silo_id,
            is_default: pool.is_default,
        }
    }
}

/// A set of updates to an IP Pool
#[derive(AsChangeset)]
#[diesel(table_name = ip_pool)]
pub struct IpPoolUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::IpPoolUpdate> for IpPoolUpdate {
    fn from(params: params::IpPoolUpdate) -> Self {
        Self {
            name: params.identity.name.map(|n| n.into()),
            description: params.identity.description,
            time_modified: Utc::now(),
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
    pub fn new(range: &IpRange, ip_pool_id: Uuid) -> Self {
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
            range: IpRange::from(&range),
        }
    }
}

impl From<&IpPoolRange> for IpRange {
    fn from(range: &IpPoolRange) -> Self {
        let maybe_range =
            match (range.first_address.ip(), range.last_address.ip()) {
                (IpAddr::V4(first), IpAddr::V4(last)) => {
                    IpRange::try_from((first, last))
                }
                (IpAddr::V6(first), IpAddr::V6(last)) => {
                    IpRange::try_from((first, last))
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
