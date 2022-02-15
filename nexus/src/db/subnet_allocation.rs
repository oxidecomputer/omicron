// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diesel queries used for subnet and IP allocation

use crate::db;
use crate::db::identity::Resource;
use crate::db::model::IncompleteNetworkInterface;
use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types;
use omicron_common::api::external;
use std::convert::TryFrom;
use uuid::Uuid;

/// Used for allocating an IP as part of [`NetworkInterface`] construction.
///
/// This is a query equivalent to:
/// SELECT <id> AS id, <name> AS name, <description> AS description,
///        <time_created> AS time_created, <time_modified> AS time_modified,
///        <instance_id> AS instance_id, <vpc_id> AS vpc_id,
///        <subnet_id> AS subnet_id, <mac> AS mac, <block_base> + off AS ip
///   FROM
///        generate_series(5, <last_address_in_block>) AS off
///   LEFT OUTER JOIN
///        network_interface
///   ON (subnet_id, ip, time_deleted IS NULL) =
///      (<subnet_id>, <block_base> + off, TRUE)
///   WHERE ip IS NULL LIMIT 1;
///
/// Note that generate_series receives a start value of 5 in accordance with
/// RFD 21's reservation of addresses 0 through 4 in a subnet. See
/// <https://rfd.shared.oxide.computer/rfd/0021#concept-subnet>, for details.
// TODO-performance: This query scales linearly with the number of IPs
// allocated, which is highly undesirable. It will also return the same
// candidate address to two parallel executors, which will cause additional
// retries.
pub struct AllocateIpQuery {
    pub interface: IncompleteNetworkInterface,
    pub block: ipnetwork::IpNetwork,
    pub now: DateTime<Utc>,
}

/// Used for using AllocateIpQuery with an INSERT statement. Do not use this
/// directly, instead pass an instance of [`AllocateIpQuery`] to
/// [`InsertStatement::values`].
pub struct AllocateIpQueryValues(AllocateIpQuery);

impl QueryId for AllocateIpQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Insertable<db::schema::network_interface::table> for AllocateIpQuery {
    type Values = AllocateIpQueryValues;

    fn values(self) -> Self::Values {
        AllocateIpQueryValues(self)
    }
}

impl QueryFragment<Pg> for AllocateIpQuery {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> diesel::QueryResult<()> {
        use db::schema::network_interface::dsl;

        // Generate last address in the range.
        //
        // NOTE: First subtraction is to convert from the subnet size to an
        // offset, since `generate_series` is inclusive of the last value.
        // Example: 256 -> 255.
        let last_address_offset = match self.block {
            ipnetwork::IpNetwork::V4(network) => network.size() as i64 - 1,
            ipnetwork::IpNetwork::V6(network) => {
                // If we're allocating from a v6 subnet with more than 2^63 - 1
                // addresses, just cap the size we'll explore.  This will never
                // fail in practice since we're never going to be storing 2^64
                // rows in the network_interface table.
                i64::try_from(network.size() - 1).unwrap_or(i64::MAX)
            }
        };

        out.push_sql("SELECT ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.identity.id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, String>(
            &self.interface.identity.name.to_string(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, String>(
            &self.interface.identity.description,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Timestamptz, _>(&self.now)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Timestamptz, _>(&self.now)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.instance_id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::instance_id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.interface.vpc_id)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.subnet.id(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::subnet_id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, String>(
            &self.interface.mac.to_string(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::mac::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &self.block.network().into(),
        )?;
        out.push_sql(" + ");
        out.push_identifier("off")?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::ip::NAME)?;

        // Skip the initial reserved addresses and the broadcast address.
        out.push_sql(" FROM generate_series(5, ");
        out.push_bind_param::<sql_types::BigInt, _>(
            &(last_address_offset - 1),
        )?;
        out.push_sql(") AS ");
        out.push_identifier("off")?;
        out.push_sql(" LEFT OUTER JOIN ");
        dsl::network_interface.from_clause().walk_ast(out.reborrow())?;

        //   ON (subnet_id, ip, time_deleted IS NULL) =
        //      (<subnet_id>, <subnet_base> + off, TRUE)
        out.push_sql(" ON (");
        out.push_identifier(dsl::subnet_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL) = (");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.subnet.id(),
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &self.block.network().into(),
        )?;
        out.push_sql(" + ");
        out.push_identifier("off")?;
        out.push_sql(", TRUE) ");
        //   WHERE ip IS NULL LIMIT 1;
        out.push_sql("WHERE ");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(" IS NULL LIMIT 1");
        Ok(())
    }
}

impl QueryId for AllocateIpQueryValues {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::insertable::CanInsertInSingleQuery<Pg> for AllocateIpQueryValues {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(1)
    }
}

impl QueryFragment<Pg> for AllocateIpQueryValues {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> diesel::QueryResult<()> {
        use db::schema::network_interface::dsl;
        out.push_sql("(");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::instance_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::subnet_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::mac::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(") ");
        self.0.walk_ast(out)
    }
}

/// Errors related to allocating VPC Subnets.
#[derive(Debug)]
pub enum SubnetError {
    OverlappingIpRange,
    External(external::Error),
}

/// Generate a CTE that can be used to insert a VPC Subnet, only if the IP
/// address ranges of that subnet don't overlap with existing Subnets in the
/// same VPC.
///
/// In particular, this generates a CTE like so:
///
/// ```sql
/// WITH candidate(
///     id,
///     name,
///     description,
///     time_created,
///     time_modified,
///     time_deleted,
///     vpc_id,
///     ipv4_block,
///     ipv6_block
/// ) AS (VALUES (
///     <id>,
///     <name>,
///     <description>,
///     <time_created>,
///     <time_modified>,
///     NULL::TIMESTAMPTZ,
///     <vpc_id>,
///     <ipv4_block>,
///     <ipv6_block>
/// ))
/// SELECT *
/// FROM candidate
/// WHERE NOT EXISTS (
///     SELECT ipv4_block, ipv6_block
///     FROM vpc_subnet
///     WHERE
///         vpc_id = <vpc_id> AND
///         time_deleted IS NULL AND
///         (
///             inet_contains_or_equals(ipv4_block, candidate.ipv4_block) OR
///             inet_contains_or_equals(ipv6_block, candidate.ipv6_block)
///         )
/// )
/// ```
pub struct FilterConflictingVpcSubnetRangesQuery(pub db::model::VpcSubnet);

impl QueryId for FilterConflictingVpcSubnetRangesQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for FilterConflictingVpcSubnetRangesQuery {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> diesel::QueryResult<()> {
        use db::schema::vpc_subnet::dsl;

        // "SELECT * FROM (WITH candidate("
        out.push_sql("SELECT * FROM (WITH candidate(");

        // "id, "
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");

        // "name, "
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");

        // "description, "
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");

        // "time_created, "
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");

        // "time_modified, "
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");

        // "time_deleted, "
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(", ");

        // "vpc_id, "
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(", ");

        // "ipv4_block, "
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");

        // "ipv6_block) AS (VALUES ("
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(") AS (VALUES (");

        // "<id>, "
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.0.id())?;
        out.push_sql(", ");

        // "<name>, "
        out.push_bind_param::<sql_types::Text, String>(
            &self.0.name().to_string(),
        )?;
        out.push_sql(", ");

        // "<description>, "
        out.push_bind_param::<sql_types::Text, &str>(&self.0.description())?;
        out.push_sql(", ");

        // "<time_created>, "
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.0.time_created(),
        )?;
        out.push_sql(", ");

        // "<time_modified>, "
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.0.time_modified(),
        )?;
        out.push_sql(", ");

        // "NULL::TIMESTAMPTZ, "
        out.push_sql("NULL::TIMESTAMPTZ, ");

        // "<vpc_id>, "
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.0.vpc_id)?;
        out.push_sql(", ");

        // <ipv4_block>, "
        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &ipnetwork::IpNetwork::from(self.0.ipv4_block.0 .0),
        )?;
        out.push_sql(", ");

        // "<ipv6_block>)"
        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &ipnetwork::IpNetwork::from(self.0.ipv6_block.0 .0),
        )?;
        out.push_sql("))");

        /*
         * Possibly filter the candidate row.
         *
         * This selects everything in the `candidate` CTE, where there is no
         * "overlapping" row in the `vpc_subnet` table. Specifically, we search
         * that table for rows with:
         *
         * - The same `vpc_id`
         * - Not soft-deleted
         * - The IPv4 range overlaps _or_ the IPv6 range overlaps
         *
         * Those are removed from `candidate`.
         */

        // " SELECT * FROM candidate WHERE NOT EXISTS ("
        out.push_sql(" SELECT * FROM candidate WHERE NOT EXISTS (");

        // " SELECT "
        out.push_sql("SELECT ");

        // "ipv4_block, "
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");

        // "ipv6_block "
        out.push_identifier(dsl::ipv6_block::NAME)?;

        // "FROM vpc_subnet"
        out.push_sql(" FROM ");
        dsl::vpc_subnet.from_clause().walk_ast(out.reborrow())?;

        // " WHERE "
        out.push_sql(" WHERE ");

        // "vpc_id = <vpc_id>"
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.0.vpc_id)?;

        // " AND time_deleted IS NULL AND (("
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL AND (");

        // "inet_contains_or_equals(ipv4_block, <ipv4_block>
        out.push_sql("inet_contains_or_equals(");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &ipnetwork::IpNetwork::from(self.0.ipv4_block.0 .0),
        )?;

        // ") OR inet_contains_or_equals(ipv6_block, <ipv6_block>))"
        out.push_sql(") OR inet_contains_or_equals(");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &ipnetwork::IpNetwork::from(self.0.ipv6_block.0 .0),
        )?;
        out.push_sql("))))");

        Ok(())
    }
}

impl Insertable<db::schema::vpc_subnet::table>
    for FilterConflictingVpcSubnetRangesQuery
{
    type Values = FilterConflictingVpcSubnetRangesQueryValues;

    fn values(self) -> Self::Values {
        FilterConflictingVpcSubnetRangesQueryValues(self)
    }
}

/// Used to allow inserting the result of the
/// `FilterConflictingVpcSubnetRangesQuery`, as in
/// `diesel::insert_into(foo).values(_). Should not be used directly.
pub struct FilterConflictingVpcSubnetRangesQueryValues(
    pub FilterConflictingVpcSubnetRangesQuery,
);

impl QueryId for FilterConflictingVpcSubnetRangesQueryValues {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::insertable::CanInsertInSingleQuery<Pg>
    for FilterConflictingVpcSubnetRangesQueryValues
{
    fn rows_to_insert(&self) -> Option<usize> {
        Some(1)
    }
}

impl QueryFragment<Pg> for FilterConflictingVpcSubnetRangesQueryValues {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> diesel::QueryResult<()> {
        use db::schema::vpc_subnet::dsl;
        out.push_sql("(");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(") ");
        self.0.walk_ast(out)
    }
}

#[cfg(test)]
mod test {
    use super::AllocateIpQuery;
    use super::FilterConflictingVpcSubnetRangesQuery;
    use super::SubnetError;
    use crate::db::model::{
        IncompleteNetworkInterface, NetworkInterface, VpcSubnet,
    };
    use crate::db::schema::network_interface;
    use crate::external_api::params;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use diesel::pg::Pg;
    use diesel::prelude::*;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::{
        IdentityMetadataCreateParams, Ipv4Net, Ipv6Net, MacAddr, Name,
    };
    use omicron_test_utils::dev;
    use std::convert::TryInto;
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn test_verify_query() {
        let interface_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0")
                .unwrap();
        let instance_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d1")
                .unwrap();
        let vpc_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d2")
                .unwrap();
        let subnet_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d3")
                .unwrap();
        let ipv4_block: ipnetwork::Ipv4Network =
            "192.168.1.0/24".parse().unwrap();
        let ipv6_block: ipnetwork::Ipv6Network = "fd00::/48".parse().unwrap();
        let subnet = VpcSubnet::new(
            subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: "test-subnet".to_string().try_into().unwrap(),
                description: "subnet description".to_string(),
            },
            Ipv4Net(ipv4_block.clone()).into(),
            Ipv6Net(ipv6_block),
        );
        let mac =
            MacAddr(macaddr::MacAddr6::from([0xA8, 0x40, 0x25, 0x0, 0x0, 0x1]))
                .into();
        let interface = IncompleteNetworkInterface::new(
            interface_id,
            instance_id,
            vpc_id,
            subnet,
            mac,
            None,
            params::NetworkInterfaceCreate {
                identity: IdentityMetadataCreateParams {
                    name: "test-iface".to_string().try_into().unwrap(),
                    description: "interface description".to_string(),
                },
            },
        );
        let select = AllocateIpQuery {
            interface,
            block: ipv4_block.into(),
            now: DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(0, 0),
                Utc,
            ),
        };
        let query = diesel::debug_query::<Pg, _>(&select).to_string();

        let expected_query = "SELECT \
            $1 AS \"id\", $2 AS \"name\", $3 AS \"description\", \
            $4 AS \"time_created\", $5 AS \"time_modified\", \
                $6 AS \"instance_id\", $7 AS \"vpc_id\", $8 AS \"subnet_id\", \
                $9 AS \"mac\", $10 + \"off\" AS \"ip\" \
            FROM generate_series(5, $11) AS \"off\" LEFT OUTER JOIN \
                \"network_interface\" ON \
                (\"subnet_id\", \"ip\", \"time_deleted\" IS NULL) = \
                    ($12, $13 + \"off\", TRUE) \
            WHERE \"ip\" IS NULL LIMIT 1 -- \
            binds: [223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \"test-iface\", \
                \"interface description\", 1970-01-01T00:00:00Z, \
                1970-01-01T00:00:00Z, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d1, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d2, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d3, \"A8:40:25:00:00:01\", \
                V4(Ipv4Network { addr: 192.168.1.0, prefix: 32 }), 254, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d3, \
                V4(Ipv4Network { addr: 192.168.1.0, prefix: 32 })]";
        assert_eq!(query, expected_query);

        let insert = diesel::insert_into(network_interface::table)
            .values(select)
            .returning(NetworkInterface::as_returning());
        let query = diesel::debug_query::<Pg, _>(&insert).to_string();
        let expected_query = "INSERT INTO \"network_interface\" \
            (\"id\", \"name\", \"description\", \"time_created\", \
             \"time_modified\", \"instance_id\", \"vpc_id\", \"subnet_id\", \
             \"mac\", \"ip\") \
            SELECT $1 AS \"id\", $2 AS \"name\", $3 AS \"description\", \
            $4 AS \"time_created\", $5 AS \"time_modified\", \
                $6 AS \"instance_id\", $7 AS \"vpc_id\", $8 AS \"subnet_id\", \
                $9 AS \"mac\", $10 + \"off\" AS \"ip\" \
            FROM generate_series(5, $11) AS \"off\" LEFT OUTER JOIN \
                \"network_interface\" ON \
                (\"subnet_id\", \"ip\", \"time_deleted\" IS NULL) = \
                    ($12, $13 + \"off\", TRUE) \
            WHERE \"ip\" IS NULL LIMIT 1 \
            RETURNING \"network_interface\".\"id\", \
                \"network_interface\".\"name\", \
                \"network_interface\".\"description\", \
                \"network_interface\".\"time_created\", \
                \"network_interface\".\"time_modified\", \
                \"network_interface\".\"time_deleted\", \
                \"network_interface\".\"instance_id\", \
                \"network_interface\".\"vpc_id\", \
                \"network_interface\".\"subnet_id\", \
                \"network_interface\".\"mac\", \"network_interface\".\"ip\" -- \
            binds: [223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \"test-iface\", \
                \"interface description\", 1970-01-01T00:00:00Z, \
                1970-01-01T00:00:00Z, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d1, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d2, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d3, \"A8:40:25:00:00:01\", \
                V4(Ipv4Network { addr: 192.168.1.0, prefix: 32 }), 254, \
                223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d3, \
                V4(Ipv4Network { addr: 192.168.1.0, prefix: 32 })]";
        assert_eq!(query, expected_query);
    }

    #[test]
    fn test_filter_conflicting_vpc_subnet_ranges_query_string() {
        use crate::db::identity::Resource;
        let ipv4_block = Ipv4Net("172.30.0.0/22".parse().unwrap());
        let ipv6_block = Ipv6Net("fd12:3456:7890::/64".parse().unwrap());
        let name = "a-name".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let identity = IdentityMetadataCreateParams { name, description };
        let vpc_id = Uuid::new_v4();
        let subnet_id = Uuid::new_v4();
        let row =
            VpcSubnet::new(subnet_id, vpc_id, identity, ipv4_block, ipv6_block);
        let query = FilterConflictingVpcSubnetRangesQuery(row.clone());
        let query_str = diesel::debug_query::<Pg, _>(&query).to_string();
        let expected_query = format!(
            concat!(
                "SELECT * FROM (WITH candidate(",
                r#""id", "name", "description", "time_created", "time_modified", "#,
                r#""time_deleted", "vpc_id", "ipv4_block", "ipv6_block") AS "#,
                "(VALUES ($1, $2, $3, $4, $5, NULL::TIMESTAMPTZ, $6, $7, $8)) ",
                "SELECT * FROM candidate WHERE NOT EXISTS (",
                r#"SELECT "ipv4_block", "ipv6_block" FROM "vpc_subnet" WHERE "#,
                r#""vpc_id" = $9 AND "time_deleted" IS NULL AND ("#,
                r#"inet_contains_or_equals("ipv4_block", $10) OR inet_contains_or_equals("ipv6_block", $11)))) "#,
                r#"-- binds: [{subnet_id}, "{name}", "{description}", {time_created:?}, "#,
                r#"{time_modified:?}, {vpc_id}, V4({ipv4_block:?}), V6({ipv6_block:?}), "#,
                r#"{vpc_id}, V4({ipv4_block:?}), V6({ipv6_block:?})]"#,
            ),
            subnet_id = row.id(),
            name = row.name(),
            description = row.description(),
            vpc_id = row.vpc_id,
            ipv4_block = row.ipv4_block.0 .0,
            ipv6_block = row.ipv6_block.0 .0,
            time_created = row.time_created(),
            time_modified = row.time_modified(),
        );
        assert_eq!(query_str, expected_query);
    }

    #[tokio::test]
    async fn test_filter_conflicting_vpc_subnet_ranges_query() {
        let make_id =
            |name: &Name, description: &str| IdentityMetadataCreateParams {
                name: name.clone(),
                description: description.to_string(),
            };
        let ipv4_block = Ipv4Net("172.30.0.0/22".parse().unwrap());
        let other_ipv4_block = Ipv4Net("172.31.0.0/22".parse().unwrap());
        let ipv6_block = Ipv6Net("fd12:3456:7890::/64".parse().unwrap());
        let other_ipv6_block = Ipv6Net("fd00::/64".parse().unwrap());
        let name = "a-name".to_string().try_into().unwrap();
        let other_name = "b-name".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let identity = make_id(&name, &description);
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let other_vpc_id =
            "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let other_subnet_id =
            "695debcc-e197-447d-ffb2-976150a7b7cf".parse().unwrap();
        let row =
            VpcSubnet::new(subnet_id, vpc_id, identity, ipv4_block, ipv6_block);

        // Setup the test database
        let logctx =
            dev::test_setup_log("test_filter_conflicting_vpc_subnet_ranges");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&cfg));
        let db_datastore =
            Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));

        // We should be able to insert anything into an empty table.
        assert!(
            matches!(db_datastore.vpc_create_subnet(row).await, Ok(_)),
            "Should be able to insert VPC subnet into empty table"
        );

        // We shouldn't be able to insert a row with the same IP ranges, even if
        // the other data does not conflict.
        let new_row = VpcSubnet::new(
            other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            ipv4_block,
            ipv6_block,
        );
        assert!(
            matches!(
                db_datastore.vpc_create_subnet(new_row).await,
                Err(SubnetError::OverlappingIpRange)
            ),
            "Should not be able to insert new VPC subnet with the same IP ranges"
        );

        // We should be able to insert data with the same ranges, if we change
        // the VPC ID.
        let new_row = VpcSubnet::new(
            other_subnet_id,
            other_vpc_id,
            make_id(&name, &description),
            ipv4_block,
            ipv6_block,
        );
        assert!(
            matches!(db_datastore.vpc_create_subnet(new_row).await, Ok(_)),
            "Should be able to insert a VPC Subnet with the same ranges in a different VPC",
        );

        // We shouldn't be able to insert a subnet if we change only the
        // IPv4 or IPv6 block. They must _both_ be non-overlapping.
        let new_row = VpcSubnet::new(
            other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            ipv6_block,
        );
        assert!(
            matches!(
                db_datastore.vpc_create_subnet(new_row).await,
                Err(SubnetError::OverlappingIpRange),
            ),
            "Should not be able to insert VPC Subnet with overlapping IPv4 range"
        );
        let new_row = VpcSubnet::new(
            other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            ipv4_block,
            other_ipv6_block,
        );
        assert!(
            matches!(
                db_datastore.vpc_create_subnet(new_row).await,
                Err(SubnetError::OverlappingIpRange),
            ),
            "Should not be able to insert VPC Subnet with overlapping IPv6 range"
        );

        // We should get an _external error_ if the IP address ranges are OK,
        // but the name conflicts.
        let new_row = VpcSubnet::new(
            other_subnet_id,
            vpc_id,
            make_id(&name, &description),
            other_ipv4_block,
            other_ipv6_block,
        );
        assert!(
            matches!(
                db_datastore.vpc_create_subnet(new_row).await,
                Err(SubnetError::External(_))
            ),
            "Should get an error inserting a VPC Subnet with unique IP ranges, but the same name"
        );

        // We should be able to insert the row if _both ranges_ are different,
        // and the name is unique as well.
        let new_row = VpcSubnet::new(
            Uuid::new_v4(),
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            other_ipv6_block,
        );
        assert!(
            matches!(db_datastore.vpc_create_subnet(new_row).await, Ok(_)),
            "Should be able to insert new VPC Subnet with non-overlapping IP ranges"
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
