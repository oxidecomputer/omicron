// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diesel queries used for subnet allocation

use crate::db;
use crate::db::identity::Resource;
use crate::db::model::IncompleteNetworkInterface;
use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types;
use std::convert::TryFrom;
use uuid::Uuid;

/// Used for allocating an IP as part of [`NetworkInterface`] construction.
///
/// This is a query equivalent to:
/// SELECT <id> AS id, <name> AS name, <description> AS description,
///        <time_created> AS time_created, <time_modified> AS time_modified,
///        <instance_id> AS instance_id, <vpc_id> AS vpc_id,
///        <subnet_id> AS subnet_id, <mac> AS mac, <block_bASe> + off AS ip
///   FROM
///        generate_series(5, <num_addresses_in_block>) AS off
///   LEFT OUTER JOIN
///        network_interface
///   ON (subnet_id, ip, time_deleted IS NULL) =
///      (<subnet_id>, <block_base> + off, TRUE)
///   WHERE ip IS NULL LIMIT 1;
///
/// Note that generate_series receives a start value of 5 in accordance with
/// RFD 21's reservation of addresses 0 through 4 in a subnet.
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

        // Start the offsets from 1 to exclude the network base address.
        out.push_sql(" FROM generate_series(5, ");
        out.push_bind_param::<sql_types::BigInt, _>(
            // Subtract 1 to exclude the broadcast address
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

#[cfg(test)]
mod test {
    use super::AllocateIpQuery;
    use crate::db::model::{
        IncompleteNetworkInterface, NetworkInterface, VpcSubnet,
    };
    use crate::db::schema::network_interface;
    use crate::external_api::params;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use diesel::pg::Pg;
    use diesel::prelude::*;
    use omicron_common::api::external::{
        IdentityMetadataCreateParams, Ipv4Net, MacAddr,
    };
    use std::convert::TryInto;

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
        let block: ipnetwork::Ipv4Network = "192.168.1.0/24".parse().unwrap();
        let subnet = VpcSubnet::new(
            subnet_id,
            vpc_id,
            params::VpcSubnetCreate {
                identity: IdentityMetadataCreateParams {
                    name: "test-subnet".to_string().try_into().unwrap(),
                    description: "subnet description".to_string(),
                },
                ipv4_block: Some(Ipv4Net(block.clone()).into()),
                ipv6_block: None,
            },
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
            block: block.into(),
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
}
