use crate::db;
use crate::db::identity::Resource;
use crate::db::model::IncompleteNetworkInterface;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types;
use std::convert::TryFrom;
use uuid::Uuid;

/// Used for allocating an IP as part of [`NetworkInterface`] construction.
/// This is a query equivalent to:
/// SELECT <id> AS id, <name> AS name, <description> AS description,
///        <time_created> AS time_created, <time_modified> AS time_modified,
///        <instance_id> AS instance_id, <vpc_id> AS vpc_id,
///        <subnet_id> AS subnet_id, <mac> AS mac, <block_bASe> + off AS ip
///   FROM
///        generate_series(1, <num_addresses_in_block>) AS off
///   LEFT OUTER JOIN
///        network_interface
///   ON (subnet_id, ip, time_deleted IS NULL) =
///      (<subnet_id>, <block_base> + off, TRUE)
///   WHERE ip IS NULL LIMIT 1;
pub struct AllocateIpQuery {
    pub interface: IncompleteNetworkInterface,
    pub block: ipnetwork::IpNetwork,
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
        let now = Utc::now();

        out.push_sql("SELECT ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.identity.id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Text, String>(
            &self.interface.identity.name.to_string(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Text, String>(
            &self.interface.identity.description,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Timestamptz, _>(&now)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Timestamptz, _>(&now)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.instance_id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::instance_id::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.interface.vpc_id)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.subnet.id(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::subnet_id::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Text, String>(
            &self.interface.mac.to_string(),
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::mac::NAME)?;
        out.push_sql(",");

        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &self.block.network().into(),
        )?;
        out.push_sql(" + ");
        out.push_identifier("off")?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::ip::NAME)?;

        // Start the offsets from 1 to exclude the network base address.
        out.push_sql(" FROM generate_series(1, ");
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
        out.push_sql(",");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL) = (");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.subnet.id(),
        )?;
        out.push_sql(",");
        out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
            &self.block.network().into(),
        )?;
        out.push_sql(" + ");
        out.push_identifier("off")?;
        out.push_sql(", TRUE) ");
        //   WHERE ip IS NULL LIMIT 1;
        out.push_sql("WHERE ");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql("IS NULL LIMIT 1");
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
        out.push_sql(",");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::instance_id::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::subnet_id::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::mac::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(") ");
        self.0.walk_ast(out)
    }
}
