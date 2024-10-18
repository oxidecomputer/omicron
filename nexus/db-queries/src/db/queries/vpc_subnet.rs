// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diesel query used for VPC Subnet allocation and insertion

use crate::db;
use crate::db::identity::Resource;
use crate::db::model::VpcSubnet;
use crate::db::schema::vpc_subnet::dsl;
use crate::db::DbConnection;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use ref_cast::RefCast;
use uuid::Uuid;

/// Query used to insert VPC Subnets.
///
/// This query is used to idempotently insert a VPC Subnet. The query also looks
/// for any other subnets in the same VPC whose IP address blocks overlap. All
/// Subnets are required to have non-overlapping IP blocks.
///
/// Note that this query is idempotent. If a record with the provided primary
/// key already exists, that record is returned exactly from the DB, without any
/// other modification or alteration. If callers care, they can inspect the
/// record to make sure it's what they expected, though that's usually a fraught
/// endeavor.
///
/// Here is the entire query:
///
/// ```sql
/// WITH
/// -- This CTE generates a casting error if any live records, other than _this_
/// -- record, have overlapping IP blocks of either family.
/// overlap AS MATERIALIZED (
///     SELECT
///         -- NOTE: This cast always fails, we just use _how_ it fails to
///         -- learn which IP block overlaps. The filter `id != <id>` below
///         -- means we're explicitly ignoring an existing, identical record.
///         -- So this cast is only run if there is another record in the same
///         -- VPC with an overlapping subnet, which is exactly the error case
///         -- we're trying to cacth.
///         CAST(
///             IF(
///                (<ipv4_block> && ipv4_block),
///                'ipv4',
///                'ipv6'
///             )
///             AS BOOL
///         )
///     FROM
///         vpc_subnet
///     WHERE
///         vpc_id = <vpc_id> AND
///         time_deleted IS NULL AND
///         id != <id> AND
///         (
///             (ipv4_block && <ipv4_block>) OR
///             (ipv6_block && <ipv6_block>)
///         )
/// )
/// INSERT INTO
///     vpc_subnet
/// VALUES (
///     <input data>
/// )
/// ON CONFLICT (id)
/// -- We use this "no-op" update to allow us to return the actual row from the
/// -- DB, either the existing or inserted one.
/// DO UPDATE SET id = id
/// RETURNING *;
/// ```
#[derive(Clone, Debug)]
pub struct InsertVpcSubnetQuery {
    /// The subnet to insert
    subnet: VpcSubnet,
    /// Owned values of the IP blocks to check, for inserting in internal pieces
    /// of the query.
    ipv4_block: IpNetwork,
    ipv6_block: IpNetwork,
}

impl InsertVpcSubnetQuery {
    /// Construct a new query to insert the provided subnet.
    pub fn new(subnet: VpcSubnet) -> Self {
        let ipv4_block = IpNetwork::V4(subnet.ipv4_block.0.into());
        let ipv6_block = IpNetwork::V6(subnet.ipv6_block.0.into());
        Self { subnet, ipv4_block, ipv6_block }
    }
}

impl QueryId for InsertVpcSubnetQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for InsertVpcSubnetQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_sql("WITH overlap AS MATERIALIZED (SELECT CAST(IF((");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(" && ");
        out.push_bind_param::<sql_types::Inet, _>(&self.ipv4_block)?;
        out.push_sql("), ");
        out.push_bind_param::<sql_types::Text, _>(
            InsertVpcSubnetError::OVERLAPPING_IPV4_BLOCK_SENTINEL,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, _>(
            InsertVpcSubnetError::OVERLAPPING_IPV6_BLOCK_SENTINEL,
        )?;
        out.push_sql(") AS BOOL) FROM ");
        VPC_SUBNET_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::vpc_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.subnet.vpc_id)?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL AND ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" != ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.subnet.identity.id)?;
        out.push_sql(" AND ((");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(" && ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(&self.ipv4_block)?;
        out.push_sql(") OR (");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(" && ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(&self.ipv6_block)?;

        out.push_sql("))) INSERT INTO ");
        VPC_SUBNET_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql("VALUES (");
        out.push_bind_param::<sql_types::Uuid, _>(&self.subnet.identity.id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, _>(db::model::Name::ref_cast(
            self.subnet.name(),
        ))?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, _>(
            &self.subnet.identity.description,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Timestamptz, _>(
            &self.subnet.identity.time_created,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Timestamptz, _>(
            &self.subnet.identity.time_modified,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Nullable<sql_types::Timestamptz>, _>(
            &self.subnet.identity.time_deleted,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.subnet.vpc_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Int8, _>(&self.subnet.rcgen)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, _>(&self.ipv4_block)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, _>(&self.ipv6_block)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, _>(
            &self.subnet.custom_router_id,
        )?;
        out.push_sql(") ON CONFLICT (");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(") DO UPDATE SET ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.subnet.identity.id)?;
        out.push_sql(" RETURNING *");

        Ok(())
    }
}

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type VpcSubnetFromClause = FromClause<db::schema::vpc_subnet::table>;
const VPC_SUBNET_FROM_CLAUSE: VpcSubnetFromClause = VpcSubnetFromClause::new();

impl RunQueryDsl<DbConnection> for InsertVpcSubnetQuery {}
impl Query for InsertVpcSubnetQuery {
    type SqlType = <<VpcSubnet as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression>::SqlType;
}

/// Errors related to inserting VPC Subnets.
#[derive(Debug, PartialEq)]
pub enum InsertVpcSubnetError {
    /// The IPv4 or IPv6 subnet overlaps with an existing VPC Subnet
    OverlappingIpRange(oxnet::IpNet),
    /// Any other error
    External(external::Error),
}

impl InsertVpcSubnetError {
    const OVERLAPPING_IPV4_BLOCK_SENTINEL: &'static str = "ipv4";
    const OVERLAPPING_IPV4_BLOCK_ERROR_MESSAGE: &'static str =
        r#"could not parse "ipv4" as type bool: invalid bool value"#;
    const OVERLAPPING_IPV6_BLOCK_SENTINEL: &'static str = "ipv6";
    const OVERLAPPING_IPV6_BLOCK_ERROR_MESSAGE: &'static str =
        r#"could not parse "ipv6" as type bool: invalid bool value"#;
    const NAME_CONFLICT_CONSTRAINT: &'static str = "vpc_subnet_vpc_id_name_key";

    /// Construct an `InsertError` from a Diesel error, catching the desired
    /// cases and building useful errors.
    pub fn from_diesel(e: DieselError, subnet: &VpcSubnet) -> Self {
        use crate::db::error;
        use diesel::result::DatabaseErrorKind;
        match e {
            // Attempt to insert an overlapping IPv4 subnet
            DieselError::DatabaseError(
                DatabaseErrorKind::Unknown,
                ref info,
            ) if info.message()
                == Self::OVERLAPPING_IPV4_BLOCK_ERROR_MESSAGE =>
            {
                InsertVpcSubnetError::OverlappingIpRange(
                    subnet.ipv4_block.0.into(),
                )
            }

            // Attempt to insert an overlapping IPv6 subnet
            DieselError::DatabaseError(
                DatabaseErrorKind::Unknown,
                ref info,
            ) if info.message()
                == Self::OVERLAPPING_IPV6_BLOCK_ERROR_MESSAGE =>
            {
                InsertVpcSubnetError::OverlappingIpRange(
                    subnet.ipv6_block.0.into(),
                )
            }

            // Conflicting name for the subnet within a VPC
            DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                ref info,
            ) if info.constraint_name()
                == Some(Self::NAME_CONFLICT_CONSTRAINT) =>
            {
                InsertVpcSubnetError::External(error::public_error_from_diesel(
                    e,
                    error::ErrorHandler::Conflict(
                        external::ResourceType::VpcSubnet,
                        subnet.identity().name.as_str(),
                    ),
                ))
            }

            // Any other error at all is a bug
            _ => InsertVpcSubnetError::External(
                error::public_error_from_diesel(e, error::ErrorHandler::Server),
            ),
        }
    }

    /// Convert into a public error
    pub fn into_external(self) -> external::Error {
        match self {
            InsertVpcSubnetError::OverlappingIpRange(ip) => {
                external::Error::invalid_request(
                    format!(
                        "IP address range '{}' \
                        conflicts with an existing subnet",
                        ip,
                    )
                    .as_str(),
                )
            }
            InsertVpcSubnetError::External(e) => e,
        }
    }
}

#[cfg(test)]
mod test {
    use super::InsertVpcSubnetError;
    use super::InsertVpcSubnetQuery;
    use crate::db::explain::ExplainableAsync as _;
    use crate::db::model::VpcSubnet;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_test_utils::dev;
    use std::convert::TryInto;

    #[tokio::test]
    async fn explain_insert_query() {
        let ipv4_block = "172.30.0.0/24".parse().unwrap();
        let ipv6_block = "fd12:3456:7890::/64".parse().unwrap();
        let name = "a-name".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let identity = IdentityMetadataCreateParams { name, description };
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let row =
            VpcSubnet::new(subnet_id, vpc_id, identity, ipv4_block, ipv6_block);
        let query = InsertVpcSubnetQuery::new(row);
        let logctx = dev::test_setup_log("explain_insert_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let conn = db.pool().claim().await.unwrap();
        let explain = query.explain_async(&conn).await.unwrap();
        println!("{explain}");
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_vpc_subnet_query() {
        let make_id =
            |name: &Name, description: &str| IdentityMetadataCreateParams {
                name: name.clone(),
                description: description.to_string(),
            };
        let ipv4_block = "172.30.0.0/22".parse().unwrap();
        let other_ipv4_block = "172.31.0.0/22".parse().unwrap();
        let overlapping_ipv4_block_longer = "172.30.0.0/21".parse().unwrap();
        let overlapping_ipv4_block_shorter = "172.30.0.0/23".parse().unwrap();
        let ipv6_block = "fd12:3456:7890::/64".parse().unwrap();
        let other_ipv6_block = "fd00::/64".parse().unwrap();
        let overlapping_ipv6_block_longer =
            "fd12:3456:7890::/60".parse().unwrap();
        let overlapping_ipv6_block_shorter =
            "fd12:3456:7890::/68".parse().unwrap();
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
        let other_other_subnet_id =
            "ddbdc2b7-d22f-40d9-98df-fef5da151e0d".parse().unwrap();
        let row =
            VpcSubnet::new(subnet_id, vpc_id, identity, ipv4_block, ipv6_block);

        // Setup the test database
        let logctx = dev::test_setup_log("test_insert_vpc_subnet_query");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let db_datastore = db.datastore();

        // We should be able to insert anything into an empty table.
        assert!(
            matches!(
                db_datastore.vpc_create_subnet_raw(row.clone()).await,
                Ok(_)
            ),
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
                db_datastore.vpc_create_subnet_raw(new_row.clone()).await,
                Err(InsertVpcSubnetError::OverlappingIpRange { .. }),
            ),
            "Should not be able to insert new VPC subnet with the \
            same IPv4 and IPv6 ranges,\n\
            first row: {row:?}\n\
            new row: {new_row:?}",
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
            matches!(db_datastore.vpc_create_subnet_raw(new_row).await, Ok(_)),
            "Should be able to insert a VPC Subnet with the same ranges in a different VPC",
        );

        // We shouldn't be able to insert a subnet if the IPv4 or IPv6 blocks overlap.
        // Explicitly test for different CIDR masks with the same network address
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            overlapping_ipv4_block_longer,
            other_ipv6_block,
        );
        let err = db_datastore.vpc_create_subnet_raw(new_row).await.expect_err(
            "Should not be able to insert VPC Subnet with \
                overlapping IPv4 range {overlapping_ipv4_block_longer}",
        );
        assert_eq!(
            err,
            InsertVpcSubnetError::OverlappingIpRange(
                overlapping_ipv4_block_longer.into()
            ),
            "InsertError variant should indicate an IP block overlaps"
        );
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            overlapping_ipv4_block_shorter,
            other_ipv6_block,
        );
        let err = db_datastore.vpc_create_subnet_raw(new_row).await.expect_err(
            "Should not be able to insert VPC Subnet with \
                overlapping IPv4 range {overlapping_ipv4_block_shorter}",
        );
        assert_eq!(
            err,
            InsertVpcSubnetError::OverlappingIpRange(
                overlapping_ipv4_block_shorter.into()
            ),
            "InsertError variant should indicate an IP block overlaps"
        );
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            overlapping_ipv6_block_longer,
        );
        let err = db_datastore.vpc_create_subnet_raw(new_row).await.expect_err(
            "Should not be able to insert VPC Subnet with \
                overlapping IPv6 range {overlapping_ipv6_block_longer}",
        );
        assert_eq!(
            err,
            InsertVpcSubnetError::OverlappingIpRange(
                overlapping_ipv6_block_longer.into()
            ),
            "InsertError variant should indicate an IP block overlaps"
        );
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            overlapping_ipv6_block_shorter,
        );
        let err = db_datastore.vpc_create_subnet_raw(new_row).await.expect_err(
            "Should not be able to insert VPC Subnet with \
                overlapping IPv6 range {overlapping_ipv6_block_shorter}",
        );
        assert_eq!(
            err,
            InsertVpcSubnetError::OverlappingIpRange(
                overlapping_ipv6_block_shorter.into()
            ),
            "InsertError variant should indicate an IP block overlaps"
        );

        // We shouldn't be able to insert a subnet if we change only the
        // IPv4 or IPv6 block. They must _both_ be non-overlapping.
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            ipv6_block,
        );
        let err = db_datastore.vpc_create_subnet_raw(new_row).await.expect_err(
            "Should not be able to insert VPC Subnet with \
                overlapping IPv6 range",
        );
        assert_eq!(
            err,
            InsertVpcSubnetError::OverlappingIpRange(ipv6_block.into()),
            "InsertError variant should indicate an IP block overlaps"
        );
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            ipv4_block,
            other_ipv6_block,
        );
        let err = db_datastore
            .vpc_create_subnet_raw(new_row)
            .await
            .expect_err("Should not be able to insert VPC Subnet with overlapping IPv4 range");
        assert_eq!(
            err,
            InsertVpcSubnetError::OverlappingIpRange(ipv4_block.into()),
            "InsertError variant should indicate an IP block overlaps"
        );

        // We should get an _external error_ if the IP address ranges are OK,
        // but the name conflicts.
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&name, &description),
            other_ipv4_block,
            other_ipv6_block,
        );
        assert!(
            matches!(
                db_datastore.vpc_create_subnet_raw(new_row).await,
                Err(InsertVpcSubnetError::External(_))
            ),
            "Should get an error inserting a VPC Subnet with unique IP ranges, but the same name"
        );

        // We should be able to insert the row if _both ranges_ are different,
        // and the name is unique as well.
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            other_ipv6_block,
        );
        assert!(
            matches!(db_datastore.vpc_create_subnet_raw(new_row).await, Ok(_)),
            "Should be able to insert new VPC Subnet with non-overlapping IP ranges"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Helper to verify equality of rows, handling timestamp precision.
    fn assert_rows_eq(left: &VpcSubnet, right: &VpcSubnet) {
        assert_eq!(
            left.identity.id, right.identity.id,
            "VPC Subnet rows should be equal"
        );
        assert_eq!(
            left.identity.name, right.identity.name,
            "VPC Subnet rows should be equal"
        );
        assert_eq!(
            left.identity.description, right.identity.description,
            "VPC Subnet rows should be equal"
        );
        // Timestamps in CRDB have microsecond precision, so ensure we're
        // within 1000 nanos.
        assert!(
            (left.identity.time_modified - right.identity.time_modified)
                .num_nanoseconds()
                .unwrap()
                < 1_000,
            "VPC Subnet rows should be equal",
        );
        assert!(
            (left.identity.time_created - right.identity.time_created)
                .num_nanoseconds()
                .unwrap()
                < 1_000,
            "VPC Subnet rows should be equal",
        );
        assert_eq!(
            left.identity.time_deleted, right.identity.time_deleted,
            "VPC Subnet rows should be equal",
        );
        assert_eq!(
            left.vpc_id, right.vpc_id,
            "VPC Subnet rows should be equal"
        );
        assert_eq!(left.rcgen, right.rcgen, "VPC Subnet rows should be equal");
        assert_eq!(
            left.ipv4_block, right.ipv4_block,
            "VPC Subnet rows should be equal"
        );
        assert_eq!(
            left.ipv6_block, right.ipv6_block,
            "VPC Subnet rows should be equal"
        );
        assert_eq!(
            left.custom_router_id, right.custom_router_id,
            "VPC Subnet rows should be equal"
        );
    }

    // Regression test for https://github.com/oxidecomputer/omicron/issues/6069.
    #[tokio::test]
    async fn test_insert_vpc_subnet_query_is_idempotent() {
        let ipv4_block = "172.30.0.0/24".parse().unwrap();
        let ipv6_block = "fd12:3456:7890::/64".parse().unwrap();
        let name = "a-name".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let identity = IdentityMetadataCreateParams { name, description };
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let row =
            VpcSubnet::new(subnet_id, vpc_id, identity, ipv4_block, ipv6_block);

        // Setup the test database
        let logctx =
            dev::test_setup_log("test_insert_vpc_subnet_query_is_idempotent");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let db_datastore = db.datastore();

        // We should be able to insert anything into an empty table.
        let inserted = db_datastore
            .vpc_create_subnet_raw(row.clone())
            .await
            .expect("Should be able to insert VPC subnet into empty table");
        assert_rows_eq(&inserted, &row);

        // We should be able to insert the exact same row again. The IP ranges
        // overlap, but the ID is also identical, which should not be an error.
        // This is important for saga idempotency.
        let inserted = db_datastore
            .vpc_create_subnet_raw(row.clone())
            .await
            .expect(
            "Must be able to insert the exact same VPC subnet more than once",
        );
        assert_rows_eq(&inserted, &row);
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
