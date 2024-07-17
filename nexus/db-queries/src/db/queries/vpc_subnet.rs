// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diesel query used for VPC Subnet allocation and insertion

use crate::db;
use crate::db::column_walker::ColumnWalker;
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
/// This query is used to idempotently insert a VPC Subnet, while checking for a
/// few different conditions that should generate errors. The main check looks
/// for any other subnets in the same VPC whose IP address blocks overlap. All
/// Subnets are required to have non-overlapping IP blocks.
///
/// Additionally, the query handles a few other cases:
///
/// - Idempotent insertion of the same record. This is used to make the VPC
/// creation saga node idempotent, and detects / handles the case where the
/// exact same active (non-soft-deleted) record is inserted more than once. It
/// will _fail_ if a duplicate primary key is inserted which does _not_ share
/// the same IP blocks. **NOTE**: Other data, like the name, is not compared
/// against the input data.
///
/// - As an edge case, if there exists a _soft-deleted_ record with the same
/// primary key, the insertion also succeeds, and _updates_ the existing record
/// unconditionally to the provided VPC Subnet.
///
/// # Details
///
/// Below is the query, in all its terrible detail. Inline comments explain the
/// rationale and mechanisms. There are 4 cases we're trying to handle here:
///
/// 1. Active row, overlapping IP blocks, different PK
///     The query should **fail**
/// 2. Active row, non-equal IP blocks, same PK
///     The query should **fail**
/// 3. Active row, equal IP blocks, same PK
///     The query should **do nothing**
/// 4. Inactive row, no constraint on IP blocks, same PK
///     The query should **update** the record to exactly the input.
///
/// ```sql
/// -- First, select a possible existing record, checking that its IP blocks are
/// -- exactly equal to the input's
/// WITH existing_record (
///     SELECT
///         *,
///         -- Generate a cast error if there is an active row (not deleted)
///         -- with exactly the same PK, and whose IP blocks do not both equal
///         -- the input.
///         --
///         -- This catches case (2) listed above.
///         CAST(
///             IF(
///                 time_deleted IS NULL AND
///                 (ipv4_block != <ipv4_block> OR ipv6_block != <ipv6_block>),
///                 'same-id-with-different-blocks',
///                 'true'
///             )
///         AS BOOL)
///     FROM
///         vpc_subnet
///     WHERE
///         id = <id>
/// ),
/// overlap AS MATERIALIZED (
///     SELECT
///         -- Generate a cast error (with a particular sentinel) if there is
///         -- any active (non-deleted) row in the same VPC, whose IP blocks
///         -- overlap with the input blocks. Note that we're explicitly _not_
///         -- comparing against records with the same primary key; that is
///         -- handled by the previous CTE.
///         --
///         -- This catches case (1) listed above.
///         --
///         -- NOTE: This cast will always fail, we just use the way it fails to
///         -- tell which block overlapped. (It could be both, in which case we
///         -- report that the IPv4 overlapped.)
///         CAST(
///             IF(
///                 inet_contains_or_equals(ipv4_block, <ipv4_block>),
///                 'overlapping-ipv4-block',
///                 'overlapping-ipv6-block'
///             )
///         AS BOOL)
///     FROM
///         vpc_subnet
///     WHERE
///         vpc_id = <vpc_id> AND
///         time_deleted IS NULL AND
///         id != <id>
///         (
///             inet_contains_or_equals(ipv4_block, <ipv4_block>) OR
///             inet_contains_or_equals(ipv6_block, <ipv6_block>)
///         )
/// ),
/// input_data(
///     id,
///     name,
///     description,
///     time_created,
///     time_deleted,
///     time_modified,
///     vpc_id,
///     rcgen,
///     ipv4_block,
///     ipv6_block,
///     custom_router_id
/// ) AS (
///     -- This CTE generates a row from the input data, which we may or may not
///     -- use in the actual insertion below.
///     VALUES (
///         <input data values>,
///     )
/// ),
/// -- At this "point" in the CTE, we've generated a detectable failure. We now
/// -- need to decide how to achieve cases (3) and (4) above. In case (3) we
/// -- want to do nothing, which is equivalent to updating the record with
/// -- itself. In case (4), we have a soft-deleted row with the same PK, which
/// -- we want to "resurrect" and set to exactly the input data.
/// --
/// -- We thus use whether the existing row was soft-deleted to choose to insert
/// -- either the existing record; or the input data.
/// data_to_insert AS (
///     SELECT (record).* FROM IF(
///         -- Return `TRUE` if the existing row is still active.
///         (SELECT time_deleted IS NULL FROM existing_record),
///         -- Insert the existing record. This is equivalent to `ON CONFLICT DO
///         -- NOTHING`.
///         (SELECT <all columns> FROM existing_record),
///         -- If the row was soft-deleted, or did not exist (falsey), then we
///         -- insert the input data.
///         (SELECT <all columns> FROM input_data),
///     )
/// )
/// -- In all cases, we want to completely overwrite the existing record. We've
/// -- either chosen to do that with the existing record itself or the new data,
/// -- depending on the `data_to_insert` CTE. Return the row to the caller.
/// UPSERT INTO vpc_subnet
/// SELECT * FROM data_to_insert
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
        out.push_sql("WITH existing_record AS (SELECT *, CAST(IF(");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL AND (");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(" != ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(&self.ipv4_block)?;
        out.push_sql(" OR ");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(" != ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(&self.ipv6_block)?;
        out.push_sql("), ");
        out.push_bind_param::<sql_types::Text, str>(
            InsertVpcSubnetError::SAME_ID_WITH_DIFFERENT_BLOCKS_SENTINEL,
        )?;
        out.push_sql(", 'TRUE') AS BOOL) FROM ");
        VPC_SUBNET_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.subnet.identity.id)?;

        out.push_sql(
            "), overlap AS MATERIALIZED (SELECT CAST(IF(inet_contains_or_equals("
        );
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");
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
        out.push_sql(" AND (inet_contains_or_equals(");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(&self.ipv4_block)?;
        out.push_sql(") OR inet_contains_or_equals(");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Inet, IpNetwork>(&self.ipv6_block)?;

        out.push_sql("))), input_data(");
        let all_columns =
            ColumnWalker::<<dsl::vpc_subnet as Table>::AllColumns>::new()
                .into_iter();
        let n_columns = all_columns.len();
        for (i, col) in all_columns.clone().enumerate() {
            out.push_identifier(col)?;
            if i < n_columns - 1 {
                out.push_sql(", ");
            }
        }
        out.push_sql(") AS (VALUES (");
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
        out.push_sql(")), ");

        out.push_sql("data_to_insert AS (SELECT (record).* FROM IF((SELECT ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL FROM existing_record), ");

        out.push_sql("(SELECT ");
        for (i, col) in all_columns.clone().enumerate() {
            out.push_identifier(col)?;
            if i < n_columns - 1 {
                out.push_sql(", ");
            }
        }
        out.push_sql(" FROM existing_record), ");

        out.push_sql("(SELECT ");
        for (i, col) in all_columns.clone().enumerate() {
            out.push_identifier(col)?;
            if i < n_columns - 1 {
                out.push_sql(", ");
            }
        }
        out.push_sql(" FROM input_data)) AS record) ");

        out.push_sql("UPSERT INTO ");
        VPC_SUBNET_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" SELECT * FROM data_to_insert RETURNING *");

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
    /// An attempt to insert an existing record, but with different IP blocks.
    SameIdWithDifferentIpBlocks(external::Error),
    /// Any other error
    External(external::Error),
}

impl InsertVpcSubnetError {
    fn same_id_with_different_blocks(id: Uuid) -> Self {
        let err = external::Error::internal_error(&format!(
            "Failed to create VPC Subnet, found an active \
            record with the same primary key ({id}), but with \
            different IP blocks",
        ));
        InsertVpcSubnetError::SameIdWithDifferentIpBlocks(err)
    }

    const SAME_ID_WITH_DIFFERENT_BLOCKS_SENTINEL: &'static str =
        "same-id-different-ip-blocks";
    const SAME_ID_WITH_DIFFERENT_BLOCKS_ERROR_MESSAGE: &'static str = r#"could not parse "same-id-different-ip-blocks" as type bool: invalid bool value"#;
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
            // Attempt to insert the same ID as a different IP block.
            DieselError::DatabaseError(
                DatabaseErrorKind::Unknown,
                ref info,
            ) if info.message()
                == Self::SAME_ID_WITH_DIFFERENT_BLOCKS_ERROR_MESSAGE =>
            {
                InsertVpcSubnetError::same_id_with_different_blocks(
                    subnet.identity.id,
                )
            }

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
            InsertVpcSubnetError::SameIdWithDifferentIpBlocks(e) => e,
            InsertVpcSubnetError::External(e) => e,
        }
    }
}

#[cfg(test)]
mod test {
    use super::dsl;
    use super::InsertVpcSubnetError;
    use super::InsertVpcSubnetQuery;
    use crate::db::explain::ExplainableAsync as _;
    use crate::db::model::VpcSubnet;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::ExpressionMethods as _;
    use diesel::QueryDsl as _;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_test_utils::dev;
    use std::convert::TryInto;
    use std::sync::Arc;

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
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&logctx.log, &cfg));
        let conn = pool.pool().get().await.unwrap();
        let explain = query.explain_async(&conn).await.unwrap();
        println!("{explain}");
        db.cleanup().await.unwrap();
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
        let ipv6_block = "fd12:3456:7890::/64".parse().unwrap();
        let other_ipv6_block = "fd00::/64".parse().unwrap();
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
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&logctx.log, &cfg));
        let db_datastore = Arc::new(
            crate::db::DataStore::new(&log, Arc::clone(&pool), None)
                .await
                .unwrap(),
        );

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

        // We shouldn't be able to insert a subnet if we change only the
        // IPv4 or IPv6 block. They must _both_ be non-overlapping.
        let new_row = VpcSubnet::new(
            other_other_subnet_id,
            vpc_id,
            make_id(&other_name, &description),
            other_ipv4_block,
            ipv6_block,
        );
        let err = db_datastore
            .vpc_create_subnet_raw(new_row)
            .await
            .expect_err("Should not be able to insert VPC Subnet with overlapping IPv6 range");
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

        db.cleanup().await.unwrap();
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
        let other_ipv4_block: oxnet::Ipv4Net = "172.30.1.0/24".parse().unwrap();
        let ipv6_block = "fd12:3456:7890::/64".parse().unwrap();
        let other_ipv6_block: oxnet::Ipv6Net =
            "fd12:3456:7800::/64".parse().unwrap();
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
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&logctx.log, &cfg));
        let db_datastore = Arc::new(
            crate::db::DataStore::new(&log, Arc::clone(&pool), None)
                .await
                .unwrap(),
        );

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

        // Note that if we change either or both of the actual IP subnets, this
        // _should_ continue to fail. That would happen if for some reason we
        // tried to modify the row during the saga, or if we did actually
        // generate the same UUID more than once.
        let with_new_ipv4_block =
            VpcSubnet { ipv4_block: other_ipv4_block.into(), ..row.clone() };
        let with_new_ipv6_block =
            VpcSubnet { ipv6_block: other_ipv6_block.into(), ..row.clone() };
        let with_new_both = VpcSubnet {
            ipv4_block: other_ipv4_block.into(),
            ipv6_block: other_ipv6_block.into(),
            ..row.clone()
        };
        let expected_err = InsertVpcSubnetError::same_id_with_different_blocks(
            row.identity.id,
        );
        for each in [with_new_ipv4_block, with_new_ipv6_block, with_new_both] {
            let result =
                db_datastore.vpc_create_subnet_raw(each).await.expect_err(
                    "query should fail when reinserting same ID \
                    with different IP block",
                );
            assert_eq!(
                result, expected_err,
                "Must NOT be able to insert a VPC subnet row with \
                the same ID, if the IP subnet data does not match \
                exactly with the existing row. Found {result:#?}"
            );
        }
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_vpc_subnet_query_updates_soft_deleted_row() {
        let ipv4_block = "172.30.0.0/24".parse().unwrap();
        let other_ipv4_block: oxnet::Ipv4Net = "172.30.1.0/24".parse().unwrap();
        let ipv6_block = "fd12:3456:7890::/64".parse().unwrap();
        let other_ipv6_block: oxnet::Ipv6Net =
            "fd12:3456:7800::/64".parse().unwrap();
        let name = "a-name".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let identity = IdentityMetadataCreateParams { name, description };
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let other_subnet_id =
            "695debcc-e197-447d-ffb2-976150a7b7cf".parse().unwrap();
        let row = VpcSubnet::new(
            subnet_id,
            vpc_id,
            identity.clone(),
            ipv4_block,
            ipv6_block,
        );

        // Setup the test database
        let logctx = dev::test_setup_log(
            "test_insert_vpc_subnet_query_updates_soft_deleted_row",
        );
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&logctx.log, &cfg));
        let conn = pool.pool().get().await.unwrap();
        let db_datastore = Arc::new(
            crate::db::DataStore::new(&log, Arc::clone(&pool), None)
                .await
                .unwrap(),
        );

        // We should be able to insert anything into an empty table.
        let inserted = db_datastore
            .vpc_create_subnet_raw(row.clone())
            .await
            .expect("Should be able to insert VPC subnet into empty table");

        // Soft-delete the row.
        diesel::update(dsl::vpc_subnet.find(row.identity.id))
            .set(dsl::time_deleted.eq(Some(Utc::now())))
            .execute_async(&*conn)
            .await
            .unwrap();

        // Insert the row again, which should "undelete" the row.
        let new = db_datastore.vpc_create_subnet_raw(row.clone()).await.expect(
            "Should be able to insert the VPC subnet again \
                after soft-deleting the existing row",
        );
        assert_rows_eq(&inserted, &new);

        // Do the same thing, but this time inserting a row with different IP
        // blocks. We should also update the row to the new data in this case.
        diesel::update(dsl::vpc_subnet.find(row.identity.id))
            .set(dsl::time_deleted.eq(Some(Utc::now())))
            .execute_async(&*conn)
            .await
            .unwrap();
        let new_row = VpcSubnet::new(
            other_subnet_id,
            vpc_id,
            identity,
            other_ipv4_block,
            other_ipv6_block,
        );
        let inserted =
            db_datastore.vpc_create_subnet_raw(new_row.clone()).await.expect(
                "Should be able to insert a VPC subnet with \
                different IP blocks after soft-deleting the \
                existing row",
            );
        assert_rows_eq(&new_row, &inserted);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
