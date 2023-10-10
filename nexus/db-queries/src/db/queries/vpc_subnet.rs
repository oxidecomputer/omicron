// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diesel query used for VPC Subnet allocation and insertion

use crate::db;
use crate::db::identity::Resource;
use crate::db::model::VpcSubnet;
use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types;
use omicron_common::api::external;
use ref_cast::RefCast;
use uuid::Uuid;

/// Errors related to allocating VPC Subnets.
#[derive(Debug, PartialEq)]
pub enum SubnetError {
    /// An IPv4 or IPv6 subnet overlaps with an existing VPC Subnet
    OverlappingIpRange(ipnetwork::IpNetwork),
    /// An other error
    External(external::Error),
}

impl SubnetError {
    /// Construct a `SubnetError` from a Diesel error, catching the desired
    /// cases and building useful errors.
    pub fn from_diesel(
        e: async_bb8_diesel::ConnectionError,
        subnet: &VpcSubnet,
    ) -> Self {
        use crate::db::error;
        use async_bb8_diesel::ConnectionError;
        use diesel::result::DatabaseErrorKind;
        use diesel::result::Error;
        const IPV4_OVERLAP_ERROR_MESSAGE: &str =
            r#"null value in column "ipv4_block" violates not-null constraint"#;
        const IPV6_OVERLAP_ERROR_MESSAGE: &str =
            r#"null value in column "ipv6_block" violates not-null constraint"#;
        const NAME_CONFLICT_CONSTRAINT: &str = "vpc_subnet_vpc_id_name_key";
        match e {
            // Attempt to insert overlapping IPv4 subnet
            ConnectionError::Query(Error::DatabaseError(
                DatabaseErrorKind::NotNullViolation,
                ref info,
            )) if info.message() == IPV4_OVERLAP_ERROR_MESSAGE => {
                SubnetError::OverlappingIpRange(subnet.ipv4_block.0 .0.into())
            }

            // Attempt to insert overlapping IPv6 subnet
            ConnectionError::Query(Error::DatabaseError(
                DatabaseErrorKind::NotNullViolation,
                ref info,
            )) if info.message() == IPV6_OVERLAP_ERROR_MESSAGE => {
                SubnetError::OverlappingIpRange(subnet.ipv6_block.0 .0.into())
            }

            // Conflicting name for the subnet within a VPC
            ConnectionError::Query(Error::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                ref info,
            )) if info.constraint_name() == Some(NAME_CONFLICT_CONSTRAINT) => {
                SubnetError::External(error::public_error_from_diesel(
                    e,
                    error::ErrorHandler::Conflict(
                        external::ResourceType::VpcSubnet,
                        subnet.identity().name.as_str(),
                    ),
                ))
            }

            // Any other error at all is a bug
            _ => SubnetError::External(error::public_error_from_diesel(
                e,
                error::ErrorHandler::Server,
            )),
        }
    }

    /// Convert into a public error
    pub fn into_external(self) -> external::Error {
        match self {
            SubnetError::OverlappingIpRange(ip) => {
                external::Error::invalid_request(
                    format!("IP address range '{}' conflicts with an existing subnet", ip).as_str()
                )
            },
            SubnetError::External(e) => e,
        }
    }
}

/// Generate a subquery that selects any overlapping address ranges of the same
/// type as the input IP subnet.
///
/// This generates a query that, in full, looks like:
///
/// ```sql
/// SELECT
///     <ip>
/// FROM
///     vpc_subnet
/// WHERE
///     vpc_id = <vpc_id> AND
///     time_deleted IS NULL AND
///     inet_contains_or_equals(ipv*_block, <ip>)
/// LIMIT 1
/// ```
///
/// The input may be either an IPv4 or IPv6 subnet, and the corresponding column
/// is compared against. Note that the exact input IP range is returned on
/// purpose.
fn push_select_overlapping_ip_range<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    vpc_id: &'a Uuid,
    ip: &'a ipnetwork::IpNetwork,
) -> diesel::QueryResult<()> {
    use crate::db::schema::vpc_subnet::dsl;
    out.push_sql("SELECT ");
    out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(ip)?;
    out.push_sql(" FROM ");
    VPC_SUBNET_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(vpc_id)?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(" IS NULL AND inet_contains_or_equals(");
    if ip.is_ipv4() {
        out.push_identifier(dsl::ipv4_block::NAME)?;
    } else {
        out.push_identifier(dsl::ipv6_block::NAME)?;
    }
    out.push_sql(", ");
    out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(ip)?;
    out.push_sql(")");
    Ok(())
}

/// Generate a subquery that returns NULL if there is an overlapping IP address
/// range of any type.
///
/// This specifically generates a query that looks like:
///
/// ```sql
/// SELECT NULLIF(
///     <ip>,
///     push_select_overlapping_ip_range(<vpc_id>, <ip>)
/// )
/// ```
///
/// The `NULLIF` function returns NULL if those two expressions are equal, and
/// the first expression otherwise. That is, this returns NULL if there exists
/// an overlapping IP range already in the VPC Subnet table, and the requested
/// IP range if not.
fn push_null_if_overlapping_ip_range<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    vpc_id: &'a Uuid,
    ip: &'a ipnetwork::IpNetwork,
) -> diesel::QueryResult<()> {
    out.push_sql("SELECT NULLIF(");
    out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(ip)?;
    out.push_sql(", (");
    push_select_overlapping_ip_range(out.reborrow(), vpc_id, ip)?;
    out.push_sql("))");
    Ok(())
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
///     rcgen
/// ) AS (VALUES (
///     <id>,
///     <name>,
///     <description>,
///     <time_created>,
///     <time_modified>,
///     NULL::TIMESTAMPTZ,
///     <vpc_id>,
///     0
/// )),
/// candidate_ipv4(ipv4_block) AS (
///     SELECT(
///         NULLIF(
///             <ipv4_block>,
///             (
///                 SELECT
///                     ipv4_block
///                 FROM
///                     vpc_subnet
///                 WHERE
///                     vpc_id = <vpc_id> AND
///                     time_deleted IS NULL AND
///                     inet_contains_or_equals(<ipv4_block>, ipv4_block)
///                 LIMIT 1
///             )
///        )
///   )
/// ),
/// candidate_ipv6(ipv6_block) AS (
///     <same as above, for ipv6>
/// )
/// SELECT *
/// FROM candidate, candidate_ipv4, candidate_ipv6
/// ```
pub struct FilterConflictingVpcSubnetRangesQuery {
    // TODO: update with random one if the insertion fails.
    subnet: VpcSubnet,

    // The following fields are derived from the previous field. This begs the
    // question: "Why bother storing them at all?"
    //
    // Diesel's [`diesel::query_builder::ast_pass::AstPass:push_bind_param`] method
    // requires that the provided value now live as long as the entire AstPass
    // type. By storing these values in the struct, they'll live at least as
    // long as the entire call to [`QueryFragment<Pg>::walk_ast`].
    ipv4_block: ipnetwork::IpNetwork,
    ipv6_block: ipnetwork::IpNetwork,
}

impl FilterConflictingVpcSubnetRangesQuery {
    pub fn new(subnet: VpcSubnet) -> Self {
        let ipv4_block = ipnetwork::IpNetwork::from(subnet.ipv4_block.0 .0);
        let ipv6_block = ipnetwork::IpNetwork::from(subnet.ipv6_block.0 .0);
        Self { subnet, ipv4_block, ipv6_block }
    }
}

impl QueryId for FilterConflictingVpcSubnetRangesQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for FilterConflictingVpcSubnetRangesQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        use db::schema::vpc_subnet::dsl;

        // Create the base `candidate` from values provided that need no
        // verificiation.
        out.push_sql("SELECT * FROM (WITH candidate(");
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
        out.push_sql(",");
        out.push_identifier(dsl::rcgen::NAME)?;
        out.push_sql(") AS (VALUES (");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.subnet.identity.id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, db::model::Name>(
            db::model::Name::ref_cast(self.subnet.name()),
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, String>(
            &self.subnet.identity.description,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.subnet.identity.time_created,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.subnet.identity.time_modified,
        )?;
        out.push_sql(", ");
        out.push_sql("NULL::TIMESTAMPTZ, ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.subnet.vpc_id)?;
        out.push_sql(", 0)), ");

        // Push the candidate IPv4 and IPv6 selection subqueries, which return
        // NULL if the corresponding address range overlaps.
        out.push_sql("candidate_ipv4(");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(") AS (");
        push_null_if_overlapping_ip_range(
            out.reborrow(),
            &self.subnet.vpc_id,
            &self.ipv4_block,
        )?;

        out.push_sql("), candidate_ipv6(");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(") AS (");
        push_null_if_overlapping_ip_range(
            out.reborrow(),
            &self.subnet.vpc_id,
            &self.ipv6_block,
        )?;
        out.push_sql(") ");

        // Select the entire set of candidate columns.
        out.push_sql(
            "SELECT * FROM candidate, candidate_ipv4, candidate_ipv6)",
        );
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
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
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
        out.push_identifier(dsl::rcgen::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ipv4_block::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ipv6_block::NAME)?;
        out.push_sql(") ");
        self.0.walk_ast(out)
    }
}

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type VpcSubnetFromClause = FromClause<db::schema::vpc_subnet::table>;
const VPC_SUBNET_FROM_CLAUSE: VpcSubnetFromClause = VpcSubnetFromClause::new();

#[cfg(test)]
mod test {
    use super::SubnetError;
    use crate::db::model::VpcSubnet;
    use ipnetwork::IpNetwork;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Ipv4Net;
    use omicron_common::api::external::Ipv6Net;
    use omicron_common::api::external::Name;
    use omicron_test_utils::dev;
    use std::convert::TryInto;
    use std::sync::Arc;
    use uuid::Uuid;

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
        let pool = Arc::new(crate::db::Pool::new(&logctx.log, &cfg));
        let db_datastore = Arc::new(
            crate::db::DataStore::new(&log, Arc::clone(&pool), None)
                .await
                .unwrap(),
        );

        // We should be able to insert anything into an empty table.
        assert!(
            matches!(db_datastore.vpc_create_subnet_raw(row).await, Ok(_)),
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
                db_datastore.vpc_create_subnet_raw(new_row).await,
                Err(SubnetError::OverlappingIpRange(IpNetwork::V4(_)))
            ),
            "Should not be able to insert new VPC subnet with the same IPv4 and IPv6 ranges"
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
            other_subnet_id,
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
            SubnetError::OverlappingIpRange(IpNetwork::from(ipv6_block.0)),
            "SubnetError variant should include the exact IP range that overlaps"
        );
        let new_row = VpcSubnet::new(
            other_subnet_id,
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
            SubnetError::OverlappingIpRange(IpNetwork::from(ipv4_block.0)),
            "SubnetError variant should include the exact IP range that overlaps"
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
                db_datastore.vpc_create_subnet_raw(new_row).await,
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
            matches!(db_datastore.vpc_create_subnet_raw(new_row).await, Ok(_)),
            "Should be able to insert new VPC Subnet with non-overlapping IP ranges"
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
