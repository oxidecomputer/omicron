// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Query for inserting a guest network interface.

use crate::app::MAX_NICS_PER_INSTANCE;
use crate::db;
use crate::db::model::IncompleteNetworkInterface;
use crate::db::queries::next_item::NextItem;
use crate::defaults::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::prelude::Column;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Insertable;
use diesel::QueryResult;
use ipnetwork::IpNetwork;
use ipnetwork::Ipv4Network;
use omicron_common::api::external;
use std::net::IpAddr;
use uuid::Uuid;

/// Errors related to inserting or attaching a NetworkInterface
#[derive(Debug)]
pub enum NetworkInterfaceError {
    /// The instance specified for this interface is already associated with a
    /// different VPC from this interface.
    InstanceSpansMultipleVpcs(Uuid),
    /// There are no available IP addresses in the requested subnet
    NoAvailableIpAddresses,
    /// An explicitly-requested IP address is already in use
    IpAddressNotAvailable(std::net::IpAddr),
    /// A primary key violation, which is intentionally caused in some cases
    /// during instance creation sagas.
    DuplicatePrimaryKey(Uuid),
    /// There are no slots available on the instance
    NoSlotsAvailable,
    /// Any other error
    External(external::Error),
}

impl NetworkInterfaceError {
    /// Construct a `NetworkInterfaceError` from a database error
    ///
    /// This catches the various errors that the `InsertNetworkInterfaceQuery`
    /// can generate, especially the intentional errors that indicate either IP
    /// address exhaustion or an attempt to attach an interface to an instance
    /// that is already associated with another VPC.
    pub fn from_pool(
        e: async_bb8_diesel::PoolError,
        interface: &IncompleteNetworkInterface,
    ) -> Self {
        use crate::db::error;
        use async_bb8_diesel::ConnectionError;
        use async_bb8_diesel::PoolError;
        use diesel::result::Error;
        match e {
            // Catch the specific errors designed to communicate the failures we
            // want to distinguish
            PoolError::Connection(ConnectionError::Query(
                Error::DatabaseError(_, _),
            )) => decode_database_error(e, interface),
            // Any other error at all is a bug
            _ => NetworkInterfaceError::External(
                error::public_error_from_diesel_pool(
                    e,
                    error::ErrorHandler::Server,
                ),
            ),
        }
    }

    /// Convert this error into an external one.
    pub fn into_external(self) -> external::Error {
        match self {
            NetworkInterfaceError::NoAvailableIpAddresses => {
                external::Error::invalid_request(
                    "No available IP addresses for interface",
                )
            }
            NetworkInterfaceError::InstanceSpansMultipleVpcs(_) => {
                external::Error::invalid_request(concat!(
                    "Networking may not span multiple VPCs, but the ",
                    "requested instance is associated with another VPC"
                ))
            }
            NetworkInterfaceError::IpAddressNotAvailable(ip) => {
                external::Error::invalid_request(&format!(
                    "The IP address '{}' is not available",
                    ip
                ))
            }
            NetworkInterfaceError::DuplicatePrimaryKey(id) => {
                external::Error::InternalError {
                    internal_message: format!(
                        "Found duplicate primary key '{}' when inserting network interface",
                        id
                    ),
                }
            }
            NetworkInterfaceError::NoSlotsAvailable => {
                external::Error::invalid_request(&format!(
                    "Instances may not have more than {} network interfaces",
                    MAX_NICS_PER_INSTANCE
                ))
            }
            NetworkInterfaceError::External(e) => e,
        }
    }
}

/// Decode an error from the database to determine why our NIC query failed.
///
/// When inserting network interfaces, we use the `InsertNetworkInterfaceQuery`,
/// which is designed to fail in particular ways depending on the requested
/// data. For example, if the client requests a new NIC on an instance, where
/// that instance already has a NIC from a VPC that's different from the new
/// one, we handle that here.
///
/// This function works by inspecting the detailed error messages, including
/// indexes used or constraints violated, to determine the cause of the failure.
/// As such, it naturally is extremely tightly coupled to the database itself,
/// including the software version and our schema.
fn decode_database_error(
    err: async_bb8_diesel::PoolError,
    interface: &IncompleteNetworkInterface,
) -> NetworkInterfaceError {
    use crate::db::error;
    use async_bb8_diesel::ConnectionError;
    use async_bb8_diesel::PoolError;
    use diesel::result::DatabaseErrorKind;
    use diesel::result::Error;

    // Error message generated when we attempt to insert an interface in a
    // different VPC from the interface(s) already associated with the instance
    const MULTIPLE_VPC_ERROR_MESSAGE: &str =
        r#"could not parse "" as type uuid: uuid: incorrect UUID length: "#;

    // Error message generated when we attempt to insert NULL in the `ip`
    // column, which only happens when we run out of IPs in the subnet.
    const IP_EXHAUSTION_ERROR_MESSAGE: &str =
        r#"null value in column "ip" violates not-null constraint"#;

    // The name of the index whose uniqueness is violated if we try to assign an
    // IP that is already allocated to another interface in the same subnet.
    const IP_NOT_AVAILABLE_CONSTRAINT: &str =
        "network_interface_subnet_id_ip_key";

    // The name of the index whose uniqueness is violated if we try to assign a
    // name to an interface that is already used for another interface on the
    // same instance.
    const NAME_CONFLICT_CONSTRAINT: &str =
        "network_interface_instance_id_name_key";

    // The primary key constraint. This is intended only to be caught, and
    // usually ignored, in sagas. UUIDs are allocated in one node of the saga,
    // and then the NICs in a following node. In that case, primary key
    // conflicts would be expected, if we're recovering from a crash during that
    // node and replaying it, without first having unwound the whole saga. This
    // should _only_ be ignored in that case. In any other circumstance, this
    // should likely be converted to a 500-level server error
    const PRIMARY_KEY_CONSTRAINT: &str = "primary";

    // The check  violated in the case where we try to insert more that the
    // maximum number of NICs (`MAX_NICS_PER_INSTANCE`).
    const NO_SLOTS_AVAILABLE_ERROR_MESSAGE: &str = concat!(
        "failed to satisfy CHECK constraint ",
        "((slot >= 0:::INT8) AND (slot < 8:::INT8))",
    );

    match err {
        // If the address allocation subquery fails, we'll attempt to insert
        // NULL for the `ip` column. This checks that the non-NULL constraint on
        // that colum has been violated.
        PoolError::Connection(ConnectionError::Query(
            Error::DatabaseError(DatabaseErrorKind::NotNullViolation, ref info),
        )) if info.message() == IP_EXHAUSTION_ERROR_MESSAGE => {
            NetworkInterfaceError::NoAvailableIpAddresses
        }

        // This catches the error intentionally introduced by the
        // `push_ensure_unique_vpc_expression` subquery, which generates a
        // UUID parsing error if an instance is already associated with
        // another VPC.
        PoolError::Connection(ConnectionError::Query(
            Error::DatabaseError(DatabaseErrorKind::Unknown, ref info),
        )) if info.message() == MULTIPLE_VPC_ERROR_MESSAGE => {
            NetworkInterfaceError::InstanceSpansMultipleVpcs(
                interface.instance_id,
            )
        }

        // This checks the constraint on the interface slot numbers, used to
        // limit instances to a maximum number.
        PoolError::Connection(ConnectionError::Query(
            Error::DatabaseError(DatabaseErrorKind::CheckViolation, ref info),
        )) if info.message() == NO_SLOTS_AVAILABLE_ERROR_MESSAGE => {
            NetworkInterfaceError::NoSlotsAvailable
        }

        // This path looks specifically at constraint names.
        PoolError::Connection(ConnectionError::Query(
            Error::DatabaseError(DatabaseErrorKind::UniqueViolation, ref info),
        )) => match info.constraint_name() {
            // Constraint violated if a user-requested IP address has
            // already been assigned within the same VPC Subnet.
            Some(constraint) if constraint == IP_NOT_AVAILABLE_CONSTRAINT => {
                let ip = interface
                    .ip
                    .unwrap_or_else(|| std::net::Ipv4Addr::UNSPECIFIED.into());
                NetworkInterfaceError::IpAddressNotAvailable(ip)
            }

            // Constraint violated if the user-requested name is already
            // assigned to an interface on this instance.
            Some(constraint) if constraint == NAME_CONFLICT_CONSTRAINT => {
                NetworkInterfaceError::External(
                    error::public_error_from_diesel_pool(
                        err,
                        error::ErrorHandler::Conflict(
                            external::ResourceType::NetworkInterface,
                            interface.identity.name.as_str(),
                        ),
                    ),
                )
            }

            // Primary key constraint violation. See notes above.
            Some(constraint) if constraint == PRIMARY_KEY_CONSTRAINT => {
                NetworkInterfaceError::DuplicatePrimaryKey(
                    interface.identity.id,
                )
            }

            // Any other constraint violation is a bug
            _ => NetworkInterfaceError::External(
                error::public_error_from_diesel_pool(
                    err,
                    error::ErrorHandler::Server,
                ),
            ),
        },

        // Any other error at all is a bug
        _ => NetworkInterfaceError::External(
            error::public_error_from_diesel_pool(
                err,
                error::ErrorHandler::Server,
            ),
        ),
    }
}

// Helper to return the offset of the last valid/allocatable IP in a subnet.
// Note that this is the offset from the _first available address_, not the
// network address.
fn last_address_offset(subnet: &IpNetwork) -> u32 {
    // Generate last address in the range.
    //
    // NOTE: First subtraction is to convert from the subnet size to an
    // offset, since `generate_series` is inclusive of the last value.
    // Example: 256 -> 255.
    let last_address_offset = match subnet {
        IpNetwork::V4(network) => network.size() - 1,
        IpNetwork::V6(network) => {
            // TODO-robustness: IPv6 subnets are always /64s, so in theory we
            // could require searching all ~2^64 items for the next address.
            // That won't happen in practice, because there will be other limits
            // on the number of IPs (such as MAC addresses, or just project
            // accounting limits). However, we should update this to be the
            // actual maximum size we expect or want to support, once we get a
            // better sense of what that is.
            u32::try_from(network.size() - 1).unwrap_or(u32::MAX - 1)
        }
    };

    // This subtraction is because the last address in a subnet is
    // explicitly reserved for Oxide use.
    last_address_offset
        .checked_sub(1 + NUM_INITIAL_RESERVED_IP_ADDRESSES as u32)
        .unwrap_or_else(|| panic!("Unexpectedly small IP subnet: '{}'", subnet))
}

// Return the first available address in a subnet. This is not the network
// address, since Oxide reserves the first few addresses.
fn first_available_address(subnet: &IpNetwork) -> IpAddr {
    match subnet {
        IpNetwork::V4(network) => network
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as _)
            .unwrap_or_else(|| {
                panic!("Unexpectedly small IPv4 subnetwork: '{}'", network)
            })
            .into(),
        IpNetwork::V6(network) => {
            // TODO-performance: This is unfortunate. `ipnetwork` implements a
            // direct addition-based approach for IPv4 but not IPv6. This will
            // loop, which, while it may not matter much, can be nearly
            // trivially avoided by converting to u128, adding, and converting
            // back. Given that these spaces can be _really_ big, that is
            // probably worth doing.
            network
                .iter()
                .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as _)
                .unwrap_or_else(|| {
                    panic!("Unexpectedly small IPv4 subnetwork: '{}'", network)
                })
                .into()
        }
    }
}

/// The `NextGuestIpv4Address` query is a `NextItem` query for choosing the next
/// available IPv4 address for a guest interface.
#[derive(Debug, Clone, Copy)]
pub struct NextGuestIpv4Address {
    inner: NextItem<
        db::schema::network_interface::table,
        IpNetwork,
        db::schema::network_interface::dsl::ip,
        Uuid,
        db::schema::network_interface::dsl::subnet_id,
    >,
}

impl NextGuestIpv4Address {
    pub fn new(subnet: Ipv4Network, subnet_id: Uuid) -> Self {
        let subnet = IpNetwork::from(subnet);
        let net = IpNetwork::from(first_available_address(&subnet));
        let max_offset = last_address_offset(&subnet);
        Self { inner: NextItem::new_scoped(net, subnet_id, max_offset) }
    }
}

delegate_query_fragment_impl!(NextGuestIpv4Address);

/// A `[NextItem`] subquery that selects the next empty slot for an interface.
///
/// Instances are limited to 8 interfaces (RFD 135). This pushes a subquery that
/// looks like:
///
/// ```sql
/// SELECT COALESCE((
///     SELECT
///         next_slot
///     FROM
///         generate_series(0, <max nics per instance>)
///     AS
///         next_slot
///     LEFT OUTER JOIN
///         network_interface
///     ON
///         (instance_id, time_deleted IS NULL, slot) =
///         (<instance_id>, TRUE, next_slot)
///     WHERE
///         slot IS NULL
///     LIMIT 1)
/// ), 0)
/// ```
///
/// That is, we select the lowest slot that has not yet been claimed by an
/// interface on this instance, or zero if there is no such instance at all.
///
/// Errors
/// ------
///
/// Note that the `generate_series` function is inclusive of its upper bound.
/// We intentionally use the upper bound of the maximum number of NICs per
/// instance. In the case where there are no available slots (the current max
/// slot number is 7), this query will return 8. However, this violates the
/// check on the slot column being between `[0, 8)`. This check violation is
/// used to detect the case when there are no slots available.
#[derive(Debug, Clone, Copy)]
pub struct NextNicSlot {
    inner: NextItem<
        db::schema::network_interface::table,
        i16,
        db::schema::network_interface::dsl::slot,
        Uuid,
        db::schema::network_interface::dsl::instance_id,
    >,
}

impl NextNicSlot {
    pub fn new(instance_id: Uuid) -> Self {
        Self {
            inner: NextItem::new_scoped(0, instance_id, MAX_NICS_PER_INSTANCE),
        }
    }
}

impl QueryFragment<Pg> for NextNicSlot {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        out.push_sql("SELECT COALESCE((");
        self.inner.walk_ast(out.reborrow())?;
        out.push_sql("), 0)");
        Ok(())
    }
}

/// Add a subquery intended to verify that an Instance's networking does not
/// span multiple VPCs.
///
/// As described in RFD 21, an Instance's networking is confined to a single
/// VPC. That is, any NetworkInterfaces attached to an Instance must all have
/// the same VPC ID. This function adds a subquery, shown below, that fails in a
/// specific way (parsing error) if that invariant is violated. The basic
/// structure of the query is:
///
/// ```text
/// CAST(IF(<instance is in one VPC>, '<vpc_id>', '') AS UUID)
/// ```
///
/// This selects either the actual VPC UUID (as a string) or the empty string,
/// if any existing VPC IDs for this instance are the same. If true, we cast the
/// VPC ID string back to a UUID. If false, we try to cast the empty string,
/// which fails in a detectable way.
///
/// Details
/// -------
///
/// The exact query generated looks like this:
///
/// ```sql
/// CAST(IF(
///      COALESCE(
///          (
///             SELECT vpc_id
///             FROM network_interface
///             WHERE
///                 time_deleted IS NULL AND
///                 instance_id = <instance_id>
///             LIMIT 1
///          ),
///          <vpc_id>
///      ) = <vpc_id>,
///      '<vpc_id>', -- UUID as a string
///      ''
/// ) AS UUID)
/// ```
///
/// This uses a partial index on the `network_interface` table to look up the
/// first record with the provided `instance_id`, if any. It then compares that
/// stored `vpc_id` to the one provided to this query. If those IDs match, then
/// the ID is returned. If they do _not_ match, the `IF` statement returns an
/// empty string, which it tries to cast as a UUID. That fails, in a detectable
/// way, so that we can check this case as distinct from other errors.
///
/// Note that the `COALESCE` expression is there to handle the case where there
/// _is_ no record with the given `instance_id`. In that case, the `vpc_id`
/// provided is returned directly, so everything works as if the IDs matched.
fn push_ensure_unique_vpc_expression<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    vpc_id: &'a Uuid,
    vpc_id_str: &'a String,
    instance_id: &'a Uuid,
) -> diesel::QueryResult<()> {
    use db::schema::network_interface::dsl;

    out.push_sql("CAST(IF(COALESCE((SELECT ");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(" FROM ");
    NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(" IS NULL AND ");
    out.push_identifier(dsl::instance_id::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(instance_id)?;
    out.push_sql(" LIMIT 1), ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(vpc_id)?;
    out.push_sql(") = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(vpc_id)?;
    out.push_sql(", ");

    // NOTE: This bind-parameter is intentionally a string, rather than a UUID.
    //
    // This query relies on the fact that it generates a parsing error in the
    // case where there is an interface attached to a VPC that's _different_
    // from the VPC of the candidate interface. This is so that we can
    // distinguish this error case from the one where there is no IP address
    // available.
    //
    // To do that, we generate a query like:
    //
    // ```
    // CAST(IF(<instance VPC is the same>, '<vpc_id>', '') AS UUID)
    // ```
    //
    // That empty-string cannot be cast to a UUID, so we get a parsing error,
    // but only if the condition _succeeds_. It's not evaluated otherwise.
    //
    // However, if we push this parameter as a UUID explicitly, the database
    // looks at the parts of the `IF` statement, and tries to make them a common
    // type, a UUID. That's the exact error we're trying to produce, but it's
    // evaluated too early. So we ensure both are strings here, and then ask the
    // DB to cast them after that condition is evaluated.
    out.push_bind_param::<sql_types::Text, String>(vpc_id_str)?;
    out.push_sql(", '') AS UUID)");
    Ok(())
}

/// Subquery used to insert a _new_ `NetworkInterface` from parameters.
///
/// This function is used to construct a query that allows inserting a
/// `NetworkInterface`, supporting both optionally allocating a new IP address
/// and verifying that the attached instance's networking is contained within a
/// single VPC. The general query looks like:
///
/// ```sql
/// <instance validation CTE>
/// SELECT <id> AS id, <name> AS name, <description> AS description,
///        <time_created> AS time_created, <time_modified> AS time_modified,
///        NULL AS time_deleted, <instance_id> AS instance_id, <vpc_id> AS vpc_id,
///        <subnet_id> AS subnet_id, <mac> AS mac, <maybe IP allocation
///        subquery>
/// ```
///
/// Instance validation
/// -------------------
///
/// This query generates a CTE that checks that the requested instance is not
/// already associated with another VPC (since an instance's networking cannot
/// span multiple VPCs). This query is designed to fail in a particular way if
/// that invariant is violated, so that we can detect and report that case to
/// the user. See [`push_ensure_unique_vpc_expression`] for details of that
/// subquery, including how it fails.
///
/// IP allocation subquery
/// ----------------------
///
/// If the user explicitly requests an IP address, this part of the query is
/// just that exact IP. The query may still fail if the IP is already in use,
/// which is detected and forwarded as a client error.
///
/// If the user wants an address allocated, then this generates a subquery that
/// tries to find the next available IP address (if any). See
/// [`NextGuestIpv4Address`] for details on that allocation subquery. If that
/// fails, due to address exhaustion, this is detected and forwarded to the
/// caller.
///
/// Errors
/// ------
///
/// See [`NetworkInterfaceError`] for the errors caught and propagated by this
/// query.
///
/// Notes
/// -----
///
/// This query is designed so that, if the instance-validation subquery fails,
/// we do not run the address allocation query. This is just for performance;
/// since the allocation query runs in a time proportional to the smallest
/// unallocated address in the subnet, we'd like to avoid that if the query will
/// just fail the other, VPC-validation check.
///
/// It's not easy to verify that this is indeed the case, since running `EXPLAIN
/// ANALYZE` to get details about the number of rows read can't work, as the
/// query will fail. By putting this in a CTE, prior to the rest of the main
/// query, it seems likely that the database will run that portion first. In
/// particular, [this
/// note](https://www.cockroachlabs.com/docs/v21.2/subqueries#performance-best-practices)
/// claims that scalar subqueries, which generate a single value as this one
/// does, are completely executed and stored in memory before the surrounding
/// query starts. Thus the instance-validation subquery should run entirely
/// before the remainder of the query.
///
/// It's still possible that the engine runs the IP allocation subquery too,
/// either before or concurrently with the instance-validation subquery. It's
/// not clear how to test for this. But if this does become obvious, that
/// portion of the query might need to be placed behind a conditional evaluation
/// expression, such as `IF` or `COALESCE`, which only runs the subquery when
/// the instance-validation check passes.
fn push_interface_allocation_subquery<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    query: &'a InsertNetworkInterfaceQuery,
) -> diesel::QueryResult<()> {
    use db::schema::network_interface::dsl;
    // Push the CTE that ensures that any other interface with the same
    // instance_id also has the same vpc_id. See
    // `push_ensure_unique_vpc_expression` for more details. This ultimately
    // fails the query if the requested instance is already associated with
    // a different VPC.
    out.push_sql("WITH vpc(");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(") AS ");
    out.push_sql("(SELECT ");
    push_ensure_unique_vpc_expression(
        out.reborrow(),
        &query.interface.vpc_id,
        &query.vpc_id_str,
        &query.interface.instance_id,
    )?;
    out.push_sql(") ");

    // Push the columns, values and names, that are named directly. These
    // are known regardless of whether we're allocating an IP address. These
    // are all written as `SELECT <value1> AS <name1>, <value2> AS <name2>, ...
    out.push_sql("SELECT ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(&query.interface.identity.id)?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::id::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Text, db::model::Name>(
        &query.interface.identity.name,
    )?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::name::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Text, String>(
        &query.interface.identity.description,
    )?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::description::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(&query.now)?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::time_created::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(&query.now)?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::time_modified::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Nullable<sql_types::Timestamptz>, Option<DateTime<Utc>>>(&None)?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Uuid, Uuid>(&query.interface.instance_id)?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::instance_id::NAME)?;
    out.push_sql(", ");

    // Tiny subquery to select the `vpc_id` from the preceding CTE.
    out.push_sql("(SELECT vpc_id FROM vpc) AS ");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Uuid, Uuid>(
        &query.interface.subnet.identity.id,
    )?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::subnet_id::NAME)?;
    out.push_sql(", ");

    out.push_bind_param::<sql_types::Text, String>(&query.mac_sql)?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::mac::NAME)?;
    out.push_sql(", ");

    // If the user specified an IP address, then insert it by value. If they
    // did not, meaning we're allocating the next available one on their
    // behalf, then insert that subquery here.
    if let Some(ref ip) = &query.ip_sql {
        out.push_bind_param::<sql_types::Inet, IpNetwork>(ip)?;
    } else {
        out.push_sql("(");
        query.next_ipv4_address_subquery.walk_ast(out.reborrow())?;
        out.push_sql(")");
    }
    out.push_sql(" AS ");
    out.push_identifier(dsl::ip::NAME)?;

    // Push the suqbuery used to select and validate the slot number for the
    // interface, including validating that there are available slots on the
    // instance.
    out.push_sql(", (");
    query.next_slot_subquery.walk_ast(out.reborrow())?;
    out.push_sql(") AS ");
    out.push_identifier(dsl::slot::NAME)?;

    Ok(())
}

/// Type used to insert conditionally insert a network interface.
///
/// This type implements a query that does one of two things
///
/// - Insert a new network interface, performing validation and possibly IP
/// allocation
/// - Return an existing interface record, if it has the same primary key.
///
/// The first case is implemented in the [`push_interface_allocation_subquery`]
/// function. See that function's documentation for the details.
///
/// The second case is implemented in this type's `walk_ast` method.
///
/// Details
/// -------
///
/// The `push_interface_allocation_subquery` performs a number of validations on
/// the data provided, such as verifying that a requested IP address isn't
/// already assigned, or ensuring that the instance that will receive this
/// interface isn't already associated with another VPC.
///
/// However, the query is also meant to run during an instance creation saga. In
/// that case, the guardrails and unique indexes on this table make it
/// impossible for the query to both be idempotent and also catch these
/// constraints.
///
/// For example, imaging the instance creation saga crashes partway through
/// allocation a list of NICs. That node of the saga will be replayed during
/// saga recovery. This will create a record with exactly the same UUID for each
/// interface, which will ulimately result in a conflicting primary key error
/// from the database. This is both intentional and integral to the sagas
/// correct functioning. We catch this error deliberately, assuming that the
/// uniqueness of 128-bit UUIDs guarantees that the only practical situation
/// under which this can occur is a saga node replay after a crash.
///
/// Query structure
/// ---------------
///
/// This query looks like the following:
///
/// ```text
/// SELECT (candidate).* FROM (SELECT COALESCE(
///     <existing interface, if it has the same primary key>,
///     <subquery to insert new interface, with data validation, and return it>
/// )
/// ```
///
/// That is, we return the exact record that's already in the database, if there
/// is one, or run the entire validating query otherwise. In the context of
/// sagas, this is helpful because we generate the UUIDs for the interfaces in a
/// separate saga node, prior to inserting any interfaces. So if we have a
/// record with that exact UUID, we assert that it must be the record from the
/// saga itself. Note that, at this point, we return only the primary key, since
/// it's sufficiently unlikely that there's an existing key whose other data
/// does _not_ match the data we wanted to insert in a saga.
///
/// The odd syntax in the initial section, `SELECT (candidate).*` is because the
/// result of the `COALESCE` expression is a tuple. That is CockroachDB's syntax
/// for expanding a tuple into its constituent columns.
///
/// Note that the result of this expression is ultimately inserted into the
/// `network_interface` table. The way that fails (VPC-validation, IP
/// exhaustion, primary key violation), is used for either forwarding an error
/// on to the client (in the case of IP exhaustion, for example), or continuing
/// with a saga (for PK uniqueness violations). See [`NetworkInterfaceError`]
/// for a summary of the error conditions and their meaning, and the functions
/// constructing the subqueries in this type for more details.
#[derive(Debug, Clone)]
pub struct InsertNetworkInterfaceQuery {
    interface: IncompleteNetworkInterface,
    now: DateTime<Utc>,

    // The following fields are derived from the previous fields. This begs the
    // question: "Why bother storing them at all?"
    //
    // Diesel's [`diesel::query_builder::ast_pass::AstPass:push_bind_param`] method
    // requires that the provided value now live as long as the entire AstPass
    // type. By storing these values in the struct, they'll live at least as
    // long as the entire call to [`QueryFragment<Pg>::walk_ast`].
    vpc_id_str: String,
    ip_sql: Option<IpNetwork>,
    mac_sql: String,
    next_ipv4_address_subquery: NextGuestIpv4Address,
    next_slot_subquery: NextNicSlot,
}

impl InsertNetworkInterfaceQuery {
    pub fn new(interface: IncompleteNetworkInterface) -> Self {
        let vpc_id_str = interface.vpc_id.to_string();
        let ip_sql = interface.ip.map(|ip| ip.into());
        let mac_sql = interface.mac.to_string();

        let next_ipv4_address_subquery = NextGuestIpv4Address::new(
            interface.subnet.ipv4_block.0 .0,
            interface.subnet.identity.id,
        );
        let next_slot_subquery = NextNicSlot::new(interface.instance_id);

        Self {
            interface,
            now: Utc::now(),
            vpc_id_str,
            ip_sql,
            mac_sql,
            next_ipv4_address_subquery,
            next_slot_subquery,
        }
    }
}

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type NetworkInterfaceFromClause =
    FromClause<db::schema::network_interface::table>;
const NETWORK_INTERFACE_FROM_CLAUSE: NetworkInterfaceFromClause =
    NetworkInterfaceFromClause::new();

impl QueryId for InsertNetworkInterfaceQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Insertable<db::schema::network_interface::table>
    for InsertNetworkInterfaceQuery
{
    type Values = InsertNetworkInterfaceQueryValues;

    fn values(self) -> Self::Values {
        InsertNetworkInterfaceQueryValues(self)
    }
}

impl QueryFragment<Pg> for InsertNetworkInterfaceQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        use db::schema::network_interface::dsl;
        let push_columns =
            |mut out: AstPass<'_, 'a, Pg>| -> diesel::QueryResult<()> {
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
                out.push_identifier(dsl::instance_id::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::vpc_id::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::subnet_id::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::mac::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::ip::NAME)?;
                out.push_sql(", ");
                out.push_identifier(dsl::slot::NAME)?;
                Ok(())
            };

        out.push_sql("SELECT (candidate).* FROM (SELECT COALESCE((");

        // Add subquery to find exactly the record we might have already
        // inserted during a saga.
        out.push_sql("SELECT ");
        push_columns(out.reborrow())?;
        out.push_sql(" FROM ");
        NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.identity.id,
        )?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL)");

        // Push the main, data-validating subquery.
        out.push_sql(", (");
        push_interface_allocation_subquery(out.reborrow(), &self)?;
        out.push_sql(")) AS candidate)");
        Ok(())
    }
}

/// Type used to add the results of the `InsertNetworkInterfaceQuery` as values
/// in a Diesel statement, e.g., `insert_into(network_interface).values(query).`
/// Not for direct use.
pub struct InsertNetworkInterfaceQueryValues(InsertNetworkInterfaceQuery);

impl QueryId for InsertNetworkInterfaceQueryValues {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::insertable::CanInsertInSingleQuery<Pg>
    for InsertNetworkInterfaceQueryValues
{
    fn rows_to_insert(&self) -> Option<usize> {
        Some(1)
    }
}

impl QueryFragment<Pg> for InsertNetworkInterfaceQueryValues {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
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
        out.push_identifier(dsl::time_deleted::NAME)?;
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
        out.push_sql(", ");
        out.push_identifier(dsl::slot::NAME)?;
        out.push_sql(") ");
        self.0.walk_ast(out)
    }
}

#[cfg(test)]
mod tests {
    use super::first_available_address;
    use super::last_address_offset;
    use super::NetworkInterfaceError;
    use super::MAX_NICS_PER_INSTANCE;
    use crate::context::OpContext;
    use crate::db::model;
    use crate::db::model::IncompleteNetworkInterface;
    use crate::db::model::NetworkInterface;
    use crate::db::model::VpcSubnet;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Ipv4Net;
    use omicron_common::api::external::Ipv6Net;
    use omicron_test_utils::dev;
    use std::convert::TryInto;
    use std::net::IpAddr;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_insert_network_interface_query() {
        // Setup the test database
        let logctx = dev::test_setup_log("test_insert_network_interface_query");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&cfg));
        let db_datastore =
            Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));
        let opctx = OpContext::for_tests(log.new(o!()), db_datastore.clone());

        // Two test VpcSubnets, in different VPCs. The IPv4 range has space for
        // 16 addresses, less the 6 that are reserved.
        let ipv4_block = Ipv4Net("172.30.0.0/28".parse().unwrap());
        let ipv6_block = Ipv6Net("fd12:3456:7890::/64".parse().unwrap());
        let subnet_name = "subnet-a".to_string().try_into().unwrap();
        let other_subnet_name = "subnet-b".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let other_vpc_id =
            "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let other_subnet_id =
            "695debcc-e197-447d-ffb2-976150a7b7cf".parse().unwrap();
        let subnet = VpcSubnet::new(
            subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: subnet_name,
                description: description.to_string(),
            },
            ipv4_block,
            ipv6_block,
        );
        let other_subnet = VpcSubnet::new(
            other_subnet_id,
            other_vpc_id,
            IdentityMetadataCreateParams {
                name: other_subnet_name,
                description: description.to_string(),
            },
            ipv4_block,
            ipv6_block,
        );

        // Insert a network interface with a known valid IP address, attached to
        // a specific instance.
        let instance_id =
            "90d8542f-52dc-cacb-fa2b-ea0940d6bcb7".parse().unwrap();
        let requested_ip = "172.30.0.5".parse().unwrap();
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet.clone(),
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-a".parse().unwrap(),
                description: String::from("description"),
            },
            Some(requested_ip),
        )
        .unwrap();
        let inserted_interface = db_datastore
            .instance_create_network_interface_raw(&opctx, interface.clone())
            .await
            .expect("Failed to insert interface with known-good IP address");
        assert_interfaces_eq(&interface, &inserted_interface);
        assert_eq!(
            inserted_interface.ip.ip(),
            requested_ip,
            "The requested IP address should be available when no interfaces exist in the table"
        );

        // Insert an interface on the same instance, but with an
        // automatically-assigned IP address. It should have the next address.
        let expected_address =
            "172.30.0.6".parse::<std::net::IpAddr>().unwrap();
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet.clone(),
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-b".parse().unwrap(),
                description: String::from("description"),
            },
            None,
        )
        .unwrap();
        let inserted_interface = db_datastore
            .instance_create_network_interface_raw(&opctx, interface.clone())
            .await
            .expect("Failed to insert interface with known-good IP address");
        assert_interfaces_eq(&interface, &inserted_interface);
        assert_eq!(
            inserted_interface.ip.ip(),
            expected_address,
            "Failed to automatically assign the next available IP address"
        );

        // Inserting an interface with the same IP should fail.
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet.clone(),
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            Some(requested_ip),
        )
        .unwrap();
        let result = db_datastore
            .instance_create_network_interface_raw(&opctx, interface)
            .await;
        assert!(
            matches!(
                result,
                Err(NetworkInterfaceError::IpAddressNotAvailable(_))
            ),
            "Requesting an interface with an existing IP should fail"
        );

        // Inserting an interface with a new IP but the same name should
        // generate an invalid request error.
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet.clone(),
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-b".parse().unwrap(),
                description: String::from("description"),
            },
            None,
        )
        .unwrap();
        let result = db_datastore
            .instance_create_network_interface_raw(&opctx, interface)
            .await;
        assert!(
            matches!(
                result,
                Err(NetworkInterfaceError::External(Error::ObjectAlreadyExists { .. })),
            ),
            "Requesting an interface with the same name on the same instance should fail"
        );

        // Inserting an interface that is attached to the same instance, but in a different VPC,
        // should fail regardless of whether the IP is explicitly requested or allocated.
        for addr in [Some(expected_address), None] {
            let interface = IncompleteNetworkInterface::new(
                Uuid::new_v4(),
                instance_id,
                other_vpc_id,
                other_subnet.clone(),
                model::MacAddr::new().unwrap(),
                IdentityMetadataCreateParams {
                    name: "interface-a".parse().unwrap(),
                    description: String::from("description"),
                },
                addr,
            )
            .unwrap();
            let result = db_datastore
                .instance_create_network_interface_raw(&opctx, interface)
                .await;
            assert!(
                matches!(result, Err(NetworkInterfaceError::InstanceSpansMultipleVpcs(_))),
                "Attaching an interface to an instance which already has one in a different VPC should fail"
            );
        }

        // At this point, we should have allocated 2 addresses in this VPC
        // Subnet. That has a subnet of 172.30.0.0/28, so 16 total addresses are
        // available, but there are 6 reserved. Assert that we fail after
        // allocating 16 - 6 - 2 = 8 more interfaces, with an address exhaustion error.
        //
        // Note that we do this on _different_ instances, to avoid hitting the
        // per-instance limit of 8 NICs.
        for i in 0..8 {
            let interface = IncompleteNetworkInterface::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                vpc_id,
                subnet.clone(),
                model::MacAddr::new().unwrap(),
                IdentityMetadataCreateParams {
                    name: format!("interface-{}", i).try_into().unwrap(),
                    description: String::from("description"),
                },
                None,
            )
            .unwrap();
            let result = db_datastore
                .instance_create_network_interface_raw(&opctx, interface)
                .await;
            assert!(
                result.is_ok(),
                "We should be able to allocate 8 more interfaces successfully",
            );
        }
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet.clone(),
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-d".parse().unwrap(),
                description: String::from("description"),
            },
            None,
        )
        .unwrap();
        let result = db_datastore
            .instance_create_network_interface_raw(&opctx, interface)
            .await;
        assert!(
            matches!(
                result,
                Err(NetworkInterfaceError::NoAvailableIpAddresses)
            ),
            "Address exhaustion should be detected and handled"
        );

        // We should _not_ fail to allocate two interfaces with the same name if
        // they're in the same VPC and VPC Subnet, but on different instances.
        // another instance.
        for _ in 0..2 {
            let interface = IncompleteNetworkInterface::new(
                Uuid::new_v4(),
                Uuid::new_v4(), // New instance ID
                other_vpc_id,
                other_subnet.clone(),
                model::MacAddr::new().unwrap(),
                IdentityMetadataCreateParams {
                    name: "interface-e".parse().unwrap(), // Same name
                    description: String::from("description"),
                },
                None,
            )
            .unwrap();
            let result = db_datastore
                .instance_create_network_interface_raw(&opctx, interface)
                .await;
            assert!(
                result.is_ok(),
                concat!(
                    "Should be able to allocate multiple interfaces with the same name ",
                    "as long as they're attached to different instances",
                ),
            );
        }

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Test equality of a complete/inserted interface, for parts that are always known.
    fn assert_interfaces_eq(
        incomplete: &IncompleteNetworkInterface,
        inserted: &NetworkInterface,
    ) {
        use crate::db::identity::Resource;
        assert_eq!(inserted.id(), incomplete.identity.id);
        assert_eq!(inserted.name(), &incomplete.identity.name);
        assert_eq!(inserted.description(), incomplete.identity.description);
        assert_eq!(inserted.instance_id, incomplete.instance_id);
        assert_eq!(inserted.vpc_id, incomplete.vpc_id);
        assert_eq!(inserted.subnet_id, incomplete.subnet.id());
        assert_eq!(inserted.mac, incomplete.mac);
    }

    // Test that inserting a record into the database with the same primary key
    // returns the exact same record.
    //
    // This is an explicit test for the first expression within the `COALESCE`
    // part of the query. That is specifically designed to be executed during
    // sagas. In that case, we construct the UUIDs of each interface in one
    // action, and then in the next, create and insert each interface.
    #[tokio::test]
    async fn test_insert_network_interface_with_identical_primary_key() {
        // Setup the test database
        let logctx = dev::test_setup_log(
            "test_insert_network_interface_with_identical_primary_key",
        );
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&cfg));
        let db_datastore =
            Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));
        let opctx = OpContext::for_tests(log.new(o!()), db_datastore.clone());
        let ipv4_block = Ipv4Net("172.30.0.0/28".parse().unwrap());
        let ipv6_block = Ipv6Net("fd12:3456:7890::/64".parse().unwrap());
        let subnet_name = "subnet-a".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let subnet = VpcSubnet::new(
            subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: subnet_name,
                description: description.to_string(),
            },
            ipv4_block,
            ipv6_block,
        );
        let instance_id =
            "90d8542f-52dc-cacb-fa2b-ea0940d6bcb7".parse().unwrap();
        let requested_ip = "172.30.0.5".parse().unwrap();
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet,
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-a".parse().unwrap(),
                description: String::from("description"),
            },
            Some(requested_ip),
        )
        .unwrap();
        let inserted_interface = db_datastore
            .instance_create_network_interface_raw(&opctx, interface.clone())
            .await
            .expect("Failed to insert interface with known-good IP address");

        // Attempt to insert the exact same record again.
        let result = db_datastore
            .instance_create_network_interface_raw(&opctx, interface.clone())
            .await;
        if let Err(NetworkInterfaceError::DuplicatePrimaryKey(key)) = result {
            assert_eq!(key, inserted_interface.identity.id);
        } else {
            panic!(
                "Expected a NetworkInterfaceError::DuplicatePrimaryKey \
                error when inserting the exact same interface"
            );
        }

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Test that we fail to insert an interface if there are no available slots
    // on the instance.
    #[tokio::test]
    async fn test_limit_number_of_interfaces_per_instance_query() {
        // Setup the test database
        let logctx = dev::test_setup_log(
            "test_limit_number_of_interfaces_per_instance_query",
        );
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(crate::db::Pool::new(&cfg));
        let db_datastore =
            Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));
        let opctx = OpContext::for_tests(log.new(o!()), db_datastore.clone());
        let ipv4_block = Ipv4Net("172.30.0.0/26".parse().unwrap());
        let ipv6_block = Ipv6Net("fd12:3456:7890::/64".parse().unwrap());
        let subnet_name = "subnet-a".to_string().try_into().unwrap();
        let description = "some description".to_string();
        let vpc_id = "d402369d-c9ec-c5ad-9138-9fbee732d53e".parse().unwrap();
        let subnet_id = "093ad2db-769b-e3c2-bc1c-b46e84ce5532".parse().unwrap();
        let subnet = VpcSubnet::new(
            subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: subnet_name,
                description: description.to_string(),
            },
            ipv4_block,
            ipv6_block,
        );
        let instance_id =
            "90d8542f-52dc-cacb-fa2b-ea0940d6bcb7".parse().unwrap();
        for slot in 0..MAX_NICS_PER_INSTANCE {
            let interface = IncompleteNetworkInterface::new(
                Uuid::new_v4(),
                instance_id,
                vpc_id,
                subnet.clone(),
                model::MacAddr::new().unwrap(),
                IdentityMetadataCreateParams {
                    name: format!("interface-{}", slot).parse().unwrap(),
                    description: String::from("description"),
                },
                None,
            )
            .unwrap();
            let inserted_interface = db_datastore
                .instance_create_network_interface_raw(
                    &opctx,
                    interface.clone(),
                )
                .await
                .expect("Should be able to insert up to 8 interfaces");
            let actual_slot =
                u32::try_from(inserted_interface.slot).expect("Bad slot index");
            assert_eq!(
                slot, actual_slot,
                "Failed to allocate next available interface slot"
            );
        }

        // The next one should fail
        let interface = IncompleteNetworkInterface::new(
            Uuid::new_v4(),
            instance_id,
            vpc_id,
            subnet,
            model::MacAddr::new().unwrap(),
            IdentityMetadataCreateParams {
                name: "interface-8".parse().unwrap(),
                description: String::from("description"),
            },
            None,
        )
        .unwrap();
        let result = db_datastore
            .instance_create_network_interface_raw(&opctx, interface.clone())
            .await
            .expect_err("Should not be able to insert more than 8 interfaces");
        assert!(matches!(result, NetworkInterfaceError::NoSlotsAvailable,));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[test]
    fn test_last_address_offset() {
        let subnet = "172.30.0.0/28".parse().unwrap();
        assert_eq!(
            last_address_offset(&subnet),
            // /28 = 2 ** 4 = 16 total addresses
            // ... - 1 for converting from size to index = 15
            // ... - 1 for reserved broadcast address = 14
            // ... - 5 for reserved initial addresses = 9
            9,
        );
        let subnet = "fd00::/64".parse().unwrap();
        assert_eq!(
            last_address_offset(&subnet),
            u32::MAX - 1 - 1 - super::NUM_INITIAL_RESERVED_IP_ADDRESSES as u32,
        );
    }

    #[test]
    fn test_first_available_address() {
        let subnet = "172.30.0.0/28".parse().unwrap();
        assert_eq!(
            first_available_address(&subnet),
            "172.30.0.5".parse::<IpAddr>().unwrap(),
        );
        let subnet = "fd00::/64".parse().unwrap();
        assert_eq!(
            first_available_address(&subnet),
            "fd00::5".parse::<IpAddr>().unwrap(),
        );
    }
}
