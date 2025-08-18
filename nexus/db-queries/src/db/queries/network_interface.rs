// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries for inserting and deleting network interfaces.

use crate::db;
use crate::db::model::IncompleteNetworkInterface;
use crate::db::queries::next_item::DefaultShiftGenerator;
use crate::db::queries::next_item::{NextItem, NextItemSelfJoined};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::Insertable;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use diesel::pg::Pg;
use diesel::prelude::Column;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::query_builder::{AstPass, Query};
use diesel::result::Error as DieselError;
use diesel::sql_types::{self, Nullable};
use ipnetwork::IpNetwork;
use ipnetwork::Ipv4Network;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel, retryable};
use nexus_db_lookup::DbConnection;
use nexus_db_model::SqlU8;
use nexus_db_model::{MAX_NICS_PER_INSTANCE, NetworkInterfaceKind};
use nexus_db_schema::enums::NetworkInterfaceKindEnum;
use nexus_db_schema::schema::network_interface::dsl;
use omicron_common::api::external;
use omicron_common::api::external::MacAddr;
use slog_error_chain::SlogInlineError;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::LazyLock;
use uuid::Uuid;

// These are sentinel values and other constants used to verify the state of the
// system when operating on network interfaces

// States an instance must be in to operate on its network interfaces, in
// most situations.
const INSTANCE_STOPPED: db::model::InstanceState =
    db::model::InstanceState::NoVmm;

const INSTANCE_FAILED: db::model::InstanceState =
    db::model::InstanceState::Failed;

// An instance can be in the creating state while we manipulate its
// interfaces. The intention is for this only to be the case during sagas.
const INSTANCE_CREATING: db::model::InstanceState =
    db::model::InstanceState::Creating;

// A sentinel value for the instance state when the instance actually does
// not exist.
const INSTANCE_DESTROYED: db::model::InstanceState =
    db::model::InstanceState::Destroyed;

// A sentinel value for the instance state when the instance has an active
// VMM, irrespective of that VMM's actual state.
const INSTANCE_RUNNING: db::model::InstanceState =
    db::model::InstanceState::Vmm;

static NO_INSTANCE_SENTINEL_STRING: LazyLock<String> =
    LazyLock::new(|| String::from(NO_INSTANCE_SENTINEL));

static INSTANCE_BAD_STATE_SENTINEL_STRING: LazyLock<String> =
    LazyLock::new(|| String::from(INSTANCE_BAD_STATE_SENTINEL));

// Uncastable sentinel used to detect when an instance exists, but is not
// in the right state to have its network interfaces altered
const INSTANCE_BAD_STATE_SENTINEL: &'static str = "bad-state";

// Error message generated when we're attempting to operate on an instance,
// either inserting or deleting an interface, and that instance exists but is
// in a state we can't work on.
const INSTANCE_BAD_STATE_ERROR_MESSAGE: &'static str = "could not parse \"bad-state\" as type uuid: uuid: incorrect UUID length: bad-state";

// Uncastable sentinel used to detect when an instance doesn't exist
const NO_INSTANCE_SENTINEL: &'static str = "no-instance";

// Error message generated when we're attempting to operate on an instance,
// either inserting or deleting an interface, and that instance does not exist
// at all or has been destroyed. These are the same thing from the point of view
// of the client's API call.
const NO_INSTANCE_ERROR_MESSAGE: &'static str = "could not parse \"no-instance\" as type uuid: uuid: incorrect UUID length: no-instance";

/// Errors related to inserting or attaching a NetworkInterface
#[derive(Debug)]
pub enum InsertError {
    /// This interface already exists
    ///
    /// Note: An interface with the same name existing is a different error;
    /// this error matches the case where the UUID already exists.
    InterfaceAlreadyExists(String, NetworkInterfaceKind),
    /// The resource specified for this interface is already associated with a
    /// different VPC from this interface, e.g., the instance has a different
    /// interface that is associated with another VPC.
    ResourceSpansMultipleVpcs(Uuid),
    /// There are no available IP addresses in the requested subnet
    NoAvailableIpAddresses { name: String, id: Uuid },
    /// An explicitly-requested IP address is already in use
    IpAddressNotAvailable(std::net::IpAddr),
    /// An explicity-requested MAC address is already in use
    MacAddressNotAvailable(MacAddr),
    /// An explicity-requested interface slot is already in use
    SlotNotAvailable(u8),
    /// There are no slots available for a new interface
    NoSlotsAvailable,
    /// There are no MAC addresses available
    NoMacAddrressesAvailable,
    /// Multiple NICs must be in different VPC Subnets
    NonUniqueVpcSubnets,
    /// Instance must be stopped prior to adding interfaces to it
    InstanceMustBeStopped(Uuid),
    /// The instance does not exist at all, or is in the destroyed state.
    InstanceNotFound(Uuid),
    /// The operation occurred within a transaction, and is retryable
    Retryable(DieselError),
    /// Any other error
    External(external::Error),
}

impl InsertError {
    /// Construct an `InsertError` from a database error
    ///
    /// This catches the various errors that the `InsertQuery`
    /// can generate, especially the intentional errors that indicate either IP
    /// address exhaustion or an attempt to attach an interface to an instance
    /// that is already associated with another VPC.
    pub fn from_diesel(
        e: DieselError,
        interface: &IncompleteNetworkInterface,
    ) -> Self {
        match e {
            // Catch the specific errors designed to communicate the failures we
            // want to distinguish
            DieselError::DatabaseError(_, _) => {
                decode_database_error(e, interface)
            }
            // Any other error at all is a bug
            _ => InsertError::External(public_error_from_diesel(
                e,
                ErrorHandler::Server,
            )),
        }
    }

    /// Convert this error into an external one.
    pub fn into_external(self) -> external::Error {
        match self {
            InsertError::InterfaceAlreadyExists(
                name,
                NetworkInterfaceKind::Instance,
            ) => external::Error::ObjectAlreadyExists {
                type_name: external::ResourceType::InstanceNetworkInterface,
                object_name: name,
            },
            InsertError::InterfaceAlreadyExists(
                _name,
                NetworkInterfaceKind::Service,
            ) => {
                unimplemented!("service network interface")
            }
            InsertError::InterfaceAlreadyExists(
                _name,
                NetworkInterfaceKind::Probe,
            ) => {
                unimplemented!("probe network interface")
            }
            InsertError::NoAvailableIpAddresses { name, id } => {
                external::Error::invalid_request(format!(
                    "No available IP addresses for interface in \
                        subnet '{name}' with ID '{id}'"
                ))
            }
            InsertError::ResourceSpansMultipleVpcs(_) => {
                external::Error::invalid_request(concat!(
                    "Networking may not span multiple VPCs, but the ",
                    "requested resource is associated with another VPC"
                ))
            }
            InsertError::IpAddressNotAvailable(ip) => {
                external::Error::invalid_request(format!(
                    "The IP address '{}' is not available",
                    ip
                ))
            }
            InsertError::MacAddressNotAvailable(mac) => {
                external::Error::invalid_request(format!(
                    "The MAC address '{}' is not available",
                    mac
                ))
            }
            InsertError::SlotNotAvailable(slot) => {
                external::Error::invalid_request(format!(
                    "The interface slot '{slot}' is not available",
                ))
            }
            InsertError::NoSlotsAvailable => {
                external::Error::invalid_request(format!(
                    "May not attach more than {} network interfaces",
                    MAX_NICS_PER_INSTANCE
                ))
            }
            InsertError::NoMacAddrressesAvailable => {
                external::Error::invalid_request(
                    "No available MAC addresses for interface",
                )
            }
            InsertError::NonUniqueVpcSubnets => {
                external::Error::invalid_request(
                    "Each interface must be in a distinct VPC Subnet",
                )
            }
            InsertError::InstanceMustBeStopped(_) => {
                external::Error::invalid_request(
                    "Instance must be stopped to attach a new network interface",
                )
            }
            InsertError::InstanceNotFound(id) => {
                external::Error::not_found_by_id(
                    external::ResourceType::Instance,
                    &id,
                )
            }
            InsertError::Retryable(err) => {
                public_error_from_diesel(err, ErrorHandler::Server)
            }
            InsertError::External(e) => e,
        }
    }
}

/// Decode an error from the database to determine why our NIC query failed.
///
/// When inserting network interfaces, we use the `InsertQuery`,
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
    err: DieselError,
    interface: &IncompleteNetworkInterface,
) -> InsertError {
    use diesel::result::DatabaseErrorKind;

    // Error message generated when we attempt to insert an interface in a
    // different VPC from the interface(s) already associated with the instance
    const MULTIPLE_VPC_ERROR_MESSAGE: &str = concat!(
        r#"could not parse "multiple-vpcs" as type uuid: uuid: "#,
        r#"incorrect UUID length: multiple-vpcs"#,
    );

    // Error message generated when we attempt to insert NULL in the `ip`
    // column, which only happens when we run out of IPs in the subnet.
    const IP_EXHAUSTION_ERROR_MESSAGE: &str =
        r#"null value in column "ip" violates not-null constraint"#;

    // The name of the index whose uniqueness is violated if we try to assign an
    // IP that is already allocated to another interface in the same subnet.
    const IP_NOT_AVAILABLE_CONSTRAINT: &str =
        "network_interface_subnet_id_ip_key";

    // The name of the index whose uniqueness is violated if we try to assign a
    // MAC that is already allocated to another interface in the same VPC.
    const MAC_NOT_AVAILABLE_CONSTRAINT: &str =
        "network_interface_vpc_id_mac_key";

    // The name of the index whose uniqueness is violated if we try to assign a
    // slot to an interface that is already allocated to another interface in
    // the same instance or service.
    const SLOT_NOT_AVAILABLE_CONSTRAINT: &str =
        "network_interface_parent_id_slot_key";

    // The name of the index whose uniqueness is violated if we try to assign a
    // name to an interface that is already used for another interface on the
    // same resource.
    const NAME_CONFLICT_CONSTRAINT: &str =
        "network_interface_parent_id_name_kind_key";

    // The name of the constraint violated if we try to re-insert an already
    // existing interface with the same UUID.
    const ID_CONFLICT_CONSTRAINT: &str = "network_interface_pkey";

    // The check violated in the case where we try to insert more that the
    // maximum number of NICs (`MAX_NICS`).
    const NO_SLOTS_AVAILABLE_ERROR_MESSAGE: &str = concat!(
        "failed to satisfy CHECK constraint ",
        "((slot >= 0:::INT8) AND (slot < 8:::INT8))",
    );

    // Error message generated when we attempt to insert NULL in the `mac`
    // column, which only happens when we run out of MAC addresses. This is
    // probably quite unlikely, but not impossible given that MACs are unique
    // within an entire VPC. We'll probably have other constraints we hit first,
    // or explicit limits, but until those are in place, we opt for an explicit
    // message.
    const MAC_EXHAUSTION_ERROR_MESSAGE: &str =
        r#"null value in column "mac" violates not-null constraint"#;

    // Error message received when attempting to add an interface in a VPC
    // Subnet, where that resource already has an interface in that VPC Subnet.
    // This enforces the constraint that all interfaces are in distinct VPC
    // Subnets.
    const NON_UNIQUE_VPC_SUBNET_ERROR_MESSAGE: &str = concat!(
        r#"could not parse "non-unique-subnets" as type uuid: "#,
        r#"uuid: incorrect UUID length: non-unique-subnets"#,
    );

    if retryable(&err) {
        return InsertError::Retryable(err);
    }

    match err {
        // If the address allocation subquery fails, we'll attempt to insert
        // NULL for the `ip` column. This checks that the non-NULL constraint on
        // that colum has been violated.
        DieselError::DatabaseError(
            DatabaseErrorKind::NotNullViolation,
            info,
        ) if info.message() == IP_EXHAUSTION_ERROR_MESSAGE => {
            InsertError::NoAvailableIpAddresses {
                name: interface.subnet.identity.name.to_string(),
                id: interface.subnet.identity.id,
            }
        }

        // This catches the error intentionally introduced by the
        // `push_ensure_unique_vpc_expression` subquery, which generates a
        // UUID parsing error if the resource (e.g. instance) we want to attach
        // to is already associated with another VPC.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
            if info.message() == MULTIPLE_VPC_ERROR_MESSAGE =>
        {
            InsertError::ResourceSpansMultipleVpcs(interface.parent_id)
        }

        // This checks the constraint on the interface slot numbers, used to
        // limit total number of interfaces per resource to a maximum number.
        DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, info)
            if info.message() == NO_SLOTS_AVAILABLE_ERROR_MESSAGE =>
        {
            InsertError::NoSlotsAvailable
        }

        // If the MAC allocation subquery fails, we'll attempt to insert NULL
        // for the `mac` column. This checks that the non-NULL constraint on
        // that column has been violated.
        DieselError::DatabaseError(
            DatabaseErrorKind::NotNullViolation,
            info,
        ) if info.message() == MAC_EXHAUSTION_ERROR_MESSAGE => {
            InsertError::NoMacAddrressesAvailable
        }

        // This catches the error intentionally introduced by the
        // `push_ensure_unique_vpc_subnet_expression` subquery, which generates
        // a UUID parsing error if the resource has another interface in the VPC
        // Subnet of the one we're trying to insert.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
            if info.message() == NON_UNIQUE_VPC_SUBNET_ERROR_MESSAGE =>
        {
            InsertError::NonUniqueVpcSubnets
        }

        // This catches the UUID-cast failure intentionally introduced by
        // `push_instance_state_verification_subquery`, which verifies that
        // the instance is actually stopped when running this query.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
            if info.message() == INSTANCE_BAD_STATE_ERROR_MESSAGE =>
        {
            assert_eq!(interface.kind, NetworkInterfaceKind::Instance);
            InsertError::InstanceMustBeStopped(interface.parent_id)
        }
        // This catches the UUID-cast failure intentionally introduced by
        // `push_instance_state_verification_subquery`, which verifies that
        // the instance doesn't even exist when running this query.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
            if info.message() == NO_INSTANCE_ERROR_MESSAGE =>
        {
            assert_eq!(interface.kind, NetworkInterfaceKind::Instance);
            InsertError::InstanceNotFound(interface.parent_id)
        }

        // This path looks specifically at constraint names.
        DieselError::DatabaseError(
            DatabaseErrorKind::UniqueViolation,
            ref info,
        ) => match info.constraint_name() {
            // Constraint violated if a user-requested IP address has
            // already been assigned within the same VPC Subnet.
            Some(constraint) if constraint == IP_NOT_AVAILABLE_CONSTRAINT => {
                let ip = interface
                    .ip
                    .unwrap_or_else(|| std::net::Ipv4Addr::UNSPECIFIED.into());
                InsertError::IpAddressNotAvailable(ip)
            }
            // Constraint violated if a user-requested MAC address has
            // already been assigned within the same VPC.
            Some(constraint) if constraint == MAC_NOT_AVAILABLE_CONSTRAINT => {
                let mac = interface.mac.unwrap_or_else(|| MacAddr::from_i64(0));
                InsertError::MacAddressNotAvailable(mac)
            }
            // Constraint violated if a user-requested slot has
            // already been assigned within the same instance or service.
            Some(constraint) if constraint == SLOT_NOT_AVAILABLE_CONSTRAINT => {
                let slot = interface.slot.unwrap_or(0);
                InsertError::SlotNotAvailable(slot)
            }
            // Constraint violated if the user-requested name is already
            // assigned to an interface on this resource
            Some(constraint) if constraint == NAME_CONFLICT_CONSTRAINT => {
                let resource_type = match interface.kind {
                    NetworkInterfaceKind::Instance => {
                        external::ResourceType::InstanceNetworkInterface
                    }
                    NetworkInterfaceKind::Service => {
                        external::ResourceType::ServiceNetworkInterface
                    }
                    NetworkInterfaceKind::Probe => {
                        external::ResourceType::ProbeNetworkInterface
                    }
                };
                InsertError::External(
                    nexus_db_errors::public_error_from_diesel(
                        err,
                        nexus_db_errors::ErrorHandler::Conflict(
                            resource_type,
                            interface.identity.name.as_str(),
                        ),
                    ),
                )
            }
            // Constraint violated if the user-requested UUID has already
            // been inserted.
            Some(constraint) if constraint == ID_CONFLICT_CONSTRAINT => {
                InsertError::InterfaceAlreadyExists(
                    interface.identity.name.to_string(),
                    interface.kind,
                )
            }
            // Any other constraint violation is a bug
            _ => InsertError::External(
                nexus_db_errors::public_error_from_diesel(
                    err,
                    nexus_db_errors::ErrorHandler::Server,
                ),
            ),
        },

        // Any other error at all is a bug
        _ => InsertError::External(nexus_db_errors::public_error_from_diesel(
            err,
            nexus_db_errors::ErrorHandler::Server,
        )),
    }
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
            // NOTE: This call to `nth()` will loop and call the `next()`
            // implementation. That's inefficient, but the number of reserved
            // addresses is very small, so it should not matter.
            network
                .iter()
                .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES as _)
                .unwrap_or_else(|| {
                    panic!("Unexpectedly small IPv6 subnetwork: '{}'", network)
                })
                .into()
        }
    }
}

// Return the last available address in a subnet. This is not the broadcast
// address, since that is reserved.
fn last_available_address(subnet: &IpNetwork) -> IpAddr {
    // NOTE: In both cases below, we subtract 2 from the network size. That's
    // because we first subtract 1 to go from a size to an index, and then
    // another 1 because the broadcast address isn't valid for an interface.
    match subnet {
        IpNetwork::V4(network) => network
            .size()
            .checked_sub(2)
            .and_then(|n| network.nth(n))
            .map(IpAddr::V4)
            .unwrap_or_else(|| {
                panic!("Unexpectedly small IPv4 subnetwork: '{}'", network);
            }),
        IpNetwork::V6(network) => {
            // NOTE: The iterator implementation for `Ipv6Network` only
            // implements the required `Iterator::next()` method. That means we
            // get the default implementation of the `nth()` method, which will
            // loop and call `next()`. That is ridiculously inefficient, so we
            // manually compute the nth address through addition instead.
            let base = u128::from(network.network());
            let n = network.size().checked_sub(2).unwrap_or_else(|| {
                panic!("Unexpectedly small IPv6 subnetwork: '{}'", network);
            });
            IpAddr::V6(Ipv6Addr::from(base + n))
        }
    }
}

/// The `NextIpv4Address` query is a `NextItem` query for choosing the next
/// available IPv4 address for an interface.
#[derive(Debug, Clone, Copy)]
pub struct NextIpv4Address {
    inner: NextItemSelfJoined<
        nexus_db_schema::schema::network_interface::table,
        IpNetwork,
        nexus_db_schema::schema::network_interface::dsl::ip,
        Uuid,
        nexus_db_schema::schema::network_interface::dsl::subnet_id,
    >,
}

impl NextIpv4Address {
    pub fn new(subnet: Ipv4Network, subnet_id: Uuid) -> Self {
        let subnet = IpNetwork::from(subnet);
        let min = IpNetwork::from(first_available_address(&subnet));
        let max = IpNetwork::from(last_available_address(&subnet));
        Self { inner: NextItemSelfJoined::new_scoped(subnet_id, min, max) }
    }
}

delegate_query_fragment_impl!(NextIpv4Address);

/// A `NextItem` subquery that selects the next empty slot for an interface.
///
/// This pushes a subquery that looks like:
///
/// ```sql
/// SELECT COALESCE((
///     SELECT
///         next_slot
///     FROM
///         generate_series(0, <max nics per resource>)
///     AS
///         next_slot
///     LEFT OUTER JOIN
///         network_interface
///     ON
///         (parent_id, time_deleted IS NULL, slot) =
///         (<parent_id>, TRUE, next_slot)
///     WHERE
///         slot IS NULL
///     LIMIT 1)
/// ), 0)
/// ```
///
/// That is, we select the lowest slot that has not yet been claimed by an
/// interface on this instance, or zero if there is no such instance at all.
///
/// `parent_id` is the UUID of the parent resource (e.g., `instance_id`) the
/// interface will be associated with.
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
        nexus_db_schema::schema::network_interface::table,
        i16,
        nexus_db_schema::schema::network_interface::dsl::slot,
        Uuid,
        nexus_db_schema::schema::network_interface::dsl::parent_id,
    >,
}

impl NextNicSlot {
    pub fn new(parent_id: Uuid) -> Self {
        let generator = DefaultShiftGenerator::new(
            0,
            i64::try_from(MAX_NICS_PER_INSTANCE)
                .expect("Too many network interfaces"),
            0,
        )
        .expect("invalid min/max shift");
        Self { inner: NextItem::new_scoped(generator, parent_id) }
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

/// A `NextItem` query that selects a random available MAC address for
/// a network interface.
#[derive(Debug, Clone, Copy)]
pub struct NextMacAddress {
    inner: NextItemSelfJoined<
        nexus_db_schema::schema::network_interface::table,
        db::model::MacAddr,
        nexus_db_schema::schema::network_interface::dsl::mac,
        Uuid,
        nexus_db_schema::schema::network_interface::dsl::vpc_id,
    >,
}

impl NextMacAddress {
    pub fn new(vpc_id: Uuid, kind: NetworkInterfaceKind) -> Self {
        let (min, max) = match kind {
            NetworkInterfaceKind::Instance | NetworkInterfaceKind::Probe => {
                (MacAddr::MIN_GUEST_ADDR, MacAddr::MAX_GUEST_ADDR)
            }
            NetworkInterfaceKind::Service => {
                (MacAddr::MIN_SYSTEM_ADDR, MacAddr::MAX_SYSTEM_ADDR)
            }
        };
        let min = db::model::MacAddr(MacAddr::from_i64(min));
        let max = db::model::MacAddr(MacAddr::from_i64(max));
        Self { inner: NextItemSelfJoined::new_scoped(vpc_id, min, max) }
    }
}

delegate_query_fragment_impl!(NextMacAddress);

/// Add a subquery intended to verify that a resource's networking does not
/// span multiple VPCs.
///
/// As described in RFD 21, an Instance's networking is confined to a single
/// VPC. That is, any NetworkInterfaces attached to an Instance must all have
/// the same VPC ID. We apply that same restriction to any interface kind.
/// This function adds a subquery, shown below, that fails in a
/// specific way (parsing error) if that invariant is violated. The basic
/// structure of the query is:
///
/// ```text
/// CAST(IF(<resource is in one VPC>, '<vpc_id>', 'multiple-vpcs') AS UUID)
/// ```
///
/// This selects either the actual VPC UUID (as a string) or the literal string
/// "multiple-vpcs" if any existing VPC IDs for this resource are the same. If
/// true, we cast the VPC ID string back to a UUID. If false, we try to cast the
/// string `"multiple-vpcs"` which fails in a detectable way.
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
///                 parent_id = <parent_id> AND
///                 kind = <kind>
///             LIMIT 1
///          ),
///          <vpc_id>
///      ) = <vpc_id>,
///      '<vpc_id>', -- UUID as a string
///      'multiple-vpcs' -- The literal string "multiple-vpcs"
/// ) AS UUID)
/// ```
///
/// This uses a partial index on the `network_interface` table to look up the
/// first record with the provided `parent_id`, if any. It then compares that
/// stored `vpc_id` to the one provided to this query. If those IDs match, then
/// the ID is returned. If they do _not_ match, the `IF` statement returns the
/// string "multiple-vpcs", which it tries to cast as a UUID. That fails, in a
/// detectable way, so that we can check this case as distinct from other
/// errors.
///
/// Note that the `COALESCE` expression is there to handle the case where there
/// _is_ no record with the given `instance_id`. In that case, the `vpc_id`
/// provided is returned directly, so everything works as if the IDs matched.
fn push_ensure_unique_vpc_expression<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    vpc_id: &'a Uuid,
    vpc_id_str: &'a String,
    kind: &'a NetworkInterfaceKind,
    parent_id: &'a Uuid,
) -> diesel::QueryResult<()> {
    out.push_sql("CAST(IF(COALESCE((SELECT ");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(" FROM ");
    NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(" IS NULL AND ");
    out.push_identifier(dsl::parent_id::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(parent_id)?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::kind::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<NetworkInterfaceKindEnum, NetworkInterfaceKind>(
        kind,
    )?;
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
    // distinguish this error case from all the others.
    //
    // To do that, we generate a query like:
    //
    // ```
    // CAST(IF(<VPC is the same>, '<vpc_id>', 'multiple-vpcs') AS UUID)
    // ```
    //
    // The string "multiple-vpcs" cannot be cast to a UUID, so we get a parsing
    // error, but only if the condition _succeeds_. That conversion is not done
    // otherwise.
    //
    // However, if we push this parameter as a UUID explicitly, the database
    // looks at the parts of the `IF` statement, and tries to make them a common
    // type, a UUID. That's the exact error we're trying to produce, but it's
    // evaluated too early. So we ensure both are strings here, and then ask the
    // DB to cast them after that condition is evaluated.
    out.push_bind_param::<sql_types::Text, String>(vpc_id_str)?;
    out.push_sql(", 'multiple-vpcs') AS UUID)");
    Ok(())
}

/// Push a subquery that checks that all NICs for a resource are in distinct
/// VPC Subnets.
///
/// This generates a subquery like:
///
/// ```sql
/// CAST(IF(
///     EXISTS(
///        SELECT subnet_id
///        FROM network_interface
///        WHERE
///            id != <interface_id> AND
///            parent_id = <parent_id> AND
///            kind = <kind> AND
///            time_deleted IS NULL AND
///            subnet_id = <subnet_id>
///     ),
///     'non-unique-subnets', -- the literal string "non-unique-subnets",
///     '<subnet_id>', -- <subnet_id> as a string,
///     ) AS UUID
/// )
/// ```
///
/// That is, if the subnet ID provided in the query already exists for an
/// interface on the target instance, we return the literal string
/// `'non-unique-subnets'`, which will fail casting to a UUID.
fn push_ensure_unique_vpc_subnet_expression<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    interface_id: &'a Uuid,
    subnet_id: &'a Uuid,
    subnet_id_str: &'a String,
    kind: &'a NetworkInterfaceKind,
    parent_id: &'a Uuid,
) -> diesel::QueryResult<()> {
    out.push_sql("CAST(IF(EXISTS(SELECT ");
    out.push_identifier(dsl::subnet_id::NAME)?;
    out.push_sql(" FROM ");
    NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_identifier(dsl::id::NAME)?;
    out.push_sql(" != ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(interface_id)?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::parent_id::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(parent_id)?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::kind::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<NetworkInterfaceKindEnum, NetworkInterfaceKind>(
        kind,
    )?;
    out.push_sql(" AND ");
    out.push_identifier(dsl::time_deleted::NAME)?;
    out.push_sql(" IS NULL AND ");
    out.push_identifier(dsl::subnet_id::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(subnet_id)?;
    out.push_sql("), 'non-unique-subnets', ");
    out.push_bind_param::<sql_types::Text, String>(subnet_id_str)?;
    out.push_sql(") AS UUID)");
    Ok(())
}

/// Push the main interface-validation common-table expression.
///
/// This generates a CTE that looks like:
///
/// ```sql
/// WITH validated_interface(vpc_id, subnet_id, slot, is_primary) AS
///     (
///         <ensure valid VPC>,
///         <ensure valid VPC Subnet>,
///         <ensure instance exists and is stopped, if kind=Interface>,
///         <compute next slot>,
///         <compute is_primary>
///     )
/// ```
#[allow(clippy::too_many_arguments)]
fn push_interface_validation_cte<'a>(
    mut out: AstPass<'_, 'a, Pg>,
    interface_id: &'a Uuid,
    vpc_id: &'a Uuid,
    vpc_id_str: &'a String,
    subnet_id: &'a Uuid,
    subnet_id_str: &'a String,
    kind: &'a NetworkInterfaceKind,
    parent_id: &'a Uuid,
    parent_id_str: &'a String,
    next_slot_subquery: &'a NextNicSlot,
    is_primary_subquery: &'a IsPrimaryNic,
) -> diesel::QueryResult<()> {
    // Push the `validated_interface` CTE, which ensures that the VPC and VPC
    // Subnet are valid, and also selects the slot / is_primary.
    out.push_sql("WITH validated_interface(");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(", ");
    out.push_identifier(dsl::subnet_id::NAME)?;
    out.push_sql(", ");
    out.push_identifier(dsl::parent_id::NAME)?;
    out.push_sql(", ");
    out.push_identifier(dsl::slot::NAME)?;
    out.push_sql(", ");
    out.push_identifier(dsl::is_primary::NAME)?;
    out.push_sql(") AS (SELECT ");
    push_ensure_unique_vpc_expression(
        out.reborrow(),
        vpc_id,
        vpc_id_str,
        kind,
        parent_id,
    )?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::vpc_id::NAME)?;
    out.push_sql(", ");
    push_ensure_unique_vpc_subnet_expression(
        out.reborrow(),
        interface_id,
        subnet_id,
        subnet_id_str,
        kind,
        parent_id,
    )?;
    out.push_sql(" AS ");
    out.push_identifier(dsl::subnet_id::NAME)?;

    out.push_sql(", (");
    // Push the subquery to ensure the instance state when trying to insert the
    // new interface, if `kind=instance`
    if *kind == NetworkInterfaceKind::Instance {
        push_instance_state_verification_subquery(
            parent_id,
            parent_id_str,
            out.reborrow(),
            false,
        )?;
    } else {
        out.push_sql("SELECT ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(parent_id)?;
    }

    // Push the subquery used to select and validate the slot number for the
    // interface, including validating that there are available slots on the
    // resource.
    out.push_sql("), (");
    next_slot_subquery.walk_ast(out.reborrow())?;

    // Push the subquery used to detect whether this interface is the primary.
    // That's true iff there are zero interfaces for this resource at the time
    // this interface is inserted.
    out.push_sql("), (");
    is_primary_subquery.walk_ast(out.reborrow())?;

    // Close is_primary_subquery and the validated_instance CTE.
    out.push_sql(")) ");
    Ok(())
}

/// Subquery used to insert a new `NetworkInterface` from parameters.
///
/// This type is used to construct a query that allows inserting a
/// `NetworkInterface`, supporting both optionally allocating a new IP address
/// and verifying that the attached instance's networking is contained within a
/// single VPC. The general query looks like:
///
/// ```sql
/// <instance validation CTE>
/// SELECT <id> AS id, <name> AS name, <description> AS description,
///        <time_created> AS time_created, <time_modified> AS time_modified,
///        NULL AS time_deleted, <instance_id> AS instance_id, <vpc_id> AS vpc_id,
///        <subnet_id> AS subnet_id, <mac> AS mac, <maybe IP allocation subquery>,
///        <slot> AS slot, <is_primary> AS is_primary
/// ```
///
/// Instance validation
/// -------------------
///
/// This common-table expression checks that the provided instance meets a few
/// basic criteria, and computes some values for inserting in the new record if
/// those checks pass. In particular this checks that:
///
/// 1. The instance is not already associated with another VPC, since an
///    instance's network cannot span multiple VPCs.
/// 2. This interface is in a distinct subnet from any other interfaces already
///    attached to the instance.
///
/// It also computes:
///
/// 1. The slot index for this instance, verifying that the slot number is
///    valid.
/// 2. Whether this is the primary index for the instance. That's true iff this
///    is the first interface inserted for the instance.
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
/// [`NextIpv4Address`] for details on that allocation subquery. If that
/// fails, due to address exhaustion, this is detected and forwarded to the
/// caller.
///
/// Errors
/// ------
///
/// See [`InsertError`] for the errors caught and propagated by this query.
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
#[derive(Debug, Clone)]
pub struct InsertQuery {
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
    subnet_id_str: String,
    parent_id_str: String,
    ip_sql: Option<IpNetwork>,
    mac_sql: Option<db::model::MacAddr>,
    slot_sql: Option<SqlU8>,
    next_mac_subquery: NextMacAddress,
    next_ipv4_address_subquery: NextIpv4Address,
    next_slot_subquery: NextNicSlot,
    is_primary_subquery: IsPrimaryNic,
}

impl InsertQuery {
    pub fn new(interface: IncompleteNetworkInterface) -> Self {
        let vpc_id_str = interface.subnet.vpc_id.to_string();
        let subnet_id_str = interface.subnet.identity.id.to_string();
        let kind = interface.kind;
        let parent_id_str = interface.parent_id.to_string();
        let ip_sql = interface.ip.map(|ip| ip.into());
        let mac_sql = interface.mac.map(|mac| mac.into());
        let slot_sql = interface.slot.map(|slot| slot.into());
        let next_mac_subquery =
            NextMacAddress::new(interface.subnet.vpc_id, interface.kind);
        let next_ipv4_address_subquery = NextIpv4Address::new(
            interface.subnet.ipv4_block.0.into(),
            interface.subnet.identity.id,
        );
        let next_slot_subquery = NextNicSlot::new(interface.parent_id);
        let is_primary_subquery =
            IsPrimaryNic { kind, parent_id: interface.parent_id };
        Self {
            interface,
            now: Utc::now(),
            vpc_id_str,
            subnet_id_str,
            parent_id_str,
            ip_sql,
            mac_sql,
            slot_sql,
            next_mac_subquery,
            next_ipv4_address_subquery,
            next_slot_subquery,
            is_primary_subquery,
        }
    }
}

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type NetworkInterfaceFromClause =
    FromClause<nexus_db_schema::schema::network_interface::table>;
const NETWORK_INTERFACE_FROM_CLAUSE: NetworkInterfaceFromClause =
    NetworkInterfaceFromClause::new();

impl QueryId for InsertQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Insertable<nexus_db_schema::schema::network_interface::table>
    for InsertQuery
{
    type Values = InsertQueryValues;

    fn values(self) -> Self::Values {
        InsertQueryValues(self)
    }
}

impl QueryFragment<Pg> for InsertQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        // Push subqueries that validate the provided instance. This generates a CTE
        // with the name `validated_instance` and columns:
        //  - `vpc_id`
        //  - `subnet_id`
        //  - `slot`
        //  - `is_primary`
        push_interface_validation_cte(
            out.reborrow(),
            &self.interface.identity.id,
            &self.interface.subnet.vpc_id,
            &self.vpc_id_str,
            &self.interface.subnet.identity.id,
            &self.subnet_id_str,
            &self.interface.kind,
            &self.interface.parent_id,
            &self.parent_id_str,
            &self.next_slot_subquery,
            &self.is_primary_subquery,
        )?;

        // Push the columns, values and names, that are named directly. These
        // are known regardless of whether we're allocating an IP address. These
        // are all written as `SELECT <value1> AS <name1>, <value2> AS <name2>, ...
        out.push_sql("SELECT ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.identity.id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, db::model::Name>(
            &self.interface.identity.name,
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

        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.now,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.now,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Nullable<sql_types::Timestamptz>, Option<DateTime<Utc>>>(&None)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<NetworkInterfaceKindEnum, NetworkInterfaceKind>(
            &self.interface.kind,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::kind::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.interface.parent_id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::parent_id::NAME)?;
        out.push_sql(", ");

        // Helper function to push a subquery selecting something from the CTE.
        fn select_from_cte(
            mut out: AstPass<Pg>,
            column: &'static str,
        ) -> diesel::QueryResult<()> {
            out.push_sql("(SELECT ");
            out.push_identifier(column)?;
            out.push_sql(" FROM validated_interface)");
            Ok(())
        }

        select_from_cte(out.reborrow(), dsl::vpc_id::NAME)?;
        out.push_sql(", ");
        select_from_cte(out.reborrow(), dsl::subnet_id::NAME)?;
        out.push_sql(", ");

        // If the user specified a MAC address, then insert it by value.
        // Otherwise we use a subquery to select the next available MAC.
        if let Some(mac) = &self.mac_sql {
            out.push_bind_param::<sql_types::BigInt, db::model::MacAddr>(mac)?;
        } else {
            out.push_sql("(");
            self.next_mac_subquery.walk_ast(out.reborrow())?;
            out.push_sql(")");
        }
        out.push_sql(" AS ");
        out.push_identifier(dsl::mac::NAME)?;
        out.push_sql(", ");

        // If the user specified an IP address, then insert it by value. If they
        // did not, meaning we're allocating the next available one on their
        // behalf, then insert that subquery here.
        if let Some(ref ip) = &self.ip_sql {
            out.push_bind_param::<sql_types::Inet, IpNetwork>(ip)?;
        } else {
            out.push_sql("(");
            self.next_ipv4_address_subquery.walk_ast(out.reborrow())?;
            out.push_sql(")");
        }
        out.push_sql(" AS ");
        out.push_identifier(dsl::ip::NAME)?;
        out.push_sql(", ");

        if let Some(slot) = &self.slot_sql {
            out.push_bind_param::<sql_types::Int2, SqlU8>(slot)?;
        } else {
            select_from_cte(out.reborrow(), dsl::slot::NAME)?;
        }
        out.push_sql(" AS ");
        out.push_identifier(dsl::slot::NAME)?;
        out.push_sql(", ");

        select_from_cte(out.reborrow(), dsl::is_primary::NAME)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::is_primary::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Array<sql_types::Inet>, Vec<IpNetwork>>(
            &self.interface.transit_ips,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::transit_ips::NAME)?;

        Ok(())
    }
}

/// Type used to add the results of the `InsertQuery` as values
/// in a Diesel statement, e.g., `insert_into(network_interface).values(query).`
/// Not for direct use.
pub struct InsertQueryValues(InsertQuery);

impl QueryId for InsertQueryValues {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::insertable::CanInsertInSingleQuery<Pg> for InsertQueryValues {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(1)
    }
}

impl QueryFragment<Pg> for InsertQueryValues {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
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
        out.push_identifier(dsl::kind::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::parent_id::NAME)?;
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
        out.push_sql(", ");
        out.push_identifier(dsl::is_primary::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::transit_ips::NAME)?;
        out.push_sql(") ");
        self.0.walk_ast(out)
    }
}

/// A small helper subquery that automatically assigns the `is_primary` column
/// for a new network interface.
///
/// A resource with any network interfaces must have exactly one primary.
/// (It may have zero interfaces, however.) This subquery is used to insert
/// the value `true` if there are no extant interfaces, or `false` if there are.
#[derive(Debug, Clone, Copy)]
struct IsPrimaryNic {
    parent_id: Uuid,
    kind: NetworkInterfaceKind,
}

impl QueryId for IsPrimaryNic {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for IsPrimaryNic {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_sql("SELECT NOT EXISTS(SELECT 1 FROM ");
        NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::parent_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.parent_id)?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::kind::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<NetworkInterfaceKindEnum, NetworkInterfaceKind>(
            &self.kind,
        )?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL LIMIT 1)");
        Ok(())
    }
}

type InstanceFromClause = FromClause<nexus_db_schema::schema::instance::table>;
const INSTANCE_FROM_CLAUSE: InstanceFromClause = InstanceFromClause::new();

// Subquery used to ensure an instance both exists and is either stopped (or
// optionally failed) before inserting or deleting a network interface.
//
// This pushes a subquery like:
//
// ```sql
// CAST(
//  CASE
//      COALESCE(
//          -- Identify the state of the instance
//          (
//              SELECT
//                  CASE
//                      WHEN active_propolis_id IS NULL THEN state
//                      ELSE 'running'
//                  END
//              FROM
//                  instance
//              WHERE
//                  id = <instance_id> AND time_deleted IS NULL
//          ),
//          'destroyed' -- Default state, if not found
//      )
//      WHEN 'stopped' THEN '<instance_id_str>' -- Instance UUID as a string
//      WHEN 'creating' THEN '<instance_id_str>' -- Instance UUID as a string
//      WHEN 'failed' THEN '<instance_id_str>' -- Instance UUID as a string
//      WHEN 'destroyed' THEN 'no-instance' -- Sentinel for an instance not existing
//      ELSE 'bad-state' -- Any other state is invalid for operating on instances
//      END
// AS UUID)
// ```
//
// This uses the familiar cast-fail trick to select the instance's UUID if the
// instance is in a state that allows network interfaces to be altered or
// produce a cast error if they cannot. The COALESCE statement and its innards
// yield the following state string:
//
// - 'destroyed' if the instance is not found at all
// - 'running' if the instance is found and has an active VMM (this forbids
//   network interface changes irrespective of that VMM's actual state)
// - the instance's `state` otherwise
//
// If this produces 'stopped', 'creating', or (if applicable) 'failed', the
// outer CASE returns the instance ID as a string, which casts to a UUID. The
// 'destroyed' and 'bad-state' cases return non-UUID strings that cause a cast
// failure that can be caught and interpreted as a specific class of error.
//
// 'failed' is conditionally an accepted state: it would not be accepted as part
// of InsertQuery, but it should be as part of DeleteQuery (for example if the
// instance creation saga failed).
//
// Note that 'stopped', 'failed', and 'creating' are considered valid states.
// 'stopped' is used for most situations, especially client-facing, but
// 'creating' is critical for the instance-creation saga. When an instance is
// first provisioned, it remains in the 'creating' state until provisioning is
// copmleted and it transitions to 'stopped'; it is permissible to add
// interfaces during that provisioning process.

fn push_instance_state_verification_subquery<'a>(
    instance_id: &'a Uuid,
    instance_id_str: &'a String,
    mut out: AstPass<'_, 'a, Pg>,
    failed_ok: bool,
) -> QueryResult<()> {
    use nexus_db_schema::enums::InstanceStateEnum;

    out.push_sql("CAST(CASE COALESCE((SELECT ");
    out.push_sql("CASE WHEN ");
    out.push_identifier(
        nexus_db_schema::schema::instance::dsl::active_propolis_id::NAME,
    )?;
    out.push_sql(" IS NULL THEN ");
    out.push_identifier(nexus_db_schema::schema::instance::dsl::state::NAME)?;
    out.push_sql(" ELSE ");
    out.push_bind_param::<InstanceStateEnum, db::model::InstanceState>(
        &INSTANCE_RUNNING,
    )?;
    out.push_sql(" END ");
    out.push_sql(" FROM ");
    INSTANCE_FROM_CLAUSE.walk_ast(out.reborrow())?;
    out.push_sql(" WHERE ");
    out.push_identifier(nexus_db_schema::schema::instance::dsl::id::NAME)?;
    out.push_sql(" = ");
    out.push_bind_param::<sql_types::Uuid, Uuid>(instance_id)?;
    out.push_sql(" AND ");
    out.push_identifier(
        nexus_db_schema::schema::instance::dsl::time_deleted::NAME,
    )?;
    out.push_sql(" IS NULL), ");
    out.push_bind_param::<InstanceStateEnum, db::model::InstanceState>(
        &INSTANCE_DESTROYED,
    )?;
    out.push_sql(") WHEN ");
    out.push_bind_param::<InstanceStateEnum, db::model::InstanceState>(
        &INSTANCE_STOPPED,
    )?;
    out.push_sql(" THEN ");
    out.push_bind_param::<sql_types::Text, String>(instance_id_str)?;
    out.push_sql(" WHEN ");
    out.push_bind_param::<InstanceStateEnum, db::model::InstanceState>(
        &INSTANCE_CREATING,
    )?;
    out.push_sql(" THEN ");
    out.push_bind_param::<sql_types::Text, String>(instance_id_str)?;
    if failed_ok {
        // FAILED is ok for DeleteQuery, but not for InsertQuery!
        out.push_sql(" WHEN ");
        out.push_bind_param::<InstanceStateEnum, db::model::InstanceState>(
            &INSTANCE_FAILED,
        )?;
        out.push_sql(" THEN ");
        out.push_bind_param::<sql_types::Text, String>(instance_id_str)?;
    }
    out.push_sql(" WHEN ");
    out.push_bind_param::<InstanceStateEnum, db::model::InstanceState>(
        &INSTANCE_DESTROYED,
    )?;
    out.push_sql(" THEN ");
    out.push_bind_param::<sql_types::Text, String>(
        &NO_INSTANCE_SENTINEL_STRING,
    )?;
    out.push_sql(" ELSE ");
    out.push_bind_param::<sql_types::Text, String>(
        &INSTANCE_BAD_STATE_SENTINEL_STRING,
    )?;
    out.push_sql(" END AS UUID)");
    Ok(())
}

/// Delete a network interface.
///
/// There are a few preconditions that need to be checked when deleting a NIC.
/// First, if it's an instance-kind interface then the instance must currently
/// be stopped, though we may relax this in the future.
/// Second, while an instance may have zero or more interfaces, if it has one
/// or more, exactly one of those must be the primary interface. That means
/// we can only delete the primary interface if there are no secondary interfaces.
/// The full query is:
///
/// ```sql
/// WITH
///     instance AS MATERIALIZED (SELECT CAST(
///         CASE
///             COALESCE(
///                 (SELECT
///                     state
///                  FROM
///                     instance
///                  WHERE
///                     id = <instance_id> AND
///                     time_deleted IS NULL
///                 ),
///                 'destroyed'
///             )
///             WHEN 'stopped' THEN '<instance_id>'
///             WHEN 'creating' THEN '<instanced_id>'
///             WHEN 'failed' THEN '<instanced_id>'
///             WHEN 'destroyed' THEN 'no-instance'
///             ELSE 'bad-state'
///         END
///     AS UUID)),
///     interface AS MATERIALIZED (
///         SELECT CAST(IF(
///             (
///                 SELECT
///                     NOT is_primary
///                 FROM
///                     network_interface
///                 WHERE
///                     id = <interface_id> AND
///                     time_deleted IS NULL
///             )
///                 OR
///             (
///                 SELECT
///                     COUNT(*)
///                 FROM
///                     network_interface
///                 WHERE
///                     parent_id = <parent_id> AND
///                     kind = <kind> AND
///                     time_deleted IS NULL
///             ) <= 1,
///             '<interface_id>',
///             'secondaries'
///         ) AS UUID)
///     ),
///     found_interface AS (
///         SELECT
///             id
///         FROM
///             network_interface
///         WHERE
///             id = <interface_id>
///     ),
///     updated AS (
///         UPDATE
///             network_interface
///         SET
///             time_deleted = NOW()
///         WHERE
///             id = <interface_id> AND
///             time_deleted IS NULL
///         RETURNING
///             id
///     )
/// SELECT
///     found_interface.id,
///     updated.id
/// FROM
///     found_interface
/// LEFT JOIN
///     updated
/// ON
///     found_interface.id = updated.id
/// ```
///
/// Notes
/// -----
///
/// As with some of the other queries in this module, this uses some casting
/// trickery to learn why the query fails. This is why we store the
/// `parent_id` as a string in this type.
///
/// The `instance` CTE is only present if the interface is an instance-kind.
#[derive(Debug, Clone)]
pub struct DeleteQuery {
    interface_id: Uuid,
    kind: NetworkInterfaceKind,
    parent_id: Uuid,
    parent_id_str: String,
}

impl DeleteQuery {
    pub fn new(
        kind: NetworkInterfaceKind,
        parent_id: Uuid,
        interface_id: Uuid,
    ) -> Self {
        Self {
            interface_id,
            kind,
            parent_id,
            parent_id_str: parent_id.to_string(),
        }
    }

    /// Issue the delete and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Row exists and was deleted)
    /// - Ok(Row exists, but was not deleted)
    /// - Error (row doesn't exist, or other diesel error)
    pub async fn execute_and_check(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<bool, DieselError> {
        let (found_id, deleted_id) =
            self.get_result_async::<(Option<Uuid>, Option<Uuid>)>(conn).await?;
        match (found_id, deleted_id) {
            (Some(found), Some(deleted)) => {
                assert_eq!(
                    found, deleted,
                    "internal query error: mismatched interface IDs"
                );
                Ok(true)
            }
            (Some(_), None) => Ok(false),
            (None, Some(deleted)) => {
                panic!(
                    "internal query error: \
                     deleted nonexisted interface {deleted}"
                )
            }
            (None, None) => Err(DieselError::NotFound),
        }
    }
}

impl QueryId for DeleteQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for DeleteQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_sql("WITH ");
        if self.kind == NetworkInterfaceKind::Instance {
            out.push_sql("instance AS MATERIALIZED (SELECT ");
            push_instance_state_verification_subquery(
                &self.parent_id,
                &self.parent_id_str,
                out.reborrow(),
                true,
            )?;
            out.push_sql("), ");
        }
        out.push_sql("interface AS MATERIALIZED (SELECT CAST(IF((SELECT NOT ");
        out.push_identifier(dsl::is_primary::NAME)?;
        out.push_sql(" FROM ");
        NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.interface_id)?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL) OR (SELECT COUNT(*) FROM ");
        NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::parent_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.parent_id)?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::kind::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<NetworkInterfaceKindEnum, NetworkInterfaceKind>(
            &self.kind,
        )?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL) <= 1, ");
        out.push_bind_param::<sql_types::Text, String>(&self.parent_id_str)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, &str>(
            &DeleteError::HAS_SECONDARIES_SENTINEL,
        )?;
        out.push_sql(") AS UUID)), found_interface AS (SELECT ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" FROM ");
        NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.interface_id)?;
        out.push_sql("), updated AS (UPDATE ");
        NETWORK_INTERFACE_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" = NOW() WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.interface_id)?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL RETURNING ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(") SELECT found_interface.");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", updated.");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" FROM found_interface LEFT JOIN updated");
        out.push_sql(" ON found_interface.");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = updated.");
        out.push_identifier(dsl::id::NAME)?;
        Ok(())
    }
}

impl Query for DeleteQuery {
    type SqlType = (Nullable<sql_types::Uuid>, Nullable<sql_types::Uuid>);
}

impl RunQueryDsl<DbConnection> for DeleteQuery {}

/// Errors related to deleting a network interface
#[derive(Debug, thiserror::Error, SlogInlineError, PartialEq)]
pub enum DeleteError {
    /// Attempting to delete the primary interface, while there still exist
    /// secondary interfaces.
    #[error("cannot delete primary interface while secondaries exist")]
    SecondariesExist(Uuid),
    /// Instance must be stopped or failed prior to deleting interfaces from it
    #[error("cannot delete interface in current instance state")]
    InstanceBadState(Uuid),
    /// The instance does not exist at all, or is in the destroyed state.
    #[error("instance not found ({0})")]
    InstanceNotFound(Uuid),
    /// Any other error
    #[error("cannot delete interface")]
    External(#[source] external::Error),
}

impl DeleteError {
    const HAS_SECONDARIES_SENTINEL: &'static str = "secondaries";

    /// Construct a `DeleteError` from a database error
    ///
    /// This catches the various errors that the `DeleteQuery`
    /// can generate, specifically the intentional errors that indicate that
    /// either the instance is still running, or that the instance has one or
    /// more secondary interfaces.
    pub fn from_diesel(e: DieselError, query: &DeleteQuery) -> Self {
        match e {
            // Catch the specific errors designed to communicate the failures we
            // want to distinguish
            DieselError::DatabaseError(_, _) => {
                decode_delete_network_interface_database_error(
                    e,
                    query.parent_id,
                )
            }
            // Faithfully plumb through `NotFound`
            DieselError::NotFound => {
                let type_name = match query.kind {
                    NetworkInterfaceKind::Instance => {
                        external::ResourceType::InstanceNetworkInterface
                    }
                    NetworkInterfaceKind::Service => {
                        external::ResourceType::ServiceNetworkInterface
                    }
                    NetworkInterfaceKind::Probe => {
                        external::ResourceType::ProbeNetworkInterface
                    }
                };
                DeleteError::External(external::Error::ObjectNotFound {
                    type_name,
                    lookup_type: external::LookupType::ById(query.interface_id),
                })
            }
            // Any other error at all is a bug
            _ => DeleteError::External(
                nexus_db_errors::public_error_from_diesel(
                    e,
                    nexus_db_errors::ErrorHandler::Server,
                ),
            ),
        }
    }

    /// Convert this error into an external one.
    pub fn into_external(self) -> external::Error {
        match self {
            DeleteError::SecondariesExist(_) => {
                external::Error::invalid_request(
                    "The primary interface \
                    may not be deleted while secondary interfaces \
                    are still attached",
                )
            }
            DeleteError::InstanceBadState(_) => {
                external::Error::invalid_request(
                    "Instance must be stopped or failed to detach a network interface",
                )
            }
            DeleteError::InstanceNotFound(id) => {
                external::Error::not_found_by_id(
                    external::ResourceType::Instance,
                    &id,
                )
            }
            DeleteError::External(e) => e,
        }
    }
}

/// Decode an error from the database to determine why deleting an interface
/// failed.
///
/// This function works by inspecting the detailed error messages, including
/// indexes used or constraints violated, to determine the cause of the failure.
/// As such, it naturally is extremely tightly coupled to the database itself,
/// including the software version and our schema.
fn decode_delete_network_interface_database_error(
    err: DieselError,
    parent_id: Uuid,
) -> DeleteError {
    use diesel::result::DatabaseErrorKind;

    // Error message generated when we're attempting to delete a primary
    // interface, and that instance also has one or more secondary interfaces
    const HAS_SECONDARIES_ERROR_MESSAGE: &'static str = "could not parse \"secondaries\" as type uuid: uuid: \
        incorrect UUID length: secondaries";

    match err {
        // This catches the error intentionally introduced by the
        // first CTE, which generates a UUID parsing error if we're trying to
        // delete the primary interface, and the instance also has one or more
        // secondaries.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info)
            if info.message() == HAS_SECONDARIES_ERROR_MESSAGE =>
        {
            DeleteError::SecondariesExist(parent_id)
        }

        // This catches the UUID-cast failure intentionally introduced by
        // `push_instance_state_verification_subquery`, which verifies that
        // the instance can be worked on when running this query.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info)
            if info.message() == INSTANCE_BAD_STATE_ERROR_MESSAGE =>
        {
            DeleteError::InstanceBadState(parent_id)
        }
        // This catches the UUID-cast failure intentionally introduced by
        // `push_instance_state_verification_subquery`, which verifies that
        // the instance doesn't even exist when running this query.
        DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info)
            if info.message() == NO_INSTANCE_ERROR_MESSAGE =>
        {
            DeleteError::InstanceNotFound(parent_id)
        }

        // Any other error at all is a bug
        _ => DeleteError::External(nexus_db_errors::public_error_from_diesel(
            err,
            nexus_db_errors::ErrorHandler::Server,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::DeleteError;
    use super::InsertError;
    use super::MAX_NICS_PER_INSTANCE;
    use super::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use super::first_available_address;
    use crate::authz;
    use crate::context::OpContext;
    use crate::db::datastore::DataStore;
    use crate::db::identity::Resource;
    use crate::db::model;
    use crate::db::model::IncompleteNetworkInterface;
    use crate::db::model::Instance;
    use crate::db::model::InstanceState;
    use crate::db::model::NetworkInterface;
    use crate::db::model::Project;
    use crate::db::model::VpcSubnet;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::queries::network_interface::last_available_address;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use dropshot::test_util::LogContext;
    use model::NetworkInterfaceKind;
    use nexus_db_lookup::LookupPath;
    use nexus_types::external_api::params;
    use nexus_types::external_api::params::InstanceCreate;
    use nexus_types::external_api::params::InstanceNetworkInterfaceAttachment;
    use omicron_common::api::external;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::InstanceCpuCount;
    use omicron_common::api::external::MacAddr;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use oxnet::Ipv4Net;
    use oxnet::Ipv6Net;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use uuid::Uuid;

    // Add an instance. We'll use this to verify that the instance must be
    // stopped to add or delete interfaces.
    async fn create_instance(
        opctx: &OpContext,
        project_id: Uuid,
        db_datastore: &DataStore,
    ) -> Instance {
        let instance_id = InstanceUuid::new_v4();
        // Use the first chunk of the UUID as the name, to avoid conflicts.
        // Start with a lower ascii character to satisfy the name constraints.
        let name = format!("a{}", instance_id)[..9].parse().unwrap();
        let params = InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name,
                description: "desc".to_string(),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(4),
            hostname: "inst".parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: Some(Vec::new()),
            network_interfaces: InstanceNetworkInterfaceAttachment::None,
            external_ips: vec![],
            disks: vec![],
            boot_disk: None,
            start: true,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        };

        let instance = Instance::new(instance_id, project_id, &params);

        let (.., authz_project) = LookupPath::new(&opctx, db_datastore)
            .project_id(project_id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .expect("Failed to lookup project for instance creation");

        db_datastore
            .project_create_instance(&opctx, &authz_project, instance)
            .await
            .expect("Failed to create new instance record")
    }

    async fn create_stopped_instance(
        opctx: &OpContext,
        project_id: Uuid,
        db_datastore: &DataStore,
    ) -> Instance {
        let instance = create_instance(opctx, project_id, db_datastore).await;
        instance_set_state(db_datastore, instance, InstanceState::NoVmm).await
    }

    async fn instance_set_state(
        db_datastore: &DataStore,
        mut instance: Instance,
        state: InstanceState,
    ) -> Instance {
        let propolis_id = match state {
            InstanceState::Vmm => Some(Uuid::new_v4()),
            _ => None,
        };

        let new_runtime = model::InstanceRuntimeState {
            nexus_state: state,
            propolis_id,
            gen: instance.runtime_state.gen.next().into(),
            ..instance.runtime_state.clone()
        };
        let res = db_datastore
            .instance_update_runtime(
                &InstanceUuid::from_untyped_uuid(instance.id()),
                &new_runtime,
            )
            .await;
        assert!(matches!(res, Ok(true)), "Failed to change instance state");
        instance.runtime_state = new_runtime;
        instance
    }

    // VPC with several distinct subnets.
    struct Network {
        subnets: Vec<VpcSubnet>,
    }

    impl Network {
        // Create a VPC with N distinct VPC Subnets.
        fn new(n_subnets: u8) -> Self {
            let vpc_id = Uuid::new_v4();
            let mut subnets = Vec::with_capacity(n_subnets as _);
            for i in 0..n_subnets {
                let ipv4net =
                    Ipv4Net::new(Ipv4Addr::new(172, 30, 0, i), 28).unwrap();
                let ipv6net = Ipv6Net::new(
                    Ipv6Addr::new(0xfd12, 0x3456, 0x7890, i.into(), 0, 0, 0, 0),
                    64,
                )
                .unwrap();
                let subnet = VpcSubnet::new(
                    Uuid::new_v4(),
                    vpc_id,
                    IdentityMetadataCreateParams {
                        name: format!("subnet-{i}").try_into().unwrap(),
                        description: String::from("first test subnet"),
                    },
                    ipv4net,
                    ipv6net,
                );
                subnets.push(subnet);
            }
            Self { subnets }
        }

        fn available_ipv4_addresses(&self) -> Vec<usize> {
            self.subnets
                .iter()
                .map(|subnet| {
                    let size_minus_1 = match subnet.ipv4_block.size() {
                        Some(n) => n - 1,
                        None => u32::MAX,
                    } as usize;
                    size_minus_1 - NUM_INITIAL_RESERVED_IP_ADDRESSES
                })
                .collect()
        }
    }

    // Context for testing network interface queries.
    struct TestContext {
        logctx: LogContext,
        db: TestDatabase,
        project_id: Uuid,
        net1: Network,
        net2: Network,
    }

    impl TestContext {
        async fn new(test_name: &str, n_subnets: u8) -> Self {
            let logctx = dev::test_setup_log(test_name);
            let log = logctx.log.new(o!());
            let db = TestDatabase::new_with_datastore(&log).await;
            let (opctx, datastore) = (db.opctx(), db.datastore());

            let authz_silo = opctx.authn.silo_required().unwrap();

            // Create a project
            let project = Project::new(
                authz_silo.id(),
                params::ProjectCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "project".parse().unwrap(),
                        description: "desc".to_string(),
                    },
                },
            );
            let (.., project) =
                datastore.project_create(&opctx, project).await.unwrap();

            use nexus_db_schema::schema::vpc_subnet::dsl::vpc_subnet;
            let conn =
                datastore.pool_connection_authorized(&opctx).await.unwrap();
            let net1 = Network::new(n_subnets);
            let net2 = Network::new(n_subnets);
            for subnet in net1.subnets.iter().chain(net2.subnets.iter()) {
                diesel::insert_into(vpc_subnet)
                    .values(subnet.clone())
                    .execute_async(&*conn)
                    .await
                    .unwrap();
            }
            drop(conn);
            Self { logctx, db, project_id: project.id(), net1, net2 }
        }

        fn opctx(&self) -> &OpContext {
            self.db.opctx()
        }

        fn datastore(&self) -> &DataStore {
            self.db.datastore()
        }

        async fn success(self) {
            self.db.terminate().await;
            self.logctx.cleanup_successful();
        }

        async fn create_stopped_instance(&self) -> Instance {
            instance_set_state(
                self.datastore(),
                create_instance(
                    self.opctx(),
                    self.project_id,
                    self.datastore(),
                )
                .await,
                InstanceState::NoVmm,
            )
            .await
        }

        async fn create_running_instance(&self) -> Instance {
            instance_set_state(
                self.datastore(),
                create_instance(
                    self.opctx(),
                    self.project_id,
                    self.datastore(),
                )
                .await,
                InstanceState::Vmm,
            )
            .await
        }

        async fn delete_instance_nics(&self, instance_id: Uuid) {
            let (.., authz_instance) =
                LookupPath::new(self.opctx(), self.datastore())
                    .instance_id(instance_id)
                    .lookup_for(authz::Action::Modify)
                    .await
                    .expect("Failed to lookup instance");
            self.datastore()
                .instance_delete_all_network_interfaces(
                    self.opctx(),
                    &authz_instance,
                )
                .await
                .expect("Failed to delete NICs");
        }
    }

    #[tokio::test]
    async fn test_delete_service_is_idempotent() {
        let context =
            TestContext::new("test_delete_service_is_idempotent", 2).await;
        let service_id = Uuid::new_v4();
        let ip = context.net1.subnets[0]
            .ipv4_block
            .addr_iter()
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .unwrap();
        let interface = IncompleteNetworkInterface::new_service(
            Uuid::new_v4(),
            service_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "service-nic".parse().unwrap(),
                description: String::from("service nic"),
            },
            ip.into(),
            MacAddr::random_system(),
            0,
        )
        .unwrap();
        let inserted_interface = context
            .datastore()
            .service_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");

        // We should be able to delete twice, and be told that the first delete
        // modified the row and the second did not.
        let first_deleted = context
            .datastore()
            .service_delete_network_interface(
                context.opctx(),
                service_id,
                inserted_interface.id(),
            )
            .await
            .expect("failed first delete");
        assert!(first_deleted, "first delete removed interface");

        let second_deleted = context
            .datastore()
            .service_delete_network_interface(
                context.opctx(),
                service_id,
                inserted_interface.id(),
            )
            .await
            .expect("failed second delete");
        assert!(!second_deleted, "second delete did nothing");

        // Attempting to delete a nonexistent interface should fail.
        let bogus_id = Uuid::new_v4();
        let err = context
            .datastore()
            .service_delete_network_interface(
                context.opctx(),
                service_id,
                bogus_id,
            )
            .await
            .expect_err(
                "unexpectedly succeeded deleting nonexistent interface",
            );
        let expected_err =
            DeleteError::External(external::Error::ObjectNotFound {
                type_name: external::ResourceType::ServiceNetworkInterface,
                lookup_type: external::LookupType::ById(bogus_id),
            });
        assert_eq!(err, expected_err);
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_running_instance_fails() {
        let context =
            TestContext::new("test_insert_running_instance_fails", 2).await;
        let instance = context.create_running_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let requested_ip = "172.30.0.5".parse().unwrap();
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-a".parse().unwrap(),
                description: String::from("description"),
            },
            Some(requested_ip),
            vec![],
        )
        .unwrap();
        let err = context.datastore()
            .instance_create_network_interface_raw(context.opctx(), interface.clone())
            .await
            .expect_err("Should not be able to create an interface for a running instance");
        assert!(
            matches!(err, InsertError::InstanceMustBeStopped(_)),
            "Expected an InstanceMustBeStopped error, found {:?}",
            err
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_request_exact_ip() {
        let context = TestContext::new("test_insert_request_exact_ip", 2).await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let requested_ip = "172.30.0.5".parse().unwrap();
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-a".parse().unwrap(),
                description: String::from("description"),
            },
            Some(requested_ip),
            vec![],
        )
        .unwrap();
        let inserted_interface = context
            .datastore()
            .instance_create_network_interface_raw(
                context.opctx(),
                interface.clone(),
            )
            .await
            .expect("Failed to insert interface with known-good IP address");
        assert_interfaces_eq(&interface, &inserted_interface.clone().into());
        assert_eq!(
            inserted_interface.ip.ip(),
            requested_ip,
            "The requested IP address should be available when no interfaces exist in the table"
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_no_instance_fails() {
        let context =
            TestContext::new("test_insert_no_instance_fails", 2).await;
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            InstanceUuid::new_v4(),
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-b".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let err = context.datastore()
            .instance_create_network_interface_raw(context.opctx(), interface.clone())
            .await
            .expect_err("Should not be able to insert an interface for an instance that doesn't exist");
        assert!(
            matches!(err, InsertError::InstanceNotFound(_)),
            "Expected an InstanceNotFound error, found {:?}",
            err,
        );
        context.success().await;
    }

    // Create one interface on the instance, and then verify that the next from
    // the same VPC Subnet (which must be on a different instance) has the next
    // IP address.
    #[tokio::test]
    async fn test_insert_sequential_ip_allocation() {
        let context =
            TestContext::new("test_insert_sequential_ip_allocation", 2).await;
        let addresses = context.net1.subnets[0]
            .ipv4_block
            .addr_iter()
            .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES);

        for (i, expected_address) in addresses.take(2).enumerate() {
            let instance = context.create_stopped_instance().await;
            let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
            let interface = IncompleteNetworkInterface::new_instance(
                Uuid::new_v4(),
                instance_id,
                context.net1.subnets[0].clone(),
                IdentityMetadataCreateParams {
                    name: format!("interface-{}", i).parse().unwrap(),
                    description: String::from("description"),
                },
                None,
                vec![],
            )
            .unwrap();
            let inserted_interface = context
                .datastore()
                .instance_create_network_interface_raw(
                    context.opctx(),
                    interface.clone(),
                )
                .await
                .expect("Failed to insert interface");
            assert_interfaces_eq(
                &interface,
                &inserted_interface.clone().into(),
            );
            let actual_address = inserted_interface.ip.ip();
            assert_eq!(
                actual_address, expected_address,
                "Failed to auto-assign correct sequential address to interface"
            );
        }
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_request_same_ip_fails() {
        let context =
            TestContext::new("test_insert_request_same_ip_fails", 2).await;

        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let new_instance = context.create_stopped_instance().await;
        let new_instance_id =
            InstanceUuid::from_untyped_uuid(new_instance.id());

        // Insert an interface on the first instance.
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let inserted_interface = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");

        // Inserting an interface with the same IP should fail, even if all
        // other parameters are valid.
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            new_instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            Some(inserted_interface.ip.ip()),
            vec![],
        )
        .unwrap();
        let result = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await;
        assert!(
            matches!(result, Err(InsertError::IpAddressNotAvailable(_))),
            "Requesting an interface with an existing IP should fail"
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_request_mac() {
        let context = TestContext::new("test_insert_request_mac", 1).await;

        // Ensure service NICs are recorded with the explicit requested MAC
        // address
        let service_id = Uuid::new_v4();
        let ip = context.net1.subnets[0]
            .ipv4_block
            .addr_iter()
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .unwrap();
        let mac = MacAddr::random_system();
        let interface = IncompleteNetworkInterface::new_service(
            Uuid::new_v4(),
            service_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "service-nic".parse().unwrap(),
                description: String::from("service nic"),
            },
            ip.into(),
            mac,
            0,
        )
        .unwrap();
        let inserted_interface = context
            .datastore()
            .service_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        assert_eq!(inserted_interface.mac.0, mac);

        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_request_slot() {
        let context = TestContext::new("test_insert_request_slot", 1).await;

        // Ensure service NICs are recorded with the explicit requested slot
        let mut used_macs = HashSet::new();
        let mut ips = context.net1.subnets[0]
            .ipv4_block
            .addr_iter()
            .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES);
        for slot in 0..u8::try_from(MAX_NICS_PER_INSTANCE).unwrap() {
            let service_id = Uuid::new_v4();
            let ip = ips.next().expect("exhausted test subnet");
            let mut mac = MacAddr::random_system();
            while !used_macs.insert(mac) {
                mac = MacAddr::random_system();
            }
            let interface = IncompleteNetworkInterface::new_service(
                Uuid::new_v4(),
                service_id,
                context.net1.subnets[0].clone(),
                IdentityMetadataCreateParams {
                    name: "service-nic".parse().unwrap(),
                    description: String::from("service nic"),
                },
                ip.into(),
                mac,
                slot,
            )
            .unwrap();
            let inserted_interface = context
                .datastore()
                .service_create_network_interface_raw(
                    context.opctx(),
                    interface,
                )
                .await
                .expect("Failed to insert interface");
            assert_eq!(*inserted_interface.slot, slot);
        }

        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_request_same_mac_fails() {
        let context =
            TestContext::new("test_insert_request_same_mac_fails", 2).await;

        let mut ips = context.net1.subnets[0]
            .ipv4_block
            .addr_iter()
            .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES);

        // Insert a service NIC
        let service_id = Uuid::new_v4();
        let mac = MacAddr::random_system();
        let interface = IncompleteNetworkInterface::new_service(
            Uuid::new_v4(),
            service_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "service-nic".parse().unwrap(),
                description: String::from("service nic"),
            },
            ips.next().expect("exhausted test subnet").into(),
            mac,
            0,
        )
        .unwrap();
        let inserted_interface = context
            .datastore()
            .service_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        assert_eq!(inserted_interface.mac.0, mac);

        // Inserting an interface with the same MAC should fail, even if all
        // other parameters are valid.
        let new_service_id = Uuid::new_v4();
        let new_interface = IncompleteNetworkInterface::new_service(
            Uuid::new_v4(),
            new_service_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "new-service-nic".parse().unwrap(),
                description: String::from("new-service nic"),
            },
            ips.next().expect("exhausted test subnet").into(),
            mac,
            0,
        )
        .unwrap();
        let result = context
            .datastore()
            .service_create_network_interface_raw(
                context.opctx(),
                new_interface,
            )
            .await;
        assert!(
            matches!(result, Err(InsertError::MacAddressNotAvailable(_))),
            "Requesting an interface with an existing MAC should fail"
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_request_same_slot_fails() {
        let context =
            TestContext::new("test_insert_request_same_slot_fails", 2).await;

        let ip0 = context.net1.subnets[0]
            .ipv4_block
            .addr_iter()
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .unwrap();
        let ip1 = context.net1.subnets[1]
            .ipv4_block
            .addr_iter()
            .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .unwrap();

        let mut next_mac = {
            let mut used_macs = HashSet::new();
            move || {
                let mut mac = MacAddr::random_system();
                while !used_macs.insert(mac) {
                    mac = MacAddr::random_system();
                }
                mac
            }
        };

        // Insert a service NIC
        let service_id = Uuid::new_v4();
        let interface = IncompleteNetworkInterface::new_service(
            Uuid::new_v4(),
            service_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "service-nic".parse().unwrap(),
                description: String::from("service nic"),
            },
            ip0.into(),
            next_mac(),
            0,
        )
        .unwrap();
        let inserted_interface = context
            .datastore()
            .service_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        assert_eq!(*inserted_interface.slot, 0);

        // Inserting an interface with the same slot on the same service should
        let new_interface = IncompleteNetworkInterface::new_service(
            Uuid::new_v4(),
            service_id,
            context.net1.subnets[1].clone(),
            IdentityMetadataCreateParams {
                name: "new-service-nic".parse().unwrap(),
                description: String::from("new-service nic"),
            },
            ip1.into(),
            next_mac(),
            0,
        )
        .unwrap();
        let result = context
            .datastore()
            .service_create_network_interface_raw(
                context.opctx(),
                new_interface,
            )
            .await;
        assert!(
            matches!(result, Err(InsertError::SlotNotAvailable(0))),
            "Requesting an interface with an existing slot should fail"
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_with_duplicate_name_fails() {
        let context =
            TestContext::new("test_insert_with_duplicate_name_fails", 2).await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let _ = context
            .datastore()
            .instance_create_network_interface_raw(
                context.opctx(),
                interface.clone(),
            )
            .await
            .expect("Failed to insert interface");
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[1].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let result = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await;
        assert!(
            matches!(
                result,
                Err(InsertError::External(Error::ObjectAlreadyExists { .. })),
            ),
            "Requesting an interface with the same name on the same instance should fail"
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_same_vpc_subnet_fails() {
        let context =
            TestContext::new("test_insert_same_vpc_subnet_fails", 2).await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let _ = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-d".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let result = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await;
        assert!(
            matches!(result, Err(InsertError::NonUniqueVpcSubnets)),
            "Each interface for an instance must be in distinct VPC Subnets"
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_same_interface_fails() {
        let context =
            TestContext::new("test_insert_same_interface_fails", 2).await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let _ = context
            .datastore()
            .instance_create_network_interface_raw(
                context.opctx(),
                interface.clone(),
            )
            .await
            .expect("Failed to insert interface");
        let result = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await;
        assert!(
            matches!(
                result,
                Err(InsertError::InterfaceAlreadyExists(
                    _,
                    NetworkInterfaceKind::Instance
                )),
            ),
            "Expected that interface would already exist",
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_multiple_vpcs_fails() {
        let context =
            TestContext::new("test_insert_multiple_vpcs_fails", 2).await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let _ = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        let expected_address = "172.30.0.5".parse().unwrap();
        for addr in [Some(expected_address), None] {
            let interface = IncompleteNetworkInterface::new_instance(
                Uuid::new_v4(),
                instance_id,
                context.net2.subnets[0].clone(),
                IdentityMetadataCreateParams {
                    name: "interface-a".parse().unwrap(),
                    description: String::from("description"),
                },
                addr,
                vec![],
            )
            .unwrap();
            let result = context
                .datastore()
                .instance_create_network_interface_raw(
                    context.opctx(),
                    interface,
                )
                .await;
            assert!(
                matches!(
                    result,
                    Err(InsertError::ResourceSpansMultipleVpcs(_))
                ),
                "Attaching an interface to a resource which already has one in a different VPC should fail"
            );
        }
        context.success().await;
    }

    // Ensure that we can allocate exactly many interfaces as there are IPs in
    // the VPC Subnet, and no more. We do this on different instances to avoid
    // hitting the per-instance limit of NICs.
    #[tokio::test]
    async fn test_detect_ip_exhaustion() {
        let context = TestContext::new("test_detect_ip_exhaustion", 2).await;
        let n_interfaces = context.net1.available_ipv4_addresses()[0];
        for _ in 0..n_interfaces {
            let instance = context.create_stopped_instance().await;
            let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
            let interface = IncompleteNetworkInterface::new_instance(
                Uuid::new_v4(),
                instance_id,
                context.net1.subnets[0].clone(),
                IdentityMetadataCreateParams {
                    name: "interface-c".parse().unwrap(),
                    description: String::from("description"),
                },
                None,
                vec![],
            )
            .unwrap();
            let _ = context
                .datastore()
                .instance_create_network_interface_raw(
                    context.opctx(),
                    interface,
                )
                .await
                .expect("Failed to insert interface");
        }

        // Next one should fail
        let instance = create_stopped_instance(
            context.opctx(),
            context.project_id,
            context.datastore(),
        )
        .await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-d".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let result = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await;
        assert!(
            matches!(result, Err(InsertError::NoAvailableIpAddresses { .. })),
            "Address exhaustion should be detected and handled, found {:?}",
            result,
        );
        context.success().await;
    }

    // Ensure that we can insert more than one interface for an instance,
    // provided they're in different VPC Subnets
    #[tokio::test]
    async fn test_insert_multiple_vpc_subnets_succeeds() {
        let context =
            TestContext::new("test_insert_multiple_vpc_subnets_succeeds", 2)
                .await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        for (i, subnet) in context.net1.subnets.iter().enumerate() {
            let interface = IncompleteNetworkInterface::new_instance(
                Uuid::new_v4(),
                instance_id,
                subnet.clone(),
                IdentityMetadataCreateParams {
                    name: format!("if{}", i).parse().unwrap(),
                    description: String::from("description"),
                },
                None,
                vec![],
            )
            .unwrap();
            let result = context
                .datastore()
                .instance_create_network_interface_raw(
                    context.opctx(),
                    interface,
                )
                .await;
            assert!(
                result.is_ok(),
                "Should be able to allocate multiple interfaces on the same \
                instance, as long as they're in different VPC Subnets",
            );
        }
        context.success().await;
    }

    // Test equality of a complete/inserted interface, for parts that are always known.
    fn assert_interfaces_eq(
        incomplete: &IncompleteNetworkInterface,
        inserted: &NetworkInterface,
    ) {
        assert_eq!(inserted.id(), incomplete.identity.id);
        assert_eq!(inserted.name(), &incomplete.identity.name.0);
        assert_eq!(inserted.description(), incomplete.identity.description);
        assert_eq!(inserted.kind, incomplete.kind);
        assert_eq!(inserted.parent_id, incomplete.parent_id);
        assert_eq!(inserted.vpc_id, incomplete.subnet.vpc_id);
        assert_eq!(inserted.subnet_id, incomplete.subnet.id());
        let (mac_in_range, kind) = match incomplete.kind {
            NetworkInterfaceKind::Instance => {
                (inserted.mac.is_guest(), "guest")
            }
            NetworkInterfaceKind::Service => {
                (inserted.mac.is_system(), "system")
            }
            NetworkInterfaceKind::Probe => (inserted.mac.is_system(), "probe"),
        };
        assert!(
            mac_in_range,
            "The random MAC address {:?} is not a valid {} address",
            inserted.mac, kind,
        );
        assert_eq!(inserted.transit_ips, incomplete.transit_ips);
    }

    // Test that we fail to insert an interface if there are no available slots
    // on the instance.
    #[tokio::test]
    async fn test_limit_number_of_interfaces_per_instance_query() {
        let context = TestContext::new(
            "test_limit_number_of_interfaces_per_instance_query",
            MAX_NICS_PER_INSTANCE as u8 + 1,
        )
        .await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        for slot in 0..MAX_NICS_PER_INSTANCE {
            let subnet = &context.net1.subnets[slot];
            let interface = IncompleteNetworkInterface::new_instance(
                Uuid::new_v4(),
                instance_id,
                subnet.clone(),
                IdentityMetadataCreateParams {
                    name: format!("interface-{}", slot).parse().unwrap(),
                    description: String::from("description"),
                },
                None,
                vec![],
            )
            .unwrap();
            let inserted_interface = context
                .datastore()
                .instance_create_network_interface_raw(
                    context.opctx(),
                    interface.clone(),
                )
                .await
                .expect("Should be able to insert up to 8 interfaces");
            let actual_slot = usize::from(*inserted_interface.slot);
            assert_eq!(
                slot, actual_slot,
                "Failed to allocate next available interface slot"
            );

            // Check that only the first NIC is designated the primary
            assert_eq!(
                inserted_interface.primary,
                slot == 0,
                "Only the first NIC inserted for an instance should \
                be marked the primary"
            );
        }

        // The next one should fail
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets.last().unwrap().clone(),
            IdentityMetadataCreateParams {
                name: "interface-8".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let result = context
            .datastore()
            .instance_create_network_interface_raw(
                context.opctx(),
                interface.clone(),
            )
            .await
            .expect_err("Should not be able to insert more than 8 interfaces");
        assert!(matches!(result, InsertError::NoSlotsAvailable,));

        context.success().await;
    }

    // Regression for https://github.com/oxidecomputer/omicron/issues/8208
    #[tokio::test]
    async fn allocation_and_deallocation_takes_next_smallest_address() {
        let context = TestContext::new(
            "allocation_and_deallocation_takes_next_smallest_address",
            1,
        )
        .await;

        // Create three instances, each with an interface.
        const N_INSTANCES: usize = 3;
        let mut instances = Vec::with_capacity(N_INSTANCES);
        for _ in 0..N_INSTANCES {
            let instance = context.create_stopped_instance().await;
            let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
            let interface = IncompleteNetworkInterface::new_instance(
                Uuid::new_v4(),
                instance_id,
                context.net1.subnets[0].clone(),
                IdentityMetadataCreateParams {
                    name: "interface-c".parse().unwrap(),
                    description: String::from("description"),
                },
                None,
                vec![],
            )
            .unwrap();
            let intf = context
                .datastore()
                .instance_create_network_interface_raw(
                    context.opctx(),
                    interface,
                )
                .await
                .expect("Failed to insert interface");
            instances.push((instance, intf));
        }

        // Delete the NIC on the first instance.
        let original_ip = instances[0].1.ip.ip();
        context.delete_instance_nics(instances[0].0.id()).await;

        // And recreate it, ensuring we get the same IP address again.
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            InstanceUuid::from_untyped_uuid(instances[0].0.id()),
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let intf = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        instances[0].1 = intf;
        assert_eq!(
            instances[0].1.ip.ip(),
            original_ip,
            "Should have recreated the first available IP address again"
        );

        // Now delete the NICs from the first and second instances.
        for (inst, _) in instances[..2].iter() {
            context.delete_instance_nics(inst.id()).await;
        }

        // Create a new one, and ensure we've taken the _second_ address. The
        // allocation query looks at the first gap upwards and the first gap
        // downwards from any allocated items.
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            InstanceUuid::from_untyped_uuid(instances[0].0.id()),
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();
        let intf = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        assert_eq!(
            intf.ip.ip(),
            instances[1].1.ip.ip(),
            "Should have used the second address",
        );

        context.success().await;
    }

    // Regression for https://github.com/oxidecomputer/omicron/issues/8208
    #[tokio::test]
    async fn allocation_after_explicit_ip_address_takes_next_smallest_address()
    {
        let context = TestContext::new(
            "allocation_after_explicit_ip_address_takes_next_smallest_address",
            1,
        )
        .await;

        // Create one instance, with a specific address.
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());
        const NTH: usize = 8;
        let addr =
            context.net2.subnets[0].ipv4_block.nth(NTH).unwrap_or_else(|| {
                panic!("Should have been able to get the {NTH}-th address")
            });
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net2.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            Some(IpAddr::V4(addr)),
            vec![],
        )
        .unwrap();
        let _ = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");

        // Now create another one, attaching an automatic address.
        let instance2 = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance2.id());
        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net2.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-c".parse().unwrap(),
                description: String::from("description"),
            },
            None,
            vec![],
        )
        .unwrap();

        // The new address should be 1 less than the previous one.
        let interface2 = context
            .datastore()
            .instance_create_network_interface_raw(context.opctx(), interface)
            .await
            .expect("Failed to insert interface");
        assert_eq!(
            IpAddr::V4(
                context
                    .net2
                    .subnets[0]
                    .ipv4_block
                    .nth(NTH - 1)
                    .unwrap_or_else(|| {
                        panic!("Should have been able to get the {NTH}-1-th address")
                    })
            ),
            interface2.ip.ip(),
            "Should have allocated 1 less than the smallest existing address"
        );

        context.success().await;
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

    #[test]
    fn test_last_available_address() {
        let subnet = "172.30.0.0/28".parse().unwrap();
        assert_eq!(
            last_available_address(&subnet),
            "172.30.0.14".parse::<IpAddr>().unwrap(),
        );
        let subnet = "fd00::/64".parse().unwrap();
        assert_eq!(
            last_available_address(&subnet),
            "fd00::ffff:ffff:ffff:fffe".parse::<IpAddr>().unwrap(),
        );
    }

    #[tokio::test]
    async fn test_insert_with_transit_ips() {
        let context = TestContext::new("test_insert_with_transit_ips", 2).await;
        let instance = context.create_stopped_instance().await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.id());

        // Create transit IPs to test with
        let transit_ips = vec![
            "10.0.0.0/24".parse().unwrap(),
            "192.168.1.0/24".parse().unwrap(),
            "172.16.0.0/16".parse().unwrap(),
        ];

        let interface = IncompleteNetworkInterface::new_instance(
            Uuid::new_v4(),
            instance_id,
            context.net1.subnets[0].clone(),
            IdentityMetadataCreateParams {
                name: "interface-with-transit".parse().unwrap(),
                description: String::from("Test interface with transit IPs"),
            },
            None, // Auto-assign IP
            transit_ips.clone(),
        )
        .unwrap();

        let inserted_interface = context
            .datastore()
            .instance_create_network_interface_raw(
                context.opctx(),
                interface.clone(),
            )
            .await
            .expect("Failed to insert interface with transit IPs");

        // Verify the basic interface properties
        assert_interfaces_eq(&interface, &inserted_interface.clone().into());

        // Verify transit IPs are correctly persisted
        assert_eq!(
            inserted_interface.transit_ips.len(),
            transit_ips.len(),
            "Transit IPs count should match"
        );

        for (actual, expected) in
            inserted_interface.transit_ips.iter().zip(transit_ips.iter())
        {
            assert_eq!(
                actual, expected,
                "Transit IP {} should match expected {}",
                actual, expected
            );
        }

        context.success().await;
    }
}
