/*!
 * Facilities for mapping model Rust types to and from database types
 *
 * We'd prefer to put this in omicron-nexus, but the Rust orphan rules prevent
 * that.
 *
 * There are essentially two patterns used for database conversions:
 *
 * (1) For Rust types that map directly to database types, we impl
 *     tokio_postgres's [`tokio_postgres::types::ToSql`] and
 *     [`tokio_postgres::types::FromSql`] traits.  For the most part, these are
 *     newtypes in Rust that wrap a type for which there is already an impl for
 *     these traits and we delegate to those impls where possible.  For example,
 *     [`ByteCount`] is a numeric newtype containing a u64, which maps
 *     directly to a CockroachDB (PostgreSQL) `int`, which is essentially an
 *     `i64`.  The `ToSql` and `FromSql` impls for `ByteCount` delegate to
 *     the existing impls for `i64`.
 *
 * (2) For Rust types that require multiple database values (e.g., an
 *     [`Disk`], which represents an entire row from the Disk table, we impl
 *     `TryFrom<&tokio_postgres::Row>`.  The impl pulls multiple values out of
 *     the row, generally using the names of columns from the table.  We also
 *     impl `SqlSerialize`, which records key-value pairs that can be safely
 *     inserted into SQL "UPDATE" and "INSERT" statements.
 *
 * These often combine to form a hierarchy.  For example, to load an
 * [`Project`] from a row from the Project table:
 *
 * * We start with `Project::try_from(row: &tokio_postgres::Row)`.
 * * The main field of an `Project` is its `IdentityMetadata`, so
 *   `Project::try_from` invokes `IdentityMetadata::try_from(row)` with
 *   the same `row` argument.
 * * `IdentityMetadata` pulls out the fields that it knows about, including
 *   `id` and `name`.
 * * `name` is an [`Name`], which is a newtype that wraps a Rust `String`.
 *   This has a `FromSql` impl.
 *
 * The ToSql/FromSql, and TryFrom impls are in this file because the Rust orphan
 * rules prevent them from going in omicron_nexus.  We're not so restricted on
 * the SqlSerialize interface, and we don't want to have to pull in the other
 * facilities that those impls depend on, so those remain in omicron_nexus.
 */
/*
 * TODO-cleanup We could potentially derive these TryFrom impls.  Diesel and
 * sqlx both have derive macros that do this.  It's not clear how that will
 * compose with our custom types having their own impls of `ToSql` and
 * `FromSql`.
 *
 * TODO-coverage tests for these FromSql and ToSql implementations
 */

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use super::db::sql_row_value;
use crate::api::ByteCount;
use crate::api::Disk;
use crate::api::DiskAttachment;
use crate::api::DiskRuntimeState;
use crate::api::DiskState;
use crate::api::Error;
use crate::api::Generation;
use crate::api::IdentityMetadata;
use crate::api::Instance;
use crate::api::InstanceCpuCount;
use crate::api::InstanceRuntimeState;
use crate::api::InstanceState;
use crate::api::MacAddr;
use crate::api::Name;
use crate::api::OximeterAssignment;
use crate::api::OximeterInfo;
use crate::api::ProducerEndpoint;
use crate::api::Project;
use crate::api::VPCSubnet;
use crate::api::VNIC;
use crate::api::VPC;
use crate::api::{Ipv4Net, Ipv6Net};
use crate::bail_unless;
use chrono::DateTime;
use chrono::Utc;
use std::convert::TryFrom;
use uuid::Uuid;

/*
 * FromSql/ToSql impls used for simple Rust types
 * (see module-level documentation above)
 */

/**
 * Define impls for ToSql and FromSql for a type T such that &T: Into<D> and
 * T: TryFrom<D>.  These impls delegate to the "D" impls of these traits.  See
 * the module-level documentation for why this is useful.
 */
#[macro_export]
macro_rules! impl_sql_wrapping {
    ($T:ident, $D:ty) => {
        impl tokio_postgres::types::ToSql for $T {
            fn to_sql(
                &self,
                ty: &tokio_postgres::types::Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<
                tokio_postgres::types::IsNull,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                <$D>::from(self).to_sql(ty, out)
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as tokio_postgres::types::ToSql>::accepts(ty)
            }

            tokio_postgres::types::to_sql_checked!();
        }

        impl<'a> tokio_postgres::types::FromSql<'a> for $T {
            fn from_sql(
                ty: &tokio_postgres::types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                let value: $D =
                    <$D as tokio_postgres::types::FromSql>::from_sql(ty, raw)?;
                $T::try_from(value).map_err(|e| e.into())
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as tokio_postgres::types::FromSql>::accepts(ty)
            }
        }
    };
}

impl_sql_wrapping!(ByteCount, i64);
impl_sql_wrapping!(Generation, i64);
impl_sql_wrapping!(InstanceCpuCount, i64);
impl_sql_wrapping!(Name, &str);

// Conversion to/from SQL types for Ipv4Net.
impl tokio_postgres::types::ToSql for Ipv4Net {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<
        tokio_postgres::types::IsNull,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        postgres_protocol::types::inet_to_sql(
            self.addr().into(),
            self.prefix_len(),
            out,
        );
        Ok(tokio_postgres::types::IsNull::No)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty, &tokio_postgres::types::Type::INET)
    }

    tokio_postgres::types::to_sql_checked!();
}

impl<'a> tokio_postgres::types::FromSql<'a> for Ipv4Net {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let value = postgres_protocol::types::inet_from_sql(raw)?;
        let (addr, mask) = (value.addr(), value.netmask());
        if let std::net::IpAddr::V4(addr) = addr {
            Ok(Ipv4Net(ipnet::Ipv4Net::new(addr, mask)?))
        } else {
            panic!("Attempted to deserialize IPv6 subnet from database, expected IPv4 subnet");
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty, &tokio_postgres::types::Type::INET)
    }
}

// Conversion to/from SQL types for Ipv6Net.
impl tokio_postgres::types::ToSql for Ipv6Net {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<
        tokio_postgres::types::IsNull,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        postgres_protocol::types::inet_to_sql(
            self.addr().into(),
            self.prefix_len(),
            out,
        );
        Ok(tokio_postgres::types::IsNull::No)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty, &tokio_postgres::types::Type::INET)
    }

    tokio_postgres::types::to_sql_checked!();
}

impl<'a> tokio_postgres::types::FromSql<'a> for Ipv6Net {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let value = postgres_protocol::types::inet_from_sql(raw)?;
        let (addr, mask) = (value.addr(), value.netmask());
        if let std::net::IpAddr::V6(addr) = addr {
            Ok(Ipv6Net(ipnet::Ipv6Net::new(addr, mask)?))
        } else {
            panic!("Attempted to deserialize IPv4 subnet from database, expected IPv6 subnet");
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty, &tokio_postgres::types::Type::INET)
    }
}

// Conversion to/from SQL types for MacAddr.
//
// As with the IP subnet above, there's no existing crate for MAC addresses that satisfies all our
// needs. The `eui48` crate implements SQL conversions, but not `Serialize`/`Deserialize` or
// `JsonSchema`. The `macaddr` crate implements serialization, but not the other two. Since
// serialization is more annoying that the others, we choose the latter, so we're implementing the
// SQL serialization here. CockroachDB does not currently support the Postgres MACADDR type, so
// we're storing it as a CHAR(17), 12 characters for the hexadecimal and 5 ":"-separators.
impl From<&MacAddr> for String {
    fn from(addr: &MacAddr) -> String {
        format!("{}", addr)
    }
}

impl TryFrom<String> for MacAddr {
    type Error = macaddr::ParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse().map(|addr| MacAddr(addr))
    }
}
impl_sql_wrapping!(MacAddr, String);

/*
 * TryFrom impls used for more complex Rust types
 */

/// Load an [`IdentityMetadata`] from a row of any table that contains the
/// usual identity fields: "id", "name", "description, "time_created", and
/// "time_modified".
impl TryFrom<&tokio_postgres::Row> for IdentityMetadata {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let time_deleted: Option<DateTime<Utc>> =
            sql_row_value(value, "time_deleted")?;
        /*
         * We could support representing deleted objects, but we would want to
         * think about how to do that.  For example, we might want to use
         * separate types so that the control plane can't accidentally do things
         * like attach a disk to a deleted Instance.  We haven't figured any of
         * this out, and there's no need yet.
         */
        bail_unless!(
            time_deleted.is_none(),
            "model does not support objects that have been deleted"
        );
        Ok(IdentityMetadata {
            id: sql_row_value(value, "id")?,
            name: sql_row_value(value, "name")?,
            description: sql_row_value(value, "description")?,
            time_created: sql_row_value(value, "time_created")?,
            time_modified: sql_row_value(value, "time_modified")?,
        })
    }
}

impl TryFrom<&tokio_postgres::Row> for InstanceState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let variant: &str = sql_row_value(value, "instance_state")?;
        InstanceState::try_from(variant)
            .map_err(|err| Error::InternalError { message: err })
    }
}

/// Load an [`Project`] from a whole row of the "Project" table.
impl TryFrom<&tokio_postgres::Row> for Project {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Project { identity: IdentityMetadata::try_from(value)? })
    }
}

/// Load an [`Instance`] from a whole row of the "Instance" table.
impl TryFrom<&tokio_postgres::Row> for Instance {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Instance {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            ncpus: sql_row_value(value, "ncpus")?,
            memory: sql_row_value(value, "memory")?,
            hostname: sql_row_value(value, "hostname")?,
            runtime: InstanceRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`InstanceRuntimeState`] from a row of the "Instance" table,
/// using the "instance_state", "active_server_id", "state_generation", and
/// "time_state_updated" columns.
impl TryFrom<&tokio_postgres::Row> for InstanceRuntimeState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(InstanceRuntimeState {
            run_state: InstanceState::try_from(value)?,
            sled_uuid: sql_row_value(value, "active_server_id")?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// Load an [`Disk`] from a row of the "Disk" table.
impl TryFrom<&tokio_postgres::Row> for Disk {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Disk {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            create_snapshot_id: sql_row_value(value, "origin_snapshot")?,
            size: sql_row_value(value, "size_bytes")?,
            runtime: DiskRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`DiskAttachment`] from a database row containing those columns
/// of the Disk table that describe the attachment: "id", "name", "disk_state",
/// "attach_instance_id"
impl TryFrom<&tokio_postgres::Row> for DiskAttachment {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(DiskAttachment {
            instance_id: sql_row_value(value, "attach_instance_id")?,
            disk_id: sql_row_value(value, "id")?,
            disk_name: sql_row_value(value, "name")?,
            disk_state: DiskState::try_from(value)?,
        })
    }
}

/// Load an [`DiskRuntimeState`'] from a row from the Disk table, using the
/// columns needed for [`DiskState`], plus "state_generation" and
/// "time_state_updated".
impl TryFrom<&tokio_postgres::Row> for DiskRuntimeState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(DiskRuntimeState {
            disk_state: DiskState::try_from(value)?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// Load an [`DiskState`] from a row from the Disk table, using the columns
/// "disk_state" and "attach_instance_id".
impl TryFrom<&tokio_postgres::Row> for DiskState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let disk_state_str: &str = sql_row_value(value, "disk_state")?;
        let instance_uuid: Option<Uuid> =
            sql_row_value(value, "attach_instance_id")?;
        DiskState::try_from((disk_state_str, instance_uuid))
            .map_err(|e| Error::internal_error(&e))
    }
}

/// Load a [`ProducerEndpoint`] from a row in the `MetricProducer` table, using
/// the columns "id", "ip", "port", "interval", and "route"
impl TryFrom<&tokio_postgres::Row> for ProducerEndpoint {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let id: Uuid = sql_row_value(value, "id")?;
        let ip: IpAddr = sql_row_value(value, "ip")?;
        let port: i32 = sql_row_value(value, "port")?;
        let address = SocketAddr::new(ip, port as _);
        let base_route: String = sql_row_value(value, "route")?;
        let interval =
            Duration::from_secs_f64(sql_row_value(value, "interval")?);
        Ok(Self { id, address, base_route, interval })
    }
}

/// Load an [`OximeterInfo`] from a row in the `Oximeter` table, using the
/// columns "id", "ip", and "port".
impl TryFrom<&tokio_postgres::Row> for OximeterInfo {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let collector_id: Uuid = sql_row_value(value, "id")?;
        let ip: IpAddr = sql_row_value(value, "ip")?;
        let port: i32 = sql_row_value(value, "port")?;
        let address = SocketAddr::new(ip, port as _);
        Ok(Self { collector_id, address })
    }
}

/// Load an [`OximeterAssignment`] from a row in the `OximeterAssignment`
/// table, using the columns "oximeter_id" and "producer_id"
impl TryFrom<&tokio_postgres::Row> for OximeterAssignment {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let oximeter_id: Uuid = sql_row_value(value, "oximeter_id")?;
        let producer_id: Uuid = sql_row_value(value, "producer_id")?;
        Ok(Self { oximeter_id, producer_id })
    }
}

/// Load an [`VPC`] from a row in the `VPC` table.
impl TryFrom<&tokio_postgres::Row> for VPC {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self { identity: IdentityMetadata::try_from(value)? })
    }
}

/// Load an [`VPCSubnet`] from a row in the `VPCSubnet` table.
impl TryFrom<&tokio_postgres::Row> for VPCSubnet {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: IdentityMetadata::try_from(value)?,
            vpc_id: sql_row_value(value, "vpc_id")?,
            ipv4_block: sql_row_value(value, "ipv4_block")?,
            ipv6_block: sql_row_value(value, "ipv6_block")?,
        })
    }
}

/// Load an [`VPC`] from a row in the `VNIC` table.
impl TryFrom<&tokio_postgres::Row> for VNIC {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: IdentityMetadata::try_from(value)?,
            vpc_id: sql_row_value(value, "vpc_id")?,
            subnet_id: sql_row_value(value, "subnet_id")?,
            mac: sql_row_value(value, "mac")?,
            ip: sql_row_value(value, "ip")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::private::BytesMut;
    use tokio_postgres::types::{FromSql, ToSql};

    #[test]
    fn test_ipv4net_conversion() {
        let subnet = Ipv4Net("192.168.1.0/24".parse().unwrap());
        let ty = tokio_postgres::types::Type::INET;
        let mut buf = BytesMut::with_capacity(128);
        let is_null = subnet.to_sql(&ty, &mut buf).unwrap();
        assert!(matches!(is_null, tokio_postgres::types::IsNull::No));
        let from_sql = Ipv4Net::from_sql(&ty, &buf).unwrap();
        assert_eq!(subnet, from_sql);
        assert_eq!(
            from_sql.addr(),
            "192.168.1.0".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(from_sql.prefix_len(), 24);

        let bad = b"some-bad-net";
        assert!(Ipv4Net::from_sql(&ty, &bad[..]).is_err());
    }

    #[test]
    fn test_ipv6net_conversion() {
        let subnet = Ipv6Net("fd00:1234::/24".parse().unwrap());
        let ty = tokio_postgres::types::Type::INET;
        let mut buf = BytesMut::with_capacity(256);
        let is_null = subnet.to_sql(&ty, &mut buf).unwrap();
        assert!(matches!(is_null, tokio_postgres::types::IsNull::No));
        let from_sql = Ipv6Net::from_sql(&ty, &buf).unwrap();
        assert_eq!(subnet, from_sql);
        assert_eq!(
            from_sql.addr(),
            "fd00:1234::".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(from_sql.prefix_len(), 24);

        let bad = b"some-bad-net";
        assert!(Ipv6Net::from_sql(&ty, &bad[..]).is_err());
    }

    #[test]
    fn test_macaddr_conversion() {
        let mac = MacAddr([0xAFu8; 6].into());
        let ty = tokio_postgres::types::Type::MACADDR;
        let mut buf = BytesMut::with_capacity(128);
        let is_null = mac.to_sql(&ty, &mut buf).unwrap();
        assert!(matches!(is_null, tokio_postgres::types::IsNull::No));
        let from_sql = MacAddr::from_sql(&ty, &buf).unwrap();
        assert_eq!(mac, from_sql);
        assert_eq!(from_sql.into_array(), [0xaf; 6]);

        let bad = b"not:a:mac:addr";
        assert!(MacAddr::from_sql(&ty, &bad[..]).is_err());
    }
}
