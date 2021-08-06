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

use super::db::sql_row_value;
use crate::api::external::ByteCount;
use crate::api::external::Error;
use crate::api::external::Generation;
use crate::api::external::IdentityMetadata;
use crate::api::external::InstanceCpuCount;
use crate::api::external::MacAddr;
use crate::api::external::Name;
use crate::api::external::NetworkInterface;
use crate::api::external::Vpc;
use crate::api::external::VpcSubnet;
use crate::api::external::{Ipv4Net, Ipv6Net};
use crate::bail_unless;
use chrono::DateTime;
use chrono::Utc;
use std::convert::TryFrom;

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

/// Load an [`Vpc`] from a row in the `Vpc` table.
impl TryFrom<&tokio_postgres::Row> for Vpc {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
        })
    }
}

/// Load a [`VpcSubnet`] from a row in the `VpcSubnet` table.
impl TryFrom<&tokio_postgres::Row> for VpcSubnet {
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

/// Load a [`NetworkInterface`] from a row in the `NetworkInterface` table.
impl TryFrom<&tokio_postgres::Row> for NetworkInterface {
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
