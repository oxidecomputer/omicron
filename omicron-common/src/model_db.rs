/*!
 * Facilities for mapping model Rust types to and from database types
 *
 * For Rust types that map directly to database types, we impl
 * tokio_postgres's [`tokio_postgres::types::ToSql`] and
 * [`tokio_postgres::types::FromSql`] traits.  For the most part, these are
 * newtypes in Rust that wrap a type for which there is already an impl for
 * these traits and we delegate to those impls where possible.  For example,
 * [`ByteCount`] is a numeric newtype containing a u64, which maps
 * directly to a CockroachDB (PostgreSQL) `int`, which is essentially an
 * `i64`.  The `ToSql` and `FromSql` impls for `ByteCount` delegate to
 * the existing impls for `i64`.
 */
/*
 * TODO-cleanup We could potentially derive these TryFrom impls.  Diesel and
 * sqlx both have derive macros that do this.  It's not clear how that will
 * compose with our custom types having their own impls of `ToSql` and
 * `FromSql`.
 *
 * TODO-coverage tests for these FromSql and ToSql implementations
 */

use crate::api::external::ByteCount;
use crate::api::external::Generation;
use crate::api::external::InstanceCpuCount;
use crate::api::external::MacAddr;
use crate::api::external::Name;
use crate::api::external::{Ipv4Net, Ipv6Net};
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
