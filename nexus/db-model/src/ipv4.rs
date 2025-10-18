// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database-friendly IPv4 addresses

use diesel::backend::Backend;
use diesel::deserialize;
use diesel::deserialize::FromSql;
use diesel::pg::Pg;
use diesel::serialize;
use diesel::serialize::Output;
use diesel::serialize::ToSql;
use diesel::sql_types::Inet;
use ipnetwork::IpNetwork;
use ipnetwork::Ipv4Network;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;

#[derive(
    Clone,
    Copy,
    AsExpression,
    FromSqlRow,
    PartialEq,
    Ord,
    PartialOrd,
    Eq,
    Deserialize,
    Serialize,
)]
#[diesel(sql_type = Inet)]
pub struct Ipv4Addr(std::net::Ipv4Addr);

NewtypeDebug! { () pub struct Ipv4Addr(std::net::Ipv4Addr); }
NewtypeFrom! { () pub struct Ipv4Addr(std::net::Ipv4Addr); }
NewtypeDeref! { () pub struct Ipv4Addr(std::net::Ipv4Addr); }

impl From<&std::net::Ipv4Addr> for Ipv4Addr {
    fn from(addr: &std::net::Ipv4Addr) -> Self {
        Self(*addr)
    }
}

impl From<Ipv4Addr> for std::net::IpAddr {
    fn from(value: Ipv4Addr) -> Self {
        std::net::IpAddr::from(value.0)
    }
}

impl From<&Ipv4Addr> for std::net::IpAddr {
    fn from(value: &Ipv4Addr) -> Self {
        (*value).into()
    }
}

impl From<Ipv4Addr> for Ipv4Network {
    fn from(value: Ipv4Addr) -> Self {
        Ipv4Network::from(value.0)
    }
}

impl From<Ipv4Addr> for IpNetwork {
    fn from(value: Ipv4Addr) -> Self {
        IpNetwork::V4(Ipv4Network::from(value.0))
    }
}

impl ToSql<Inet, Pg> for Ipv4Addr {
    fn to_sql<'a>(&'a self, out: &mut Output<'a, '_, Pg>) -> serialize::Result {
        let net = IpNetwork::V4(Ipv4Network::from(self.0));
        <IpNetwork as ToSql<Inet, Pg>>::to_sql(&net, &mut out.reborrow())
    }
}

impl<DB> FromSql<Inet, DB> for Ipv4Addr
where
    DB: Backend,
    IpNetwork: FromSql<Inet, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        match IpNetwork::from_sql(bytes)?.ip() {
            std::net::IpAddr::V4(ip) => Ok(Self(ip)),
            v6 => {
                Err(Box::new(Error::internal_error(
                    format!("Expected an IPv4 address from the database, found IPv6: '{}'", v6).as_str()
                )))
            }
        }
    }
}
