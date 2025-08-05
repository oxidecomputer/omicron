// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize;
use diesel::deserialize::FromSql;
use diesel::pg::Pg;
use diesel::serialize;
use diesel::serialize::ToSql;
use diesel::sql_types;
use ipnetwork::IpNetwork;
use serde::Deserialize;
use serde::Serialize;

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = sql_types::Inet)]
pub enum IpNet {
    V4(crate::Ipv4Net),
    V6(crate::Ipv6Net),
}

impl ::std::fmt::Display for IpNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IpNet::V4(inner) => inner.fmt(f),
            IpNet::V6(inner) => inner.fmt(f),
        }
    }
}

impl From<IpNet> for ::std::net::IpAddr {
    fn from(value: IpNet) -> Self {
        match value {
            IpNet::V4(inner) => ::std::net::IpAddr::V4(inner.addr()),
            IpNet::V6(inner) => ::std::net::IpAddr::V6(inner.addr()),
        }
    }
}

impl From<ipnetwork::IpNetwork> for IpNet {
    fn from(value: ipnetwork::IpNetwork) -> Self {
        match value {
            IpNetwork::V4(ipv4) => Self::from(oxnet::Ipv4Net::from(ipv4)),
            IpNetwork::V6(ipv6) => Self::from(oxnet::Ipv6Net::from(ipv6)),
        }
    }
}

impl From<oxnet::Ipv4Net> for IpNet {
    fn from(value: oxnet::Ipv4Net) -> Self {
        Self::V4(crate::Ipv4Net::from(value))
    }
}

impl From<oxnet::Ipv6Net> for IpNet {
    fn from(value: oxnet::Ipv6Net) -> Self {
        Self::V6(crate::Ipv6Net::from(value))
    }
}

impl From<oxnet::IpNet> for IpNet {
    fn from(value: oxnet::IpNet) -> Self {
        match value {
            oxnet::IpNet::V4(ipv4_net) => {
                Self::V4(crate::Ipv4Net::from(ipv4_net))
            }
            oxnet::IpNet::V6(ipv6_net) => {
                Self::V6(crate::Ipv6Net::from(ipv6_net))
            }
        }
    }
}

impl ToSql<sql_types::Inet, Pg> for IpNet {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        let inner = match self {
            IpNet::V4(inner) => IpNetwork::V4(inner.0.into()),
            IpNet::V6(inner) => IpNetwork::V6(inner.0.into()),
        };
        <IpNetwork as ToSql<sql_types::Inet, Pg>>::to_sql(
            &inner,
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Inet, DB> for IpNet
where
    DB: Backend,
    IpNetwork: FromSql<sql_types::Inet, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let inet = IpNetwork::from_sql(bytes)?;
        match inet {
            IpNetwork::V4(net) => Ok(Self::V4(crate::Ipv4Net(net.into()))),
            IpNetwork::V6(net) => Ok(Self::V6(crate::Ipv6Net(net.into()))),
        }
    }
}
