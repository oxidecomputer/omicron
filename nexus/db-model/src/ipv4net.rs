// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use omicron_common::nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv4Addr;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = sql_types::Inet)]
pub struct Ipv4Net(pub external::Ipv4Net);

NewtypeFrom! { () pub struct Ipv4Net(external::Ipv4Net); }
NewtypeDeref! { () pub struct Ipv4Net(external::Ipv4Net); }

impl Ipv4Net {
    /// Check if an address is a valid user-requestable address for this subnet
    pub fn check_requestable_addr(&self, addr: Ipv4Addr) -> bool {
        self.contains(addr)
            && (
                // First N addresses are reserved
                self.iter()
                    .take(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                    .all(|this| this != addr)
            )
            && (
                // Last address in the subnet is reserved
                addr != self.broadcast()
            )
    }
}

impl ToSql<sql_types::Inet, Pg> for Ipv4Net {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <IpNetwork as ToSql<sql_types::Inet, Pg>>::to_sql(
            &IpNetwork::V4(*self.0),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Inet, DB> for Ipv4Net
where
    DB: Backend,
    IpNetwork: FromSql<sql_types::Inet, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        let inet = IpNetwork::from_sql(bytes)?;
        match inet {
            IpNetwork::V4(net) => Ok(Ipv4Net(external::Ipv4Net(net))),
            _ => Err("Expected IPV4".into()),
        }
    }
}
