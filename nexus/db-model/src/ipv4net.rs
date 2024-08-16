// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::vpc_subnet::RequestAddressError;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv4Addr;

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
pub struct Ipv4Net(pub oxnet::Ipv4Net);

NewtypeFrom! { () pub struct Ipv4Net(oxnet::Ipv4Net); }
NewtypeDeref! { () pub struct Ipv4Net(oxnet::Ipv4Net); }

impl Ipv4Net {
    /// Check if an address is a valid user-requestable address for this subnet
    pub fn check_requestable_addr(
        &self,
        addr: Ipv4Addr,
    ) -> Result<(), RequestAddressError> {
        if !self.contains(addr) {
            return Err(RequestAddressError::OutsideSubnet(
                addr.into(),
                oxnet::IpNet::from(self.0).into(),
            ));
        }
        // Only the first N addresses are reserved
        if self
            .addr_iter()
            .take(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .any(|this| this == addr)
        {
            return Err(RequestAddressError::Reserved);
        }
        // Last address in the subnet is reserved
        if addr == self.broadcast().expect("narrower subnet than expected") {
            return Err(RequestAddressError::Broadcast);
        }

        Ok(())
    }
}

impl ToSql<sql_types::Inet, Pg> for Ipv4Net {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <IpNetwork as ToSql<sql_types::Inet, Pg>>::to_sql(
            &IpNetwork::V4(self.0.into()),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Inet, DB> for Ipv4Net
where
    DB: Backend,
    IpNetwork: FromSql<sql_types::Inet, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let inet = IpNetwork::from_sql(bytes)?;
        match inet {
            IpNetwork::V4(net) => Ok(Ipv4Net(net.into())),
            _ => Err("Expected IPV4".into()),
        }
    }
}
