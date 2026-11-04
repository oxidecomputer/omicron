// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use rand::{SeedableRng, rngs::StdRng};
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;

use crate::RequestAddressError;

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
pub struct Ipv6Net(pub oxnet::Ipv6Net);

NewtypeFrom! { () pub struct Ipv6Net(oxnet::Ipv6Net); }
NewtypeDeref! { () pub struct Ipv6Net(oxnet::Ipv6Net); }

impl Ipv6Net {
    /// Generate a random subnetwork from this one, of the given prefix length.
    ///
    /// `None` is returned if:
    ///
    ///  - `prefix` is less than this address's prefix
    ///  - `prefix` is greater than 128
    ///
    /// Note that if the prefix is the same as this address's prefix, a copy of
    /// `self` is returned.
    pub fn random_subnet(&self, prefix: u8) -> Option<Self> {
        use rand::RngCore;

        const MAX_IPV6_SUBNET_PREFIX: u8 = 128;
        if prefix < self.width() || prefix > MAX_IPV6_SUBNET_PREFIX {
            return None;
        }
        if prefix == self.width() {
            return Some(*self);
        }

        // Generate a random address
        let mut rng = if cfg!(test) {
            StdRng::seed_from_u64(0)
        } else {
            StdRng::from_os_rng()
        };
        let random =
            (u128::from(rng.next_u64()) << 64) | u128::from(rng.next_u64());

        // Generate a mask for the new address.
        //
        // We're operating on the big-endian byte representation of the address.
        // So shift down by the prefix, and then invert, so that we have 1's
        // on the leading bits up to the prefix.
        let full_mask = !(u128::MAX >> prefix);

        // Get the existing network address and mask.
        let network = u128::from(self.prefix());
        let network_mask = u128::from(self.mask_addr());

        // Take random bits _only_ where the new mask is set.
        let random_mask = full_mask ^ network_mask;

        let out = (network & network_mask) | (random & random_mask);
        let addr = std::net::Ipv6Addr::from(out);
        let net = oxnet::Ipv6Net::new(addr, prefix)
            .expect("Failed to create random subnet");
        Some(Self(net))
    }

    /// Check if an address is a valid user-requestable address for this subnet
    pub fn check_requestable_addr(
        &self,
        addr: Ipv6Addr,
    ) -> Result<(), RequestAddressError> {
        if !self.contains(addr) {
            return Err(RequestAddressError::OutsideSubnet(
                addr.into(),
                oxnet::IpNet::from(self.0).into(),
            ));
        }
        // Only the first N addresses are reserved
        if self
            .iter()
            .take(NUM_INITIAL_RESERVED_IP_ADDRESSES)
            .any(|this| this == addr)
        {
            return Err(RequestAddressError::Reserved);
        }
        Ok(())
    }
}

impl ToSql<sql_types::Inet, Pg> for Ipv6Net {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <IpNetwork as ToSql<sql_types::Inet, Pg>>::to_sql(
            &IpNetwork::V6(self.0.into()),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Inet, DB> for Ipv6Net
where
    DB: Backend,
    IpNetwork: FromSql<sql_types::Inet, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let inet = IpNetwork::from_sql(bytes)?;
        match inet {
            IpNetwork::V6(net) => Ok(Ipv6Net(net.into())),
            _ => Err("Expected IPV6".into()),
        }
    }
}
