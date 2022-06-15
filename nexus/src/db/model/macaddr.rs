// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use rand::thread_rng;
use rand::Rng;

#[derive(Clone, Copy, Debug, PartialEq, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::BigInt)]
pub struct MacAddr(pub external::MacAddr);

impl MacAddr {
    // Guest MAC addresses begin with the Oxide OUI A8:40:25. Further, guest
    // address are constrained to be in the virtual address range
    // A8:40:24:F_:__:__. Even further, the range F0:00:00 - FE:FF:FF is
    // reserved for customer-visible addresses (FF:00:00-FF:FF:FF is for
    // system MAC addresses). See RFD 174 for the discussion of the virtual
    // range, and
    // https://github.com/oxidecomputer/omicron/pull/955#discussion_r856432498
    // for an initial discussion of the customer/system address range split.
    pub(crate) const MIN_GUEST_ADDR: i64 = 0xA8_40_25_F0_00_00;
    pub(crate) const MAX_GUEST_ADDR: i64 = 0xA8_40_25_FE_FF_FF;

    /// Generate a random MAC address for a guest network interface
    pub fn random_guest() -> Self {
        let value =
            thread_rng().gen_range(Self::MIN_GUEST_ADDR..=Self::MAX_GUEST_ADDR);
        Self::from_i64(value)
    }

    /// Construct a MAC address from its i64 big-endian byte representation.
    // NOTE: This is the representation used in the database.
    pub(crate) fn from_i64(value: i64) -> Self {
        let bytes = value.to_be_bytes();
        Self(external::MacAddr(macaddr::MacAddr6::new(
            bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        )))
    }

    /// Convert a MAC address to its i64 big-endian byte representation
    // NOTE: This is the representation used in the database.
    pub(crate) fn to_i64(self) -> i64 {
        let bytes = self.0.as_bytes();
        i64::from_be_bytes([
            0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
        ])
    }
}

NewtypeFrom! { () pub struct MacAddr(external::MacAddr); }
NewtypeDeref! { () pub struct MacAddr(external::MacAddr); }

impl ToSql<sql_types::BigInt, Pg> for MacAddr {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &self.to_i64(),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for MacAddr
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        let value = i64::from_sql(bytes)?;
        Ok(MacAddr::from_i64(value))
    }
}

#[cfg(test)]
mod tests {
    use super::MacAddr;

    #[test]
    fn test_mac_to_int_conversions() {
        let original: i64 = 0xa8_40_25_ff_00_01;
        let mac = MacAddr::from_i64(original);
        assert_eq!(mac.0.as_bytes(), &[0xa8, 0x40, 0x25, 0xff, 0x00, 0x01]);
        let conv = mac.to_i64();
        assert_eq!(original, conv);
    }
}
