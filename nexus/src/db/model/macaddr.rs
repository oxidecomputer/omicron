// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use rand::{rngs::StdRng, SeedableRng};
use std::convert::TryFrom;

#[derive(Clone, Copy, Debug, PartialEq, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::Text)]
pub struct MacAddr(pub external::MacAddr);

impl MacAddr {
    /// Generate a unique MAC address for an interface
    pub fn new() -> Result<Self, external::Error> {
        use rand::Fill;
        // Use the Oxide OUI A8 40 25
        let mut addr = [0xA8, 0x40, 0x25, 0x00, 0x00, 0x00];
        addr[3..].try_fill(&mut StdRng::from_entropy()).map_err(|_| {
            external::Error::internal_error("failed to generate MAC")
        })?;
        // From RFD 174, Oxide virtual MACs are constrained to have these bits
        // set.
        addr[3] |= 0xF0;
        // TODO-correctness: We should use an explicit allocator for the MACs
        // given the small address space. Right now creation requests may fail
        // due to MAC collision, especially given the 20-bit space.
        Ok(Self(external::MacAddr(macaddr::MacAddr6::from(addr))))
    }
}

NewtypeFrom! { () pub struct MacAddr(external::MacAddr); }
NewtypeDeref! { () pub struct MacAddr(external::MacAddr); }

impl ToSql<sql_types::Text, Pg> for MacAddr {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Text, DB> for MacAddr
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        external::MacAddr::try_from(String::from_sql(bytes)?)
            .map(MacAddr)
            .map_err(|e| e.into())
    }
}
