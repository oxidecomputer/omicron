// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::BlockSize;
use anyhow::bail;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(
    Copy,
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    PartialEq,
)]
#[diesel(sql_type = sql_types::BigInt)]
pub struct ByteCount(pub external::ByteCount);

NewtypeFrom! { () pub struct ByteCount(external::ByteCount); }
NewtypeDeref! { () pub struct ByteCount(external::ByteCount); }

impl ToSql<sql_types::BigInt, Pg> for ByteCount {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::from(self.0),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for ByteCount
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        external::ByteCount::try_from(i64::from_sql(bytes)?)
            .map(ByteCount)
            .map_err(|e| e.into())
    }
}

impl From<BlockSize> for ByteCount {
    fn from(bs: BlockSize) -> Self {
        Self(bs.to_bytes().into())
    }
}

impl TryFrom<i64> for ByteCount {
    type Error = <external::ByteCount as TryFrom<i64>>::Error;
    fn try_from(i: i64) -> Result<Self, Self::Error> {
        Ok(external::ByteCount::try_from(i)?.into())
    }
}

impl From<ByteCount> for i64 {
    fn from(b: ByteCount) -> Self {
        b.0.into()
    }
}

impl TryFrom<diesel::pg::data_types::PgNumeric> for ByteCount {
    type Error = anyhow::Error;
    fn try_from(
        num: diesel::pg::data_types::PgNumeric,
    ) -> Result<Self, Self::Error> {
        match num {
            diesel::pg::data_types::PgNumeric::Positive {
                weight: _,
                scale,
                digits,
            } => {
                // fail if there are digits to right of decimal place -
                // ByteCount is a whole number
                if scale != 0 {
                    bail!("PgNumeric::Positive scale is {}", scale);
                }

                let mut result: i64 = 0;
                let mut multiplier = 1;

                for digit in digits.iter().rev() {
                    result += i64::from(*digit) * multiplier;
                    multiplier *= 10000;
                }

                let result: external::ByteCount = result.try_into()?;

                Ok(result.into())
            }

            diesel::pg::data_types::PgNumeric::Negative { .. } => {
                bail!("negative PgNumeric, cannot convert to ByteCount");
            }

            diesel::pg::data_types::PgNumeric::NaN => {
                bail!("NaN PgNumeric, cannot convert to ByteCount");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pg_numeric_to_byte_count() {
        type IntoResult = Result<ByteCount, anyhow::Error>;

        // assert this does not work for negative or NaN

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Negative {
            weight: 1,
            scale: 0,
            digits: vec![1],
        }
        .try_into();
        assert!(result.is_err());

        let result: IntoResult =
            diesel::pg::data_types::PgNumeric::NaN.try_into();
        assert!(result.is_err());

        // assert this doesn't work for non-whole numbers

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 0,
            scale: 1,
            digits: vec![1],
        }
        .try_into();
        assert!(result.is_err());

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 0,
            scale: 1,
            digits: vec![999],
        }
        .try_into();
        assert!(result.is_err());

        // From https://docs.diesel.rs/diesel/pg/data_types/enum.PgNumeric.html:
        //
        // weight: i16
        //   How many digits come before the decimal point?
        // scale: u16
        //   How many significant digits are there?
        // digits: Vec<i16>
        //   The digits in this number, stored in base 10000

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 1,
            scale: 0,
            digits: vec![1],
        }
        .try_into();
        assert_eq!(result.unwrap().to_bytes(), 1);

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 5,
            scale: 0,
            digits: vec![1, 0],
        }
        .try_into();
        assert_eq!(result.unwrap().to_bytes(), 10000);

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 9,
            scale: 0,
            digits: vec![1, 0, 0],
        }
        .try_into();
        assert_eq!(result.unwrap().to_bytes(), 100000000);

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 9,
            scale: 0,
            digits: vec![1, 0, 9999],
        }
        .try_into();
        assert_eq!(result.unwrap().to_bytes(), 100009999);

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 11,
            scale: 0,
            digits: vec![512, 512, 512],
        }
        .try_into();
        assert_eq!(result.unwrap().to_bytes(), 51205120512);

        let result: IntoResult = diesel::pg::data_types::PgNumeric::Positive {
            weight: 12,
            scale: 0,
            digits: vec![9999, 9999, 9999],
        }
        .try_into();
        assert_eq!(result.unwrap().to_bytes(), 999999999999);
    }

    #[test]
    fn test_bytecount_i64_conversions() {
        let i: i64 = 123;
        let b: ByteCount = ByteCount::try_from(i).unwrap();

        assert_eq!(i64::from(b), i);
    }
}
