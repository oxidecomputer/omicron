// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Macros used to create ToSql and FromSql impls required by Diesel

/// Shamelessly cribbed and mutated from buildomat/common/src/db.rs
/// Thanks @jmc
macro_rules! bcs_new_type {
    ($name:ident, $mytype:ty) => {
        #[derive(
            Clone,
            PartialEq,
            Debug,
            FromSqlRow,
            diesel::expression::AsExpression,
        )]
        #[diesel(sql_type = diesel::sql_types::Binary)]
        pub struct $name(pub $mytype);

        impl ToSql<diesel::sql_types::Binary, diesel::sqlite::Sqlite> for $name
        where
            Vec<u8>: ToSql<diesel::sql_types::Binary, diesel::sqlite::Sqlite>,
        {
            fn to_sql(
                &self,
                out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                out.set_value(bcs::to_bytes(&self.0)?);
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB> FromSql<diesel::sql_types::Binary, DB> for $name
        where
            DB: diesel::backend::Backend,
            Vec<u8>: FromSql<diesel::sql_types::Binary, DB>,
        {
            fn from_sql(
                bytes: diesel::backend::RawValue<DB>,
            ) -> diesel::deserialize::Result<Self> {
                Ok($name(bcs::from_bytes(&Vec::<u8>::from_sql(bytes)?)?))
            }
        }

        impl From<$name> for $mytype {
            fn from(t: $name) -> Self {
                t.0
            }
        }

        impl From<$mytype> for $name {
            fn from(t: $mytype) -> $name {
                $name(t)
            }
        }

        impl std::ops::Deref for $name {
            type Target = $mytype;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

macro_rules! array_new_type {
    ($name:ident, $len:expr) => {
        #[derive(
            PartialEq,
            Clone,
            Debug,
            FromSqlRow,
            diesel::expression::AsExpression,
        )]
        #[diesel(sql_type = diesel::sql_types::Binary)]
        pub struct $name(pub [u8; $len]);

        impl ToSql<diesel::sql_types::Binary, diesel::sqlite::Sqlite> for $name
        where
            Vec<u8>: ToSql<diesel::sql_types::Binary, diesel::sqlite::Sqlite>,
        {
            fn to_sql(
                &self,
                out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                let mut copy = vec![0; $len];
                copy.copy_from_slice(&self.0[..]);
                out.set_value(copy);
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB> FromSql<diesel::sql_types::Binary, DB> for $name
        where
            DB: diesel::backend::Backend,
            Vec<u8>: FromSql<diesel::sql_types::Binary, DB>,
        {
            fn from_sql(
                bytes: diesel::backend::RawValue<DB>,
            ) -> diesel::deserialize::Result<Self> {
                let read_bytes = Vec::<u8>::from_sql(bytes)?;
                if read_bytes.len() != $len {
                    return Err(format!(
                        "Invalid length. Expected: {}, Actual: {}",
                        $len,
                        read_bytes.len()
                    )
                    .into());
                }
                let mut out = [0u8; $len];
                out.copy_from_slice(&read_bytes[..]);
                Ok($name(out))
            }
        }

        impl From<$name> for [u8; $len] {
            fn from(t: $name) -> Self {
                t.0
            }
        }

        impl From<[u8; $len]> for $name {
            fn from(t: [u8; $len]) -> $name {
                $name(t)
            }
        }

        impl std::ops::Deref for $name {
            type Target = [u8; $len];

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

pub(crate) use array_new_type;
pub(crate) use bcs_new_type;
