// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{DatabaseString, impl_from_sql_text};
use diesel::sql_types;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::str::FromStr;

/// Newtype wrapper around [`external::L4PortRange`] so we can derive
/// diesel traits for it
#[derive(
    Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize,
)]
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct L4PortRange(pub external::L4PortRange);
NewtypeFrom! { () pub struct L4PortRange(external::L4PortRange); }
NewtypeDeref! { () pub struct L4PortRange(external::L4PortRange); }

impl DatabaseString for L4PortRange {
    type Error = <external::L4PortRange as FromStr>::Err;

    fn to_database_string(&self) -> Cow<'_, str> {
        self.0.to_string().into()
    }

    fn from_database_string(s: &str) -> Result<Self, Self::Error> {
        s.parse::<external::L4PortRange>().map(Self)
    }
}

impl_from_sql_text!(L4PortRange);
