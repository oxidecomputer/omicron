// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::internal_api;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "physical_disk_kind"))]
    pub struct PhysicalDiskKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = PhysicalDiskKindEnum)]
    pub enum PhysicalDiskKind;

    // Enum values
    M2 => b"m2"
    U2 => b"u2"
);

impl From<internal_api::params::PhysicalDiskKind> for PhysicalDiskKind {
    fn from(k: internal_api::params::PhysicalDiskKind) -> Self {
        match k {
            internal_api::params::PhysicalDiskKind::M2 => PhysicalDiskKind::M2,
            internal_api::params::PhysicalDiskKind::U2 => PhysicalDiskKind::U2,
        }
    }
}
