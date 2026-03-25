// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::Deserialize;
use serde::Serialize;

impl_enum_type!(
    EreporterTypeEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Serialize,
        Deserialize,
        AsExpression,
        FromSqlRow,
        strum::VariantArray,
    )]
    #[serde(rename_all = "snake_case")]
    pub enum EreporterType;

    Sp => b"sp"
    Host => b"host"
);

impl EreporterType {
    pub fn for_reporter(reporter: &nexus_types::fm::ereport::Reporter) -> Self {
        match reporter {
            nexus_types::fm::ereport::Reporter::Sp { .. } => Self::Sp,
            nexus_types::fm::ereport::Reporter::HostOs { .. } => Self::Host,
        }
    }
}
