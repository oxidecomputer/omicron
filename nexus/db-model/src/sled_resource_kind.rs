// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sled_resource_kind", schema = "public"))]
    pub struct SledResourceKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SledResourceKindEnum)]
    pub enum SledResourceKind;

    // Enum values
    Instance => b"instance"
);
