// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{SqlU32, impl_enum_type};
use chrono::DateTime;
use chrono::Utc;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::bfd_session;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    BfdModeEnum:

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialEq,
        Serialize,
        Deserialize
    )]
    pub enum BfdMode;

    SingleHop => b"single_hop"
    MultiHop => b"multi_hop"
);

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = bfd_session)]
pub struct BfdSession {
    pub id: Uuid,
    pub local: Option<IpNetwork>,
    pub remote: IpNetwork,
    pub detection_threshold: SqlU32,
    pub required_rx: SqlU32,
    pub switch: String,
    pub mode: BfdMode,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl From<sled_agent_types::early_networking::BfdMode> for BfdMode {
    fn from(value: sled_agent_types::early_networking::BfdMode) -> Self {
        match value {
            sled_agent_types::early_networking::BfdMode::SingleHop => {
                Self::SingleHop
            }
            sled_agent_types::early_networking::BfdMode::MultiHop => {
                Self::MultiHop
            }
        }
    }
}

impl From<BfdMode> for sled_agent_types::early_networking::BfdMode {
    fn from(value: BfdMode) -> Self {
        match value {
            BfdMode::SingleHop => Self::SingleHop,
            BfdMode::MultiHop => Self::MultiHop,
        }
    }
}
