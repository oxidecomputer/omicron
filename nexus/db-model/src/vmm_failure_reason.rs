// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::internal::nexus::VmmFailureReason as ApiFailureReason;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone)]
    #[diesel(postgres_type(name = "vmm_failure_reason", schema = "public"))]
    pub struct VmmFailureReasonEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VmmFailureReasonEnum)]
    pub enum VmmFailureReason;

    SledExpunged => b"sled_expunged"
);

impl VmmFailureReason {
    pub fn label(&self) -> &'static str {
        match self {
            Self::SledExpunged => "sled_expunged",
        }
    }
}

impl fmt::Display for VmmFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl From<ApiFailureReason> for VmmFailureReason {
    fn from(reason: ApiFailureReason) -> Self {
        match reason {
            ApiFailureReason::SledExpunged => Self::SledExpunged,
        }
    }
}

impl From<VmmFailureReason> for ApiFailureReason {
    fn from(reason: VmmFailureReason) -> Self {
        match reason {
            VmmFailureReason::SledExpunged => Self::SledExpunged,
        }
    }
}

impl From<sled_agent_client::types::VmmFailureReason> for VmmFailureReason {
    fn from(reason: sled_agent_client::types::VmmFailureReason) -> Self {
        match reason {
            sled_agent_client::types::VmmFailureReason::SledExpunged => {
                Self::SledExpunged
            }
        }
    }
}

impl From<VmmFailureReason> for sled_agent_client::types::VmmFailureReason {
    fn from(reason: VmmFailureReason) -> Self {
        match reason {
            VmmFailureReason::SledExpunged => Self::SledExpunged,
        }
    }
}

impl diesel::query_builder::QueryId for VmmFailureReasonEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
