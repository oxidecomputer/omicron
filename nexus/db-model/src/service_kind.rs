// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::internal_api;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "service_kind"))]
    pub struct ServiceKindEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = ServiceKindEnum)]
    pub enum ServiceKind;

    // Enum values
    InternalDNS => b"internal_dns"
    Nexus => b"nexus"
    Oximeter => b"oximeter"
    Dendrite => b"dendrite"
    Tfport => b"tfport"
);

impl From<internal_api::params::ServiceKind> for ServiceKind {
    fn from(k: internal_api::params::ServiceKind) -> Self {
        match k {
            internal_api::params::ServiceKind::InternalDNS => {
                ServiceKind::InternalDNS
            }
            internal_api::params::ServiceKind::Nexus => ServiceKind::Nexus,
            internal_api::params::ServiceKind::Oximeter => {
                ServiceKind::Oximeter
            }
            internal_api::params::ServiceKind::Dendrite => {
                ServiceKind::Dendrite
            }
            internal_api::params::ServiceKind::Tfport => ServiceKind::Tfport,
        }
    }
}
