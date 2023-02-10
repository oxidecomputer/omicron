// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use external_api::shared::ServiceUsingCertificate;
use nexus_types::{external_api, internal_api};
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "service_kind"))]
    pub struct ServiceKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = ServiceKindEnum)]
    pub enum ServiceKind;

    // Enum values
    InternalDNS => b"internal_dns"
    Nexus => b"nexus"
    Oximeter => b"oximeter"
    Dendrite => b"dendrite"
    Tfport => b"tfport"
    CruciblePantry => b"crucible_pantry"
);

impl TryFrom<ServiceKind> for ServiceUsingCertificate {
    type Error = omicron_common::api::external::Error;
    fn try_from(k: ServiceKind) -> Result<Self, Self::Error> {
        match k {
            ServiceKind::Nexus => Ok(ServiceUsingCertificate::ExternalApi),
            _ => Err(Self::Error::internal_error("Invalid service type")),
        }
    }
}

impl From<ServiceUsingCertificate> for ServiceKind {
    fn from(k: ServiceUsingCertificate) -> Self {
        match k {
            ServiceUsingCertificate::ExternalApi => Self::Nexus,
        }
    }
}

impl From<internal_api::params::ServiceKind> for ServiceKind {
    fn from(k: internal_api::params::ServiceKind) -> Self {
        match k {
            internal_api::params::ServiceKind::InternalDNS => {
                ServiceKind::InternalDNS
            }
            internal_api::params::ServiceKind::Nexus { .. } => {
                ServiceKind::Nexus
            }
            internal_api::params::ServiceKind::Oximeter => {
                ServiceKind::Oximeter
            }
            internal_api::params::ServiceKind::Dendrite => {
                ServiceKind::Dendrite
            }
            internal_api::params::ServiceKind::Tfport => ServiceKind::Tfport,
            internal_api::params::ServiceKind::CruciblePantry => {
                ServiceKind::CruciblePantry
            }
        }
    }
}
