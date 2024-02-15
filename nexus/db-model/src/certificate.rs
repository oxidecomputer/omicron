// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::schema::certificate;
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_certificates::CertificateValidator;
use omicron_common::api::external::Error;
use uuid::Uuid;

pub use omicron_certificates::CertificateError;

/// Representation of x509 certificates used by services.
#[derive(Queryable, Insertable, Clone, Selectable, Resource)]
#[diesel(table_name = certificate)]
pub struct Certificate {
    #[diesel(embed)]
    identity: CertificateIdentity,

    pub silo_id: Uuid,
    pub service: ServiceKind,

    pub cert: Vec<u8>,
    pub key: Vec<u8>,
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certificate")
            .field("identity", &self.identity)
            .field("service", &self.service)
            .field("cert", &self.cert)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl Certificate {
    pub fn new(
        silo_id: Uuid,
        id: Uuid,
        service: ServiceKind,
        params: params::CertificateCreate,
        possible_dns_names: &[String],
    ) -> Result<Self, CertificateError> {
        let validator = CertificateValidator::default();

        validator.validate(
            params.cert.as_bytes(),
            params.key.as_bytes(),
            possible_dns_names,
        )?;

        Ok(Self::new_unvalidated(silo_id, id, service, params))
    }

    // For testing only: allow creation of certificates that would otherwise not
    // be allowed (e.g., expired)
    pub fn new_unvalidated(
        silo_id: Uuid,
        id: Uuid,
        service: ServiceKind,
        params: params::CertificateCreate,
    ) -> Self {
        Self {
            identity: CertificateIdentity::new(id, params.identity),
            silo_id,
            service,
            cert: params.cert.into_bytes(),
            key: params.key.into_bytes(),
        }
    }
}

impl TryFrom<Certificate> for views::Certificate {
    type Error = Error;
    fn try_from(cert: Certificate) -> Result<Self, Error> {
        Ok(Self {
            identity: cert.identity(),
            service: cert.service.try_into()?,
            cert: String::from_utf8(cert.cert)
                .map_err(|err| {
                    Error::InternalError {
                        internal_message: format!(
                            "Failed to construct string from stored certificate: {}",
                            err
                        )
                    }
                }
            )?,
        })
    }
}
