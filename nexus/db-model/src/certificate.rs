// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use db_macros::Resource;
use nexus_db_schema::schema::certificate;
use nexus_types::external_api::certificate as certificate_types;
use nexus_types::identity::Resource;
use omicron_certificates::CertificateValidator;
use omicron_certificates::TlsCertificate;
use omicron_common::api::external::Error;
use uuid::Uuid;

use crate::DbTypedUuid;
pub use omicron_certificates::CertificateError;
use omicron_uuid_kinds::SiloKind;
use omicron_uuid_kinds::SiloUuid;

/// Representation of x509 certificates used by services.
#[derive(Queryable, Insertable, Clone, Selectable, Resource)]
#[diesel(table_name = certificate)]
pub struct Certificate {
    #[diesel(embed)]
    identity: CertificateIdentity,

    pub silo_id: DbTypedUuid<SiloKind>,
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
    pub fn silo_id(&self) -> SiloUuid {
        self.silo_id.into()
    }

    pub fn new(
        silo_id: SiloUuid,
        id: Uuid,
        service: ServiceKind,
        params: certificate_types::CertificateCreate,
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
        silo_id: SiloUuid,
        id: Uuid,
        service: ServiceKind,
        params: certificate_types::CertificateCreate,
    ) -> Self {
        Self {
            identity: CertificateIdentity::new(id, params.identity),
            silo_id: silo_id.into(),
            service,
            cert: params.cert.into_bytes(),
            key: params.key.into_bytes(),
        }
    }
}

impl TryFrom<Certificate> for certificate_types::Certificate {
    type Error = Error;
    fn try_from(cert: Certificate) -> Result<Self, Error> {
        Ok(Self {
            identity: cert.identity(),
            service: cert.service.try_into()?,
            // This is expected to succeed in normal circumstances. Certificates are stored in the
            // database with PEM encoding which are essentially bundles of Base64 encoded text.
            // The only cases in which this conversion should fail is when our internal database
            // representation of the certificate is invalid.
            cert: String::from_utf8(cert.cert).map_err(|_| {
                Error::internal_error("Certificate is not valid UTF-8")
            })?,
        })
    }
}

impl TryFrom<Certificate> for TlsCertificate {
    type Error = anyhow::Error;

    fn try_from(cert: Certificate) -> Result<Self, anyhow::Error> {
        let raw = omicron_certificates::RawCertKey {
            certs: &cert.cert,
            private_key: &cert.key,
        };
        Self::new(cert.id(), raw)
    }
}
