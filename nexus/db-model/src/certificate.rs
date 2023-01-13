// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ServiceKind;
use crate::schema::certificate;
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use openssl::pkey::PKey;
use openssl::x509::X509;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    #[error("Failed to parse certificate: {0}")]
    BadCertificate(openssl::error::ErrorStack),

    #[error("Certificate exists, but is empty")]
    CertificateEmpty,

    #[error("Failed to parse private key")]
    BadPrivateKey(openssl::error::ErrorStack),
}

impl From<CertificateError> for Error {
    fn from(error: CertificateError) -> Self {
        use CertificateError::*;
        match error {
            BadCertificate(_) | CertificateEmpty => Error::InvalidValue {
                label: String::from("certificate"),
                message: error.to_string(),
            },
            BadPrivateKey(_) => Error::InvalidValue {
                label: String::from("private-key"),
                message: error.to_string(),
            },
        }
    }
}

fn validate_certs(input: Vec<u8>) -> Result<(), CertificateError> {
    let certs = X509::stack_from_pem(&input.as_slice())
        .map_err(CertificateError::BadCertificate)?;
    if certs.is_empty() {
        return Err(CertificateError::CertificateEmpty);
    }
    Ok(())
}

fn validate_private_key(key: Vec<u8>) -> Result<(), CertificateError> {
    let _ = PKey::private_key_from_pem(&key.as_slice())
        .map_err(CertificateError::BadPrivateKey)?;

    Ok(())
}

/// Representation of x509 certificates used by services.
#[derive(Queryable, Insertable, Clone, Selectable, Resource)]
#[diesel(table_name = certificate)]
pub struct Certificate {
    #[diesel(embed)]
    identity: CertificateIdentity,

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
        id: Uuid,
        service: ServiceKind,
        params: params::CertificateCreate,
    ) -> Result<Self, CertificateError> {
        validate_certs(params.cert.clone())?;
        validate_private_key(params.key.clone())?;

        Ok(Self {
            identity: CertificateIdentity::new(id, params.identity),
            service,
            cert: params.cert,
            key: params.key,
        })
    }
}

impl TryFrom<Certificate> for views::Certificate {
    type Error = Error;
    fn try_from(cert: Certificate) -> Result<Self, Error> {
        Ok(Self {
            identity: cert.identity(),
            service: cert.service.try_into()?,
        })
    }
}
