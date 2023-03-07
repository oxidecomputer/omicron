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
use openssl::asn1::Asn1Time;
use openssl::pkey::PKey;
use openssl::x509::X509;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    #[error("Failed to parse certificate: {0}")]
    BadCertificate(openssl::error::ErrorStack),

    #[error("Certificate exists, but is empty")]
    CertificateEmpty,

    #[error("Certificate exists, but is expired")]
    CertificateExpired,

    #[error("Failed to parse private key")]
    BadPrivateKey(openssl::error::ErrorStack),

    #[error("Certificate and private key do not match")]
    Mismatch,

    #[error("Unexpected error: {0}")]
    Unexpected(openssl::error::ErrorStack),
}

impl From<CertificateError> for Error {
    fn from(error: CertificateError) -> Self {
        use CertificateError::*;
        match error {
            BadCertificate(_) | CertificateEmpty | CertificateExpired
            | Mismatch => Error::InvalidValue {
                label: String::from("certificate"),
                message: error.to_string(),
            },
            BadPrivateKey(_) => Error::InvalidValue {
                label: String::from("private-key"),
                message: error.to_string(),
            },
            Unexpected(_) => {
                Error::InternalError { internal_message: error.to_string() }
            }
        }
    }
}

fn validate_certs(input: &[u8]) -> Result<X509, CertificateError> {
    let mut certs = X509::stack_from_pem(input)
        .map_err(CertificateError::BadCertificate)?;
    if certs.is_empty() {
        return Err(CertificateError::CertificateEmpty);
    }
    let now =
        Asn1Time::days_from_now(0).map_err(CertificateError::Unexpected)?;
    for cert in &certs {
        if cert.not_after() < now {
            return Err(CertificateError::CertificateExpired);
        }
    }
    // Return the first certificate in the chain (the leaf certificate)
    // to use with verifying the private key.
    Ok(certs.swap_remove(0))
}

fn validate_private_key(
    cert: X509,
    key: &[u8],
) -> Result<(), CertificateError> {
    let key = PKey::private_key_from_pem(key)
        .map_err(CertificateError::BadPrivateKey)?;

    // Verify the public key corresponding to this private key
    // matches the public key in the certificate.
    if !cert
        .public_key()
        .map_err(CertificateError::BadCertificate)?
        .public_eq(&key)
    {
        return Err(CertificateError::Mismatch);
    }

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
        let cert = validate_certs(&params.cert)?;
        validate_private_key(cert, &params.key)?;

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
