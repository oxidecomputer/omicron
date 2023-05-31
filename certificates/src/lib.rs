// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for validating X509 certificates, used by both nexus and wicketd.

use omicron_common::api::external::Error;
use openssl::asn1::Asn1Time;
use openssl::pkey::PKey;
use openssl::x509::X509;

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

pub struct CertificateValidator {
    validate_expiration: bool,
}

impl Default for CertificateValidator {
    fn default() -> Self {
        Self { validate_expiration: true }
    }
}

impl CertificateValidator {
    /// Disable validation of certificate expiration dates.
    ///
    /// This exists to support basic certificate validation even before time is
    /// available (e.g., before the rack has been initialized and NTP has
    /// started).
    pub fn danger_disable_expiration_validation(&mut self) {
        self.validate_expiration = false;
    }

    /// Validate that we can parse the cert chain, that the key matches, and
    /// that the certs in the chain are not expired (unless we have disabled
    /// expiration validation).
    //
    // TODO-completeness: Can we take an optional hostname and validate that
    // this cert will work for it? (This might be a paramter to this method, or
    // could be an additional property on `Self`.)
    pub fn validate(
        &self,
        certs: &[u8],
        key: &[u8],
    ) -> Result<(), CertificateError> {
        // Checks on the certs themselves.
        let mut certs = X509::stack_from_pem(certs)
            .map_err(CertificateError::BadCertificate)?;
        if certs.is_empty() {
            return Err(CertificateError::CertificateEmpty);
        }

        if self.validate_expiration {
            let now = Asn1Time::days_from_now(0)
                .map_err(CertificateError::Unexpected)?;
            for cert in &certs {
                if cert.not_after() < now {
                    return Err(CertificateError::CertificateExpired);
                }
            }
        }

        // Extract the first certificate in the chain (the leaf certificate)
        // to use with verifying the private key.
        let cert = certs.swap_remove(0);

        // Checks on the private key.
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
}
