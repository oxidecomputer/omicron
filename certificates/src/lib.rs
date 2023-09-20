// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for validating X509 certificates, used by both nexus and wicketd.

use display_error_chain::DisplayErrorChain;
use omicron_common::api::external::Error;
use openssl::asn1::Asn1Time;
use openssl::pkey::PKey;
use openssl::x509::X509;
use std::ffi::CString;

mod openssl_ext;

use openssl_ext::X509Ext;

#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    #[error("Failed to parse certificate")]
    BadCertificate(#[source] openssl::error::ErrorStack),

    #[error("Certificate exists, but is empty")]
    CertificateEmpty,

    #[error("Certificate exists, but is expired")]
    CertificateExpired,

    #[error("Failed to parse private key")]
    BadPrivateKey(#[source] openssl::error::ErrorStack),

    #[error("Certificate and private key do not match")]
    Mismatch,

    #[error("Hostname provided for validation is invalid: {0:?}")]
    InvalidValidationHostname(String),

    #[error("Error validating certificate hostname")]
    ErrorValidatingHostname(#[source] openssl::error::ErrorStack),

    #[error("Certificate not valid for {hostname:?}: {cert_description:?}")]
    NoDnsNameMatchingHostname { hostname: String, cert_description: String },

    #[error("Unsupported certificate purpose (not usable for server auth)")]
    UnsupportedPurpose,

    #[error("Unexpected error")]
    Unexpected(#[source] openssl::error::ErrorStack),
}

impl From<CertificateError> for Error {
    fn from(error: CertificateError) -> Self {
        use CertificateError::*;
        match error {
            BadCertificate(_)
            | CertificateEmpty
            | CertificateExpired
            | Mismatch
            | InvalidValidationHostname(_)
            | ErrorValidatingHostname(_)
            | NoDnsNameMatchingHostname { .. }
            | UnsupportedPurpose => Error::InvalidValue {
                label: String::from("certificate"),
                message: DisplayErrorChain::new(&error).to_string(),
            },
            BadPrivateKey(_) => Error::InvalidValue {
                label: String::from("private-key"),
                message: DisplayErrorChain::new(&error).to_string(),
            },
            Unexpected(_) => Error::InternalError {
                internal_message: DisplayErrorChain::new(&error).to_string(),
            },
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
    ///
    /// `certs` is expected to be a certificate chain in PEM format.
    ///
    /// `key` is expected to be the private key for the leaf certificate of
    /// `certs` in PEM format.
    ///
    /// If `hostname` is not `None`, the leaf certificate of `certs` must be
    /// valid for `hostname`, as determined by a dNSName entry in its subject
    /// alternate names or (if there are no dNSName SANs) the cert's common
    /// name.
    pub fn validate(
        &self,
        certs: &[u8],
        key: &[u8],
        hostname: Option<&str>,
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

        if let Some(hostname) = hostname {
            let c_hostname = CString::new(hostname).map_err(|_| {
                CertificateError::InvalidValidationHostname(
                    hostname.to_string(),
                )
            })?;
            if !cert
                .valid_for_hostname(&c_hostname)
                .map_err(CertificateError::ErrorValidatingHostname)?
            {
                let cert_description =
                    cert.hostname_description().unwrap_or_else(|err| {
                        format!(
                            "Error reading cert hostname: {}",
                            DisplayErrorChain::new(&err)
                        )
                    });
                return Err(CertificateError::NoDnsNameMatchingHostname {
                    hostname: hostname.to_string(),
                    cert_description,
                });
            }
        }

        // If the cert has extended key usage bits set at all, require the bit
        // for servers (`XKU_SSL_SERVER` corresponds to `id-kp-serverAuth`,
        // which is "TLS WWW server authentication" from RFC 5280).
        if let Some(extended_key_usage) = cert.extended_key_usage() {
            if extended_key_usage & openssl_sys::XKU_SSL_SERVER == 0 {
                return Err(CertificateError::UnsupportedPurpose);
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::certificates::CertificateChain;
    use rcgen::CertificateParams;
    use rcgen::DistinguishedName;
    use rcgen::DnType;
    use rcgen::ExtendedKeyUsagePurpose;
    use rcgen::SanType;

    fn validate_cert_with_params(
        params: CertificateParams,
        hostname: Option<&str>,
    ) -> Result<(), CertificateError> {
        let cert_chain = CertificateChain::with_params(params);
        CertificateValidator::default().validate(
            cert_chain.cert_chain_as_pem().as_bytes(),
            cert_chain.end_cert_private_key_as_pem().as_bytes(),
            hostname,
        )
    }

    #[test]
    fn test_subject_alternate_names_are_validated() {
        // Expected-successful matches
        for (dns_name, hostname) in &[
            ("oxide.computer", "oxide.computer"),
            ("*.oxide.computer", "*.oxide.computer"),
            ("*.oxide.computer", "foo.oxide.computer"),
        ] {
            let mut params = CertificateParams::new([]);
            params.subject_alt_names =
                vec![SanType::DnsName(dns_name.to_string())];
            match validate_cert_with_params(params, Some(hostname)) {
                Ok(()) => (),
                Err(err) => panic!(
                    "certificate with SAN {dns_name} \
                     failed to validate for hostname {hostname}: {err}"
                ),
            }
        }

        // Expected-unsuccessful matches
        for &(dns_name, server_hostname) in &[
            ("oxide.computer", "foo.oxide.computer"),
            ("oxide.computer", "*.oxide.computer"),
            ("*.oxide.computer", "foo.bar.oxide.computer"),
        ] {
            let mut params = CertificateParams::new([]);
            params.subject_alt_names =
                vec![SanType::DnsName(dns_name.to_string())];
            match validate_cert_with_params(params, Some(server_hostname)) {
                Ok(()) => panic!(
                    "certificate with SAN {dns_name} unexpectedly \
                     passed validation for hostname {server_hostname}"
                ),
                Err(CertificateError::NoDnsNameMatchingHostname {
                    hostname,
                    cert_description,
                }) => {
                    assert_eq!(hostname, server_hostname);
                    assert_eq!(cert_description, format!("SANs: {dns_name}"));
                }
                Err(err) => panic!(
                    "certificate with SAN {dns_name} \
                     validation failed with unexpected error {err}"
                ),
            }
        }
    }

    #[test]
    fn test_common_name_is_validated() {
        // Expected-successful matches
        for &(dns_name, hostname) in &[
            ("oxide.computer", "oxide.computer"),
            ("*.oxide.computer", "*.oxide.computer"),
            ("*.oxide.computer", "foo.oxide.computer"),
        ] {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, dns_name);
            let mut params = CertificateParams::new([]);
            params.distinguished_name = dn;

            match validate_cert_with_params(params, Some(hostname)) {
                Ok(()) => (),
                Err(err) => panic!(
                    "certificate with SAN {dns_name} \
                     failed to validate for hostname {hostname}: {err}"
                ),
            }
        }

        // Expected-unsuccessful matches
        for &(dns_name, server_hostname) in &[
            ("oxide.computer", "foo.oxide.computer"),
            ("oxide.computer", "*.oxide.computer"),
            ("*.oxide.computer", "foo.bar.oxide.computer"),
        ] {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, dns_name);
            let mut params = CertificateParams::new([]);
            params.distinguished_name = dn;

            match validate_cert_with_params(params, Some(server_hostname)) {
                Ok(()) => panic!(
                    "certificate with SAN {dns_name} unexpectedly \
                     passed validation for hostname {server_hostname}"
                ),
                Err(CertificateError::NoDnsNameMatchingHostname {
                    hostname,
                    cert_description,
                }) => {
                    assert_eq!(hostname, server_hostname);
                    assert_eq!(cert_description, format!("CN: {dns_name}"));
                }
                Err(err) => panic!(
                    "certificate with SAN {dns_name} \
                     validation failed with unexpected error {err}"
                ),
            }
        }
    }

    #[test]
    fn common_name_is_ignored_if_subject_alternate_names_exist() {
        // Set a common name that will pass validation, but a SAN that will not.
        // If a SAN exists, the CN should not be used in validation.
        const COMMON_NAME: &str = "*.oxide.computer";
        const SUBJECT_ALT_NAME: &str = "bar.oxide.computer";
        const HOSTNAME: &str = "foo.oxide.computer";

        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, COMMON_NAME);

        let mut params = CertificateParams::new([]);
        params.distinguished_name = dn;

        params.subject_alt_names =
            vec![SanType::DnsName(SUBJECT_ALT_NAME.to_string())];

        match validate_cert_with_params(params, Some(HOSTNAME)) {
            Ok(()) => panic!(
                "certificate unexpectedly passed validation for hostname"
            ),
            Err(CertificateError::NoDnsNameMatchingHostname {
                hostname,
                cert_description,
            }) => {
                assert_eq!(hostname, HOSTNAME);
                assert_eq!(
                    cert_description,
                    format!("SANs: {SUBJECT_ALT_NAME}")
                );
            }
            Err(err) => panic!(
                "certificate validation failed with unexpected error {err}"
            ),
        }
    }

    #[test]
    fn test_cert_extended_key_usage() {
        const HOST: &str = "foo.oxide.computer";

        let valid_ext_key_usage = vec![
            vec![],
            // Restore once https://github.com/est31/rcgen/issues/130 is fixed
            // vec![ExtendedKeyUsagePurpose::Any],
            vec![ExtendedKeyUsagePurpose::ServerAuth],
            vec![
                ExtendedKeyUsagePurpose::Any,
                ExtendedKeyUsagePurpose::ServerAuth,
            ],
            vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        ];

        // Valid certs: either no key usage values, or valid ones.
        for ext_key_usage in &valid_ext_key_usage {
            let mut params = CertificateParams::new(vec![HOST.to_string()]);
            params.extended_key_usages = ext_key_usage.clone();

            assert!(
                validate_cert_with_params(params, Some(HOST)).is_ok(),
                "unexpected failure with {ext_key_usage:?}"
            );
        }

        let invalid_ext_key_usage = vec![
            vec![ExtendedKeyUsagePurpose::ClientAuth],
            vec![ExtendedKeyUsagePurpose::EmailProtection],
            vec![
                ExtendedKeyUsagePurpose::EmailProtection,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        ];

        for ext_key_usage in &invalid_ext_key_usage {
            let mut params = CertificateParams::new(vec![HOST.to_string()]);
            params.extended_key_usages = ext_key_usage.clone();

            assert!(
                matches!(
                    validate_cert_with_params(params, Some(HOST)),
                    Err(CertificateError::UnsupportedPurpose)
                ),
                "unexpected success with {ext_key_usage:?}"
            );
        }
    }
}
