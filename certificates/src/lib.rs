// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for validating X509 certificates, used by both nexus and wicketd.

use omicron_common::api::external::Error;
use openssl::asn1::Asn1Time;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::x509::X509;
use trust_dns_proto::rr::Name as DnsName;

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

    #[error("Failed to convert common name to UTF8")]
    NonUtf8CommonName(#[source] openssl::error::ErrorStack),

    #[error("Certificate subject does not contain a common name")]
    NoCommonName,

    #[error("Certificate contains no subject alternate name dNSName entries and multiple common names")]
    NoDnsNameSansMultipleCommonNames,

    #[error("Certificate contains invalid DNS name")]
    InvalidDnsName(#[source] trust_dns_proto::error::ProtoError),

    #[error("Certificate common name is not a valid DNS name")]
    InvalidCommonNameAsDnsName(#[source] trust_dns_proto::error::ProtoError),

    #[error("Certificate does not match hostname {0:?}")]
    NoDnsNameMatchingHostname(String),

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
            | NonUtf8CommonName(_)
            | Mismatch
            | NoCommonName
            | NoDnsNameSansMultipleCommonNames
            | InvalidDnsName(_)
            | InvalidCommonNameAsDnsName(_)
            | NoDnsNameMatchingHostname(_)
            | UnsupportedPurpose => Error::InvalidValue {
                label: String::from("certificate"),
                message: format!("{error:#}"),
            },
            BadPrivateKey(_) => Error::InvalidValue {
                label: String::from("private-key"),
                message: format!("{error:#}"),
            },
            Unexpected(_) => {
                Error::InternalError { internal_message: format!("{error:#}") }
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
            let dns_names = cert_dns_names(&cert)?;
            let hostname = DnsName::from_utf8(hostname)
                .map_err(CertificateError::InvalidDnsName)?;
            if !dns_names
                .iter()
                .any(|dns_name| dns_name_matches_hostname(dns_name, &hostname))
            {
                return Err(CertificateError::NoDnsNameMatchingHostname(
                    hostname.to_string(),
                ));
            }
        }

        // We want to check whether this cert is suitable for use by a web
        // server, which requires checking the x509 extended attributes.
        // Unfortunately, the openssl crate does not expose these
        // (https://github.com/sfackler/rust-openssl/issues/373), so we'll do
        // something pretty gross: convert this cert to DER, then re-parse it
        // via the pure Rust x509-parser crate, which does. We want to err on
        // the side of "allow the cert" here; we're using a non-openssl crate
        // and manually checking some SSL extensions, so we will only fail if we
        // _know_ the cert is not valid.
        if let Ok(cert_der) = cert.to_der() {
            validate_cert_der_extended_key_usage(&cert_der)?;
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

// As noted above, this helper uses a different x509 parsing crate. We don't
// want to fail spuriously here (e.g., if this function fails to parse, what
// does that mean? we've already successfully parsed the cert via openssl), so
// this function only fails if we successfully parse the extensions we want
// _and_ they fail to satisfy the requirements from
// https://man.openbsd.org/X509_check_purpose.3#X509_PURPOSE_SSL_SERVER.
fn validate_cert_der_extended_key_usage(
    der: &[u8],
) -> Result<(), CertificateError> {
    use x509_parser::certificate::X509CertificateParser;
    use x509_parser::nom::Parser;

    let mut parser = X509CertificateParser::new();
    let Ok((_remaining, cert)) = parser.parse(der) else {
        return Ok(());
    };

    if let Ok(Some(key_usage)) = cert.key_usage() {
        dbg!(&key_usage);
        // Per
        // https://man.openbsd.org/X509_check_purpose.3#X509_PURPOSE_SSL_SERVER,
        // if we have the Key Usage extension, we must have at least one of the
        // digitalSignature / keyEncipherment bits set.
        if !key_usage.value.digital_signature()
            && !key_usage.value.key_encipherment()
        {
            return Err(CertificateError::UnsupportedPurpose);
        }
    }

    if let Ok(Some(ext_key_usage)) = cert.extended_key_usage() {
        dbg!(&ext_key_usage);
        // Per
        // https://man.openbsd.org/X509_check_purpose.3#X509_PURPOSE_SSL_SERVER,
        // if we have the Extended Key Usage extension, we must have either the
        // server auth bit or one of a couple proprietary (Netscape/Microsoft)
        // bits; we'll ignore the proprietary bits and require server auth.
        //
        // TODO-correctness: Is it right to also accept "any" here?
        if !ext_key_usage.value.any && !ext_key_usage.value.server_auth {
            return Err(CertificateError::UnsupportedPurpose);
        }
    }

    // We either don't have Key Usage / Extended Key Usage, or we do but they're
    // satisfactory.
    Ok(())
}

fn cert_dns_names(cert: &X509) -> Result<Vec<DnsName>, CertificateError> {
    let mut dns_names = Vec::new();

    // Per RFC 2818 § 3.1:
    //
    // > If a subjectAltName extension of type dNSName is present, that MUST
    // > be used as the identity. Otherwise, the (most specific) Common Name
    // > field in the Subject field of the certificate MUST be used.
    //
    // we first check for any dnsname entries in `cert.subject_alt_names()`, and
    // failing that we use the common name from the subject field.

    if let Some(sans) = cert.subject_alt_names() {
        for san in sans {
            if let Some(dnsname) = san.dnsname() {
                dns_names.push(
                    DnsName::from_utf8(dnsname)
                        .map_err(CertificateError::InvalidDnsName)?,
                );
            }
        }
    }

    // If we found at least one dNSName in the SANs; we're done.
    if !dns_names.is_empty() {
        return Ok(dns_names);
    }

    let sn = cert.subject_name();

    for cn in sn.entries_by_nid(Nid::COMMONNAME) {
        let cn =
            cn.data().as_utf8().map_err(CertificateError::NonUtf8CommonName)?;
        dns_names.push(
            DnsName::from_utf8(cn)
                .map_err(CertificateError::InvalidCommonNameAsDnsName)?,
        );
    }

    // Per the RFC, we should now choose the "most specific" common name, but
    //
    // (a) it doesn't say how to do that, and
    // (b) we don't really expect to get here (we expect to find SANs)
    //
    // so we'll bail out if we found multiple common names. If we only found
    // one, picking the most specific is easy!
    //
    // The crate we use for generating test cert parameters (rcgen) does not
    // even support multiple common name entries.
    match dns_names.len() {
        0 => Err(CertificateError::NoCommonName),
        1 => Ok(dns_names),
        _ => Err(CertificateError::NoDnsNameSansMultipleCommonNames),
    }
}

// Returns `Ok(true)` if `dns_name` (from a cert) matches `hostname`, including
// handling a leading `*` wildcard:
//
// * If `hostname` begins with a wildcard, `dns_name` must be an exact match
//   (including a leading wildcard).
// * If neither `hostname` nor `dns_name` begin with a wildcard, they must match
//   exactly.
// * If `hostname` does not begin with a wildcard but `dns_name` does, the first
//   portion of each will be removed, and the remainders must match exactly.
fn dns_name_matches_hostname(dns_name: &DnsName, hostname: &DnsName) -> bool {
    // If `hostname` starts with a wildcard _or_ `dns_name` does not, the only
    // way for them to match is if they match exactly. (Note that `DnsName`
    // provides _case insensitive_ equality, so "match exactly" means "match
    // exactly but case insentively".)
    if hostname.is_wildcard() || !dns_name.is_wildcard() {
        hostname == dns_name
    } else {
        // dns_name has a leading wildcard but hostname does not: trim the
        // wildcard off of dns_name and the leading part of hostname, and then
        // compare.
        dns_name.base_name() == hostname.base_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::certificates::CertificateChain;
    use rcgen::CertificateParams;
    use rcgen::DistinguishedName;
    use rcgen::DnType;
    use rcgen::DnValue;
    use rcgen::ExtendedKeyUsagePurpose;
    use rcgen::KeyUsagePurpose;
    use rcgen::SanType;

    #[test]
    fn test_cert_dns_name_matches_hostname() {
        // Expected-successful matches
        for (dns_name, hostname) in &[
            ("oxide.computer", "oxide.computer"),
            ("*.oxide.computer", "*.oxide.computer"),
            ("*.oxide.computer", "foo.oxide.computer"),
        ] {
            assert!(
                dns_name_matches_hostname(
                    &DnsName::from_utf8(dns_name).unwrap(),
                    &DnsName::from_utf8(hostname).unwrap(),
                ),
                "{dns_name} failed to match {hostname}"
            );
        }

        // Expected-unsuccessful matches
        for (dns_name, hostname) in &[
            ("oxide.computer", "foo.oxide.computer"),
            ("oxide.computer", "*.oxide.computer"),
            ("*.oxide.computer", "foo.bar.oxide.computer"),
        ] {
            assert!(
                !dns_name_matches_hostname(
                    &DnsName::from_utf8(dns_name).unwrap(),
                    &DnsName::from_utf8(hostname).unwrap(),
                ),
                "{dns_name} failed to match {hostname}"
            );
        }
    }

    #[test]
    fn test_cert_dns_names() {
        fn leaf_cert_with_params(params: CertificateParams) -> X509 {
            let cert_chain = CertificateChain::with_params(params);
            let mut certs =
                X509::stack_from_pem(cert_chain.cert_chain_as_pem().as_bytes())
                    .unwrap();
            certs.swap_remove(0)
        }

        // "Normal" cert - we have some dNSName subject alt names; those should
        // be returned, and non-DNS SANs and the common name should be ignored.
        let mut params = CertificateParams::new([]);
        params.subject_alt_names = vec![
            SanType::DnsName("foo.oxide.computer".into()),
            SanType::IpAddress("1.1.1.1".parse().unwrap()),
            SanType::DnsName("*.sys.oxide.computer".into()),
            SanType::Rfc822Name("root@oxide.computer".into()),
            SanType::URI("http://oxide.computer".into()),
        ];
        params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(
                DnType::CommonName,
                DnValue::PrintableString("bar.oxide.computer".into()),
            );
            dn
        };
        let cert = leaf_cert_with_params(params);
        let dns_names = cert_dns_names(&cert).unwrap();
        assert_eq!(
            dns_names,
            &[
                "foo.oxide.computer".parse().unwrap(),
                "*.sys.oxide.computer".parse().unwrap()
            ]
        );

        // Cert with no subject alt names; we should get the common name.
        let mut params = CertificateParams::new([]);
        params.distinguished_name = {
            let mut dn = DistinguishedName::new();
            dn.push(
                DnType::CommonName,
                DnValue::PrintableString("bar.oxide.computer".into()),
            );
            dn
        };
        let cert = leaf_cert_with_params(params);
        let dns_names = cert_dns_names(&cert).unwrap();
        assert_eq!(dns_names, &["bar.oxide.computer".parse().unwrap()]);
    }

    #[test]
    fn test_cert_key_usage() {
        const HOST: &str = "foo.oxide.computer";

        let mut validator = CertificateValidator::default();
        validator.danger_disable_expiration_validation();

        let valid_key_usage = vec![
            vec![],
            vec![KeyUsagePurpose::DigitalSignature],
            vec![KeyUsagePurpose::KeyEncipherment],
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyEncipherment,
            ],
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::CrlSign, // unrelated, but fine
            ],
            vec![
                KeyUsagePurpose::KeyEncipherment,
                KeyUsagePurpose::CrlSign, // unrelated, but fine
            ],
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyEncipherment,
                KeyUsagePurpose::CrlSign, // unrelated, but fine
            ],
        ];
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
        for key_usage in &valid_key_usage {
            for ext_key_usage in &valid_ext_key_usage {
                let mut params = CertificateParams::new(vec![HOST.to_string()]);
                params.key_usages = key_usage.clone();
                params.extended_key_usages = ext_key_usage.clone();

                let cert_chain = CertificateChain::with_params(params);
                let certs = cert_chain.cert_chain_as_pem();
                let key = cert_chain.end_cert_private_key_as_pem();
                assert!(
                    validator
                        .validate(certs.as_bytes(), key.as_bytes(), Some(HOST))
                        .is_ok(),
                    "unexpected failure with {key_usage:?}, {ext_key_usage:?}"
                );
            }
        }

        let invalid_key_usage = vec![
            vec![KeyUsagePurpose::CrlSign],
            vec![KeyUsagePurpose::CrlSign, KeyUsagePurpose::KeyCertSign],
        ];

        let invalid_ext_key_usage = vec![
            vec![ExtendedKeyUsagePurpose::ClientAuth],
            vec![ExtendedKeyUsagePurpose::EmailProtection],
            vec![
                ExtendedKeyUsagePurpose::EmailProtection,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        ];

        for key_usage in &invalid_key_usage {
            // `key_usage` is invalid: we should fail with both valid and
            // invalid ext_key_usage.
            for ext_key_usage in
                valid_ext_key_usage.iter().chain(invalid_ext_key_usage.iter())
            {
                let mut params = CertificateParams::new(vec![HOST.to_string()]);
                params.key_usages = key_usage.clone();
                params.extended_key_usages = ext_key_usage.clone();

                let cert_chain = CertificateChain::with_params(params);
                let certs = cert_chain.cert_chain_as_pem();
                let key = cert_chain.end_cert_private_key_as_pem();
                assert!(
                    matches!(
                        validator.validate(
                            certs.as_bytes(),
                            key.as_bytes(),
                            Some(HOST)
                        ),
                        Err(CertificateError::UnsupportedPurpose)
                    ),
                    "unexpected success with {key_usage:?}, {ext_key_usage:?}"
                );
            }
        }

        for ext_key_usage in &invalid_ext_key_usage {
            // `ext_key_usage` is invalid: we should fail with both valid and
            // invalid key_usage.
            for key_usage in
                valid_key_usage.iter().chain(invalid_key_usage.iter())
            {
                let mut params = CertificateParams::new(vec![HOST.to_string()]);
                params.key_usages = key_usage.clone();
                params.extended_key_usages = ext_key_usage.clone();

                let cert_chain = CertificateChain::with_params(params);
                let certs = cert_chain.cert_chain_as_pem();
                let key = cert_chain.end_cert_private_key_as_pem();
                assert!(
                    matches!(
                        validator.validate(
                            certs.as_bytes(),
                            key.as_bytes(),
                            Some(HOST)
                        ),
                        Err(CertificateError::UnsupportedPurpose)
                    ),
                    "unexpected success with {key_usage:?}, {ext_key_usage:?}"
                );
            }
        }
    }
}
