// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parsing and validation of the TLS certificates Nexus serves on its external
//! endpoints.
//!
//! Having an entire crate for this one type may seem somewhat silly, and,
//! indeed, this crate exists entirely for build-optimization reasons. In order
//! to ensure that the Nexus `external_endpoints` task (in the top-level
//! `omicron-nexus` binary) and fault-management input loading (in
//! `nexus-fm-preparation`) use the same parsing and validation logic, the
//! [`TlsCertificate`] type must be defined someplace that
//! `nexus-fm-preparation` can depend on. Unfortunately, neither of the obvious
//! crates in which to put this type (`omicron-certificates` or `nexus-types`)
//! make sense as a place to put this type. Constructing a `TlsCertificate`
//! requires a dependency on `rustls`, which in turn will compile cryptography
//! libraries, and is therefore a somewhat expensive dependency. Similarly, the
//! code for converting a database [`nexus_db_model::Certificate`] record into a
//! [`TlsCertificate`] requires a dependency on `nexus-db-model` (naturally!)
//! which neither `nexus-types` nor `omicron-certificates` may depend on.

use anyhow::Context;
use anyhow::anyhow;
use nexus_types::identity::Resource;
use omicron_certificates::CertificateValidity;
use openssl::pkey::PKey;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::pem::PemObject;
use rustls::sign::CertifiedKey;
use serde::Serialize;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/// A parsed, validated TLS certificate ready to use with an external TLS server
///
/// Constructing one of these (via `TryFrom<Certificate>`) is the acceptance
/// check for whether Nexus can serve a stored certificate at all. Anything
/// else that needs to reason about the certificates Nexus actually presents,
/// like the fault management certificate diagnosis engine, must go through the
/// same conversion so that it sees the same set of certificates.
#[derive(Serialize)]
#[serde(transparent)]
pub struct TlsCertificate {
    /// This is what we need to provide to the TLS stack when we decide to use
    /// this certificate for an incoming TLS connection
    // NOTE: It's important that we do not serialize the private key!
    #[serde(skip)]
    certified_key: Arc<CertifiedKey>,

    /// ID of the `certificate` row this was built from
    #[serde(skip)]
    id: Uuid,

    /// Validity window of the leaf certificate
    #[serde(skip)]
    validity: CertificateValidity,

    /// certificate digest (historically sometimes called a "fingerprint")
    // This is the only field that appears in the serialized output or debug
    // output.
    digest: String,
}

impl fmt::Debug for TlsCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // It's important that only the digest appear in the debug output.  We
        // definitely don't want to leak the private key this way.  Really,
        // we don't want even the public parts adding noise to debug output.
        f.debug_struct("TlsCertificate").field("digest", &self.digest).finish()
    }
}

impl Eq for TlsCertificate {}
impl PartialEq for TlsCertificate {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl TlsCertificate {
    /// Returns the certificate chain and signing key to hand to the TLS stack
    /// when presenting this certificate.
    ///
    /// The returned value contains the private key; it must not be logged or
    /// serialized.
    pub fn certified_key(&self) -> &Arc<CertifiedKey> {
        &self.certified_key
    }

    /// Returns the ID of the `certificate` database row this was built from
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the validity window of the leaf certificate
    pub fn validity(&self) -> CertificateValidity {
        self.validity
    }

    /// Returns the hex-encoded SHA-256 digest of the leaf certificate
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Attempts to construct a [`TlsCertificate`] from the given raw
    /// (unvalidated) certificate bytes, with the provided UUID.
    pub fn new(id: Uuid, raw_cert: RawCertKey) -> anyhow::Result<Self> {
        // Parse and validate what we've got.
        let certs_pem = openssl::x509::X509::stack_from_pem(raw_cert.certs)
            .context("parsing PEM stack")?;
        let private_key = PKey::private_key_from_pem(raw_cert.private_key)
            .context("parsing private key PEM")?;

        // Assemble a rustls CertifiedKey with both the certificate and the key.
        let certified_key = {
            let rustls_private_key =
                PrivateKeyDer::from_pem_slice(raw_cert.private_key)
                    .context("parsing private key PEM for rustls")?;
            let rustls_signing_key =
                rustls::crypto::aws_lc_rs::sign::any_supported_type(
                    &rustls_private_key,
                )
                .context("parsing DER private key")?;
            let rustls_certs = certs_pem
                .iter()
                .map(|x509| {
                    x509.to_der()
                        .context("serializing cert to DER")
                        .map(rustls::pki_types::CertificateDer::from)
                })
                .collect::<Result<_, _>>()?;
            Arc::new(CertifiedKey::new(rustls_certs, rustls_signing_key))
        };

        let end_cert = certs_pem
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no certificates in PEM stack"))?;
        anyhow::ensure!(
            end_cert
                .public_key()
                .context("certificate publickey")?
                .public_eq(&private_key),
            "certificate public key does not match stored private key"
        );

        // Compute a digest (fingerprint) that we can use for debugging.
        let digest = {
            let digest_bytes = end_cert
                .digest(openssl::hash::MessageDigest::sha256())
                .context("computing fingerprint")?;
            hex::encode(&digest_bytes)
        };

        let validity = omicron_certificates::validity(&end_cert)
            .context("reading leaf certificate validity")?;

        Ok(TlsCertificate { id, certified_key, digest, validity })
    }
}

/// A raw, un-validated sequence of bytes which may or may not be a valid TLS
/// key pair. This is used only for constructing a [`TlsCertificate`], so that
/// the certificate and the private key, both of which are just byte slices, are
/// passed as named parameters and are harder to mix up.
#[derive(Copy, Clone)]
pub struct RawCertKey<'a> {
    pub certs: &'a [u8],
    pub private_key: &'a [u8],
}

impl TryFrom<nexus_db_model::Certificate> for TlsCertificate {
    type Error = anyhow::Error;

    fn try_from(
        db_cert: nexus_db_model::Certificate,
    ) -> Result<TlsCertificate, anyhow::Error> {
        TlsCertificate::new(
            db_cert.id(),
            RawCertKey { certs: &db_cert.cert, private_key: &db_cert.key },
        )
    }
}
