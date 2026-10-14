// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms for interacting with the sled's RoT.

use dice_verifier::{Attest, AttestError, ipcc, mock};
use dropshot::HttpError;
use sled_agent_types::rot as SaRotTypes;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use sprockets_tls::keys::AttestConfig;
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard, Semaphore, SemaphorePermit};
use x509_cert::der::{EncodePem, pem::LineEnding};

pub struct MeasurementLog(dice_verifier::Log);

impl From<dice_verifier::Log> for MeasurementLog {
    fn from(log: dice_verifier::Log) -> Self {
        MeasurementLog(log)
    }
}

impl From<MeasurementLog> for SaRotTypes::MeasurementLog {
    fn from(log: MeasurementLog) -> Self {
        let measurements = log
            .0
            .iter()
            .copied()
            .map(|m| match m {
                dice_verifier::Measurement::Sha3_256(d) => {
                    SaRotTypes::Measurement::Sha3_256(
                        SaRotTypes::Sha3_256Digest(d.0),
                    )
                }
            })
            .collect();
        SaRotTypes::MeasurementLog(measurements)
    }
}

pub struct CertificateChain(Vec<String>);

impl TryFrom<x509_cert::PkiPath> for CertificateChain {
    type Error = x509_cert::der::Error;

    fn try_from(chain: x509_cert::PkiPath) -> Result<Self, Self::Error> {
        let certs: Result<Vec<_>, _> =
            chain.into_iter().map(|cert| cert.to_pem(LineEnding::LF)).collect();
        Ok(CertificateChain(certs?))
    }
}

impl From<CertificateChain> for SaRotTypes::CertificateChain {
    fn from(chain: CertificateChain) -> Self {
        SaRotTypes::CertificateChain(chain.0)
    }
}

pub struct Nonce(dice_verifier::Nonce);

impl From<SaRotTypes::Nonce> for Nonce {
    fn from(nonce: SaRotTypes::Nonce) -> Self {
        match nonce {
            SaRotTypes::Nonce::N32(n32) => {
                Nonce(dice_verifier::Nonce::N32(n32.into()))
            }
        }
    }
}

pub struct Attestation(dice_verifier::Attestation);

impl From<dice_verifier::Attestation> for Attestation {
    fn from(att: dice_verifier::Attestation) -> Self {
        Attestation(att)
    }
}

impl From<Attestation> for SaRotTypes::Attestation {
    fn from(att: Attestation) -> Self {
        match att.0 {
            dice_verifier::Attestation::Ed25519(sig) => {
                SaRotTypes::Attestation::Ed25519(SaRotTypes::Ed25519Signature(
                    sig.0,
                ))
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum RotError {
    #[error("failed to create mock attestor: {0}")]
    AttestMock(#[from] mock::AttestMockError),

    #[error("attestation request failed: {0}")]
    Attest(#[from] AttestError),

    #[error("RoT attestation queue full")]
    Busy,
}

impl From<RotError> for HttpError {
    fn from(e: RotError) -> Self {
        match e {
            RotError::Busy => HttpError::for_unavail(
                None,
                InlineErrorChain::new(&e).to_string(),
            ),
            _ => HttpError::for_internal_error(
                InlineErrorChain::new(&e).to_string(),
            ),
        }
    }
}

/// Maximum number of outstanding RoT requests.
const QUEUE_SIZE: usize = 256;

/// `RotAttestor` mediates access to the Oxide RoT on the sled
///
/// Attestation requests (on a real sled) are proxied through a few layers:
/// Sled Agent ([`RotAttestor`]) <--(via IPCC)--> SP <-> RoT. The RoT services
/// a single attestation request at a time, so we serialize requests here
/// rather than having concurrent callers contend for IPCC. At most `QUEUE_SIZE`
/// requests may be outstanding at once. Requests made beyond this limit fail
/// immediately with [`RotError::Busy`].
pub struct RotAttestor {
    log: Logger,
    queue: Semaphore,
    attest: Mutex<Box<dyn Attest + Send + Sync>>,
}

impl RotAttestor {
    pub fn new(
        log: &Logger,
        attest_config: &AttestConfig,
    ) -> Result<Self, RotError> {
        let log = log.new(o!(
            "component" => "RotAttestor",
            "interface" => match attest_config {
                AttestConfig::Ipcc => "ipcc",
                AttestConfig::Local { .. } => "mock",
            }
        ));

        let attest: Box<dyn Attest + Send + Sync> = match attest_config {
            AttestConfig::Ipcc => Box::new(ipcc::AttestIpcc::new()),
            AttestConfig::Local {
                priv_key,
                cert_chain,
                log,
                test_corpus: _,
            } => Box::new(mock::AttestMock::load(cert_chain, log, priv_key)?),
        };

        Ok(RotAttestor {
            log,
            queue: Semaphore::new(QUEUE_SIZE),
            attest: Mutex::new(attest),
        })
    }

    /// Wait for exclusive access to the attestor, reserving a queue slot for
    /// the duration of the request. Fails with [`RotError::Busy`] if the
    /// queue is full.
    async fn acquire(&self) -> Result<AttestGuard<'_>, RotError> {
        let _permit = self.queue.try_acquire().map_err(|_| RotError::Busy)?;
        let attest = self.attest.lock().await;
        Ok(AttestGuard { _permit, attest })
    }

    pub async fn get_measurement_log(
        &self,
    ) -> Result<MeasurementLog, RotError> {
        let guard = self.acquire().await?;
        debug!(self.log, "getting measurement log");
        Ok(guard.attest.get_measurement_log().await?.into())
    }

    pub async fn get_certificate_chain(
        &self,
    ) -> Result<CertificateChain, RotError> {
        let guard = self.acquire().await?;
        debug!(self.log, "getting certificate chain");
        let chain = guard.attest.get_certificates().await?;
        Ok(CertificateChain::try_from(chain).map_err(AttestError::from)?)
    }

    pub async fn attest(&self, nonce: Nonce) -> Result<Attestation, RotError> {
        let guard = self.acquire().await?;
        debug!(self.log, "getting attestation");
        Ok(guard.attest.attest(&nonce.0).await?.into())
    }
}

/// `AttestGuard` represents exclusive access to the attestor
struct AttestGuard<'a> {
    _permit: SemaphorePermit<'a>,
    attest: MutexGuard<'a, Box<dyn Attest + Send + Sync>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use attest_mock::MockData;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use dice_verifier::verify_attestation;
    use omicron_test_utils::dev::test_setup_log;
    use sprockets_tls_test_utils::{
        OutputFileExistsBehavior, alias_prefix, certlist_path, private_key_path,
    };
    use x509_cert::{Certificate, der::DecodePem};

    // An arbitrary digest for the single measurement in the mock log.
    const DIGEST: &str =
        "be4df4e085175f3de0c8ac4837e1c2c9a34e8983209dac6b549e94154f7cdd9c";

    /// Write the keys, certs, and measurement log for the mock RoT to `dir`
    fn write_mock_data(dir: &Utf8Path) {
        let behavior = OutputFileExistsBehavior::Overwrite;

        let doc = sprockets_tls_test_utils::generate_config(1);
        doc.write_key_pairs(dir.to_owned(), behavior).unwrap();
        doc.write_certificates(dir.to_owned(), behavior).unwrap();
        doc.write_certificate_lists(dir.to_owned(), behavior).unwrap();

        let log_kdl = format!(
            r#"
            measurement {{
                algorithm "sha3-256"
                digest "{DIGEST}"
            }}
            "#
        );

        let log = attest_mock::MockLog::parse("log.kdl", &log_kdl)
            .unwrap()
            .to_bytes()
            .unwrap();
        std::fs::write(dir.join("log.bin"), &log).unwrap();
    }

    /// Write the mock RoT's data to `dir` and return a config that uses it
    fn mock_config(dir: &Utf8Path) -> AttestConfig {
        write_mock_data(dir);

        let alias = alias_prefix(1);
        AttestConfig::Local {
            priv_key: private_key_path(dir.to_owned(), &alias),
            cert_chain: certlist_path(dir.to_owned(), &alias),
            log: dir.join("log.bin"),
            test_corpus: vec![],
        }
    }

    /// Drive "normal" operation of the `RotAttestationTask` through the
    /// `RotAttestationHandle` to get and appraise an attestation
    #[tokio::test]
    async fn rot_attestor_verify_attestation() {
        let logctx = test_setup_log("rot_attestor_mock");
        let tmp = Utf8TempDir::new().unwrap();
        let cfg = mock_config(tmp.path());

        // Get artifacts required to verify the attestation
        let attestor = RotAttestor::new(&logctx.log, &cfg).unwrap();

        let log = attestor.get_measurement_log().await.unwrap();
        let chain = attestor.get_certificate_chain().await.unwrap();
        let nonce: Nonce = SaRotTypes::Nonce::N32([7u8; 32]).into();
        let attestation = attestor.attest(Nonce(nonce.0)).await.unwrap();

        // The first cert in the chain is the leaf. This cert holds the public
        // part of the key used to sign the attestation.
        let alias = Certificate::from_pem(&chain.0[0]).unwrap();
        verify_attestation(&alias, &attestation.0, &log.0, &nonce.0)
            .expect("attestation should verify against the alias cert");

        let log: SaRotTypes::MeasurementLog = log.into();
        assert_eq!(log.0.len(), 1);

        let SaRotTypes::Measurement::Sha3_256(digest) = &log.0[0];
        assert_eq!(hex::encode(digest.0), DIGEST);

        let chain: SaRotTypes::CertificateChain = chain.into();
        assert!(!chain.0.is_empty());
        assert!(
            chain
                .0
                .iter()
                .all(|c| c.starts_with("-----BEGIN CERTIFICATE-----"))
        );

        let SaRotTypes::Attestation::Ed25519(sig) = attestation.into();
        assert_eq!(sig.0.len(), 64);

        logctx.cleanup_successful();
    }

    /// Ensure new requests are rejected when the request queue is full
    #[tokio::test]
    async fn rot_attestor_queue_full() {
        let logctx = test_setup_log("rot_attestor_busy");
        let tmp = Utf8TempDir::new().unwrap();
        let cfg = mock_config(tmp.path());
        let attestor = RotAttestor::new(&logctx.log, &cfg).unwrap();

        // fill request queue
        attestor.queue.try_acquire_many(QUEUE_SIZE as u32).unwrap().forget();

        // Request queue is full, additional requests are rejected
        assert!(matches!(
            attestor.get_measurement_log().await,
            Err(RotError::Busy)
        ));
        assert!(matches!(
            attestor.get_certificate_chain().await,
            Err(RotError::Busy)
        ));
        let nonce: Nonce = SaRotTypes::Nonce::N32([7u8; 32]).into();
        assert!(matches!(attestor.attest(nonce).await, Err(RotError::Busy)));

        // RotError::Busy is reported to clients as 503 / Service Unavailable
        let err: HttpError = RotError::Busy.into();
        assert_eq!(
            err.status_code.as_status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );

        // Once the outstanding requests complete, new ones succeed.
        attestor.queue.add_permits(QUEUE_SIZE);
        assert!(attestor.get_measurement_log().await.is_ok());

        logctx.cleanup_successful();
    }
}
