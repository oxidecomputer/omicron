// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms for interacting with the sled's RoT.

use dropshot::HttpError;
use oxide_rot::{OxideRot, OxideRotError, ipcc, mock};
use sled_agent_types::rot as SaRotTypes;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use sprockets_tls::keys::AttestConfig;
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard, Semaphore, SemaphorePermit};
use x509_cert::der::{EncodePem, pem::LineEnding};

pub struct MeasurementLog(oxide_rot::Log);

impl From<oxide_rot::Log> for MeasurementLog {
    fn from(log: oxide_rot::Log) -> Self {
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
                oxide_rot::Measurement::Sha3_256(d) => {
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

pub struct QualifyingData(oxide_rot::QualifyingData);

impl From<SaRotTypes::Nonce> for QualifyingData {
    fn from(nonce: SaRotTypes::Nonce) -> Self {
        match nonce {
            SaRotTypes::Nonce::N32(n32) => {
                QualifyingData(oxide_rot::QualifyingData::Q32(n32.into()))
            }
        }
    }
}

pub struct Attestation(oxide_rot::Attestation);

impl From<oxide_rot::Attestation> for Attestation {
    fn from(att: oxide_rot::Attestation) -> Self {
        Attestation(att)
    }
}

impl From<Attestation> for SaRotTypes::Attestation {
    fn from(att: Attestation) -> Self {
        match att.0 {
            oxide_rot::Attestation::Ed25519(sig) => {
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
    AttestMock(#[from] mock::OxideRotMockError),

    #[error("attestation request failed: {0}")]
    Attest(#[from] OxideRotError),

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
    attest: Mutex<Box<dyn OxideRot + Send + Sync>>,
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

        let attest: Box<dyn OxideRot + Send + Sync> = match attest_config {
            AttestConfig::Ipcc => Box::new(ipcc::OxideRotIpcc::new()),
            AttestConfig::Local {
                priv_key,
                cert_chain,
                log,
                test_corpus: _,
            } => Box::new(mock::OxideRotMock::load(cert_chain, log, priv_key)?),
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
        Ok(CertificateChain::try_from(chain).map_err(OxideRotError::from)?)
    }

    pub async fn attest(
        &self,
        qdata: QualifyingData,
    ) -> Result<Attestation, RotError> {
        let guard = self.acquire().await?;
        debug!(self.log, "getting attestation");
        Ok(guard.attest.attest(&qdata.0).await?.into())
    }
}

/// `AttestGuard` represents exclusive access to the attestor
struct AttestGuard<'a> {
    _permit: SemaphorePermit<'a>,
    attest: MutexGuard<'a, Box<dyn OxideRot + Send + Sync>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use attest_mock::MockData;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use dice_verifier::oxide_rot::verify_attestation;
    use omicron_test_utils::dev::test_setup_log;
    use sprockets_tls_test_utils::{
        OutputFileExistsBehavior, alias_prefix, certlist_path, private_key_path,
    };
    use x509_cert::{Certificate, der::DecodePem};

    // An arbitrary digest for the single measurement in the mock log.
    const DIGEST: &str =
        "be4df4e085175f3de0c8ac4837e1c2c9a34e8983209dac6b549e94154f7cdd9c";

    /// Generate an `AttestConfig from `sprockets_tls::keys` w/ artifacts
    /// required to mock the Oxide RoT
    fn gen_mock_config(dir: &Utf8Path) -> AttestConfig {
        // tests in this module need a single cert chain to represent the
        // attestation signer
        const LEAF_COUNT: usize = 1;

        let behavior = OutputFileExistsBehavior::Overwrite;

        let doc = sprockets_tls_test_utils::generate_config(LEAF_COUNT);
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

        // sprockets test utils / pki-playground append a positive integer to
        // the file name of the generated leaf. We've only created a single
        // leaf & cert chain so this must be `1`
        let alias = alias_prefix(LEAF_COUNT);

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
        let cfg = gen_mock_config(tmp.path());

        // Get artifacts required to verify the attestation
        let attestor = RotAttestor::new(&logctx.log, &cfg).unwrap();

        let log = attestor.get_measurement_log().await.unwrap();
        let chain = attestor.get_certificate_chain().await.unwrap();
        let qdata: QualifyingData = SaRotTypes::Nonce::N32([7u8; 32]).into();
        let attestation =
            attestor.attest(QualifyingData(qdata.0)).await.unwrap();

        // The first cert in the chain is the leaf. This cert holds the public
        // part of the key used to sign the attestation.
        let alias = Certificate::from_pem(&chain.0[0]).unwrap();
        verify_attestation(&alias, &attestation.0, &log.0, &qdata.0)
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
        let cfg = gen_mock_config(tmp.path());
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
        let qdata: QualifyingData = SaRotTypes::Nonce::N32([7u8; 32]).into();
        assert!(matches!(attestor.attest(qdata).await, Err(RotError::Busy)));

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
