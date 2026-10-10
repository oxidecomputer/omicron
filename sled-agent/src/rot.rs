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
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    oneshot,
};
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
    #[error("failed to create IPCC attestor: {0}")]
    AttestIpcc(#[from] dice_verifier::ipcc::IpccError),

    #[error("failed to create mock attestor: {0}")]
    AttestMock(#[from] mock::AttestMockError),

    #[error("attestation request failed: {0}")]
    Attest(#[from] AttestError),

    #[error("RoT attestation queue full")]
    Busy,

    #[error("RoT attestation task gone - request cancelled")]
    Shutdown,
}

impl From<TrySendError<AttestationMessage>> for RotError {
    fn from(e: TrySendError<AttestationMessage>) -> Self {
        match e {
            // Given the relatively large queue size, we always attempt to
            // send the attestation messages via `try_send()` instead of
            // potentially blocking forever.
            TrySendError::Full(_) => RotError::Busy,
            TrySendError::Closed(_) => RotError::Shutdown,
        }
    }
}

impl From<RotError> for HttpError {
    fn from(e: RotError) -> Self {
        match e {
            RotError::Busy | RotError::Shutdown => HttpError::for_unavail(
                None,
                InlineErrorChain::new(&e).to_string(),
            ),
            _ => HttpError::for_internal_error(
                InlineErrorChain::new(&e).to_string(),
            ),
        }
    }
}

/// Depth of the request queue for Sled Agent to the RoT.
const QUEUE_SIZE: usize = 256;

type AttestationResponseTx<T> = oneshot::Sender<Result<T, AttestError>>;

enum AttestationMessage {
    GetMeasurementLog(AttestationResponseTx<MeasurementLog>),
    GetCertificateChain(AttestationResponseTx<CertificateChain>),
    Attest(Nonce, AttestationResponseTx<Attestation>),
}

#[derive(Debug)]
pub struct RotAttestationHandle {
    tx: mpsc::Sender<AttestationMessage>,
}

impl RotAttestationHandle {
    pub async fn get_measurement_log(
        &self,
    ) -> Result<MeasurementLog, RotError> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(AttestationMessage::GetMeasurementLog(tx))?;
        Ok(rx.await.map_err(|_| RotError::Shutdown)??)
    }

    pub async fn get_certificate_chain(
        &self,
    ) -> Result<CertificateChain, RotError> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(AttestationMessage::GetCertificateChain(tx))?;
        Ok(rx.await.map_err(|_| RotError::Shutdown)??)
    }

    pub async fn attest(&self, nonce: Nonce) -> Result<Attestation, RotError> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(AttestationMessage::Attest(nonce, tx))?;
        Ok(rx.await.map_err(|_| RotError::Shutdown)??)
    }
}

/// Mediates access to the Oxide RoT on the current sled.
///
/// The semantics are:
///  1. Attestation requests (on a real sled) are proxied through a few layers:
///     Sled Agent ([`RotAttestationTask`]) <--(via IPCC)--> SP <-> RoT.
///     Those IPCC calls are made via an IOCTL (via [`ipcc::AttestIpcc`]) and
///     thus should be treated as "blocking I/O".
///  2. There is only a single outstanding attestation request at any point.
///  3. (1) + (2) leads us to service requests (via [`RotAttestationHandle`])
///     from a single queue which is drained by a task started via
///     `spawn_blocking()` -- [`RotAttestationTask::run`].
pub struct RotAttestationTask {
    log: Logger,
    rx: mpsc::Receiver<AttestationMessage>,
    attest: Box<dyn Attest + Send>,
}

impl RotAttestationTask {
    /// Creates and launches the RoT Attestation task with the given config.
    /// On success, a handle is returned to use for submitting attestation
    /// requests.
    pub fn launch(
        log: &Logger,
        attest_config: &AttestConfig,
    ) -> Result<RotAttestationHandle, RotError> {
        let (task, handle) = Self::new(log, attest_config)?;
        tokio::task::spawn_blocking(move || task.run());
        Ok(handle)
    }

    fn new(
        log: &Logger,
        attest_config: &AttestConfig,
    ) -> Result<(RotAttestationTask, RotAttestationHandle), RotError> {
        let log = log.new(o!(
            "component" => "RotAttestationTask",
            "interface" => match attest_config {
                AttestConfig::Ipcc => "ipcc",
                AttestConfig::Local { .. } => "mock",
            }
        ));

        let attest: Box<dyn Attest + Send> = match attest_config {
            AttestConfig::Ipcc => Box::new(ipcc::AttestIpcc::new()?),
            AttestConfig::Local {
                priv_key,
                cert_chain,
                log,
                test_corpus: _,
            } => Box::new(mock::AttestMock::load(cert_chain, log, priv_key)?),
        };

        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        Ok((
            RotAttestationTask { log, rx, attest },
            RotAttestationHandle { tx },
        ))
    }

    /// Run the main request handler loop that processes incoming RoT
    /// attestation requests.
    ///
    /// This should be run via `spawn_blocking` as we perform ipcc operations
    /// via an ioctl and don't want to block any other tasks.
    fn run(mut self) {
        info!(self.log, "request loop started.");
        loop {
            let Some(req) = self.rx.blocking_recv() else {
                warn!(self.log, "All senders dropped. Exiting.");
                break;
            };

            match req {
                AttestationMessage::GetMeasurementLog(reply_tx) => {
                    let log = self.attest.get_measurement_log();
                    let _ = reply_tx.send(log.map(Into::into));
                }
                AttestationMessage::GetCertificateChain(reply_tx) => {
                    let chain =
                        self.attest.get_certificates().and_then(|chain| {
                            CertificateChain::try_from(chain)
                                .map_err(AttestError::from)
                        });
                    let _ = reply_tx.send(chain);
                }
                AttestationMessage::Attest(nonce, reply_tx) => {
                    let attestation = self.attest.attest(&nonce.0);
                    let _ = reply_tx.send(attestation.map(Into::into));
                }
            }
        }
    }
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
        let attestor = RotAttestationTask::launch(&logctx.log, &cfg).unwrap();
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

    /// This type mocks the interaction between the `RotAttestationTask` and
    /// the `RotAttestationHandle` to test the request queuing mechanics.
    /// `fill_queue` should be called first to simulate the task accepting
    /// `QUEUE_SIZE` requests. After the queue is full calls to the methods
    /// on `RotAttestationHandle` should return `Busy`. The queue can then be
    /// drained by calling `drain_queue`. This is accomplished by starting the
    /// `RotAttestationTask` that will service all outstanding requests using
    /// the mock `Attest` impl. Once the queue has been drained future calls
    /// through the `RotAttestationHandle` should succeed.
    struct AttestorQueueFiller {
        attestor: RotAttestationHandle,
        task: Option<RotAttestationTask>,
        pending: Vec<oneshot::Receiver<Result<MeasurementLog, AttestError>>>,
    }

    impl AttestorQueueFiller {
        fn new(log: &Logger, dir: &Utf8Path) -> Self {
            let (task, attestor) =
                RotAttestationTask::new(log, &mock_config(dir)).unwrap();
            Self { attestor, task: Some(task), pending: Vec::new() }
        }

        /// Fill the request queue simulating `QUEUE_SIZE` requests outstanding
        fn fill_queue(&mut self) {
            for _ in 0..QUEUE_SIZE {
                let (tx, rx) = oneshot::channel();
                let msg = AttestationMessage::GetMeasurementLog(tx);
                assert!(self.attestor.tx.try_send(msg).is_ok());
                self.pending.push(rx);
            }
        }

        /// Drain the queue by starting the `RotAttestationTask` and awaiting
        /// all pending requests
        async fn drain_queue(&mut self) {
            if let Some(task) = self.task.take() {
                tokio::task::spawn_blocking(move || task.run());
            }

            for rx in self.pending.drain(..) {
                rx.await.unwrap().unwrap();
            }
        }
    }

    /// Use the `AttestorQueueFiller` to test behavior when the request queue
    /// is full
    #[tokio::test]
    async fn rot_attestor_queue_full() {
        let logctx = test_setup_log("rot_attestor_busy");
        let tmp = Utf8TempDir::new().unwrap();
        let mut rot = AttestorQueueFiller::new(&logctx.log, tmp.path());
        rot.fill_queue();

        // Request queue is full, additional requests are rejected
        assert!(matches!(
            rot.attestor.get_measurement_log().await,
            Err(RotError::Busy)
        ));
        assert!(matches!(
            rot.attestor.get_certificate_chain().await,
            Err(RotError::Busy)
        ));
        let nonce: Nonce = SaRotTypes::Nonce::N32([7u8; 32]).into();
        assert!(matches!(
            rot.attestor.attest(nonce).await,
            Err(RotError::Busy)
        ));

        // RotError::Busy is reported to clients as 503 / Service Unavailable
        let err: HttpError = RotError::Busy.into();
        assert_eq!(
            err.status_code.as_status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );

        // Once the outstanding requests complete, new ones succeed.
        rot.drain_queue().await;
        assert!(rot.attestor.get_measurement_log().await.is_ok());

        logctx.cleanup_successful();
    }
}
