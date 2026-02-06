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

impl From<TrySendError<RotAttestationMessage>> for RotError {
    fn from(e: TrySendError<RotAttestationMessage>) -> Self {
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

type RotAttestationResponse<T> = oneshot::Sender<Result<T, AttestError>>;

enum RotAttestationMessage {
    GetMeasurementLog(RotAttestationResponse<MeasurementLog>),
    GetCertificateChain(RotAttestationResponse<CertificateChain>),
    Attest(Nonce, RotAttestationResponse<Attestation>),
}

#[derive(Debug)]
pub struct RotAttestationHandle {
    tx: mpsc::Sender<RotAttestationMessage>,
}

impl RotAttestationHandle {
    pub async fn get_measurement_log(
        &self,
    ) -> Result<MeasurementLog, RotError> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(RotAttestationMessage::GetMeasurementLog(tx))?;
        Ok(rx.await.map_err(|_| RotError::Shutdown)??)
    }

    pub async fn get_certificate_chain(
        &self,
    ) -> Result<CertificateChain, RotError> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(RotAttestationMessage::GetCertificateChain(tx))?;
        Ok(rx.await.map_err(|_| RotError::Shutdown)??)
    }

    pub async fn attest(&self, nonce: Nonce) -> Result<Attestation, RotError> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send(RotAttestationMessage::Attest(nonce, tx))?;
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
    rx: mpsc::Receiver<RotAttestationMessage>,
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
                RotAttestationMessage::GetMeasurementLog(reply_tx) => {
                    let log = self.attest.get_measurement_log();
                    let _ = reply_tx.send(log.map(Into::into));
                }
                RotAttestationMessage::GetCertificateChain(reply_tx) => {
                    let chain =
                        self.attest.get_certificates().and_then(|chain| {
                            CertificateChain::try_from(chain)
                                .map_err(AttestError::from)
                        });
                    let _ = reply_tx.send(chain);
                }
                RotAttestationMessage::Attest(nonce, reply_tx) => {
                    let attestation = self.attest.attest(&nonce.0);
                    let _ = reply_tx.send(attestation.map(Into::into));
                }
            }
        }
    }
}
