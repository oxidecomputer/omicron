// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms for interacting with the sled's RoT.

use dropshot::HttpError;
use ipcc::AttestError;
use sled_agent_types::rot as SaRotTypes;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    oneshot,
};
use x509_cert::der::{EncodePem, pem::LineEnding};

use crate::sled_agent::Error;

pub struct MeasurementLog(attest_data::Log);

impl From<attest_data::Log> for MeasurementLog {
    fn from(log: attest_data::Log) -> Self {
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
                attest_data::Measurement::Sha3_256(d) => {
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

pub struct Nonce(attest_data::Nonce);

impl From<SaRotTypes::Nonce> for Nonce {
    fn from(nonce: SaRotTypes::Nonce) -> Self {
        match nonce {
            SaRotTypes::Nonce::N32(n32) => {
                Nonce(attest_data::Nonce::N32(n32.into()))
            }
        }
    }
}

pub struct Attestation(attest_data::Attestation);

impl From<attest_data::Attestation> for Attestation {
    fn from(att: attest_data::Attestation) -> Self {
        Attestation(att)
    }
}

impl From<Attestation> for SaRotTypes::Attestation {
    fn from(att: Attestation) -> Self {
        match att.0 {
            attest_data::Attestation::Ed25519(sig) => {
                SaRotTypes::Attestation::Ed25519(SaRotTypes::Ed25519Signature(
                    sig.0,
                ))
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum RotError {
    #[error(transparent)]
    Ipcc(#[from] ipcc::IpccError),

    #[error(transparent)]
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

pub struct RotAttestationTask {
    log: Logger,
    rx: mpsc::Receiver<RotAttestationMessage>,
    ipcc: Option<ipcc::Ipcc>,
}

type RotAttestation = (RotAttestationTask, RotAttestationHandle);

impl RotAttestationTask {
    pub fn new(log: &Logger) -> Result<RotAttestation, Error> {
        let log = log.new(o!("component" => "RotAttestationTask"));

        let ipcc = match ipcc::Ipcc::new() {
            Ok(ipcc) => Some(ipcc),
            Err(e) => {
                // Only fail here if we're on an Oxide sled where we expect to
                // have IPCC available. Otherwise, we just log the error and
                // continue. Any subsequent attestation requests will fail as
                // `RotAttestationTask::run` will bail without an Ipcc handle.
                error!(log, "failed to get ipcc handle: {e}");
                let is_oxide = sled_hardware::is_oxide_sled().map_err(|e| {
                    Error::Underlay(
                        sled_hardware::underlay::Error::SystemDetection(e),
                    )
                })?;
                if is_oxide {
                    return Err(RotError::Ipcc(e).into());
                }
                None
            }
        };

        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        Ok((RotAttestationTask { log, rx, ipcc }, RotAttestationHandle { tx }))
    }

    /// Run the main request handler loop that processes incoming RoT
    /// attestation requests.
    ///
    /// This should be run via `spawn_blocking` as we perform ipcc operations
    /// via an ioctl and don't want to block any other tasks.
    pub fn run(mut self) {
        let Some(ipcc) = self.ipcc.take() else {
            warn!(self.log, "No ipcc handle. Exiting.");
            return;
        };
        loop {
            let Some(req) = self.rx.blocking_recv() else {
                warn!(self.log, "All senders dropped. Exiting.");
                break;
            };

            match req {
                RotAttestationMessage::GetMeasurementLog(reply_tx) => {
                    let log = ipcc.get_measurement_log();
                    let _ = reply_tx.send(log.map(Into::into));
                }
                RotAttestationMessage::GetCertificateChain(reply_tx) => {
                    let chain = ipcc.get_certificates().and_then(|chain| {
                        CertificateChain::try_from(chain)
                            .map_err(AttestError::from)
                    });
                    let _ = reply_tx.send(chain);
                }
                RotAttestationMessage::Attest(nonce, reply_tx) => {
                    let attest_data::Nonce::N32(nonce) = nonce.0;
                    let attestation = ipcc.attest(nonce);
                    let _ = reply_tx.send(attestation.map(Into::into));
                }
            }
        }
    }
}
