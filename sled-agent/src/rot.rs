// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mechanisms for interacting with the sled's RoT.

use dropshot::HttpError;
use ipcc::AttestError;
use sled_agent_types::rot::{
    Attestation, CertificateChain, MeasurementLog, Nonce,
};

use slog::Logger;
use slog_error_chain::InlineErrorChain;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    oneshot,
};

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
    ipcc: ipcc::Ipcc,
}

type RotAttestation = (RotAttestationTask, RotAttestationHandle);

impl RotAttestationTask {
    pub fn new(log: &Logger) -> Result<RotAttestation, RotError> {
        let ipcc = ipcc::Ipcc::new()?;
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        Ok((
            RotAttestationTask {
                log: log.new(o!("component" => "RotAttestationTask")),
                rx,
                ipcc,
            },
            RotAttestationHandle { tx },
        ))
    }

    /// Run the main request handler loop that processes incoming RoT
    /// attestation requests.
    ///
    /// This should be run via `spawn_blocking` as we perform ipcc operations
    /// via an ioctl and don't want to block any other tasks.
    pub fn run(mut self) {
        loop {
            let Some(req) = self.rx.blocking_recv() else {
                warn!(self.log, "All senders dropped. Exiting.");
                break;
            };

            match req {
                RotAttestationMessage::GetMeasurementLog(reply_tx) => {
                    let log = self.ipcc.get_measurement_log();
                    let _ = reply_tx.send(log.map(Into::into));
                }
                RotAttestationMessage::GetCertificateChain(reply_tx) => {
                    let chain =
                        self.ipcc.get_certificates().and_then(|chain| {
                            CertificateChain::try_from(chain)
                                .map_err(AttestError::from)
                        });
                    let _ = reply_tx.send(chain);
                }
                RotAttestationMessage::Attest(nonce, reply_tx) => {
                    let Nonce::N32(nonce) = nonce;
                    let attestation = self.ipcc.attest(nonce.into());
                    let _ = reply_tx.send(attestation.map(Into::into));
                }
            }
        }
    }
}
