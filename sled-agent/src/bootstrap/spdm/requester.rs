// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::Logger;

use spdm::msgs::algorithms::*;
use spdm::msgs::capabilities::{GetCapabilities, ReqFlags};
use spdm::requester::{self, algorithms, capabilities, id_auth};
use spdm::{
    config::{MAX_CERT_CHAIN_SIZE, NUM_SLOTS},
    Transcript,
};

use super::{SpdmError, Transport, MAX_BUF_SIZE};

// A `Ctx` contains shared types for use by a requester task
struct Ctx {
    buf: [u8; MAX_BUF_SIZE],
    log: Logger,
    transport: Transport,
    transcript: Transcript,
}

impl Ctx {
    fn new(log: Logger, transport: Transport) -> Ctx {
        Ctx {
            buf: [0u8; MAX_BUF_SIZE],
            log,
            transport,
            transcript: Transcript::new(),
        }
    }

    async fn negotiate_version(
        &mut self,
    ) -> Result<capabilities::State, SpdmError> {
        let state = requester::start();
        let data =
            state.write_get_version(&mut self.buf, &mut self.transcript)?;

        debug!(self.log, "Requester sending GET_VERSION");
        self.transport.send(data).await?;

        let rsp = self.transport.recv().await?;
        debug!(self.log, "Requester received VERSION");

        state.handle_msg(&rsp[..], &mut self.transcript).map_err(|e| e.into())
    }

    async fn negotiate_capabilities(
        &mut self,
        mut state: capabilities::State,
    ) -> Result<algorithms::State, SpdmError> {
        let req = GetCapabilities {
            ct_exponent: 12,
            flags: ReqFlags::CERT_CAP
                | ReqFlags::CHAL_CAP
                | ReqFlags::ENCRYPT_CAP
                | ReqFlags::MAC_CAP
                | ReqFlags::MUT_AUTH_CAP
                | ReqFlags::KEY_EX_CAP
                | ReqFlags::ENCAP_CAP
                | ReqFlags::HBEAT_CAP
                | ReqFlags::KEY_UPD_CAP,
        };

        debug!(self.log, "Requester sending GET_CAPABILITIES");
        let data =
            state.write_msg(&req, &mut self.buf, &mut self.transcript)?;
        self.transport.send(data).await?;

        let rsp = self.transport.recv().await?;
        debug!(self.log, "Requester received CAPABILITIES");
        state.handle_msg(&rsp, &mut self.transcript).map_err(|e| e.into())
    }

    async fn negotiate_algorithms(
        &mut self,
        mut state: algorithms::State,
    ) -> Result<id_auth::State, SpdmError> {
        let req = NegotiateAlgorithms {
            measurement_spec: MeasurementSpec::DMTF,
            base_asym_algo: BaseAsymAlgo::ECDSA_ECC_NIST_P256,
            base_hash_algo: BaseHashAlgo::SHA_256,
            num_algorithm_requests: 4,
            algorithm_requests: [
                AlgorithmRequest::Dhe(DheAlgorithm {
                    supported: DheFixedAlgorithms::SECP_256_R1,
                }),
                AlgorithmRequest::Aead(AeadAlgorithm {
                    supported: AeadFixedAlgorithms::AES_256_GCM,
                }),
                AlgorithmRequest::ReqBaseAsym(ReqBaseAsymAlgorithm {
                    supported: ReqBaseAsymFixedAlgorithms::ECDSA_ECC_NIST_P256,
                }),
                AlgorithmRequest::KeySchedule(KeyScheduleAlgorithm {
                    supported: KeyScheduleFixedAlgorithms::SPDM,
                }),
            ],
        };

        debug!(self.log, "Requester sending NEGOTIATE_ALGORITHMS");
        let data = state.write_msg(req, &mut self.buf, &mut self.transcript)?;
        self.transport.send(data).await?;

        let rsp = self.transport.recv().await?;
        debug!(self.log, "Requester received ALGORITHMS");

        state
            .handle_msg::<NUM_SLOTS, MAX_CERT_CHAIN_SIZE>(
                &rsp,
                &mut self.transcript,
            )
            .map_err(|e| e.into())
    }
}

/// Run the requester side of the SPDM protocol.
///
/// The protocol operates over a TCP stream framed with a 2 byte size
/// header. Requesters and Responders are decoupled from whether the endpoint of
/// a socket is a TCP client or server.
#[allow(dead_code)]
pub async fn run(
    log: Logger,
    transport: Transport,
) -> Result<Transport, SpdmError> {
    let mut ctx = Ctx::new(log, transport);

    info!(ctx.log, "Requester starting version negotiation");
    let state = ctx.negotiate_version().await?;

    info!(ctx.log, "Requester starting capabilities negotiation");
    let state = ctx.negotiate_capabilities(state).await?;

    info!(ctx.log, "Requester starting algorithms negotiation");
    let _state = ctx.negotiate_algorithms(state).await?;

    info!(ctx.log, "Requester completed negotiation phase");
    debug!(ctx.log, "Requester transcript: {:x?}", ctx.transcript.get());

    Ok(ctx.transport)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};

    use super::super::responder;
    use super::*;

    #[tokio::test]
    async fn negotiation() {
        let log = omicron_test_utils::dev::test_setup_log("negotiation").log;
        let log2 = log.clone();
        let log3 = log.clone();

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let listener = TcpListener::bind(addr.clone()).await.unwrap();

        let handle = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let log2 = log.clone();
            let transport = Transport::new(sock, log);
            responder::run(log2, transport).await.unwrap();
        });

        let sock = TcpStream::connect(addr).await.unwrap();
        let transport = Transport::new(sock, log2);
        run(log3, transport).await.unwrap();

        handle.await.unwrap();
    }
}
