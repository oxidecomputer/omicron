use bytes::BytesMut;

use futures::SinkExt;
use slog::Logger;

use spdm::msgs::algorithms::*;
use spdm::msgs::capabilities::{GetCapabilities, ReqFlags};
use spdm::requester::{self, algorithms, capabilities, id_auth};
use spdm::{
    config::{MAX_CERT_CHAIN_SIZE, NUM_SLOTS},
    Transcript,
};

use super::{recv, SpdmError, Transport};

/// Run the requester side of the SPDM protocol.
///
/// The protocol operates over a TCP stream framed with a 2 byte size
/// header. Requesters and Responders are decoupled from whether the endpoint of
/// a socket is a TCP client or server.
#[allow(dead_code)]
pub async fn run(
    log: Logger,
    mut transport: Transport,
) -> Result<(), SpdmError> {
    let mut transcript = Transcript::new();

    info!(log, "Requester starting version negotiation");
    let state =
        negotiate_version(&log, &mut transport, &mut transcript).await?;

    info!(log, "Requester starting capabilities negotiation");
    let state =
        negotiate_capabilities(&log, state, &mut transport, &mut transcript)
            .await?;

    info!(log, "Requester starting algorithms negotiation");
    let _state =
        negotiate_algorithms(&log, state, &mut transport, &mut transcript)
            .await?;

    info!(log, "Requester completed negotiation phase");
    debug!(log, "Requester transcript: {:x?}", transcript.get());

    Ok(())
}

async fn negotiate_version(
    log: &Logger,
    transport: &mut Transport,
    transcript: &mut Transcript,
) -> Result<capabilities::State, SpdmError> {
    let mut buf = BytesMut::with_capacity(10);
    buf.resize(10, 0);
    let state = requester::start();
    let size = state.write_get_version(&mut buf, transcript)?;

    debug!(log, "Requester sending GET_VERSION");
    let data = buf.split_to(size).freeze();
    transport.send(data).await?;
    let rsp = recv(log, transport).await?;
    debug!(log, "Requester received VERSION");

    state.handle_msg(&rsp[..], transcript).map_err(|e| e.into())
}

async fn negotiate_capabilities(
    log: &Logger,
    mut state: capabilities::State,
    transport: &mut Transport,
    transcript: &mut Transcript,
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

    let mut buf = BytesMut::with_capacity(64);
    buf.resize(64, 0);

    debug!(log, "Requester sending GET_CAPABILITIES");
    let size = state.write_msg(&req, &mut buf, transcript)?;
    let data = buf.split_to(size).freeze();
    transport.send(data).await?;
    debug!(log, "Requester received CAPABILITIES");
    let rsp = recv(log, transport).await?;
    state.handle_msg(&rsp, transcript).map_err(|e| e.into())
}

async fn negotiate_algorithms(
    log: &Logger,
    mut state: algorithms::State,
    transport: &mut Transport,
    transcript: &mut Transcript,
) -> Result<id_auth::State, SpdmError> {
    let req = NegotiateAlgorithms {
        measurement_spec: MeasurementSpec::DMTF,
        base_asym_algo: BaseAsymAlgo::ECDSA_ECC_NIST_P384,
        base_hash_algo: BaseHashAlgo::SHA_256 | BaseHashAlgo::SHA3_256,
        num_algorithm_requests: 4,
        algorithm_requests: [
            AlgorithmRequest::Dhe(DheAlgorithm {
                supported: DheFixedAlgorithms::FFDHE_3072
                    | DheFixedAlgorithms::SECP_384_R1,
            }),
            AlgorithmRequest::Aead(AeadAlgorithm {
                supported: AeadFixedAlgorithms::AES_256_GCM
                    | AeadFixedAlgorithms::CHACHA20_POLY1305,
            }),
            AlgorithmRequest::ReqBaseAsym(ReqBaseAsymAlgorithm {
                supported: ReqBaseAsymFixedAlgorithms::ECDSA_ECC_NIST_P384
                    | ReqBaseAsymFixedAlgorithms::ECDSA_ECC_NIST_P256,
            }),
            AlgorithmRequest::KeySchedule(KeyScheduleAlgorithm {
                supported: KeyScheduleFixedAlgorithms::SPDM,
            }),
        ],
    };

    let mut buf = BytesMut::with_capacity(64);
    buf.resize(64, 0);
    debug!(log, "Requester sending NEGOTIATE_ALGORITHMS");
    let size = state.write_msg(req, &mut buf, transcript)?;
    let data = buf.split_to(size).freeze();
    transport.send(data).await?;
    debug!(log, "Requester received ALGORITHMS");

    let rsp = recv(log, transport).await?;

    state
        .handle_msg::<NUM_SLOTS, MAX_CERT_CHAIN_SIZE>(&rsp, transcript)
        .map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use slog::Drain;
    use tokio::net::{TcpListener, TcpStream};

    use super::super::framed_transport;
    use super::super::responder;
    use super::*;

    #[tokio::test]
    async fn negotiation() {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!("component" => "spdm"));
        let log2 = log.clone();

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let listener = TcpListener::bind(addr.clone()).await.unwrap();

        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let transport = framed_transport(sock);
            responder::run(&log, transport).await.unwrap();
        });

        let sock = TcpStream::connect(addr).await.unwrap();
        let transport = framed_transport(sock);
        run(log2, transport).await.unwrap();
    }
}
