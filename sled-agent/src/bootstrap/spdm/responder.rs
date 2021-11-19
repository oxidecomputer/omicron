use bytes::BytesMut;

use futures::SinkExt;
use slog::Logger;

use spdm::msgs::algorithms::*;
use spdm::msgs::capabilities::{Capabilities, RspFlags};
use spdm::responder::{self, algorithms, capabilities, id_auth};
use spdm::Transcript;

use super::{recv, SpdmError, Transport};

/// Run the responder side of the SPDM protocol.
///
/// The protocol operates over a TCP stream framed with a 2 byte size
/// header. Requesters and Responders are decoupled from whether the endpoint of
/// a socket is a TCP client or server.
pub async fn run(
    log: &Logger,
    mut transport: Transport,
) -> Result<(), SpdmError> {
    let mut transcript = Transcript::new();

    info!(log, "Responder starting version negotiation");
    let state = negotiate_version(log, &mut transport, &mut transcript).await?;

    info!(log, "Responder starting capabilities negotiation");
    let state =
        negotiate_capabilities(log, state, &mut transport, &mut transcript)
            .await?;

    println!("Responder starting algorithms selection");
    let _state =
        select_algorithms(log, state, &mut transport, &mut transcript).await?;

    info!(log, "Responder completed negotiation phase");
    debug!(log, "Responder transcript: {:x?}\n", transcript.get());
    Ok(())
}

async fn negotiate_version(
    log: &Logger,
    transport: &mut Transport,
    transcript: &mut Transcript,
) -> Result<capabilities::State, SpdmError> {
    let mut buf = BytesMut::with_capacity(10);
    buf.resize(10, 0);

    let state = responder::start();
    let req = recv(log, transport).await?;

    let (size, state) = state.handle_msg(&req[..], &mut buf, transcript)?;
    debug!(log, "Responder received GET_VERSION");

    let data = buf.split_to(size).freeze();
    transport.send(data).await?;
    debug!(log, "Responder sent VERSION");
    Ok(state)
}

async fn negotiate_capabilities(
    log: &Logger,
    state: capabilities::State,
    transport: &mut Transport,
    transcript: &mut Transcript,
) -> Result<algorithms::State, SpdmError> {
    let supported = Capabilities {
        ct_exponent: 14,
        flags: RspFlags::CERT_CAP
            | RspFlags::CHAL_CAP
            | RspFlags::ENCRYPT_CAP
            | RspFlags::MAC_CAP
            | RspFlags::MUT_AUTH_CAP
            | RspFlags::KEY_EX_CAP
            | RspFlags::ENCAP_CAP
            | RspFlags::HBEAT_CAP
            | RspFlags::KEY_UPD_CAP,
    };

    let req = recv(log, transport).await?;

    let mut rsp = BytesMut::with_capacity(64);
    rsp.resize(64, 0);
    let (size, transition) =
        state.handle_msg(supported, &req[..], &mut rsp, transcript)?;
    debug!(log, "Responder received GET_CAPABILITIES");

    // We expect to transition to the Algorithms state
    // TODO: Handle both states
    use responder::capabilities::Transition;
    let state = match transition {
        Transition::Algorithms(state) => state,
        _ => panic!("Expected transition to Algorithms state"),
    };

    let data = rsp.split_to(size).freeze();
    transport.send(data).await?;
    debug!(log, "Responder sent CAPABILITIES");
    Ok(state)
}

async fn select_algorithms(
    log: &Logger,
    state: algorithms::State,
    transport: &mut Transport,
    transcript: &mut Transcript,
) -> Result<id_auth::State, SpdmError> {
    let req = recv(log, transport).await?;

    let mut buf = BytesMut::with_capacity(64);
    buf.resize(64, 0);

    let (size, transition) =
        state.handle_msg(&req[..], &mut buf, transcript)?;
    debug!(log, "Responder received NEGOTIATE_ALGORITHMS");

    // We expect to transition to the Algorithms state
    // TODO: Handle both states
    use responder::algorithms::Transition;
    let state = match transition {
        Transition::IdAuth(state) => state,
        _ => panic!("Expected transition to Algorithms state"),
    };

    let data = buf.split_to(size).freeze();
    transport.send(data).await?;
    debug!(log, "Responder sent ALGORITHMS");
    Ok(state)
}
