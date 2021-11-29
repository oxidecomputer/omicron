use slog::Logger;

use spdm::msgs::capabilities::{Capabilities, RspFlags};
use spdm::responder::{self, algorithms, capabilities, id_auth};
use spdm::Transcript;

use super::{SpdmError, Transport, MAX_BUF_SIZE};

// A `Ctx` contains shared types for use by a responder task
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
        let state = responder::start();
        let req = self.transport.recv(&self.log).await?;

        let (data, state) =
            state.handle_msg(&req[..], &mut self.buf, &mut self.transcript)?;
        debug!(self.log, "Responder received GET_VERSION");

        self.transport.send(data).await?;
        debug!(self.log, "Responder sent VERSION");
        Ok(state)
    }

    async fn negotiate_capabilities(
        &mut self,
        state: capabilities::State,
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

        let req = self.transport.recv(&self.log).await?;
        let (data, transition) = state.handle_msg(
            supported,
            &req[..],
            &mut self.buf,
            &mut self.transcript,
        )?;
        debug!(self.log, "Responder received GET_CAPABILITIES");

        // We expect to transition to the Algorithms state
        // TODO: Handle both states
        use responder::capabilities::Transition;
        let state = match transition {
            Transition::Algorithms(state) => state,
            _ => panic!("Expected transition to Algorithms state"),
        };

        self.transport.send(data).await?;
        debug!(self.log, "Responder sent CAPABILITIES");
        Ok(state)
    }

    async fn select_algorithms(
        &mut self,
        state: algorithms::State,
    ) -> Result<id_auth::State, SpdmError> {
        let req = self.transport.recv(&self.log).await?;
        let (data, transition) =
            state.handle_msg(&req[..], &mut self.buf, &mut self.transcript)?;
        debug!(self.log, "Responder received NEGOTIATE_ALGORITHMS");

        // We expect to transition to the Algorithms state
        // TODO: Handle both states
        use responder::algorithms::Transition;
        let state = match transition {
            Transition::IdAuth(state) => state,
            _ => panic!("Expected transition to Algorithms state"),
        };

        self.transport.send(data).await?;
        debug!(self.log, "Responder sent ALGORITHMS");
        Ok(state)
    }
}

/// Run the responder side of the SPDM protocol.
///
/// The protocol operates over a TCP stream framed with a 2 byte size
/// header. Requesters and Responders are decoupled from whether the endpoint of
/// a socket is a TCP client or server.
#[allow(dead_code)]
pub async fn run(log: Logger, transport: Transport) -> Result<(), SpdmError> {
    let mut ctx = Ctx::new(log, transport);

    info!(ctx.log, "Responder starting version negotiation");
    let state = ctx.negotiate_version().await?;

    info!(ctx.log, "Responder starting capabilities negotiation");
    let state = ctx.negotiate_capabilities(state).await?;

    info!(ctx.log, "Responder starting algorithms selection");
    let _state = ctx.select_algorithms(state).await?;

    info!(ctx.log, "Responder completed negotiation phase");
    debug!(ctx.log, "Responder transcript: {:x?}\n", ctx.transcript.get());
    Ok(())
}
