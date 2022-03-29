// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO: This may need to move in part or in its entirety to a separate crate
// for reuse by RSS, once that exists.

//! Inteface for communicating with SPs over UDP on the management network.

mod bulk_state_get;
mod serial_console_history;

pub(crate) use self::bulk_state_get::BulkSpStateSingleResult;
pub(crate) use self::bulk_state_get::BulkStateProgress;
pub(crate) use self::bulk_state_get::SpStateRequestId;
pub(crate) use self::serial_console_history::SerialConsoleContents;

use self::bulk_state_get::OutstandingSpStateRequests;
use self::serial_console_history::SerialConsoleHistory;

use crate::http_entrypoints::SpState;
use dropshot::HttpError;
use gateway_messages::sp_impl::SerialConsolePacketizer;
use gateway_messages::version;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::Request;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SerialConsole;
use gateway_messages::SerializedSize;
use gateway_messages::SpComponent;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use gateway_sp_comms::error::StartupError;
use gateway_sp_comms::KnownSps;
use gateway_sp_comms::ManagementSwitch;
use gateway_sp_comms::ManagementSwitchDiscovery;
use gateway_sp_comms::SpIdentifier;
use gateway_sp_comms::SpSocket;
use gateway_sp_comms::SwitchPort;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::time::Instant;

// TODO This has some duplication with `gateway::error::Error`. This might be
// right if these comms are going to move to their own crate, but at the moment
// it's confusing. For now we'll keep them separate, but maybe we should split
// this module out into its own crate sooner rather than later.
#[derive(Debug, Clone, Error)]
pub enum Error {
    // ----
    // internal errors
    // ----
    // `Error` needs to be `Clone` because we hold onto `Result<SpState, Error>`
    // in memory during bulk "get all SP state" requests, and then we clone
    // those results to give to any clients that want info for that request.
    // `io::Error` isn't `Clone`, so we wrap it in an `Arc`.
    #[error("error sending to UDP address {addr}: {err}")]
    UdpSend { addr: SocketAddr, err: Arc<io::Error> },
    #[error(
        "SP sent a bogus response type (got `{got}`; expected `{expected}`)"
    )]
    BogusResponseType { got: &'static str, expected: &'static str },
    #[error("error from SP: {0}")]
    SpError(#[from] ResponseError),
    #[error("timeout")]
    Timeout,
    #[error("received ignition target for an unknown port ({0})")]
    UnknownIgnitionTargetPort(usize),
    #[error("unknown SP destination address for {0:?}")]
    UnknownSpAddress(SwitchPort),
    #[error("nonexistent SP {0:?}")]
    SpDoesNotExist(SpIdentifier),

    // ----
    // client errors
    // ----
    #[error("invalid page token (no such request)")]
    NoSuchRequest,
    #[error("invalid page token (invalid last SP seen)")]
    InvalidLastSpSeen,
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::NoSuchRequest => HttpError::for_bad_request(
                Some(String::from("NoSuchRequest")),
                err.to_string(),
            ),
            Error::InvalidLastSpSeen => HttpError::for_bad_request(
                Some(String::from("InvalidLastSpSeen")),
                err.to_string(),
            ),
            Error::UdpSend { .. }
            | Error::BogusResponseType { .. }
            | Error::SpError(_)
            | Error::Timeout
            | Error::UnknownIgnitionTargetPort(_)
            | Error::UnknownSpAddress(_)
            | Error::SpDoesNotExist(_) => {
                HttpError::for_internal_error(err.to_string())
            }
        }
    }
}

impl Error {
    fn from_unhandled_response_kind(
        kind: &ResponseKind,
        expected: &'static str,
    ) -> Self {
        let got = match kind {
            ResponseKind::Pong => response_kind_names::PONG,
            ResponseKind::IgnitionState(_) => {
                response_kind_names::IGNITION_STATE
            }
            ResponseKind::BulkIgnitionState(_) => {
                response_kind_names::BULK_IGNITION_STATE
            }
            ResponseKind::IgnitionCommandAck => {
                response_kind_names::IGNITION_COMMAND_ACK
            }
            ResponseKind::SpState(_) => response_kind_names::SP_STATE,
            ResponseKind::SerialConsoleWriteAck => {
                response_kind_names::SERIAL_CONSOLE_WRITE_ACK
            }
        };
        Self::BogusResponseType { got, expected }
    }
}

// helper constants mapping SP response kinds to stringy names for error
// messages
mod response_kind_names {
    pub(super) const PONG: &str = "pong";
    pub(super) const IGNITION_STATE: &str = "ignition_state";
    pub(super) const BULK_IGNITION_STATE: &str = "bulk_ignition_state";
    pub(super) const IGNITION_COMMAND_ACK: &str = "ignition_command_ack";
    pub(super) const SP_STATE: &str = "sp_state";
    pub(super) const SERIAL_CONSOLE_WRITE_ACK: &str =
        "serial_console_write_ack";
}

#[derive(Debug)]
pub struct SpCommunicator {
    log: Logger,
    switch: ManagementSwitch,
    sp_state: Arc<AllSpState>,
    bulk_state_requests: OutstandingSpStateRequests,
    request_id: AtomicU32,
}

impl SpCommunicator {
    pub async fn new(
        known_sps: KnownSps,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        let log = log.new(o!("componennt" => "SpCommunicator"));
        let discovery = ManagementSwitchDiscovery::placeholder_start(
            known_sps,
            log.clone(),
        )
        .await?;

        // build our map of switch ports to state-of-the-SP-on-that-port
        let sp_state = Arc::new(AllSpState::new(&discovery));

        let recv_handler = RecvHandler::new(Arc::clone(&sp_state), log.clone());
        let switch = discovery
            .start_recv_task(move |port, data| recv_handler.handle(port, data));

        info!(&log, "started SP communicator");
        Ok(Self {
            log,
            switch,
            sp_state,
            bulk_state_requests: OutstandingSpStateRequests::default(),
            request_id: AtomicU32::new(0),
        })
    }

    fn id_to_port(&self, sp: SpIdentifier) -> Result<SwitchPort, Error> {
        self.switch.switch_port(sp).ok_or(Error::SpDoesNotExist(sp))
    }

    pub(crate) fn port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        self.switch.switch_port_to_id(port)
    }

    pub(crate) async fn state_get(
        &self,
        sp: SpIdentifier,
        timeout: Instant,
    ) -> Result<SpState, Error> {
        self.state_get_by_port(self.id_to_port(sp)?, timeout).await
    }

    async fn state_get_by_port(
        &self,
        port: SwitchPort,
        timeout: Instant,
    ) -> Result<SpState, Error> {
        tokio::time::timeout_at(timeout, self.state_get_impl(port)).await?
    }

    pub(crate) fn bulk_state_start(
        self: &Arc<Self>,
        timeout: Instant,
        retain_grace_period: Duration,
        sps: impl Iterator<Item = (SwitchPort, IgnitionState)>,
    ) -> SpStateRequestId {
        self.bulk_state_requests.start(timeout, retain_grace_period, sps, self)
    }

    pub(crate) async fn bulk_state_get_progress(
        &self,
        id: &SpStateRequestId,
        last_seen: Option<SpIdentifier>,
        timeout: Duration,
        limit: usize,
    ) -> Result<BulkStateProgress, Error> {
        let last_seen = match last_seen {
            Some(sp) => Some(self.id_to_port(sp)?),
            None => None,
        };
        self.bulk_state_requests
            .get(id, last_seen, timeout, limit, &self.log)
            .await
    }

    async fn state_get_impl(&self, port: SwitchPort) -> Result<SpState, Error> {
        let sp =
            self.switch.sp_socket(port).ok_or(Error::UnknownSpAddress(port))?;

        match self.request_response(&sp, RequestKind::SpState).await? {
            ResponseKind::SpState(state) => Ok(SpState::Enabled {
                serial_number: hex::encode(&state.serial_number[..]),
            }),
            other => Err(Error::from_unhandled_response_kind(
                &other,
                response_kind_names::SP_STATE,
            )),
        }
    }

    pub(crate) fn serial_console_get(
        &self,
        sp: SpIdentifier,
        component: &SpComponent,
    ) -> Result<Option<SerialConsoleContents>, Error> {
        let state = self.sp_state.get(self.id_to_port(sp)?);
        Ok(state.serial_console_from_sp.lock().unwrap().contents(component))
    }

    pub(crate) async fn serial_console_post(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
        data: &[u8],
        timeout: Duration,
    ) -> Result<(), Error> {
        tokio::time::timeout(
            timeout,
            self.serial_console_post_impl(sp, component, data),
        )
        .await?
    }

    async fn serial_console_post_impl(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
        data: &[u8],
    ) -> Result<(), Error> {
        let sp_port = self.id_to_port(sp)?;
        let sp_state = self.sp_state.get(self.id_to_port(sp)?);

        let sp_sock = self
            .switch
            .sp_socket(sp_port)
            .ok_or(Error::UnknownSpAddress(sp_port))?;

        let mut packetizers = sp_state.serial_console_to_sp.lock().await;
        let packetizer = packetizers
            .entry(component)
            .or_insert_with(|| SerialConsolePacketizer::new(component));

        for packet in packetizer.packetize(data) {
            let request = RequestKind::SerialConsoleWrite(packet);
            match self.request_response(&sp_sock, request).await? {
                ResponseKind::SerialConsoleWriteAck => (),
                other => {
                    return Err(Error::from_unhandled_response_kind(
                        &other,
                        response_kind_names::SERIAL_CONSOLE_WRITE_ACK,
                    ))
                }
            }
        }

        Ok(())
    }

    pub async fn bulk_ignition_get(
        &self,
        timeout: Duration,
    ) -> Result<Vec<(SwitchPort, IgnitionState)>, Error> {
        tokio::time::timeout(timeout, self.bulk_ignition_get_impl()).await?
    }

    async fn bulk_ignition_get_impl(
        &self,
    ) -> Result<Vec<(SwitchPort, IgnitionState)>, Error> {
        let controller = self.switch.ignition_controller();
        let request = RequestKind::BulkIgnitionState;

        match self.request_response(&controller, request).await? {
            ResponseKind::BulkIgnitionState(state) => {
                let mut results = Vec::new();
                for (i, state) in state.targets
                    [..usize::from(state.num_targets)]
                    .iter()
                    .copied()
                    .enumerate()
                {
                    let port = self
                        .switch
                        .switch_port_from_ignition_target(i)
                        .ok_or_else(|| Error::UnknownIgnitionTargetPort(i))?;
                    results.push((port, state));
                }
                Ok(results)
            }
            other => {
                return Err(Error::from_unhandled_response_kind(
                    &other,
                    response_kind_names::BULK_IGNITION_STATE,
                ))
            }
        }
    }

    // How do we want to describe ignition targets? Currently we want to
    // send a u8 in the UDP message, so just take that for now.
    pub async fn ignition_get(
        &self,
        sp: SpIdentifier,
        timeout: Duration,
    ) -> Result<IgnitionState, Error> {
        tokio::time::timeout(timeout, self.ignition_get_impl(sp)).await?
    }

    async fn ignition_get_impl(
        &self,
        sp: SpIdentifier,
    ) -> Result<IgnitionState, Error> {
        let controller = self.switch.ignition_controller();
        let port = self.id_to_port(sp)?;
        let request =
            RequestKind::IgnitionState { target: port.as_ignition_target() };

        match self.request_response(&controller, request).await? {
            ResponseKind::IgnitionState(state) => Ok(state),
            other => {
                return Err(Error::from_unhandled_response_kind(
                    &other,
                    response_kind_names::IGNITION_STATE,
                ))
            }
        }
    }

    pub async fn ignition_power_on(
        &self,
        sp: SpIdentifier,
        timeout: Duration,
    ) -> Result<(), Error> {
        tokio::time::timeout(
            timeout,
            self.ignition_command(sp, IgnitionCommand::PowerOn),
        )
        .await?
    }

    pub async fn ignition_power_off(
        &self,
        sp: SpIdentifier,
        timeout: Duration,
    ) -> Result<(), Error> {
        tokio::time::timeout(
            timeout,
            self.ignition_command(sp, IgnitionCommand::PowerOff),
        )
        .await?
    }

    async fn ignition_command(
        &self,
        sp: SpIdentifier,
        command: IgnitionCommand,
    ) -> Result<(), Error> {
        let controller = self.switch.ignition_controller();
        let port = self.id_to_port(sp)?;
        let request = RequestKind::IgnitionCommand {
            target: port.as_ignition_target(),
            command,
        };

        match self.request_response(&controller, request).await? {
            ResponseKind::IgnitionCommandAck => Ok(()),
            other => {
                return Err(Error::from_unhandled_response_kind(
                    &other,
                    response_kind_names::IGNITION_COMMAND_ACK,
                ))
            }
        }
    }

    async fn request_response(
        &self,
        sp: &SpSocket<'_>,
        request: RequestKind,
    ) -> Result<ResponseKind, Error> {
        // request IDs will eventually roll over; since we enforce timeouts
        // this should be a non-issue in practice. does this need testing?
        let request_id =
            self.request_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // tell our background receiver to expect a response to this request
        let response =
            self.sp_state.insert_expected_response(sp.port(), request_id);

        // Serialize and send our request. We know `buf` is large enough for any
        // `Request`, so unwrapping here is fine.
        let request =
            Request { version: version::V1, request_id, kind: request };
        let mut buf = [0; Request::MAX_SIZE];
        let n = gateway_messages::serialize(&mut buf, &request).unwrap();

        let serialized_request = &buf[..n];
        debug!(&self.log, "sending {:?} to SP {:?}", request, sp);
        sp.send(serialized_request).await.map_err(|err| Error::UdpSend {
            addr: sp.addr(),
            err: Arc::new(err),
        })?;

        // recv() can only fail if the sender is dropped, but we're holding it
        // in `self.outstanding_requests`; unwrap() is fine.
        Ok(response.recv().await.unwrap()?)
    }
}

/// Handler for incoming packets received on management switch ports.
///
/// When the communicator wants to send a request on behalf of an HTTP request:
///
/// 1. `SpCommunicator` creates a tokio oneshot channel for this handler to use
///    to send the response.
/// 2. `SpCommunicator` inserts the sending half of that channel into
///    `outstanding_requests`, which is keyed by both the switch port and the
///    u32 ID attached to the request.
/// 3. `SpCommunicator` sends the UDP packet containing the request to the
///    target SP, and waits for a response on the channel it created in 1.
/// 4. When we receive a packet, we check:
///    a. Does it parse as a `Response`?
///    b. Is there a corresponding entry in `outstanding_requests` for this port
///       + request ID?
///    If so, we send the response on the channel, which unblocks
///    `SpCommunicator`, who can now return the response to its caller.
///
/// If a timeout or other error occurs between step 2 and the end of step 4, the
/// `ResponseReceiver` wrapper below is responsible for cleaning up the entry in
/// `outstanding_requests` (via its `Drop` impl).
///
/// We can also receive messages from SPs that are not responses to oustanding
/// requests. These are handled on a case-by-case basis; e.g., serial console
/// data is pushed into the in-memory ringbuffer corresponding to the source.
struct RecvHandler {
    sp_state: Arc<AllSpState>,
    log: Logger,
}

impl RecvHandler {
    fn new(sp_state: Arc<AllSpState>, log: Logger) -> Self {
        Self { sp_state, log }
    }

    fn handle(&self, port: SwitchPort, buf: &[u8]) {
        debug!(&self.log, "received {} bytes from {:?}", buf.len(), port);

        // parse into an `SpMessage`
        let sp_msg = match gateway_messages::deserialize::<SpMessage>(buf) {
            Ok((msg, _extra)) => {
                // TODO should we check that `extra` is empty? if the
                // response is maximal size any extra data is silently
                // discarded anyway, so probably not?
                msg
            }
            Err(err) => {
                error!(&self.log, "discarding malformed message ({})", err);
                return;
            }
        };
        debug!(&self.log, "received {:?} from {:?}", sp_msg, port);

        // `version` is intentionally the first 4 bytes of the packet; we
        // could check it before trying to deserialize?
        if sp_msg.version != version::V1 {
            error!(
                &self.log,
                "discarding message with unsupported version {}",
                sp_msg.version
            );
            return;
        }

        // decide whether this is a response to an outstanding request or an
        // unprompted message
        match sp_msg.kind {
            SpMessageKind::Response { request_id, result } => {
                self.handle_response(port, request_id, result);
            }
            SpMessageKind::SerialConsole(serial_console) => {
                self.handle_serial_console(port, serial_console);
            }
        }
    }

    fn handle_response(
        &self,
        port: SwitchPort,
        request_id: u32,
        result: Result<ResponseKind, ResponseError>,
    ) {
        // see if we know who to send the response to
        let tx = match self.sp_state.remove_expected_response(port, request_id)
        {
            Some(tx) => tx,
            None => {
                error!(
                    &self.log,
                    "discarding unexpected response {} from {:?} (possibly past timeout?)",
                    request_id,
                    port,
                );
                return;
            }
        };

        // actually send it
        if tx.send(result).is_err() {
            // This can only fail if the receiving half has been dropped.
            // That's held in the relevant `SpCommunicator` method above
            // that initiated this request; they should only have dropped
            // the rx half if they've been dropped (in which case we've been
            // aborted and can't get here) or if we landed in a race where
            // the `SpCommunicator` task was cancelled (presumably by
            // timeout) in between us pulling `tx` out of
            // `outstanding_requests` and actually sending the response on
            // it. But that window does exist, so log when we fail to send.
            // I believe these should be interpreted as timeout failures;
            // most of the time failing to get a `tx` at all (above) is also
            // caused by a timeout, but that path is also invoked if we get
            // a garbage response somehow.
            error!(
                &self.log,
                "discarding unexpected response {} from {:?} (receiver gone)",
                request_id,
                port,
            );
        }
    }

    fn handle_serial_console(&self, port: SwitchPort, packet: SerialConsole) {
        debug!(
            &self.log,
            "received serial console data from {:?}: {:?}", port, packet
        );
        self.sp_state.push_serial_console(port, packet, &self.log);
    }
}

#[derive(Debug)]
struct AllSpState {
    all_sps: HashMap<SwitchPort, SingleSpState>,
}

impl AllSpState {
    fn new(switch: &ManagementSwitchDiscovery) -> Self {
        let all_ports = switch.all_ports();
        let mut all_sps = HashMap::with_capacity(all_ports.len());
        for port in all_ports {
            all_sps.insert(port, SingleSpState::default());
        }
        Self { all_sps }
    }

    fn get(&self, port: SwitchPort) -> &SingleSpState {
        // we initialize `all_sps` with state for every switch port, so the only
        // way to panic here is to construct a port that didn't exist when we
        // were created
        self.all_sps
            .get(&port)
            .expect("sp state doesn't contain an entry for a valid switch port")
    }

    fn push_serial_console(
        &self,
        port: SwitchPort,
        packet: SerialConsole,
        log: &Logger,
    ) {
        let state = self.get(port);
        state.serial_console_from_sp.lock().unwrap().push(packet, log);
    }

    fn insert_expected_response(
        &self,
        port: SwitchPort,
        request_id: u32,
    ) -> ResponseReceiver {
        let state = self.get(port);
        state.outstanding_requests.insert(request_id)
    }

    fn remove_expected_response(
        &self,
        port: SwitchPort,
        request_id: u32,
    ) -> Option<Sender<Result<ResponseKind, ResponseError>>> {
        let state = self.get(port);
        state.outstanding_requests.remove(request_id)
    }
}

#[derive(Debug, Default)]
struct SingleSpState {
    // map of requests we're waiting to receive
    outstanding_requests: Arc<OutstandingRequests>,
    // ringbuffer of serial console data from the SP
    serial_console_from_sp: Mutex<SerialConsoleHistory>,
    // counter of bytes we've sent per SP component; we want to hold this mutex
    // across await points as we packetize data, so we have to use a tokio mutex
    // here instead of a `std::sync::Mutex`
    serial_console_to_sp:
        tokio::sync::Mutex<HashMap<SpComponent, SerialConsolePacketizer>>,
}

#[derive(Debug, Default)]
struct OutstandingRequests {
    // map of request ID -> receiving oneshot channel
    requests: Mutex<HashMap<u32, Sender<Result<ResponseKind, ResponseError>>>>,
}

impl OutstandingRequests {
    fn insert(self: &Arc<Self>, request_id: u32) -> ResponseReceiver {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().unwrap().insert(request_id, tx);

        ResponseReceiver {
            parent: Arc::clone(self),
            request_id,
            rx,
            removed_from_parent: false,
        }
    }

    fn remove(
        &self,
        request_id: u32,
    ) -> Option<Sender<Result<ResponseKind, ResponseError>>> {
        self.requests.lock().unwrap().remove(&request_id)
    }
}

// Wrapper around a tokio oneshot receiver that removes itself from its parent
// `OutstandingRequests` either when the message is received (happy path) or
// we're dropped (due to either a timeout or some other kind of
// error/cancellation)
struct ResponseReceiver {
    parent: Arc<OutstandingRequests>,
    request_id: u32,
    rx: Receiver<Result<ResponseKind, ResponseError>>,
    removed_from_parent: bool,
}

impl Drop for ResponseReceiver {
    fn drop(&mut self) {
        self.remove_from_parent();
    }
}

impl ResponseReceiver {
    async fn recv(
        mut self,
    ) -> Result<Result<ResponseKind, ResponseError>, RecvError> {
        let result = (&mut self.rx).await;
        self.remove_from_parent();
        result
    }

    fn remove_from_parent(&mut self) {
        // we're unconditionally called from our `Drop` impl, but if we already
        // successfully received a response we already removed ourselves
        if self.removed_from_parent {
            return;
        }

        self.parent.requests.lock().unwrap().remove(&self.request_id);
        self.removed_from_parent = true;
    }
}
