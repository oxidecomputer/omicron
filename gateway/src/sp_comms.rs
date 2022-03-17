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

use crate::config::KnownSps;
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
use tokio::net::UdpSocket;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("error binding to UDP address {addr}: {err}")]
    UdpBind { addr: SocketAddr, err: io::Error },
}

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
    #[error("no known SP at {0}")]
    SpDoesNotExist(SocketAddr),

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
    socket: Arc<UdpSocket>,
    known_sps: KnownSps,
    sp_state: Arc<AllSpState>,
    bulk_state_requests: OutstandingSpStateRequests,
    request_id: AtomicU32,
    recv_task: JoinHandle<()>,
}

impl Drop for SpCommunicator {
    fn drop(&mut self) {
        // default `JoinHandle` drop behavior is to detach; we want to kill our
        // recv task if we're dropped.
        self.recv_task.abort();
    }
}

impl SpCommunicator {
    pub async fn new(
        bind_addr: SocketAddr,
        known_sps: KnownSps,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        let socket =
            Arc::new(UdpSocket::bind(bind_addr).await.map_err(|err| {
                StartupError::UdpBind { addr: bind_addr, err }
            })?);
        let log = log.new(o!(
            "componennt" => "SpCommunicator",
            "local_addr" => bind_addr,
        ));

        // build our fixed-keys-after-this-point map of known SP IPs
        let sp_state = Arc::new(AllSpState::new(&known_sps));

        let recv_task = RecvTask::new(
            Arc::clone(&socket),
            Arc::clone(&sp_state),
            log.clone(),
        );
        let recv_task = tokio::spawn(recv_task.run());
        info!(&log, "started sp-server");
        Ok(Self {
            log,
            socket,
            known_sps,
            sp_state,
            bulk_state_requests: OutstandingSpStateRequests::default(),
            request_id: AtomicU32::new(0),
            recv_task,
        })
    }

    pub fn placeholder_known_sps(&self) -> &KnownSps {
        &self.known_sps
    }

    pub(crate) async fn state_get(
        &self,
        sp: SocketAddr,
        timeout: Instant,
    ) -> Result<SpState, Error> {
        tokio::time::timeout_at(timeout, self.state_get_impl(sp)).await?
    }

    pub(crate) fn bulk_state_start(
        self: &Arc<Self>,
        timeout: Instant,
        retain_grace_period: Duration,
        sps: impl Iterator<Item = (usize, IgnitionState, Option<SocketAddr>)>,
    ) -> SpStateRequestId {
        self.bulk_state_requests.start(timeout, retain_grace_period, sps, self)
    }

    pub(crate) async fn bulk_state_get_progress(
        &self,
        id: &SpStateRequestId,
        last_seen_target: Option<u8>,
        timeout: Duration,
        limit: usize,
    ) -> Result<BulkStateProgress, Error> {
        self.bulk_state_requests
            .get(id, last_seen_target, timeout, limit, &self.log)
            .await
    }

    async fn state_get_impl(&self, sp: SocketAddr) -> Result<SpState, Error> {
        match self.request_response(sp, RequestKind::SpState).await? {
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
        sp: SocketAddr,
        component: &SpComponent,
    ) -> Result<Option<SerialConsoleContents>, Error> {
        let sp =
            self.sp_state.all_sps.get(&sp).ok_or(Error::SpDoesNotExist(sp))?;
        Ok(sp.serial_console_from_sp.lock().unwrap().contents(component))
    }

    pub(crate) async fn serial_console_post(
        &self,
        sp: SocketAddr,
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
        sp: SocketAddr,
        component: SpComponent,
        data: &[u8],
    ) -> Result<(), Error> {
        let sp_state = self
            .sp_state
            .all_sps
            .get(&sp)
            .ok_or_else(|| Error::SpDoesNotExist(sp))?;

        let mut packetizers = sp_state.serial_console_to_sp.lock().await;
        let packetizer = packetizers
            .entry(component)
            .or_insert_with(|| SerialConsolePacketizer::new(component));

        for packet in packetizer.packetize(data) {
            let request = RequestKind::SerialConsoleWrite(packet);
            match self.request_response(sp, request).await? {
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
    ) -> Result<Vec<IgnitionState>, Error> {
        tokio::time::timeout(timeout, self.bulk_ignition_get_impl()).await?
    }

    async fn bulk_ignition_get_impl(
        &self,
    ) -> Result<Vec<IgnitionState>, Error> {
        // XXX We currently assume we know which ignition controller is our
        // local one, and only use it for ignition interactions.
        let controller = self.known_sps.ignition_controller;

        let request = RequestKind::BulkIgnitionState;

        match self.request_response(controller, request).await? {
            ResponseKind::BulkIgnitionState(state) => Ok(state.targets
                [..usize::from(state.num_targets)]
                .iter()
                .copied()
                .collect()),
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
        target: u8,
        timeout: Duration,
    ) -> Result<IgnitionState, Error> {
        tokio::time::timeout(timeout, self.ignition_get_impl(target)).await?
    }

    async fn ignition_get_impl(
        &self,
        target: u8,
    ) -> Result<IgnitionState, Error> {
        // XXX We currently assume we know which ignition controller is our
        // local one, and only use it for ignition interactions.
        let controller = self.known_sps.ignition_controller;

        let request = RequestKind::IgnitionState { target };

        match self.request_response(controller, request).await? {
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
        target: u8,
        timeout: Duration,
    ) -> Result<(), Error> {
        tokio::time::timeout(
            timeout,
            self.ignition_command(target, IgnitionCommand::PowerOn),
        )
        .await?
    }

    pub async fn ignition_power_off(
        &self,
        target: u8,
        timeout: Duration,
    ) -> Result<(), Error> {
        tokio::time::timeout(
            timeout,
            self.ignition_command(target, IgnitionCommand::PowerOff),
        )
        .await?
    }

    async fn ignition_command(
        &self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), Error> {
        // XXX We currently assume we know which ignition controller is our
        // local one, and only use it for ignition interactions.
        let controller = self.known_sps.ignition_controller;
        let request = RequestKind::IgnitionCommand { target, command };

        match self.request_response(controller, request).await? {
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
        sp: SocketAddr,
        request: RequestKind,
    ) -> Result<ResponseKind, Error> {
        // request IDs will eventually roll over; since we enforce timeouts
        // this should be a non-issue in practice. does this need testing?
        let request_id =
            self.request_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // tell our background receiver to expect a response to this request
        let response = self.sp_state.insert_expected_response(sp, request_id);

        // Serialize and send our request. We know `buf` is large enough for any
        // `Request`, so unwrapping here is fine.
        let request =
            Request { version: version::V1, request_id, kind: request };
        let mut buf = [0; Request::MAX_SIZE];
        let n = gateway_messages::serialize(&mut buf, &request).unwrap();

        let serialized_request = &buf[..n];
        debug!(&self.log, "sending {:?} to SP {}", request, sp);
        self.socket
            .send_to(serialized_request, sp)
            .await
            .map_err(|err| Error::UdpSend { addr: sp, err: Arc::new(err) })?;

        // recv() can only fail if the sender is dropped, but we're holding it
        // in `self.outstanding_requests`; unwrap() is fine.
        Ok(response.recv().await.unwrap()?)
    }
}

/// Handle for the background tokio task responsible for receiving incoming UDP
/// messages.
///
/// TODO We currently assume that we know (before this task is spawned) the IP
/// address of all SPs with which we want to communicate, and that those IP
/// addresses will not change while we're running. These assumptions are wrong -
/// hot swapping sleds will lead to both being violated.
///
/// This task is spawned when `SpCommunicator` is created, and runs until it is
/// dropped. When the communicator wants to send a request on behalf of an HTTP
/// request:
///
/// 1. `SpCommunicator` creates a tokio oneshot channel for this task to use to
///    send the response.
/// 2. `SpCommunicator` inserts the sending half of that channel into
///    `outstanding_requests`, which is keyed by both the SP IP address and the
///    u32 ID attached to the request.
/// 3. `SpCommunicator` sends the UDP packet containing the request to the
///    target SP, and waits for a response on the channel it created in 1.
/// 4. When we receive a packet, we check:
///    a. Did it come from an IP address in our list of SP IPs?
///    b. Does it parse as a `Response`?
///    c. Is there a corresponding entry in `outstanding_requests` for this IP +
///    request ID?
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
struct RecvTask {
    socket: Arc<UdpSocket>,
    sp_state: Arc<AllSpState>,
    log: Logger,
}

impl RecvTask {
    fn new(
        socket: Arc<UdpSocket>,
        sp_state: Arc<AllSpState>,
        log: Logger,
    ) -> Self {
        Self { socket, sp_state, log }
    }

    async fn run(self) {
        let mut buf = [0; SpMessage::MAX_SIZE];
        loop {
            // raw recv
            let (n, addr) = match self.socket.recv_from(&mut buf).await {
                Ok((n, addr)) => (n, addr),
                Err(err) => {
                    error!(&self.log, "recv_from() failed: {}", err);
                    continue;
                }
            };
            debug!(&self.log, "received {} bytes from {}", n, addr);

            // parse into an `SpMessage`
            let sp_msg =
                match gateway_messages::deserialize::<SpMessage>(&buf[..n]) {
                    Ok((msg, _extra)) => {
                        // TODO should we check that `extra` is empty? if the
                        // response is maximal size any extra data is silently
                        // discarded anyway, so probably not?
                        msg
                    }
                    Err(err) => {
                        error!(
                            &self.log,
                            "discarding malformed message ({})", err
                        );
                        continue;
                    }
                };
            debug!(&self.log, "received {:?} from {}", sp_msg, addr);

            // `version` is intentionally the first 4 bytes of the packet; we
            // could check it before trying to deserialize?
            if sp_msg.version != version::V1 {
                error!(
                    &self.log,
                    "discarding message with unsupported version {}",
                    sp_msg.version
                );
                continue;
            }

            // decide whether this is a response to an outstanding request or an
            // unprompted message
            match sp_msg.kind {
                SpMessageKind::Response { request_id, result } => {
                    self.handle_response(addr, request_id, result);
                }
                SpMessageKind::SerialConsole(serial_console) => {
                    self.handle_serial_console(addr, serial_console);
                }
            }
        }
    }

    fn handle_response(
        &self,
        addr: SocketAddr,
        request_id: u32,
        result: Result<ResponseKind, ResponseError>,
    ) {
        // see if we know who to send the response to
        let tx = match self.sp_state.remove_expected_response(addr, request_id)
        {
            Some(tx) => tx,
            None => {
                error!(&self.log,
                        "discarding unexpected response {} from {} (possibly past timeout?)",
                        request_id,
                        addr,
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
                "discarding unexpected response {} from {} (receiver gone)",
                request_id,
                addr,
            );
        }
    }

    fn handle_serial_console(&self, addr: SocketAddr, packet: SerialConsole) {
        debug!(
            &self.log,
            "received serial console data from {}: {:?}", addr, packet
        );
        self.sp_state.push_serial_console(addr, packet, &self.log);
    }
}

#[derive(Debug)]
struct AllSpState {
    all_sps: HashMap<SocketAddr, SingleSpState>,
}

impl AllSpState {
    fn new(known_sps: &KnownSps) -> Self {
        let mut all_sps = HashMap::new();
        for sp_list in [
            &known_sps.switches,
            &known_sps.sleds,
            &known_sps.power_controllers,
        ] {
            for &sp in sp_list {
                all_sps.insert(sp, SingleSpState::default());
            }
        }
        Self { all_sps }
    }

    fn push_serial_console(
        &self,
        sp: SocketAddr,
        packet: SerialConsole,
        log: &Logger,
    ) {
        // caller should never try to send a request to an SP we don't know
        // about, since it created us with all SPs it knows.
        let state = self.all_sps.get(&sp).expect("nonexistent SP");
        state.serial_console_from_sp.lock().unwrap().push(packet, log);
    }

    fn insert_expected_response(
        &self,
        sp: SocketAddr,
        request_id: u32,
    ) -> ResponseReceiver {
        // caller should never try to send a request to an SP we don't know
        // about, since it created us with all SPs it knows.
        let state = self.all_sps.get(&sp).expect("nonexistent SP");
        state.outstanding_requests.insert(request_id)
    }

    fn remove_expected_response(
        &self,
        sp: SocketAddr,
        request_id: u32,
    ) -> Option<Sender<Result<ResponseKind, ResponseError>>> {
        // caller should never try to send a request to an SP we don't know
        // about, since it created us with all SPs it knows.
        let state = self.all_sps.get(&sp).expect("nonexistent SP");
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
