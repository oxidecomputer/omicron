// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::error::Error;
use crate::management_switch::SwitchPort;
use crate::Communicator;
use crate::Timeout;
use futures::future::Fuse;
use futures::Future;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use futures::TryFutureExt;
use gateway_messages::sp_impl::SerialConsolePacketizer;
use gateway_messages::version;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SerialConsole;
use gateway_messages::SpComponent;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use hyper::upgrade::OnUpgrade;
use hyper::upgrade::Upgraded;
use slog::debug;
use slog::error;
use slog::info;
use slog::trace;
use slog::warn;
use slog::Logger;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::WebSocketStream;

mod request_response_map;

use self::request_response_map::RequestResponseMap;
use self::request_response_map::ResponseIngestResult;

/// Handler for incoming packets received on management switch ports.
///
/// When [`RecvHandler::new()`] is created, it starts an indefinite tokio task
/// that calls [`RecvHandler::handle_incoming_packet()`] for each UDP packet
/// received. Those packets come in two flavors: responses to requests that we
/// sent to an SP, and unprompted messages generated by an SP. The flow for
/// request / response is:
///
/// 1. [`RecvHandler::register_request_id()`] is called with the 32-bit request
///    ID associated with the request. This returns a future that will be
///    fulfilled with the response once it arrives. This method should be called
///    after determining the request ID but before actually sending the request
///    to avoid a race window where the response could be received before the
///    receive handler task knows to expect it. Internally, this future holds
///    the receiving half of a [`tokio::oneshot`] channel.
/// 2. The request packet is sent to the SP.
/// 3. The requester `.await`s the future returned by `register_request_id()`.
/// 4. If the future is dropped before a response is received (e.g., due to a
///    timeout), dropping the future will unregister the request ID provided in
///    step 1 (see [`RequestResponseMap::wait_for_response()`] for details).
/// 5. Assuming the future has not been dropped when the response arrives,
///    [`RecvHandler::handle_incoming_packet()`] will look up the request ID and
///    send the response on the sending half of the [`tokio::oneshot`] channel
///    corresponding to the future returned in step 1, fulfilling it.
#[derive(Debug)]
pub(crate) struct RecvHandler {
    request_id: AtomicU32,
    sp_state: HashMap<SwitchPort, SingleSpState>,
    log: Logger,
}

impl RecvHandler {
    /// Create a new `RecvHandler` that is aware of all ports described by
    /// `switch`.
    pub(crate) fn new(
        ports: impl ExactSizeIterator<Item = SwitchPort>,
        log: Logger,
    ) -> Arc<Self> {
        // prime `sp_state` with all known ports of the switch
        let mut sp_state = HashMap::with_capacity(ports.len());
        for port in ports {
            sp_state.insert(port, SingleSpState::default());
        }

        // TODO: Should we init our request_id randomly instead of always
        // starting at 0?
        Arc::new(Self { request_id: AtomicU32::new(0), sp_state, log })
    }

    fn sp_state(&self, port: SwitchPort) -> &SingleSpState {
        // SwitchPort instances can only be created by `ManagementSwitch`, so we
        // should never be able to instantiate a port that we don't have in
        // `self.sp_state` (which we initialize with all ports declared by the
        // switch we were given).
        self.sp_state.get(&port).expect("invalid switch port")
    }

    /// Spawn a tokio task responsible for forwarding serial console data
    /// between the SP component on `port` and the websocket connection provided
    /// by `upgrade_fut`.
    pub(crate) fn serial_console_attach(
        &self,
        communicator: Arc<Communicator>,
        port: SwitchPort,
        component: SpComponent,
        sp_ack_timeout: Duration,
        upgrade_fut: OnUpgrade,
    ) -> Result<(), Error> {
        // lazy closure to spawn the task; called below _unless_ we already have
        // at attached task with this SP
        let spawn_task = move || {
            let (detach, detach_rx) = oneshot::channel();
            let (packets_from_sp, packets_from_sp_rx) =
                mpsc::unbounded_channel();
            let log = self.log.new(slog::o!(
                "port" => format!("{:?}", port),
                "component" => format!("{:?}", component),
            ));

            tokio::spawn(async move {
                let upgraded = match upgrade_fut.await {
                    Ok(u) => u,
                    Err(e) => {
                        error!(log, "serial task failed"; "err" => %e);
                        return;
                    }
                };
                let config = WebSocketConfig {
                    max_send_queue: Some(4096),
                    ..Default::default()
                };
                let ws_stream = WebSocketStream::from_raw_socket(
                    upgraded,
                    Role::Server,
                    Some(config),
                )
                .await;

                let task = SerialConsoleTask {
                    communicator,
                    port,
                    component,
                    detach: detach_rx,
                    packets: packets_from_sp_rx,
                    ws_stream,
                    sp_ack_timeout,
                };
                match task.run(&log).await {
                    Ok(()) => debug!(log, "serial task complete"),
                    Err(e) => {
                        error!(log, "serial task failed"; "err" => %e)
                    }
                }
            });

            SerialConsoleTaskHandle { packets_from_sp, detach }
        };

        let mut tasks =
            self.sp_state(port).serial_console_tasks.lock().unwrap();
        match tasks.entry(component) {
            Entry::Occupied(mut slot) => {
                if slot.get().is_attached() {
                    Err(Error::SerialConsoleAttached)
                } else {
                    // old task is already detached; replace it
                    let _ = slot.insert(spawn_task());
                    Ok(())
                }
            }
            Entry::Vacant(slot) => {
                slot.insert(spawn_task());
                Ok(())
            }
        }
    }

    /// Shut down the serial console task associated with the given port and
    /// component, if one exists and is attached.
    pub(crate) fn serial_console_detach(
        &self,
        port: SwitchPort,
        component: &SpComponent,
    ) -> Result<(), Error> {
        let mut tasks =
            self.sp_state(port).serial_console_tasks.lock().unwrap();
        if let Some(task) = tasks.remove(component) {
            // If send fails, we're already detached.
            let _ = task.detach.send(());
        }
        Ok(())
    }

    /// Returns a new request ID and a future that will complete when we receive
    /// a response on the given `port` with that request ID.
    ///
    /// Panics if `port` is not one of the ports defined by the `switch` given
    /// to this `RecvHandler` when it was constructed.
    pub(crate) fn register_request_id(
        &self,
        port: SwitchPort,
    ) -> (u32, impl Future<Output = Result<ResponseKind, ResponseError>> + '_)
    {
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        (request_id, self.sp_state(port).requests.wait_for_response(request_id))
    }

    /// Returns the address of the SP connected to `port`, if we know it.
    pub(crate) fn remote_addr(&self, port: SwitchPort) -> Option<SocketAddr> {
        *self.sp_state(port).addr.lock().unwrap()
    }

    // Only available for tests: set the remote address of a given port.
    #[cfg(test)]
    pub(crate) fn set_remote_addr(&self, port: SwitchPort, addr: SocketAddr) {
        *self.sp_state(port).addr.lock().unwrap() = Some(addr);
    }

    pub(crate) fn handle_incoming_packet(
        &self,
        port: SwitchPort,
        addr: SocketAddr,
        buf: &[u8],
    ) {
        trace!(&self.log, "received {} bytes from {:?}", buf.len(), port);

        // the first four bytes of packets we expect is always a version number;
        // check for that first
        //
        // TODO? We're (ab)using our knowledge of our packet wire format here -
        // knowledge contained in another crate - both in expecting that the
        // first four bytes are the version (perfectly reasonable) and that
        // they're stored in little endian (a bit less reasonable, but still
        // probably okay). We could consider moving this check into
        // `gateway_messages` itself?
        let version_raw = match buf.get(0..4) {
            Some(bytes) => u32::from_le_bytes(bytes.try_into().unwrap()),
            None => {
                error!(&self.log, "discarding too-short packet");
                return;
            }
        };
        match version_raw {
            version::V1 => (),
            _ => {
                error!(
                    &self.log,
                    "discarding message with unsupported version {}",
                    version_raw
                );
                return;
            }
        }

        // parse into an `SpMessage`
        let sp_msg = match gateway_messages::deserialize::<SpMessage>(buf) {
            Ok((msg, _extra)) => {
                // TODO should we check that `extra` is empty? if the
                // response is maximal size any extra data is silently
                // discarded anyway, so probably not?
                msg
            }
            Err(err) => {
                error!(
                    &self.log, "discarding malformed message";
                    "err" => %err,
                    "raw" => ?buf,
                );
                return;
            }
        };
        debug!(&self.log, "received {:?} from {:?}", sp_msg, port);

        // update our knowledge of the sender's address
        let state = self.sp_state(port);
        match state.addr.lock().unwrap().replace(addr) {
            None => {
                // expected but rare: our first packet on this port
                debug!(
                    &self.log, "discovered remote address for port";
                    "port" => ?port,
                    "addr" => %addr,
                );
            }
            Some(old) if old == addr => {
                // expected; we got another packet from the expected address
            }
            Some(old) => {
                // unexpected; the remote address changed
                // TODO-security - What should we do here? Could the sled have
                // been physically replaced and we're now hearing from a new SP?
                // This question/TODO may go away on its own if we add an
                // authenticated channel?
                warn!(
                    &self.log, "updated remote address for port";
                    "port" => ?port,
                    "old_addr" => %old,
                    "new_addr" => %addr,
                );
            }
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
        probes::recv_response!(|| (&port, request_id, &result));
        match self.sp_state(port).requests.ingest_response(&request_id, result)
        {
            ResponseIngestResult::Ok => (),
            ResponseIngestResult::UnknownRequestId => {
                error!(
                    &self.log,
                    "discarding unexpected response {} from {:?} (possibly past timeout?)",
                    request_id,
                    port,
                );
            }
        }
    }

    fn handle_serial_console(&self, port: SwitchPort, packet: SerialConsole) {
        probes::recv_serial_console!(|| (
            &port,
            &packet.component,
            packet.offset,
            packet.data.as_ptr() as usize as u64,
            u64::from(packet.len)
        ));

        let tasks = self.sp_state(port).serial_console_tasks.lock().unwrap();

        // if we have a task managing an active client connection, forward this
        // packet to it; otherwise, drop it on the floor.
        match tasks.get(&packet.component).map(|task| {
            let data = packet.data[..usize::from(packet.len)].to_vec();
            task.recv_data_from_sp(data)
        }) {
            Some(Ok(())) => {
                debug!(
                    self.log,
                    "forwarded serial console packet to attached client"
                );
            }

            // The only error we can get from `recv_data_from_sp()` is "the task
            // went away", which from our point of view is the same as it not
            // existing in the first place.
            Some(Err(_)) | None => {
                debug!(
                    self.log,
                    "discarding serial console packet (no attached client)"
                );
            }
        }
    }
}

#[derive(Debug, Default)]
struct SingleSpState {
    addr: Mutex<Option<SocketAddr>>,
    requests: RequestResponseMap<u32, Result<ResponseKind, ResponseError>>,
    serial_console_tasks: Mutex<HashMap<SpComponent, SerialConsoleTaskHandle>>,
}

#[derive(Debug)]
struct SerialConsoleTaskHandle {
    packets_from_sp: mpsc::UnboundedSender<Vec<u8>>,
    detach: oneshot::Sender<()>,
}

impl SerialConsoleTaskHandle {
    fn is_attached(&self) -> bool {
        !self.detach.is_closed()
    }
    fn recv_data_from_sp(
        &self,
        data: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<Vec<u8>>> {
        self.packets_from_sp.send(data)
    }
}

struct SerialConsoleTask {
    communicator: Arc<Communicator>,
    port: SwitchPort,
    component: SpComponent,
    detach: oneshot::Receiver<()>,
    packets: mpsc::UnboundedReceiver<Vec<u8>>,
    ws_stream: WebSocketStream<Upgraded>,
    sp_ack_timeout: Duration,
}

impl SerialConsoleTask {
    async fn run(mut self, log: &Logger) -> Result<(), SerialTaskError> {
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
        use tokio_tungstenite::tungstenite::Message;

        let (mut ws_sink, mut ws_stream) = self.ws_stream.split();

        // TODO Currently we do not apply any backpressure on the SP and are
        // willing to buffer up an arbitrary amount of data in memory. Is it
        // reasonable to apply backpressure to the SP over UDP? Should we have
        // caps on memory and start discarding data if we exceed them? We _do_
        // apply backpressure to the websocket, delaying reading from it if we
        // still have data waiting to be sent to the SP.
        let mut data_from_sp: VecDeque<Vec<u8>> = VecDeque::new();
        let mut data_to_sp: Vec<u8> = Vec::new();
        let mut packetizer_to_sp = SerialConsolePacketizer::new(self.component);

        loop {
            let ws_send = if let Some(data) = data_from_sp.pop_front() {
                ws_sink.send(Message::Binary(data)).fuse()
            } else {
                Fuse::terminated()
            };

            let ws_recv;
            let sp_send;
            if data_to_sp.is_empty() {
                sp_send = Fuse::terminated();
                ws_recv = ws_stream.next().fuse();
            } else {
                ws_recv = Fuse::terminated();

                let (packet, _remaining) =
                    packetizer_to_sp.first_packet(data_to_sp.as_slice());
                let packet_data_len = usize::from(packet.len);

                sp_send = self
                    .communicator
                    .serial_console_send_packet(
                        self.port,
                        packet,
                        Timeout::from_now(self.sp_ack_timeout),
                    )
                    .map_ok(move |()| packet_data_len)
                    .fuse();
            }

            tokio::select! {
                // Poll in the order written
                biased;

                // It's important we always poll the detach channel first
                // so that a constant stream of incoming/outgoing messages
                // don't cause us to ignore a detach
                _ = &mut self.detach => {
                    info!(log, "detaching from serial console");
                    let close = CloseFrame {
                        code: CloseCode::Policy,
                        reason: Cow::Borrowed("serial console was detached"),
                    };
                    ws_sink.send(Message::Close(Some(close))).await?;
                    break;
                }

                // Send a UDP packet to the SP
                send_success = sp_send => {
                    let n = send_success?;
                    data_to_sp.drain(..n);
                }

                // Receive a UDP packet from the SP.
                data = self.packets.recv() => {
                    // The sending half of `packets` is held by the
                    // `SerialConsoleTask` that created us. It is only dropped
                    // if we are detached; i.e., no longer running; therefore,
                    // we can safely unwrap this recv.
                    let data = data.expect("sending half dropped");
                    data_from_sp.push_back(data);
                }

                // Send a previously-received UDP packet of data to the websocket
                // client
                write_success = ws_send => {
                    write_success?;
                }

                // Receive from the websocket to send to the SP.
                msg = ws_recv => {
                    match msg {
                        Some(Ok(Message::Binary(mut data))) => {
                            // we only populate ws_recv when we have no data
                            // currently queued up; sanity check that here
                            assert!(data_to_sp.is_empty());
                            data_to_sp.append(&mut data);
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            info!(
                                log,
                                "remote end closed websocket; terminating task",
                            );
                            break;
                        }
                        Some(other) => {
                            let wrong_message = other?;
                            error!(
                                log,
                                "bogus websocket message; terminating task";
                                "message" => ?wrong_message,
                            );
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum SerialTaskError {
    #[error(transparent)]
    Error(#[from] Error),
    #[error(transparent)]
    TungsteniteError(#[from] tokio_tungstenite::tungstenite::Error),
}

#[usdt::provider(provider = "gateway_sp_comms")]
mod probes {
    fn recv_response(
        _port: &SwitchPort,
        _request_id: u32,
        _result: &Result<ResponseKind, ResponseError>,
    ) {
    }

    fn recv_serial_console(
        _port: &SwitchPort,
        _component: &SpComponent,
        _offset: u64,
        _data: u64, // TODO actually a `*const u8`, but that isn't allowed by usdt
        _len: u64,
    ) {
    }
}
