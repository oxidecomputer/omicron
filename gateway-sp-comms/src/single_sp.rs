// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Interface for communicating with a single SP.

use crate::communicator::ResponseKindExt;
use crate::error::BadResponseType;
use crate::error::SpCommunicationError;
use crate::error::UpdateError;
use gateway_messages::sp_impl;
use gateway_messages::version;
use gateway_messages::BulkIgnitionState;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::Request;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SpComponent;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use gateway_messages::SpPort;
use gateway_messages::SpState;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdatePrepare;
use gateway_messages::UpdatePrepareStatusRequest;
use omicron_common::backoff;
use omicron_common::backoff::Backoff;
use rand::Rng;
use slog::debug;
use slog::error;
use slog::info;
use slog::trace;
use slog::warn;
use slog::Logger;
use std::convert::TryInto;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::timeout;

pub const DISCOVERY_MULTICAST_ADDR: Ipv6Addr =
    Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0x1de, 0);

// Once we've discovered an SP, continue to send discovery packets on this
// interval to detect changes.
//
// TODO-correctness/TODO-security What do we do if the SP address changes?
const DISCOVERY_INTERVAL_IDLE: Duration = Duration::from_secs(60);

type Result<T, E = SpCommunicationError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SingleSp {
    cmds_tx: mpsc::Sender<InnerCommand>,
    sp_addr_rx: watch::Receiver<Option<(SocketAddrV6, SpPort)>>,
    inner_task: JoinHandle<()>,
    log: Logger,
}

impl Drop for SingleSp {
    fn drop(&mut self) {
        self.inner_task.abort();
    }
}

impl SingleSp {
    /// Construct a new `SingleSp` that will periodically attempt to discover an
    /// SP reachable at `discovery_addr` (typically
    /// [`DISCOVERY_MULTICAST_ADDR`], but possibly different in test /
    /// development setups).
    pub fn new(
        socket: UdpSocket,
        discovery_addr: SocketAddrV6,
        max_attempts: usize,
        per_attempt_timeout: Duration,
        log: Logger,
    ) -> Self {
        // SPs don't support pipelining, so any command we send to `Inner` that
        // involves contacting an SP will effectively block until it completes.
        // We use a more-or-less arbitrary chanel size of 8 here to allow (a)
        // non-SP commands (e.g., detaching the serial console) and (b) a small
        // number of enqueued SP commands to be submitted without blocking the
        // caller.
        let (cmds_tx, cmds_rx) = mpsc::channel(8);
        let (sp_addr_tx, sp_addr_rx) = watch::channel(None);
        let inner = Inner::new(
            log.clone(),
            socket,
            sp_addr_tx,
            discovery_addr,
            max_attempts,
            per_attempt_timeout,
            cmds_rx,
        );
        let inner_task = tokio::spawn(inner.run());

        Self { cmds_tx, sp_addr_rx, inner_task, log }
    }

    /// Retrieve the [`watch::Receiver`] for notifications of discovery of an
    /// SP's address.
    pub fn sp_addr_watch(
        &self,
    ) -> &watch::Receiver<Option<(SocketAddrV6, SpPort)>> {
        &self.sp_addr_rx
    }

    /// Request the state of an ignition target.
    ///
    /// This will fail if this SP is not connected to an ignition controller.
    pub async fn ignition_state(&self, target: u8) -> Result<IgnitionState> {
        self.rpc(RequestKind::IgnitionState { target }).await.and_then(
            |(_peer, response)| {
                response.expect_ignition_state().map_err(Into::into)
            },
        )
    }

    /// Request the state of all ignition targets.
    ///
    /// This will fail if this SP is not connected to an ignition controller.
    pub async fn bulk_ignition_state(&self) -> Result<BulkIgnitionState> {
        self.rpc(RequestKind::BulkIgnitionState).await.and_then(
            |(_peer, response)| {
                response.expect_bulk_ignition_state().map_err(Into::into)
            },
        )
    }

    /// Send an ignition command to the given target.
    ///
    /// This will fail if this SP is not connected to an ignition controller.
    pub async fn ignition_command(
        &self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<()> {
        self.rpc(RequestKind::IgnitionCommand { target, command })
            .await
            .and_then(|(_peer, response)| {
                response.expect_ignition_command_ack().map_err(Into::into)
            })
    }

    /// Request the state of the SP.
    pub async fn state(&self) -> Result<SpState> {
        self.rpc(RequestKind::SpState).await.and_then(|(_peer, response)| {
            response.expect_sp_state().map_err(Into::into)
        })
    }

    /// Update a component of the SP (or the SP itself!).
    ///
    /// This is a bulk operation that will make multiple RPC calls to the SP to
    /// deliver all of `image`.
    ///
    /// # Panics
    ///
    /// Panics if `image.is_empty()`.
    pub async fn update(
        &self,
        component: SpComponent,
        slot: u16,
        image: Vec<u8>,
    ) -> Result<(), UpdateError> {
        assert!(!image.is_empty());
        let total_size = image
            .len()
            .try_into()
            .map_err(|_err| UpdateError::ImageTooLarge)?;

        let stream_id: u64 = rand::thread_rng().gen();

        info!(
            self.log, "starting update";
            "component" => component.as_str(),
            "stream_id" => stream_id,
            "total_size" => total_size,
        );
        self.update_prepare(component, stream_id, slot, total_size).await?;

        // Wait until the SP finishes whatever prep work it needs to do.
        self.poll_for_update_prepare_status_done(component, stream_id).await?;
        info!(
            self.log, "SP update preparation complete";
            "stream_id" => stream_id,
        );

        let mut image = Cursor::new(image);
        let mut offset = 0;
        while !CursorExt::is_empty(&image) {
            let prior_pos = image.position();
            debug!(
                self.log, "sending update chunk";
                "stream_id" => stream_id,
                "offset" => offset,
            );

            image =
                self.update_chunk(component, stream_id, offset, image).await?;

            // Update our offset according to how far our cursor advanced.
            offset += (image.position() - prior_pos) as u32;
        }
        info!(self.log, "update complete");

        Ok(())
    }

    /// Instruct the SP to begin the update process.
    ///
    /// This should be followed by a series of `update_chunk()` calls totalling
    /// `total_size` bytes of data.
    async fn update_prepare(
        &self,
        component: SpComponent,
        stream_id: u64,
        slot: u16,
        total_size: u32,
    ) -> Result<()> {
        self.rpc(RequestKind::UpdatePrepare(UpdatePrepare {
            component,
            stream_id,
            slot,
            total_size,
        }))
        .await
        .and_then(|(_peer, response)| {
            response.expect_update_prepare_ack().map_err(Into::into)
        })
    }

    /// Send a steady stream of "update prepare status" messages until the SP
    /// indicates update prepartion is complete.
    async fn poll_for_update_prepare_status_done(
        &self,
        component: SpComponent,
        stream_id: u64,
    ) -> Result<()> {
        // We expect most update prepartion to be fast, but for some components
        // (e.g., host boot flash), it can take up to several minutes to prepare
        // the target flash slot. Send a few status requests with a short
        // polling interval, then back off to polling every few seconds.
        //
        // These choses of durations/number of attempts are all relatively
        // arbitrary and could be tuned over time or for specific components.
        const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(200);
        const INITIAL_POLL_ATTEMPTS: usize = 5;
        const SLOW_POLL_INTERVAL: Duration = Duration::from_secs(3);

        let request =
            RequestKind::UpdatePrepareStatus(UpdatePrepareStatusRequest {
                component,
                stream_id,
            });

        for attempt in 1.. {
            let status =
                self.rpc(request).await.and_then(|(_peer, response)| {
                    response.expect_update_prepare_status().map_err(Into::into)
                })?;
            if status.done {
                return Ok(());
            }
            let sleep_time = if attempt <= INITIAL_POLL_ATTEMPTS {
                INITIAL_POLL_INTERVAL
            } else {
                SLOW_POLL_INTERVAL
            };
            tokio::time::sleep(sleep_time).await;
        }

        // We can't break out of the for loop without retrying usize::MAX times,
        // which isn't phsyically possible (but the compiler doesn't know that).
        unreachable!();
    }

    /// Send a portion of an update to the SP.
    ///
    /// Must be preceded by a call to `update_start()` (and may be preceded by
    /// earlier chunks of this update)`.
    ///
    /// The completion of an update is implicit, and is detected by the SP based
    /// on size of the update (specified by the `total_size` given when the
    /// update starts).
    ///
    /// Panics if `chunk.len() > UpdateChunk::MAX_CHUNK_SIZE`.
    async fn update_chunk(
        &self,
        component: SpComponent,
        stream_id: u64,
        offset: u32,
        data: Cursor<Vec<u8>>,
    ) -> Result<Cursor<Vec<u8>>> {
        let update_chunk = UpdateChunk { component, stream_id, offset };
        let (result, data) = self
            .rpc_with_trailing_data(
                RequestKind::UpdateChunk(update_chunk),
                data,
            )
            .await;

        result.and_then(|(_peer, response)| {
            response.expect_update_chunk_ack().map_err(Into::into)
        })?;

        Ok(data)
    }

    /// Abort an in-progress update.
    pub async fn update_abort(&self, component: SpComponent) -> Result<()> {
        // RequestKind::UpdateAbort does not take a `stream_id` argument,
        // because this command is designed to allow aborts to in-progress
        // updates that may have been started by other MGS instances.
        self.rpc(RequestKind::UpdateAbort(component)).await.and_then(
            |(_peer, response)| {
                response.expect_update_abort_ack().map_err(Into::into)
            },
        )
    }

    /// Instruct the SP that a reset trigger will be coming.
    ///
    /// This is part of a two-phase reset process. MGS should set a
    /// `reset_prepare()` followed by `reset_trigger()`. Internally,
    /// `reset_trigger()` continues to send the reset trigger message until the
    /// SP responds with an error that it wasn't expecting it, at which point we
    /// assume a reset has happened. In critical situations (e.g., updates),
    /// callers should verify through a separate channel that the operation they
    /// needed the reset for has happened (e.g., checking the SP's version, in
    /// the case of updates).
    pub async fn reset_prepare(&self) -> Result<()> {
        self.rpc(RequestKind::SysResetPrepare).await.and_then(
            |(_peer, response)| {
                response.expect_sys_reset_prepare_ack().map_err(Into::into)
            },
        )
    }

    /// Instruct the SP to reset.
    ///
    /// Only valid after a successful call to `reset_prepare()`.
    pub async fn reset_trigger(&self) -> Result<()> {
        // Reset trigger should retry until we get back an error indicating the
        // SP wasn't expecting a reset trigger (because it has reset!).
        match self.rpc(RequestKind::SysResetTrigger).await {
            Ok((_peer, response)) => {
                Err(SpCommunicationError::BadResponseType(BadResponseType {
                    expected: "system-reset",
                    got: response.name(),
                }))
            }
            Err(SpCommunicationError::SpError(
                ResponseError::SysResetTriggerWithoutPrepare,
            )) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// "Attach" to the serial console, setting up a tokio channel for all
    /// incoming serial console packets from the SP.
    pub async fn serial_console_attach(
        &self,
        component: SpComponent,
    ) -> Result<AttachedSerialConsole> {
        let (tx, rx) = oneshot::channel();

        // `Inner::run()` doesn't exit until we are dropped, so unwrapping here
        // only panics if it itself panicked.
        self.cmds_tx
            .send(InnerCommand::SerialConsoleAttach(component, tx))
            .await
            .unwrap();

        let attachment = rx.await.unwrap()?;

        Ok(AttachedSerialConsole {
            key: attachment.key,
            rx: attachment.incoming,
            inner_tx: self.cmds_tx.clone(),
            log: self.log.clone(),
        })
    }

    /// Detach any existing attached serial console connection.
    pub async fn serial_console_detach(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        // `Inner::run()` doesn't exit until we are dropped, so unwrapping here
        // only panics if it itself panicked.
        self.cmds_tx
            .send(InnerCommand::SerialConsoleDetach(None, tx))
            .await
            .unwrap();

        rx.await.unwrap()
    }

    pub(crate) async fn rpc(
        &self,
        kind: RequestKind,
    ) -> Result<(SocketAddrV6, ResponseKind)> {
        rpc(&self.cmds_tx, kind, None).await.result
    }

    async fn rpc_with_trailing_data(
        &self,
        kind: RequestKind,
        trailing_data: Cursor<Vec<u8>>,
    ) -> (Result<(SocketAddrV6, ResponseKind)>, Cursor<Vec<u8>>) {
        rpc_with_trailing_data(&self.cmds_tx, kind, trailing_data).await
    }
}

async fn rpc_with_trailing_data(
    inner_tx: &mpsc::Sender<InnerCommand>,
    kind: RequestKind,
    trailing_data: Cursor<Vec<u8>>,
) -> (Result<(SocketAddrV6, ResponseKind)>, Cursor<Vec<u8>>) {
    let RpcResponse { result, trailing_data } =
        rpc(inner_tx, kind, Some(trailing_data)).await;

    // We sent `Some(_)` trailing data, so we get `Some(_)` back; unwrap it
    // so our caller can remain ignorant of this detail.
    (result, trailing_data.unwrap())
}

async fn rpc(
    inner_tx: &mpsc::Sender<InnerCommand>,
    kind: RequestKind,
    trailing_data: Option<Cursor<Vec<u8>>>,
) -> RpcResponse {
    let (resp_tx, resp_rx) = oneshot::channel();

    // `Inner::run()` doesn't exit as long as `inner_tx` exists, so unwrapping
    // here only panics if it itself panicked.
    inner_tx
        .send(InnerCommand::Rpc(RpcRequest {
            kind,
            trailing_data,
            response_tx: resp_tx,
        }))
        .await
        .unwrap();

    resp_rx.await.unwrap()
}

#[derive(Debug)]
pub struct AttachedSerialConsole {
    key: u64,
    rx: mpsc::Receiver<(u64, Vec<u8>)>,
    inner_tx: mpsc::Sender<InnerCommand>,
    log: Logger,
}

impl AttachedSerialConsole {
    pub fn split(
        self,
    ) -> (AttachedSerialConsoleSend, AttachedSerialConsoleRecv) {
        (
            AttachedSerialConsoleSend {
                key: self.key,
                tx_offset: 0,
                inner_tx: self.inner_tx,
            },
            AttachedSerialConsoleRecv {
                rx_offset: 0,
                rx: self.rx,
                log: self.log,
            },
        )
    }
}

#[derive(Debug)]
pub struct AttachedSerialConsoleSend {
    key: u64,
    tx_offset: u64,
    inner_tx: mpsc::Sender<InnerCommand>,
}

impl AttachedSerialConsoleSend {
    /// Write `data` to the serial console of the SP.
    pub async fn write(&mut self, data: Vec<u8>) -> Result<()> {
        let mut data = Cursor::new(data);
        let mut remaining_data = CursorExt::remaining_slice(&data).len();
        while remaining_data > 0 {
            let (result, new_data) = rpc_with_trailing_data(
                &self.inner_tx,
                RequestKind::SerialConsoleWrite { offset: self.tx_offset },
                data,
            )
            .await;

            let data_sent = (remaining_data
                - CursorExt::remaining_slice(&new_data).len())
                as u64;

            let n = result.and_then(|(_peer, response)| {
                response.expect_serial_console_write_ack().map_err(Into::into)
            })?;

            // Confirm the ack we got back makes sense; its `n` should be in the
            // range `[self.tx_offset..self.tx_offset + data_sent]`.
            if n < self.tx_offset {
                return Err(SpCommunicationError::BogusSerialConsoleState);
            }
            let bytes_accepted = n - self.tx_offset;
            if bytes_accepted > data_sent {
                return Err(SpCommunicationError::BogusSerialConsoleState);
            }

            data = new_data;

            // If the SP only accepted part of the data we sent, we need to
            // rewind our cursor and resend what it couldn't accept.
            if bytes_accepted < data_sent {
                let rewind = data_sent - bytes_accepted;
                data.seek(SeekFrom::Current(-(rewind as i64))).unwrap();
            }

            self.tx_offset += bytes_accepted;
            remaining_data = CursorExt::remaining_slice(&data).len();
        }
        Ok(())
    }

    /// Detach this serial console connection.
    pub async fn detach(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.inner_tx
            .send(InnerCommand::SerialConsoleDetach(Some(self.key), tx))
            .await
            .unwrap();

        rx.await.unwrap()
    }
}

#[derive(Debug)]
pub struct AttachedSerialConsoleRecv {
    rx_offset: u64,
    rx: mpsc::Receiver<(u64, Vec<u8>)>,
    log: Logger,
}

impl AttachedSerialConsoleRecv {
    /// Receive a `SerialConsole` packet from the SP.
    ///
    /// Returns `None` if the underlying channel has been closed (e.g., if the
    /// serial console has been detached).
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        let (offset, data) = self.rx.recv().await?;
        if offset != self.rx_offset {
            warn!(
                self.log,
                "gap in serial console data (dropped packet or buffer overrun)",
            );
        }
        self.rx_offset = offset + data.len() as u64;
        Some(data)
    }
}

// All RPC request/responses are handled by message passing to the `Inner` task
// below. `trailing_data` deserves some extra documentation: Some packet types
// (e.g., update chunks) want to send potentially-large binary data. We
// serialize this data with `gateway_messages::serialize_with_trailing_data()`,
// which appends as much data as will fit after the message header, but the
// caller doesn't know how much data that is until serialization happens. To
// handle this, we traffic in `Cursor<Vec<u8>>`s for communicating trailing data
// to `Inner`. If `trailing_data` in the `RpcRequest` is `Some(_)`, it will
// always be returned as `Some(_)` in the response as well, and the cursor will
// have been advanced by however much data was packed into the single RPC packet
// exchanged with the SP.
#[derive(Debug)]
struct RpcRequest {
    kind: RequestKind,
    trailing_data: Option<Cursor<Vec<u8>>>,
    response_tx: oneshot::Sender<RpcResponse>,
}

#[derive(Debug)]
struct RpcResponse {
    result: Result<(SocketAddrV6, ResponseKind)>,
    trailing_data: Option<Cursor<Vec<u8>>>,
}

#[derive(Debug)]
struct SerialConsoleAttachment {
    key: u64,
    incoming: mpsc::Receiver<(u64, Vec<u8>)>,
}

#[derive(Debug)]
// `Rpc` is the large variant, which is by far the most common, so silence
// clippy's warning that recommends boxing it.
#[allow(clippy::large_enum_variant)]
enum InnerCommand {
    Rpc(RpcRequest),
    SerialConsoleAttach(
        SpComponent,
        oneshot::Sender<Result<SerialConsoleAttachment>>,
    ),
    // The associated value is the connection key; if `Some(_)`, only detach if
    // the currently-attached key number matches. If `None`, detach any current
    // connection. These correspond to "detach the current session" (performed
    // automatically when a connection is closed) and "force-detach any session"
    // (performed by a user).
    SerialConsoleDetach(Option<u64>, oneshot::Sender<Result<()>>),
}

struct Inner {
    log: Logger,
    socket: UdpSocket,
    sp_addr_tx: watch::Sender<Option<(SocketAddrV6, SpPort)>>,
    discovery_addr: SocketAddrV6,
    max_attempts: usize,
    per_attempt_timeout: Duration,
    serial_console_tx: Option<mpsc::Sender<(u64, Vec<u8>)>>,
    cmds_rx: mpsc::Receiver<InnerCommand>,
    request_id: u32,
    serial_console_connection_key: u64,
}

impl Inner {
    fn new(
        log: Logger,
        socket: UdpSocket,
        sp_addr_tx: watch::Sender<Option<(SocketAddrV6, SpPort)>>,
        discovery_addr: SocketAddrV6,
        max_attempts: usize,
        per_attempt_timeout: Duration,
        cmds_rx: mpsc::Receiver<InnerCommand>,
    ) -> Self {
        Self {
            log,
            socket,
            sp_addr_tx,
            discovery_addr,
            max_attempts,
            per_attempt_timeout,
            serial_console_tx: None,
            cmds_rx,
            request_id: 0,
            serial_console_connection_key: 0,
        }
    }

    async fn run(mut self) {
        let mut incoming_buf = [0; gateway_messages::MAX_SERIALIZED_SIZE];

        let maybe_known_addr = *self.sp_addr_tx.borrow();
        let mut sp_addr = match maybe_known_addr {
            Some((addr, _port)) => addr,
            None => {
                // We can't do anything useful until we find an SP; loop
                // discovery packets first.
                debug!(
                    self.log, "attempting SP discovery";
                    "discovery_addr" => %self.discovery_addr,
                );
                loop {
                    match self.discover(&mut incoming_buf).await {
                        Ok(addr) => {
                            debug!(self.log, "SP discovered"; "addr" => %addr);
                            break addr;
                        }
                        Err(err) => {
                            debug!(self.log, "discovery failed"; "err" => %err);
                            continue;
                        }
                    }
                }
            }
        };

        let mut discovery_idle = time::interval(DISCOVERY_INTERVAL_IDLE);

        loop {
            tokio::select! {
                cmd = self.cmds_rx.recv() => {
                    let cmd = match cmd {
                        Some(cmd) => cmd,
                        None => return,
                    };

                    self.handle_command(sp_addr, cmd, &mut incoming_buf).await;
                    discovery_idle.reset();
                }

                result = recv(&self.socket, &mut incoming_buf, &self.log) => {
                    self.handle_incoming_message(result);
                    discovery_idle.reset();
                }

                _ = discovery_idle.tick() => {
                    debug!(
                        self.log, "attempting SP discovery (idle timeout)";
                        "discovery_addr" => %self.discovery_addr,
                    );
                    match self.discover(&mut incoming_buf).await {
                        Ok(addr) => {
                            if sp_addr != addr {
                                warn!(
                                    self.log, "discovered new SP";
                                    "new_addr" => %addr,
                                    "old_addr" => %sp_addr,
                                );
                            }
                            sp_addr = addr;
                        }
                        Err(err) => {
                            warn!(
                                self.log, "idle discovery check failed";
                                "err" => %err,
                            );
                        }
                    }
                }
            }
        }
    }

    async fn discover(
        &mut self,
        incoming_buf: &mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    ) -> Result<SocketAddrV6> {
        let (addr, response) = self
            .rpc_call(
                self.discovery_addr,
                RequestKind::Discover,
                None,
                incoming_buf,
            )
            .await?;

        let discovery = response.expect_discover()?;

        // The receiving half of `sp_addr_tx` is held by the `SingleSp` that
        // created us, and it aborts our task when it's dropped. This send
        // therefore can't fail; ignore the returned result.
        let _ = self.sp_addr_tx.send(Some((addr, discovery.sp_port)));

        Ok(addr)
    }

    async fn handle_command(
        &mut self,
        sp_addr: SocketAddrV6,
        command: InnerCommand,
        incoming_buf: &mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    ) {
        match command {
            InnerCommand::Rpc(mut rpc) => {
                let result = self
                    .rpc_call(
                        sp_addr,
                        rpc.kind,
                        rpc.trailing_data.as_mut(),
                        incoming_buf,
                    )
                    .await;
                let response =
                    RpcResponse { result, trailing_data: rpc.trailing_data };

                if rpc.response_tx.send(response).is_err() {
                    warn!(
                        self.log,
                        "RPC requester disappeared while waiting for response"
                    );
                }
            }
            InnerCommand::SerialConsoleAttach(component, response_tx) => {
                let resp = self
                    .attach_serial_console(sp_addr, component, incoming_buf)
                    .await;
                response_tx.send(resp).unwrap();
            }
            InnerCommand::SerialConsoleDetach(key, response_tx) => {
                let resp = if key.is_none()
                    || key == Some(self.serial_console_connection_key)
                {
                    self.detach_serial_console(sp_addr, incoming_buf).await
                } else {
                    Ok(())
                };
                response_tx.send(resp).unwrap();
            }
        }
    }

    fn handle_incoming_message(
        &mut self,
        result: Result<(SocketAddrV6, SpMessage, &[u8])>,
    ) {
        let (peer, message, trailing_data) = match result {
            Ok((peer, message, trailing_data)) => {
                (peer, message, trailing_data)
            }
            Err(err) => {
                error!(
                    self.log,
                    "error processing incoming data (ignoring)";
                    "err" => %err,
                );
                return;
            }
        };

        // TODO-correctness / TODO-security What does it mean to receive a
        // message that doesn't match what we believe the SP's address is? For
        // now, we will log and drop it, but this needs work.
        if let Some(&(addr, _port)) = self.sp_addr_tx.borrow().as_ref() {
            if peer != addr {
                warn!(
                    self.log,
                    "ignoring message from unexpected IPv6 address";
                    "address" => %peer,
                    "sp_address" => %addr,
                );
                return;
            }
        }

        match message.kind {
            SpMessageKind::Response { request_id, result } => {
                warn!(
                    self.log,
                    "ignoring unexpected RPC response";
                    "request_id" => request_id,
                    "result" => ?result,
                );
            }
            SpMessageKind::SerialConsole { component, offset } => {
                self.forward_serial_console(component, offset, trailing_data);
            }
        }
    }

    async fn rpc_call(
        &mut self,
        addr: SocketAddrV6,
        kind: RequestKind,
        trailing_data: Option<&mut Cursor<Vec<u8>>>,
        incoming_buf: &mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    ) -> Result<(SocketAddrV6, ResponseKind)> {
        // Build and serialize our request once.
        self.request_id += 1;
        let request =
            Request { version: version::V1, request_id: self.request_id, kind };

        let mut outgoing_buf = [0; gateway_messages::MAX_SERIALIZED_SIZE];
        let n = match trailing_data {
            Some(data) => {
                let (n, written) =
                    gateway_messages::serialize_with_trailing_data(
                        &mut outgoing_buf,
                        &request,
                        &[CursorExt::remaining_slice(data)],
                    );
                // `data` is an in-memory cursor; seeking can only fail if we
                // provide a bogus offset, so it's safe to unwrap here.
                data.seek(SeekFrom::Current(written as i64)).unwrap();
                n
            }
            None => {
                // We know statically that `outgoing_buf` is large enough to
                // hold any `Request`, which in practice is the only possible
                // serialization error. Therefore, we can `.unwrap()`.
                gateway_messages::serialize(&mut outgoing_buf[..], &request)
                    .unwrap()
            }
        };
        let outgoing_buf = &outgoing_buf[..n];

        for attempt in 1..=self.max_attempts {
            trace!(
                self.log, "sending request to SP";
                "request" => ?request,
                "attempt" => attempt,
            );

            match self
                .rpc_call_one_attempt(
                    addr,
                    request.request_id,
                    outgoing_buf,
                    incoming_buf,
                )
                .await?
            {
                Some(result) => return Ok(result),
                None => continue,
            }
        }

        Err(SpCommunicationError::ExhaustedNumAttempts(self.max_attempts))
    }

    async fn rpc_call_one_attempt(
        &mut self,
        addr: SocketAddrV6,
        request_id: u32,
        serialized_request: &[u8],
        incoming_buf: &mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    ) -> Result<Option<(SocketAddrV6, ResponseKind)>> {
        // We consider an RPC attempt to be our attempt to contact the SP. It's
        // possible for the SP to respond and say it's busy; we shouldn't count
        // that as a failed UDP RPC attempt, so we loop within this "one
        // attempt" function to handle busy SP responses.
        let mut busy_sp_backoff = sp_busy_policy();

        loop {
            send(&self.socket, addr, serialized_request).await?;

            let result = match timeout(
                self.per_attempt_timeout,
                recv(&self.socket, incoming_buf, &self.log),
            )
            .await
            {
                Ok(result) => result,
                Err(_elapsed) => return Ok(None),
            };

            let (peer, response, trailing_data) = match result {
                Ok((peer, response, data)) => (peer, response, data),
                Err(err) => {
                    warn!(
                        self.log, "error receiving response";
                        "err" => %err,
                    );
                    return Ok(None);
                }
            };

            let result = match response.kind {
                SpMessageKind::Response { request_id: response_id, result } => {
                    if !trailing_data.is_empty() {
                        warn!(self.log, "received unexpected trailing data with response (discarding)");
                    }
                    if response_id == request_id {
                        result
                    } else {
                        debug!(
                            self.log, "ignoring unexpected response";
                            "id" => response_id,
                            "peer" => %peer,
                        );
                        return Ok(None);
                    }
                }
                SpMessageKind::SerialConsole { component, offset } => {
                    self.forward_serial_console(
                        component,
                        offset,
                        trailing_data,
                    );
                    continue;
                }
            };

            match result {
                Err(ResponseError::Busy) => {
                    // Our SP busy policy never gives up, so we can unwrap.
                    let backoff_sleep = busy_sp_backoff.next_backoff().unwrap();
                    time::sleep(backoff_sleep).await;
                    continue;
                }
                other => return Ok(Some((peer, other?))),
            }
        }
    }

    fn forward_serial_console(
        &mut self,
        _component: SpComponent,
        offset: u64,
        data: &[u8],
    ) {
        // TODO-cleanup component support for serial console is half baked;
        // should we check here that it matches the attached serial console? For
        // the foreseeable future we only support one component, so we skip that
        // for now.

        if let Some(tx) = self.serial_console_tx.as_ref() {
            match tx.try_send((offset, data.to_vec())) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.serial_console_tx = None;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    error!(
                        self.log,
                        "discarding SP serial console data (buffer full)"
                    );
                    return;
                }
            }
        }
        warn!(self.log, "discarding SP serial console data (no receiver)");
    }

    async fn attach_serial_console(
        &mut self,
        sp_addr: SocketAddrV6,
        component: SpComponent,
        incoming_buf: &mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    ) -> Result<SerialConsoleAttachment> {
        // When a caller attaches to the SP's serial console, we return an
        // `mpsc::Receiver<_>` on which we send any packets received from the
        // SP. We have to pick a depth for that channel, and given we're not
        // able to apply backpressure to the SP / host sending the data, we
        // choose to drop data if the channel fills. We want something large
        // enough that hiccups in the receiver doesn't cause data loss, but
        // small enough that if the receiver stops handling messages we don't
        // eat a bunch of memory buffering up console data. We'll take a WAG and
        // pick a depth of 32 for now.
        const SERIAL_CONSOLE_CHANNEL_DEPTH: usize = 32;

        if self.serial_console_tx.is_some() {
            // Returning an `SpError` here is a little suspect since we didn't
            // actually talk to an SP, but we already know we're attached to it.
            // If we asked it to attach again, it would send back this error.
            return Err(SpCommunicationError::SpError(
                ResponseError::SerialConsoleAlreadyAttached,
            ));
        }

        let (_peer, response) = self
            .rpc_call(
                sp_addr,
                RequestKind::SerialConsoleAttach(component),
                None,
                incoming_buf,
            )
            .await?;
        response.expect_serial_console_attach_ack()?;

        let (tx, rx) = mpsc::channel(SERIAL_CONSOLE_CHANNEL_DEPTH);
        self.serial_console_tx = Some(tx);
        self.serial_console_connection_key += 1;
        Ok(SerialConsoleAttachment {
            key: self.serial_console_connection_key,
            incoming: rx,
        })
    }

    async fn detach_serial_console(
        &mut self,
        sp_addr: SocketAddrV6,
        incoming_buf: &mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    ) -> Result<()> {
        let (_peer, response) = self
            .rpc_call(
                sp_addr,
                RequestKind::SerialConsoleDetach,
                None,
                incoming_buf,
            )
            .await?;
        response.expect_serial_console_detach_ack()?;
        self.serial_console_tx = None;
        Ok(())
    }
}

async fn send(
    socket: &UdpSocket,
    addr: SocketAddrV6,
    data: &[u8],
) -> Result<()> {
    let n = socket
        .send_to(data, addr)
        .await
        .map_err(|err| SpCommunicationError::UdpSendTo { addr, err })?;

    // `send_to` should never write a partial packet; this is UDP.
    assert_eq!(data.len(), n, "partial UDP packet sent to {}?!", addr);

    Ok(())
}

async fn recv<'a>(
    socket: &UdpSocket,
    incoming_buf: &'a mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    log: &Logger,
) -> Result<(SocketAddrV6, SpMessage, &'a [u8])> {
    let (n, peer) = socket
        .recv_from(&mut incoming_buf[..])
        .await
        .map_err(SpCommunicationError::UdpRecv)?;

    probes::recv_packet!(|| {
        (peer, incoming_buf.as_ptr() as usize as u64, n as u64)
    });

    let peer = match peer {
        SocketAddr::V6(addr) => addr,
        SocketAddr::V4(_) => {
            // We're exclusively using IPv6; we can't get a response from an
            // IPv4 peer.
            unreachable!()
        }
    };

    let (message, leftover) =
        gateway_messages::deserialize::<SpMessage>(&incoming_buf[..n])
            .map_err(|err| SpCommunicationError::Deserialize { peer, err })?;

    trace!(
        log, "received message from SP";
        "sp" => %peer,
        "message" => ?message,
    );

    let trailing_data = if leftover.is_empty() {
        &[]
    } else {
        sp_impl::unpack_trailing_data(leftover)
            .map_err(|err| SpCommunicationError::Deserialize { peer, err })?
    };

    Ok((peer, message, trailing_data))
}

fn sp_busy_policy() -> backoff::ExponentialBackoff {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(20);
    const MAX_INTERVAL: Duration = Duration::from_millis(1_000);

    backoff::ExponentialBackoff {
        current_interval: INITIAL_INTERVAL,
        initial_interval: INITIAL_INTERVAL,
        multiplier: 2.0,
        max_interval: MAX_INTERVAL,
        max_elapsed_time: None,
        ..Default::default()
    }
}

// Helper trait to provide methods on `io::Cursor` that are currently unstable.
trait CursorExt {
    fn is_empty(&self) -> bool;
    fn remaining_slice(&self) -> &[u8];
}

impl CursorExt for Cursor<Vec<u8>> {
    fn is_empty(&self) -> bool {
        self.position() as usize >= self.get_ref().len()
    }

    fn remaining_slice(&self) -> &[u8] {
        let data = self.get_ref();
        let pos = usize::min(self.position() as usize, data.len());
        &data[pos..]
    }
}

#[usdt::provider(provider = "gateway_sp_comms")]
mod probes {
    fn recv_packet(
        _source: &SocketAddr,
        _data: u64, // TODO actually a `*const u8`, but that isn't allowed by usdt
        _len: u64,
    ) {
    }
}
