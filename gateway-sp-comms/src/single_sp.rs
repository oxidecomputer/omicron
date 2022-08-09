// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Interface for communicating with a single SP.

use gateway_messages::version;
use gateway_messages::Request;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SerialConsole;
use gateway_messages::SerializedSize;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use gateway_messages::SpPort;
use gateway_messages::SpState;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateStart;
use omicron_common::backoff;
use omicron_common::backoff::Backoff;
use slog::debug;
use slog::error;
use slog::info;
use slog::trace;
use slog::warn;
use slog::Logger;
use std::convert::TryInto;
use std::io;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::time::Duration;
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::timeout;

use crate::communicator::ResponseKindExt;
use crate::error::BadResponseType;

pub const DISCOVERY_MULTICAST_ADDR: Ipv6Addr =
    Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0x1de, 0);

// Once we've discovered an SP, continue to send discovery packets on this
// interval to detect changes.
//
// TODO-correctness/TODO-security What do we do if the SP address changes?
const DISCOVERY_INTERVAL_IDLE: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to send UDP packet to {addr}: {err}")]
    UdpSendTo { addr: SocketAddrV6, err: io::Error },
    #[error("failed to recv UDP packet: {0}")]
    UdpRecv(io::Error),
    #[error("failed to deserialize SP message from {peer}: {err}")]
    Deserialize { peer: SocketAddrV6, err: gateway_messages::HubpackError },
    #[error("RPC call failed (gave up after {0} attempts)")]
    ExhaustedNumAttempts(usize),
    #[error("serial console already attached")]
    SerialConsoleAlreadyAttached,
    #[error(transparent)]
    BadResponseType(#[from] BadResponseType),
    #[error("Error response from SP: {0}")]
    SpError(#[from] ResponseError),
}

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("update image is too large")]
    ImageTooLarge,
    #[error("error starting update: {0}")]
    Start(Error),
    #[error("error updating chunk at offset {offset}: {err}")]
    Chunk { offset: u32, err: Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SingleSp {
    inner_commands: mpsc::Sender<InnerCommand>,
    sp_addr: watch::Receiver<Option<(SocketAddrV6, SpPort)>>,
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
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (addr_tx, addr_rx) = watch::channel(None);
        let inner = Inner::new(
            log.clone(),
            socket,
            addr_tx,
            discovery_addr,
            max_attempts,
            per_attempt_timeout,
            cmd_rx,
        );
        let inner_task = tokio::spawn(inner.run());

        Self { inner_commands: cmd_tx, sp_addr: addr_rx, inner_task, log }
    }

    /// Retrieve the [`watch::Receiver`] for notifications of discovery of an
    /// SP's address.
    pub fn sp_addr_watch(
        &self,
    ) -> &watch::Receiver<Option<(SocketAddrV6, SpPort)>> {
        &self.sp_addr
    }

    /// Request the state of the SP.
    pub async fn state(&self) -> Result<SpState> {
        self.rpc(RequestKind::SpState).await.and_then(|(_peer, response)| {
            response.try_into_sp_state().map_err(Into::into)
        })
    }

    /// Update th SP.
    ///
    /// This is a bulk operation that will call [`Self::update_start()`]
    /// followed by [`Self::update_chunk()`] the necessary number of times.
    pub async fn update(&self, image: &[u8]) -> Result<(), UpdateError> {
        let total_size = image
            .len()
            .try_into()
            .map_err(|_err| UpdateError::ImageTooLarge)?;

        info!(self.log, "starting SP update"; "total_size" => total_size);
        self.update_start(total_size).await.map_err(UpdateError::Start)?;

        for (i, data) in image.chunks(UpdateChunk::MAX_CHUNK_SIZE).enumerate() {
            let offset = (i * UpdateChunk::MAX_CHUNK_SIZE) as u32;
            debug!(
                self.log, "sending update chunk";
                "offset" => offset,
                "size" => data.len(),
            );
            self.update_chunk(offset, data)
                .await
                .map_err(|err| UpdateError::Chunk { offset, err })?;
        }
        info!(self.log, "update complete");

        Ok(())
    }

    /// Instruct the SP to begin the update process.
    ///
    /// This should be followed by a series of `update_chunk()` calls totalling
    /// `total_size` bytes of data.
    pub async fn update_start(&self, total_size: u32) -> Result<()> {
        self.rpc(RequestKind::UpdateStart(UpdateStart { total_size }))
            .await
            .and_then(|(_peer, response)| {
                response.try_into_update_start_ack().map_err(Into::into)
            })
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
    pub async fn update_chunk(&self, offset: u32, chunk: &[u8]) -> Result<()> {
        assert!(chunk.len() <= UpdateChunk::MAX_CHUNK_SIZE);
        let mut update_chunk = UpdateChunk {
            offset,
            chunk_length: chunk.len() as u16,
            data: [0; UpdateChunk::MAX_CHUNK_SIZE],
        };
        update_chunk.data[..chunk.len()].copy_from_slice(chunk);

        self.rpc(RequestKind::UpdateChunk(update_chunk)).await.and_then(
            |(_peer, response)| {
                response.try_into_update_chunk_ack().map_err(Into::into)
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
                response.try_into_sys_reset_prepare_ack().map_err(Into::into)
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
                Err(Error::BadResponseType(BadResponseType {
                    expected: "system-reset",
                    got: response.name(),
                }))
            }
            Err(Error::SpError(
                ResponseError::SysResetTriggerWithoutPrepare,
            )) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// "Attach" to the serial console, setting up a tokio channel for all
    /// incoming serial console packets from the SP.
    pub async fn serial_console_attach(&self) -> Result<AttachedSerialConsole> {
        let (tx, rx) = oneshot::channel();

        // `Inner::run()` doesn't exit until we are dropped, so unwrapping here
        // only panics if it itself panicked.
        self.inner_commands
            .send(InnerCommand::SerialConsoleAttach(tx))
            .await
            .unwrap();

        let rx = rx.await.unwrap()?;

        Ok(AttachedSerialConsole { rx, inner_tx: self.inner_commands.clone() })
    }

    /// Detach an existing attached serial console connection.
    pub async fn serial_console_detach(&self) {
        // `Inner::run()` doesn't exit until we are dropped, so unwrapping here
        // only panics if it itself panicked.
        self.inner_commands
            .send(InnerCommand::SerialConsoleDetach)
            .await
            .unwrap();
    }

    pub(crate) async fn rpc(
        &self,
        kind: RequestKind,
    ) -> Result<(SocketAddrV6, ResponseKind)> {
        rpc(&self.inner_commands, kind).await
    }
}

async fn rpc(
    inner_tx: &mpsc::Sender<InnerCommand>,
    kind: RequestKind,
) -> Result<(SocketAddrV6, ResponseKind)> {
    let (resp_tx, resp_rx) = oneshot::channel();

    // `Inner::run()` doesn't exit as long as `inner_tx` exists, so unwrapping
    // here only panics if it itself panicked.
    inner_tx
        .send(InnerCommand::Rpc(RpcRequest { kind, response: resp_tx }))
        .await
        .unwrap();

    resp_rx.await.unwrap()
}

#[derive(Debug)]
pub struct AttachedSerialConsole {
    rx: mpsc::Receiver<SerialConsole>,
    inner_tx: mpsc::Sender<InnerCommand>,
}

impl AttachedSerialConsole {
    pub fn split(
        self,
    ) -> (AttachedSerialConsoleSend, AttachedSerialConsoleRecv) {
        (
            AttachedSerialConsoleSend { inner_tx: self.inner_tx },
            AttachedSerialConsoleRecv { rx: self.rx },
        )
    }
}

#[derive(Debug)]
pub struct AttachedSerialConsoleSend {
    inner_tx: mpsc::Sender<InnerCommand>,
}

impl AttachedSerialConsoleSend {
    /// Write `data` to the serial console of the SP.
    pub async fn write(&self, data: SerialConsole) -> Result<()> {
        rpc(&self.inner_tx, RequestKind::SerialConsoleWrite(data))
            .await
            .and_then(|(_peer, response)| {
                response.try_into_serial_console_write_ack().map_err(Into::into)
            })
    }
}

#[derive(Debug)]
pub struct AttachedSerialConsoleRecv {
    rx: mpsc::Receiver<SerialConsole>,
}

impl AttachedSerialConsoleRecv {
    /// Receive a `SerialConsole` packet from the SP.
    ///
    /// Returns `None` if the underlying channel has been closed (e.g., if the
    /// serial console has been detached).
    pub async fn recv(&mut self) -> Option<SerialConsole> {
        self.rx.recv().await
    }
}

#[derive(Debug)]
struct RpcRequest {
    kind: RequestKind,
    response: oneshot::Sender<Result<(SocketAddrV6, ResponseKind)>>,
}

#[derive(Debug)]
// `Rpc` is the large variant, which is by far the most common, so silence
// clippy's warning that recommends boxing it.
#[allow(clippy::large_enum_variant)]
enum InnerCommand {
    Rpc(RpcRequest),
    SerialConsoleAttach(oneshot::Sender<Result<mpsc::Receiver<SerialConsole>>>),
    SerialConsoleDetach,
}

struct Inner {
    log: Logger,
    socket: UdpSocket,
    sp_addr: watch::Sender<Option<(SocketAddrV6, SpPort)>>,
    discovery_addr: SocketAddrV6,
    max_attempts: usize,
    per_attempt_timeout: Duration,
    serial_console_tx: Option<mpsc::Sender<SerialConsole>>,
    cmds: mpsc::Receiver<InnerCommand>,
    request_id: u32,
}

impl Inner {
    fn new(
        log: Logger,
        socket: UdpSocket,
        sp_addr: watch::Sender<Option<(SocketAddrV6, SpPort)>>,
        discovery_addr: SocketAddrV6,
        max_attempts: usize,
        per_attempt_timeout: Duration,
        cmds: mpsc::Receiver<InnerCommand>,
    ) -> Self {
        Self {
            log,
            socket,
            sp_addr,
            discovery_addr,
            max_attempts,
            per_attempt_timeout,
            serial_console_tx: None,
            cmds,
            request_id: 0,
        }
    }

    async fn run(mut self) {
        let mut incoming_buf = [0; SpMessage::MAX_SIZE];

        let maybe_known_addr = *self.sp_addr.borrow();
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
                cmd = self.cmds.recv() => {
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
        incoming_buf: &mut [u8; SpMessage::MAX_SIZE],
    ) -> Result<SocketAddrV6> {
        let (addr, response) = self
            .rpc_call(self.discovery_addr, RequestKind::Discover, incoming_buf)
            .await?;

        let discovery = response.try_into_discover()?;

        let _ = self.sp_addr.send(Some((addr, discovery.sp_port)));
        Ok(addr)
    }

    async fn handle_command(
        &mut self,
        sp_addr: SocketAddrV6,
        command: InnerCommand,
        incoming_buf: &mut [u8; SpMessage::MAX_SIZE],
    ) {
        match command {
            InnerCommand::Rpc(rpc) => {
                let result =
                    self.rpc_call(sp_addr, rpc.kind, incoming_buf).await;

                if rpc.response.send(result).is_err() {
                    warn!(
                        self.log,
                        "RPC requester disappeared while waiting for response"
                    );
                }
            }
            InnerCommand::SerialConsoleAttach(response_tx) => {
                let resp = if self.serial_console_tx.is_some() {
                    Err(Error::SerialConsoleAlreadyAttached)
                } else {
                    let (tx, rx) = mpsc::channel(32);
                    self.serial_console_tx = Some(tx);
                    Ok(rx)
                };
                response_tx.send(resp).unwrap();
            }
            InnerCommand::SerialConsoleDetach => {
                self.serial_console_tx = None;
            }
        }
    }

    fn handle_incoming_message(
        &mut self,
        result: Result<(SocketAddrV6, SpMessage)>,
    ) {
        // TODO-correctness / TODO-security Should we check `peer` against what
        // we believe the SP's address to be? What would we do if they don't
        // match?
        let (_peer, message) = match result {
            Ok((peer, message)) => (peer, message),
            Err(err) => {
                error!(
                    self.log,
                    "error processing incoming data (ignoring)";
                    "err" => %err,
                );
                return;
            }
        };

        match message.kind {
            SpMessageKind::Response { request_id, result } => {
                warn!(
                    self.log,
                    "ignoring unexpected RPC response";
                    "request_id" => request_id,
                    "result" => ?result,
                );
            }
            SpMessageKind::SerialConsole(serial_console) => {
                self.forward_serial_console(serial_console);
            }
        }
    }

    async fn rpc_call(
        &mut self,
        addr: SocketAddrV6,
        kind: RequestKind,
        incoming_buf: &mut [u8; SpMessage::MAX_SIZE],
    ) -> Result<(SocketAddrV6, ResponseKind)> {
        // Build and serialize our request once.
        self.request_id += 1;
        let request =
            Request { version: version::V1, request_id: self.request_id, kind };

        // We know statically that `outgoing_buf` is large enough to hold any
        // `Request`, which in practice is the only possible serialization
        // error. Therefore, we can `.unwrap()`.
        let mut outgoing_buf = [0; Request::MAX_SIZE];
        let n = gateway_messages::serialize(&mut outgoing_buf[..], &request)
            .unwrap();
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

        Err(Error::ExhaustedNumAttempts(self.max_attempts))
    }

    async fn rpc_call_one_attempt(
        &mut self,
        addr: SocketAddrV6,
        request_id: u32,
        serialized_request: &[u8],
        incoming_buf: &mut [u8; SpMessage::MAX_SIZE],
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

            let (peer, response) = match result {
                Ok((peer, response)) => (peer, response),
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
                SpMessageKind::SerialConsole(serial_console) => {
                    self.forward_serial_console(serial_console);
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

    fn forward_serial_console(&mut self, serial_console: SerialConsole) {
        if let Some(tx) = self.serial_console_tx.as_ref() {
            match tx.try_send(serial_console) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.serial_console_tx = None;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // TODO-correctness Should we apply backpressure to the SP
                    // instead of dropping data? We would need to add acks to
                    // currently-async SP -> MGS messaging.
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
}

async fn send(
    socket: &UdpSocket,
    addr: SocketAddrV6,
    data: &[u8],
) -> Result<(), Error> {
    let n = socket
        .send_to(data, addr)
        .await
        .map_err(|err| Error::UdpSendTo { addr, err })?;

    // `send_to` should never write a partial packet; this is UDP.
    assert_eq!(data.len(), n, "partial UDP packet sent to {}?!", addr);

    Ok(())
}

async fn recv(
    socket: &UdpSocket,
    incoming_buf: &mut [u8; SpMessage::MAX_SIZE],
    log: &Logger,
) -> Result<(SocketAddrV6, SpMessage), Error> {
    let (n, peer) = socket
        .recv_from(&mut incoming_buf[..])
        .await
        .map_err(Error::UdpRecv)?;

    let peer = match peer {
        std::net::SocketAddr::V6(addr) => addr,
        std::net::SocketAddr::V4(_) => {
            // We're exclusively using IPv6; we can't get a response from an
            // IPv4 peer.
            unreachable!()
        }
    };

    let (message, _n) =
        gateway_messages::deserialize::<SpMessage>(&incoming_buf[..n])
            .map_err(|err| Error::Deserialize { peer, err })?;

    trace!(
        log, "received message from SP";
        "sp" => %peer,
        "message" => ?message,
    );

    Ok((peer, message))
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
