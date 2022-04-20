// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::GimletConfig;
use crate::server::UdpServer;
use crate::{Responsiveness, SimulatedSp};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use gateway_messages::sp_impl::{SerialConsolePacketizer, SpHandler, SpServer};
use gateway_messages::{
    version, ResponseError, SerialConsole, SerialNumber, SerializedSize,
    SpComponent, SpMessage, SpMessageKind, SpState,
};
use slog::{debug, error, info, warn, Logger};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};

pub struct Gimlet {
    local_addr: SocketAddr,
    serial_number: SerialNumber,
    serial_console_addrs: HashMap<String, SocketAddr>,
    commands:
        mpsc::UnboundedSender<(Command, oneshot::Sender<CommandResponse>)>,
    inner_tasks: Vec<JoinHandle<()>>,
}

impl Drop for Gimlet {
    fn drop(&mut self) {
        // default join handle drop behavior is to detach; we want to abort
        for task in &self.inner_tasks {
            task.abort();
        }
    }
}

#[async_trait]
impl SimulatedSp for Gimlet {
    fn serial_number(&self) -> String {
        hex::encode(self.serial_number)
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    async fn set_responsiveness(&self, r: Responsiveness) {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send((Command::SetResponsiveness(r), tx))
            .map_err(|_| "gimlet task died unexpectedly")
            .unwrap();
        rx.await.unwrap();
    }
}

impl Gimlet {
    pub async fn spawn(gimlet: &GimletConfig, log: Logger) -> Result<Self> {
        info!(log, "setting up simualted gimlet");

        let server = UdpServer::new(gimlet.bind_address).await?;
        let sock = Arc::clone(server.socket());
        let local_addr = sock
            .local_addr()
            .with_context(|| "could not get local address of bound socket")?;

        let mut incoming_console_tx = HashMap::new();
        let mut inner_tasks = Vec::new();

        // We want to be able to start without knowing the gateway's socket
        // address, but we're spawning both the primary UDP task (which receives
        // messages from the gateway) and a helper TCP task (which emulates a
        // serial console and sends messages to the gateway unprompted). We'll
        // share a locked `Option<SocketAddr>` between the tasks, and have the
        // UDP task populate it. If the TCP task receives data but doesn't know
        // the gateways address, it will just discard it.
        let gateway_address: Arc<Mutex<Option<SocketAddr>>> = Arc::default();

        let mut serial_console_addrs = HashMap::new();

        for component_config in &gimlet.components {
            let name = component_config.name.as_str();
            let component = SpComponent::try_from(name)
                .map_err(|_| anyhow!("component id {:?} too long", name))?;

            if let Some(addr) = component_config.serial_console {
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("failed to bind to {}", addr))?;
                serial_console_addrs.insert(
                    component.as_str().unwrap().to_string(),
                    listener.local_addr().with_context(|| {
                        "failed to get local address of bound socket"
                    })?,
                );

                let (tx, rx) = mpsc::unbounded_channel();
                incoming_console_tx.insert(component, tx);

                let serial_console = SerialConsoleTcpTask::new(
                    component,
                    listener,
                    rx,
                    Arc::clone(&sock),
                    Arc::clone(&gateway_address),
                    log.new(slog::o!("serial-console" => name.to_string())),
                );
                inner_tasks.push(task::spawn(async move {
                    serial_console.run().await
                }));
            }
        }

        let (commands, commands_rx) = mpsc::unbounded_channel();
        let inner = UdpTask::new(
            server,
            gateway_address,
            gimlet.serial_number,
            incoming_console_tx,
            commands_rx,
            log,
        );
        inner_tasks
            .push(task::spawn(async move { inner.run().await.unwrap() }));

        Ok(Self {
            local_addr,
            serial_number: gimlet.serial_number,
            serial_console_addrs,
            commands,
            inner_tasks,
        })
    }

    pub fn serial_console_addr(&self, component: &str) -> Option<SocketAddr> {
        self.serial_console_addrs.get(component).copied()
    }
}

struct SerialConsoleTcpTask {
    listener: TcpListener,
    incoming_serial_console: UnboundedReceiver<SerialConsole>,
    sock: Arc<UdpSocket>,
    gateway_address: Arc<Mutex<Option<SocketAddr>>>,
    console_packetizer: SerialConsolePacketizer,
    log: Logger,
}

impl SerialConsoleTcpTask {
    fn new(
        component: SpComponent,
        listener: TcpListener,
        incoming_serial_console: UnboundedReceiver<SerialConsole>,
        sock: Arc<UdpSocket>,
        gateway_address: Arc<Mutex<Option<SocketAddr>>>,
        log: Logger,
    ) -> Self {
        Self {
            listener,
            incoming_serial_console,
            sock,
            gateway_address,
            console_packetizer: SerialConsolePacketizer::new(component),
            log,
        }
    }

    async fn send_serial_console(&mut self, mut data: &[u8]) -> Result<()> {
        let gateway_address = self.gateway_address.lock().unwrap().ok_or_else(|| anyhow!("serial console task does not know gateway's UDP address (yet?)"))?;

        // if we're told to send something starting with "SKIP ", emulate a
        // dropped packet spanning 10 bytes before sending the rest of the data.
        if let Some(remaining) = data.strip_prefix(b"SKIP ") {
            self.console_packetizer.danger_emulate_dropped_packets(10);
            data = remaining;
        }

        let mut out = [0; SpMessage::MAX_SIZE];
        for packet in self.console_packetizer.packetize(data) {
            let message = SpMessage {
                version: version::V1,
                kind: SpMessageKind::SerialConsole(packet),
            };

            // We know `out` is big enough for any `SpMessage`, so no need to
            // bubble up an error here.
            let n =
                gateway_messages::serialize(&mut out[..], &message).unwrap();
            self.sock.send_to(&out[..n], gateway_address).await?;
        }

        Ok(())
    }

    async fn run(mut self) {
        loop {
            // wait for incoming connections, discarding any serial console
            // packets received while we don't have one
            let (conn, addr) = loop {
                select! {
                    try_conn = self.listener.accept() => {
                        match try_conn {
                            Ok((conn, addr)) => break (conn, addr),
                            Err(err) => {
                                warn!(self.log, "error accepting TCP connection: {}", err);
                                continue;
                            }
                        }
                    }
                    _ = self.incoming_serial_console.recv() => {
                        info!(self.log, "dropping incoming serial console packet (no attached TCP connection)");
                        continue;
                    }
                };
            };

            info!(
                self.log,
                "accepted serial console TCP connection from {}", addr
            );
            match self.drive_tcp_connection(conn).await {
                Ok(()) => {
                    info!(self.log, "closing serial console TCP connection")
                }
                Err(err) => {
                    error!(
                        self.log,
                        "serial TCP connection failed";
                        "err" => %err,
                    )
                }
            }
        }
    }

    async fn drive_tcp_connection(
        &mut self,
        mut conn: TcpStream,
    ) -> Result<()> {
        let mut buf = [0; 512];

        // copy serial console data in both directions
        loop {
            select! {
                res = conn.read(&mut buf) => {
                    let n = res?;
                    if n == 0 {
                        return Ok(());
                    }
                    self
                        .send_serial_console(&buf[..n])
                        .await
                        .with_context(||"UDP send error")?;
                }
                incoming = self.incoming_serial_console.recv() => {
                    // we can only get `None` if the tx half was dropped,
                    // which means we're in the process of shutting down
                    let incoming = match incoming {
                        Some(incoming) => incoming,
                        None => return Ok(()),
                    };

                    let data = &incoming.data[..usize::from(incoming.len)];
                    conn
                        .write_all(data)
                        .await
                        .with_context(|| "TCP write error")?;
                }
            }
        }
    }
}

enum Command {
    SetResponsiveness(Responsiveness),
}

enum CommandResponse {
    SetResponsivenessAck,
}

struct UdpTask {
    udp: UdpServer,
    gateway_address: Arc<Mutex<Option<SocketAddr>>>,
    handler: Handler,
    commands:
        mpsc::UnboundedReceiver<(Command, oneshot::Sender<CommandResponse>)>,
}

impl UdpTask {
    fn new(
        server: UdpServer,
        gateway_address: Arc<Mutex<Option<SocketAddr>>>,
        serial_number: SerialNumber,
        incoming_serial_console: HashMap<
            SpComponent,
            UnboundedSender<SerialConsole>,
        >,
        commands: mpsc::UnboundedReceiver<(
            Command,
            oneshot::Sender<CommandResponse>,
        )>,
        log: Logger,
    ) -> Self {
        Self {
            udp: server,
            gateway_address,
            handler: Handler { log, serial_number, incoming_serial_console },
            commands,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut server = SpServer::default();
        let mut responsiveness = Responsiveness::Responsive;
        loop {
            select! {
                recv = self.udp.recv_from() => {
                    if responsiveness != Responsiveness::Responsive {
                        continue;
                    }

                    let (data, addr) = recv?;
                    *self.gateway_address.lock().unwrap() = Some(addr);

                    let resp = match server.dispatch(data, &mut self.handler) {
                        Ok(resp) => resp,
                        Err(err) => {
                            error!(
                                self.handler.log,
                                "dispatching message failed: {:?}", err,
                            );
                            continue;
                        }
                    };

                    self.udp.send_to(resp, addr).await?;
                }

                command = self.commands.recv() => {
                    // if sending half is gone, we're about to be killed anyway
                    let (command, tx) = match command {
                        Some((command, tx)) => (command, tx),
                        None => return Ok(()),
                    };

                    match command {
                        Command::SetResponsiveness(r) => {
                            responsiveness = r;
                            tx.send(CommandResponse::SetResponsivenessAck)
                                .map_err(|_| "receiving half died").unwrap();
                        }
                    }
                }
            }
        }
    }
}

struct Handler {
    log: Logger,
    serial_number: SerialNumber,
    incoming_serial_console:
        HashMap<SpComponent, UnboundedSender<SerialConsole>>,
}

impl SpHandler for Handler {
    fn ping(&mut self) -> Result<(), ResponseError> {
        debug!(&self.log, "received ping; sending pong");
        Ok(())
    }

    fn ignition_state(
        &mut self,
        target: u8,
    ) -> Result<gateway_messages::IgnitionState, ResponseError> {
        warn!(
            &self.log,
            "received ignition state request for {}; not supported by gimlet",
            target,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_state(
        &mut self,
    ) -> Result<gateway_messages::BulkIgnitionState, ResponseError> {
        warn!(
            &self.log,
            "received bulk ignition state request; not supported by gimlet",
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> Result<(), ResponseError> {
        warn!(
            &self.log,
            "received ignition command {:?} for target {}; not supported by gimlet",
            command,
            target
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn serial_console_write(
        &mut self,
        packet: gateway_messages::SerialConsole,
    ) -> Result<(), ResponseError> {
        debug!(
            &self.log,
            "received serial console packet with {} bytes at offset {} for component {:?}",
            packet.len,
            packet.offset,
            packet.component,
        );

        let incoming_serial_console = self
            .incoming_serial_console
            .get(&packet.component)
            .ok_or(ResponseError::RequestUnsupportedForComponent)?;

        // should we sanity check `offset`? for now just assume everything
        // comes in order; we're just a simulator anyway
        //
        // if the receiving half is gone, we're in the process of shutting down;
        // ignore errors here
        let _ = incoming_serial_console.send(packet);

        Ok(())
    }

    fn sp_state(&mut self) -> Result<SpState, ResponseError> {
        let state = SpState { serial_number: self.serial_number };
        debug!(&self.log, "received state request; sending {:?}", state);
        Ok(state)
    }
}
