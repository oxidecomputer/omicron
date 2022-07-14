// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::GimletConfig;
use crate::rot::RotSprocketExt;
use crate::server;
use crate::server::UdpServer;
use crate::{Responsiveness, SimulatedSp};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures::future;
use gateway_messages::sp_impl::{SerialConsolePacketizer, SpHandler};
use gateway_messages::version;
use gateway_messages::DiscoverResponse;
use gateway_messages::ResponseError;
use gateway_messages::SerialConsole;
use gateway_messages::SerialNumber;
use gateway_messages::SerializedSize;
use gateway_messages::SpComponent;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use gateway_messages::SpPort;
use gateway_messages::SpState;
use slog::{debug, error, info, warn, Logger};
use sprockets_rot::common::msgs::{RotRequestV1, RotResponseV1};
use sprockets_rot::common::Ed25519PublicKey;
use sprockets_rot::{RotSprocket, RotSprocketError};
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};

pub struct Gimlet {
    rot: Mutex<RotSprocket>,
    manufacturing_public_key: Ed25519PublicKey,
    local_addrs: Option<[SocketAddrV6; 2]>,
    serial_number: SerialNumber,
    serial_console_addrs: HashMap<String, SocketAddrV6>,
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

    fn manufacturing_public_key(&self) -> Ed25519PublicKey {
        self.manufacturing_public_key
    }

    fn local_addr(&self, port: SpPort) -> Option<SocketAddrV6> {
        let i = match port {
            SpPort::One => 0,
            SpPort::Two => 1,
        };
        self.local_addrs.map(|addrs| addrs[i])
    }

    async fn set_responsiveness(&self, r: Responsiveness) {
        let (tx, rx) = oneshot::channel();
        if let Ok(()) = self.commands.send((Command::SetResponsiveness(r), tx))
        {
            rx.await.unwrap();
        }
    }

    fn rot_request(
        &self,
        request: RotRequestV1,
    ) -> Result<RotResponseV1, RotSprocketError> {
        self.rot.lock().unwrap().handle_deserialized(request)
    }
}

impl Gimlet {
    pub async fn spawn(gimlet: &GimletConfig, log: Logger) -> Result<Self> {
        info!(log, "setting up simualted gimlet");

        // We want to be able to start without knowing the gateways' socket
        // addresses, but we're spawning both the primary UDP task (which
        // receives messages from the gateway) and a helper TCP task (which
        // emulates a serial console and sends messages to the gateway
        // unprompted). We'll share a locked `Option<SocketAddrV6>` between the
        // tasks, and have the UDP task populate it. If the TCP task receives
        // data but doesn't know either of the gateways addresses, it will just
        // discard it.
        let gateway_addresses: Arc<Mutex<[Option<SocketAddrV6>; 2]>> =
            Arc::default();

        let mut incoming_console_tx = HashMap::new();
        let mut serial_console_addrs = HashMap::new();
        let mut inner_tasks = Vec::new();
        let (commands, commands_rx) = mpsc::unbounded_channel();

        let local_addrs = if let Some(bind_addrs) = gimlet.common.bind_addrs {
            // bind to our two local "KSZ" ports
            assert_eq!(bind_addrs.len(), 2); // gimlet SP always has 2 ports
            let servers = future::try_join(
                UdpServer::new(
                    bind_addrs[0],
                    gimlet.common.multicast_addr,
                    &log,
                ),
                UdpServer::new(
                    bind_addrs[1],
                    gimlet.common.multicast_addr,
                    &log,
                ),
            )
            .await?;
            let servers = [servers.0, servers.1];

            for component_config in &gimlet.components {
                let name = component_config.name.as_str();
                let component = SpComponent::try_from(name)
                    .map_err(|_| anyhow!("component id {:?} too long", name))?;

                if let Some(addr) = component_config.serial_console {
                    let listener =
                        TcpListener::bind(addr).await.with_context(|| {
                            format!("failed to bind to {}", addr)
                        })?;
                    info!(
                        log, "bound fake serial console to TCP port";
                        "addr" => %addr,
                        "component" => ?component,
                    );

                    serial_console_addrs.insert(
                        component
                            .as_str()
                            .with_context(|| "non-utf8 component")?
                            .to_string(),
                        listener
                            .local_addr()
                            .with_context(|| {
                                "failed to get local address of bound socket"
                            })
                            .and_then(|addr| match addr {
                                SocketAddr::V4(addr) => {
                                    bail!("bound IPv4 address {}", addr)
                                }
                                SocketAddr::V6(addr) => Ok(addr),
                            })?,
                    );

                    let (tx, rx) = mpsc::unbounded_channel();
                    incoming_console_tx.insert(component, tx);

                    let serial_console = SerialConsoleTcpTask::new(
                        component,
                        listener,
                        rx,
                        [
                            Arc::clone(servers[0].socket()),
                            Arc::clone(servers[1].socket()),
                        ],
                        Arc::clone(&gateway_addresses),
                        log.new(slog::o!("serial-console" => name.to_string())),
                    );
                    inner_tasks.push(task::spawn(async move {
                        serial_console.run().await
                    }));
                }
            }
            let local_addrs =
                [servers[0].local_addr(), servers[1].local_addr()];
            let inner = UdpTask::new(
                servers,
                gateway_addresses,
                gimlet.common.serial_number,
                incoming_console_tx,
                commands_rx,
                log,
            );
            inner_tasks
                .push(task::spawn(async move { inner.run().await.unwrap() }));

            Some(local_addrs)
        } else {
            None
        };

        let (manufacturing_public_key, rot) =
            RotSprocket::bootstrap_from_config(&gimlet.common);
        Ok(Self {
            rot: Mutex::new(rot),
            manufacturing_public_key,
            local_addrs,
            serial_number: gimlet.common.serial_number,
            serial_console_addrs,
            commands,
            inner_tasks,
        })
    }

    pub fn serial_console_addr(&self, component: &str) -> Option<SocketAddrV6> {
        self.serial_console_addrs.get(component).copied()
    }
}

struct SerialConsoleTcpTask {
    listener: TcpListener,
    incoming_serial_console: UnboundedReceiver<SerialConsole>,
    socks: [Arc<UdpSocket>; 2],
    gateway_addresses: Arc<Mutex<[Option<SocketAddrV6>; 2]>>,
    console_packetizer: SerialConsolePacketizer,
    log: Logger,
}

impl SerialConsoleTcpTask {
    fn new(
        component: SpComponent,
        listener: TcpListener,
        incoming_serial_console: UnboundedReceiver<SerialConsole>,
        socks: [Arc<UdpSocket>; 2],
        gateway_addresses: Arc<Mutex<[Option<SocketAddrV6>; 2]>>,
        log: Logger,
    ) -> Self {
        Self {
            listener,
            incoming_serial_console,
            socks,
            gateway_addresses,
            console_packetizer: SerialConsolePacketizer::new(component),
            log,
        }
    }

    async fn send_serial_console(&mut self, mut data: &[u8]) -> Result<()> {
        let gateway_addrs = *self.gateway_addresses.lock().unwrap();
        for (i, (sock, &gateway_addr)) in
            self.socks.iter().zip(&gateway_addrs).enumerate()
        {
            let gateway_addr = match gateway_addr {
                Some(addr) => addr,
                None => {
                    info!(
                        self.log,
                        concat!(
                            "MGS address on port {} not known - ",
                            "not sending it serial console data",
                        ),
                        i,
                    );
                    continue;
                }
            };

            // if we're told to send something starting with "SKIP ", emulate a
            // dropped packet spanning 10 bytes before sending the rest of the
            // data.
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

                // We know `out` is big enough for any `SpMessage`, so no need
                // to bubble up an error here.
                let n = gateway_messages::serialize(&mut out[..], &message)
                    .unwrap();
                sock.send_to(&out[..n], gateway_addr).await?;
            }
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
    udp0: UdpServer,
    udp1: UdpServer,
    handler: Handler,
    commands:
        mpsc::UnboundedReceiver<(Command, oneshot::Sender<CommandResponse>)>,
}

impl UdpTask {
    fn new(
        servers: [UdpServer; 2],
        gateway_addresses: Arc<Mutex<[Option<SocketAddrV6>; 2]>>,
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
        let [udp0, udp1] = servers;
        Self {
            udp0,
            udp1,
            handler: Handler {
                log,
                gateway_addresses,
                serial_number,
                incoming_serial_console,
            },
            commands,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut out_buf = [0; SpMessage::MAX_SIZE];
        let mut responsiveness = Responsiveness::Responsive;
        loop {
            select! {
                recv0 = self.udp0.recv_from() => {
                    if let Some((resp, addr)) = server::handle_request(
                        &mut self.handler,
                        recv0,
                        &mut out_buf,
                        responsiveness,
                        SpPort::One,
                    ).await? {
                        self.udp0.send_to(resp, addr).await?;
                    }
                }

                recv1 = self.udp1.recv_from() => {
                    if let Some((resp, addr)) = server::handle_request(
                        &mut self.handler,
                        recv1,
                        &mut out_buf,
                        responsiveness,
                        SpPort::Two,
                    ).await? {
                        self.udp1.send_to(resp, addr).await?;
                    }
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
    gateway_addresses: Arc<Mutex<[Option<SocketAddrV6>; 2]>>,
    incoming_serial_console:
        HashMap<SpComponent, UnboundedSender<SerialConsole>>,
}

impl Handler {
    fn update_gateway_address(&self, addr: SocketAddrV6, port: SpPort) {
        let i = match port {
            SpPort::One => 0,
            SpPort::Two => 1,
        };
        self.gateway_addresses.lock().unwrap()[i] = Some(addr);
    }
}

impl SpHandler for Handler {
    fn discover(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<DiscoverResponse, ResponseError> {
        debug!(
            &self.log,
            "received discover; sending response";
            "sender" => %sender,
            "port" => ?port,
        );
        Ok(DiscoverResponse { sp_port: port })
    }

    fn ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
    ) -> Result<gateway_messages::IgnitionState, ResponseError> {
        self.update_gateway_address(sender, port);
        warn!(
            &self.log,
            "received ignition state request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<gateway_messages::BulkIgnitionState, ResponseError> {
        self.update_gateway_address(sender, port);
        warn!(
            &self.log,
            "received bulk ignition state request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn ignition_command(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> Result<(), ResponseError> {
        self.update_gateway_address(sender, port);
        warn!(
            &self.log,
            "received ignition command; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
            "command" => ?command,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn serial_console_write(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        packet: gateway_messages::SerialConsole,
    ) -> Result<(), ResponseError> {
        self.update_gateway_address(sender, port);
        debug!(
            &self.log,
            "received serial console packet";
            "sender" => %sender,
            "port" => ?port,
            "len" => packet.len,
            "offset" => packet.offset,
            "component" => ?packet.component,
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

    fn sp_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<SpState, ResponseError> {
        self.update_gateway_address(sender, port);
        let state = SpState { serial_number: self.serial_number };
        debug!(
            &self.log, "received state request";
            "sender" => %sender,
            "port" => ?port,
            "reply-state" => ?state,
        );
        Ok(state)
    }
}
