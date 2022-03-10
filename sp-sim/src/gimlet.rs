// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::{Config, GimletConfig};
use crate::server::UdpServer;
use anyhow::{anyhow, Context, Result};
use gateway_messages::sp_impl::{SerialConsolePacketizer, SpHandler, SpServer};
use gateway_messages::{
    version, ResponseError, SerialConsole, SerialNumber, SerializedSize,
    SpComponent, SpMessage, SpMessageKind, SpState,
};
use slog::{debug, error, info, warn, Logger};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::{self, JoinHandle};

pub struct Gimlet {
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

impl Gimlet {
    pub async fn spawn(
        config: &Config,
        gimlet: &GimletConfig,
        log: Logger,
    ) -> Result<Self> {
        info!(log, "setting up simualted gimlet");

        let server = UdpServer::new(gimlet.bind_address).await?;
        let sock = Arc::clone(server.socket());

        let mut incoming_console_tx = HashMap::new();
        let mut inner_tasks = Vec::new();

        for component_config in &gimlet.components {
            let name = component_config.name.as_str();
            let component = SpComponent::try_from(name)
                .map_err(|_| anyhow!("component id {:?} too long", name))?;

            if let Some(addr) = component_config.serial_console {
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("failed to bind to {}", addr))?;

                let (tx, rx) = mpsc::unbounded_channel();
                incoming_console_tx.insert(component, tx);

                let serial_console = SerialConsoleTcpTask::new(
                    component,
                    listener,
                    rx,
                    Arc::clone(&sock),
                    config.gateway_address,
                    log.new(slog::o!("serial-console" => name.to_string())),
                );
                inner_tasks.push(task::spawn(async move {
                    serial_console.run().await
                }));
            }
        }

        let inner = UdpTask::new(
            server,
            gimlet.serial_number,
            incoming_console_tx,
            log,
        );
        inner_tasks
            .push(task::spawn(async move { inner.run().await.unwrap() }));

        Ok(Self { inner_tasks })
    }
}

struct SerialConsoleTcpTask {
    listener: TcpListener,
    incoming_serial_console: UnboundedReceiver<SerialConsole>,
    sock: Arc<UdpSocket>,
    gateway_address: SocketAddr,
    console_packetizer: SerialConsolePacketizer,
    log: Logger,
}

impl SerialConsoleTcpTask {
    fn new(
        component: SpComponent,
        listener: TcpListener,
        incoming_serial_console: UnboundedReceiver<SerialConsole>,
        sock: Arc<UdpSocket>,
        gateway_address: SocketAddr,
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
            self.sock.send_to(&out[..n], self.gateway_address).await?;
        }

        Ok(())
    }

    async fn run(mut self) {
        loop {
            // wait for incoming connections, discarding any serial console
            // packets received while we don't have one
            let (mut conn, addr) = loop {
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
            let mut buf = [0; 512];

            // copy serial console data in both directions
            loop {
                select! {
                    res = conn.read(&mut buf) => {
                        let n = match res {
                            Ok(n) => n,
                            Err(err) => {
                                error!(self.log, "closing serial console TCP connection ({})", err);
                                break;
                            }
                        };
                        match self.send_serial_console(&buf[..n]).await {
                            Ok(()) => (),
                            Err(err) => {
                                error!(self.log, "ignoring UDP send failure {}", err);
                                continue;
                            }
                        }
                    }
                    incoming = self.incoming_serial_console.recv() => {
                        // we can only get `None` if the tx half was dropped,
                        // which means we're in the process of shutting down
                        let incoming = match incoming {
                            Some(incoming) => incoming,
                            None => return,
                        };
                        // we're the sim; don't bother bounds checking
                        // `incoming.len` - panicking if we get bogus data is
                        // fine
                        match conn.write_all(&incoming.data[..usize::from(incoming.len)]).await {
                            Ok(()) => (),
                            Err(err) => {
                                error!(self.log, "closing serial console TCP connection ({})", err);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

struct UdpTask {
    udp: UdpServer,
    handler: Handler,
}

impl UdpTask {
    fn new(
        server: UdpServer,
        serial_number: SerialNumber,
        incoming_serial_console: HashMap<
            SpComponent,
            UnboundedSender<SerialConsole>,
        >,
        log: Logger,
    ) -> Self {
        Self {
            udp: server,
            handler: Handler { log, serial_number, incoming_serial_console },
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut server = SpServer::default();
        loop {
            select! {
                recv = self.udp.recv_from() => {
                    let (data, addr) = recv?;

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
