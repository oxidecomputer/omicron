// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Config;
use crate::server::{self, UdpServer};
use anyhow::{anyhow, bail, Result};
use gateway_messages::sp_impl::{SerialConsolePacketizer, SpHandler, SpServer};
use gateway_messages::{
    version, ResponseError, ResponseKind, SerialConsole, SerializedSize,
    SpMessage, SpMessageKind,
};
use slog::{debug, error, info, warn, Logger};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::{
    select,
    task::{self, JoinHandle},
};

type SerialConsolePacket = [u8; SerialConsole::MAX_DATA_PER_PACKET];

pub struct Gimlet {
    sock: Arc<UdpSocket>,
    gateway_address: SocketAddr,
    console_packetizer: SerialConsolePacketizer,
    incoming_serial_console: UnboundedReceiver<SerialConsolePacket>,
    inner_task: JoinHandle<()>,
}

impl Drop for Gimlet {
    fn drop(&mut self) {
        // default join handle drop behavior is to detach; we want to abort
        self.inner_task.abort();
    }
}

impl Gimlet {
    pub async fn spawn(config: &Config) -> Result<Self> {
        let log = server::logger(&config, "gimlet")?;
        info!(log, "setting up simualted gimlet");

        let server = UdpServer::new(config).await?;
        let sock = Arc::clone(server.socket());

        let (incoming_console_tx, incoming_console_rx) =
            mpsc::unbounded_channel();

        let inner = Inner::new(server, incoming_console_tx, log);
        let inner_task = task::spawn(async move { inner.run().await.unwrap() });

        if config.components.serial_console.len() != 1 {
            bail!("simulated gimlet currently requires exactly 1 component with a serial console");
        }

        Ok(Self {
            sock,
            gateway_address: config.gateway_address,
            console_packetizer: SerialConsolePacketizer::new(
                gateway_messages::SpComponent::try_from(
                    config.components.serial_console[0].as_str(),
                )
                .map_err(|_| {
                    anyhow!(
                        "component id {} too long",
                        config.components.serial_console[0]
                    )
                })?,
            ),
            incoming_serial_console: incoming_console_rx,
            inner_task,
        })
    }

    pub async fn incoming_serial_console(&mut self) -> SerialConsolePacket {
        // `recv()` returns `None` when the sending half is dropped, but we're
        // holding a handle to `Inner` which holds the sender.
        self.incoming_serial_console.recv().await.unwrap()
    }

    pub async fn send_serial_console(&mut self, mut data: &[u8]) -> Result<()> {
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
}

struct Inner {
    udp: UdpServer,
    handler: Handler,
}

impl Inner {
    fn new(
        server: UdpServer,
        incoming_serial_console: UnboundedSender<SerialConsolePacket>,
        log: Logger,
    ) -> Self {
        Self { udp: server, handler: Handler { log, incoming_serial_console } }
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
    incoming_serial_console: UnboundedSender<SerialConsolePacket>,
}

impl SpHandler for Handler {
    fn ping(&mut self) -> ResponseKind {
        debug!(&self.log, "received ping; sending pong");
        ResponseKind::Pong
    }

    fn ignition_state(&mut self, target: u8) -> ResponseKind {
        warn!(
            &self.log,
            "received ignition state request for {}; not supported by gimlet",
            target,
        );
        ResponseKind::Error(ResponseError::RequestUnsupported)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> ResponseKind {
        warn!(
            &self.log,
            "received ignition command {:?} for target {}; not supported by gimlet",
            command,
            target
        );
        ResponseKind::Error(ResponseError::RequestUnsupported)
    }

    fn serial_console_write(
        &mut self,
        packet: gateway_messages::SerialConsole,
    ) -> ResponseKind {
        debug!(
            &self.log,
            "received serial console packet with {} bytes at offset {}",
            packet.len,
            packet.offset
        );

        // should we sanity check `offset`? for now just assume everything
        // comes in order; we're just a simulator anyway
        //
        // the receiving half still exists if we exist, since `Gimlet` aborts
        // our task when it's dropped
        self.incoming_serial_console.send(packet.data).unwrap();

        ResponseKind::SerialConsoleWriteAck
    }
}
