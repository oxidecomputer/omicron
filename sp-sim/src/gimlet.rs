// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Config;
use crate::server::{self, UdpServer};
use anyhow::{anyhow, bail, Result};
use gateway_messages::sp_impl::{SerialConsolePacketizer, SpHandler, SpServer};
use gateway_messages::{
    ResponseError, ResponseKind, SerializedSize, SpMessage,
};
use slog::{debug, error, info, warn, Logger};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::{
    select,
    task::{self, JoinHandle},
};

pub struct Gimlet {
    sock: Arc<UdpSocket>,
    gateway_address: SocketAddr,
    console_packetizer: SerialConsolePacketizer,
    buf: [u8; SpMessage::MAX_SIZE],
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
        let inner = Inner::new(server, log);
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
            buf: [0; SpMessage::MAX_SIZE],
            inner_task,
        })
    }

    pub async fn send_serial_console(&mut self, mut data: &[u8]) -> Result<()> {
        // if we're told to send something starting with "SKIP ", emulate a
        // dropped packet spanning 10 bytes before sending the rest of the data.
        if let Some(remaining) = data.strip_prefix(b"SKIP ") {
            self.console_packetizer.danger_emulate_dropped_packets(10);
            data = remaining;
        }
        let mut packets = self.console_packetizer.packetize(data);
        while let Some(buf) = packets.next_packet(&mut self.buf) {
            self.sock.send_to(buf, self.gateway_address).await?;
        }
        Ok(())
    }
}

struct Handler {
    log: Logger,
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
}

struct Inner {
    udp: UdpServer,
    server: SpServer<Handler>,
}

impl Inner {
    fn new(server: UdpServer, log: Logger) -> Self {
        Self { udp: server, server: SpServer::new(Handler { log }) }
    }

    async fn run(mut self) -> Result<()> {
        loop {
            select! {
                recv = self.udp.recv_from() => {
                    let (data, addr) = recv?;

                    let resp = match self.server.dispatch(data) {
                        Ok(resp) => resp,
                        Err(err) => {
                            error!(
                                self.server.handler().log,
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
