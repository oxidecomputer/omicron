// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Config;
use crate::server::{self, UdpServer};
use anyhow::Result;
use gateway_messages::sp_impl::{SpHandler, SpServer};
use gateway_messages::{
    IgnitionFlags, IgnitionState, ResponseError, ResponseKind,
};
use slog::{debug, error, info, warn, Logger};
use tokio::{
    select,
    task::{self, JoinHandle},
};

pub struct Sidecar {
    inner_task: JoinHandle<()>,
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        // default join handle drop behavior is to detach; we want to abort
        self.inner_task.abort();
    }
}

impl Sidecar {
    pub async fn spawn(config: &Config) -> Result<Self> {
        let log = server::logger(&config, "sidecar")?;
        info!(log, "setting up simualted sidecar");
        let server = UdpServer::new(config).await?;
        let inner = Inner::new(server, log);
        let inner_task = task::spawn(async move { inner.run().await.unwrap() });
        Ok(Self { inner_task })
    }
}

struct Inner {
    handler: Handler,
    udp: UdpServer,
}

impl Inner {
    fn new(server: UdpServer, log: Logger) -> Self {
        Self { handler: Handler { log }, udp: server }
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
}

impl SpHandler for Handler {
    fn ping(&mut self) -> ResponseKind {
        debug!(&self.log, "received ping; sending pong");
        ResponseKind::Pong
    }

    fn ignition_state(&mut self, target: u8) -> ResponseKind {
        const SIDECAR_ID: u16 = 0b01_0010;

        let state = IgnitionState {
            id: SIDECAR_ID,
            flags: IgnitionFlags::POWER | IgnitionFlags::CTRL_DETECT_0,
        };

        debug!(
            &self.log,
            "received ignition state request for {}; sending {:?}",
            target,
            state
        );
        ResponseKind::IgnitionState(state)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> ResponseKind {
        debug!(
            &self.log,
            "received ignition command {:?} for target {}; sending ack",
            command,
            target
        );
        ResponseKind::IgnitionCommandAck
    }

    fn serial_console_write(
        &mut self,
        _packet: gateway_messages::SerialConsole,
    ) -> ResponseKind {
        warn!(&self.log, "received request to write to serial console (unsupported on sidecar)");
        ResponseKind::Error(ResponseError::RequestUnsupported)
    }
}
