// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::{Config, SidecarConfig};
use crate::server::UdpServer;
use anyhow::Result;
use gateway_messages::sp_impl::{SpHandler, SpServer};
use gateway_messages::{
    IgnitionCommand, IgnitionFlags, IgnitionState, ResponseError, ResponseKind,
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
    pub async fn spawn(
        config: &Config,
        sidecar_config: &SidecarConfig,
        log: Logger,
    ) -> Result<Self> {
        const ID_GIMLET: u16 = 0b0000_0000_0001_0001;
        const ID_SIDECAR: u16 = 0b0000_0000_0001_0010;

        info!(log, "setting up simualted sidecar");
        let server = UdpServer::new(sidecar_config.bind_address).await?;

        let mut ignition_targets = Vec::new();
        for _ in &config.simulated_sps.sidecar {
            ignition_targets.push(IgnitionState {
                id: ID_SIDECAR,
                flags: IgnitionFlags::POWER | IgnitionFlags::CTRL_DETECT_0,
            });
        }
        for _ in &config.simulated_sps.gimlet {
            ignition_targets.push(IgnitionState {
                id: ID_GIMLET,
                flags: IgnitionFlags::POWER | IgnitionFlags::CTRL_DETECT_0,
            });
        }

        let inner = Inner::new(server, ignition_targets, log);
        let inner_task = task::spawn(async move { inner.run().await.unwrap() });
        Ok(Self { inner_task })
    }
}

struct Inner {
    handler: Handler,
    udp: UdpServer,
}

impl Inner {
    fn new(
        server: UdpServer,
        ignition_targets: Vec<IgnitionState>,
        log: Logger,
    ) -> Self {
        Self { handler: Handler { log, ignition_targets }, udp: server }
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
    ignition_targets: Vec<IgnitionState>,
}

impl Handler {
    fn get_target(&self, target: u8) -> Result<&IgnitionState, ResponseError> {
        self.ignition_targets
            .get(usize::from(target))
            .ok_or(ResponseError::IgnitionTargetDoesNotExist(target))
    }

    fn get_target_mut(
        &mut self,
        target: u8,
    ) -> Result<&mut IgnitionState, ResponseError> {
        self.ignition_targets
            .get_mut(usize::from(target))
            .ok_or(ResponseError::IgnitionTargetDoesNotExist(target))
    }
}

impl SpHandler for Handler {
    fn ping(&mut self) -> ResponseKind {
        debug!(&self.log, "received ping; sending pong");
        ResponseKind::Pong
    }

    fn ignition_state(&mut self, target: u8) -> ResponseKind {
        let state = match self.get_target(target) {
            Ok(state) => *state,
            Err(err) => return ResponseKind::Error(err),
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
        command: IgnitionCommand,
    ) -> ResponseKind {
        let state = match self.get_target_mut(target) {
            Ok(state) => state,
            Err(err) => return ResponseKind::Error(err),
        };
        match command {
            IgnitionCommand::PowerOn => {
                state.flags.set(IgnitionFlags::POWER, true)
            }
            IgnitionCommand::PowerOff => {
                state.flags.set(IgnitionFlags::POWER, false)
            }
        }

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
        ResponseKind::Error(ResponseError::RequestUnsupportedForSp)
    }
}
