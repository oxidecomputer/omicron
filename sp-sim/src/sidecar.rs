// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddr;

use crate::config::{Config, SidecarConfig};
use crate::server::UdpServer;
use crate::{ignition_id, Responsiveness, SimulatedSp};
use anyhow::{Context, Result};
use async_trait::async_trait;
use gateway_messages::sp_impl::{SpHandler, SpServer};
use gateway_messages::{
    BulkIgnitionState, IgnitionCommand, IgnitionFlags, IgnitionState,
    ResponseError, SerialNumber, SpState,
};
use slog::{debug, error, info, warn, Logger};
use tokio::sync::{mpsc, oneshot};
use tokio::{
    select,
    task::{self, JoinHandle},
};

pub struct Sidecar {
    local_addr: SocketAddr,
    serial_number: SerialNumber,
    commands:
        mpsc::UnboundedSender<(Command, oneshot::Sender<CommandResponse>)>,
    inner_task: JoinHandle<()>,
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        // default join handle drop behavior is to detach; we want to abort
        self.inner_task.abort();
    }
}

#[async_trait]
impl SimulatedSp for Sidecar {
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

impl Sidecar {
    pub async fn spawn(
        config: &Config,
        sidecar_config: &SidecarConfig,
        log: Logger,
    ) -> Result<Self> {
        info!(log, "setting up simualted sidecar");
        let server = UdpServer::new(sidecar_config.bind_address).await?;
        let local_addr = server
            .socket()
            .local_addr()
            .with_context(|| "could not get local address of bound socket")?;

        let mut ignition_targets = Vec::new();
        for _ in &config.simulated_sps.sidecar {
            ignition_targets.push(IgnitionState {
                id: ignition_id::SIDECAR,
                flags: IgnitionFlags::POWER | IgnitionFlags::CTRL_DETECT_0,
            });
        }
        for _ in &config.simulated_sps.gimlet {
            ignition_targets.push(IgnitionState {
                id: ignition_id::GIMLET,
                flags: IgnitionFlags::POWER | IgnitionFlags::CTRL_DETECT_0,
            });
        }

        let (commands, commands_rx) = mpsc::unbounded_channel();
        let inner = Inner::new(
            server,
            sidecar_config.serial_number,
            ignition_targets,
            commands_rx,
            log,
        );
        let inner_task = task::spawn(async move { inner.run().await.unwrap() });
        Ok(Self {
            local_addr,
            serial_number: sidecar_config.serial_number,
            commands,
            inner_task,
        })
    }

    pub async fn current_ignition_state(&self) -> Vec<IgnitionState> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send((Command::CurrentIgnitionState, tx))
            .map_err(|_| "sidecar task died unexpectedly")
            .unwrap();
        match rx.await.unwrap() {
            CommandResponse::CurrentIgnitionState(state) => state,
            other => panic!("unexpected response {:?}", other),
        }
    }
}

#[derive(Debug)]
enum Command {
    CurrentIgnitionState,
    SetResponsiveness(Responsiveness),
}

#[derive(Debug)]
enum CommandResponse {
    CurrentIgnitionState(Vec<IgnitionState>),
    SetResponsivenessAck,
}

struct Inner {
    handler: Handler,
    udp: UdpServer,
    commands:
        mpsc::UnboundedReceiver<(Command, oneshot::Sender<CommandResponse>)>,
}

impl Inner {
    fn new(
        server: UdpServer,
        serial_number: SerialNumber,
        ignition_targets: Vec<IgnitionState>,
        commands: mpsc::UnboundedReceiver<(
            Command,
            oneshot::Sender<CommandResponse>,
        )>,
        log: Logger,
    ) -> Self {
        Self {
            handler: Handler { log, serial_number, ignition_targets },
            udp: server,
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
                        Command::CurrentIgnitionState => {
                            tx.send(CommandResponse::CurrentIgnitionState(
                                self.handler.ignition_targets.clone()
                            )).map_err(|_| "receiving half died").unwrap();
                        }
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
    fn ping(&mut self) -> Result<(), ResponseError> {
        debug!(&self.log, "received ping; sending pong");
        Ok(())
    }

    fn ignition_state(
        &mut self,
        target: u8,
    ) -> Result<IgnitionState, ResponseError> {
        let state = self.get_target(target)?;
        debug!(
            &self.log,
            "received ignition state request for {}; sending {:?}",
            target,
            state
        );
        Ok(*state)
    }

    fn bulk_ignition_state(
        &mut self,
    ) -> Result<BulkIgnitionState, ResponseError> {
        let num_targets = self.ignition_targets.len();
        assert!(
            num_targets <= BulkIgnitionState::MAX_IGNITION_TARGETS,
            "too many configured ignition targets (max is {})",
            BulkIgnitionState::MAX_IGNITION_TARGETS
        );
        let mut out = BulkIgnitionState {
            num_targets: u16::try_from(num_targets).unwrap(),
            targets: [IgnitionState::default();
                BulkIgnitionState::MAX_IGNITION_TARGETS],
        };
        out.targets[..num_targets].copy_from_slice(&self.ignition_targets);

        debug!(
            &self.log,
            "received bulk ignition state request; sending state for {} targets",
            num_targets,
        );
        Ok(out)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), ResponseError> {
        let state = self.get_target_mut(target)?;
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
        Ok(())
    }

    fn serial_console_write(
        &mut self,
        _packet: gateway_messages::SerialConsole,
    ) -> Result<(), ResponseError> {
        warn!(&self.log, "received request to write to serial console (unsupported on sidecar)");
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn sp_state(&mut self) -> Result<SpState, ResponseError> {
        let state = SpState { serial_number: self.serial_number };
        debug!(&self.log, "received state request; sending {:?}", state);
        Ok(state)
    }
}
