// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Config;
use crate::config::SidecarConfig;
use crate::ignition_id;
use crate::rot::RotSprocketExt;
use crate::server;
use crate::server::UdpServer;
use crate::Responsiveness;
use crate::SimulatedSp;
use anyhow::Result;
use async_trait::async_trait;
use futures::future;
use gateway_messages::sp_impl::SpHandler;
use gateway_messages::BulkIgnitionState;
use gateway_messages::DiscoverResponse;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionFlags;
use gateway_messages::IgnitionState;
use gateway_messages::ResponseError;
use gateway_messages::SerialNumber;
use gateway_messages::SpComponent;
use gateway_messages::SpPort;
use gateway_messages::SpState;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use sprockets_rot::common::msgs::RotRequestV1;
use sprockets_rot::common::msgs::RotResponseV1;
use sprockets_rot::common::Ed25519PublicKey;
use sprockets_rot::RotSprocket;
use sprockets_rot::RotSprocketError;
use std::net::SocketAddrV6;
use std::sync::Mutex;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::task::JoinHandle;

const SIM_SIDECAR_VERSION: u32 = 1;

pub struct Sidecar {
    rot: Mutex<RotSprocket>,
    manufacturing_public_key: Ed25519PublicKey,
    local_addrs: Option<[SocketAddrV6; 2]>,
    serial_number: SerialNumber,
    commands:
        mpsc::UnboundedSender<(Command, oneshot::Sender<CommandResponse>)>,
    inner_task: Option<JoinHandle<()>>,
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        if let Some(inner_task) = self.inner_task.as_ref() {
            // default join handle drop behavior is to detach; we want to abort
            inner_task.abort();
        }
    }
}

#[async_trait]
impl SimulatedSp for Sidecar {
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
        self.commands
            .send((Command::SetResponsiveness(r), tx))
            .map_err(|_| "gimlet task died unexpectedly")
            .unwrap();
        rx.await.unwrap();
    }

    fn rot_request(
        &self,
        request: RotRequestV1,
    ) -> Result<RotResponseV1, RotSprocketError> {
        self.rot.lock().unwrap().handle_deserialized(request)
    }
}

impl Sidecar {
    pub async fn spawn(
        config: &Config,
        sidecar: &SidecarConfig,
        log: Logger,
    ) -> Result<Self> {
        info!(log, "setting up simualted sidecar");

        let (commands, commands_rx) = mpsc::unbounded_channel();

        let (local_addrs, inner_task) = if let Some(bind_addrs) =
            sidecar.common.bind_addrs
        {
            // bind to our two local "KSZ" ports
            assert_eq!(bind_addrs.len(), 2);
            let servers = future::try_join(
                UdpServer::new(
                    bind_addrs[0],
                    sidecar.common.multicast_addr,
                    &log,
                ),
                UdpServer::new(
                    bind_addrs[1],
                    sidecar.common.multicast_addr,
                    &log,
                ),
            )
            .await?;
            let servers = [servers.0, servers.1];
            let local_addrs =
                [servers[0].local_addr(), servers[1].local_addr()];

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

            let inner = Inner::new(
                servers,
                sidecar.common.serial_number,
                ignition_targets,
                commands_rx,
                log,
            );
            let inner_task =
                task::spawn(async move { inner.run().await.unwrap() });

            (Some(local_addrs), Some(inner_task))
        } else {
            (None, None)
        };

        let (manufacturing_public_key, rot) =
            RotSprocket::bootstrap_from_config(&sidecar.common);
        Ok(Self {
            rot: Mutex::new(rot),
            manufacturing_public_key,
            local_addrs,
            serial_number: sidecar.common.serial_number,
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
    udp0: UdpServer,
    udp1: UdpServer,
    commands:
        mpsc::UnboundedReceiver<(Command, oneshot::Sender<CommandResponse>)>,
}

impl Inner {
    fn new(
        servers: [UdpServer; 2],
        serial_number: SerialNumber,
        ignition_targets: Vec<IgnitionState>,
        commands: mpsc::UnboundedReceiver<(
            Command,
            oneshot::Sender<CommandResponse>,
        )>,
        log: Logger,
    ) -> Self {
        let [udp0, udp1] = servers;
        Self {
            handler: Handler { log, serial_number, ignition_targets },
            udp0,
            udp1,
            commands,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut out_buf = [0; gateway_messages::MAX_SERIALIZED_SIZE];
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
    fn discover(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<gateway_messages::DiscoverResponse, ResponseError> {
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
    ) -> Result<IgnitionState, ResponseError> {
        let state = self.get_target(target)?;
        debug!(
            &self.log,
            "received ignition state request";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
            "reply-state" => ?state,
        );
        Ok(*state)
    }

    fn bulk_ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<BulkIgnitionState, ResponseError> {
        let num_targets = self.ignition_targets.len();
        assert!(
            num_targets <= BulkIgnitionState::MAX_IGNITION_TARGETS,
            "too many configured ignition targets (max is {})",
            BulkIgnitionState::MAX_IGNITION_TARGETS
        );
        let mut out = BulkIgnitionState {
            targets: [IgnitionState::default();
                BulkIgnitionState::MAX_IGNITION_TARGETS],
        };
        out.targets[..num_targets].copy_from_slice(&self.ignition_targets);

        debug!(
            &self.log,
            "received bulk ignition state request; sending state for {} targets",
            num_targets;
            "sender" => %sender,
            "port" => ?port,
        );
        Ok(out)
    }

    fn ignition_command(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
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
            "received ignition command; sending ack";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
            "command" => ?command,
        );
        Ok(())
    }

    fn serial_console_attach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        _component: SpComponent,
    ) -> Result<(), ResponseError>
    {
        warn!(
            &self.log, "received serial console attach; unsupported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn serial_console_write(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        _offset: u64,
        _data: &[u8],
    ) -> Result<u64, ResponseError> {
        warn!(
            &self.log, "received serial console write; unsupported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn serial_console_detach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), ResponseError>
    {
        warn!(
            &self.log, "received serial console detach; unsupported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn sp_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<SpState, ResponseError> {
        let state = SpState {
            serial_number: self.serial_number,
            version: SIM_SIDECAR_VERSION,
        };
        debug!(
            &self.log, "received state request";
            "sender" => %sender,
            "port" => ?port,
            "reply-state" => ?state,
        );
        Ok(state)
    }

    fn update_start(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        update: gateway_messages::UpdateStart,
    ) -> Result<(), ResponseError> {
        warn!(
            &self.log,
            "received update start request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "update" => ?update,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn update_chunk(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        chunk: gateway_messages::UpdateChunk,
        data: &[u8],
    ) -> Result<(), ResponseError> {
        warn!(
            &self.log,
            "received update chunk; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "offset" => chunk.offset,
            "length" => data.len(),
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn reset_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), ResponseError> {
        warn!(
            &self.log, "received sys-reset prepare request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }

    fn reset_trigger(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<std::convert::Infallible, ResponseError> {
        warn!(
            &self.log, "received sys-reset trigger request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(ResponseError::RequestUnsupportedForSp)
    }
}
