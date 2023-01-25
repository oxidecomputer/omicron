// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Config;
use crate::config::SidecarConfig;
use crate::config::SimulatedSpsConfig;
use crate::config::SpComponentConfig;
use crate::rot::RotSprocketExt;
use crate::serial_number_padded;
use crate::server;
use crate::server::UdpServer;
use crate::Responsiveness;
use crate::SimulatedSp;
use anyhow::Result;
use async_trait::async_trait;
use futures::future;
use gateway_messages::ignition;
use gateway_messages::ignition::IgnitionError;
use gateway_messages::ignition::LinkEvents;
use gateway_messages::sp_impl::BoundsChecked;
use gateway_messages::sp_impl::DeviceDescription;
use gateway_messages::sp_impl::SpHandler;
use gateway_messages::ComponentDetails;
use gateway_messages::DiscoverResponse;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::ImageVersion;
use gateway_messages::MgsError;
use gateway_messages::PowerState;
use gateway_messages::RotBootState;
use gateway_messages::RotSlot;
use gateway_messages::RotState;
use gateway_messages::RotUpdateDetails;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpState;
use gateway_messages::StartupOptions;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use sprockets_rot::common::msgs::RotRequestV1;
use sprockets_rot::common::msgs::RotResponseV1;
use sprockets_rot::common::Ed25519PublicKey;
use sprockets_rot::RotSprocket;
use sprockets_rot::RotSprocketError;
use std::iter;
use std::net::SocketAddrV6;
use std::sync::Mutex;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::task::JoinHandle;

const SIM_SIDECAR_VERSION: ImageVersion = ImageVersion { epoch: 0, version: 0 };

pub struct Sidecar {
    rot: Mutex<RotSprocket>,
    manufacturing_public_key: Ed25519PublicKey,
    local_addrs: Option<[SocketAddrV6; 2]>,
    serial_number: String,
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
        self.serial_number.clone()
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
            .map_err(|_| "sidecar task died unexpectedly")
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

        let (local_addrs, inner_task) =
            if let Some(bind_addrs) = sidecar.common.bind_addrs {
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

                let inner = Inner::new(
                    servers,
                    sidecar.common.components.clone(),
                    sidecar.common.serial_number.clone(),
                    FakeIgnition::new(&config.simulated_sps),
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
            serial_number: sidecar.common.serial_number.clone(),
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
        components: Vec<SpComponentConfig>,
        serial_number: String,
        ignition: FakeIgnition,
        commands: mpsc::UnboundedReceiver<(
            Command,
            oneshot::Sender<CommandResponse>,
        )>,
        log: Logger,
    ) -> Self {
        let [udp0, udp1] = servers;
        Self {
            handler: Handler::new(serial_number, components, ignition, log),
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
                                self.handler
                                    .ignition
                                    .state
                                    .clone()
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
    components: Vec<SpComponentConfig>,

    // `SpHandler` wants `&'static str` references when describing components;
    // this is fine on the real SP where the strings are baked in at build time,
    // but awkward here where we read them in at runtime. We'll leak the strings
    // to conform to `SpHandler` rather than making it more complicated to ease
    // our life as a simulator.
    leaked_component_device_strings: Vec<&'static str>,
    leaked_component_description_strings: Vec<&'static str>,

    serial_number: String,
    ignition: FakeIgnition,
    power_state: PowerState,
}

impl Handler {
    fn new(
        serial_number: String,
        components: Vec<SpComponentConfig>,
        ignition: FakeIgnition,
        log: Logger,
    ) -> Self {
        let mut leaked_component_device_strings =
            Vec::with_capacity(components.len());
        let mut leaked_component_description_strings =
            Vec::with_capacity(components.len());

        for c in &components {
            leaked_component_device_strings
                .push(&*Box::leak(c.device.clone().into_boxed_str()));
            leaked_component_description_strings
                .push(&*Box::leak(c.description.clone().into_boxed_str()));
        }

        Self {
            log,
            components,
            leaked_component_device_strings,
            leaked_component_description_strings,
            serial_number,
            ignition,
            power_state: PowerState::A2,
        }
    }
}

impl SpHandler for Handler {
    type BulkIgnitionStateIter = iter::Skip<std::vec::IntoIter<IgnitionState>>;
    type BulkIgnitionLinkEventsIter =
        iter::Skip<std::vec::IntoIter<LinkEvents>>;

    fn discover(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<gateway_messages::DiscoverResponse, SpError> {
        debug!(
            &self.log,
            "received discover; sending response";
            "sender" => %sender,
            "port" => ?port,
        );
        Ok(DiscoverResponse { sp_port: port })
    }

    fn num_ignition_ports(&mut self) -> Result<u32, SpError> {
        Ok(self.ignition.num_targets() as u32)
    }

    fn ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
    ) -> Result<IgnitionState, SpError> {
        let state = self.ignition.get_target(target)?;
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
        offset: u32,
    ) -> Result<Self::BulkIgnitionStateIter, SpError> {
        debug!(
            &self.log,
            "received bulk ignition state request";
            "sender" => %sender,
            "port" => ?port,
            "offset" => offset,
            "state" => ?self.ignition.state,
        );
        Ok(self.ignition.state.clone().into_iter().skip(offset as usize))
    }

    fn ignition_link_events(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
    ) -> Result<LinkEvents, SpError> {
        // Check validity of `target`
        _ = self.ignition.get_target(target)?;

        let events = self.ignition.link_events[usize::from(target)];

        debug!(
            &self.log,
            "received ignition link events request";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
            "events" => ?events,
        );

        Ok(events)
    }

    fn bulk_ignition_link_events(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        offset: u32,
    ) -> Result<Self::BulkIgnitionLinkEventsIter, SpError> {
        debug!(
            &self.log,
            "received bulk ignition link events request";
            "sender" => %sender,
            "port" => ?port,
            "offset" => offset,
        );
        Ok(self.ignition.link_events.clone().into_iter().skip(offset as usize))
    }

    /// If `target` is `None`, clear link events for all targets.
    fn clear_ignition_link_events(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: Option<u8>,
        transceiver_select: Option<ignition::TransceiverSelect>,
    ) -> Result<(), SpError> {
        let targets = match target {
            Some(t) => {
                // Check validity
                _ = self.ignition.get_target(t)?;
                usize::from(t)..usize::from(t) + 1
            }
            None => 0..self.ignition.num_targets(),
        };

        for t in targets {
            match transceiver_select {
                Some(ignition::TransceiverSelect::Controller) => {
                    self.ignition.link_events[t].controller =
                        empty_transceiver_events();
                }
                Some(ignition::TransceiverSelect::TargetLink0) => {
                    self.ignition.link_events[t].target_link0 =
                        empty_transceiver_events();
                }
                Some(ignition::TransceiverSelect::TargetLink1) => {
                    self.ignition.link_events[t].target_link1 =
                        empty_transceiver_events();
                }
                None => {
                    self.ignition.link_events[t] = empty_link_events();
                }
            }
        }

        debug!(
            &self.log,
            "cleared ignition link events";
            "sender" => %sender,
            "port" => ?port,
            "target" => ?target,
            "transceiver_select" => ?transceiver_select,
        );
        Ok(())
    }

    fn ignition_command(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), SpError> {
        self.ignition.command(target, command)?;
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
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received serial console attach; unsupported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_write(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        _offset: u64,
        _data: &[u8],
    ) -> Result<u64, SpError> {
        warn!(
            &self.log, "received serial console write; unsupported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_detach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received serial console detach; unsupported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn sp_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<SpState, SpError> {
        const FAKE_SIDECAR_MODEL: &[u8] = b"FAKE_SIM_SIDECAR";

        let mut model = [0; 32];
        model[..FAKE_SIDECAR_MODEL.len()].copy_from_slice(FAKE_SIDECAR_MODEL);

        let state = SpState {
            hubris_archive_id: [0; 8],
            serial_number: serial_number_padded(&self.serial_number),
            model,
            revision: 0,
            base_mac_address: [0; 6],
            version: SIM_SIDECAR_VERSION,
            power_state: self.power_state,
            rot: Ok(RotState {
                rot_updates: RotUpdateDetails {
                    // TODO replace with configurable data once something cares
                    // about this?
                    boot_state: RotBootState {
                        active: RotSlot::A,
                        slot_a: None,
                        slot_b: None,
                    },
                },
            }),
        };
        debug!(
            &self.log, "received state request";
            "sender" => %sender,
            "port" => ?port,
            "reply-state" => ?state,
        );
        Ok(state)
    }

    fn sp_update_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        update: gateway_messages::SpUpdatePrepare,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received update prepare request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "update" => ?update,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn component_update_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        update: gateway_messages::ComponentUpdatePrepare,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received update prepare request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "update" => ?update,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn update_status(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<gateway_messages::UpdateStatus, SpError> {
        warn!(
            &self.log,
            "received update status request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn update_chunk(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        chunk: gateway_messages::UpdateChunk,
        data: &[u8],
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received update chunk; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "offset" => chunk.offset,
            "length" => data.len(),
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn update_abort(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
        id: gateway_messages::UpdateId,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received update abort; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
            "id" => ?id,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn power_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<gateway_messages::PowerState, SpError> {
        debug!(
            &self.log, "received power state";
            "sender" => %sender,
            "port" => ?port,
            "power_state" => ?self.power_state,
        );
        Ok(self.power_state)
    }

    fn set_power_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        power_state: gateway_messages::PowerState,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received set power state";
            "sender" => %sender,
            "port" => ?port,
            "power_state" => ?power_state,
        );
        self.power_state = power_state;
        Ok(())
    }

    fn reset_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received sys-reset prepare request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn reset_trigger(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<std::convert::Infallible, SpError> {
        warn!(
            &self.log, "received sys-reset trigger request; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn num_devices(&mut self, _: SocketAddrV6, _: SpPort) -> u32 {
        self.components.len().try_into().unwrap()
    }

    fn device_description(
        &mut self,
        index: BoundsChecked,
    ) -> DeviceDescription<'static> {
        let index = index.0 as usize;
        let c = &self.components[index];
        DeviceDescription {
            component: SpComponent::try_from(c.id.as_str()).unwrap(),
            device: self.leaked_component_device_strings[index],
            description: self.leaked_component_description_strings[index],
            capabilities: c.capabilities,
            presence: c.presence,
        }
    }

    fn num_component_details(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<u32, SpError> {
        warn!(
            &self.log, "asked for component details (returning 0 details)";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        Ok(0)
    }

    fn component_details(
        &mut self,
        component: SpComponent,
        index: BoundsChecked,
    ) -> ComponentDetails {
        // We return 0 for all components, so we should never be called (`index`
        // would have to have been bounds checked to live in 0..0).
        unreachable!("asked for {component:?} details index {index:?}")
    }

    fn component_clear_status(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to clear status (not supported for sim components)";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn component_get_active_slot(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<u16, SpError> {
        warn!(
            &self.log, "asked for component active slot (not supported for sim components)";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn component_set_active_slot(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
        slot: u16,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to set component active slot (not supported for sim components)";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
            "slot" => slot,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn get_startup_options(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<StartupOptions, SpError> {
        warn!(
            &self.log, "asked for startup options (unsupported by sidecar)";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn set_startup_options(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        startup_options: StartupOptions,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to set startup options (unsupported by sidecar)";
            "sender" => %sender,
            "port" => ?port,
            "options" => ?startup_options,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn mgs_response_error(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        message_id: u32,
        err: MgsError,
    ) {
        warn!(
            &self.log, "received MGS error response";
            "sender" => %sender,
            "port" => ?port,
            "message_id" => message_id,
            "err" => ?err,
        );
    }

    fn mgs_response_host_phase2_data(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        message_id: u32,
        hash: [u8; 32],
        offset: u64,
        data: &[u8],
    ) {
        debug!(
            &self.log, "received host phase 2 data from MGS";
            "sender" => %sender,
            "port" => ?port,
            "message_id" => message_id,
            "hash" => ?hash,
            "offset" => offset,
            "data_len" => data.len(),
        );
    }
}

struct FakeIgnition {
    state: Vec<IgnitionState>,
    link_events: Vec<LinkEvents>,
}

fn empty_transceiver_events() -> ignition::TransceiverEvents {
    ignition::TransceiverEvents {
        encoding_error: false,
        decoding_error: false,
        ordered_set_invalid: false,
        message_version_invalid: false,
        message_type_invalid: false,
        message_checksum_invalid: false,
    }
}

fn empty_link_events() -> LinkEvents {
    LinkEvents {
        controller: empty_transceiver_events(),
        target_link0: empty_transceiver_events(),
        target_link1: empty_transceiver_events(),
    }
}

fn initial_ignition_state(system_type: ignition::SystemType) -> IgnitionState {
    fn valid_receiver() -> ignition::ReceiverStatus {
        ignition::ReceiverStatus {
            aligned: true,
            locked: true,
            polarity_inverted: false,
        }
    }
    IgnitionState {
        receiver: valid_receiver(),
        target: Some(ignition::TargetState {
            system_type,
            power_state: ignition::SystemPowerState::On,
            power_reset_in_progress: false,
            faults: ignition::SystemFaults {
                power_a3: false,
                power_a2: false,
                sp: false,
                rot: false,
            },
            controller0_present: true,
            controller1_present: false,
            link0_receiver_status: valid_receiver(),
            link1_receiver_status: valid_receiver(),
        }),
    }
}

impl FakeIgnition {
    // Ignition always has 35 ports: 32 sleds, 2 psc, 1 sidecar (the other one)
    const NUM_IGNITION_TARGETS: usize = 35;

    fn new(config: &SimulatedSpsConfig) -> Self {
        let mut state = Vec::new();

        for _ in &config.sidecar {
            state.push(initial_ignition_state(ignition::SystemType::Sidecar));
        }
        for _ in &config.gimlet {
            state.push(initial_ignition_state(ignition::SystemType::Gimlet));
        }

        assert!(
            state.len() <= Self::NUM_IGNITION_TARGETS,
            "too many simulated SPs"
        );
        while state.len() < Self::NUM_IGNITION_TARGETS {
            state.push(IgnitionState {
                receiver: ignition::ReceiverStatus {
                    aligned: false,
                    locked: false,
                    polarity_inverted: false,
                },
                target: None,
            });
        }

        Self {
            state,
            link_events: vec![empty_link_events(); Self::NUM_IGNITION_TARGETS],
        }
    }

    fn num_targets(&self) -> usize {
        self.state.len()
    }

    fn get_target(&self, target: u8) -> Result<&IgnitionState, SpError> {
        self.state
            .get(usize::from(target))
            .ok_or(SpError::Ignition(IgnitionError::InvalidPort))
    }

    fn get_target_mut(
        &mut self,
        target: u8,
    ) -> Result<&mut IgnitionState, SpError> {
        self.state
            .get_mut(usize::from(target))
            .ok_or(SpError::Ignition(IgnitionError::InvalidPort))
    }

    fn command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), SpError> {
        let target = self
            .get_target_mut(target)?
            .target
            .as_mut()
            .ok_or(SpError::Ignition(IgnitionError::NoTargetPresent))?;

        match command {
            IgnitionCommand::PowerOn | IgnitionCommand::PowerReset => {
                target.power_state = ignition::SystemPowerState::On;
            }
            IgnitionCommand::PowerOff => {
                target.power_state = ignition::SystemPowerState::Off;
            }
        }

        Ok(())
    }
}
