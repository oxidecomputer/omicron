// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Config;
use crate::config::SidecarConfig;
use crate::config::SimulatedSpsConfig;
use crate::config::SpComponentConfig;
use crate::helpers::rot_slot_id_from_u16;
use crate::helpers::rot_slot_id_to_u16;
use crate::serial_number_padded;
use crate::server;
use crate::server::SimSpHandler;
use crate::server::UdpServer;
use crate::update::SimSpUpdate;
use crate::Responsiveness;
use crate::SimulatedSp;
use crate::SIM_ROT_BOARD;
use crate::SIM_ROT_STAGE0_BOARD;
use anyhow::Result;
use async_trait::async_trait;
use futures::future;
use futures::Future;
use gateway_messages::ignition;
use gateway_messages::ignition::IgnitionError;
use gateway_messages::ignition::LinkEvents;
use gateway_messages::sp_impl::BoundsChecked;
use gateway_messages::sp_impl::DeviceDescription;
use gateway_messages::sp_impl::SpHandler;
use gateway_messages::CfpaPage;
use gateway_messages::ComponentAction;
use gateway_messages::ComponentDetails;
use gateway_messages::DiscoverResponse;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::MgsError;
use gateway_messages::PowerState;
use gateway_messages::RotBootInfo;
use gateway_messages::RotRequest;
use gateway_messages::RotResponse;
use gateway_messages::RotSlotId;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpStateV2;
use gateway_messages::StartupOptions;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use std::iter;
use std::net::SocketAddrV6;
use std::pin::Pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Mutex as TokioMutex;
use tokio::task;
use tokio::task::JoinHandle;

pub const SIM_SIDECAR_BOARD: &str = "SimSidecarSp";

pub struct Sidecar {
    local_addrs: Option<[SocketAddrV6; 2]>,
    handler: Option<Arc<TokioMutex<Handler>>>,
    commands: mpsc::UnboundedSender<Command>,
    inner_task: Option<JoinHandle<()>>,
    responses_sent_count: Option<watch::Receiver<usize>>,
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
    async fn state(&self) -> omicron_gateway::http_entrypoints::SpState {
        omicron_gateway::http_entrypoints::SpState::from(
            self.handler.as_ref().unwrap().lock().await.sp_state_impl(),
        )
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
            .send(Command::SetResponsiveness(r, tx))
            .map_err(|_| "sidecar task died unexpectedly")
            .unwrap();
        rx.await.unwrap();
    }

    async fn last_sp_update_data(&self) -> Option<Box<[u8]>> {
        let handler = self.handler.as_ref()?;
        let handler = handler.lock().await;
        handler.update_state.last_sp_update_data()
    }

    async fn last_rot_update_data(&self) -> Option<Box<[u8]>> {
        let handler = self.handler.as_ref()?;
        let handler = handler.lock().await;
        handler.update_state.last_rot_update_data()
    }

    async fn last_host_phase1_update_data(
        &self,
        _slot: u16,
    ) -> Option<Box<[u8]>> {
        // sidecars do not have attached hosts
        None
    }

    async fn current_update_status(&self) -> gateway_messages::UpdateStatus {
        let Some(handler) = self.handler.as_ref() else {
            return gateway_messages::UpdateStatus::None;
        };

        handler.lock().await.update_state.status()
    }

    fn responses_sent_count(&self) -> Option<watch::Receiver<usize>> {
        self.responses_sent_count.clone()
    }

    async fn install_udp_accept_semaphore(
        &self,
    ) -> mpsc::UnboundedSender<usize> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (resp_tx, resp_rx) = oneshot::channel();
        if let Ok(()) =
            self.commands.send(Command::SetThrottler(Some(rx), resp_tx))
        {
            resp_rx.await.unwrap();
        }
        tx
    }
}

impl Sidecar {
    pub async fn spawn(
        config: &Config,
        sidecar: &SidecarConfig,
        log: Logger,
    ) -> Result<Self> {
        info!(log, "setting up simulated sidecar");

        let (commands, commands_rx) = mpsc::unbounded_channel();

        let (local_addrs, inner_task, handler, responses_sent_count) =
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

                let (inner, handler, responses_sent_count) = Inner::new(
                    servers,
                    sidecar.common.components.clone(),
                    sidecar.common.serial_number.clone(),
                    FakeIgnition::new(&config.simulated_sps),
                    commands_rx,
                    log,
                    sidecar.common.old_rot_state,
                    sidecar.common.no_stage0_caboose,
                );
                let inner_task =
                    task::spawn(async move { inner.run().await.unwrap() });

                (
                    Some(local_addrs),
                    Some(inner_task),
                    Some(handler),
                    Some(responses_sent_count),
                )
            } else {
                (None, None, None, None)
            };

        Ok(Self {
            local_addrs,
            handler,
            commands,
            inner_task,
            responses_sent_count,
        })
    }

    pub async fn current_ignition_state(&self) -> Vec<IgnitionState> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::CurrentIgnitionState(tx))
            .map_err(|_| "sidecar task died unexpectedly")
            .unwrap();
        rx.await.unwrap()
    }
}

#[derive(Debug)]
enum Command {
    CurrentIgnitionState(oneshot::Sender<Vec<IgnitionState>>),
    SetResponsiveness(Responsiveness, oneshot::Sender<Ack>),
    SetThrottler(Option<mpsc::UnboundedReceiver<usize>>, oneshot::Sender<Ack>),
}

#[derive(Debug)]
struct Ack;

struct Inner {
    handler: Arc<TokioMutex<Handler>>,
    udp0: UdpServer,
    udp1: UdpServer,
    commands: mpsc::UnboundedReceiver<Command>,
    responses_sent_count: watch::Sender<usize>,
}

impl Inner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        servers: [UdpServer; 2],
        components: Vec<SpComponentConfig>,
        serial_number: String,
        ignition: FakeIgnition,
        commands: mpsc::UnboundedReceiver<Command>,
        log: Logger,
        old_rot_state: bool,
        no_stage0_caboose: bool,
    ) -> (Self, Arc<TokioMutex<Handler>>, watch::Receiver<usize>) {
        let [udp0, udp1] = servers;
        let handler = Arc::new(TokioMutex::new(Handler::new(
            serial_number,
            components,
            ignition,
            log,
            old_rot_state,
            no_stage0_caboose,
        )));
        let responses_sent_count = watch::Sender::new(0);
        let responses_sent_count_rx = responses_sent_count.subscribe();
        (
            Self {
                handler: Arc::clone(&handler),
                udp0,
                udp1,
                commands,
                responses_sent_count,
            },
            handler,
            responses_sent_count_rx,
        )
    }

    async fn run(mut self) -> Result<()> {
        let mut out_buf = [0; gateway_messages::MAX_SERIALIZED_SIZE];
        let mut responsiveness = Responsiveness::Responsive;
        let mut throttle_count = usize::MAX;
        let mut throttler: Option<mpsc::UnboundedReceiver<usize>> = None;
        loop {
            let incr_throttle_count: Pin<
                Box<dyn Future<Output = Option<usize>> + Send>,
            > = if let Some(throttler) = throttler.as_mut() {
                Box::pin(throttler.recv())
            } else {
                Box::pin(future::pending())
            };
            select! {
                Some(n) = incr_throttle_count => {
                    throttle_count = throttle_count.saturating_add(n);
                }

                recv0 = self.udp0.recv_from(), if throttle_count > 0 => {
                    if let Some((resp, addr)) = server::handle_request(
                        &mut *self.handler.lock().await,
                        recv0,
                        &mut out_buf,
                        responsiveness,
                        SpPort::One,
                    ).await? {
                        throttle_count -= 1;
                        self.udp0.send_to(resp, addr).await?;
                        self.responses_sent_count.send_modify(|n| *n += 1);
                    }
                }

                recv1 = self.udp1.recv_from(), if throttle_count > 0 => {
                    if let Some((resp, addr)) = server::handle_request(
                        &mut *self.handler.lock().await,
                        recv1,
                        &mut out_buf,
                        responsiveness,
                        SpPort::Two,
                    ).await? {
                        throttle_count -= 1;
                        self.udp1.send_to(resp, addr).await?;
                        self.responses_sent_count.send_modify(|n| *n += 1);
                    }
                }

                command = self.commands.recv() => {
                    // if sending half is gone, we're about to be killed anyway
                    let command = match command {
                        Some(command) => command,
                        None => return Ok(()),
                    };

                    match command {
                        Command::CurrentIgnitionState(tx) => {
                            tx.send(self.handler
                                .lock()
                                .await
                                .ignition
                                .state
                                .clone()
                            ).map_err(|_| "receiving half died").unwrap();
                        }
                        Command::SetResponsiveness(r, tx) => {
                            responsiveness = r;
                            tx.send(Ack)
                                .map_err(|_| "receiving half died").unwrap();
                        }
                        Command::SetThrottler(thr, tx) => {
                            throttler = thr;

                            // Either immediately start throttling, or
                            // immediately stop throttling.
                            if throttler.is_some() {
                                throttle_count = 0;
                            } else {
                                throttle_count = usize::MAX;
                            }
                            tx.send(Ack)
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
    rot_active_slot: RotSlotId,
    power_state: PowerState,

    update_state: SimSpUpdate,
    reset_pending: Option<SpComponent>,

    // To simulate an SP reset, we should (after doing whatever housekeeping we
    // need to track the reset) intentionally _fail_ to respond to the request,
    // simulating a `-> !` function on the SP that triggers a reset. To provide
    // this, our caller will pass us a function to call if they should ignore
    // whatever result we return and fail to respond at all.
    should_fail_to_respond_signal: Option<Box<dyn FnOnce() + Send>>,
    no_stage0_caboose: bool,
    old_rot_state: bool,
}

impl Handler {
    fn new(
        serial_number: String,
        components: Vec<SpComponentConfig>,
        ignition: FakeIgnition,
        log: Logger,
        old_rot_state: bool,
        no_stage0_caboose: bool,
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
            rot_active_slot: RotSlotId::A,
            power_state: PowerState::A2,
            update_state: SimSpUpdate::default(),
            reset_pending: None,
            should_fail_to_respond_signal: None,
            old_rot_state,
            no_stage0_caboose,
        }
    }

    fn sp_state_impl(&self) -> SpStateV2 {
        const FAKE_SIDECAR_MODEL: &[u8] = b"FAKE_SIM_SIDECAR";

        let mut model = [0; 32];
        model[..FAKE_SIDECAR_MODEL.len()].copy_from_slice(FAKE_SIDECAR_MODEL);

        SpStateV2 {
            hubris_archive_id: [0; 8],
            serial_number: serial_number_padded(&self.serial_number),
            model,
            revision: 0,
            base_mac_address: [0; 6],
            power_state: self.power_state,
            rot: Ok(gateway_messages::RotStateV2 {
                active: RotSlotId::A,
                persistent_boot_preference: RotSlotId::A,
                pending_persistent_boot_preference: None,
                transient_boot_preference: None,
                slot_a_sha3_256_digest: None,
                slot_b_sha3_256_digest: None,
            }),
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

    fn serial_console_keepalive(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console keepalive; unsupported by sidecar";
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

    fn serial_console_break(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console break; not supported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn send_host_nmi(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received host NMI request; not supported by sidecar";
            "sender" => %sender,
            "port" => ?port,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn sp_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<SpStateV2, SpError> {
        let state = self.sp_state_impl();
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
        debug!(
            &self.log,
            "received update prepare request";
            "sender" => %sender,
            "port" => ?port,
            "update" => ?update,
        );
        self.update_state.prepare(
            SpComponent::SP_ITSELF,
            update.id,
            update.sp_image_size.try_into().unwrap(),
        )
    }

    fn component_update_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        update: gateway_messages::ComponentUpdatePrepare,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update prepare request";
            "sender" => %sender,
            "port" => ?port,
            "update" => ?update,
        );
        self.update_state.prepare(
            update.component,
            update.id,
            update.total_size.try_into().unwrap(),
        )
    }

    fn update_status(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<gateway_messages::UpdateStatus, SpError> {
        debug!(
            &self.log,
            "received update status request";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        Ok(self.update_state.status())
    }

    fn update_chunk(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        chunk: gateway_messages::UpdateChunk,
        chunk_data: &[u8],
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update chunk";
            "sender" => %sender,
            "port" => ?port,
            "offset" => chunk.offset,
            "length" => chunk_data.len(),
        );
        self.update_state.ingest_chunk(chunk, chunk_data)
    }

    fn update_abort(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
        update_id: gateway_messages::UpdateId,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update abort; not supported by simulated sidecar";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
            "id" => ?update_id,
        );
        self.update_state.abort(update_id)
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

    fn reset_component_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received reset prepare request";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        if component == SpComponent::SP_ITSELF || component == SpComponent::ROT
        {
            self.reset_pending = Some(component);
            Ok(())
        } else {
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn reset_component_trigger(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received sys-reset trigger request";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        if component == SpComponent::SP_ITSELF {
            if self.reset_pending == Some(SpComponent::SP_ITSELF) {
                self.update_state.sp_reset();
                self.reset_pending = None;
                if let Some(signal) = self.should_fail_to_respond_signal.take()
                {
                    // Instruct `server::handle_request()` to _not_ respond to
                    // this request at all, simulating an SP actually resetting.
                    signal();
                }
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else if component == SpComponent::ROT {
            if self.reset_pending == Some(SpComponent::ROT) {
                self.update_state.rot_reset();
                self.reset_pending = None;
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else {
            Err(SpError::RequestUnsupportedForComponent)
        }
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
            &self.log, "asked for component active slot";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        if component == SpComponent::ROT {
            Ok(rot_slot_id_to_u16(self.rot_active_slot))
        } else {
            // The real SP returns `RequestUnsupportedForComponent` for anything
            // other than the RoT, including SP_ITSELF.
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn component_set_active_slot(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
        slot: u16,
        persist: bool,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to set component active slot";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
            "slot" => slot,
            "persist" => persist,
        );
        if component == SpComponent::ROT {
            self.rot_active_slot = rot_slot_id_from_u16(slot)?;
            Ok(())
        } else {
            // The real SP returns `RequestUnsupportedForComponent` for anything
            // other than the RoT, including SP_ITSELF.
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn component_action(
        &mut self,
        sender: SocketAddrV6,
        component: SpComponent,
        action: ComponentAction,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to perform component action (not supported for sim components)";
            "sender" => %sender,
            "component" => ?component,
            "action" => ?action,
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

    fn set_ipcc_key_lookup_value(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        key: u8,
        value: &[u8],
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received IPCC key/value; not supported by sidecar";
            "sender" => %sender,
            "port" => ?port,
            "key" => key,
            "value" => ?value,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_component_caboose_value(
        &mut self,
        component: SpComponent,
        slot: u16,
        key: [u8; 4],
        buf: &mut [u8],
    ) -> std::result::Result<usize, SpError> {
        static SP_GITC0: &[u8] = b"ffffffff";
        static SP_GITC1: &[u8] = b"fefefefe";
        static SP_BORD: &[u8] = SIM_SIDECAR_BOARD.as_bytes();
        static SP_NAME: &[u8] = b"SimSidecar";
        static SP_VERS0: &[u8] = b"0.0.2";
        static SP_VERS1: &[u8] = b"0.0.1";

        static ROT_GITC0: &[u8] = b"eeeeeeee";
        static ROT_GITC1: &[u8] = b"edededed";
        static ROT_BORD: &[u8] = SIM_ROT_BOARD.as_bytes();
        static ROT_NAME: &[u8] = b"SimSidecar";
        static ROT_VERS0: &[u8] = b"0.0.4";
        static ROT_VERS1: &[u8] = b"0.0.3";

        static STAGE0_GITC0: &[u8] = b"dddddddd";
        static STAGE0_GITC1: &[u8] = b"dadadada";
        static STAGE0_BORD: &[u8] = SIM_ROT_STAGE0_BOARD.as_bytes();
        static STAGE0_NAME: &[u8] = b"SimSidecar";
        static STAGE0_VERS0: &[u8] = b"0.0.200";
        static STAGE0_VERS1: &[u8] = b"0.0.200";

        let val = match (component, &key, slot, self.no_stage0_caboose) {
            (SpComponent::SP_ITSELF, b"GITC", 0, _) => SP_GITC0,
            (SpComponent::SP_ITSELF, b"GITC", 1, _) => SP_GITC1,
            (SpComponent::SP_ITSELF, b"BORD", _, _) => SP_BORD,
            (SpComponent::SP_ITSELF, b"NAME", _, _) => SP_NAME,
            (SpComponent::SP_ITSELF, b"VERS", 0, _) => SP_VERS0,
            (SpComponent::SP_ITSELF, b"VERS", 1, _) => SP_VERS1,
            (SpComponent::ROT, b"GITC", 0, _) => ROT_GITC0,
            (SpComponent::ROT, b"GITC", 1, _) => ROT_GITC1,
            (SpComponent::ROT, b"BORD", _, _) => ROT_BORD,
            (SpComponent::ROT, b"NAME", _, _) => ROT_NAME,
            (SpComponent::ROT, b"VERS", 0, _) => ROT_VERS0,
            (SpComponent::ROT, b"VERS", 1, _) => ROT_VERS1,
            (SpComponent::STAGE0, b"GITC", 0, false) => STAGE0_GITC0,
            (SpComponent::STAGE0, b"GITC", 1, false) => STAGE0_GITC1,
            (SpComponent::STAGE0, b"BORD", _, false) => STAGE0_BORD,
            (SpComponent::STAGE0, b"NAME", _, false) => STAGE0_NAME,
            (SpComponent::STAGE0, b"VERS", 0, false) => STAGE0_VERS0,
            (SpComponent::STAGE0, b"VERS", 1, false) => STAGE0_VERS1,
            _ => return Err(SpError::NoSuchCabooseKey(key)),
        };

        buf[..val.len()].copy_from_slice(val);
        Ok(val.len())
    }

    fn read_sensor(
        &mut self,
        _request: gateway_messages::SensorRequest,
    ) -> std::result::Result<gateway_messages::SensorResponse, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn current_time(&mut self) -> std::result::Result<u64, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn read_rot(
        &mut self,
        request: RotRequest,
        buf: &mut [u8],
    ) -> std::result::Result<RotResponse, SpError> {
        let dummy_page = match request {
            RotRequest::ReadCmpa => "sidecar-cmpa",
            RotRequest::ReadCfpa(CfpaPage::Active) => "sidecar-cfpa-active",
            RotRequest::ReadCfpa(CfpaPage::Inactive) => "sidecar-cfpa-inactive",
            RotRequest::ReadCfpa(CfpaPage::Scratch) => "sidecar-cfpa-scratch",
        };
        buf[..dummy_page.len()].copy_from_slice(dummy_page.as_bytes());
        buf[dummy_page.len()..].fill(0);
        Ok(RotResponse::Ok)
    }

    fn vpd_lock_status_all(
        &mut self,
        _buf: &mut [u8],
    ) -> Result<usize, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn reset_component_trigger_with_watchdog(
        &mut self,
        component: SpComponent,
        _time_ms: u32,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received sys-reset trigger with wathcdog request";
            "component" => ?component,
        );
        if component == SpComponent::SP_ITSELF {
            if self.reset_pending == Some(SpComponent::SP_ITSELF) {
                self.update_state.sp_reset();
                self.reset_pending = None;
                if let Some(signal) = self.should_fail_to_respond_signal.take()
                {
                    // Instruct `server::handle_request()` to _not_ respond to
                    // this request at all, simulating an SP actually resetting.
                    signal();
                }
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else if component == SpComponent::ROT {
            if self.reset_pending == Some(SpComponent::ROT) {
                self.update_state.rot_reset();
                self.reset_pending = None;
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else {
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn disable_component_watchdog(
        &mut self,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        Ok(())
    }
    fn component_watchdog_supported(
        &mut self,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        Ok(())
    }

    fn versioned_rot_boot_info(
        &mut self,
        _sender: SocketAddrV6,
        _port: SpPort,
        version: u8,
    ) -> Result<RotBootInfo, SpError> {
        if self.old_rot_state {
            Err(SpError::RequestUnsupportedForSp)
        } else {
            const SLOT_A_DIGEST: [u8; 32] = [0xaa; 32];
            const SLOT_B_DIGEST: [u8; 32] = [0xbb; 32];
            const STAGE0_DIGEST: [u8; 32] = [0xcc; 32];
            const STAGE0NEXT_DIGEST: [u8; 32] = [0xdd; 32];

            match version {
                0 => Err(SpError::Update(
                    gateway_messages::UpdateError::VersionNotSupported,
                )),
                1 => Ok(RotBootInfo::V2(gateway_messages::RotStateV2 {
                    active: RotSlotId::A,
                    persistent_boot_preference: RotSlotId::A,
                    pending_persistent_boot_preference: None,
                    transient_boot_preference: None,
                    slot_a_sha3_256_digest: Some(SLOT_A_DIGEST),
                    slot_b_sha3_256_digest: Some(SLOT_B_DIGEST),
                })),
                _ => Ok(RotBootInfo::V3(gateway_messages::RotStateV3 {
                    active: RotSlotId::A,
                    persistent_boot_preference: RotSlotId::A,
                    pending_persistent_boot_preference: None,
                    transient_boot_preference: None,
                    slot_a_fwid: gateway_messages::Fwid::Sha3_256(
                        SLOT_A_DIGEST,
                    ),
                    slot_b_fwid: gateway_messages::Fwid::Sha3_256(
                        SLOT_B_DIGEST,
                    ),
                    stage0_fwid: gateway_messages::Fwid::Sha3_256(
                        STAGE0_DIGEST,
                    ),
                    stage0next_fwid: gateway_messages::Fwid::Sha3_256(
                        STAGE0NEXT_DIGEST,
                    ),
                    slot_a_status: Ok(()),
                    slot_b_status: Ok(()),
                    stage0_status: Ok(()),
                    stage0next_status: Ok(()),
                })),
            }
        }
    }
}

impl SimSpHandler for Handler {
    fn set_sp_should_fail_to_respond_signal(
        &mut self,
        signal: Box<dyn FnOnce() + Send>,
    ) {
        self.should_fail_to_respond_signal = Some(signal);
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
