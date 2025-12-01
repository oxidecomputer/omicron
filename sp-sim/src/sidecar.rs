// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::HostFlashHashPolicy;
use crate::Responsiveness;
use crate::SimulatedSp;
use crate::config::Config;
use crate::config::SidecarConfig;
use crate::config::SimulatedSpsConfig;
use crate::config::SpComponentConfig;
use crate::ereport;
use crate::ereport::EreportState;
use crate::helpers::rot_state_v2;
use crate::sensors::Sensors;
use crate::serial_number_padded;
use crate::server;
use crate::server::SimSpHandler;
use crate::server::UdpServer;
use crate::update::BaseboardKind;
use crate::update::SimSpUpdate;
use anyhow::Result;
use async_trait::async_trait;
use futures::Future;
use futures::future;
use gateway_messages::CfpaPage;
use gateway_messages::ComponentAction;
use gateway_messages::ComponentActionResponse;
use gateway_messages::ComponentDetails;
use gateway_messages::DiscoverResponse;
use gateway_messages::DumpCompression;
use gateway_messages::DumpError;
use gateway_messages::DumpSegment;
use gateway_messages::DumpTask;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::MgsError;
use gateway_messages::MgsRequest;
use gateway_messages::MgsResponse;
use gateway_messages::PowerState;
use gateway_messages::RotBootInfo;
use gateway_messages::RotRequest;
use gateway_messages::RotResponse;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpStateV2;
use gateway_messages::StartupOptions;
use gateway_messages::ignition;
use gateway_messages::ignition::IgnitionError;
use gateway_messages::ignition::LinkEvents;
use gateway_messages::sp_impl::BoundsChecked;
use gateway_messages::sp_impl::DeviceDescription;
use gateway_messages::sp_impl::Sender;
use gateway_messages::sp_impl::SpHandler;
use gateway_types::component::SpState;
use slog::Logger;
use slog::debug;
use slog::info;
use slog::warn;
use std::collections::HashMap;
use std::iter;
use std::net::SocketAddrV6;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::select;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task;
use tokio::task::JoinHandle;

pub const SIM_SIDECAR_BOARD: &str = "SimSidecarSp";

pub struct Sidecar {
    local_addrs: Option<[SocketAddrV6; 2]>,
    ereport_addrs: Option<[SocketAddrV6; 2]>,
    handler: Option<Arc<TokioMutex<Handler>>>,
    commands: mpsc::UnboundedSender<Command>,
    inner_task: Option<JoinHandle<()>>,
    power_state_changes: Arc<AtomicUsize>,
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
    async fn state(&self) -> SpState {
        SpState::from(
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

    fn local_ereport_addr(&self, port: SpPort) -> Option<SocketAddrV6> {
        let i = match port {
            SpPort::One => 0,
            SpPort::Two => 1,
        };
        self.ereport_addrs.map(|addrs| addrs[i])
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

    async fn host_phase1_data(&self, _slot: u16) -> Option<Vec<u8>> {
        // sidecars do not have attached hosts
        None
    }

    async fn current_update_status(&self) -> gateway_messages::UpdateStatus {
        let Some(handler) = self.handler.as_ref() else {
            return gateway_messages::UpdateStatus::None;
        };

        handler.lock().await.update_state.status()
    }

    fn power_state_changes(&self) -> usize {
        self.power_state_changes.load(Ordering::Relaxed)
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

    async fn ereport_restart(&self, restart: crate::config::EreportRestart) {
        let (tx, rx) = oneshot::channel();
        if self
            .commands
            .send(Command::Ereport(ereport::Command::Restart(restart, tx)))
            .is_ok()
        {
            rx.await.unwrap();
        }
    }

    async fn ereport_append(
        &self,
        ereport: crate::config::Ereport,
    ) -> gateway_ereport_messages::Ena {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Ereport(ereport::Command::Append(ereport, tx)))
            .expect("simulated sidecar task has died");
        rx.await.unwrap()
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

        if let Some(network_config) = &sidecar.common.network_config {
            // bind to our two local "KSZ" ports
            let servers = future::try_join(
                UdpServer::new(&network_config[0], &log),
                UdpServer::new(&network_config[1], &log),
            )
            .await?;

            let servers = [servers.0, servers.1];
            let local_addrs =
                [servers[0].local_addr(), servers[1].local_addr()];

            let ereport_log = log.new(slog::o!("component" => "ereport-sim"));
            let (ereport_servers, ereport_addrs) =
                match &sidecar.common.ereport_network_config {
                    Some(cfg) => {
                        assert_eq!(cfg.len(), 2); // gimlet SP always has 2 ports

                        let servers = future::try_join(
                            UdpServer::new(&cfg[0], &ereport_log),
                            UdpServer::new(&cfg[1], &ereport_log),
                        )
                        .await?;
                        let addrs =
                            [servers.0.local_addr(), servers.1.local_addr()];
                        (Some([servers.0, servers.1]), Some(addrs))
                    }
                    None => (None, None),
                };
            let ereport_state = {
                let mut cfg = sidecar.common.ereport_config.clone();
                if cfg.restart.metadata.is_empty() {
                    let map = &mut cfg.restart.metadata;
                    map.insert(
                        "baseboard_part_number".to_string(),
                        SIM_SIDECAR_BOARD.into(),
                    );
                    map.insert(
                        "baseboard_serial_number".to_string(),
                        sidecar.common.serial_number.clone().into(),
                    );
                    map.insert(
                        "hubris_archive_id".to_string(),
                        "asdfasdfasdf".into(),
                    );
                }
                EreportState::new(cfg, ereport_log)
            };

            let update_state = SimSpUpdate::new(
                BaseboardKind::Sidecar,
                sidecar.common.no_stage0_caboose,
                // sidecar doesn't have phase 1 flash; any policy is fine
                HostFlashHashPolicy::assume_already_hashed(),
                sidecar.common.cabooses.clone(),
            );

            let power_state_changes = Arc::new(AtomicUsize::new(0));
            let (inner, handler, responses_sent_count) = Inner::new(
                servers,
                ereport_servers,
                ereport_state,
                sidecar.common.components.clone(),
                sidecar.common.serial_number.clone(),
                FakeIgnition::new(&config.simulated_sps),
                commands_rx,
                log,
                sidecar.common.old_rot_state,
                update_state,
                Arc::clone(&power_state_changes),
            );
            let inner_task =
                task::spawn(async move { inner.run().await.unwrap() });

            Ok(Self {
                local_addrs: Some(local_addrs),
                ereport_addrs,
                handler: Some(handler),
                commands,
                inner_task: Some(inner_task),
                responses_sent_count: Some(responses_sent_count),
                power_state_changes,
            })
        } else {
            Ok(Self {
                local_addrs: None,
                ereport_addrs: None,
                handler: None,
                commands,
                inner_task: None,
                responses_sent_count: None,
                power_state_changes: Arc::new(AtomicUsize::new(0)),
            })
        }
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
    Ereport(ereport::Command),
}

#[derive(Debug)]
struct Ack;

struct Inner {
    handler: Arc<TokioMutex<Handler>>,
    udp0: UdpServer,
    udp1: UdpServer,
    ereport0: Option<UdpServer>,
    ereport1: Option<UdpServer>,
    ereport_state: EreportState,
    commands: mpsc::UnboundedReceiver<Command>,
    responses_sent_count: watch::Sender<usize>,
}

impl Inner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        servers: [UdpServer; 2],
        ereport_servers: Option<[UdpServer; 2]>,
        ereport_state: EreportState,
        components: Vec<SpComponentConfig>,
        serial_number: String,
        ignition: FakeIgnition,
        commands: mpsc::UnboundedReceiver<Command>,
        log: Logger,
        old_rot_state: bool,
        update_state: SimSpUpdate,
        power_state_changes: Arc<AtomicUsize>,
    ) -> (Self, Arc<TokioMutex<Handler>>, watch::Receiver<usize>) {
        let [udp0, udp1] = servers;
        let handler = Arc::new(TokioMutex::new(Handler::new(
            serial_number,
            components,
            ignition,
            log,
            old_rot_state,
            update_state,
            power_state_changes,
        )));
        let responses_sent_count = watch::Sender::new(0);
        let responses_sent_count_rx = responses_sent_count.subscribe();
        let (ereport0, ereport1) = match ereport_servers {
            Some([e0, e1]) => (Some(e0), Some(e1)),
            None => (None, None),
        };
        (
            Self {
                handler: Arc::clone(&handler),
                ereport0,
                ereport1,
                ereport_state,
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

                recv = ereport::recv_request(self.ereport0.as_mut()) => {
                    let (req, addr, sock) = recv?;
                    let rsp = self.ereport_state.handle_request(&req, addr, &mut out_buf);
                    sock.send_to(rsp, addr).await?;
                }

                recv = ereport::recv_request(self.ereport1.as_mut()) => {
                    let (req, addr, sock) = recv?;
                    let rsp = self.ereport_state.handle_request(&req, addr, &mut out_buf);
                    sock.send_to(rsp, addr).await?;
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
                        Command::Ereport(cmd) => self.ereport_state.handle_command(cmd),
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
    sensors: Sensors,

    serial_number: String,
    ignition: FakeIgnition,
    power_state: PowerState,
    power_state_changes: Arc<AtomicUsize>,

    update_state: SimSpUpdate,
    reset_pending: Option<SpComponent>,

    // To simulate an SP reset, we should (after doing whatever housekeeping we
    // need to track the reset) intentionally _fail_ to respond to the request,
    // simulating a `-> !` function on the SP that triggers a reset. To provide
    // this, our caller will pass us a function to call if they should ignore
    // whatever result we return and fail to respond at all.
    should_fail_to_respond_signal: Option<Box<dyn FnOnce() + Send>>,
    old_rot_state: bool,
    sp_dumps: HashMap<[u8; 16], u32>,
}

impl Handler {
    fn new(
        serial_number: String,
        components: Vec<SpComponentConfig>,
        ignition: FakeIgnition,
        log: Logger,
        old_rot_state: bool,
        update_state: SimSpUpdate,
        power_state_changes: Arc<AtomicUsize>,
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

        let sensors = Sensors::from_component_configs(&components);

        let sp_dumps = HashMap::new();

        Self {
            log,
            components,
            sensors,
            leaked_component_device_strings,
            leaked_component_description_strings,
            serial_number,
            ignition,
            power_state: PowerState::A2,
            power_state_changes,
            update_state,
            reset_pending: None,
            should_fail_to_respond_signal: None,
            old_rot_state,
            sp_dumps,
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
            rot: Ok(rot_state_v2(self.update_state.rot_state())),
        }
    }
}

impl SpHandler for Handler {
    type BulkIgnitionStateIter = iter::Skip<std::vec::IntoIter<IgnitionState>>;
    type BulkIgnitionLinkEventsIter =
        iter::Skip<std::vec::IntoIter<LinkEvents>>;
    type VLanId = SpPort;

    fn ensure_request_trusted(
        &mut self,
        kind: MgsRequest,
        _sender: Sender<Self::VLanId>,
    ) -> Result<MgsRequest, SpError> {
        Ok(kind)
    }

    fn ensure_response_trusted(
        &mut self,
        kind: MgsResponse,
        _sender: Sender<Self::VLanId>,
    ) -> Option<MgsResponse> {
        Some(kind)
    }

    fn discover(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<gateway_messages::DiscoverResponse, SpError> {
        debug!(
            &self.log,
            "received discover; sending response";
            "sender" => ?sender,
        );
        Ok(DiscoverResponse { sp_port: sender.vid })
    }

    fn num_ignition_ports(&mut self) -> Result<u32, SpError> {
        Ok(self.ignition.num_targets() as u32)
    }

    fn ignition_state(&mut self, target: u8) -> Result<IgnitionState, SpError> {
        let state = self.ignition.get_target(target)?;
        debug!(
            &self.log,
            "received ignition state request";
            "target" => target,
            "reply-state" => ?state,
        );
        Ok(*state)
    }

    fn bulk_ignition_state(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionStateIter, SpError> {
        debug!(
            &self.log,
            "received bulk ignition state request";
            "offset" => offset,
            "state" => ?self.ignition.state,
        );
        Ok(self.ignition.state.clone().into_iter().skip(offset as usize))
    }

    fn ignition_link_events(
        &mut self,
        target: u8,
    ) -> Result<LinkEvents, SpError> {
        // Check validity of `target`
        _ = self.ignition.get_target(target)?;

        let events = self.ignition.link_events[usize::from(target)];

        debug!(
            &self.log,
            "received ignition link events request";
            "target" => target,
            "events" => ?events,
        );

        Ok(events)
    }

    fn bulk_ignition_link_events(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionLinkEventsIter, SpError> {
        debug!(
            &self.log,
            "received bulk ignition link events request";
            "offset" => offset,
        );
        Ok(self.ignition.link_events.clone().into_iter().skip(offset as usize))
    }

    /// If `target` is `None`, clear link events for all targets.
    fn clear_ignition_link_events(
        &mut self,
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
            "target" => ?target,
            "transceiver_select" => ?transceiver_select,
        );
        Ok(())
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), SpError> {
        self.ignition.command(target, command)?;
        debug!(
            &self.log,
            "received ignition command; sending ack";
            "target" => target,
            "command" => ?command,
        );
        Ok(())
    }

    fn serial_console_attach(
        &mut self,
        sender: Sender<Self::VLanId>,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received serial console attach; unsupported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_write(
        &mut self,
        sender: Sender<Self::VLanId>,
        _offset: u64,
        _data: &[u8],
    ) -> Result<u64, SpError> {
        warn!(
            &self.log, "received serial console write; unsupported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_keepalive(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console keepalive; unsupported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_detach(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received serial console detach; unsupported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_break(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console break; not supported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn send_host_nmi(&mut self) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received host NMI request; not supported by sidecar";
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn sp_state(&mut self) -> Result<SpStateV2, SpError> {
        let state = self.sp_state_impl();
        debug!(
            &self.log, "received state request";
            "reply-state" => ?state,
        );
        Ok(state)
    }

    fn sp_update_prepare(
        &mut self,
        update: gateway_messages::SpUpdatePrepare,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update prepare request";
            "update" => ?update,
        );
        self.update_state.sp_update_prepare(
            update.id,
            update.sp_image_size.try_into().unwrap(),
        )
    }

    fn component_update_prepare(
        &mut self,
        update: gateway_messages::ComponentUpdatePrepare,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update prepare request";
            "update" => ?update,
        );
        self.update_state.component_update_prepare(
            update.component,
            update.id,
            update.total_size.try_into().unwrap(),
            update.slot,
        )
    }

    fn update_status(
        &mut self,
        component: SpComponent,
    ) -> Result<gateway_messages::UpdateStatus, SpError> {
        debug!(
            &self.log,
            "received update status request";
            "component" => ?component,
        );
        Ok(self.update_state.status())
    }

    fn update_chunk(
        &mut self,
        chunk: gateway_messages::UpdateChunk,
        chunk_data: &[u8],
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update chunk";
            "offset" => chunk.offset,
            "length" => chunk_data.len(),
        );
        self.update_state.ingest_chunk(chunk, chunk_data)
    }

    fn update_abort(
        &mut self,
        component: SpComponent,
        update_id: gateway_messages::UpdateId,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update abort; not supported by simulated sidecar";
            "component" => ?component,
            "id" => ?update_id,
        );
        self.update_state.abort(update_id)
    }

    fn power_state(&mut self) -> Result<gateway_messages::PowerState, SpError> {
        debug!(
            &self.log, "received power state";
            "power_state" => ?self.power_state,
        );
        Ok(self.power_state)
    }

    fn set_power_state(
        &mut self,
        sender: Sender<Self::VLanId>,
        power_state: gateway_messages::PowerState,
    ) -> Result<gateway_messages::PowerStateTransition, SpError> {
        // NOTE(eliza): This is *currently* accurate to real life sidecar
        // behavior, as the sidecar sequencer does not treat `set_power_state`
        // calls with the current power state idempotently, the way the compute
        // sled sequencer does.
        // See: https://github.com/oxidecomputer/hubris/blob/13808140c49fdf8f1ce462184395d3b28212c217/task/control-plane-agent/src/mgs_sidecar.rs#L838-L840
        let transition = gateway_messages::PowerStateTransition::Changed;
        debug!(
            &self.log, "received set power state";
            "sender" => ?sender,
            "power_state" => ?power_state,
            "transition" => ?transition,
        );
        self.power_state = power_state;
        match transition {
            gateway_messages::PowerStateTransition::Changed => {
                self.power_state_changes.fetch_add(1, Ordering::Relaxed);
            }
            gateway_messages::PowerStateTransition::Unchanged => (),
        }
        Ok(transition)
    }

    fn reset_component_prepare(
        &mut self,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received reset prepare request";
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
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received sys-reset trigger request";
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

    fn num_devices(&mut self) -> u32 {
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
        component: SpComponent,
    ) -> Result<u32, SpError> {
        let num_sensor_details =
            self.sensors.num_component_details(&component).unwrap_or(0);
        // TODO: here is where we might also handle port statuses, if we decide
        // to simulate that later...
        debug!(
            &self.log, "asked for number of component details";
            "component" => ?component,
            "num_details" => num_sensor_details
        );
        Ok(num_sensor_details)
    }

    fn component_details(
        &mut self,
        component: SpComponent,
        index: BoundsChecked,
    ) -> ComponentDetails {
        let Some(sensor_details) =
            self.sensors.component_details(&component, index)
        else {
            todo!("simulate port status details...");
        };
        debug!(
            &self.log, "asked for component details for a sensor";
            "component" => ?component,
            "index" => index.0,
            "details" => ?sensor_details
        );
        sensor_details
    }

    fn component_clear_status(
        &mut self,
        component: SpComponent,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to clear status (not supported for sim components)";
            "component" => ?component,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn component_get_active_slot(
        &mut self,
        component: SpComponent,
    ) -> Result<u16, SpError> {
        warn!(
            &self.log, "asked for component active slot";
            "component" => ?component,
        );
        self.update_state.component_get_active_slot(component)
    }

    fn component_set_active_slot(
        &mut self,
        component: SpComponent,
        slot: u16,
        persist: bool,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to set component active slot";
            "component" => ?component,
            "slot" => slot,
            "persist" => persist,
        );
        self.update_state.component_set_active_slot(component, slot, persist)
    }

    fn component_action(
        &mut self,
        sender: Sender<Self::VLanId>,
        component: SpComponent,
        action: ComponentAction,
    ) -> Result<ComponentActionResponse, SpError> {
        warn!(
            &self.log, "asked to perform component action (not supported for sim components)";
            "sender" => ?sender,
            "component" => ?component,
            "action" => ?action,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn get_startup_options(&mut self) -> Result<StartupOptions, SpError> {
        warn!(
            &self.log, "asked for startup options (unsupported by sidecar)";
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn set_startup_options(
        &mut self,
        startup_options: StartupOptions,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to set startup options (unsupported by sidecar)";
            "options" => ?startup_options,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn mgs_response_error(&mut self, message_id: u32, err: MgsError) {
        warn!(
            &self.log, "received MGS error response";
            "message_id" => message_id,
            "err" => ?err,
        );
    }

    fn mgs_response_host_phase2_data(
        &mut self,
        sender: Sender<Self::VLanId>,
        message_id: u32,
        hash: [u8; 32],
        offset: u64,
        data: &[u8],
    ) {
        debug!(
            &self.log, "received host phase 2 data from MGS";
            "sender" => ?sender,
            "message_id" => message_id,
            "hash" => ?hash,
            "offset" => offset,
            "data_len" => data.len(),
        );
    }

    fn set_ipcc_key_lookup_value(
        &mut self,
        key: u8,
        value: &[u8],
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received IPCC key/value; not supported by sidecar";
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
        self.update_state.get_component_caboose_value(component, slot, key, buf)
    }

    fn read_sensor(
        &mut self,
        request: gateway_messages::SensorRequest,
    ) -> std::result::Result<gateway_messages::SensorResponse, SpError> {
        self.sensors.read_sensor(request).map_err(SpError::Sensor)
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
        version: u8,
    ) -> Result<RotBootInfo, SpError> {
        if self.old_rot_state {
            Err(SpError::RequestUnsupportedForSp)
        } else {
            match version {
                0 => Err(SpError::Update(
                    gateway_messages::UpdateError::VersionNotSupported,
                )),
                1 => Ok(RotBootInfo::V2(rot_state_v2(
                    self.update_state.rot_state(),
                ))),
                _ => Ok(RotBootInfo::V3(self.update_state.rot_state())),
            }
        }
    }

    fn get_task_dump_count(&mut self) -> Result<u32, SpError> {
        Ok(1)
    }

    fn task_dump_read_start(
        &mut self,
        index: u32,
        key: [u8; 16],
    ) -> Result<DumpTask, SpError> {
        if index != 0 {
            return Err(SpError::Dump(DumpError::BadIndex));
        }

        // Hubris allows clients to reuse existing keys.
        // Overwrite any in-flight requests using this key.
        self.sp_dumps.insert(key, 0);

        Ok(DumpTask { time: 1, task: 0, compression: DumpCompression::Lzss })
    }

    fn task_dump_read_continue(
        &mut self,
        key: [u8; 16],
        seq: u32,
        buf: &mut [u8],
    ) -> Result<Option<DumpSegment>, SpError> {
        let Some(expected_seq) = self.sp_dumps.get_mut(&key) else {
            return Err(SpError::Dump(DumpError::BadKey));
        };

        if seq != *expected_seq {
            return Err(SpError::Dump(DumpError::BadSequenceNumber));
        }

        const UNCOMPRESSED_MSG: &[u8] = b"my cool SP dump";
        // "my cool SP dump" encoded with `lzss-cli e 6,4,0x20`
        const COMPRESSED_MSG: &[u8] = &[
            0xb6, 0xde, 0x64, 0x16, 0x3b, 0x7d, 0xbe, 0xd9, 0x20, 0xa9, 0xd4,
            0x24, 0x16, 0x4b, 0xad, 0xb6, 0xe0,
        ];
        buf[..COMPRESSED_MSG.len()].copy_from_slice(COMPRESSED_MSG);

        *expected_seq += 1;

        match seq {
            ..3 => Ok(Some(DumpSegment {
                address: 1,
                compressed_length: COMPRESSED_MSG.len() as u16,
                uncompressed_length: UNCOMPRESSED_MSG.len() as u16,
                seq,
            })),
            3.. => {
                self.sp_dumps.remove(&key);
                Ok(None)
            }
        }
    }

    fn read_host_flash(
        &mut self,
        _slot: u16,
        _addr: u32,
        _buf: &mut [u8],
    ) -> Result<(), SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn start_host_flash_hash(&mut self, _slot: u16) -> Result<(), SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_host_flash_hash(&mut self, _slot: u16) -> Result<[u8; 32], SpError> {
        Err(SpError::RequestUnsupportedForSp)
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
            IgnitionCommand::AlwaysTransmit { .. } => {
                // This is only used in manufacturing; do nothing.
            }
        }

        Ok(())
    }
}
