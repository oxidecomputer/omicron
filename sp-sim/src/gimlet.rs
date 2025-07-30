// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::HostFlashHashPolicy;
use crate::Responsiveness;
use crate::SimulatedSp;
use crate::config::GimletConfig;
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
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures::Future;
use futures::future;
use gateway_messages::CfpaPage;
use gateway_messages::ComponentAction;
use gateway_messages::ComponentActionResponse;
use gateway_messages::DumpCompression;
use gateway_messages::DumpError;
use gateway_messages::DumpSegment;
use gateway_messages::DumpTask;
use gateway_messages::Header;
use gateway_messages::MgsRequest;
use gateway_messages::MgsResponse;
use gateway_messages::PowerStateTransition;
use gateway_messages::RotBootInfo;
use gateway_messages::RotRequest;
use gateway_messages::RotResponse;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpRequest;
use gateway_messages::SpStateV2;
use gateway_messages::ignition::{self, LinkEvents};
use gateway_messages::sp_impl::Sender;
use gateway_messages::sp_impl::SpHandler;
use gateway_messages::sp_impl::{BoundsChecked, DeviceDescription};
use gateway_messages::{ComponentDetails, Message, MgsError, StartupOptions};
use gateway_messages::{DiscoverResponse, IgnitionState, PowerState};
use gateway_messages::{MessageKind, version};
use gateway_types::component::SpState;
use omicron_common::disk::M2Slot;
use slog::{Logger, debug, error, info, warn};
use std::cell::Cell;
use std::collections::HashMap;
use std::iter;
use std::net::{SocketAddr, SocketAddrV6};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::select;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::{self, JoinHandle};

pub const SIM_GIMLET_BOARD: &str = "SimGimletSp";

// Type alias for the remote end of an MGS serial console connection.
type AttachedMgsSerialConsole =
    Arc<Mutex<Option<(SpComponent, Sender<SpPort>)>>>;

/// Type of request most recently handled by a simulated SP.
///
/// Many request types are not covered by this enum. This only exists to enable
/// certain particular tests.
// If you need an additional request type to be reported by this enum, feel free
// to add it and update the appropriate `Handler` function below (see
// `update_status()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimSpHandledRequest {
    /// The most recent request was for the update status of a component.
    ComponentUpdateStatus(SpComponent),
    /// The most recent request was some other type that is currently not
    /// implemented in this tracker.
    NotImplemented,
}

/// Current power state and, if in A0, which M2 slot is currently active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GimletPowerState {
    A2,
    A0(M2Slot),
}

impl From<GimletPowerState> for PowerState {
    fn from(value: GimletPowerState) -> Self {
        match value {
            GimletPowerState::A2 => Self::A2,
            GimletPowerState::A0(_) => Self::A0,
        }
    }
}

pub struct Gimlet {
    local_addrs: Option<[SocketAddrV6; 2]>,
    ereport_addrs: Option<[SocketAddrV6; 2]>,
    handler: Option<Arc<TokioMutex<Handler>>>,
    serial_console_addrs: HashMap<String, SocketAddrV6>,
    commands: mpsc::UnboundedSender<Command>,
    inner_tasks: Vec<JoinHandle<()>>,
    responses_sent_count: Option<watch::Receiver<usize>>,
    last_request_handled: Arc<Mutex<Option<SimSpHandledRequest>>>,
    power_state_rx: Option<watch::Receiver<GimletPowerState>>,
}

impl Drop for Gimlet {
    fn drop(&mut self) {
        // default join handle drop behavior is to detach; we want to abort
        for task in &self.inner_tasks {
            task.abort();
        }
    }
}

#[async_trait]
impl SimulatedSp for Gimlet {
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
        if let Ok(()) = self.commands.send(Command::SetResponsiveness(r, tx)) {
            rx.await.unwrap();
        }
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

    async fn host_phase1_data(
        &self,
        slot: u16,
    ) -> Option<Vec<u8>> {
        let handler = self.handler.as_ref()?;
        let handler = handler.lock().await;
        handler.update_state.host_phase1_data(slot)
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
            .expect("simulated gimlet task has died");
        rx.await.unwrap()
    }
}

impl Gimlet {
    pub async fn spawn(
        gimlet: &GimletConfig,
        phase1_hash_policy: HostFlashHashPolicy,
        log: Logger,
    ) -> Result<Self> {
        info!(log, "setting up simulated gimlet");

        let attached_mgs = Arc::new(Mutex::new(None));

        let mut incoming_console_tx = HashMap::new();
        let mut serial_console_addrs = HashMap::new();
        let mut inner_tasks = Vec::new();
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let last_request_handled = Arc::default();

        // Weird case - if we don't have any network config, we're only being
        // created to simulate an RoT, so go ahead and return without actually
        // starting a simulated SP.
        let Some(network_config) = &gimlet.common.network_config else {
            return Ok(Self {
                local_addrs: None,
                ereport_addrs: None,
                handler: None,
                serial_console_addrs,
                commands,
                inner_tasks,
                responses_sent_count: None,
                last_request_handled,
                power_state_rx: None,
            });
        };

        // bind to our two local "KSZ" ports
        assert_eq!(network_config.len(), 2); // gimlet SP always has 2 ports

        let servers = future::try_join(
            UdpServer::new(&network_config[0], &log),
            UdpServer::new(&network_config[1], &log),
        )
        .await?;

        let servers = [servers.0, servers.1];

        let ereport_log = log.new(slog::o!("component" => "ereport-sim"));
        let (ereport_servers, ereport_addrs) =
            match &gimlet.common.ereport_network_config {
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
        let mut update_state = SimSpUpdate::new(
            BaseboardKind::Gimlet,
            gimlet.common.no_stage0_caboose,
            phase1_hash_policy,
        );
        let ereport_state = {
            let mut cfg = gimlet.common.ereport_config.clone();
            let mut buf = [0u8; 256];
            let hubris_gitc = {
                let len = update_state
                    .get_component_caboose_value(
                        SpComponent::SP_ITSELF,
                        0,
                        *b"GITC",
                        &mut buf,
                    )
                    .expect(
                        "update state should tell us the caboose git commit",
                    );
                std::str::from_utf8(&buf[..len])
                    .expect("update state GITC should be valid UTF-8")
                    .to_string()
            };
            let hubris_vers = {
                let len = update_state
                    .get_component_caboose_value(
                        SpComponent::SP_ITSELF,
                        0,
                        *b"VERS",
                        &mut buf,
                    )
                    .expect(
                        "update state should tell us the caboose git commit",
                    );
                std::str::from_utf8(&buf[..len])
                    .expect("update state GITC should be valid UTF-8")
                    .to_string()
            };

            if cfg.restart.metadata.is_empty() {
                let map = &mut cfg.restart.metadata;
                map.insert(
                    "baseboard_part_number".to_string(),
                    SIM_GIMLET_BOARD.into(),
                );
                map.insert(
                    "baseboard_serial_number".to_string(),
                    gimlet.common.serial_number.clone().into(),
                );
                map.insert("hubris_archive_id".to_string(), hubris_gitc.into());
                map.insert("hubris_version".to_string(), hubris_vers.into());
            }
            EreportState::new(cfg, ereport_log)
        };

        for component_config in &gimlet.common.components {
            let id = component_config.id.as_str();
            let component = SpComponent::try_from(id)
                .map_err(|_| anyhow!("component id {:?} too long", id))?;

            if let Some(addr) = component_config.serial_console {
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("failed to bind to {}", addr))?;
                info!(
                    log, "bound fake serial console to TCP port";
                    "addr" => %addr,
                    "component" => ?component,
                );

                serial_console_addrs.insert(
                    component
                        .as_str()
                        .with_context(|| "non-utf8 component")?
                        .to_string(),
                    listener
                        .local_addr()
                        .with_context(
                            || "failed to get local address of bound socket",
                        )
                        .and_then(|addr| match addr {
                            SocketAddr::V4(addr) => {
                                bail!("bound IPv4 address {}", addr)
                            }
                            SocketAddr::V6(addr) => Ok(addr),
                        })?,
                );

                let (tx, rx) = mpsc::unbounded_channel();
                incoming_console_tx.insert(component, tx);

                let serial_console = SerialConsoleTcpTask::new(
                    component,
                    listener,
                    rx,
                    [
                        Arc::clone(servers[0].socket()),
                        Arc::clone(servers[1].socket()),
                    ],
                    Arc::clone(&attached_mgs),
                    log.new(slog::o!("serial-console" => id.to_string())),
                );
                inner_tasks.push(task::spawn(async move {
                    serial_console.run().await
                }));
            }
        }
        let local_addrs = [servers[0].local_addr(), servers[1].local_addr()];
        let (power_state, power_state_rx) =
            watch::channel(GimletPowerState::A0(M2Slot::A));
        let (inner, handler, responses_sent_count) = UdpTask::new(
            servers,
            ereport_servers,
            ereport_state,
            gimlet.common.components.clone(),
            attached_mgs,
            gimlet.common.serial_number.clone(),
            incoming_console_tx,
            power_state,
            commands_rx,
            Arc::clone(&last_request_handled),
            log,
            gimlet.common.old_rot_state,
            update_state,
        );
        inner_tasks
            .push(task::spawn(async move { inner.run().await.unwrap() }));

        Ok(Self {
            local_addrs: Some(local_addrs),
            ereport_addrs,
            handler: Some(handler),
            serial_console_addrs,
            commands,
            inner_tasks,
            responses_sent_count: Some(responses_sent_count),
            last_request_handled,
            power_state_rx: Some(power_state_rx),
        })
    }

    pub fn power_state_rx(&self) -> Option<watch::Receiver<GimletPowerState>> {
        self.power_state_rx.clone()
    }

    pub fn serial_console_addr(&self, component: &str) -> Option<SocketAddrV6> {
        self.serial_console_addrs.get(component).copied()
    }

    pub fn last_request_handled(&self) -> Option<SimSpHandledRequest> {
        *self.last_request_handled.lock().unwrap()
    }

    /// Set the policy for simulating host phase 1 flash hashing.
    ///
    /// # Panics
    ///
    /// Panics if this `Gimlet` was created with only an RoT instead of a full
    /// SP + RoT complex.
    pub async fn set_phase1_hash_policy(&self, policy: HostFlashHashPolicy) {
        self.handler
            .as_ref()
            .expect("gimlet was created with SP config")
            .lock()
            .await
            .update_state
            .set_phase1_hash_policy(policy)
    }
}

struct SerialConsoleTcpTask {
    listener: TcpListener,
    incoming_serial_console: UnboundedReceiver<Vec<u8>>,
    socks: [Arc<UdpSocket>; 2],
    attached_mgs: AttachedMgsSerialConsole,
    serial_console_tx_offset: u64,
    component: SpComponent,
    log: Logger,
    request_message_id: Cell<u32>,
}

impl SerialConsoleTcpTask {
    fn new(
        component: SpComponent,
        listener: TcpListener,
        incoming_serial_console: UnboundedReceiver<Vec<u8>>,
        socks: [Arc<UdpSocket>; 2],
        attached_mgs: AttachedMgsSerialConsole,
        log: Logger,
    ) -> Self {
        Self {
            listener,
            incoming_serial_console,
            socks,
            attached_mgs,
            serial_console_tx_offset: 0,
            component,
            log,
            request_message_id: Cell::new(0),
        }
    }

    fn next_request_message_id(&self) -> u32 {
        let id = self.request_message_id.get();
        self.request_message_id.set(id.wrapping_add(1));
        id
    }

    async fn send_serial_console(&mut self, data: &[u8]) -> Result<()> {
        let (component, sender) = match *self.attached_mgs.lock().unwrap() {
            Some((component, sender)) => (component, sender),
            None => {
                info!(
                    self.log,
                    "No attached MGS; discarding serial console data"
                );
                return Ok(());
            }
        };

        if component != self.component {
            info!(
                self.log,
                "MGS is attached to a different component; \
                 discarding serial console data"
            );
            return Ok(());
        }

        let sock = match sender.vid {
            SpPort::One => &self.socks[0],
            SpPort::Two => &self.socks[1],
        };

        let mut out = [0; gateway_messages::MAX_SERIALIZED_SIZE];
        let mut remaining = data;
        while !remaining.is_empty() {
            let message = Message {
                header: Header {
                    version: version::CURRENT,
                    message_id: self.next_request_message_id(),
                },
                kind: MessageKind::SpRequest(SpRequest::SerialConsole {
                    component: self.component,
                    offset: self.serial_console_tx_offset,
                }),
            };
            let (n, written) = gateway_messages::serialize_with_trailing_data(
                &mut out,
                &message,
                &[remaining],
            );
            sock.send_to(&out[..n], sender.addr).await?;
            remaining = &remaining[written..];
            self.serial_console_tx_offset += written as u64;
        }

        Ok(())
    }

    async fn run(mut self) {
        loop {
            // wait for incoming connections, discarding any serial console
            // packets received while we don't have one
            let (conn, addr) = loop {
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
            match self.drive_tcp_connection(conn).await {
                Ok(()) => {
                    info!(self.log, "closing serial console TCP connection")
                }
                Err(err) => {
                    error!(
                        self.log,
                        "serial TCP connection failed";
                        "err" => %err,
                    )
                }
            }
        }
    }

    async fn drive_tcp_connection(
        &mut self,
        mut conn: TcpStream,
    ) -> Result<()> {
        let mut buf = [0; 512];

        // copy serial console data in both directions
        loop {
            select! {
                res = conn.read(&mut buf) => {
                    let n = res?;
                    if n == 0 {
                        return Ok(());
                    }
                    self
                        .send_serial_console(&buf[..n])
                        .await
                        .with_context(||"UDP send error")?;
                }
                incoming = self.incoming_serial_console.recv() => {
                    // we can only get `None` if the tx half was dropped,
                    // which means we're in the process of shutting down
                    let data = match incoming {
                        Some(data) => data,
                        None => return Ok(()),
                    };

                    conn
                        .write_all(&data)
                        .await
                        .with_context(|| "TCP write error")?;
                }
            }
        }
    }
}

enum Command {
    SetResponsiveness(Responsiveness, oneshot::Sender<Ack>),
    SetThrottler(Option<mpsc::UnboundedReceiver<usize>>, oneshot::Sender<Ack>),
    Ereport(ereport::Command),
}

struct Ack;

struct UdpTask {
    udp0: UdpServer,
    udp1: UdpServer,
    ereport_servers: Option<[UdpServer; 2]>,
    ereport_state: EreportState,
    handler: Arc<TokioMutex<Handler>>,
    commands: mpsc::UnboundedReceiver<Command>,
    responses_sent_count: watch::Sender<usize>,
    last_request_handled: Arc<Mutex<Option<SimSpHandledRequest>>>,
}

impl UdpTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        servers: [UdpServer; 2],
        ereport_servers: Option<[UdpServer; 2]>,
        ereport_state: EreportState,
        components: Vec<SpComponentConfig>,
        attached_mgs: AttachedMgsSerialConsole,
        serial_number: String,
        incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
        power_state: watch::Sender<GimletPowerState>,
        commands: mpsc::UnboundedReceiver<Command>,
        last_request_handled: Arc<Mutex<Option<SimSpHandledRequest>>>,
        log: Logger,
        old_rot_state: bool,
        update_state: SimSpUpdate,
    ) -> (Self, Arc<TokioMutex<Handler>>, watch::Receiver<usize>) {
        let [udp0, udp1] = servers;
        let handler = Arc::new(TokioMutex::new(Handler::new(
            serial_number,
            components,
            attached_mgs,
            incoming_serial_console,
            power_state,
            log.clone(),
            old_rot_state,
            update_state,
        )));
        let responses_sent_count = watch::Sender::new(0);
        let responses_sent_count_rx = responses_sent_count.subscribe();
        (
            Self {
                udp0,
                udp1,
                ereport_servers,
                ereport_state,
                handler: Arc::clone(&handler),
                commands,
                responses_sent_count,
                last_request_handled,
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
            let (ereport0, ereport1) = match self.ereport_servers.as_mut() {
                Some([e0, e1]) => (Some(e0), Some(e1)),
                None => (None, None),
            };
            select! {
                Some(n) = incr_throttle_count => {
                    throttle_count = throttle_count.saturating_add(n);
                }

                recv0 = self.udp0.recv_from(), if throttle_count > 0 => {
                    let (result, handled_request) = {
                        let mut handler = self.handler.lock().await;
                        handler.last_request_handled = None;
                        let result = server::handle_request(
                            &mut *handler,
                            recv0,
                            &mut out_buf,
                            responsiveness,
                            SpPort::One,
                        ).await?;
                        (result,
                         handler.last_request_handled.unwrap_or(
                             SimSpHandledRequest::NotImplemented,
                        ))
                    };
                    if let Some((resp, addr)) = result {
                        throttle_count -= 1;
                        self.udp0.send_to(resp, addr).await?;
                        self.responses_sent_count.send_modify(|n| *n += 1);
                        *self.last_request_handled.lock().unwrap() =
                            Some(handled_request);
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

                recv = ereport::recv_request(ereport0) => {
                    let (req, addr, sock) = recv?;
                    let rsp = self.ereport_state.handle_request(&req, addr, &mut out_buf);
                    sock.send_to(rsp, addr).await?;
                }

                recv = ereport::recv_request(ereport1) => {
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
                        },
                        Command::Ereport(cmd) => self.ereport_state.handle_command(cmd),
                    }
                }
            }
        }
    }
}

struct Handler {
    log: Logger,
    serial_number: String,

    components: Vec<SpComponentConfig>,
    // `SpHandler` wants `&'static str` references when describing components;
    // this is fine on the real SP where the strings are baked in at build time,
    // but awkward here where we read them in at runtime. We'll leak the strings
    // to conform to `SpHandler` rather than making it more complicated to ease
    // our life as a simulator.
    leaked_component_device_strings: Vec<&'static str>,
    leaked_component_description_strings: Vec<&'static str>,

    attached_mgs: AttachedMgsSerialConsole,
    incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
    power_state: watch::Sender<GimletPowerState>,
    startup_options: StartupOptions,
    update_state: SimSpUpdate,
    reset_pending: Option<SpComponent>,
    sensors: Sensors,

    last_request_handled: Option<SimSpHandledRequest>,

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
    #[allow(clippy::too_many_arguments)]
    fn new(
        serial_number: String,
        components: Vec<SpComponentConfig>,
        attached_mgs: AttachedMgsSerialConsole,
        incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
        power_state: watch::Sender<GimletPowerState>,
        log: Logger,
        old_rot_state: bool,
        update_state: SimSpUpdate,
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
            attached_mgs,
            incoming_serial_console,
            startup_options: StartupOptions::empty(),
            update_state,
            reset_pending: None,
            power_state,
            last_request_handled: None,
            should_fail_to_respond_signal: None,
            old_rot_state,
            sp_dumps,
        }
    }

    fn sp_state_impl(&self) -> SpStateV2 {
        // Make the Baseboard a PC so that our testbeds work as expected.
        const FAKE_GIMLET_MODEL: &[u8] = b"i86pc";

        let mut model = [0; 32];
        model[..FAKE_GIMLET_MODEL.len()].copy_from_slice(FAKE_GIMLET_MODEL);

        SpStateV2 {
            hubris_archive_id: [0; 8],
            serial_number: serial_number_padded(&self.serial_number),
            model,
            revision: 0,
            base_mac_address: [0; 6],
            power_state: self.power_state.borrow().clone().into(),
            rot: Ok(rot_state_v2(self.update_state.rot_state())),
        }
    }
}

impl SpHandler for Handler {
    type BulkIgnitionStateIter = iter::Empty<IgnitionState>;
    type BulkIgnitionLinkEventsIter = iter::Empty<LinkEvents>;
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
    ) -> Result<DiscoverResponse, SpError> {
        debug!(
            &self.log,
            "received discover; sending response";
            "sender" => ?sender,
        );
        Ok(DiscoverResponse { sp_port: sender.vid })
    }

    fn num_ignition_ports(&mut self) -> Result<u32, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_state(
        &mut self,
        target: u8,
    ) -> Result<gateway_messages::IgnitionState, SpError> {
        warn!(
            &self.log,
            "received ignition state request; not supported by gimlet";
            "target" => target,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_state(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionStateIter, SpError> {
        warn!(
            &self.log,
            "received bulk ignition state request; not supported by gimlet";
            "offset" => offset,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_link_events(
        &mut self,
        target: u8,
    ) -> Result<LinkEvents, SpError> {
        warn!(
            &self.log,
            "received ignition link events request; not supported by gimlet";
            "target" => target,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_link_events(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionLinkEventsIter, SpError> {
        warn!(
            &self.log,
            "received bulk ignition link events request; not supported by gimlet";
            "offset" => offset,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    /// If `target` is `None`, clear link events for all targets.
    fn clear_ignition_link_events(
        &mut self,
        target: Option<u8>,
        transceiver_select: Option<ignition::TransceiverSelect>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received clear ignition link events request; not supported by gimlet";
            "target" => ?target,
            "transceiver_select" => ?transceiver_select,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received ignition command; not supported by gimlet";
            "target" => target,
            "command" => ?command,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_attach(
        &mut self,
        sender: Sender<Self::VLanId>,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console attach request";
            "sender" => ?sender,
            "component" => ?component,
        );

        let mut attached_mgs = self.attached_mgs.lock().unwrap();
        if attached_mgs.is_some() {
            return Err(SpError::SerialConsoleAlreadyAttached);
        }

        *attached_mgs = Some((component, sender));
        Ok(())
    }

    fn serial_console_write(
        &mut self,
        sender: Sender<Self::VLanId>,
        offset: u64,
        data: &[u8],
    ) -> Result<u64, SpError> {
        debug!(
            &self.log,
            "received serial console packet";
            "sender" => ?sender,
            "len" => data.len(),
            "offset" => offset,
        );

        let component = self
            .attached_mgs
            .lock()
            .unwrap()
            .map(|(component, _sender)| component)
            .ok_or(SpError::SerialConsoleNotAttached)?;

        let incoming_serial_console = self
            .incoming_serial_console
            .get(&component)
            .ok_or(SpError::RequestUnsupportedForComponent)?;

        // should we sanity check `offset`? for now just assume everything
        // comes in order; we're just a simulator anyway
        //
        // if the receiving half is gone, we're in the process of shutting down;
        // ignore errors here
        let _ = incoming_serial_console.send(data.to_vec());

        Ok(offset + data.len() as u64)
    }

    fn serial_console_keepalive(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> std::result::Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console keepalive";
            "sender" => ?sender,
        );

        let component = self
            .attached_mgs
            .lock()
            .unwrap()
            .map(|(component, _sender)| component)
            .ok_or(SpError::SerialConsoleNotAttached)?;

        let _incoming_serial_console = self
            .incoming_serial_console
            .get(&component)
            .ok_or(SpError::RequestUnsupportedForComponent)?;

        Ok(())
    }

    fn serial_console_detach(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console detach request";
            "sender" => ?sender,
        );
        *self.attached_mgs.lock().unwrap() = None;
        Ok(())
    }

    fn serial_console_break(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console break";
            "sender" => ?sender,
        );
        let component = self
            .attached_mgs
            .lock()
            .unwrap()
            .map(|(component, _sender)| component)
            .ok_or(SpError::SerialConsoleNotAttached)?;

        let incoming_serial_console = self
            .incoming_serial_console
            .get(&component)
            .ok_or(SpError::RequestUnsupportedForComponent)?;

        // if the receiving half is gone, we're in the process of shutting down;
        // ignore errors here
        _ = incoming_serial_console.send(b"*** serial break ***".to_vec());

        Ok(())
    }

    fn send_host_nmi(&mut self) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received host NMI request; not supported by simulated gimlet";
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
            "received SP update prepare request";
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
            "received component update prepare request";
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
        self.last_request_handled =
            Some(SimSpHandledRequest::ComponentUpdateStatus(component));
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
        update_component: SpComponent,
        update_id: gateway_messages::UpdateId,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update abort";
            "component" => ?update_component,
            "id" => ?update_id,
        );
        self.update_state.abort(update_id)
    }

    fn power_state(&mut self) -> Result<PowerState, SpError> {
        let power_state = *self.power_state.borrow();
        debug!(
            &self.log, "received power state";
            "power_state" => ?power_state,
        );
        Ok(power_state.into())
    }

    fn set_power_state(
        &mut self,
        sender: Sender<Self::VLanId>,
        power_state: PowerState,
    ) -> Result<PowerStateTransition, SpError> {
        let prev_power_state = *self.power_state.borrow();
        let transition = if power_state != prev_power_state.into() {
            PowerStateTransition::Changed
        } else {
            PowerStateTransition::Unchanged
        };

        debug!(
            &self.log, "received set power state";
            "sender" => ?sender,
            "prev_power_state" => ?power_state,
            "power_state" => ?power_state,
            "transition" => ?transition,
        );

        let new_power_state = match power_state {
            PowerState::A0 => {
                let slot = self
                    .update_state
                    .component_get_active_slot(SpComponent::HOST_CPU_BOOT_FLASH)
                    .expect("can always get active slot for valid component");
                let slot = M2Slot::from_mgs_firmware_slot(slot)
                    .expect("sp-sim ensures host slot is always valid");
                GimletPowerState::A0(slot)
            }
            PowerState::A1 | PowerState::A2 => GimletPowerState::A2,
        };
        self.power_state.send_modify(|s| {
            *s = new_power_state;
        });

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
            &self.log, "received reset trigger request";
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
        let num_details =
            self.sensors.num_component_details(&component).unwrap_or(0);
        debug!(
            &self.log, "asked for number of component details";
            "component" => ?component,
            "num_details" => num_details
        );
        Ok(num_details)
    }

    fn component_details(
        &mut self,
        component: SpComponent,
        index: BoundsChecked,
    ) -> ComponentDetails {
        let Some(sensor_details) =
            self.sensors.component_details(&component, index)
        else {
            unreachable!(
                "this is a gimlet, so it should have no port status details"
            );
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
        debug!(
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
        debug!(
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
        debug!(
            &self.log, "asked for startup options";
            "options" => ?self.startup_options,
        );
        Ok(self.startup_options)
    }

    fn set_startup_options(
        &mut self,
        startup_options: StartupOptions,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "setting for startup options";
            "options" => ?startup_options,
        );
        self.startup_options = startup_options;
        Ok(())
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
            "received IPCC key/value; not supported by simulated gimlet";
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
    ) -> Result<usize, SpError> {
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
            RotRequest::ReadCmpa => "gimlet-cmpa",
            RotRequest::ReadCfpa(CfpaPage::Active) => "gimlet-cfpa-active",
            RotRequest::ReadCfpa(CfpaPage::Inactive) => "gimlet-cfpa-inactive",
            RotRequest::ReadCfpa(CfpaPage::Scratch) => "gimlet-cfpa-scratch",
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
            &self.log, "received reset trigger with watchdog request";
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

    fn start_host_flash_hash(&mut self, slot: u16) -> Result<(), SpError> {
        self.update_state.start_host_flash_hash(slot)
    }

    fn get_host_flash_hash(&mut self, slot: u16) -> Result<[u8; 32], SpError> {
        self.update_state.get_host_flash_hash(slot)
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
