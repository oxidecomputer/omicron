// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::GimletConfig;
use crate::config::SpComponentConfig;
use crate::helpers::rot_slot_id_from_u16;
use crate::helpers::rot_slot_id_to_u16;
use crate::rot::RotSprocketExt;
use crate::serial_number_padded;
use crate::server;
use crate::server::SimSpHandler;
use crate::server::UdpServer;
use crate::update::SimSpUpdate;
use crate::Responsiveness;
use crate::SimulatedSp;
use crate::SIM_ROT_BOARD;
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures::future;
use futures::Future;
use gateway_messages::ignition::{self, LinkEvents};
use gateway_messages::sp_impl::SpHandler;
use gateway_messages::sp_impl::{BoundsChecked, DeviceDescription};
use gateway_messages::CfpaPage;
use gateway_messages::ComponentAction;
use gateway_messages::Header;
use gateway_messages::RotBootInfo;
use gateway_messages::RotRequest;
use gateway_messages::RotResponse;
use gateway_messages::RotSlotId;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpRequest;
use gateway_messages::SpStateV2;
use gateway_messages::{version, MessageKind};
use gateway_messages::{ComponentDetails, Message, MgsError, StartupOptions};
use gateway_messages::{DiscoverResponse, IgnitionState, PowerState};
use slog::{debug, error, info, warn, Logger};
use sprockets_rot::common::msgs::{RotRequestV1, RotResponseV1};
use sprockets_rot::common::Ed25519PublicKey;
use sprockets_rot::{RotSprocket, RotSprocketError};
use std::cell::Cell;
use std::collections::HashMap;
use std::iter;
use std::net::{SocketAddr, SocketAddrV6};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::{self, JoinHandle};

pub const SIM_GIMLET_BOARD: &str = "SimGimletSp";
const SP_GITC0: &[u8] = b"ffffffff";
const SP_GITC1: &[u8] = b"fefefefe";
const SP_BORD: &[u8] = SIM_GIMLET_BOARD.as_bytes();
const SP_NAME: &[u8] = b"SimGimlet";
const SP_VERS0: &[u8] = b"0.0.2";
const SP_VERS1: &[u8] = b"0.0.1";

const ROT_GITC0: &[u8] = b"eeeeeeee";
const ROT_GITC1: &[u8] = b"edededed";
const ROT_BORD: &[u8] = SIM_ROT_BOARD.as_bytes();
const ROT_NAME: &[u8] = b"SimGimletRot";
const ROT_VERS0: &[u8] = b"0.0.4";
const ROT_VERS1: &[u8] = b"0.0.3";

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

pub struct Gimlet {
    rot: Mutex<RotSprocket>,
    manufacturing_public_key: Ed25519PublicKey,
    local_addrs: Option<[SocketAddrV6; 2]>,
    handler: Option<Arc<TokioMutex<Handler>>>,
    serial_console_addrs: HashMap<String, SocketAddrV6>,
    commands: mpsc::UnboundedSender<Command>,
    inner_tasks: Vec<JoinHandle<()>>,
    responses_sent_count: Option<watch::Receiver<usize>>,
    last_request_handled: Arc<Mutex<Option<SimSpHandledRequest>>>,
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
    async fn state(&self) -> omicron_gateway::http_entrypoints::SpState {
        omicron_gateway::http_entrypoints::SpState::from(
            self.handler.as_ref().unwrap().lock().await.sp_state_impl(),
        )
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
        if let Ok(()) = self.commands.send(Command::SetResponsiveness(r, tx)) {
            rx.await.unwrap();
        }
    }

    fn rot_request(
        &self,
        request: RotRequestV1,
    ) -> Result<RotResponseV1, RotSprocketError> {
        self.rot.lock().unwrap().handle_deserialized(request)
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
        slot: u16,
    ) -> Option<Box<[u8]>> {
        let handler = self.handler.as_ref()?;
        let handler = handler.lock().await;
        handler.update_state.last_host_phase1_update_data(slot)
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

impl Gimlet {
    pub async fn spawn(gimlet: &GimletConfig, log: Logger) -> Result<Self> {
        info!(log, "setting up simulated gimlet");

        let attached_mgs = Arc::new(Mutex::new(None));

        let mut incoming_console_tx = HashMap::new();
        let mut serial_console_addrs = HashMap::new();
        let mut inner_tasks = Vec::new();
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let last_request_handled = Arc::default();

        let (manufacturing_public_key, rot) =
            RotSprocket::bootstrap_from_config(&gimlet.common);

        // Weird case - if we don't have any bind addresses, we're only being
        // created to simulate an RoT, so go ahead and return without actually
        // starting a simulated SP.
        let Some(bind_addrs) = gimlet.common.bind_addrs else {
            return Ok(Self {
                rot: Mutex::new(rot),
                manufacturing_public_key,
                local_addrs: None,
                handler: None,
                serial_console_addrs,
                commands,
                inner_tasks,
                responses_sent_count: None,
                last_request_handled,
            });
        };

        // bind to our two local "KSZ" ports
        assert_eq!(bind_addrs.len(), 2); // gimlet SP always has 2 ports
        let servers = future::try_join(
            UdpServer::new(bind_addrs[0], gimlet.common.multicast_addr, &log),
            UdpServer::new(bind_addrs[1], gimlet.common.multicast_addr, &log),
        )
        .await?;
        let servers = [servers.0, servers.1];

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
                        .with_context(|| {
                            "failed to get local address of bound socket"
                        })
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
        let (inner, handler, responses_sent_count) = UdpTask::new(
            servers,
            gimlet.common.components.clone(),
            attached_mgs,
            gimlet.common.serial_number.clone(),
            incoming_console_tx,
            commands_rx,
            Arc::clone(&last_request_handled),
            log,
            gimlet.common.old_rot_state,
            gimlet.common.no_stage0_caboose,
        );
        inner_tasks
            .push(task::spawn(async move { inner.run().await.unwrap() }));

        Ok(Self {
            rot: Mutex::new(rot),
            manufacturing_public_key,
            local_addrs: Some(local_addrs),
            handler: Some(handler),
            serial_console_addrs,
            commands,
            inner_tasks,
            responses_sent_count: Some(responses_sent_count),
            last_request_handled,
        })
    }

    pub fn serial_console_addr(&self, component: &str) -> Option<SocketAddrV6> {
        self.serial_console_addrs.get(component).copied()
    }

    pub fn last_request_handled(&self) -> Option<SimSpHandledRequest> {
        *self.last_request_handled.lock().unwrap()
    }
}

struct SerialConsoleTcpTask {
    listener: TcpListener,
    incoming_serial_console: UnboundedReceiver<Vec<u8>>,
    socks: [Arc<UdpSocket>; 2],
    attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
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
        attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
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
        let (component, sp_port, mgs_addr) =
            match *self.attached_mgs.lock().unwrap() {
                Some((component, sp_port, mgs_addr)) => {
                    (component, sp_port, mgs_addr)
                }
                None => {
                    info!(
                        self.log,
                        "No attached MGS; discarding serial console data"
                    );
                    return Ok(());
                }
            };

        if component != self.component {
            info!(self.log, "MGS is attached to a different component; discarding serial console data");
            return Ok(());
        }

        let sock = match sp_port {
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
            sock.send_to(&out[..n], mgs_addr).await?;
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
}

struct Ack;

struct UdpTask {
    udp0: UdpServer,
    udp1: UdpServer,
    handler: Arc<TokioMutex<Handler>>,
    commands: mpsc::UnboundedReceiver<Command>,
    responses_sent_count: watch::Sender<usize>,
    last_request_handled: Arc<Mutex<Option<SimSpHandledRequest>>>,
}

impl UdpTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        servers: [UdpServer; 2],
        components: Vec<SpComponentConfig>,
        attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
        serial_number: String,
        incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
        commands: mpsc::UnboundedReceiver<Command>,
        last_request_handled: Arc<Mutex<Option<SimSpHandledRequest>>>,
        log: Logger,
        old_rot_state: bool,
        no_stage0_caboose: bool,
    ) -> (Self, Arc<TokioMutex<Handler>>, watch::Receiver<usize>) {
        let [udp0, udp1] = servers;
        let handler = Arc::new(TokioMutex::new(Handler::new(
            serial_number,
            components,
            attached_mgs,
            incoming_serial_console,
            log,
            old_rot_state,
            no_stage0_caboose,
        )));
        let responses_sent_count = watch::Sender::new(0);
        let responses_sent_count_rx = responses_sent_count.subscribe();
        (
            Self {
                udp0,
                udp1,
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
                        }
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

    attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
    incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
    rot_active_slot: RotSlotId,
    power_state: PowerState,
    startup_options: StartupOptions,
    update_state: SimSpUpdate,
    reset_pending: Option<SpComponent>,

    last_request_handled: Option<SimSpHandledRequest>,

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
        attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
        incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
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
            attached_mgs,
            incoming_serial_console,
            rot_active_slot: RotSlotId::A,
            power_state: PowerState::A2,
            startup_options: StartupOptions::empty(),
            update_state: SimSpUpdate::default(),
            reset_pending: None,
            last_request_handled: None,
            should_fail_to_respond_signal: None,
            old_rot_state,
            no_stage0_caboose,
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
            power_state: self.power_state,
            rot: Ok(gateway_messages::RotStateV2 {
                active: RotSlotId::A,
                persistent_boot_preference: RotSlotId::A,
                pending_persistent_boot_preference: None,
                transient_boot_preference: None,
                slot_a_sha3_256_digest: Some([0x55; 32]),
                slot_b_sha3_256_digest: Some([0x66; 32]),
            }),
        }
    }
}

impl SpHandler for Handler {
    type BulkIgnitionStateIter = iter::Empty<IgnitionState>;
    type BulkIgnitionLinkEventsIter = iter::Empty<LinkEvents>;

    fn discover(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<DiscoverResponse, SpError> {
        debug!(
            &self.log,
            "received discover; sending response";
            "sender" => %sender,
            "port" => ?port,
        );
        Ok(DiscoverResponse { sp_port: port })
    }

    fn num_ignition_ports(&mut self) -> Result<u32, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
    ) -> Result<gateway_messages::IgnitionState, SpError> {
        warn!(
            &self.log,
            "received ignition state request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        offset: u32,
    ) -> Result<Self::BulkIgnitionStateIter, SpError> {
        warn!(
            &self.log,
            "received bulk ignition state request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "offset" => offset,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_link_events(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
    ) -> Result<LinkEvents, SpError> {
        warn!(
            &self.log,
            "received ignition link events request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_link_events(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        offset: u32,
    ) -> Result<Self::BulkIgnitionLinkEventsIter, SpError> {
        warn!(
            &self.log,
            "received bulk ignition link events request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "offset" => offset,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    /// If `target` is `None`, clear link events for all targets.
    fn clear_ignition_link_events(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: Option<u8>,
        transceiver_select: Option<ignition::TransceiverSelect>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received clear ignition link events request; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "target" => ?target,
            "transceiver_select" => ?transceiver_select,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_command(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received ignition command; not supported by gimlet";
            "sender" => %sender,
            "port" => ?port,
            "target" => target,
            "command" => ?command,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_attach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console attach request";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );

        let mut attached_mgs = self.attached_mgs.lock().unwrap();
        if attached_mgs.is_some() {
            return Err(SpError::SerialConsoleAlreadyAttached);
        }

        *attached_mgs = Some((component, port, sender));
        Ok(())
    }

    fn serial_console_write(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        offset: u64,
        data: &[u8],
    ) -> Result<u64, SpError> {
        debug!(
            &self.log,
            "received serial console packet";
            "sender" => %sender,
            "port" => ?port,
            "len" => data.len(),
            "offset" => offset,
        );

        let component = self
            .attached_mgs
            .lock()
            .unwrap()
            .map(|(component, _port, _addr)| component)
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
        sender: SocketAddrV6,
        port: SpPort,
    ) -> std::result::Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console keepalive";
            "sender" => %sender,
            "port" => ?port,
        );

        let component = self
            .attached_mgs
            .lock()
            .unwrap()
            .map(|(component, _port, _addr)| component)
            .ok_or(SpError::SerialConsoleNotAttached)?;

        let _incoming_serial_console = self
            .incoming_serial_console
            .get(&component)
            .ok_or(SpError::RequestUnsupportedForComponent)?;

        Ok(())
    }

    fn serial_console_detach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console detach request";
            "sender" => %sender,
            "port" => ?port,
        );
        *self.attached_mgs.lock().unwrap() = None;
        Ok(())
    }

    fn serial_console_break(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received serial console break";
            "sender" => %sender,
            "port" => ?port,
        );
        let component = self
            .attached_mgs
            .lock()
            .unwrap()
            .map(|(component, _port, _addr)| component)
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

    fn send_host_nmi(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received host NMI request; not supported by simulated gimlet";
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
            "received SP update prepare request";
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
            "received component update prepare request";
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
        self.last_request_handled =
            Some(SimSpHandledRequest::ComponentUpdateStatus(component));
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
        update_component: SpComponent,
        update_id: gateway_messages::UpdateId,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update abort";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?update_component,
            "id" => ?update_id,
        );
        self.update_state.abort(update_id)
    }

    fn power_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<PowerState, SpError> {
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
        power_state: PowerState,
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
            &self.log, "received reset trigger request";
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
        debug!(
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
        debug!(
            &self.log, "asked for component active slot";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
        );
        match component {
            SpComponent::ROT => Ok(rot_slot_id_to_u16(self.rot_active_slot)),
            // The only active component is stage0
            SpComponent::STAGE0 => Ok(0),
            // The real SP returns `RequestUnsupportedForComponent` for anything
            // other than the RoT, including SP_ITSELF.
            _ => Err(SpError::RequestUnsupportedForComponent),
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
        debug!(
            &self.log, "asked to set component active slot";
            "sender" => %sender,
            "port" => ?port,
            "component" => ?component,
            "slot" => slot,
            "persist" => persist,
        );
        match component {
            SpComponent::ROT => {
                self.rot_active_slot = rot_slot_id_from_u16(slot)?;
                Ok(())
            }
            SpComponent::STAGE0 => {
                if slot == 1 {
                    return Ok(());
                } else {
                    Err(SpError::RequestUnsupportedForComponent)
                }
            }
            SpComponent::HOST_CPU_BOOT_FLASH => {
                self.update_state.set_active_host_slot(slot);
                Ok(())
            }
            _ => {
                // The real SP returns `RequestUnsupportedForComponent` for anything
                // other than the RoT and host boot flash, including SP_ITSELF.
                Err(SpError::RequestUnsupportedForComponent)
            }
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
        debug!(
            &self.log, "asked for startup options";
            "sender" => %sender,
            "port" => ?port,
            "options" => ?self.startup_options,
        );
        Ok(self.startup_options)
    }

    fn set_startup_options(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        startup_options: StartupOptions,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "setting for startup options";
            "sender" => %sender,
            "port" => ?port,
            "options" => ?startup_options,
        );
        self.startup_options = startup_options;
        Ok(())
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
            "received IPCC key/value; not supported by simulated gimlet";
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
        use crate::SIM_ROT_STAGE0_BOARD;

        const STAGE0_GITC0: &[u8] = b"ddddddddd";
        const STAGE0_GITC1: &[u8] = b"dadadadad";
        const STAGE0_BORD: &[u8] = SIM_ROT_STAGE0_BOARD.as_bytes();
        const STAGE0_NAME: &[u8] = b"SimGimletRot";
        const STAGE0_VERS0: &[u8] = b"0.0.200";
        const STAGE0_VERS1: &[u8] = b"0.0.200";

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

    #[cfg(any(feature = "no-caboose", feature = "old-state"))]
    fn get_component_caboose_value(
        &mut self,
        component: SpComponent,
        slot: u16,
        key: [u8; 4],
        buf: &mut [u8],
    ) -> std::result::Result<usize, SpError> {
        let val = match (component, &key, slot) {
            (SpComponent::SP_ITSELF, b"GITC", 0) => SP_GITC0,
            (SpComponent::SP_ITSELF, b"GITC", 1) => SP_GITC1,
            (SpComponent::SP_ITSELF, b"BORD", _) => SP_BORD,
            (SpComponent::SP_ITSELF, b"NAME", _) => SP_NAME,
            (SpComponent::SP_ITSELF, b"VERS", 0) => SP_VERS0,
            (SpComponent::SP_ITSELF, b"VERS", 1) => SP_VERS1,
            (SpComponent::ROT, b"GITC", 0) => ROT_GITC0,
            (SpComponent::ROT, b"GITC", 1) => ROT_GITC1,
            (SpComponent::ROT, b"BORD", _) => ROT_BORD,
            (SpComponent::ROT, b"NAME", _) => ROT_NAME,
            (SpComponent::ROT, b"VERS", 0) => ROT_VERS0,
            (SpComponent::ROT, b"VERS", 1) => ROT_VERS1,
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
