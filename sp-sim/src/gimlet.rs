// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::GimletConfig;
use crate::config::SpComponentConfig;
use crate::rot::RotSprocketExt;
use crate::serial_number_padded;
use crate::server;
use crate::server::UdpServer;
use crate::Responsiveness;
use crate::SimulatedSp;
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures::future;
use gateway_messages::ignition::{self, LinkEvents};
use gateway_messages::sp_impl::SpHandler;
use gateway_messages::sp_impl::{BoundsChecked, DeviceDescription};
use gateway_messages::RotBootState;
use gateway_messages::RotSlot;
use gateway_messages::RotUpdateDetails;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpRequest;
use gateway_messages::SpState;
use gateway_messages::{version, MessageKind};
use gateway_messages::{ComponentDetails, Message, MgsError, StartupOptions};
use gateway_messages::{
    DiscoverResponse, IgnitionState, ImageVersion, PowerState,
};
use gateway_messages::{Header, RotState};
use slog::{debug, error, info, warn, Logger};
use sprockets_rot::common::msgs::{RotRequestV1, RotResponseV1};
use sprockets_rot::common::Ed25519PublicKey;
use sprockets_rot::{RotSprocket, RotSprocketError};
use std::cell::Cell;
use std::collections::HashMap;
use std::iter;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::{self, JoinHandle};

const SIM_GIMLET_VERSION: ImageVersion = ImageVersion { epoch: 0, version: 0 };

pub struct Gimlet {
    rot: Mutex<RotSprocket>,
    manufacturing_public_key: Ed25519PublicKey,
    local_addrs: Option<[SocketAddrV6; 2]>,
    handler: Option<Arc<TokioMutex<Handler>>>,
    serial_console_addrs: HashMap<String, SocketAddrV6>,
    commands:
        mpsc::UnboundedSender<(Command, oneshot::Sender<CommandResponse>)>,
    inner_tasks: Vec<JoinHandle<()>>,
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
        omicron_gateway::http_entrypoints::SpState::from(Ok::<
            _,
            omicron_gateway::CommunicationError,
        >(
            self.handler.as_ref().unwrap().lock().await.sp_state_impl(),
        ))
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
        if let Ok(()) = self.commands.send((Command::SetResponsiveness(r), tx))
        {
            rx.await.unwrap();
        }
    }

    fn rot_request(
        &self,
        request: RotRequestV1,
    ) -> Result<RotResponseV1, RotSprocketError> {
        self.rot.lock().unwrap().handle_deserialized(request)
    }
}

impl Gimlet {
    pub async fn spawn(gimlet: &GimletConfig, log: Logger) -> Result<Self> {
        info!(log, "setting up simualted gimlet");

        let attached_mgs = Arc::new(Mutex::new(None));

        let mut incoming_console_tx = HashMap::new();
        let mut serial_console_addrs = HashMap::new();
        let mut inner_tasks = Vec::new();
        let (commands, commands_rx) = mpsc::unbounded_channel();

        let (local_addrs, handler) = if let Some(bind_addrs) =
            gimlet.common.bind_addrs
        {
            // bind to our two local "KSZ" ports
            assert_eq!(bind_addrs.len(), 2); // gimlet SP always has 2 ports
            let servers = future::try_join(
                UdpServer::new(
                    bind_addrs[0],
                    gimlet.common.multicast_addr,
                    &log,
                ),
                UdpServer::new(
                    bind_addrs[1],
                    gimlet.common.multicast_addr,
                    &log,
                ),
            )
            .await?;
            let servers = [servers.0, servers.1];

            for component_config in &gimlet.common.components {
                let id = component_config.id.as_str();
                let component = SpComponent::try_from(id)
                    .map_err(|_| anyhow!("component id {:?} too long", id))?;

                if let Some(addr) = component_config.serial_console {
                    let listener =
                        TcpListener::bind(addr).await.with_context(|| {
                            format!("failed to bind to {}", addr)
                        })?;
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
            let local_addrs =
                [servers[0].local_addr(), servers[1].local_addr()];
            let (inner, handler) = UdpTask::new(
                servers,
                gimlet.common.components.clone(),
                attached_mgs,
                gimlet.common.serial_number.clone(),
                incoming_console_tx,
                commands_rx,
                log,
            );
            inner_tasks
                .push(task::spawn(async move { inner.run().await.unwrap() }));

            (Some(local_addrs), Some(handler))
        } else {
            (None, None)
        };

        let (manufacturing_public_key, rot) =
            RotSprocket::bootstrap_from_config(&gimlet.common);
        Ok(Self {
            rot: Mutex::new(rot),
            manufacturing_public_key,
            local_addrs,
            handler,
            serial_console_addrs,
            commands,
            inner_tasks,
        })
    }

    pub fn serial_console_addr(&self, component: &str) -> Option<SocketAddrV6> {
        self.serial_console_addrs.get(component).copied()
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
                    version: version::V2,
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
    SetResponsiveness(Responsiveness),
}

enum CommandResponse {
    SetResponsivenessAck,
}

struct UdpTask {
    udp0: UdpServer,
    udp1: UdpServer,
    handler: Arc<TokioMutex<Handler>>,
    commands:
        mpsc::UnboundedReceiver<(Command, oneshot::Sender<CommandResponse>)>,
}

impl UdpTask {
    fn new(
        servers: [UdpServer; 2],
        components: Vec<SpComponentConfig>,
        attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
        serial_number: String,
        incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
        commands: mpsc::UnboundedReceiver<(
            Command,
            oneshot::Sender<CommandResponse>,
        )>,
        log: Logger,
    ) -> (Self, Arc<TokioMutex<Handler>>) {
        let [udp0, udp1] = servers;
        let handler = Arc::new(TokioMutex::new(Handler::new(
            serial_number,
            components,
            attached_mgs,
            incoming_serial_console,
            log,
        )));
        (Self { udp0, udp1, handler: Arc::clone(&handler), commands }, handler)
    }

    async fn run(mut self) -> Result<()> {
        let mut out_buf = [0; gateway_messages::MAX_SERIALIZED_SIZE];
        let mut responsiveness = Responsiveness::Responsive;
        loop {
            select! {
                recv0 = self.udp0.recv_from() => {
                    if let Some((resp, addr)) = server::handle_request(
                        &mut *self.handler.lock().await,
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
                        &mut *self.handler.lock().await,
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
    power_state: PowerState,
    startup_options: StartupOptions,
}

impl Handler {
    fn new(
        serial_number: String,
        components: Vec<SpComponentConfig>,
        attached_mgs: Arc<Mutex<Option<(SpComponent, SpPort, SocketAddrV6)>>>,
        incoming_serial_console: HashMap<SpComponent, UnboundedSender<Vec<u8>>>,
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
            attached_mgs,
            incoming_serial_console,
            power_state: PowerState::A2,
            startup_options: StartupOptions::empty(),
        }
    }

    fn sp_state_impl(&self) -> SpState {
        const FAKE_GIMLET_MODEL: &[u8] = b"FAKE_SIM_GIMLET";

        let mut model = [0; 32];
        model[..FAKE_GIMLET_MODEL.len()].copy_from_slice(FAKE_GIMLET_MODEL);

        SpState {
            hubris_archive_id: [0; 8],
            serial_number: serial_number_padded(&self.serial_number),
            model,
            revision: 0,
            base_mac_address: [0; 6],
            version: SIM_GIMLET_VERSION,
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
    ) -> Result<SpState, SpError> {
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
        warn!(
            &self.log,
            "received update prepare request; not supported by simulated gimlet";
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
            "received update prepare request; not supported by simulated gimlet";
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
            "received update status request; not supported by simulated gimlet";
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
            "received update chunk; not supported by simulated gimlet";
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
            "received update abort; not supported by simulated gimlet";
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

    fn reset_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received sys-reset prepare request; not supported by simulated gimlet";
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
            &self.log, "received sys-reset trigger request; not supported by simulated gimlet";
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
}
