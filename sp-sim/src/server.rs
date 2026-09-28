// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Responsiveness;
use crate::config::Config;
use crate::config::NetworkConfig;
use crate::ereport;
use crate::ereport::EreportState;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use futures::Future;
use futures::future;
use gateway_messages::SpPort;
use gateway_messages::sp_impl;
use gateway_messages::sp_impl::Sender;
use gateway_messages::sp_impl::SpHandler;
use nix::net::if_::if_nametoindex;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

/// Thin wrapper pairing a [`UdpSocket`] with a buffer sized for gateway
/// messages.
pub(crate) struct UdpServer {
    sock: Arc<UdpSocket>,
    local_addr: SocketAddrV6,
    buf: [u8; gateway_messages::MAX_SERIALIZED_SIZE],
}

impl UdpServer {
    pub(crate) async fn new(
        network_config: &NetworkConfig,
        log: &Logger,
    ) -> Result<Self> {
        let sock = match network_config {
            // In some environments where sp-sim runs (e.g., some CI runners),
            // we're not able to join ipv6 multicast groups. In those cases,
            // we're configured with an address that isn't actually multicast,
            // so don't try to join the group if we have such an address.
            NetworkConfig::Simulated { bind_addr } => {
                UdpSocket::bind(bind_addr).await.with_context(|| {
                    format!("failed to bind to {}", bind_addr)
                })?
            }

            NetworkConfig::Real {
                bind_addr,
                multicast_addr,
                multicast_interface,
            } => {
                if !multicast_addr.is_multicast() {
                    bail!("{multicast_addr} is not multicast!");
                }

                let interface_index =
                    if_nametoindex(multicast_interface.as_str()).with_context(
                        || format!("if_nametoindex for {multicast_interface}"),
                    )?;

                let sock =
                    UdpSocket::bind(bind_addr).await.with_context(|| {
                        format!("failed to bind to {}", bind_addr)
                    })?;

                sock.join_multicast_v6(&multicast_addr, interface_index)
                    .with_context(|| {
                        format!(
                            "failed to join multicast group {multicast_addr} \
                            for interface {multicast_interface} index \
                            {interface_index}",
                        )
                    })?;

                sock
            }
        };

        let local_addr = sock
            .local_addr()
            .with_context(|| "failed to get local address of bound socket")
            .and_then(|addr| match addr {
                SocketAddr::V4(addr) => bail!("bound IPv4 address {}", addr),
                SocketAddr::V6(addr) => Ok(addr),
            })?;

        info!(log, "simulated SP UDP socket bound";
            "local_addr" => %local_addr,
            network_config,
        );

        Ok(Self {
            sock: Arc::new(sock),
            local_addr,
            buf: [0; gateway_messages::MAX_SERIALIZED_SIZE],
        })
    }

    pub(crate) fn socket(&self) -> &Arc<UdpSocket> {
        &self.sock
    }

    pub(crate) fn local_addr(&self) -> SocketAddrV6 {
        self.local_addr
    }

    pub(crate) async fn recv_from(&mut self) -> Result<(&[u8], SocketAddrV6)> {
        let (len, addr) = self
            .sock
            .recv_from(&mut self.buf)
            .await
            .with_context(|| "recv_from failed")?;
        let addr = match addr {
            SocketAddr::V4(addr) => {
                bail!("received data from IPv4 address {}", addr)
            }
            SocketAddr::V6(addr) => addr,
        };
        Ok((&self.buf[..len], addr))
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddrV6) -> Result<()> {
        self.sock.send_to(buf, addr).await.with_context(|| "send_to failed")?;
        Ok(())
    }
}

pub fn logger(config: &Config) -> Result<Logger> {
    const NAME: &str = "sp-sim";
    use slog::Drain;

    let (drain, registration) = slog_dtrace::with_drain(
        config.log.to_logger(NAME).with_context(|| "initializing logger")?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!("component" => NAME));
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        bail!("{}", msg);
    } else {
        debug!(log, "registered DTrace probes");
    }
    Ok(log)
}

// TODO: This doesn't need to return Result anymore
pub(crate) async fn handle_request<'a, H: SimSpHandler>(
    handler: &mut H,
    recv: Result<(&[u8], SocketAddrV6)>,
    out: &'a mut [u8; gateway_messages::MAX_SERIALIZED_SIZE],
    responsiveness: Responsiveness,
    port_num: H::VLanId,
) -> Result<Option<(&'a [u8], SocketAddrV6)>>
where
    H::VLanId: std::fmt::Debug,
{
    match responsiveness {
        Responsiveness::Responsive => (), // proceed
        Responsiveness::Unresponsive => {
            // pretend to be unresponsive - drop this packet
            return Ok(None);
        }
    }

    let (data, addr) =
        recv.with_context(|| format!("recv on {:?}", port_num))?;

    let should_respond = Arc::new(AtomicBool::new(true));

    {
        let should_respond = Arc::clone(&should_respond);
        handler.set_sp_should_fail_to_respond_signal(Box::new(move || {
            should_respond.store(false, Ordering::SeqCst);
        }));
    }

    let sender = Sender { addr, vid: port_num };
    let response = sp_impl::handle_message(sender, data, handler, out)
        .map(|n| (&out[..n], addr));

    if should_respond.load(Ordering::SeqCst) { Ok(response) } else { Ok(None) }
}

pub(crate) trait SimSpHandler: SpHandler {
    fn set_sp_should_fail_to_respond_signal(
        &mut self,
        signal: Box<dyn FnOnce() + Send>,
    );
}

/// Commands sent from a simulated SP's handle to its [`UdpTask`].
#[derive(Debug)]
pub(crate) enum Command {
    SetResponsiveness(Responsiveness, oneshot::Sender<Ack>),
    SetThrottler(Option<mpsc::UnboundedReceiver<usize>>, oneshot::Sender<Ack>),
    Ereport(ereport::Command),
}

#[derive(Debug)]
pub(crate) struct Ack;

/// A task that drives a [`SimSpHandler`] implementation (of type `H`) and its
/// associated UDP ports.
///
/// This task receives `gateway-messages` requests on the simulated SP's two UDP
/// ports ereport requests on the two (optional) ereport UDP ports, and
/// [`Command`]s sent by the test framework. Control-plane `gateway-messages`
/// requests are dispatched to the board-specific handler implementation. The
/// handler is locked for the duration of each request.
pub(crate) struct UdpTask<H> {
    udp0: UdpServer,
    udp1: UdpServer,
    ereport_servers: Option<[UdpServer; 2]>,
    ereport_state: EreportState,
    handler: Arc<TokioMutex<H>>,
    commands: mpsc::UnboundedReceiver<Command>,
    responses_sent_count: watch::Sender<usize>,
}

impl<H> UdpTask<H>
where
    H: SimSpHandler<VLanId = SpPort>,
{
    /// Returns the task, along with a receiver for the count of responses it
    /// has sent.
    pub(crate) fn new(
        servers: [UdpServer; 2],
        ereport_servers: Option<[UdpServer; 2]>,
        ereport_state: EreportState,
        handler: Arc<TokioMutex<H>>,
        commands: mpsc::UnboundedReceiver<Command>,
    ) -> (Self, watch::Receiver<usize>) {
        let [udp0, udp1] = servers;
        let responses_sent_count = watch::Sender::new(0);
        let responses_sent_count_rx = responses_sent_count.subscribe();
        (
            Self {
                udp0,
                udp1,
                ereport_servers,
                ereport_state,
                handler,
                commands,
                responses_sent_count,
            },
            responses_sent_count_rx,
        )
    }

    /// Runs the UDP task, handling requests in a loop until the handle is
    /// dropped.
    pub(crate) async fn run(mut self) -> Result<()> {
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
                    if let Some((resp, addr)) = handle_request(
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
                    if let Some((resp, addr)) = handle_request(
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
                        }
                        Command::Ereport(cmd) => self.ereport_state.handle_command(cmd),
                    }
                }
            }
        }
    }
}
