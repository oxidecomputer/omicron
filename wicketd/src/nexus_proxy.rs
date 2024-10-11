// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TCP proxy to expose Nexus's external API via the techport.

use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use omicron_common::address::NEXUS_TECHPORT_EXTERNAL_PORT;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::io;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

pub(crate) struct NexusTcpProxy {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Drop for NexusTcpProxy {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl NexusTcpProxy {
    pub(crate) async fn start(
        listen_addr: SocketAddrV6,
        internal_dns_resolver: Arc<Mutex<Option<Resolver>>>,
        log: &Logger,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(listen_addr).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let inner = Inner {
            listener,
            internal_dns_resolver,
            shutdown_rx,
            log: log.new(o!("component" => "NexusTcpProxy")),
        };

        tokio::spawn(inner.run());

        Ok(Self { shutdown_tx: Some(shutdown_tx) })
    }

    pub(crate) fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            // We want to shutdown the task we spawned; failure to message it
            // means it's already done.
            _ = tx.send(());
        }
    }
}

struct Inner {
    listener: TcpListener,
    internal_dns_resolver: Arc<Mutex<Option<Resolver>>>,
    shutdown_rx: oneshot::Receiver<()>,
    log: Logger,
}

impl Inner {
    async fn run(mut self) {
        loop {
            tokio::select! {
                // Cancel-safe per the docs on `TcpListener::accept()`
                result = self.listener.accept() => {
                    self.spawn_proxy_handler(result);
                }

                // Cancel-safe: awaiting a `&mut Fut` does not drop the future
                _ = &mut self.shutdown_rx => {
                    info!(self.log, "exiting");
                    return;
                }
            }
        }
    }

    fn spawn_proxy_handler(
        &mut self,
        result: io::Result<(TcpStream, SocketAddr)>,
    ) {
        let (stream, log) = match result {
            Ok((stream, peer)) => (stream, self.log.new(o!("peer" => peer))),
            Err(err) => {
                warn!(self.log, "accept() failed"; "err" => %err);
                return;
            }
        };

        info!(log, "accepted connection");

        let Some(resolver) = self.internal_dns_resolver.lock().unwrap().clone()
        else {
            info!(
                log,
                "closing connection; no internal DNS resolver available \
                 (rack subnet unknown?)"
            );
            return;
        };

        tokio::spawn(run_proxy(stream, resolver, log));
    }
}

async fn run_proxy(
    mut client_stream: TcpStream,
    resolver: Resolver,
    log: Logger,
) {
    // Can we talk to the internal DNS server(s) to find Nexus's IPs?
    let nexus_addrs = match resolver.lookup_all_ipv6(ServiceName::Nexus).await {
        Ok(ips) => ips
            .into_iter()
            .map(|ip| {
                SocketAddr::V6(SocketAddrV6::new(
                    ip,
                    NEXUS_TECHPORT_EXTERNAL_PORT,
                    0,
                    0,
                ))
            })
            .collect::<Vec<SocketAddr>>(),
        Err(err) => {
            warn!(
                log, "failed to look up Nexus IP addrs";
                "err" => %err,
            );
            return;
        }
    };

    // Can we connect to any Nexus instance?
    let mut nexus_stream =
        match TcpStream::connect(nexus_addrs.as_slice()).await {
            Ok(stream) => stream,
            Err(err) => {
                warn!(
                    log, "failed to connect to Nexus";
                    "nexus_addrs" => ?nexus_addrs,
                    "err" => %err,
                );
                return;
            }
        };

    let log = match nexus_stream.peer_addr() {
        Ok(addr) => log.new(o!("nexus_addr" => addr)),
        Err(err) => log.new(o!("nexus_addr" =>
                       format!("failed to read Nexus peer addr: {err}"))),
    };
    info!(log, "connected to Nexus");

    match tokio::io::copy_bidirectional(&mut client_stream, &mut nexus_stream)
        .await
    {
        Ok((client_to_nexus, nexus_to_client)) => {
            info!(
                log, "closing successful proxy connection to Nexus";
                "bytes_sent_to_nexus" => client_to_nexus,
                "bytes_sent_to_client" => nexus_to_client,
            );
        }
        Err(err) => {
            warn!(log, "error proxying data to Nexus"; "err" => %err);
        }
    }
}
