// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Guts of the DNS (protocol) server within our DNS server program
//!
//! This module uses hickory-server's ServerFuture to provide a full-featured
//! DNS server with eDNS support, TCP transport, and proper UDP truncation.

use crate::dns::authority::OmicronAuthority;
use crate::storage::Store;
use anyhow::Context;
use hickory_proto::rr::Name;
use hickory_server::ServerFuture;
use hickory_server::authority::Catalog;
use serde::Deserialize;
use slog::{Logger, info, o};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};

/// Configuration related to the DNS server
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The address to listen for DNS requests on
    pub bind_address: SocketAddr,
    /// Optional TCP idle timeout in seconds
    /// Defaults to 5 seconds if not specified
    #[serde(default = "default_tcp_idle_timeout_secs")]
    pub tcp_idle_timeout_secs: u64,
}

fn default_tcp_idle_timeout_secs() -> u64 {
    5
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "[::]:53".parse().unwrap(),
            tcp_idle_timeout_secs: default_tcp_idle_timeout_secs(),
        }
    }
}

// OmicronRequestHandler removed - we use Catalog directly since it
// already implements RequestHandler

/// Handle to the DNS server
///
/// Dropping this handle shuts down the DNS server.
pub struct ServerHandle {
    local_address: SocketAddr,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.handle.abort()
    }
}

impl ServerHandle {
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }
}

/// DNS (protocol) server
///
/// You construct one of these with [`Server::start()`].  But what you get back
/// is a [`ServerHandle`].  You don't deal with the Server directly.
pub struct Server;

impl Server {
    /// Starts a DNS server whose DNS data comes from the given `store`
    pub async fn start(
        log: Logger,
        store: Store,
        config: &Config,
    ) -> anyhow::Result<ServerHandle> {
        // Build catalog with a single root Authority
        // The Authority delegates to Store for zone routing
        // Catalog implements RequestHandler, so we use it directly
        info!(&log, "building DNS catalog with root authority");
        let mut catalog = Catalog::new();
        let root_authority = Arc::new(OmicronAuthority::new(
            store.clone(),
            Name::root(),
            log.new(o!("component" => "authority")),
        ));
        catalog.upsert(Name::root().into(), vec![root_authority as Arc<_>]);

        // Bind UDP socket
        let udp_socket =
            UdpSocket::bind(config.bind_address).await.with_context(|| {
                format!(
                    "DNS server start: UDP bind to {:?}",
                    config.bind_address
                )
            })?;

        let local_address = udp_socket.local_addr().context(
            "DNS server start: failed to get local address of bound socket",
        )?;

        info!(&log, "DNS server bound to UDP address";
            "local_address" => ?local_address
        );

        // Bind TCP socket on the same address
        let tcp_listener = TcpListener::bind(config.bind_address)
            .await
            .with_context(|| {
                format!(
                    "DNS server start: TCP bind to {:?}",
                    config.bind_address
                )
            })?;

        info!(&log, "DNS server bound to TCP address";
            "local_address" => ?local_address,
            "tcp_idle_timeout_secs" => config.tcp_idle_timeout_secs
        );

        // Create ServerFuture with our Catalog (which implements RequestHandler)
        let mut server_future = ServerFuture::new(catalog);
        info!(&log, "created ServerFuture, registering sockets");
        server_future.register_socket(udp_socket);
        server_future.register_listener(
            tcp_listener,
            Duration::from_secs(config.tcp_idle_timeout_secs),
        );

        // Spawn the server task
        info!(&log, "spawning ServerFuture task");
        let log_clone = log.clone();
        let handle = tokio::task::spawn(async move {
            info!(
                &log_clone,
                "ServerFuture task started, calling block_until_done"
            );
            let result = server_future
                .block_until_done()
                .await
                .context("DNS server task failed");
            info!(&log_clone, "ServerFuture block_until_done returned"; "result" => ?result);
            result
        });

        Ok(ServerHandle { local_address, handle })
    }
}
