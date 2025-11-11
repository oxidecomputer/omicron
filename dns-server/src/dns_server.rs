// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Guts of the DNS (protocol) server within our DNS server program
//!
//! This module uses hickory-server's ServerFuture to provide a full-featured
//! DNS server with eDNS support, TCP transport, and proper UDP truncation.

use crate::storage::Store;
use anyhow::Context;
use hickory_server::authority::Catalog;
use hickory_server::server::{Request, ResponseHandler, ResponseInfo};
use hickory_server::ServerFuture;
use serde::Deserialize;
use slog::{Logger, info};
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

/// Request handler that wraps our catalog with watch-based updates
///
/// This handler receives updates to the catalog through a watch channel,
/// ensuring it always uses the latest zone configuration without locks.
struct OmicronRequestHandler {
    catalog_rx: tokio::sync::watch::Receiver<Arc<Catalog>>,
}

impl OmicronRequestHandler {
    fn new(catalog_rx: tokio::sync::watch::Receiver<Arc<Catalog>>) -> Self {
        Self { catalog_rx }
    }
}

impl hickory_server::server::RequestHandler for OmicronRequestHandler {
    fn handle_request<'life0, 'life1, 'async_trait, R>(
        &'life0 self,
        request: &'life1 Request,
        response_handle: R,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = ResponseInfo>
                + Send
                + 'async_trait,
        >,
    >
    where
        R: 'async_trait + ResponseHandler,
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        // Clone the latest catalog Arc from the watch channel
        // Note: We must borrow INSIDE the async block to get the latest value at request time
        let mut catalog_rx = self.catalog_rx.clone();

        // Create an async block that owns the catalog receiver
        Box::pin(async move {
            // Borrow the catalog NOW, at request handling time
            let catalog = catalog_rx.borrow_and_update().clone();
            catalog.handle_request(request, response_handle).await
        })
    }
}

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
        // Get the catalog receiver from the store
        let catalog_rx = store.catalog_receiver();

        // Create the request handler
        let handler = OmicronRequestHandler::new(catalog_rx);

        // Bind UDP socket
        let udp_socket = UdpSocket::bind(config.bind_address)
            .await
            .with_context(|| {
                format!("DNS server start: UDP bind to {:?}", config.bind_address)
            })?;

        let local_address = udp_socket
            .local_addr()
            .context("DNS server start: failed to get local address of bound socket")?;

        info!(&log, "DNS server bound to UDP address";
            "local_address" => ?local_address
        );

        // Bind TCP socket on the same address
        let tcp_listener = TcpListener::bind(config.bind_address)
            .await
            .with_context(|| {
                format!("DNS server start: TCP bind to {:?}", config.bind_address)
            })?;

        info!(&log, "DNS server bound to TCP address";
            "local_address" => ?local_address,
            "tcp_idle_timeout_secs" => config.tcp_idle_timeout_secs
        );

        // Create ServerFuture and register both UDP and TCP
        let mut server_future = ServerFuture::new(handler);
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
            info!(&log_clone, "ServerFuture task started, calling block_until_done");
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
