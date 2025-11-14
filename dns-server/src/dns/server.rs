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
    /// TCP idle timeout in seconds
    pub timeout_idle_tcp_conns: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "[::]:53".parse().unwrap(),
            timeout_idle_tcp_conns: 10,
        }
    }
}

/// Handle to the DNS server
///
/// Dropping this handle shuts down the DNS server.
pub struct ServerHandle {
    udp_local_address: SocketAddr,
    tcp_local_address: SocketAddr,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.handle.abort()
    }
}

impl ServerHandle {
    pub fn udp_local_address(&self) -> SocketAddr {
        self.udp_local_address
    }
    pub fn tcp_local_address(&self) -> SocketAddr {
        self.tcp_local_address
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
        // Build catalog with a single root Authority that's used to handle all
        // incoming queries.
        //
        // Normally with hickory you'd create a separate Authority for each DNS
        // zone that you want to handle.  We could do this, but it's more
        // annoying to implement (since the set of zones can theoretically
        // change at runtime with an incoming HTTP request) and it's not very
        // useful for us because we're going to funnel all the requests to our
        // (one) Store anyway.  And there's an important case we need to
        // consider: when we get a query for a name inside a zone that we're not
        // authoritative for, we want to report SERVFAIL in order to trigger
        // dumb clients to query the next nameserver.  But when an incoming
        // query doesn't match one of its Authorities, hickory returns REFUSED.
        // This may not trigger the same behavior that we want.  By building one
        // Authority, we ensure that we're invoked for all queries and can
        // return the SERVFAIL that we want.
        info!(&log, "building DNS catalog with root authority");
        let mut catalog = Catalog::new();
        let root_authority = Arc::new(OmicronAuthority::new(
            store.clone(),
            Name::root(),
            log.new(o!("component" => "authority")),
        ));
        catalog.upsert(Name::root().into(), vec![root_authority as Arc<_>]);

        // Bind UDP socket.
        let udp_socket =
            UdpSocket::bind(config.bind_address).await.with_context(|| {
                format!(
                    "DNS server start: UDP bind to {:?}",
                    config.bind_address
                )
            })?;

        let udp_local_address = udp_socket.local_addr().context(
            "DNS server start: failed to get local address of bound UDP socket",
        )?;

        info!(&log, "DNS server bound to UDP address";
            "udp_local_address" => ?udp_local_address
        );

        // Bind TCP socket on the same address.
        let tcp_listener = TcpListener::bind(config.bind_address)
            .await
            .with_context(|| {
                format!(
                    "DNS server start: TCP bind to {:?}",
                    config.bind_address
                )
            })?;
        let tcp_local_address = tcp_listener.local_addr().context(
            "DNS server start: failed to get local address of bound TCP socket",
        )?;

        info!(&log, "DNS server bound to TCP address";
            "tcp_local_address" => ?tcp_local_address,
            "timeout_idle_tcp_conns" => config.timeout_idle_tcp_conns
        );

        // Start the ServerFuture with our Catalog.
        let mut server_future = ServerFuture::new(catalog);
        info!(&log, "created ServerFuture, registering sockets");
        server_future.register_socket(udp_socket);
        server_future.register_listener(
            tcp_listener,
            Duration::from_secs(config.timeout_idle_tcp_conns),
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
            info!(
                &log_clone,
                "ServerFuture block_until_done returned";
                "result" => ?result
            );
            result
        });

        Ok(ServerHandle { udp_local_address, tcp_local_address, handle })
    }
}
