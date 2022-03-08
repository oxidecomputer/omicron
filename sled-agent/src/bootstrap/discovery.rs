// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled announcement and discovery.

use super::multicast;
use slog::Logger;
use std::collections::HashSet;
use std::io;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Manages Sled Discovery - both our announcement to other Sleds,
/// as well as our discovery of those sleds.
pub struct PeerMonitor {
    sleds: Arc<Mutex<HashSet<SocketAddr>>>,
    _worker: JoinHandle<()>,
}

async fn monitor_worker(
    log: Logger,
    address: SocketAddrV6,
    sender: UdpSocket,
    listener: UdpSocket,
    sleds: Arc<Mutex<HashSet<SocketAddr>>>,
) {
    // Let this message be a reminder that this content is *not*
    // encrypted, authenticated, or otherwise verified. We're just using
    // it as a starting point for swapping addresses.
    let message =
        b"We've been trying to reach you about your car's extended warranty";
    loop {
        let mut buf = vec![0u8; 128];
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(5000)) => {
                trace!(log, "Bootstrap Peer Monitor: Broadcasting our own address: {}", address);
                if let Err(e) = sender.try_send_to(message, address.into()) {
                    warn!(log, "PeerMonitor failed to broadcast: {}", e);
                }
            }
            result = listener.recv_from(&mut buf) => {
                match result {
                    Ok((_, addr)) => {
                        info!(log, "Bootstrap Peer Monitor: Successfully received an address: {}", addr);
                        sleds.lock().await.insert(addr);
                    },
                    Err(e) => warn!(log, "PeerMonitor failed to receive: {}", e),
                }
            }
        }
    }
}

impl PeerMonitor {
    /// Creates a new [`PeerMonitor`].
    // TODO: Address, port, interface, etc, probably should be
    // configuration options.
    pub fn new(log: &Logger) -> Result<Self, io::Error> {
        let scope = multicast::Ipv6MulticastScope::LinkLocal.first_hextet();
        let address = SocketAddrV6::new(
            Ipv6Addr::new(scope, 0, 0, 0, 0, 0, 0, 0x1),
            7645,
            0,
            0,
        );
        let loopback = false;
        let interface = 0;
        let (sender, listener) =
            multicast::new_ipv6_udp_pair(&address, loopback, interface)?;

        let sleds = Arc::new(Mutex::new(HashSet::new()));
        let sleds_for_worker = sleds.clone();
        let log = log.clone();

        let worker = tokio::task::spawn(async move {
            monitor_worker(log, address, sender, listener, sleds_for_worker)
                .await
        });

        Ok(PeerMonitor { sleds, _worker: worker })
    }

    /// Returns the addresses of connected sleds.
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn addrs(&self) -> Vec<SocketAddr> {
        self.sleds.lock().await.iter().map(|addr| *addr).collect()
    }
}
