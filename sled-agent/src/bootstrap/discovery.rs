// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled announcement and discovery.

use super::multicast;
use slog::Logger;
use std::collections::HashSet;
use std::io;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

// NOTE: This is larger than the expected number of sleds per rack, as
// peers may change as new sleds are swapped in for old ones.
//
// See the "TODO" below about removal of sleds from the HashSet
const PEER_CAPACITY_MAXIMUM: usize = 128;

/// Manages Sled Discovery - both our announcement to other Sleds,
/// as well as our discovery of those sleds.
pub struct PeerMonitor {
    // TODO: When can we remove sleds from this HashSet? Presumably, if a sled
    // has been detached from the bootstrap network, we should drop it.
    //
    // Without such removal, the set size will be unbounded (though admittedly,
    // growing slowly).
    //
    // Options:
    // - Have some sort of expiration mechanism? This could turn the set of
    // sleds here into "the sleds which we know were connected within the past
    // hour", for example.
    // - Have some other interface to identify the detachment of a peer.
    our_address: Ipv6Addr,
    sleds: Arc<Mutex<HashSet<Ipv6Addr>>>,
    notification_sender: broadcast::Sender<Ipv6Addr>,
    _worker: JoinHandle<()>,
}

async fn monitor_worker(
    log: Logger,
    sender: UdpSocket,
    listener: UdpSocket,
    sleds: Arc<Mutex<HashSet<Ipv6Addr>>>,
    notification_sender: broadcast::Sender<Ipv6Addr>,
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
                if let Err(e) = sender.try_send_to(message, SocketAddr::V6(multicast::multicast_address())) {
                    warn!(log, "PeerMonitor failed to broadcast: {}", e);
                }
            }
            result = listener.recv_from(&mut buf) => {
                match result {
                    Ok((_, addr)) => {
                        match addr {
                            SocketAddr::V6(addr) =>  {
                                let mut sleds = sleds.lock().await;
                                if sleds.insert(*addr.ip()) {
                                    info!(log, "Bootstrap Peer Monitor: Successfully received an address: {}", addr);
                                    // We don't actually care if no one is listening, so
                                    // drop the error if that's the case.
                                    let _ = notification_sender.send(*addr.ip());
                                }
                            }
                            _ => continue,
                        }
                    },
                    Err(e) => warn!(log, "PeerMonitor failed to receive: {}", e),
                }
            }
        }
    }
}

impl PeerMonitor {
    /// Creates a new [`PeerMonitor`].
    pub fn new(log: &Logger, address: Ipv6Addr) -> Result<Self, io::Error> {
        let loopback = false;
        let interface = 0;
        let (sender, listener) =
            multicast::new_ipv6_udp_pair(&address, loopback, interface)?;

        let sleds = Arc::new(Mutex::new(HashSet::new()));
        let sleds_for_worker = sleds.clone();
        let log = log.clone();

        let (tx, _) = tokio::sync::broadcast::channel(PEER_CAPACITY_MAXIMUM);

        let notification_sender = tx.clone();
        let worker = tokio::task::spawn(async move {
            monitor_worker(
                log,
                sender,
                listener,
                sleds_for_worker,
                notification_sender,
            )
            .await
        });

        Ok(PeerMonitor {
            our_address: address,
            sleds,
            notification_sender: tx,
            _worker: worker,
        })
    }

    /// Returns the addresses of connected sleds.
    ///
    /// For an interface that allows monitoring the connected sleds, rather
    /// than just sampling at a single point-in-time, consider using
    /// [`Self::observer`].
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn peer_addrs(&self) -> Vec<Ipv6Addr> {
        self.sleds.lock().await.iter().map(|addr| *addr).collect()
    }

    /// Returns a [`PeerMonitorObserver`] which can be used to view the results
    /// of monitoring for peers.
    pub async fn observer(&self) -> PeerMonitorObserver {
        PeerMonitorObserver {
            our_address: self.our_address,
            actual_sleds: self.sleds.clone(),
            sender: self.notification_sender.clone(),
        }
    }
}

/// Provides a read-only view of monitored peers, with a mechanism for
/// observing the incoming queue of new peers.
pub struct PeerMonitorObserver {
    our_address: Ipv6Addr,
    // A shared reference to the "true" set of sleds.
    //
    // This is only used to re-synchronize our set of sleds
    // if we get out-of-sync due to long notification queues.
    actual_sleds: Arc<Mutex<HashSet<Ipv6Addr>>>,
    sender: broadcast::Sender<Ipv6Addr>,
}

impl PeerMonitorObserver {
    /// Returns the address of this sled.
    pub fn our_address(&self) -> Ipv6Addr {
        self.our_address
    }

    /// Returns the current set of sleds and a receiver to hear about
    /// new ones.
    pub async fn subscribe(
        &mut self,
    ) -> (HashSet<Ipv6Addr>, broadcast::Receiver<Ipv6Addr>) {
        let sleds = self.actual_sleds.lock().await;
        let receiver = self.sender.subscribe();
        (sleds.clone(), receiver)
    }
}
