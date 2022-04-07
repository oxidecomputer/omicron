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
    sleds: Arc<Mutex<HashSet<SocketAddr>>>,
    notification_sender: broadcast::Sender<SocketAddr>,
    _worker: JoinHandle<()>,
}

async fn monitor_worker(
    log: Logger,
    address: SocketAddrV6,
    sender: UdpSocket,
    listener: UdpSocket,
    sleds: Arc<Mutex<HashSet<SocketAddr>>>,
    notification_sender: broadcast::Sender<SocketAddr>,
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
                        let mut sleds = sleds.lock().await;
                        if sleds.insert(addr) {
                            // We don't actually care if no one is listening, so
                            // drop the error if that's the case.
                            let _ = notification_sender.send(addr);
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

        let (tx, _) = tokio::sync::broadcast::channel(PEER_CAPACITY_MAXIMUM);

        let notification_sender = tx.clone();
        let worker = tokio::task::spawn(async move {
            monitor_worker(log, address, sender, listener, sleds_for_worker, notification_sender)
                .await
        });

        Ok(PeerMonitor { sleds, notification_sender: tx, _worker: worker })
    }

    /// Returns the addresses of connected sleds.
    ///
    /// For an interface that allows monitoring the connected sleds, rather
    /// than just sampling at a single point-in-time, consider using
    /// [`Self::observer`].
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn addrs(&self) -> Vec<SocketAddr> {
        self.sleds.lock().await.iter().map(|addr| *addr).collect()
    }

    /// Returns a [`PeerMonitorObserver`] which can be used to view the results
    /// of monitoring for peers.
    pub async fn observer(&self) -> PeerMonitorObserver {
        // Subscribe for notifications of new sleds right away, so
        // we won't miss any notifications.
        let receiver = self.notification_sender.subscribe();

        // Next, clone the exisitng set of sleds.
        //
        // It's possible that we get a notification for a sled which
        // exists in this set, but we handle that in
        // [`PeerMonitorObserver::recv`] to avoid surfacing it to a client.
        let sleds = self.sleds.lock().await.clone();

        PeerMonitorObserver {
            their_sleds: self.sleds.clone(),
            our_sleds: sleds,
            receiver,
        }
    }
}

/// Provides a read-only view of monitored peers, with a mechanism for
/// observing the incoming queue of new peers.
pub struct PeerMonitorObserver {
    // A shared reference to the "true" set of sleds.
    //
    // This is only used to re-synchronize our set of sleds
    // if we get out-of-sync due to long notification queues.
    their_sleds: Arc<Mutex<HashSet<SocketAddr>>>,
    // A local copy of the set of sleds. This lets observers
    // access + iterate over the set of sleds directly,
    // without any possibility of blocking the actual monitoring task.
    our_sleds: HashSet<SocketAddr>,
    receiver: broadcast::Receiver<SocketAddr>,
}

impl PeerMonitorObserver {
    /// Returns the addresses of all connected sleds.
    ///
    /// This returns the most "up-to-date" view of peers, but a new
    /// peer may be added immediately after this function returns.
    ///
    /// To monitor for changes, a call to [`Self::recv`]
    /// can be made, to observe changes beyond an initial call to
    /// [`Self::addrs`].
    pub async fn addrs(&mut self) -> &HashSet<SocketAddr> {
        // First, drain the incoming queue of sled updates.
        loop {
            match self.receiver.try_recv() {
                Ok(new_addr) => {
                    self.our_sleds.insert(new_addr);
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Closed) => panic!("Remote closed"),
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    self.our_sleds = self.their_sleds.lock().await.clone();
                },
            }
        }
        while let Ok(new_addr) = self.receiver.try_recv() {
            self.our_sleds.insert(new_addr);
        }

        // Next, return the most up-to-date set of sleds.
        //
        // Note that this set may change immediately after `addrs()` returns,
        // but a caller can see exactly what sleds were added by calling
        // `recv()`.
        &self.our_sleds
    }

    /// Returns information about a new connected sled.
    ///
    /// Note that this does not provide the "initial set" of connected
    /// sleds - to access that information, call [`Self::addrs`].
    ///
    /// Returns [`Option::None`] if the notification queue overflowed,
    /// and we needed to re-synchronize the set of sleds.
    pub async fn recv(&mut self) -> Option<SocketAddr> {
        loop {
            match self.receiver.recv().await {
                Ok(new_addr) => {
                    if self.our_sleds.insert(new_addr) {
                        return Some(new_addr);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => panic!("Remote closed"),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    self.our_sleds = self.their_sleds.lock().await.clone();
                    return None;
                },
            }
        }
    }
}
