// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//!
//! Long-running task responsible for tracking the topology of the SPs connected
//! to the management network switch ports.
//!
//! See RFD 250 for details.
//!

use crate::error::StartupError;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use gateway_messages::SerializedSize;
use gateway_messages::SpMessage;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::warn;
use slog::Logger;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

/// TODO For now, we don't do RFD 250-style discovery; instead, we take a
/// hard-coded list of known SPs. Each known SP has two address: the (presumably
/// fake) SP and the "local" address for the corresponding management switch
/// port.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct KnownSp {
    pub sp: SocketAddr,
    pub switch_port: SocketAddr,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct KnownSps {
    /// Must be nonempty. The first is interpreted as the local ignition
    /// controller.
    pub switches: Vec<KnownSp>,
    /// Must be nonempty. TBD which we assume (if any) is our local SP.
    pub sleds: Vec<KnownSp>,
    /// May be empty.
    pub power_controllers: Vec<KnownSp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpIdentifier {
    pub typ: SpType,
    pub slot: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpType {
    Switch,
    Sled,
    Power,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SwitchPort(usize);

impl SwitchPort {
    pub(crate) fn as_ignition_target(self) -> u8 {
        // TODO should we use a u16 to describe ignition targets instead? rack
        // v1 is limited to 36, unclear what ignition will look like in future
        // products
        assert!(
            self.0 <= usize::from(u8::MAX),
            "cannot exceed 255 ignition targets / switch ports"
        );
        self.0 as u8
    }
}

#[derive(Debug)]
pub(crate) struct ManagementSwitchDiscovery {
    inner: Arc<Inner>,
}

impl ManagementSwitchDiscovery {
    // TODO: Replace this with real RFD 250-style discovery. For now, just take
    // a hardcoded list of (simulated) SPs and a list of addresses to bind to.
    // For now, we assumes SPs should talk to the bound addresses in the order
    //
    // ```
    // [switch 0, switch 1, ..., switch n - 1,
    //  sled 0,   sled 1,   ..., sled n - 1,
    //  power_controller 0, ..., power_controller n - 1]
    // ```
    //
    // and any leftover bind addresses are ignored.
    pub(crate) async fn placeholder_start(
        known_sps: KnownSps,
        log: Logger,
    ) -> Result<Self, StartupError> {
        assert!(!known_sps.switches.is_empty(), "at least one switch required");
        assert!(!known_sps.sleds.is_empty(), "at least one sled required");

        let mut ports = Vec::new();
        for known_sps in [
            &known_sps.switches,
            &known_sps.sleds,
            &known_sps.power_controllers,
        ] {
            for bind_addr in known_sps.iter().map(|k| k.switch_port) {
                ports.push(UdpSocket::bind(bind_addr).await.map_err(
                    |err| StartupError::UdpBind { addr: bind_addr, err },
                )?);
            }
        }

        let inner = Arc::new(Inner { log, known_sps, ports });

        Ok(Self { inner })
    }

    /// Get a list of all ports on this switch.
    // TODO 1 currently this only returns configured ports based on
    // `placeholder_start`; eventually it should be all real management switch
    // ports.
    //
    // TODO 2 should we attach the SP type to each port? For now just return a
    // flat list.
    pub(crate) fn all_ports(
        &self,
    ) -> impl ExactSizeIterator<Item = SwitchPort> + 'static {
        (0..self.inner.ports.len()).map(SwitchPort)
    }

    /// Consume `self` and start a long-running task to receive packets on all
    /// ports, calling `recv_callback` for each.
    pub(crate) fn start_recv_task<F>(self, recv_callback: F) -> ManagementSwitch
    where
        F: Fn(SwitchPort, &'_ [u8]) + Send + 'static,
    {
        let recv_task = {
            let inner = Arc::clone(&self.inner);
            tokio::spawn(recv_task(inner, recv_callback))
        };
        ManagementSwitch { inner: self.inner, recv_task }
    }
}

#[derive(Debug)]
pub(crate) struct ManagementSwitch {
    inner: Arc<Inner>,

    // handle to the running task that calls recv on all `switch_ports` sockets;
    // we keep this handle only to kill it when we're dropped
    recv_task: JoinHandle<()>,
}

impl Drop for ManagementSwitch {
    fn drop(&mut self) {
        self.recv_task.abort();
    }
}

impl ManagementSwitch {
    /// Get the socket connected to the local ignition controller.
    pub(crate) fn ignition_controller(&self) -> SpSocket {
        // TODO for now this is guaranteed to exist based on the assertions in
        // `placeholder_start`; once that's replaced by a non-placeholder
        // implementation, revisit this.
        let port = self.inner.switch_port(SpType::Switch, 0).unwrap();
        self.inner.sp_socket(port).unwrap()
    }

    pub(crate) fn switch_port_from_ignition_target(
        &self,
        target: usize,
    ) -> Option<SwitchPort> {
        // TODO this assumes `self.inner.ports` is ordered the same as ignition
        // targets; confirm once we replace `placeholder_start`
        if target < self.inner.ports.len() {
            Some(SwitchPort(target))
        } else {
            None
        }
    }

    pub(crate) fn switch_port(&self, id: SpIdentifier) -> Option<SwitchPort> {
        self.inner.switch_port(id.typ, id.slot)
    }

    pub(crate) fn switch_port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        self.inner.port_to_id(port)
    }

    pub(crate) fn sp_socket(&self, port: SwitchPort) -> Option<SpSocket<'_>> {
        self.inner.sp_socket(port)
    }
}

/// Wrapper for a UDP socket on one of our switch ports that knows the address
/// of the SP connected to this port.
#[derive(Debug)]
pub(crate) struct SpSocket<'a> {
    socket: &'a UdpSocket,
    addr: SocketAddr,
    port: SwitchPort,
}

impl SpSocket<'_> {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn port(&self) -> SwitchPort {
        self.port
    }

    /// Wrapper around `send_to` that uses the SP address stored in `self` as
    /// the destination address.
    pub(crate) async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.socket.send_to(buf, self.addr).await
    }
}

#[derive(Debug)]
struct Inner {
    log: Logger,
    known_sps: KnownSps,

    // One UDP socket per management switch port. For now, this is guaranteed to
    // have a length equal to the sum of the lengths of the fields of
    // `known_sps`; eventually, it will be guaranteed to be the length of the
    // number of ports on the connected management switch.
    ports: Vec<UdpSocket>,
}

impl Inner {
    /// Convert a logical SP location (e.g., "sled 7") into a port on the
    /// management switch.
    fn switch_port(&self, sp_type: SpType, slot: usize) -> Option<SwitchPort> {
        // map `sp_type` down to the range of port slots that cover it
        let (base_slot, num_slots) = match sp_type {
            SpType::Switch => (0, self.known_sps.switches.len()),
            SpType::Sled => {
                (self.known_sps.switches.len(), self.known_sps.sleds.len())
            }
            SpType::Power => (
                self.known_sps.switches.len() + self.known_sps.sleds.len(),
                self.known_sps.power_controllers.len(),
            ),
        };

        if slot < num_slots {
            Some(SwitchPort(base_slot + slot))
        } else {
            None
        }
    }

    /// Convert a [`SwitchPort`] into the logical identifier of the SP connected
    /// to that port.
    fn port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        let mut n = port.0;

        for (typ, num) in [
            (SpType::Switch, self.known_sps.switches.len()),
            (SpType::Sled, self.known_sps.sleds.len()),
            (SpType::Power, self.known_sps.power_controllers.len()),
        ] {
            if n < num {
                return SpIdentifier { typ, slot: n };
            }
            n -= num;
        }

        unreachable!("invalid port instance {:?}", port);
    }

    /// Get the local bound address for the given switch port.
    #[cfg(test)] // for now we only use this in unit tests
    fn local_addr(&self, port: SwitchPort) -> io::Result<SocketAddr> {
        self.ports[port.0].local_addr()
    }

    /// Get the socket to use to communicate with an SP and the socket address
    /// of that SP.
    fn sp_socket(&self, port: SwitchPort) -> Option<SpSocket<'_>> {
        // NOTE: For now, it's not possible for this method to return `None`. We
        // control construction of `SwitchPort`s, and only hand them out for
        // valid "ports" where we know an SP is (at least supposed to) be
        // listening. In the future this may return `None` if there is no SP
        // connected on this port.
        let mut n = port.0;
        for known_sps in [
            &self.known_sps.switches,
            &self.known_sps.sleds,
            &self.known_sps.power_controllers,
        ] {
            if n < known_sps.len() {
                return Some(SpSocket {
                    socket: &self.ports[port.0],
                    addr: known_sps[n].sp,
                    port,
                });
            }
            n -= known_sps.len();
        }

        // We only construct `SwitchPort`s with valid indices, so the only way
        // to get here is if someone constructs two `ManagementSwitch` instances
        // (with different port cardinalities) and then mixes up `SwitchPort`s
        // between them (or other similarly outlandish shenanigans) - fine to
        // panic in that case.
        unreachable!("invalid port {:?}", port)
    }
}

async fn recv_task<F>(inner: Arc<Inner>, mut callback: F)
where
    F: FnMut(SwitchPort, &'_ [u8]),
{
    // helper function to tag a socket's `.readable()` future with an index; we
    // need this to make rustc happy about the types we push into
    // `recv_all_sockets` below
    async fn readable_with_port(
        port: SwitchPort,
        sock: &UdpSocket,
    ) -> (SwitchPort, io::Result<()>) {
        let result = sock.readable().await;
        (port, result)
    }

    // set up collection of futures tracking readability of all switch port
    // sockets
    let mut recv_all_sockets = FuturesUnordered::new();
    for (i, sock) in inner.ports.iter().enumerate() {
        recv_all_sockets.push(readable_with_port(SwitchPort(i), sock));
    }

    let mut buf = [0; SpMessage::MAX_SIZE];

    loop {
        // `recv_all_sockets.next()` will never return `None` because we
        // immediately push a new future into it every time we pull one out
        // (to reregister readable interest in the corresponding socket)
        let (port, result) = recv_all_sockets.next().await.unwrap();

        // checking readability of the socket can't fail without violating some
        // internal state in tokio in a presumably-strage way; at that point we
        // don't know how to recover, so just panic and let something restart us
        if let Err(err) = result {
            panic!("error in socket readability: {} (port={:?})", err, port);
        }

        // immediately push a new future requesting readability interest, as
        // noted above
        let sock = &inner.ports[port.0];
        recv_all_sockets.push(readable_with_port(port, sock));

        match sock.try_recv_from(&mut buf) {
            Ok((n, addr)) => {
                if Some(addr) != inner.sp_socket(port).map(|s| s.addr) {
                    // TODO-security: we received a packet from an address that
                    // doesn't match what we believe is the SP's address. for
                    // now, log and discard; what should we really do?
                    warn!(
                        inner.log,
                        "discarding packet from unknown source";
                        "port" => ?port,
                        "src_addr" => addr,
                    );
                } else {
                    debug!(inner.log, "received {} bytes", n; "port" => ?port);
                    callback(port, &buf[..n]);
                }
            }
            // spurious wakeup; no need to log, just continue
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
            // other kinds of errors should be rare/impossible given our use of
            // UDP and the way tokio is structured; we don't know how to recover
            // from them, so just panic and let something restart us
            Err(err) => {
                panic!("error in recv_from: {} (port={:?})", err, port);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use omicron_test_utils::dev::poll;
    use omicron_test_utils::dev::poll::CondCheckError;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::mem;
    use std::sync::Mutex;
    use std::time::Duration;

    // collection of sockets that act like SPs for the purposes of these tests
    struct Harness {
        switches: Vec<UdpSocket>,
        sleds: Vec<UdpSocket>,
        pscs: Vec<UdpSocket>,
    }

    impl Harness {
        async fn new() -> Self {
            const NUM_SWITCHES: usize = 1;
            const NUM_SLEDS: usize = 2;
            const NUM_POWER_CONTROLLERS: usize = 1;

            let mut switches = Vec::with_capacity(NUM_SWITCHES);
            let mut sleds = Vec::with_capacity(NUM_SLEDS);
            let mut pscs = Vec::with_capacity(NUM_POWER_CONTROLLERS);
            for _ in 0..NUM_SWITCHES {
                switches.push(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            }
            for _ in 0..NUM_SLEDS {
                sleds.push(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            }
            for _ in 0..NUM_POWER_CONTROLLERS {
                pscs.push(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            }

            Self { switches, sleds, pscs }
        }

        fn all_sockets(&self) -> impl Iterator<Item = &'_ UdpSocket> {
            self.switches
                .iter()
                .chain(self.sleds.iter())
                .chain(self.pscs.iter())
        }

        fn as_known_sps(&self) -> KnownSps {
            KnownSps {
                switches: self
                    .switches
                    .iter()
                    .map(|sock| KnownSp {
                        sp: sock.local_addr().unwrap(),
                        switch_port: "127.0.0.1:0".parse().unwrap(),
                    })
                    .collect(),
                sleds: self
                    .sleds
                    .iter()
                    .map(|sock| KnownSp {
                        sp: sock.local_addr().unwrap(),
                        switch_port: "127.0.0.1:0".parse().unwrap(),
                    })
                    .collect(),
                power_controllers: self
                    .pscs
                    .iter()
                    .map(|sock| KnownSp {
                        sp: sock.local_addr().unwrap(),
                        switch_port: "127.0.0.1:0".parse().unwrap(),
                    })
                    .collect(),
            }
        }
    }

    #[tokio::test]
    async fn test_recv_task() {
        let harness = Harness::new().await;

        // create a switch, pointed at our harness's fake SPs, with a
        // callback that accumlates received packets into a hashmap
        let received: Arc<Mutex<HashMap<SwitchPort, Vec<Vec<u8>>>>> =
            Arc::default();
        let discovery = ManagementSwitchDiscovery::placeholder_start(
            harness.as_known_sps(),
            Logger::root(slog::Discard, slog::o!()),
        )
        .await
        .unwrap();
        let switch = discovery.start_recv_task({
            let received = Arc::clone(&received);
            move |port, data| {
                let mut received = received.lock().unwrap();
                received.entry(port).or_default().push(data.to_vec());
            }
        });

        // Actual test - send a bunch of data to each of the ports...
        let mut expected: HashMap<SwitchPort, Vec<Vec<u8>>> = HashMap::new();
        for i in 0..10 {
            for (port_num, sock) in harness.all_sockets().enumerate() {
                let port = SwitchPort(port_num);
                let data = format!("message {} to {:?}", i, port).into_bytes();

                let addr = switch.inner.local_addr(port).unwrap();
                sock.send_to(&data, addr).await.unwrap();
                expected.entry(port).or_default().push(data);
            }
        }
        // ... and confirm we received them all. messages should be in order per
        // socket, but we don't check ordering of messages across sockets since
        // that may vary
        {
            let received = Arc::clone(&received);
            poll::wait_for_condition(
                move || {
                    let result = if expected == *received.lock().unwrap() {
                        Ok(())
                    } else {
                        Err(CondCheckError::<Infallible>::NotYet)
                    };
                    future::ready(result)
                },
                &Duration::from_millis(10),
                &Duration::from_secs(1),
            )
            .await
            .unwrap();
        }

        // before dropping `switch`, confirm that the count on `received` is
        // exactly 2: us and the receive task. after dropping `switch` it will
        // be only us.
        assert_eq!(Arc::strong_count(&received), 2);

        // dropping `switch` should cancel its corresponding recv task, which
        // we can confirm by checking that the ref count on `received` drops to
        // 1 (just us). we have to poll for this since it's not necessarily
        // immediate; recv_task is presumably running on a tokio thread
        mem::drop(switch);
        poll::wait_for_condition(
            move || {
                let result = match Arc::strong_count(&received) {
                    1 => Ok(()),
                    2 => Err(CondCheckError::<Infallible>::NotYet),
                    n => panic!("bogus count {}", n),
                };
                future::ready(result)
            },
            &Duration::from_millis(10),
            &Duration::from_secs(1),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_sp_socket() {
        let harness = Harness::new().await;

        let discovery = ManagementSwitchDiscovery::placeholder_start(
            harness.as_known_sps(),
            Logger::root(slog::Discard, slog::o!()),
        )
        .await
        .unwrap();
        let switch = discovery.start_recv_task(|_port, _data| { /* ignore */ });

        // confirm messages sent to the switch's sp sockets show up on our
        // harness sockets
        let mut buf = [0; SpMessage::MAX_SIZE];
        for (typ, sp_sockets) in [
            (SpType::Switch, &harness.switches),
            (SpType::Sled, &harness.sleds),
            (SpType::Power, &harness.pscs),
        ] {
            for (slot, sp_sock) in sp_sockets.iter().enumerate() {
                let port = switch.inner.switch_port(typ, slot).unwrap();
                let sock = switch.inner.sp_socket(port).unwrap();

                let message = format!("{:?} {}", typ, slot).into_bytes();
                sock.send(&message).await.unwrap();

                let (n, addr) = sp_sock.recv_from(&mut buf).await.unwrap();

                // confirm we received the expected message from the
                // corresponding switch port
                assert_eq!(&buf[..n], message);
                assert_eq!(addr, switch.inner.local_addr(port).unwrap());
            }
        }
    }
}
