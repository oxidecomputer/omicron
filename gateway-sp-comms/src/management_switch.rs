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

mod location_map;

pub use self::location_map::LocationConfig;
pub use self::location_map::LocationDeterminationConfig;
use self::location_map::LocationMap;
pub use self::location_map::SwitchPortConfig;

use crate::error::BadResponseType;
use crate::error::Error;
use crate::error::SpCommunicationError;
use crate::error::StartupError;
use crate::recv_handler::RecvHandler;
use crate::Communicator;
use crate::Elapsed;
use crate::Timeout;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use gateway_messages::version;
use gateway_messages::Request;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SerializedSize;
use gateway_messages::SpComponent;
use gateway_messages::SpMessage;
use hyper::upgrade::OnUpgrade;
use omicron_common::backoff;
use omicron_common::backoff::Backoff;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use slog::debug;
use slog::Logger;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[serde_as]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SwitchConfig {
    pub local_ignition_controller_port: usize,
    pub location: LocationConfig,
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub port: HashMap<usize, SwitchPortConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct SpIdentifier {
    pub typ: SpType,
    pub slot: usize,
}

impl SpIdentifier {
    pub fn new(typ: SpType, slot: usize) -> Self {
        Self { typ, slot }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpType {
    Switch,
    Sled,
    Power,
}

// We derive `Serialize` to be able to send `SwitchPort`s to usdt probes, but
// critically we do _not_ implement `Deserialize` - the only way to construct a
// `SwitchPort` should be to receive one from a `ManagementSwitch`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize,
)]
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
pub(crate) struct ManagementSwitch {
    local_ignition_controller_port: SwitchPort,
    recv_handler: Arc<RecvHandler>,
    sockets: Arc<HashMap<SwitchPort, UdpSocket>>,
    location_map: LocationMap,
    log: Logger,

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
    pub(crate) async fn new(
        config: SwitchConfig,
        discovery_deadline: Instant,
        log: Logger,
    ) -> Result<Self, StartupError> {
        // begin by binding to all our configured ports; insert them into a map
        // keyed by the switch port they're listening on
        let mut sockets = HashMap::with_capacity(config.port.len());
        // while we're at it, rekey `config.port` to use `SwitchPort` keys
        // instead of `usize`.
        let mut ports = HashMap::with_capacity(config.port.len());
        for (port, port_config) in config.port {
            let addr = port_config.data_link_addr;
            let socket = UdpSocket::bind(addr)
                .await
                .map_err(|err| StartupError::UdpBind { addr, err })?;

            let port = SwitchPort(port);
            sockets.insert(port, socket);
            ports.insert(port, port_config);
        }

        // sanity check the local ignition controller port is bound
        let local_ignition_controller_port =
            SwitchPort(config.local_ignition_controller_port);
        if !ports.contains_key(&local_ignition_controller_port) {
            return Err(StartupError::InvalidConfig {
                reasons: vec![format!(
                    "missing local ignition controller port {}",
                    local_ignition_controller_port.0
                )],
            });
        }

        // set up a handler for incoming packets
        let recv_handler =
            RecvHandler::new(sockets.keys().copied(), log.clone());

        // spawn background task that listens for incoming packets on all ports
        // and passes them to `recv_handler`
        let sockets = Arc::new(sockets);
        let recv_task = {
            let recv_handler = Arc::clone(&recv_handler);
            tokio::spawn(recv_task(
                Arc::clone(&sockets),
                move |port, addr, data| {
                    recv_handler.handle_incoming_packet(port, addr, data)
                },
                log.clone(),
            ))
        };

        // run discovery to figure out the physical location of ourselves (and
        // therefore all SPs we talk to)
        let location_map = LocationMap::run_discovery(
            config.location,
            ports,
            Arc::clone(&sockets),
            Arc::clone(&recv_handler),
            discovery_deadline,
            &log,
        )
        .await?;

        Ok(Self {
            local_ignition_controller_port,
            recv_handler,
            location_map,
            sockets,
            log,
            recv_task,
        })
    }

    /// Get the name of our location.
    ///
    /// This matches one of the names specified as a possible location in the
    /// configuration we were given.
    pub(super) fn location_name(&self) -> &str {
        &self.location_map.location_name()
    }

    /// Get the socket to use to communicate with an SP and the socket address
    /// of that SP.
    pub(crate) fn sp_socket(&self, port: SwitchPort) -> Option<SpSocket<'_>> {
        self.recv_handler.remote_addr(port).map(|addr| {
            let socket = self.sockets.get(&port).unwrap();
            SpSocket {
                location_map: Some(&self.location_map),
                socket,
                addr,
                port,
            }
        })
    }

    /// Get the socket connected to the local ignition controller.
    pub(crate) fn ignition_controller(&self) -> Option<SpSocket<'_>> {
        self.sp_socket(self.local_ignition_controller_port)
    }

    pub(crate) fn switch_port_from_ignition_target(
        &self,
        target: usize,
    ) -> Option<SwitchPort> {
        let port = SwitchPort(target);
        if self.sockets.contains_key(&port) {
            Some(port)
        } else {
            None
        }
    }

    pub(crate) fn switch_port(&self, id: SpIdentifier) -> Option<SwitchPort> {
        self.location_map.id_to_port(id)
    }

    pub(crate) fn switch_port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        self.location_map.port_to_id(port)
    }

    /// Spawn a tokio task responsible for forwarding serial console data
    /// between the SP component on `port` and the websocket connection provided
    /// by `upgrade_fut`.
    pub(crate) fn serial_console_attach(
        &self,
        communicator: Arc<Communicator>,
        port: SwitchPort,
        component: SpComponent,
        sp_ack_timeout: Duration,
        upgrade_fut: OnUpgrade,
    ) -> Result<(), Error> {
        self.recv_handler.serial_console_attach(
            communicator,
            port,
            component,
            sp_ack_timeout,
            upgrade_fut,
        )
    }

    /// Shut down the serial console task associated with the given port and
    /// component, if one exists and is attached.
    pub(crate) fn serial_console_detach(
        &self,
        port: SwitchPort,
        component: &SpComponent,
    ) -> Result<(), Error> {
        self.recv_handler.serial_console_detach(port, component)
    }

    pub(crate) async fn request_response<F, T>(
        &self,
        sp: &SpSocket<'_>,
        kind: RequestKind,
        map_response_kind: F,
        timeout: Option<Timeout>,
    ) -> Result<T, Error>
    where
        F: FnMut(ResponseKind) -> Result<T, BadResponseType>,
    {
        sp.request_response(
            &self.recv_handler,
            kind,
            map_response_kind,
            timeout,
            &self.log,
        )
        .await
    }
}

/// Wrapper for a UDP socket on one of our switch ports that knows the address
/// of the SP connected to this port.
#[derive(Debug)]
pub(crate) struct SpSocket<'a> {
    location_map: Option<&'a LocationMap>,
    socket: &'a UdpSocket,
    addr: SocketAddr,
    port: SwitchPort,
}

impl SpSocket<'_> {
    // TODO The `timeout` we take here is the overall timeout for receiving a
    // response. We only resend the request if the SP sends us a "busy"
    // response; if the SP doesn't answer at all we never resend the request.
    // Should we take a separate timeout for individual sends? E.g., with an
    // overall timeout of 5 sec and a per-request timeout of 1 sec, we could
    // treat "no response at 1 sec" the same as a "busy" and resend the request.
    async fn request_response<F, T>(
        &self,
        recv_handler: &RecvHandler,
        mut kind: RequestKind,
        mut map_response_kind: F,
        timeout: Option<Timeout>,
        log: &Logger,
    ) -> Result<T, Error>
    where
        F: FnMut(ResponseKind) -> Result<T, BadResponseType>,
    {
        // helper to wrap a future in a timeout if we have one
        async fn maybe_with_timeout<F, U>(
            timeout: Option<Timeout>,
            fut: F,
        ) -> Result<U, Elapsed>
        where
            F: Future<Output = U>,
        {
            match timeout {
                Some(t) => t.timeout_at(fut).await,
                None => Ok(fut.await),
            }
        }

        // We'll use exponential backoff if and only if the SP responds with
        // "busy"; any other error will cause the loop below to terminate.
        let mut backoff = backoff::internal_service_policy();

        loop {
            // It would be nicer to use `backoff::retry()` instead of manually
            // stepping the backoff policy, but the dance we do with `kind` to
            // avoid cloning it is hard to map into `retry()` in a way that
            // satisfies the borrow checker. ("The dance we do with `kind` to
            // avoid cloning it" being that we move it into `request` below, and
            // on a busy response from the SP we move it back out into the
            // `kind` local var.)
            let duration = backoff
                .next_backoff()
                .expect("internal backoff policy gave up");
            maybe_with_timeout(timeout, tokio::time::sleep(duration))
                .await
                .map_err(|err| Error::Timeout {
                    timeout: err.duration(),
                    port: self.port.0,
                    sp: self.location_map.map(|lm| lm.port_to_id(self.port)),
                })?;

            // update our recv_handler to expect a response for this request ID
            let (request_id, response_fut) =
                recv_handler.register_request_id(self.port);

            // Serialize and send our request. We know `buf` is large enough for
            // any `Request`, so unwrapping here is fine.
            let request = Request { version: version::V1, request_id, kind };
            let mut buf = [0; Request::MAX_SIZE];
            let n = gateway_messages::serialize(&mut buf, &request).unwrap();
            let serialized_request = &buf[..n];

            // Actual communication, guarded by `timeout` if it's not `None`.
            let result = maybe_with_timeout(timeout, async {
                debug!(
                    log, "sending request";
                    "request" => ?request,
                    "dest_addr" => %self.addr,
                    "port" => ?self.port,
                );
                self.socket
                    .send_to(serialized_request, self.addr)
                    .await
                    .map_err(|err| SpCommunicationError::UdpSend {
                        addr: self.addr,
                        err,
                    })?;

                Ok::<ResponseKind, SpCommunicationError>(response_fut.await?)
            })
            .await
            .map_err(|err| Error::Timeout {
                timeout: err.duration(),
                port: self.port.0,
                sp: self.location_map.map(|lm| lm.port_to_id(self.port)),
            })?;

            match result {
                Ok(response_kind) => {
                    return map_response_kind(response_kind)
                        .map_err(SpCommunicationError::from)
                        .map_err(Error::from)
                }
                Err(SpCommunicationError::SpError(ResponseError::Busy)) => {
                    debug!(
                        log,
                        "SP busy; sleeping before retrying send";
                        "dest_addr" => %self.addr,
                        "port" => ?self.port,
                    );

                    // move `kind` back into local var; required to satisfy
                    // borrow check of this loop
                    kind = request.kind;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}

async fn recv_task<F>(
    ports: Arc<HashMap<SwitchPort, UdpSocket>>,
    mut recv_handler: F,
    log: Logger,
) where
    F: FnMut(SwitchPort, SocketAddr, &[u8]),
{
    // helper function to tag a socket's `.readable()` future with an index; we
    // need this to make rustc happy about the types we push into
    // `recv_all_sockets` below
    async fn readable_with_port(
        port: SwitchPort,
        sock: &UdpSocket,
    ) -> (SwitchPort, &UdpSocket, io::Result<()>) {
        let result = sock.readable().await;
        (port, sock, result)
    }

    // set up collection of futures tracking readability of all switch port
    // sockets
    let mut recv_all_sockets = FuturesUnordered::new();
    for (port, sock) in ports.iter() {
        recv_all_sockets.push(readable_with_port(*port, &sock));
    }

    let mut buf = [0; SpMessage::MAX_SIZE];

    loop {
        // `recv_all_sockets.next()` will never return `None` because we
        // immediately push a new future into it every time we pull one out
        // (to reregister readable interest in the corresponding socket)
        let (port, sock, result) = recv_all_sockets.next().await.unwrap();

        // checking readability of the socket can't fail without violating some
        // internal state in tokio in a presumably-strage way; at that point we
        // don't know how to recover, so just panic and let something restart us
        if let Err(err) = result {
            panic!("error in socket readability: {} (port={:?})", err, port);
        }

        match sock.try_recv_from(&mut buf) {
            Ok((n, addr)) => {
                let buf = &buf[..n];
                probes::recv_packet!(|| (
                    &addr,
                    &port,
                    buf.as_ptr() as usize as u64,
                    buf.len() as u64
                ));
                debug!(
                    log, "received {} bytes", n;
                    "port" => ?port, "addr" => %addr,
                );
                recv_handler(port, addr, &buf[..n]);
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

        // push a new future requesting readability interest, as noted above
        recv_all_sockets.push(readable_with_port(port, sock));
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

        async fn make_management_switch<F>(
            &self,
            mut recv_callback: F,
        ) -> ManagementSwitch
        where
            F: FnMut(SwitchPort, &[u8]) + Send + 'static,
        {
            let log = Logger::root(slog::Discard, slog::o!());

            // Skip the discovery process by constructing a `ManagementSwitch`
            // by hand
            let mut sockets = HashMap::new();
            let mut port_to_id = HashMap::new();
            let mut sp_addrs = HashMap::new();
            for (typ, sp_sockets) in [
                (SpType::Switch, &self.switches),
                (SpType::Sled, &self.sleds),
                (SpType::Power, &self.pscs),
            ] {
                for (slot, sp_sock) in sp_sockets.iter().enumerate() {
                    let port = SwitchPort(sockets.len());
                    let id = SpIdentifier { typ, slot };
                    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

                    sp_addrs.insert(port, sp_sock.local_addr().unwrap());
                    port_to_id.insert(port, id);
                    sockets.insert(port, socket);
                }
            }
            let sockets = Arc::new(sockets);

            let recv_handler =
                RecvHandler::new(sockets.keys().copied(), log.clone());

            // Since we skipped the discovery process, we have to tell
            // `recv_handler` what all the fake SP addresses are.
            for (port, addr) in sp_addrs {
                recv_handler.set_remote_addr(port, addr);
            }

            let recv_task = {
                let recv_handler = Arc::clone(&recv_handler);
                tokio::spawn(recv_task(
                    Arc::clone(&sockets),
                    move |port, addr, data| {
                        recv_handler.handle_incoming_packet(port, addr, data);
                        recv_callback(port, data);
                    },
                    log.clone(),
                ))
            };

            let location_map =
                LocationMap::new_raw(String::from("test"), port_to_id);
            let local_ignition_controller_port = location_map
                .id_to_port(SpIdentifier { typ: SpType::Switch, slot: 0 })
                .unwrap();

            ManagementSwitch {
                local_ignition_controller_port,
                recv_handler,
                location_map,
                sockets,
                log,
                recv_task,
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

        let switch = harness
            .make_management_switch({
                let received = Arc::clone(&received);
                move |port, data: &[u8]| {
                    let mut received = received.lock().unwrap();
                    received.entry(port).or_default().push(data.to_vec());
                }
            })
            .await;

        // Actual test - send a bunch of data to each of the ports...
        let mut expected: HashMap<SwitchPort, Vec<Vec<u8>>> = HashMap::new();
        for i in 0..10 {
            for (port_num, sock) in harness.all_sockets().enumerate() {
                let port = SwitchPort(port_num);
                let data = format!("message {} to {:?}", i, port).into_bytes();

                let addr =
                    switch.sockets.get(&port).unwrap().local_addr().unwrap();
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

        let switch = harness.make_management_switch(|_, _| {}).await;

        // confirm messages sent to the switch's sp sockets show up on our
        // harness sockets
        let mut buf = [0; SpMessage::MAX_SIZE];
        for (typ, sp_sockets) in [
            (SpType::Switch, &harness.switches),
            (SpType::Sled, &harness.sleds),
            (SpType::Power, &harness.pscs),
        ] {
            for (slot, sp_sock) in sp_sockets.iter().enumerate() {
                let port = switch
                    .location_map
                    .id_to_port(SpIdentifier { typ, slot })
                    .unwrap();
                let sock = switch.sp_socket(port).unwrap();
                let local_addr = sock.socket.local_addr().unwrap();

                let message = format!("{:?} {}", typ, slot).into_bytes();
                sock.socket.send_to(&message, sock.addr).await.unwrap();

                let (n, addr) = sp_sock.recv_from(&mut buf).await.unwrap();

                // confirm we received the expected message from the
                // corresponding switch port
                assert_eq!(&buf[..n], message);
                assert_eq!(addr, local_addr);
            }
        }
    }
}

#[usdt::provider(provider = "gateway_sp_comms")]
mod probes {
    fn recv_packet(
        _source: &SocketAddr,
        _port: &SwitchPort,
        _data: u64, // TODO actually a `*const u8`, but that isn't allowed by usdt
        _len: u64,
    ) {
    }
}
