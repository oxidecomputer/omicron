// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::SpIdentifier;
use super::SpSocket;
use super::SwitchPort;
use crate::Timeout;
use crate::communicator::ResponseKindExt;
use crate::error::StartupError;
use crate::recv_handler::RecvHandler;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use gateway_messages::RequestKind;
use gateway_messages::SpPort;
use omicron_common::backoff;
use omicron_common::backoff::Backoff;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::info;
use slog::Logger;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

/// Configuration of a single port of the management network switch.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SwitchPortConfig {
    /// Data link addresses; this is the address on which we should bind a
    /// socket, which will be tagged with the appropriate VLAN for this switch
    /// port (see RFD 250).
    pub data_link_addr: SocketAddr,

    /// Multicast address used to find the SP connected to this port.
    // TODO: The multicast address used should be a single address, not a
    // per-port address. For now we configure it per-port to make dev/test on a
    // single system easier; we can run multiple simulated SPs that all listen
    // to different multicast addresses on one host.
    pub multicast_addr: SocketAddr,

    /// Map defining the logical identifier of the SP connected to this port for
    /// each of the possible locations where MGS is running (see
    /// [`LocationConfig::names`]).
    pub location: HashMap<String, SpIdentifier>,
}

/// Configure the topology of the rack where MGS is running, and describe how we
/// can determine our own location.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct LocationConfig {
    /// List of human-readable location names; the actual strings don't matter,
    /// but they're used in log messages and to sync with the refined locations
    /// contained in `determination`. For "rack v1" see RFD 250 § 7.2.
    pub names: Vec<String>,

    /// A list of switch ports that can be used to determine which location (of
    /// those listed in `names`) we are.
    pub determination: Vec<LocationDeterminationConfig>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct LocationDeterminationConfig {
    /// Which port to contact.
    pub switch_port: usize,

    /// If the SP on `switch_port` communicates with us on its port 1, we must
    /// be one of this set of locations (which should be a subset of our parent
    /// [`LocationConfig`]'s `names).
    pub sp_port_1: Vec<String>,

    /// If the SP on `switch_port` communicates with us on its port 2, we must
    /// be one of this set of locations (which should be a subset of our parent
    /// [`LocationConfig`]'s `names).
    pub sp_port_2: Vec<String>,
}

#[derive(Debug)]
pub(super) struct LocationMap {
    location_name: String,
    port_to_id: HashMap<SwitchPort, SpIdentifier>,
    id_to_port: HashMap<SpIdentifier, SwitchPort>,
}

impl LocationMap {
    // For unit tests we don't want to have to run discovery, so allow
    // construction of a canned `LocationMap`.
    #[cfg(test)]
    pub(super) fn new_raw(
        location_name: String,
        port_to_id: HashMap<SwitchPort, SpIdentifier>,
    ) -> Self {
        let mut id_to_port = HashMap::with_capacity(port_to_id.len());
        for (&port, &id) in port_to_id.iter() {
            id_to_port.insert(id, port);
        }
        Self { location_name, port_to_id, id_to_port }
    }

    pub(super) async fn run_discovery(
        communicator: Arc<crate::Communicator>,
        config: LocationConfig,
        ports: HashMap<SwitchPort, SwitchPortConfig>,
        sockets: Arc<HashMap<SwitchPort, UdpSocket>>,
        recv_handler: Arc<RecvHandler>,
        deadline: Instant,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        // Spawn a task that will send discovery packets on every switch port
        // until it hears back from all SPs with exponential backoff to avoid
        // slamming the network; we expect some ports to never resolve (e.g., if
        // the cubby at the other end is not populated and therefore there is no
        // SP to respond).
        let location_determination = config.determination;
        let (refined_locations_tx, refined_locations) = mpsc::channel(8);
        {
            let log = log.clone();
            let ports = ports.clone();
            tokio::spawn(async move {
                discover_sps(
                    &communicator,
                    &sockets,
                    ports,
                    &recv_handler,
                    location_determination,
                    refined_locations_tx,
                    &log,
                )
                .await;
            });
        }

        // Collect responses and solve for a single location
        let location = match tokio::time::timeout_at(
            deadline,
            resolve_location(
                config.names,
                ReceiverStream::new(refined_locations),
                log,
            ),
        )
        .await
        {
            Ok(Ok(location)) => location,
            Ok(Err(err)) => return Err(err),
            Err(_) => {
                return Err(StartupError::DiscoveryFailed {
                    reason: String::from("timeout"),
                })
            }
        };

        // based on the resolved location and the input configuration, build the
        // map of port <-> logical ID
        let mut port_to_id = HashMap::with_capacity(ports.len());
        let mut id_to_port = HashMap::with_capacity(ports.len());
        for (port, mut port_config) in ports {
            let id =
                port_config.location.remove(&location).ok_or_else(|| {
                    StartupError::InvalidConfig {
                        reason: format!(
                            "port {} has no entry for determined location {:?}",
                            port.0, location,
                        ),
                    }
                })?;
            port_to_id.insert(port, id);
            id_to_port.insert(id, port);
        }

        Ok(Self { location_name: location, port_to_id, id_to_port })
    }

    /// Get the name of our location.
    ///
    /// This matches one of the names specified as a possible location in the
    /// configuration we were given.
    pub(super) fn location_name(&self) -> &str {
        &self.location_name
    }

    /// Get the ID of a given port.
    ///
    /// Panics if `port` was not present in the list of ports provided when
    /// `self` was created.
    pub(super) fn port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        self.port_to_id.get(&port).copied().unwrap()
    }

    /// Get the port associated with the given ID, if it exists.
    pub(super) fn id_to_port(&self, id: SpIdentifier) -> Option<SwitchPort> {
        self.id_to_port.get(&id).copied()
    }
}

/// `discovery_sps()` is spawned as a tokio task. It's responsible for sending
/// discovery packets on all switch ports until it hears back from the SPs at
/// the other end (if they exist and are able to respond). Any responses we hear
/// back that correspond to a port used in `location_determination` will result
/// in a message being send on the `refined_locations` channel with that port
/// and the list of locations we could be in based on the SP's response on that
/// port. Our spawner is responsible for collecting/using those messages.
async fn discover_sps(
    communicator: &crate::Communicator,
    sockets: &HashMap<SwitchPort, UdpSocket>,
    port_config: HashMap<SwitchPort, SwitchPortConfig>,
    _recv_handler: &RecvHandler,
    mut location_determination: Vec<LocationDeterminationConfig>,
    refined_locations: mpsc::Sender<(SwitchPort, Vec<String>)>,
    log: &Logger,
) {
    // Build a collection of futures that sends discovery packets on every port;
    // each future runs until it hears back from an SP (possibly running forever
    // if there is no SP listening on the other end of that port's connection).
    let mut futs = FuturesUnordered::new();
    for (port, config) in port_config {
        futs.push(async move {
            // construct a socket pointed to a multicast addr instead of a
            // specific, known addr
            let socket = SpSocket {
                port,
                addr: config.multicast_addr,
                // all ports in `port_config` also get sockets bound to them;
                // unwrapping this lookup is fine
                socket: sockets.get(&port).unwrap(),
            };

            let mut backoff = backoff::internal_service_policy();
            loop {
                let duration = backoff
                    .next_backoff()
                    .expect("internal backoff policy gave up");
                tokio::time::sleep(duration).await;

                let result = communicator
                    .request_response(
                        &socket,
                        RequestKind::Discover,
                        ResponseKindExt::try_into_discover,
                        // TODO should this timeout be configurable or itself
                        // have some kind of backoff? we're inside a
                        // `backoff::retry()` loop, but if an SP is alive but
                        // slow (i.e., taking longer than this timeout to reply)
                        // we'll never hear it - the response will show up late
                        // and we'll ignore it. For now just leave it at some
                        // reasonably large number; this may solve itself when
                        // we move to some kind of authenticated comms channel.
                        Some(Timeout::from_now(Duration::from_secs(5))),
                    )
                    .await;

                match result {
                    Ok(response) => return (port, response),
                    Err(err) => {
                        debug!(
                            log, "discovery failed; will retry";
                            "port" => ?port,
                            "err" => %err,
                        );
                    }
                }
            }
        });
    }

    // Wait for responses.
    while let Some((port, response)) = futs.next().await {
        // See if this port can participate in location determination.
        let pos = match location_determination
            .iter()
            .position(|d| d.switch_port == port.0)
        {
            Some(pos) => pos,
            None => {
                info!(
                    log, "received discovery response (not used for location)";
                    "port" => ?port,
                    "response" => ?response,
                );
                continue;
            }
        };
        let determination = location_determination.remove(pos);

        let refined = match response.sp_port {
            SpPort::One => determination.sp_port_1,
            SpPort::Two => determination.sp_port_2
        };

        // the only failure possible here is that the receiver is gone; that's
        // harmless for us (e.g., maybe it's already fully determined the
        // location and doesn't care about more messages)
        let _ = refined_locations.send((port, refined)).await;
    }

    // TODO If we're exiting, we've now heard from an SP on every port. Is there
    // any reason to continue pinging ports with discovery packets? This is TBD
    // on how we handle sleds being power cycled, replaced, etc.
}

/// Given a list of possible location names (`names`) and a stream of
/// `determinations` (coming from `discover_sps()` above), resolve which element
/// of `names` we must be. For example, if `names` contains `["switch0",
/// "switch1"]` and we receive a determination of `(SwitchPort(1),
/// ["switch0"])`, we'll return `Ok("switch0")`. This process can fail if we
/// get bogus/conflicting determinations or if we exhaust `determinations`
/// without refining to a single location.
///
/// Note that not all bogus/conflicting results will be detected, because this
/// function will short circuit once it has resolved to a single location. For
/// example, if `names` contains `["a", "b"]` and `determinations` will yield
/// `[(SwitchPort(0), "a"), (SwitchPort(1), "b")]`, we will return `Ok("a")`
/// upon seeing the first determination without noticing the second. On the
/// other hand, if `names` contains `["a", "b", "c"]` and `determinations` will
/// yield `[(SwitchPort(0), ["a", "b"]), (SwitchPort(1), ["c"])]`, we would
/// notice the empty intersection and fail accordingly.
async fn resolve_location<S>(
    names: Vec<String>,
    determinations: S,
    log: &Logger,
) -> Result<String, StartupError>
where
    S: Stream<Item = (SwitchPort, Vec<String>)>,
{
    // `names` is a list of all possible locations we're in (e.g., "switch0"
    // and "switch1". Convert it to a set; we'll _remove_ elements from it as
    // we pull answers out of `determinations`.
    let mut locations = names.into_iter().collect::<HashSet<_>>();

    tokio::pin!(determinations);
    while let Some((port, refined_locations)) = determinations.next().await {
        // we got a successful response from an SP; restrict `locations` to only
        // locations that could be possible given that response.
        debug!(
            log, "received location deterimination response";
            "port" => ?port,
            "refined_locations" => ?refined_locations,
        );
        locations.retain(|name| refined_locations.contains(name));

        // If we're down to 1 location (or 0 if something has gone horribly
        // wrong), we don't need to wait for the remaining answers; we've
        // already fully determined our location.
        if locations.len() <= 1 {
            break;
        }
    }

    match locations.len() {
        1 => Ok(locations.into_iter().next().unwrap()),
        0 => Err(StartupError::DiscoveryFailed {
            reason: String::from(concat!(
                "could not determine unique location ",
                "(all possible locations eliminated)",
            )),
        }),
        _ => {
            let mut remaining = locations.into_iter().collect::<Vec<_>>();
            remaining.sort_unstable();
            Err(StartupError::DiscoveryFailed {
                reason: format!(
                    "could not determine unique location (remaining set `{:?}`)",
                    remaining,
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::vec;

    struct Harness {
        names: Vec<String>,
        determinations: stream::Iter<vec::IntoIter<(SwitchPort, Vec<String>)>>,
    }

    impl Harness {
        fn new(names: &[&str], determinations: &[&[&str]]) -> Self {
            let determinations: Vec<(SwitchPort, Vec<String>)> = determinations
                .iter()
                .enumerate()
                .map(|(i, names)| {
                    (
                        SwitchPort(i),
                        names.iter().copied().map(String::from).collect(),
                    )
                })
                .collect();
            Self {
                names: names.iter().copied().map(String::from).collect(),
                determinations: stream::iter(determinations),
            }
        }

        async fn resolve(self, log: &Logger) -> Result<String, StartupError> {
            resolve_location(self.names, self.determinations, log).await
        }
    }

    #[tokio::test]
    async fn test_resolve_location() {
        let log = Logger::root(slog::Discard, slog::o!());

        // basic test - of a/b/c, all determinations report "a"
        let harness = Harness::new(&["a", "b", "c"], &[&["a"], &["a"], &["a"]]);
        assert_eq!(harness.resolve(&log).await.unwrap(), "a");

        // slightly more interesting - of a/b/c, all determinations report a
        // subset, all of which intersect on "b"
        let harness = Harness::new(
            &["a", "b", "c"],
            &[&["a", "b"], &["a", "b", "c"], &["b", "c"]],
        );
        assert_eq!(harness.resolve(&log).await.unwrap(), "b");

        // failing to resolve to a single location should give us an error
        let harness =
            Harness::new(&["a", "b", "c"], &[&["a", "b"], &["b", "a"]]);
        let err = harness.resolve(&log).await.unwrap_err();
        match err {
            StartupError::DiscoveryFailed { reason } => {
                assert_eq!(
                    reason,
                    concat!(
                        "could not determine unique location ",
                        "(remaining set `[\"a\", \"b\"]`)",
                    )
                );
            }
            _ => panic!("unexpected error {}", err),
        }

        // determinations that have no name in common should give us an error
        let harness = Harness::new(&["a", "b", "c"], &[&["a", "b"], &["c"]]);
        let err = harness.resolve(&log).await.unwrap_err();
        match err {
            StartupError::DiscoveryFailed { reason } => {
                assert_eq!(
                    reason,
                    concat!(
                        "could not determine unique location ",
                        "(all possible locations eliminated)",
                    )
                );
            }
            _ => panic!("unexpected error {}", err),
        }
    }
}
