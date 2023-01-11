// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::SpIdentifier;
use super::SwitchPort;
use crate::error::ConfigError;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use gateway_messages::SpPort;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::SwitchPortConfig;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Description of the network interface for a single switch port.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct SwitchPortDescription {
    #[serde(flatten)]
    pub config: SwitchPortConfig,

    /// Map defining the logical identifier of the SP connected to this port for
    /// each of the possible locations where MGS is running (see
    /// [`LocationConfig::names`]).
    pub location: HashMap<String, SpIdentifier>,
}

/// Configure the topology of the rack where MGS is running, and describe how we
/// can determine our own location.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct LocationConfig {
    /// List of human-readable location names; the actual strings don't matter,
    /// but they're used in log messages and to sync with the refined locations
    /// contained in `determination`. For "rack v1" see RFD 250 § 7.2.
    pub names: Vec<String>,

    /// A list of switch ports that can be used to determine which location (of
    /// those listed in `names`) we are.
    pub determination: Vec<LocationDeterminationConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
    pub(super) async fn run_discovery(
        config: ValidatedLocationConfig,
        ports: HashMap<SwitchPort, SwitchPortDescription>,
        sockets: Arc<HashMap<SwitchPort, SingleSp>>,
        log: &Logger,
    ) -> Result<Self, String> {
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
                    &sockets,
                    ports,
                    location_determination,
                    refined_locations_tx,
                    &log,
                )
                .await;
            });
        }

        // Collect responses and solve for a single location
        let location = resolve_location(
            config.names,
            ReceiverStream::new(refined_locations),
            log,
        )
        .await?;

        // based on the resolved location and the input configuration, build the
        // map of port <-> logical ID
        let mut port_to_id = HashMap::with_capacity(ports.len());
        let mut id_to_port = HashMap::with_capacity(ports.len());
        for (port, mut port_config) in ports {
            // construction of `ValidatedLocationConfig` checked that all port
            // configs have entries for all locations, allowing us to unwrap
            // this `remove()`.
            let id = port_config.location.remove(&location).unwrap();
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

    /// Get an iterator of all identifier/port pairs.
    pub(super) fn all_sp_ids(
        &self,
    ) -> impl Iterator<Item = (SwitchPort, SpIdentifier)> + '_ {
        self.port_to_id.iter().map(|(&k, &v)| (k, v))
    }
}

// This repeats the fields of `LocationConfig` but
//
// a) converts `Vec<_>` to `HashSet<_>` (we care about ordering for TOML
//    serialization, but really want set operations)
// b) validates that all the fields that reference each other are self
//    consistent; e.g., there isn't a LocationDeterminationConfig that refers to
//    a nonexistent switch port.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ValidatedLocationConfig {
    names: HashSet<String>,
    determination: Vec<ValidatedLocationDeterminationConfig>,
}

#[derive(Debug, PartialEq, Eq)]
struct ValidatedLocationDeterminationConfig {
    switch_port: SwitchPort,
    sp_port_1: HashSet<String>,
    sp_port_2: HashSet<String>,
}

impl TryFrom<(&'_ HashMap<SwitchPort, SwitchPortDescription>, LocationConfig)>
    for ValidatedLocationConfig
{
    type Error = ConfigError;

    fn try_from(
        (ports, config): (
            &HashMap<SwitchPort, SwitchPortDescription>,
            LocationConfig,
        ),
    ) -> Result<Self, Self::Error> {
        // Helper function to convert a vec into a hashset, recording an error
        // string in `reasons` if the lengths don't match (i.e., the vec
        // contained at least one duplicate)
        fn vec_to_hashset<F>(
            v: Vec<String>,
            reasons: &mut Vec<String>,
            msg: F,
        ) -> HashSet<String>
        where
            F: FnOnce() -> String,
        {
            let n = v.len();
            let hs = v.into_iter().collect::<HashSet<_>>();
            if hs.len() != n {
                reasons.push(msg());
            }
            hs
        }

        // collection of reasons the config is invalid (if any)
        let mut reasons = Vec::new();

        let names = vec_to_hashset(config.names, &mut reasons, || {
            String::from("location names contains at least one duplicate entry")
        });

        // make sure every port has a defined ID for any element of `names`, and
        // no extras
        for (port, port_config) in ports {
            for name in &names {
                if !port_config.location.contains_key(name) {
                    reasons.push(format!(
                        "port {} is missing an ID for location {:?}",
                        port.0, name
                    ));
                }
            }
            for name in port_config.location.keys() {
                if !names.contains(name) {
                    reasons.push(format!(
                        "port {} contains unknown name {:?}",
                        port.0, name
                    ));
                }
            }
        }

        let determination = config
            .determination
            .into_iter()
            .enumerate()
            .map(|(i, det)| {
                // make sure this determination's switch port exists
                let switch_port = SwitchPort(det.switch_port);
                if !ports.contains_key(&switch_port) {
                    reasons.push(format!(
                        "determination {} references a nonexistent switch port ({})",
                        i, det.switch_port
                    ));
                }

                // convert names into hash sets
                let sp_port_1 = vec_to_hashset(det.sp_port_1, &mut reasons, ||
                    format!(
                        "determination `{}.sp_port_1` contains duplicate names",
                        i
                    )
                );
                let sp_port_2 = vec_to_hashset(det.sp_port_2, &mut reasons, ||
                    format!(
                        "determination `{}.sp_port_2` contains duplicate names",
                        i
                    )
                );

                // make sure these hash sets only reference known names
                if !sp_port_1.is_subset(&names) {
                    reasons.push(format!(
                        "determination `{}.sp_port_1` contains unknown names",
                        i
                    ));
                }
                if !sp_port_2.is_subset(&names) {
                    reasons.push(format!(
                        "determination `{}.sp_port_2` contains unknown names",
                        i
                    ));
                }

                // determinations should not be empty; that would result in
                // immediate failure of our location resolution
                if sp_port_1.is_empty() {
                    reasons.push(format!(
                        "determination `{}.sp_port_1` is empty",
                        i
                    ));
                }
                if sp_port_2.is_empty() {
                    reasons.push(format!(
                        "determination `{}.sp_port_2` is empty",
                        i
                    ));
                }

                ValidatedLocationDeterminationConfig {
                    switch_port,
                    sp_port_1,
                    sp_port_2,
                }
            })
            .collect::<Vec<_>>();

        if reasons.is_empty() {
            Ok(Self { names, determination })
        } else {
            Err(ConfigError::InvalidConfig { reasons })
        }
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
    sockets: &HashMap<SwitchPort, SingleSp>,
    port_config: HashMap<SwitchPort, SwitchPortDescription>,
    mut location_determination: Vec<ValidatedLocationDeterminationConfig>,
    refined_locations: mpsc::Sender<(SwitchPort, HashSet<String>)>,
    log: &Logger,
) {
    // Build a collection of futures representing the results of discovering the
    // SP address for each switch port in `sockets`.
    let mut futs = FuturesUnordered::new();
    for (switch_port, _config) in port_config {
        let log = log.clone();
        futs.push(async move {
            // all ports in `port_config` also get sockets bound to them;
            // unwrapping this lookup is fine
            let sp = sockets.get(&switch_port).unwrap();

            // Wait for local startup to complete (finding our interface,
            // binding a socket, etc.).
            if let Err(err) = sp.wait_for_startup_completion().await {
                warn!(
                    log,
                    "SP communicator startup failed; giving up on this port";
                    "switch_port" => ?switch_port,
                    "err" => %err,
                );
                return (switch_port, None);
            }

            // `sp_addr_watch()` can only fail if startup fails, which we just
            // checked; this is safe to unwrap.
            let mut addr_watch = sp.sp_addr_watch().unwrap().clone();

            loop {
                let current = *addr_watch.borrow();
                match current {
                    Some((_addr, sp_port)) => {
                        return (switch_port, Some(sp_port))
                    }
                    None => {
                        addr_watch.changed().await.unwrap();
                    }
                }
            }
        });
    }

    // Wait for responses.
    while let Some((switch_port, sp_port)) = futs.next().await {
        // Skip switch ports where we failed to start our communicator.
        let sp_port = match sp_port {
            Some(sp_port) => sp_port,
            None => continue,
        };

        // See if this port can participate in location determination.
        let pos = match location_determination
            .iter()
            .position(|d| d.switch_port == switch_port)
        {
            Some(pos) => pos,
            None => {
                info!(
                    log, "received discovery response (not used for location)";
                    "switch_port" => ?switch_port,
                    "sp_port" => ?sp_port,
                );
                continue;
            }
        };
        let determination = location_determination.remove(pos);

        let refined = match sp_port {
            SpPort::One => determination.sp_port_1,
            SpPort::Two => determination.sp_port_2,
        };

        // the only failure possible here is that the receiver is gone; that's
        // harmless for us (e.g., maybe it's already fully determined the
        // location and doesn't care about more messages)
        let _ = refined_locations.send((switch_port, refined)).await;
    }

    // TODO If we're exiting, we've now heard from an SP on every port. Is there
    // any reason to continue pinging ports with discovery packets? This is TBD
    // on how we handle sleds being power cycled, replaced, etc.
}

/// Given a list of possible location names (`locations`) and a stream of
/// `determinations` (coming from `discover_sps()` above), resolve which element
/// of `locations` we must be. For example, if `locations` contains `["switch0",
/// "switch1"]` and we receive a determination of `(SwitchPort(1),
/// ["switch0"])`, we'll return `Ok("switch0")`. This process can fail if we get
/// bogus/conflicting determinations or if we exhaust `determinations` without
/// refining to a single location.
///
/// Note that not all bogus/conflicting results will be detected, because this
/// function will short circuit once it has resolved to a single location. For
/// example, if `locations` contains `["a", "b"]` and `determinations` will
/// yield `[(SwitchPort(0), "a"), (SwitchPort(1), "b")]`, we will return
/// `Ok("a")` upon seeing the first determination without noticing the second.
/// On the other hand, if `names` contains `["a", "b", "c"]` and
/// `determinations` will yield `[(SwitchPort(0), ["a", "b"]), (SwitchPort(1),
/// ["c"])]`, we would notice the empty intersection and fail accordingly.
async fn resolve_location<S>(
    mut locations: HashSet<String>,
    determinations: S,
    log: &Logger,
) -> Result<String, String>
where
    S: Stream<Item = (SwitchPort, HashSet<String>)>,
{
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
        0 => Err(String::from(concat!(
            "could not determine unique location ",
            "(all possible locations eliminated)",
        ))),

        _ => {
            let mut remaining = locations.into_iter().collect::<Vec<_>>();
            remaining.sort_unstable();
            Err(format!(
                "could not determine unique location (remaining set `{:?}`)",
                remaining,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SpType;
    use futures::stream;
    use std::vec;

    #[test]
    fn test_config_validation() {
        let bad_ports = HashMap::from([(
            SwitchPort(0),
            SwitchPortDescription {
                config: SwitchPortConfig {
                    listen_addr: "[::1]:0".parse().unwrap(),
                    discovery_addr: "[::1]:0".parse().unwrap(),
                    interface: None,
                },
                location: HashMap::from([
                    (String::from("a"), SpIdentifier::new(SpType::Sled, 0)),
                    // missing "b", has extraneous "c"
                    (String::from("c"), SpIdentifier::new(SpType::Sled, 1)),
                ]),
            },
        )]);
        let bad_config = LocationConfig {
            names: vec![
                String::from("a"),
                String::from("b"),
                String::from("a"), // dupe
            ],
            determination: vec![
                LocationDeterminationConfig {
                    switch_port: 7, // nonexistent port
                    sp_port_1: vec![
                        String::from("a"),
                        String::from("b"),
                        String::from("a"), // dupe
                        String::from("c"), // not listed in `names`
                    ],
                    sp_port_2: vec![], // empty
                },
                LocationDeterminationConfig {
                    switch_port: 0,
                    sp_port_1: vec![], // empty
                    sp_port_2: vec![
                        String::from("a"),
                        String::from("b"),
                        String::from("b"), // dupe
                        String::from("d"), // not listed in `names`
                    ],
                },
            ],
        };

        let err = ValidatedLocationConfig::try_from((&bad_ports, bad_config))
            .unwrap_err();
        let reasons = match err {
            ConfigError::InvalidConfig { reasons } => reasons,
        };

        assert_eq!(
            reasons,
            &[
                "location names contains at least one duplicate entry",
                "port 0 is missing an ID for location \"b\"",
                "port 0 contains unknown name \"c\"",
                "determination 0 references a nonexistent switch port (7)",
                "determination `0.sp_port_1` contains duplicate names",
                "determination `0.sp_port_1` contains unknown names",
                "determination `0.sp_port_2` is empty",
                "determination `1.sp_port_2` contains duplicate names",
                "determination `1.sp_port_2` contains unknown names",
                "determination `1.sp_port_1` is empty",
            ]
        );

        // repeat the config from above but with all errors fixed; note that
        // this config is still logically nonsense, but it doesn't contain any
        // of the errors we check for (mismatched / typo'd names, etc.).
        let good_ports = HashMap::from([(
            SwitchPort(0),
            SwitchPortDescription {
                config: SwitchPortConfig {
                    listen_addr: "[::1]:0".parse().unwrap(),
                    discovery_addr: "[::1]:0".parse().unwrap(),
                    interface: None,
                },
                location: HashMap::from([
                    (String::from("a"), SpIdentifier::new(SpType::Sled, 0)),
                    (String::from("b"), SpIdentifier::new(SpType::Sled, 1)),
                ]),
            },
        )]);
        let good_config = LocationConfig {
            names: vec![String::from("a"), String::from("b")],
            determination: vec![
                LocationDeterminationConfig {
                    switch_port: 0,
                    sp_port_1: vec![String::from("a")],
                    sp_port_2: vec![String::from("b")],
                },
                LocationDeterminationConfig {
                    switch_port: 0,
                    sp_port_1: vec![String::from("b")],
                    sp_port_2: vec![String::from("a")],
                },
            ],
        };

        let config =
            ValidatedLocationConfig::try_from((&good_ports, good_config))
                .unwrap();

        assert_eq!(
            config,
            ValidatedLocationConfig {
                names: HashSet::from([String::from("a"), String::from("b")]),
                determination: vec![
                    ValidatedLocationDeterminationConfig {
                        switch_port: SwitchPort(0),
                        sp_port_1: HashSet::from([String::from("a")]),
                        sp_port_2: HashSet::from([String::from("b")]),
                    },
                    ValidatedLocationDeterminationConfig {
                        switch_port: SwitchPort(0),
                        sp_port_1: HashSet::from([String::from("b")]),
                        sp_port_2: HashSet::from([String::from("a")]),
                    },
                ],
            }
        );
    }

    struct Harness {
        names: HashSet<String>,
        determinations:
            stream::Iter<vec::IntoIter<(SwitchPort, HashSet<String>)>>,
    }

    impl Harness {
        fn new(names: &[&str], determinations: &[&[&str]]) -> Self {
            let determinations: Vec<(SwitchPort, HashSet<String>)> =
                determinations
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

        async fn resolve(self, log: &Logger) -> Result<String, String> {
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
        let reason = harness.resolve(&log).await.unwrap_err();
        assert_eq!(
            reason,
            concat!(
                "could not determine unique location ",
                "(remaining set `[\"a\", \"b\"]`)",
            )
        );

        // determinations that have no name in common should give us an error
        let harness = Harness::new(&["a", "b", "c"], &[&["a", "b"], &["c"]]);
        let reason = harness.resolve(&log).await.unwrap_err();
        assert_eq!(
            reason,
            concat!(
                "could not determine unique location ",
                "(all possible locations eliminated)",
            )
        );
    }
}
