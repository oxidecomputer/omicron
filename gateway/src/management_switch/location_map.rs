// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::SpIdentifier;
use super::SwitchPort;
use crate::error::StartupError;
use futures::Stream;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use gateway_messages::SpPort;
use gateway_sp_comms::SingleSp;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use serde::Deserialize;
use serde::Serialize;
use slog::Logger;
use slog::debug;
use slog::info;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Description of the network interface for a single switch port.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SwitchPortConfig {
    SwitchZoneInterface {
        interface: String,
    },
    #[serde(rename_all = "kebab-case")]
    Simulated {
        fake_interface: String,
        addr: SocketAddrV6,
        ereport_addr: SocketAddrV6,
    },
}

impl SwitchPortConfig {
    pub fn interface(&self) -> &str {
        match self {
            SwitchPortConfig::SwitchZoneInterface { interface } => interface,
            SwitchPortConfig::Simulated { fake_interface, .. } => {
                fake_interface
            }
        }
    }
}

/// Description of the network interface and location of a single switch port.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
// We can't use `#[serde(deny_unknown_fields)]` with `flatten`? Sad
#[serde(rename_all = "kebab-case")]
pub struct SwitchPortDescription {
    #[serde(flatten)]
    pub config: SwitchPortConfig,
    pub ignition_target: u8,

    /// Map defining the logical identifier of the SP connected to this port for
    /// each of the possible locations where MGS is running (see
    /// [`LocationConfig::names`]).
    pub location: HashMap<String, SpIdentifier>,
}

/// Configure the topology of the rack where MGS is running, and describe how we
/// can determine our own location.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocationConfig {
    /// Description of the locations where MGS might be running.
    ///
    /// For "rack v1" see RFD 250 § 7.2.
    pub description: Vec<LocationDescriptionConfig>,

    /// A list of switch ports that can be used to determine which location (of
    /// those listed in `names`) we are.
    pub determination: Vec<LocationDeterminationConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocationDescriptionConfig {
    /// Human-readable name; the actual string doesn't matter, but it's used in
    /// log messages and to sync with the refined locations contained in
    /// [`LocationDeterminationConfig`].
    pub name: String,

    /// Index of the sled on which MGS in this location is running.
    pub local_sled: u16,

    /// Config option controlling whether MGS will accept a request to reset the
    /// SP of its local sled.
    ///
    /// MGS resetting its local sled's SP is dangerous during SP updates,
    /// because the "reset" operation involves a watchdog that requires MGS to
    /// send a "disarm the watchdog" message _after_ the reset, which it can't
    /// do if it just powered itself off.
    pub allow_local_sled_sp_reset: bool,
}

impl IdOrdItem for LocationDescriptionConfig {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    iddqd::id_upcast!();
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocationDeterminationConfig {
    /// Interfaces of one or more  SPs to use for determining our location.
    pub interfaces: Vec<String>,

    /// If the SP on one of the `interfaces` communicates with us on its port 1,
    /// we must be one of this set of locations (which must be a subset of our
    /// parent [`LocationConfig`]'s names).
    pub sp_port_1: Vec<String>,

    /// If the SP on one of the `interfaces` communicates with us on its port 2,
    /// we must be one of this set of locations (which must be a subset of our
    /// parent [`LocationConfig`]'s names).
    pub sp_port_2: Vec<String>,
}

#[derive(Debug)]
pub(super) struct LocationMap {
    local_sled: u16,
    allow_local_sled_sp_reset: bool,
    port_to_id: HashMap<SwitchPort, SpIdentifier>,
    id_to_port: HashMap<SpIdentifier, SwitchPort>,
}

impl LocationMap {
    pub(super) async fn run_discovery(
        config: ValidatedLocationConfig,
        ports: Vec<SwitchPortDescription>,
        sockets: Arc<Vec<SingleSp>>,
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
            tokio::spawn(async move {
                discover_sps(
                    &sockets,
                    location_determination,
                    refined_locations_tx,
                    &log,
                )
                .await;
            });
        }

        // Collect responses and solve for a single location
        let location = resolve_location(
            config.description.iter().map(|d| d.name.as_str()).collect(),
            ReceiverStream::new(refined_locations),
            log,
        )
        .await?;

        // `resolve_location` can only return a string we gave it, so we know we
        // can look the full description back up from just the name.
        let description = config
            .description
            .get(location.as_str())
            .expect("resolve_location() returned a location we gave it");

        // based on the resolved location and the input configuration, build the
        // map of port <-> logical ID
        let mut port_to_id = HashMap::with_capacity(ports.len());
        let mut id_to_port = HashMap::with_capacity(ports.len());
        for (i, mut port_config) in ports.into_iter().enumerate() {
            let port = SwitchPort(i);
            // construction of `ValidatedLocationConfig` checked that all port
            // configs have entries for all locations, allowing us to unwrap
            // this `remove()`.
            let id = port_config.location.remove(&location).unwrap();
            port_to_id.insert(port, id);
            id_to_port.insert(id, port);
        }

        Ok(Self {
            local_sled: description.local_sled,
            allow_local_sled_sp_reset: description.allow_local_sled_sp_reset,
            port_to_id,
            id_to_port,
        })
    }

    /// Get the slot number for our local sled.
    pub(super) fn local_sled(&self) -> u16 {
        self.local_sled
    }

    /// Get whether our config told us it's okay to reset our local sled.
    pub(super) fn allow_local_sled_sp_reset(&self) -> bool {
        self.allow_local_sled_sp_reset
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
    description: IdOrdMap<LocationDescriptionConfig>,
    determination: Vec<ValidatedLocationDeterminationConfig>,
}

#[derive(Debug, PartialEq, Eq)]
struct ValidatedLocationDeterminationConfig {
    switch_port: SwitchPort,
    sp_port_1: HashSet<String>,
    sp_port_2: HashSet<String>,
}

impl ValidatedLocationConfig {
    pub(super) fn validate(
        ports: &[SwitchPortDescription],
        interface_to_port: &HashMap<String, SwitchPort>,
        config: LocationConfig,
    ) -> Result<Self, StartupError> {
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

        let description = {
            let n = config.description.len();
            let description =
                config.description.into_iter().collect::<IdOrdMap<_>>();

            // Validate that we have no duplicate location names.
            if n != description.len() {
                reasons.push(String::from(
                    "location descriptions contain at least one duplicate name",
                ));
            }

            // Validate that we have no duplicate `local_sled` values.
            let local_sled = description
                .iter()
                .map(|d| d.local_sled)
                .collect::<HashSet<_>>();
            if n != local_sled.len() {
                reasons.push(String::from(
                    "location descriptions contain at least one \
                     duplicate local_sled",
                ));
            }

            description
        };

        // Most of our validation below only cares about our description's name
        // fields; collect those into a set.
        let names =
            description.iter().map(|d| d.name.clone()).collect::<HashSet<_>>();

        // make sure every port has a defined ID for any element of `names`, and
        // no extras
        for port_desc in ports {
            for name in &names {
                if !port_desc.location.contains_key(name) {
                    reasons.push(format!(
                        "port {:?} is missing an ID for location {name:?}",
                        port_desc.config.interface()
                    ));
                }
            }
            for name in port_desc.location.keys() {
                if !names.contains(name) {
                    reasons.push(format!(
                        "port {:?} contains unknown location {name:?}",
                        port_desc.config.interface()
                    ));
                }
            }
        }

        let determination = config
            .determination
            .into_iter()
            .enumerate()
            .flat_map(|(i, det)| {
                // convert names into hash sets
                let sp_port_1 = vec_to_hashset(det.sp_port_1, &mut reasons, ||
                    format!(
                        "determination `{i}.sp_port_1` contains duplicate names",
                    )
                );
                let sp_port_2 = vec_to_hashset(det.sp_port_2, &mut reasons, ||
                    format!(
                        "determination `{i}.sp_port_2` contains duplicate names",
                    )
                );

                // make sure these hash sets only reference known names
                if !sp_port_1.is_subset(&names) {
                    reasons.push(format!(
                        "determination `{i}.sp_port_1` contains unknown names",
                    ));
                }
                if !sp_port_2.is_subset(&names) {
                    reasons.push(format!(
                        "determination `{i}.sp_port_2` contains unknown names",
                    ));
                }

                // determinations should not be empty; that would result in
                // immediate failure of our location resolution
                if sp_port_1.is_empty() {
                    reasons.push(format!(
                        "determination `{i}.sp_port_1` is empty",
                    ));
                }
                if sp_port_2.is_empty() {
                    reasons.push(format!(
                        "determination `{i}.sp_port_2` is empty",
                    ));
                }

                det.interfaces.into_iter().filter_map(|interface| {
                    if let Some(switch_port) = interface_to_port
                        .get(&interface)
                        .copied()
                    {
                        Some(ValidatedLocationDeterminationConfig {
                            switch_port,
                            sp_port_1: sp_port_1.clone(),
                            sp_port_2: sp_port_2.clone(),
                        })
                    } else {
                        reasons.push(format!(
                            "determination {i} references unknown interface {interface:?}"
                        ));
                        None
                    }
                }).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        if reasons.is_empty() {
            Ok(Self { description, determination })
        } else {
            Err(StartupError::InvalidConfig { reasons })
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
    sockets: &[SingleSp],
    mut location_determination: Vec<ValidatedLocationDeterminationConfig>,
    refined_locations: mpsc::Sender<(String, HashSet<String>)>,
    log: &Logger,
) {
    // Build a collection of futures representing the results of discovering the
    // SP address for each switch port in `sockets`.
    let mut futs = FuturesUnordered::new();
    for (i, sp) in sockets.iter().enumerate() {
        futs.push(async move {
            let mut addr_watch = sp.sp_addr_watch().clone();

            loop {
                let current = *addr_watch.borrow();
                match current {
                    Some((_addr, sp_port)) => {
                        return (
                            SwitchPort(i),
                            sp.interface().to_string(),
                            sp_port,
                        );
                    }
                    None => {
                        addr_watch.changed().await.unwrap();
                    }
                }
            }
        });
    }

    // Wait for responses.
    while let Some((switch_port, interface, sp_port)) = futs.next().await {
        // See if this port can participate in location determination.
        let pos = match location_determination
            .iter()
            .position(|d| d.switch_port == switch_port)
        {
            Some(pos) => {
                info!(
                    log, "received discovery response (used for location)";
                    "interface" => &interface,
                    "sp_port" => ?sp_port,
                    "pos" => pos,
                );
                pos
            }
            None => {
                info!(
                    log, "received discovery response (not used for location)";
                    "interface" => interface,
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
        let _ = refined_locations.send((interface, refined)).await;
    }
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
    mut locations: HashSet<&str>,
    determinations: S,
    log: &Logger,
) -> Result<String, String>
where
    S: Stream<Item = (String, HashSet<String>)>,
{
    tokio::pin!(determinations);
    while let Some((interface, refined_locations)) = determinations.next().await
    {
        // we got a successful response from an SP; restrict `locations` to only
        // locations that could be possible given that response.
        debug!(
            log, "received location determination response";
            "interface" => interface,
            "refined_locations" => ?refined_locations,
        );
        locations.retain(|name| refined_locations.contains(*name));

        // If we're down to 1 location (or 0 if something has gone horribly
        // wrong), we don't need to wait for the remaining answers; we've
        // already fully determined our location.
        if locations.len() <= 1 {
            break;
        }
    }

    match locations.len() {
        1 => Ok(locations.into_iter().next().unwrap().to_string()),
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
        let bad_ports = vec![SwitchPortDescription {
            config: SwitchPortConfig::Simulated {
                fake_interface: "fake".to_string(),
                addr: "[::1]:0".parse().unwrap(),
                ereport_addr: "[::1]:0".parse().unwrap(),
            },
            ignition_target: 0,
            location: HashMap::from([
                (String::from("a"), SpIdentifier::new(SpType::Sled, 0)),
                // missing "b", has extraneous "c"
                (String::from("c"), SpIdentifier::new(SpType::Sled, 1)),
            ]),
        }];
        let bad_config = LocationConfig {
            description: vec![
                LocationDescriptionConfig {
                    name: String::from("a"),
                    local_sled: 14,
                    allow_local_sled_sp_reset: false,
                },
                LocationDescriptionConfig {
                    name: String::from("b"),
                    local_sled: 14, // dupe
                    allow_local_sled_sp_reset: false,
                },
                LocationDescriptionConfig {
                    name: String::from("a"), // dupe
                    local_sled: 16,
                    allow_local_sled_sp_reset: false,
                },
            ],
            determination: vec![
                LocationDeterminationConfig {
                    interfaces: vec!["nonexistent-interface".to_string()],
                    sp_port_1: vec![
                        String::from("a"),
                        String::from("b"),
                        String::from("a"), // dupe
                        String::from("c"), // not listed in `names`
                    ],
                    sp_port_2: vec![], // empty
                },
                LocationDeterminationConfig {
                    interfaces: vec!["fake".to_string()],
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

        let mut interface_to_port = HashMap::new();
        interface_to_port.insert("fake".to_string(), SwitchPort(0));
        assert_eq!(bad_ports.len(), interface_to_port.len());

        let err = ValidatedLocationConfig::validate(
            &bad_ports,
            &interface_to_port,
            bad_config,
        )
        .unwrap_err();
        let reasons = match err {
            StartupError::InvalidConfig { reasons } => reasons,
            StartupError::BindError(err) => panic!("expected error: {err}"),
        };

        assert_eq!(
            reasons,
            &[
                "location descriptions contain at least one duplicate name",
                "location descriptions contain at least one duplicate local_sled",
                "port \"fake\" is missing an ID for location \"b\"",
                "port \"fake\" contains unknown location \"c\"",
                "determination `0.sp_port_1` contains duplicate names",
                "determination `0.sp_port_1` contains unknown names",
                "determination `0.sp_port_2` is empty",
                "determination 0 references unknown interface \"nonexistent-interface\"",
                "determination `1.sp_port_2` contains duplicate names",
                "determination `1.sp_port_2` contains unknown names",
                "determination `1.sp_port_1` is empty",
            ]
        );

        // repeat the config from above but with all errors fixed; note that
        // this config is still logically nonsense, but it doesn't contain any
        // of the errors we check for (mismatched / typo'd names, etc.).
        let good_ports = vec![SwitchPortDescription {
            config: SwitchPortConfig::Simulated {
                fake_interface: "fake".to_string(),
                addr: "[::1]:0".parse().unwrap(),
                ereport_addr: "[::1]:0".parse().unwrap(),
            },
            ignition_target: 0,
            location: HashMap::from([
                (String::from("a"), SpIdentifier::new(SpType::Sled, 0)),
                (String::from("b"), SpIdentifier::new(SpType::Sled, 1)),
            ]),
        }];
        let good_config = LocationConfig {
            description: vec![
                LocationDescriptionConfig {
                    name: String::from("a"),
                    local_sled: 14,
                    allow_local_sled_sp_reset: false,
                },
                LocationDescriptionConfig {
                    name: String::from("b"),
                    local_sled: 16,
                    allow_local_sled_sp_reset: false,
                },
            ],
            determination: vec![
                LocationDeterminationConfig {
                    interfaces: vec!["fake".to_string()],
                    sp_port_1: vec![String::from("a")],
                    sp_port_2: vec![String::from("b")],
                },
                LocationDeterminationConfig {
                    interfaces: vec!["fake".to_string()],
                    sp_port_1: vec![String::from("b")],
                    sp_port_2: vec![String::from("a")],
                },
            ],
        };
        let good_description =
            good_config.description.iter().cloned().collect();

        let config = ValidatedLocationConfig::validate(
            &good_ports,
            &interface_to_port,
            good_config,
        )
        .unwrap();

        assert_eq!(
            config,
            ValidatedLocationConfig {
                description: good_description,
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
        names: HashSet<&'static str>,
        determinations: stream::Iter<vec::IntoIter<(String, HashSet<String>)>>,
    }

    impl Harness {
        fn new(names: &[&'static str], determinations: &[&[&str]]) -> Self {
            let determinations: Vec<(String, HashSet<String>)> = determinations
                .iter()
                .enumerate()
                .map(|(i, names)| {
                    (
                        format!("harness-interface-{i}"),
                        names.iter().copied().map(String::from).collect(),
                    )
                })
                .collect();
            Self {
                names: names.iter().copied().collect(),
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
