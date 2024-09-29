// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics produced by the sled-agent for collection by oximeter.

use illumos_utils::running_zone::RunningZone;
use omicron_common::api::internal::shared::SledIdentifiers;
use oximeter_instruments::kstat::link::SledDataLink;
use oximeter_instruments::kstat::link::SledDataLinkTarget;
use oximeter_instruments::kstat::CollectionDetails;
use oximeter_instruments::kstat::Error as KstatError;
use oximeter_instruments::kstat::KstatSampler;
use oximeter_instruments::kstat::TargetId;
use oximeter_producer::LogConfig;
use oximeter_producer::Server as ProducerServer;
use slog::Logger;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

type TrackedLinks = HashMap<String, Target>;

/// The interval on which we ask `oximeter` to poll us for metric data.
const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(30);

/// The interval on which we sample link metrics.
//
// TODO(https://github.com/oxidecomputer/omicron/issues/5695)
// These should probably be sampled much densely. We may want to wait for
// https://github.com/oxidecomputer/omicron/issues/740, which would handle
// pagination between the producer and collector, as sampling at < 1s for many
// links could lead to quite large requests. Or we can eat the memory cost for
// now.
const LINK_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// The interval after which we expire kstat-based collection of transient
/// links.
///
/// This applies to VNICs and OPTE ports. Physical links are never expired,
/// since we should never expect them to disappear. While we strive to get
/// notifications before these transient links are deleted, it's always possible
/// we miss that, and so the data collection fails. If that fails for more than
/// this interval, we stop attempting to collect its data.
const TRANSIENT_LINK_EXPIRATION_INTERVAL: Duration = Duration::from_secs(60);

/// The maximum Dropshot request size for the metrics server.
const METRIC_REQUEST_MAX_SIZE: usize = 10 * 1024 * 1024;

/// Size of the queue used to send messages to the metrics task.
const QUEUE_SIZE: usize = 64;

/// An error during sled-agent metric production.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kstat-based metric failure")]
    Kstat(#[source] KstatError),

    #[error("Failed to start metric producer server")]
    ProducerServer(#[source] oximeter_producer::Error),
}

/// Messages sent to the sled-agent metrics collection task.
///
/// The sled-agent publish metrics to Oximeter, including statistics about
/// datalinks. This metrics task runs in the background, and code that creates
/// or deletes objects can notify this task to start / stop tracking statistics
/// for them.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, allow(dead_code))]
pub(crate) enum Message {
    /// Start tracking the named physical link.
    ///
    /// This is only use on startup, to track the underlays.
    TrackPhysical { zone_name: String, name: String },
    /// Track the named VNIC.
    TrackVnic { zone_name: String, name: String },
    /// Stop tracking the named VNIC.
    UntrackVnic { name: String },
    /// Track the named OPTE port.
    TrackOptePort { zone_name: String, name: String },
    /// Stop tracking the named OPTE port.
    UntrackOptePort { name: String },
    /// Notify the task that a sled has been synced with NTP.
    TimeSynced { sled_id: Uuid },
    // TODO-completeness: We will probably want to track other kinds of
    // statistics here too. For example, we could send messages when a zone is
    // created / destroyed to track zonestats; we might also want to support
    // passing an explicit `oximeter::Producer` type in, so that other code
    // could attach their own producers.
}

/// Helper to define kinds of tracked links.
struct LinkKind;

impl LinkKind {
    const PHYSICAL: &'static str = "physical";
    const VNIC: &'static str = "vnic";
    const OPTE: &'static str = "opte";
}

struct Target {
    id: TargetId,
    sled_datalink: SledDataLink,
}

fn get_collection_details(kind: &str) -> CollectionDetails {
    if is_transient_link(kind) {
        CollectionDetails::duration(
            LINK_SAMPLE_INTERVAL,
            TRANSIENT_LINK_EXPIRATION_INTERVAL,
        )
    } else {
        CollectionDetails::never(LINK_SAMPLE_INTERVAL)
    }
}

/// The main task used to collect and publish sled-agent metrics.
async fn metrics_task(
    sled_identifiers: SledIdentifiers,
    kstat_sampler: KstatSampler,
    _server: ProducerServer,
    log: Logger,
    mut rx: mpsc::Receiver<Message>,
) {
    let mut tracked_links: TrackedLinks = HashMap::new();
    let mut sled_time_synced: bool = false;

    // Main polling loop, waiting for messages from other pieces of the code to
    // track various statistics.
    loop {
        let Some(message) = rx.recv().await else {
            error!(log, "channel closed, exiting");
            return;
        };
        trace!(log, "received message"; "message" => ?message);

        match message {
            Message::TrackPhysical { zone_name, name } => {
                let target = SledDataLinkTarget {
                    kind: LinkKind::PHYSICAL.into(),
                    link_name: name.into(),
                    rack_id: sled_identifiers.rack_id,
                    sled_id: sled_identifiers.sled_id,
                    sled_model: sled_identifiers.model.clone().into(),
                    sled_revision: sled_identifiers.revision,
                    sled_serial: sled_identifiers.serial.clone().into(),
                    zone_name: zone_name.into(),
                };
                let link = SledDataLink::new(target, sled_time_synced);
                add_datalink(&log, &mut tracked_links, &kstat_sampler, link)
                    .await;
            }
            Message::TrackVnic { zone_name, name } => {
                let target = SledDataLinkTarget {
                    kind: LinkKind::VNIC.into(),
                    link_name: name.into(),
                    rack_id: sled_identifiers.rack_id,
                    sled_id: sled_identifiers.sled_id,
                    sled_model: sled_identifiers.model.clone().into(),
                    sled_revision: sled_identifiers.revision,
                    sled_serial: sled_identifiers.serial.clone().into(),
                    zone_name: zone_name.into(),
                };
                let link = SledDataLink::new(target, sled_time_synced);
                add_datalink(&log, &mut tracked_links, &kstat_sampler, link)
                    .await;
            }
            Message::UntrackVnic { name } => {
                remove_datalink(&log, &mut tracked_links, &kstat_sampler, name)
                    .await
            }
            Message::TrackOptePort { zone_name, name } => {
                let target = SledDataLinkTarget {
                    kind: LinkKind::OPTE.into(),
                    link_name: name.into(),
                    rack_id: sled_identifiers.rack_id,
                    sled_id: sled_identifiers.sled_id,
                    sled_model: sled_identifiers.model.clone().into(),
                    sled_revision: sled_identifiers.revision,
                    sled_serial: sled_identifiers.serial.clone().into(),
                    zone_name: zone_name.into(),
                };
                let link = SledDataLink::new(target, sled_time_synced);
                add_datalink(&log, &mut tracked_links, &kstat_sampler, link)
                    .await;
            }
            Message::UntrackOptePort { name } => {
                remove_datalink(&log, &mut tracked_links, &kstat_sampler, name)
                    .await
            }
            Message::TimeSynced { sled_id } => {
                assert!(!sled_time_synced, "This message should only be sent once (on first synchronization with NTP)");
                if sled_id == sled_identifiers.sled_id {
                    sled_time_synced = true;
                    sync_sled_datalinks(
                        &log,
                        &mut tracked_links,
                        &kstat_sampler,
                    )
                    .await
                }
            }
        }
    }
}

/// Stop tracking a link by name.
async fn remove_datalink(
    log: &Logger,
    tracked_links: &mut HashMap<String, Target>,
    kstat_sampler: &KstatSampler,
    name: String,
) {
    match tracked_links.remove(&name) {
        Some(target) => match kstat_sampler.remove_target(target.id).await {
            Ok(_) => {
                debug!(
                    log,
                    "Removed VNIC from tracked links";
                    "link_name" => name,
                );
            }
            Err(err) => {
                error!(
                    log,
                    "Failed to remove VNIC from kstat sampler, \
                    metrics may still be produced for it";
                    "link_name" => name,
                    "error" => ?err,
                );
            }
        },
        None => {
            debug!(
                log,
                "received message to delete VNIC, but \
                it is not in the list of tracked links";
                "link_name" => name,
            );
        }
    }
}

/// Start tracking a new link of the specified kind.
async fn add_datalink(
    log: &Logger,
    tracked_links: &mut HashMap<String, Target>,
    kstat_sampler: &KstatSampler,
    link: SledDataLink,
) {
    match tracked_links.entry(link.link_name().to_string()) {
        Entry::Vacant(entry) => {
            let details = get_collection_details(link.kind());
            let link_to_add = link.clone();
            match kstat_sampler.add_target(link_to_add, details).await {
                Ok(id) => {
                    debug!(
                        log,
                        "Added new link to kstat sampler";
                        "link_name" => entry.key(),
                        "link_kind" => %link.kind(),
                        "zone_name" => %link.zone_name(),
                    );
                    entry.insert(Target { id, sled_datalink: link });
                }
                Err(err) => {
                    error!(
                        log,
                        "Failed to add VNIC to kstat sampler, \
                        no metrics will be collected for it";
                        "link_name" => entry.key(),
                        "link_kind" => %link.kind(),
                        "zone_name" => %link.zone_name(),
                        "error" => ?err,
                    );
                }
            }
        }
        Entry::Occupied(entry) => {
            debug!(
                log,
                "received message to track VNIC, \
                but it is already being tracked";
                "link_name" => entry.key(),
            );
        }
    }
}

/// Update tracked links when a sled is synced.
async fn sync_sled_datalinks(
    log: &Logger,
    tracked_links: &mut TrackedLinks,
    kstat_sampler: &KstatSampler,
) {
    for (link_name, target) in tracked_links.iter_mut() {
        target.sled_datalink.time_synced = true;
        let details = get_collection_details(target.sled_datalink.kind());
        match kstat_sampler
            .update_target(target.sled_datalink.clone(), details)
            .await
        {
            Ok(_) => {
                debug!(
                    log,
                    "Updated link already tracked by kstat sampler";
                    "link_name" => link_name,
                );
            }
            Err(err) => {
                error!(
                    log,
                    "Failed to update link already tracked by kstat sampler";
                    "link_name" => link_name,
                    "error" => ?err,
                );
            }
        }
    }
}

/// Return true if this is considered a transient link, from the perspective of
/// its expiration behavior.
fn is_transient_link(kind: &str) -> bool {
    kind == LinkKind::VNIC || kind == LinkKind::OPTE
}

/// Manages sled-based metrics reported to Oximeter.
///
/// This object is used to sample kernel statistics and produce other Oximeter
/// metrics for the sled agent. It runs a small background task responsible for
/// actually generating / reporting samples. Users operate with it through the
/// `MetricsHandle`.
#[derive(Debug)]
pub struct MetricsManager {
    /// Sender-side of a channel used to pass the background task messages.
    #[cfg_attr(test, allow(dead_code))]
    tx: mpsc::Sender<Message>,
    /// The background task itself.
    _task: tokio::task::JoinHandle<()>,
}

impl MetricsManager {
    /// Construct a new metrics manager.
    pub fn new(
        log: &Logger,
        identifiers: SledIdentifiers,
        address: Ipv6Addr,
    ) -> Result<Self, Error> {
        let sampler = KstatSampler::new(log).map_err(Error::Kstat)?;
        let server = start_producer_server(&log, identifiers.sled_id, address)?;
        server
            .registry()
            .register_producer(sampler.clone())
            .expect("actually infallible");
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let task_log = log.new(o!("component" => "metrics-task"));
        let _task = tokio::task::spawn(metrics_task(
            identifiers,
            sampler,
            server,
            task_log,
            rx,
        ));
        Ok(Self { tx, _task })
    }

    /// Return a queue that can be used to send requests to the metrics task.
    pub fn request_queue(&self) -> MetricsRequestQueue {
        MetricsRequestQueue(self.tx.clone())
    }
}

/// A cheap handle used to send requests to the metrics task.
#[derive(Clone, Debug)]
pub struct MetricsRequestQueue(mpsc::Sender<Message>);

impl MetricsRequestQueue {
    #[cfg(test)]
    #[allow(dead_code)]
    /// Return both halves of the queue used to send messages to the collection
    /// task, for use in testing.
    pub(crate) fn for_test() -> (Self, mpsc::Receiver<Message>) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        (Self(tx), rx)
    }

    /// Ask the task to start tracking the named physical datalink.
    ///
    /// Return `true` if the request was successfully sent, and false otherwise.
    pub async fn track_physical(
        &self,
        zone_name: impl Into<String>,
        name: impl Into<String>,
    ) -> bool {
        self.0
            .send(Message::TrackPhysical {
                zone_name: zone_name.into(),
                name: name.into(),
            })
            .await
            .is_ok()
    }

    /// Ask the task to start tracking the named VNIC.
    ///
    /// Return `true` if the request was successfully sent, and false otherwise.
    pub async fn track_vnic(
        &self,
        zone_name: impl Into<String>,
        name: impl Into<String>,
    ) -> bool {
        self.0
            .send(Message::TrackVnic {
                zone_name: zone_name.into(),
                name: name.into(),
            })
            .await
            .is_ok()
    }

    /// Ask the task to stop tracking the named VNIC.
    ///
    /// Return `true` if the request was successfully sent, and false otherwise.
    pub async fn untrack_vnic(&self, name: impl Into<String>) -> bool {
        self.0.send(Message::UntrackVnic { name: name.into() }).await.is_ok()
    }

    /// Ask the task to start tracking the named OPTE port.
    ///
    /// Return `true` if the request was successfully sent, and false otherwise.
    pub async fn track_opte_port(
        &self,
        zone_name: impl Into<String>,
        name: impl Into<String>,
    ) -> bool {
        self.0
            .send(Message::TrackOptePort {
                zone_name: zone_name.into(),
                name: name.into(),
            })
            .await
            .is_ok()
    }

    /// Ask the task to stop tracking the named OPTE port.
    ///
    /// Return `true` if the request was successfully sent, and false otherwise.
    pub async fn untrack_opte_port(&self, name: impl Into<String>) -> bool {
        self.0
            .send(Message::UntrackOptePort { name: name.into() })
            .await
            .is_ok()
    }

    /// Track all datalinks in a zone.
    ///
    /// This will collect and track:
    ///
    /// - The bootstrap VNIC, if it exists.
    /// - The underlay control VNIC, which always exists.
    /// - Any OPTE ports, which only exist for those with external connectivity.
    ///
    /// Return `true` if the requests were successfully sent, and false
    /// otherwise. This will attempt to send all requests, even if earlier
    /// messages fail.
    pub async fn track_zone_links(&self, running_zone: &RunningZone) -> bool {
        let zone_name = running_zone.name();
        let mut success =
            self.track_vnic(zone_name, running_zone.control_vnic_name()).await;
        if let Some(bootstrap_vnic) = running_zone.bootstrap_vnic_name() {
            success &= self.track_vnic(zone_name, bootstrap_vnic).await;
        }
        for port in running_zone.opte_port_names() {
            success &= self.track_opte_port(zone_name, port).await;
        }
        success
    }

    /// Stop tracking all datalinks in a zone.
    ///
    /// Return `true` if the requests were successfully sent, and false
    /// otherwise. This will attempt to send all requests, even if earlier
    /// messages fail.
    pub async fn untrack_zone_links(&self, running_zone: &RunningZone) -> bool {
        let mut success =
            self.untrack_vnic(running_zone.control_vnic_name()).await;
        if let Some(bootstrap_vnic) = running_zone.bootstrap_vnic_name() {
            success &= self.untrack_vnic(bootstrap_vnic).await;
        }
        for port in running_zone.opte_port_names() {
            success &= self.untrack_opte_port(port).await;
        }
        success
    }

    /// Notify the task that a sled's state has been synchronized with NTP.
    pub async fn notify_time_synced_sled(&self, sled_id: Uuid) -> bool {
        self.0.send(Message::TimeSynced { sled_id }).await.is_ok()
    }
}

/// Start a metric producer server.
fn start_producer_server(
    log: &Logger,
    sled_id: Uuid,
    sled_address: Ipv6Addr,
) -> Result<ProducerServer, Error> {
    let log = log.new(slog::o!("component" => "producer-server"));

    // Listen on any available socket, using our underlay address.
    let address = SocketAddr::new(sled_address.into(), 0);

    // Resolve Nexus via DNS.
    let registration_address = None;
    let config = oximeter_producer::Config {
        server_info: oximeter_producer::EndpointConfig {
            id: sled_id,
            kind: oximeter_producer::ProducerKind::SledAgent,
            address,
            interval: METRIC_COLLECTION_INTERVAL,
        },
        registration_address,
        request_body_max_bytes: METRIC_REQUEST_MAX_SIZE,
        log: LogConfig::Logger(log),
    };
    ProducerServer::start(&config).map_err(Error::ProducerServer)
}
