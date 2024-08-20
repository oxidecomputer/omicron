// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::error::CommunicationError;
use crate::error::SpCommsError;
use crate::management_switch::SpIdentifier;
use crate::management_switch::SpType;
use crate::MgsArguments;
use crate::ServerContext;
use anyhow::Context;
use gateway_messages::measurement::MeasurementError;
use gateway_messages::measurement::MeasurementKind;
use gateway_messages::ComponentDetails;
use gateway_messages::DeviceCapabilities;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::SpComponent;
use gateway_sp_comms::VersionedSpState;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use omicron_common::backoff;
use oximeter::types::Cumulative;
use oximeter::types::ProducerRegistry;
use oximeter::types::Sample;
use oximeter::MetricsError;
use std::borrow::Cow;
use std::collections::hash_map;
use std::collections::hash_map::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

oximeter::use_timeseries!("sensor-measurement.toml");

/// Handle to the metrics tasks.
pub struct Metrics {
    addrs_tx: watch::Sender<Vec<SocketAddrV6>>,
    rack_id_tx: Option<oneshot::Sender<Uuid>>,
    server: JoinHandle<anyhow::Result<()>>,
    pollers: JoinHandle<anyhow::Result<()>>,
}

/// Configuration for metrics.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Collection interval to request from Oximeter, in seconds.
    ///
    /// This is the frequency with which Oximeter will collect samples the
    /// metrics producer endpoint, *not* the frequency with which sensor
    /// measurements are polled from service processors.
    oximeter_collection_interval_secs: usize,

    /// The interval at which service processors are polled for sensor readings,
    /// in milliseconds
    sp_poll_interval_ms: usize,

    /// Configuration settings for testing and development use.
    pub dev: Option<DevConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DevConfig {
    /// Override the Nexus address used to register the SP metrics Oximeter
    /// producer. This is intended for use in development and testing.
    ///
    /// If this argument is not present, Nexus is discovered through DNS.
    pub nexus_address: Option<SocketAddr>,

    /// Allow the metrics producer endpoint to bind on loopback.
    ///
    /// This should be disabled in production, as Nexus will not be able to
    /// reach the loopback interface, but is necessary for local development and
    /// test purposes.
    pub bind_loopback: bool,
}

/// Manages SP pollers, making sure that every SP has a poller task.
struct PollerManager {
    log: slog::Logger,
    apictx: Arc<ServerContext>,
    mgs_id: Uuid,
    /// Poller tasks
    tasks: tokio::task::JoinSet<Result<(), SpCommsError>>,
    /// The manager doesn't actually produce samples, but it needs to be able to
    /// clone a sender for every poller task it spawns.
    sample_tx: broadcast::Sender<Vec<Sample>>,
    poll_interval: Duration,
}

/// Polls sensor readings from an individual SP.
struct SpPoller {
    spid: SpIdentifier,
    known_state: Option<VersionedSpState>,
    components: HashMap<SpComponent, ComponentMetrics>,
    log: slog::Logger,
    rack_id: Uuid,
    mgs_id: Uuid,
    sample_tx: broadcast::Sender<Vec<Sample>>,
}

struct ComponentMetrics {
    target: component::Component,
    /// Counts of errors reported by sensors on this component.
    sensor_errors: HashMap<SensorErrorKey, Cumulative<u64>>,
    /// Counts of errors that occurred whilst polling the SP for measurements
    /// from this component.
    poll_errors: HashMap<&'static str, Cumulative<u64>>,
}

#[derive(Eq, PartialEq, Hash)]
struct SensorErrorKey {
    name: Cow<'static, str>,
    kind: &'static str,
    error: &'static str,
}

/// Manages a metrics server and stuff.
struct ServerManager {
    log: slog::Logger,
    addrs: watch::Receiver<Vec<SocketAddrV6>>,
    registry: ProducerRegistry,
    cfg: MetricsConfig,
}

#[derive(Debug)]
struct Producer {
    /// Receiver for samples produced by SP pollers.
    sample_rx: broadcast::Receiver<Vec<Sample>>,
    /// Logging context.
    ///
    /// We stick this on the producer because we would like to be able to log
    /// when stale samples are dropped.
    log: slog::Logger,
}

/// The maximum Dropshot request size for the metrics server.
const METRIC_REQUEST_MAX_SIZE: usize = 10 * 1024 * 1024;

/// The expected number of SPs in a fully-loaded rack.
///
/// N.B. that there *might* be more than this; we shouldn't ever panic or
/// otherwise misbehave if we see more than this number. This is just intended
/// for sizing buffers/map allocations and so forth; we can always realloc if we
/// see a bonus SP or two. That's why it's called "normal number of SPs" and not
/// "MAX_SPS" or similar.
///
/// Additionally, note that we always determine the channel capacity based on
/// the assumption that *someday*, the rack might be fully loaded with compute
/// sleds, even if it isn't *right now*. A rack with 16 sleds could always grow
/// another 16 later!
const NORMAL_NUMBER_OF_SPS: usize =
    32  // 32 compute sleds
    + 2 // two switches
    + 2 // two power shelves, someday.
    ;

impl Metrics {
    pub fn new(
        log: &slog::Logger,
        args: &MgsArguments,
        cfg: MetricsConfig,
        apictx: Arc<ServerContext>,
    ) -> anyhow::Result<Self> {
        let &MgsArguments { id, rack_id, ref addresses } = args;

        // Create a channel for the SP poller tasks to send samples to the
        // Oximeter producer endpoint.
        //
        // A broadcast channel is used here, not because we are actually
        // multi-consumer (`Producer::produce` is never called concurrently),
        // but because the broadcast channel has properly ring-buffer-like
        // behavior, where earlier messages are discarded, rather than exerting
        // backpressure on senders (as Tokio's MPSC channel does). This
        // is what we want, as we would prefer a full buffer to result in
        // clobbering the oldest measurements, rather than leaving the newest
        // ones on the floor.
        let max_buffered_sample_chunks = cfg.sample_channel_capacity();
        let (sample_tx, sample_rx) =
            broadcast::channel(max_buffered_sample_chunks);

        // Using a channel for this is, admittedly, a bit of an end-run around
        // the `OnceLock` on the `ServerContext` that *also* stores the rack ID,
        // but it has the nice benefit of allowing the `PollerManager` task to _await_
        // the rack ID being set...we might want to change other code to use a
        // similar approach in the future.
        let (rack_id_tx, rack_id_rx) = oneshot::channel();
        let rack_id_tx = if let Some(rack_id) = rack_id {
            rack_id_tx.send(rack_id).expect(
                "we just created the channel; it therefore will not be \
                     closed",
            );
            None
        } else {
            Some(rack_id_tx)
        };
        let pollers = {
            let log = log.new(slog::o!("component" => "sensor-poller"));
            let poll_interval =
                Duration::from_millis(cfg.sp_poll_interval_ms as u64);
            slog::info!(
                &log,
                "SP sensor metrics configured";
                "poll_interval" => ?poll_interval,
                "max_buffered_sample_chunks" => max_buffered_sample_chunks,
            );

            tokio::spawn(
                PollerManager {
                    sample_tx,
                    apictx,
                    poll_interval,
                    tasks: tokio::task::JoinSet::new(),
                    log,
                    mgs_id: id,
                }
                .run(rack_id_rx),
            )
        };

        let (addrs_tx, addrs_rx) =
            tokio::sync::watch::channel(addresses.clone());
        let server = {
            let log = log.new(slog::o!("component" => "producer-server"));
            let registry = ProducerRegistry::with_id(id);
            registry
                .register_producer(Producer { sample_rx, log: log.clone() })
                .context("failed to register metrics producer")?;

            tokio::spawn(
                ServerManager { log, addrs: addrs_rx, registry, cfg }.run(),
            )
        };
        Ok(Self { addrs_tx, rack_id_tx, server, pollers })
    }

    pub fn set_rack_id(&mut self, rack_id: Uuid) {
        if let Some(tx) = self.rack_id_tx.take() {
            tx.send(rack_id).expect("why has the sensor-poller task gone away?")
        }
        // ignoring duplicate attempt to set the rack ID...
    }

    pub async fn update_server_addrs(&self, new_addrs: &[SocketAddrV6]) {
        self.addrs_tx.send_if_modified(|current_addrs| {
            if current_addrs.len() == new_addrs.len()
                // N.B. that we could make this "faster" with a `HashSet`,
                // but...the size of this Vec of addresses is probably going to
                // two or three items, max, so the linear scan actually probably
                // outperforms it...
                && current_addrs.iter().all(|addr| new_addrs.contains(addr))
            {
                return false;
            }

            // Reuse existing `Vec` capacity if possible.This is almost
            // certainly not performance-critical, but it makes me feel happy.
            current_addrs.clear();
            current_addrs.extend_from_slice(new_addrs);
            true
        });
    }
}

impl Drop for Metrics {
    fn drop(&mut self) {
        // Clean up our children on drop.
        self.server.abort();
        self.pollers.abort();
    }
}

impl MetricsConfig {
    fn oximeter_collection_interval(&self) -> Duration {
        Duration::from_secs(self.oximeter_collection_interval_secs as u64)
    }

    /// Returns the number of sample chunks from individual SPs to buffer.
    fn sample_channel_capacity(&self) -> usize {
        // Roughly how many times will we poll SPs for each metrics collection
        // interval?
        let polls_per_metrics_interval = {
            let collection_interval_ms: usize = self
                .oximeter_collection_interval()
                .as_millis()
                .try_into()
                .expect("your oximeter collection interval is way too big...");
            collection_interval_ms / self.sp_poll_interval_ms
        };

        // How many sample collection intervals do we want to allow to elapse before
        // we start putting stuff on the floor?
        //
        // Let's say 16. Chosen totally arbitrarily but seems reasonable-ish.
        let sloppiness = 16;
        let capacity =
            NORMAL_NUMBER_OF_SPS * polls_per_metrics_interval * sloppiness;
        // Finally, the buffer capacity will probably be allocated in a power of two
        // anyway, so let's make sure our thing is a power of two so we don't waste
        // the allocation we're gonna get anyway.
        capacity.next_power_of_two()
    }
}

impl oximeter::Producer for Producer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
        // Drain all samples currently in the queue into a `Vec`.
        //
        // N.B. it may be tempting to pursue an alternative design where we
        // implement `Iterator` for a `broadcast::Receiver<Vec<Sample>>` and
        // just return that using `Receiver::resubscribe`...DON'T DO THAT! The
        // `resubscribe` function creates a receiver at the current *tail* of
        // the ringbuffer, so it won't see any samples produced *before* now.
        // Which  is the opposite of what we want!
        let mut samples = Vec::with_capacity(self.sample_rx.len());
        // Because we recieve the individual samples in a `Vec` of all samples
        // produced by a poller, let's also sum the length of each of those
        // `Vec`s here, so we can log it later.
        let mut total_samples = 0;
        // Also, track whether any sample chunks were dropped off the end of the
        // ring buffer.
        let mut dropped_chunks = 0;

        use broadcast::error::TryRecvError;
        loop {
            match self.sample_rx.try_recv() {
                Ok(sample_chunk) => {
                    total_samples += sample_chunk.len();
                    samples.push(sample_chunk)
                }
                // This error indicates that an old ringbuffer entry was
                // overwritten. That's fine, just get the next one.
                Err(TryRecvError::Lagged(dropped)) => {
                    dropped_chunks += dropped;
                }
                // We've drained all currently available samples! We're done here!
                Err(TryRecvError::Empty) => break,
                // This should only happen when shutting down.
                Err(TryRecvError::Closed) => {
                    slog::debug!(&self.log, "sample producer channel closed");
                    break;
                }
            }
        }

        if dropped_chunks > 0 {
            slog::info!(
                &self.log,
                "produced metric samples. some old sample chunks were dropped!";
                "samples" => total_samples,
                "sample_chunks" => samples.len(),
                "dropped_chunks" => dropped_chunks,
            );
        } else {
            slog::debug!(
                &self.log,
                "produced metric samples";
                "samples" => total_samples,
                "sample_chunks" => samples.len(),
            );
        }

        // There you go, that's all I've got.
        Ok(Box::new(samples.into_iter().flatten()))
    }
}

impl PollerManager {
    async fn run(
        mut self,
        rack_id: oneshot::Receiver<Uuid>,
    ) -> anyhow::Result<()> {
        let switch = &self.apictx.mgmt_switch;

        // First, wait until we know what the rack ID is...
        let rack_id = rack_id.await.context(
            "rack ID sender has gone away...we must be shutting down",
        )?;

        let mut known_sps: HashMap<SpIdentifier, tokio::task::AbortHandle> =
            HashMap::with_capacity(NORMAL_NUMBER_OF_SPS);
        // Wait for SP discovery to complete, if it hasn't already.
        // TODO(eliza): presently, we busy-poll here. It would be nicer to
        // replace the `OnceLock<Result<LocationMap, ...>` in `ManagementSwitch`
        // with a `tokio::sync::watch`
        backoff::retry_notify_ext(
            backoff::retry_policy_local(),
            || async {
                if switch.is_discovery_complete() {
                    Ok(())
                } else {
                    Err(backoff::BackoffError::transient(()))
                }
            },
            |_, _, elapsed| {
                let secs = elapsed.as_secs();
                if secs < 30 {
                    slog::debug!(
                        &self.log,
                        "waiting for SP discovery to complete...";
                        "elapsed" => ?elapsed,
                    );
                } else if secs < 180 {
                    slog::info!(
                        &self.log,
                        "still waiting for SP discovery to complete...";
                        "elapsed" => ?elapsed,
                    )
                } else {
                    slog::warn!(
                        &self.log,
                        "we have been waiting for SP discovery to complete \
                         for a pretty long time!";
                        "elapsed" => ?elapsed,
                    )
                }
            },
        )
        .await
        .expect("we should never return a fatal error here");

        slog::info!(
            &self.log,
            "starting to polling SP sensor data every {:?}", self.poll_interval;
        );

        loop {
            let sps = backoff::retry_notify_ext(
                backoff::retry_policy_internal_service(),
                || async {
                    switch.all_sps().map_err(backoff::BackoffError::transient)
                },
                |error, attempts, elapsed| {
                    slog::warn!(
                        &self.log,
                        "failed to list SPs! we'll try again in a little bit.";
                        "error" => error,
                        "elapsed" => ?elapsed,
                        "attempts" => attempts,
                    )
                },
            )
            .await
            .expect("we never return a permanent error here");

            for (spid, _) in sps {
                // Do we know about this li'l guy already?
                match known_sps.get(&spid) {
                    // Okay, and has it got someone checking up on it? Right?
                    Some(poller) if poller.is_finished() => {
                        // Welp.
                        slog::info!(
                            &self.log,
                            "uh-oh! a known SP's poller task has gone AWOL. restarting it...";
                            "sp_slot" => ?spid.slot,
                            "chassis_type" => ?spid.typ,
                        );
                    }
                    Some(_) => continue,
                    None => {
                        slog::info!(
                            &self.log,
                            "found a new little friend!";
                            "sp_slot" => ?spid.slot,
                            "chassis_type" => ?spid.typ,
                        );
                    }
                }

                let poller = SpPoller {
                    spid,
                    rack_id,
                    mgs_id: self.mgs_id,
                    log: self.log.new(slog::o!(
                        "sp_slot" => spid.slot,
                        "chassis_type" => format!("{:?}", spid.typ),
                    )),
                    components: HashMap::new(),
                    known_state: None,
                    sample_tx: self.sample_tx.clone(),
                };
                let poller_handle = self
                    .tasks
                    .spawn(poller.run(self.poll_interval, self.apictx.clone()));
                let _prev_poller = known_sps.insert(spid, poller_handle);
                debug_assert!(
                    _prev_poller.map(|p| p.is_finished()).unwrap_or(true),
                    "if we clobbered an existing poller task, it better have \
                     been because it was dead..."
                );
            }

            // All pollers started! Now wait to see if any of them have died...
            let mut joined = self.tasks.join_next().await;
            while let Some(result) = joined {
                if let Err(e) = result {
                    if cfg!(debug_assertions) {
                        unreachable!(
                            "we compile with `panic=\"abort\"`, so a spawned task \
                             panicking should abort the whole process..."
                        );
                    } else {
                        slog::error!(
                            &self.log,
                            "a spawned SP poller task panicked! this should \
                             never happen: we compile with `panic=\"abort\"`, so \
                             a spawned task panicking should abort the whole \
                             process...";
                            "error" => %e,
                        );
                    }
                }
                // drain any remaining errors
                joined = self.tasks.try_join_next();
            }
        }
    }
}

impl Drop for PollerManager {
    fn drop(&mut self) {
        // This is why the `JoinSet` is a field on the `PollerManager` struct
        // rather than a local variable in `async fn run()`!
        self.tasks.abort_all();
    }
}

impl SpPoller {
    async fn run(
        mut self,
        poll_interval: Duration,
        apictx: Arc<ServerContext>,
    ) -> Result<(), SpCommsError> {
        let mut interval = tokio::time::interval(poll_interval);
        let switch = &apictx.mgmt_switch;
        let sp = switch.sp(self.spid)?;
        loop {
            interval.tick().await;
            slog::trace!(&self.log, "interval elapsed, polling SP...");

            match self.poll(sp).await {
                // No sense cluttering the ringbuffer with empty vecs...
                Ok(samples) if samples.is_empty() => {
                    slog::trace!(&self.log, "polled SP, no samples returned"; "num_samples" => 0usize);
                }
                Ok(samples) => {
                    slog::trace!(&self.log, "polled SP successfully"; "num_samples" => samples.len());

                    if let Err(_) = self.sample_tx.send(samples) {
                        slog::info!(
                            &self.log,
                            "all sample receiver handles have been dropped! \
                             presumably we are shutting down...";
                        );
                        return Ok(());
                    }
                }
                // No SP is currently present for this ID. This may change in
                // the future: a cubby that is not populated at present may have
                // a sled added to it in the future. So, let's wait until it
                // changes.
                Err(CommunicationError::NoSpDiscovered) => {
                    let mut watch = sp.sp_addr_watch().clone();
                    loop {
                        if let Some((addr, port)) = *watch.borrow_and_update() {
                            // Ladies and gentlemen...we got him!
                            slog::info!(
                                &self.log,
                                "found a SP, resuming polling.";
                                "sp_addr" => ?addr,
                                "sp_port" => ?port,
                            );
                            break;
                        }

                        slog::info!(
                            &self.log,
                            "no SP is present for this slot. waiting for a \
                             little buddy to appear...";
                        );

                        // Wait for an address to be discovered.
                        if watch.changed().await.is_err() {
                            slog::debug!(
                                &self.log,
                                "SP address watch has been closed, presumably \
                                 we are shutting down";
                            );
                            return Ok(());
                        }
                    }
                }
                Err(error) => {
                    slog::warn!(
                        &self.log,
                        "failed to poll SP, will try again momentarily...";
                        "error" => %error,
                    );
                    // TODO(eliza): we should probably have a metric for failed
                    // SP polls.
                }
            }
        }
    }

    async fn poll(
        &mut self,
        sp: &SingleSp,
    ) -> Result<Vec<Sample>, CommunicationError> {
        // Check if the SP's state has changed. If it has, we need to make sure
        // we still know what all of its sensors are.
        let current_state = sp.state().await?;
        if Some(&current_state) != self.known_state.as_ref() {
            // The SP's state appears to have changed. Time to make sure our
            // understanding of its devices and identity is up to date!
            slog::debug!(
                &self.log,
                "our little friend seems to have changed in some kind of way";
                "current_state" => ?current_state,
                "known_state" => ?self.known_state,
            );
            let inv_devices = sp.inventory().await?.devices;

            // Clear out any previously-known devices, and preallocate capacity
            // for all the new ones.
            self.components.clear();
            self.components.reserve(inv_devices.len());

            // Reimplement this ourselves because we don't really care about
            // reading the RoT state at present. This is unfortunately copied
            // from `gateway_messages`.
            fn stringify_byte_string(bytes: &[u8]) -> String {
                // We expect serial and model numbers to be ASCII and 0-padded: find the first 0
                // byte and convert to a string. If that fails, hexlify the entire slice.
                let first_zero =
                    bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());

                std::str::from_utf8(&bytes[..first_zero])
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_err| hex::encode(bytes))
            }
            let (model, serial, hubris_archive_id, revision) =
                match current_state {
                    VersionedSpState::V1(ref v) => (
                        stringify_byte_string(&v.model),
                        stringify_byte_string(&v.serial_number[..]),
                        hex::encode(v.hubris_archive_id),
                        v.revision,
                    ),
                    VersionedSpState::V2(ref v) => (
                        stringify_byte_string(&v.model),
                        stringify_byte_string(&v.serial_number[..]),
                        hex::encode(v.hubris_archive_id),
                        v.revision,
                    ),
                    VersionedSpState::V3(ref v) => (
                        stringify_byte_string(&v.model),
                        stringify_byte_string(&v.serial_number[..]),
                        hex::encode(v.hubris_archive_id),
                        v.revision,
                    ),
                };
            for dev in inv_devices {
                // Skip devices which have nothing interesting for us.
                if !dev
                    .capabilities
                    .contains(DeviceCapabilities::HAS_MEASUREMENT_CHANNELS)
                {
                    continue;
                }
                let component = match dev.component.as_str() {
                    Some(c) => Cow::Owned(c.to_string()),
                    None => {
                        // These are supposed to always be strings. But, if we
                        // see one that's not a string, fall back to the hex
                        // representation rather than  panicking.
                        let hex = hex::encode(dev.component.id);
                        slog::warn!(
                            &self.log,
                            "a SP component ID was not a string! this isn't \
                             supposed to happen!";
                            "component" => %hex,
                            "device" => ?dev,
                        );
                        Cow::Owned(hex)
                    }
                };
                // TODO(eliza): i hate having to clone all these strings for
                // every device on the SP...it would be cool if Oximeter let us
                // reference count them...
                let target = component::Component {
                    chassis_type: Cow::Borrowed(match self.spid.typ {
                        SpType::Sled => "sled",
                        SpType::Switch => "switch",
                        SpType::Power => "power",
                    }),
                    slot: self.spid.slot as u32,
                    component,
                    device: Cow::Owned(dev.device),
                    model: Cow::Owned(model.clone()),
                    revision,
                    serial: Cow::Owned(serial.clone()),
                    rack_id: self.rack_id,
                    gateway_id: self.mgs_id,
                    hubris_archive_id: Cow::Owned(hubris_archive_id.clone()),
                };
                match self.components.entry(dev.component) {
                    // Found a new device!
                    hash_map::Entry::Vacant(entry) => {
                        slog::debug!(
                            &self.log,
                            "discovered a new component!";
                            "component" => ?dev.component,
                            "device" => ?target.device,
                        );
                        entry.insert(ComponentMetrics {
                            target,
                            sensor_errors: HashMap::new(),
                            poll_errors: HashMap::new(),
                        });
                    }
                    // We previously had a known device for this thing, but
                    // the metrics target has changed, so we should reset
                    // its cumulative metrics.
                    hash_map::Entry::Occupied(mut entry)
                        if entry.get().target != target =>
                    {
                        slog::trace!(
                            &self.log,
                            "target has changed, resetting cumulative metrics \
                             for component";
                            "component" => ?dev.component,
                        );
                        entry.insert(ComponentMetrics {
                            target,
                            sensor_errors: HashMap::new(),
                            poll_errors: HashMap::new(),
                        });
                    }

                    // The target for this device hasn't changed, don't reset it.
                    hash_map::Entry::Occupied(_) => {}
                }
            }

            self.known_state = Some(current_state);
        }

        let mut samples = Vec::with_capacity(self.components.len());
        for (c, metrics) in &mut self.components {
            // Metrics samples *should* always be well-formed. If we ever emit a
            // messed up one, this is a programmer error, and therefore  should
            // fail in test, but should probably *not* take down the whole
            // management gateway in a real-life rack, especially because it's
            // probably going to happen again if we were to get restarted.
            const BAD_SAMPLE: &str =
                "we emitted a bad metrics sample! this should never happen";
            macro_rules! try_sample {
                ($sample:expr) => {
                    match $sample {
                        Ok(sample) => samples.push(sample),

                        Err(err) => {
                            slog::error!(
                                &self.log,
                                "{BAD_SAMPLE}!";
                                "error" => %err,
                            );
                            #[cfg(debug_assertions)]
                            unreachable!("{BAD_SAMPLE}: {err}");
                        }
                    }
                }
            }
            let details = match sp.component_details(*c).await {
                Ok(deets) => deets,
                // SP seems gone!
                Err(CommunicationError::NoSpDiscovered) => {
                    return Err(CommunicationError::NoSpDiscovered)
                }
                Err(error) => {
                    slog::warn!(
                        &self.log,
                        "failed to read details on SP component";
                        "sp_component" => %c,
                        "error" => %error,
                    );
                    try_sample!(metrics.poll_error(comms_error_str(error)));
                    continue;
                }
            };
            if details.entries.is_empty() {
                slog::warn!(
                    &self.log,
                    "a component which claimed to have measurement channels \
                     had empty details. this seems weird...";
                    "sp_component" => %c,
                );
                try_sample!(metrics.poll_error("no_measurement_channels"));
                continue;
            }
            let ComponentMetrics { sensor_errors, target, .. } = metrics;
            for d in details.entries {
                let ComponentDetails::Measurement(m) = d else {
                    // If the component details are switch port details rather
                    // than measurement channels, ignore it for now.
                    continue;
                };
                let name = Cow::Owned(m.name);
                let sample = match (m.value, m.kind) {
                    (Ok(datum), MeasurementKind::Temperature) => Sample::new(
                        target,
                        &component::Temperature { name, datum },
                    ),
                    (Ok(datum), MeasurementKind::Current) => {
                        Sample::new(target, &component::Current { name, datum })
                    }
                    (Ok(datum), MeasurementKind::Voltage) => {
                        Sample::new(target, &component::Voltage { name, datum })
                    }
                    (Ok(datum), MeasurementKind::Power) => {
                        Sample::new(target, &component::Power { name, datum })
                    }
                    (Ok(datum), MeasurementKind::InputCurrent) => Sample::new(
                        target,
                        &component::InputCurrent { name, datum },
                    ),
                    (Ok(datum), MeasurementKind::InputVoltage) => Sample::new(
                        target,
                        &component::InputVoltage { name, datum },
                    ),
                    (Ok(datum), MeasurementKind::Speed) => Sample::new(
                        target,
                        &component::FanSpeed { name, datum },
                    ),
                    (Err(e), kind) => {
                        let kind = match kind {
                            MeasurementKind::Temperature => "temperature",
                            MeasurementKind::Current => "current",
                            MeasurementKind::Voltage => "voltage",
                            MeasurementKind::Power => "power",
                            MeasurementKind::InputCurrent => "input_current",
                            MeasurementKind::InputVoltage => "input_voltage",
                            MeasurementKind::Speed => "fan_speed",
                        };
                        let error = match e {
                            MeasurementError::InvalidSensor => "invalid_sensor",
                            MeasurementError::NoReading => "no_reading",
                            MeasurementError::NotPresent => "not_present",
                            MeasurementError::DeviceError => "device_error",
                            MeasurementError::DeviceUnavailable => {
                                "device_unavailable"
                            }
                            MeasurementError::DeviceTimeout => "device_timeout",
                            MeasurementError::DeviceOff => "device_off",
                        };
                        let datum = sensor_errors
                            .entry(SensorErrorKey {
                                name: name.clone(),
                                kind,
                                error,
                            })
                            .or_insert(Cumulative::new(0));
                        // TODO(eliza): perhaps we should treat this as
                        // "level-triggered" and only increment the counter
                        // when the sensor has *changed* to an errored
                        // state after we have seen at least one good
                        // measurement from it since the last time the error
                        // was observed?
                        datum.increment();
                        Sample::new(
                            target,
                            &component::SensorErrorCount {
                                error: Cow::Borrowed(error),
                                name,
                                datum: *datum,
                                sensor_kind: Cow::Borrowed(kind),
                            },
                        )
                    }
                };
                try_sample!(sample);
            }
        }
        Ok(samples)
    }
}

impl ServerManager {
    async fn run(mut self) -> anyhow::Result<()> {
        let (registration_address, bind_loopback) =
            if let Some(ref dev) = self.cfg.dev {
                slog::warn!(
                    &self.log,
                    "using development metrics configuration overrides!";
                    "nexus_address" => ?dev.nexus_address,
                    "bind_loopback" => dev.bind_loopback,
                );
                (dev.nexus_address, dev.bind_loopback)
            } else {
                (None, false)
            };
        let interval = self.cfg.oximeter_collection_interval();
        let id = self.registry.producer_id();

        let mut current_server: Option<oximeter_producer::Server> = None;
        loop {
            let current_ip = current_server.as_ref().map(|s| s.address().ip());
            let mut new_ip = None;
            for addr in self.addrs.borrow_and_update().iter() {
                let &ip = addr.ip();
                // Don't bind the metrics endpoint on ::1
                if ip.is_loopback() && !bind_loopback {
                    continue;
                }
                // If our current address is contained in the new addresses,
                // no need to rebind.
                if current_ip == Some(IpAddr::V6(ip)) {
                    new_ip = None;
                    break;
                } else {
                    new_ip = Some(ip);
                }
            }

            if let Some(ip) = new_ip {
                slog::debug!(
                    &self.log,
                    "rebinding producer server on new IP";
                    "new_ip" => ?ip,
                    "current_ip" => ?current_ip,
                    "collection_interval" => ?interval,
                    "producer_id" => ?id,
                );
                let server = {
                    // Listen on any available socket, using the provided underlay IP.
                    let address = SocketAddr::new(ip.into(), 0);

                    let server_info = ProducerEndpoint {
                        id,
                        kind: ProducerKind::ManagementGateway,
                        address,
                        interval,
                    };
                    let config = oximeter_producer::Config {
                        server_info,
                        registration_address,
                        request_body_max_bytes: METRIC_REQUEST_MAX_SIZE,
                        log: oximeter_producer::LogConfig::Logger(
                            self.log.clone(),
                        ),
                    };
                    oximeter_producer::Server::with_registry(
                        self.registry.clone(),
                        &config,
                    )
                    .context("failed to start producer server")?
                };

                slog::info!(
                    &self.log,
                    "bound metrics producer server";
                    "collection_interval" => ?interval,
                    "producer_id" => ?id,
                    "address" => %server.address(),
                );

                if let Some(old_server) = current_server.replace(server) {
                    let old_addr = old_server.address();
                    if let Err(error) = old_server.close().await {
                        slog::error!(
                            &self.log,
                            "failed to close old metrics producer server";
                            "address" => %old_addr,
                            "error" => %error,
                        );
                    } else {
                        slog::debug!(
                            &self.log,
                            "old metrics producer server shut down";
                            "address" => %old_addr,
                        )
                    }
                }
            }

            // Wait for a subsequent address change.
            self.addrs.changed().await?;
        }
    }
}

impl ComponentMetrics {
    fn poll_error(
        &mut self,
        error_str: &'static str,
    ) -> Result<Sample, MetricsError> {
        let datum = self
            .poll_errors
            .entry(error_str)
            .or_insert_with(|| Cumulative::new(0));
        datum.increment();
        Sample::new(
            &self.target,
            &component::PollErrorCount {
                error: Cow::Borrowed(error_str),
                datum: *datum,
            },
        )
    }
}

fn comms_error_str(error: CommunicationError) -> &'static str {
    // TODO(eliza): a bunch of these probably can't be returned by the specific
    // operations we try to do. It could be good to make the methods this code
    // calls return a smaller enum of just the errors it might actually
    // encounter? Figure this out later.
    match error {
        CommunicationError::NoSpDiscovered => "no_sp_discovered",
        CommunicationError::InterfaceError(_) => "interface",
        CommunicationError::ScopeIdChangingFrequently { .. } => {
            "scope_id_changing_frequently"
        }
        CommunicationError::JoinMulticast { .. } => "join_multicast",
        CommunicationError::UdpSendTo { .. } => "udp_send_to",
        CommunicationError::UdpRecv(_) => "udp_recv",
        CommunicationError::Deserialize { .. } => "deserialize",
        CommunicationError::ExhaustedNumAttempts(_) => "exhausted_num_attempts",
        CommunicationError::BadResponseType { .. } => "bad_response_type",
        CommunicationError::SpError { .. } => "sp_error",
        CommunicationError::BogusSerialConsoleState { .. } => {
            "bogus_serial_console_state"
        }
        CommunicationError::VersionMismatch { .. } => {
            "protocol_version_mismatch"
        }
        CommunicationError::TlvDeserialize { .. } => "tlv_deserialize",
        CommunicationError::TlvDecode(_) => "tlv_decode",
        CommunicationError::TlvPagination { .. } => "tlv_pagination",
        CommunicationError::IpccKeyLookupValueTooLarge => {
            "ipcc_key_lookup_value_too_large"
        }
        CommunicationError::UnexpectedTrailingData(_) => {
            "unexpected_trailing_data"
        }
        CommunicationError::BadTrailingDataSize { .. } => {
            "bad_trailing_data_size"
        }
    }
}
