// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::MgsArguments;
use crate::ServerContext;
use crate::error::CommunicationError;
use crate::management_switch::SpIdentifier;
use crate::management_switch::SpType;
use anyhow::Context;
use gateway_messages::ComponentDetails;
use gateway_messages::DeviceCapabilities;
use gateway_messages::measurement::MeasurementError;
use gateway_messages::measurement::MeasurementKind;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::SpComponent;
use gateway_sp_comms::VersionedSpState;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use omicron_common::backoff;
use oximeter::MetricsError;
use oximeter::types::Cumulative;
use oximeter::types::ProducerRegistry;
use oximeter::types::Sample;
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

oximeter::use_timeseries!("hardware-component.toml");
use hardware_component as metric;

/// Handle to the metrics tasks.
pub struct Metrics {
    /// If the metrics subsystem is disabled, this is `None`.
    inner: Option<Handles>,
}

struct Handles {
    addrs_tx: watch::Sender<Vec<SocketAddrV6>>,
    rack_id_tx: Option<oneshot::Sender<Uuid>>,
    server: JoinHandle<anyhow::Result<()>>,
}

/// Configuration for metrics.
///
/// In order to reduce the risk of a bad config file taking down the whole
/// management network, we try to keep the metrics-specific portion of the
/// config file as minimal as possible. At present, it only includes development
/// configurations that shouldn't be present in production configs.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Completely disable the metrics subsystem.
    ///
    /// If `disabled = true`, sensor data metrics will not be collected, and the
    /// metrics polling tasks will not be started.
    #[serde(default)]
    pub disabled: bool,

    /// Override the Nexus address used to register the SP metrics Oximeter
    /// producer. This is intended for use in development and testing.
    ///
    /// If this argument is not present, Nexus is discovered through DNS.
    #[serde(default)]
    pub dev_nexus_address: Option<SocketAddr>,

    /// Allow the metrics producer endpoint to bind on loopback.
    ///
    /// This should be disabled in production, as Nexus will not be able to
    /// reach the loopback interface, but is necessary for local development and
    /// test purposes.
    #[serde(default)]
    pub dev_bind_loopback: bool,
}

/// Polls sensor readings from an individual SP.
struct SpPoller {
    spid: SpIdentifier,
    known_state: Option<SpUnderstanding>,
    components: HashMap<SpComponent, ComponentMetrics>,
    log: slog::Logger,
    rack_id: Uuid,
    mgs_id: Uuid,
    sample_tx: broadcast::Sender<Vec<Sample>>,
}

struct ComponentMetrics {
    target: metric::HardwareComponent,
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

/// Poll interval for requesting sensor readings from SPs.
///
/// Bryan wants to try polling at 1Hz, so let's do that for now.
const SP_POLL_INTERVAL: Duration = Duration::from_secs(1);

///The interval at which we will ask Oximeter to collect our metric samples.
///
/// Every ten seconds seems good.
const OXIMETER_COLLECTION_INTERVAL: Duration = Duration::from_secs(10);

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

/// What size should we make the
const MAX_BUFFERED_SAMPLE_CHUNKS: usize = {
    // Roughly how many times will we poll SPs for each metrics collection
    // interval?
    let polls_per_metrics_interval = {
        let collection_interval_secs: usize =
            OXIMETER_COLLECTION_INTERVAL.as_secs() as usize;
        let poll_interval_secs: usize = SP_POLL_INTERVAL.as_secs() as usize;

        collection_interval_secs / poll_interval_secs
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
};

impl Metrics {
    pub fn new(
        log: &slog::Logger,
        args: &MgsArguments,
        cfg: Option<MetricsConfig>,
        apictx: Arc<ServerContext>,
    ) -> Self {
        let &MgsArguments { id, rack_id, ref addresses } = args;

        if cfg.as_ref().map(|c| c.disabled).unwrap_or(false) {
            slog::warn!(&log, "metrics subsystem disabled by config");
            return Self { inner: None };
        }

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
        let (sample_tx, sample_rx) =
            broadcast::channel(MAX_BUFFERED_SAMPLE_CHUNKS);

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

        tokio::spawn(start_pollers(
            log.new(slog::o!("component" => "sensor-poller")),
            apictx.clone(),
            rack_id_rx,
            id,
            sample_tx,
        ));

        let (addrs_tx, addrs_rx) =
            tokio::sync::watch::channel(addresses.clone());
        let server = {
            let log = log.new(slog::o!("component" => "producer-server"));
            let registry = ProducerRegistry::with_id(id);
            // Register the producer for SP sensor metrics.
            registry
                .register_producer(Producer { sample_rx, log: log.clone() })
                // TODO(ben): when you change `register_producer` to not return
                // a `Result`, delete this `expect`. thanks in advance! :)
                .expect(
                    "`ProducerRegistry::register_producer()` will never \
                     actually return an `Err`, so this shouldn't ever \
                     happen...",
                );
            // Also, register the producer for the HTTP API metrics.
            registry
                .register_producer(apictx.latencies.clone())
                // TODO(ben): do this one too pls
                .expect(
                    "`ProducerRegistry::register_producer()` will never \
                    actually return an `Err`, so this shouldn't ever \
                    happen...",
                );

            tokio::spawn(
                ServerManager { log, addrs: addrs_rx, registry }.run(cfg),
            )
        };
        Self { inner: Some(Handles { addrs_tx, rack_id_tx, server }) }
    }

    pub fn set_rack_id(&mut self, rack_id: Uuid) {
        let tx = self.inner.as_mut().and_then(|i| i.rack_id_tx.take());
        if let Some(tx) = tx {
            // If the task that starts sensor pollers has gone away already,
            // we're probably shutting down, and shouldn't panic.
            let _ = tx.send(rack_id);
        }
        // Ignoring duplicate attempt to set the rack ID...
    }

    pub async fn update_server_addrs(&self, new_addrs: &[SocketAddrV6]) {
        if let Some(ref inner) = self.inner {
            inner.addrs_tx.send_if_modified(|current_addrs| {
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
}

impl Drop for Metrics {
    fn drop(&mut self) {
        // Clean up our children on drop.
        if let Some(ref mut inner) = self.inner {
            inner.server.abort();
        }
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
        // Because we receive the individual samples in a `Vec` of all samples
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

async fn start_pollers(
    log: slog::Logger,
    apictx: Arc<ServerContext>,
    rack_id: oneshot::Receiver<Uuid>,
    mgs_id: Uuid,
    sample_tx: broadcast::Sender<Vec<Sample>>,
) -> anyhow::Result<()> {
    let switch = &apictx.mgmt_switch;

    // First, wait until we know what the rack ID is known...
    let rack_id = rack_id
        .await
        .context("rack ID sender has gone away...we must be shutting down")?;

    // Wait for SP discovery to complete, if it hasn't already.
    // TODO(eliza): presently, we busy-poll here. It would be nicer to
    // replace the `OnceLock<Result<LocationMap, ...>` in `ManagementSwitch`
    // with a `tokio::sync::watch`
    let sps = backoff::retry_notify_ext(
        backoff::retry_policy_local(),
        || async { switch.all_sps().map_err(backoff::BackoffError::transient) },
        |err, _, elapsed| {
            let secs = elapsed.as_secs();
            if secs < 30 {
                slog::debug!(
                    &log,
                    "waiting for SP discovery to complete...";
                    "elapsed" => ?elapsed,
                    "error" => err,
                );
            } else if secs < 180 {
                slog::info!(
                    &log,
                    "still waiting for SP discovery to complete...";
                    "elapsed" => ?elapsed,
                    "error" => err,
                )
            } else {
                slog::warn!(
                    &log,
                    "we have been waiting for SP discovery to complete \
                     for a pretty long time!";
                    "elapsed" => ?elapsed,
                    "error" => err,
                )
            }
        },
    )
    .await
    .context("we should never return a fatal error here")?;

    slog::info!(
        &log,
        "starting to poll SP sensor data every {SP_POLL_INTERVAL:?}"
    );

    for (spid, _) in sps {
        slog::info!(
            &log,
            "found a new little friend!";
            "sp_slot" => ?spid.slot,
            "chassis_type" => ?spid.typ,
        );

        let poller = SpPoller {
            spid,
            rack_id,
            mgs_id,
            log: log.new(slog::o!(
                "sp_slot" => spid.slot,
                "chassis_type" => format!("{:?}", spid.typ),
            )),
            components: HashMap::new(),
            known_state: None,
            sample_tx: sample_tx.clone(),
        };
        tokio::spawn(poller.run(apictx.clone()));
    }

    Ok(())
}

impl SpPoller {
    async fn run(mut self, apictx: Arc<ServerContext>) {
        let mut interval = tokio::time::interval(SP_POLL_INTERVAL);
        let switch = &apictx.mgmt_switch;
        let sp = match switch.sp(self.spid) {
            Ok(sp) => sp,
            Err(e) => {
                // This should never happen, but it's not worth taking down the
                // entire management network over that...
                const MSG: &'static str = "the `SpPoller::run` function is only called after \
                     discovery completes successfully, and the `SpIdentifier` \
                     used was returned by the management switch, \
                     so it should be valid.";
                if cfg!(debug_assertions) {
                    unreachable!(
                        "{MSG} nonetheless, we saw a {e:?} error when looking \
                         up {:?}",
                        self.spid
                    );
                } else {
                    slog::error!(
                        &self.log,
                        "THIS SHOULDN'T HAPPEN: {MSG}";
                        "error" => e,
                        "sp" => ?self.spid,
                    );
                    return;
                }
            }
        };
        loop {
            interval.tick().await;
            slog::trace!(&self.log, "interval elapsed, polling SP...");

            match self.poll(sp).await {
                // No sense cluttering the ringbuffer with empty vecs...
                Ok(samples) if samples.is_empty() => {
                    slog::trace!(
                        &self.log,
                        "polled SP, no samples returned";
                        "num_samples" => 0usize
                    );
                }
                Ok(samples) => {
                    slog::trace!(
                        &self.log,
                        "polled SP successfully";
                        "num_samples" => samples.len(),
                    );

                    if let Err(_) = self.sample_tx.send(samples) {
                        slog::debug!(
                            &self.log,
                            "all sample receiver handles have been dropped! \
                             presumably we are shutting down...";
                        );
                        return;
                    }
                }
                // No SP is currently present for this ID. This may change in
                // the future: a cubby that is not populated at present may have
                // a sled added to it in the future. So, let's wait until it
                // changes.
                Err(CommunicationError::NoSpDiscovered) => {
                    slog::info!(
                        &self.log,
                        "no SP is present for this slot. waiting for a \
                         little buddy to appear...";
                    );
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

                        // Wait for an address to be discovered.
                        slog::debug!(&self.log, "waiting for a SP to appear.");
                        if watch.changed().await.is_err() {
                            slog::debug!(
                                &self.log,
                                "SP address watch has been closed, presumably \
                                 we are shutting down";
                            );
                            return;
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
        let mut current_state = SpUnderstanding::from(sp.state().await?);
        let mut samples = Vec::new();
        // If the SP's state changes dramatically *during* a poll, it may be
        // necessary to re-do the metrics scrape, thus the loop. Normally, we
        // will only loop a single time, but may retry if necessary.
        loop {
            // Check if the SP's state has changed. If it has, we need to make sure
            // we still know what all of its sensors are.
            if Some(&current_state) != self.known_state.as_ref() {
                // The SP's state appears to have changed. Time to make sure our
                // understanding of its devices and identity is up to date!

                let chassis_kind = match self.spid.typ {
                    SpType::Sled => "sled",
                    SpType::Switch => "switch",
                    SpType::Power => "power",
                };
                let model = stringify_byte_string(&current_state.model[..]);
                let serial =
                    stringify_byte_string(&current_state.serial_number[..]);
                let hubris_archive_id =
                    hex::encode(&current_state.hubris_archive_id);

                slog::debug!(
                    &self.log,
                    "our little friend seems to have changed in some kind of way";
                    "current_state" => ?current_state,
                    "known_state" => ?self.known_state,
                    "new_model" => %model,
                    "new_serial" => %serial,
                    "new_hubris_archive_id" => %hubris_archive_id,
                );

                let inv_devices = sp.inventory().await?.devices;

                // Clear out any previously-known devices, and preallocate capacity
                // for all the new ones.
                self.components.clear();
                self.components.reserve(inv_devices.len());

                for dev in inv_devices {
                    // Skip devices which have nothing interesting for us.
                    if !dev
                        .capabilities
                        .contains(DeviceCapabilities::HAS_MEASUREMENT_CHANNELS)
                    {
                        continue;
                    }
                    let component_id = match dev.component.as_str() {
                        Some(c) => Cow::Owned(c.to_string()),
                        None => {
                            // These are supposed to always be strings. But, if we
                            // see one that's not a string, fall back to the hex
                            // representation rather than panicking.
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
                    let target = metric::HardwareComponent {
                        rack_id: self.rack_id,
                        gateway_id: self.mgs_id,
                        chassis_model: Cow::Owned(model.clone()),
                        chassis_revision: current_state.revision,
                        chassis_kind: Cow::Borrowed(chassis_kind),
                        chassis_serial: Cow::Owned(serial.clone()),
                        hubris_archive_id: Cow::Owned(
                            hubris_archive_id.clone(),
                        ),
                        slot: u32::from(self.spid.slot),
                        component_kind: Cow::Owned(dev.device),
                        component_id,
                        description: Cow::Owned(dev.description),
                    };
                    match self.components.entry(dev.component) {
                        // Found a new device!
                        hash_map::Entry::Vacant(entry) => {
                            slog::debug!(
                                &self.log,
                                "discovered a new component!";
                                "component_id" => %target.component_id,
                                "component_kind" => %target.component_kind,
                                "description" => %target.component_id,
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

            // We will need capacity for *at least* the number of components on the
            // SP --- it will probably be more, as several components have multiple
            // measurement channels which will produce independent samples (e.g. a
            // power rail will likely have both voltage and current measurements,
            // and a device may have multiple rails...) but, this way, we can avoid
            // *some* amount of reallocating...
            samples.reserve(self.components.len());
            for (c, metrics) in &mut self.components {
                // Metrics samples *should* always be well-formed. If we ever emit a
                // messed up one, this is a programmer error, and therefore should
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
                        return Err(CommunicationError::NoSpDiscovered);
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
                    let sensor: Cow<'static, str> = Cow::Owned(m.name);

                    // Temperature measurements which have the sensor name "CPU"
                    // and are from the SB-TSI device kind are CPU Tctl values.
                    // These are neither Fahrenheit nor Celsius, but a secret
                    // third thing, which is  not actually a "temperature
                    // measurement" in the sense that you probably imagined when
                    // you saw the words "temperature" and "measurement".
                    // Instead, it's a dimensionless value in the range from
                    // 0-100 that's  calculated by the CPU's thermal control
                    // loop.
                    //
                    // Therefore, we report it as a different metric from the
                    // `hardware_component:temperature` metric, which is
                    // in degrees Celsius.
                    //
                    // See this comment for details on what Tctl values mean:
                    // https://github.com/illumos/illumos-gate/blob/6cf3cc9d1e40f89e90135a48f74f03f879fce639/usr/src/uts/intel/io/amdzen/smntemp.c#L21-L57
                    let is_tctl =
                        sensor == "CPU" && target.component_kind == "sbtsi";
                    // First, if there's a measurement error, increment the
                    // error count metric. We will synthesize a missing sample
                    // for the sensor's metric as well, after we produce the
                    // measurement error sample.
                    //
                    // We do this first so that we only have to clone the
                    // sensor's name if there's an error, rather than always
                    // cloning it in *case* there's an error.
                    const TCTL_NAME: &str = "amd_cpu_tctl";
                    if let Err(error) = m.value {
                        let kind = match m.kind {
                            MeasurementKind::Temperature if is_tctl => {
                                TCTL_NAME
                            }
                            MeasurementKind::CpuTctl => TCTL_NAME,
                            MeasurementKind::Temperature => "temperature",
                            MeasurementKind::Current => "current",
                            MeasurementKind::Voltage => "voltage",
                            MeasurementKind::Power => "power",
                            MeasurementKind::InputCurrent => "input_current",
                            MeasurementKind::InputVoltage => "input_voltage",
                            MeasurementKind::Speed => "fan_speed",
                        };
                        let error = match error {
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
                                name: sensor.clone(),
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
                        try_sample!(Sample::new(
                            target,
                            &metric::SensorErrorCount {
                                error: Cow::Borrowed(error),
                                sensor: sensor.clone(),
                                datum: *datum,
                                sensor_kind: Cow::Borrowed(kind),
                            },
                        ));
                    }

                    // I don't love this massive `match`, but because the
                    // `Sample::new_missing` constructor is a different function
                    // from `Sample::new`, we need separate branches for the
                    // error and not-error cases, rather than just doing
                    // something to produce a datum from both the `Ok` and
                    // `Error` cases...
                    let sample = match (m.value, m.kind) {
                        // The CPU's Tctl value gets reported as a separate
                        // metric, as it is dimensionless.
                        (Ok(datum), MeasurementKind::Temperature)
                            if is_tctl =>
                        {
                            Sample::new(
                                target,
                                &metric::AmdCpuTctl { sensor, datum },
                            )
                        }
                        // Other measurements with the "temperature" measurement
                        // kind are physical temperatures that actually exist in
                        // reality (and are always in Celsius).
                        (Ok(datum), MeasurementKind::Temperature) => {
                            Sample::new(
                                target,
                                &metric::Temperature { sensor, datum },
                            )
                        }
                        (Err(_), MeasurementKind::Temperature) if is_tctl => {
                            Sample::new_missing(
                                target,
                                &metric::AmdCpuTctl { sensor, datum: 0.0 },
                            )
                        }
                        (Err(_), MeasurementKind::Temperature) => {
                            Sample::new_missing(
                                target,
                                &metric::Temperature { sensor, datum: 0.0 },
                            )
                        }

                        (Ok(datum), MeasurementKind::CpuTctl) => Sample::new(
                            target,
                            &metric::AmdCpuTctl { sensor, datum },
                        ),
                        (Err(_), MeasurementKind::CpuTctl) => {
                            Sample::new_missing(
                                target,
                                &metric::AmdCpuTctl { sensor, datum: 0.0 },
                            )
                        }
                        (Ok(datum), MeasurementKind::Current) => Sample::new(
                            target,
                            &metric::Current { sensor, datum },
                        ),
                        (Err(_), MeasurementKind::Current) => {
                            Sample::new_missing(
                                target,
                                &metric::Current { sensor, datum: 0.0 },
                            )
                        }
                        (Ok(datum), MeasurementKind::Voltage) => Sample::new(
                            target,
                            &metric::Voltage { sensor, datum },
                        ),

                        (Err(_), MeasurementKind::Voltage) => {
                            Sample::new_missing(
                                target,
                                &metric::Voltage { sensor, datum: 0.0 },
                            )
                        }
                        (Ok(datum), MeasurementKind::Power) => Sample::new(
                            target,
                            &metric::Power { sensor, datum },
                        ),
                        (Err(_), MeasurementKind::Power) => {
                            Sample::new_missing(
                                target,
                                &metric::Power { sensor, datum: 0.0 },
                            )
                        }
                        (Ok(datum), MeasurementKind::InputCurrent) => {
                            Sample::new(
                                target,
                                &metric::InputCurrent { sensor, datum },
                            )
                        }
                        (Err(_), MeasurementKind::InputCurrent) => {
                            Sample::new_missing(
                                target,
                                &metric::InputCurrent { sensor, datum: 0.0 },
                            )
                        }
                        (Ok(datum), MeasurementKind::InputVoltage) => {
                            Sample::new(
                                target,
                                &metric::InputVoltage { sensor, datum },
                            )
                        }
                        (Err(_), MeasurementKind::InputVoltage) => {
                            Sample::new_missing(
                                target,
                                &metric::InputVoltage { sensor, datum: 0.0 },
                            )
                        }
                        (Ok(datum), MeasurementKind::Speed) => Sample::new(
                            target,
                            &metric::FanSpeed { sensor, datum },
                        ),
                        (Err(_), MeasurementKind::Speed) => {
                            Sample::new_missing(
                                target,
                                &metric::FanSpeed { sensor, datum: 0.0 },
                            )
                        }
                    };
                    try_sample!(sample);
                }
            }

            // Now, fetch the SP's state *again*. It is possible that, while we
            // were scraping the SP's samples, the SP's identity changed in some
            // way: perhaps its version was updated during the poll, or it
            // was removed from the rack and replaced with an entirely different
            // chassis! If that's the case, some of the samples we collected may
            // have a metrics target describing the wrong thing (e.g. they could
            // still have the previous firmware's `hubris_archive_id`, if the SP
            // was updated). In that case, we need to throw away the samples we
            // collected and try again, potentially rebuilding our understanding
            // of the SP's inventory.
            let state = SpUnderstanding::from(sp.state().await?);
            if state == current_state {
                // All good, the SP is still who we thought it was! We can
                // "commit" this batch of samples
                return Ok(samples);
            }

            slog::info!(
                &self.log,
                "SP's state changed mid-poll! discarding current samples and \
                 starting over!";
                "new_state" => ?state,
                "current_state" => ?current_state,
            );
            // Let's reuse the buffer we already have for the next batch of
            // samples.
            samples.clear();
            //...and try again with the new state.
            current_state = state;
        }
    }
}

/// The fields of the `gateway_messages` `VersionedSpState` and
/// `SpStateV1`/`SpStateV2`/`SpStateV3` that we actually care about for purposes
/// of determining whether our understanding of the SP's components are still
/// valid.
///
/// In particular, we throw out the RoT state and the SP's power state, because
/// those changing won't actually invalidate our understanding of the SP's
/// components.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct SpUnderstanding {
    hubris_archive_id: [u8; 8],
    serial_number: [u8; 32],
    model: [u8; 32],
    revision: u32,
}

impl From<VersionedSpState> for SpUnderstanding {
    fn from(v: VersionedSpState) -> Self {
        match v {
            VersionedSpState::V1(gateway_messages::SpStateV1 {
                hubris_archive_id,
                serial_number,
                model,
                revision,
                ..
            }) => Self { hubris_archive_id, serial_number, model, revision },
            VersionedSpState::V2(gateway_messages::SpStateV2 {
                hubris_archive_id,
                serial_number,
                model,
                revision,
                ..
            }) => Self { hubris_archive_id, serial_number, model, revision },
            VersionedSpState::V3(gateway_messages::SpStateV3 {
                hubris_archive_id,
                serial_number,
                model,
                revision,
                ..
            }) => Self { hubris_archive_id, serial_number, model, revision },
        }
    }
}

// Reimplement this ourselves because we don't really care about
// reading the RoT state at present. This is unfortunately copied
// from `gateway_messages`.
fn stringify_byte_string(bytes: &[u8]) -> String {
    // We expect serial and model numbers to be ASCII and 0-padded: find the first 0
    // byte and convert to a string. If that fails, hexlify the entire slice.
    let first_zero = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());

    std::str::from_utf8(&bytes[..first_zero])
        .map(|s| s.to_string())
        .unwrap_or_else(|_err| hex::encode(bytes))
}

impl ServerManager {
    async fn run(mut self, cfg: Option<MetricsConfig>) -> anyhow::Result<()> {
        let (registration_address, bind_loopback) =
            if let Some(MetricsConfig {
                dev_bind_loopback,
                dev_nexus_address,
                ..
            }) = cfg
            {
                if dev_bind_loopback || dev_nexus_address.is_some() {
                    slog::warn!(
                        &self.log,
                        "using development metrics configuration overrides!";
                        "nexus_address" => ?dev_nexus_address,
                        "bind_loopback" => dev_bind_loopback,
                    );
                }
                (dev_nexus_address, dev_bind_loopback)
            } else {
                (None, false)
            };
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
                    "collection_interval" => ?OXIMETER_COLLECTION_INTERVAL,
                    "producer_id" => ?id,
                );
                let server = {
                    // Listen on any available socket, using the provided underlay IP.
                    let address = SocketAddr::new(ip.into(), 0);

                    let server_info = ProducerEndpoint {
                        id,
                        kind: ProducerKind::ManagementGateway,
                        address,
                        interval: OXIMETER_COLLECTION_INTERVAL,
                    };
                    let config = oximeter_producer::Config {
                        server_info,
                        registration_address,
                        default_request_body_max_bytes: METRIC_REQUEST_MAX_SIZE,
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
                    "collection_interval" => ?OXIMETER_COLLECTION_INTERVAL,
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
            &metric::PollErrorCount {
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
        CommunicationError::BadDecompressionSize { .. } => {
            "bad_decompression_size"
        }
    }
}
