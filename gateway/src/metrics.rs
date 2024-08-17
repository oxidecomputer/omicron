// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::management_switch::SpIdentifier;
use crate::management_switch::SpType;
use crate::ServerContext;
use anyhow::Context;
use gateway_messages::measurement::MeasurementError;
use gateway_messages::measurement::MeasurementKind;
use gateway_messages::ComponentDetails;
use gateway_messages::DeviceCapabilities;
use gateway_sp_comms::SpComponent;
use gateway_sp_comms::VersionedSpState;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
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

use crate::MgsArguments;

oximeter::use_timeseries!("sensor-measurement.toml");

/// Handle to the metrics task.
pub struct Metrics {
    addrs_tx: watch::Sender<Vec<SocketAddrV6>>,
    rack_id_tx: Option<oneshot::Sender<Uuid>>,
    manager: JoinHandle<anyhow::Result<()>>,
    poller: JoinHandle<anyhow::Result<()>>,
}

/// CLI arguments for configuring metrics.
#[derive(Copy, Clone, Debug, Default, clap::Parser)]
#[clap(next_help_heading = "SP Metrics Development Configuration")]
pub struct Args {
    /// Override the Nexus address used to register the SP metrics Oximeter
    /// producer. This is intended for use in development and testing.
    ///
    /// If this argument is not present, Nexus is discovered through DNS.
    #[clap(long = "dev-nexus-address")]
    nexus_address: Option<SocketAddr>,

    /// Allow the metrics producer endpoint to bind on loopback.
    ///
    /// This should be disabled in production, as Nexus will not be able to
    /// reach the loopback interface, but is necessary for local development and
    /// test purposes.
    #[clap(long = "dev-metrics-bind-loopback")]
    bind_loopback: bool,
}

/// Manages SP pollers, making sure that every SP has a poller task.
struct PollerManager {
    log: slog::Logger,
    apictx: Arc<ServerContext>,
    mgs_id: Uuid,
    /// Poller tasks
    tasks: tokio::task::JoinSet<anyhow::Result<()>>,
    /// The manager doesn't actually produce samples, but it needs to be able to
    /// clone a sender for every poller task it spawns.
    sample_tx: broadcast::Sender<Vec<Sample>>,
}

/// Polls sensor readings from an individual SP.
struct SpPoller {
    apictx: Arc<ServerContext>,
    spid: SpIdentifier,
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
    /// Counts of errors that occurred whilst polling the d
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
    args: Args,
}

#[derive(Debug)]
struct Producer(broadcast::Receiver<Vec<Sample>>);

/// The interval on which we ask `oximeter` to poll us for metric data.
// N.B.: I picked this pretty arbitrarily...
const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(10);

/// The interval at which we poll sensor readings from SPs. Bryan wants to try
/// 1Hz and see if the SP can handle it.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

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

/// Number of sample vectors from individual SPs to buffer.
const SAMPLE_CHANNEL_CAPACITY: usize = {
    // Roughly how many times will we poll SPs for each metrics collection
    // interval?
    let polls_per_metrics_interval = (METRIC_COLLECTION_INTERVAL.as_secs()
        / POLL_INTERVAL.as_secs())
        as usize;
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
        apictx: Arc<ServerContext>,
    ) -> anyhow::Result<Self> {
        let &MgsArguments { id, rack_id, ref addresses, metrics_args } = args;
        let registry = ProducerRegistry::with_id(id);

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
            broadcast::channel(SAMPLE_CHANNEL_CAPACITY);

        registry
            .register_producer(Producer(sample_rx))
            .context("failed to register metrics producer")?;

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
        let poller = tokio::spawn(
            PollerManager {
                sample_tx,
                apictx,
                tasks: tokio::task::JoinSet::new(),
                log: log.new(slog::o!("component" => "sensor-poller")),
                mgs_id: id,
            }
            .run(rack_id_rx),
        );

        let (addrs_tx, addrs_rx) =
            tokio::sync::watch::channel(addresses.clone());
        let manager = tokio::spawn(
            ServerManager {
                log: log.new(slog::o!("component" => "producer-server")),
                addrs: addrs_rx,
                registry,
                args: metrics_args,
            }
            .run(),
        );
        Ok(Self { addrs_tx, rack_id_tx, manager, poller })
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
        self.manager.abort();
        self.poller.abort();
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
        let mut samples = Vec::with_capacity(self.0.len());
        use broadcast::error::TryRecvError;
        loop {
            match self.0.try_recv() {
                Ok(sample_chunk) => samples.push(sample_chunk),
                // This error indicates that an old ringbuffer entry was
                // overwritten. That's fine, just get the next one.
                Err(TryRecvError::Lagged(_)) => continue,
                // We've drained all currently available samples! We're done here!
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
            }
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

        let mut poll_interval = tokio::time::interval(POLL_INTERVAL);
        let mut known_sps: HashMap<SpIdentifier, tokio::task::AbortHandle> =
            HashMap::with_capacity(NORMAL_NUMBER_OF_SPS);
        // Wait for SP discovery to complete, if it hasn't already.
        // TODO(eliza): presently, we busy-poll here. It would be nicer to
        // replace the `OnceLock<Result<LocationMap, ...>` in `ManagementSwitch`
        // with a `tokio::sync::watch`
        while !switch.is_discovery_complete() {
            poll_interval.tick().await;
        }

        slog::info!(
            &self.log,
            "SP discovery complete! starting to poll sensors..."
        );

        loop {
            let sps = match switch.all_sps() {
                Ok(sps) => sps,
                Err(e) => {
                    slog::warn!(
                        &self.log,
                        "failed to enumerate service processors! will try again in a bit";
                        "error" => %e,
                    );
                    poll_interval.tick().await;
                    continue;
                }
            };

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
                    apictx: self.apictx.clone(),
                    log: self.log.new(slog::o!(
                        "sp_slot" => spid.slot,
                        "chassis_type" => format!("{:?}", spid.typ),
                    )),
                    components: HashMap::new(),
                    sample_tx: self.sample_tx.clone(),
                };
                let poller_handle = self.tasks.spawn(poller.run(POLL_INTERVAL));
                let _prev_poller = known_sps.insert(spid, poller_handle);
                debug_assert!(
                    _prev_poller.map(|p| p.is_finished()).unwrap_or(true),
                    "if we clobbered an existing poller task, it better have \
                     been because it was dead..."
                );
            }

            // All pollers started! Now wait to see if any of them have died...
            let mut err = self.tasks.join_next().await;
            while let Some(Ok(Err(error))) = err {
                // TODO(eliza): actually handle errors polling a SP
                // nicely...
                slog::error!(
                    &self.log,
                    "something bad happened  while polling a SP...";
                    "error" => %error,
                );
                // drain any remaining errors
                err = self.tasks.try_join_next();
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
    async fn run(mut self, poll_interval: Duration) -> anyhow::Result<()> {
        let switch = &self.apictx.mgmt_switch;
        let sp = switch.sp(self.spid)?;
        let mut interval = tokio::time::interval(poll_interval);
        let mut known_state = None;

        loop {
            interval.tick().await;
            slog::trace!(&self.log, "interval elapsed, polling SP...");

            // Check if the SP's state has changed. If it has, we need to make sure
            // we still know what all of its sensors are.
            let current_state = sp.state().await?;
            if Some(&current_state) != known_state.as_ref() {
                // The SP's state appears to have changed. Time to make sure our
                // understanding of its devices and identity is up to date!
                slog::debug!(
                    &self.log,
                    "our little friend seems to have changed in some kind of way";
                    "current_state" => ?current_state,
                    "known_state" => ?known_state,
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
                    let first_zero = bytes
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(bytes.len());

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
                    let component_name = match dev.component.as_str() {
                        Some(c) => c,
                        None => {
                            // These are supposed to always be strings. But, if we
                            // see one that's not a string, bail instead of panicking.
                            slog::error!(
                                &self.log,
                                "a SP component ID was not a string! this isn't supposed to happen!";
                                "device" => ?dev,
                            );
                            anyhow::bail!("a SP component ID was not stringy!");
                        }
                    };
                    // TODO(eliza): i hate having to clone all these strings for
                    // every device on the SP...it would be cool if Oximeter let us
                    // reference count them...
                    let target = component::Component {
                        chassis_type: match self.spid.typ {
                            SpType::Sled => Cow::Borrowed("sled"),
                            SpType::Switch => Cow::Borrowed("switch"),
                            SpType::Power => Cow::Borrowed("power"),
                        },
                        slot: self.spid.slot as u32,
                        component: Cow::Owned(component_name.to_string()),
                        device: Cow::Owned(dev.device),
                        model: Cow::Owned(model.clone()),
                        revision,
                        serial: Cow::Owned(serial.clone()),
                        rack_id: self.rack_id,
                        gateway_id: self.mgs_id,
                        hubris_archive_id: Cow::Owned(
                            hubris_archive_id.clone(),
                        ),
                    };
                    match self.components.entry(dev.component) {
                        // Found a new device!
                        hash_map::Entry::Vacant(entry) => {
                            slog::debug!(
                                &self.log,
                                "discovered a new component!";
                                "component" => ?dev.component,
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
                                "target has changed, resetting cumulative metrics for component";
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

                known_state = Some(current_state);
            }

            let mut samples = Vec::with_capacity(self.components.len());
            for (c, ComponentMetrics { target, sensor_errors, poll_errors }) in
                &mut self.components
            {
                let details = match sp.component_details(*c).await {
                    Ok(deets) => deets,
                    Err(error) => {
                        slog::warn!(
                            &self.log,
                            "failed to read details on SP component";
                            "sp_component" => %c,
                            "error" => %error,
                        );
                        // TODO(eliza): we should increment a metric here...
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
                }
                for d in details.entries {
                    let ComponentDetails::Measurement(m) = d else {
                        // If the component details are switch port details rather
                        // than measurement channels, ignore it for now.
                        continue;
                    };
                    let name = Cow::Owned(m.name);
                    let sample = match (m.value, m.kind) {
                        (Ok(datum), MeasurementKind::Temperature) => {
                            Sample::new(
                                target,
                                &component::Temperature { name, datum },
                            )
                        }
                        (Ok(datum), MeasurementKind::Current) => Sample::new(
                            target,
                            &component::Current { name, datum },
                        ),
                        (Ok(datum), MeasurementKind::Voltage) => Sample::new(
                            target,
                            &component::Voltage { name, datum },
                        ),
                        (Ok(datum), MeasurementKind::Power) => Sample::new(
                            target,
                            &component::Power { name, datum },
                        ),
                        (Ok(datum), MeasurementKind::InputCurrent) => {
                            Sample::new(
                                target,
                                &component::InputCurrent { name, datum },
                            )
                        }
                        (Ok(datum), MeasurementKind::InputVoltage) => {
                            Sample::new(
                                target,
                                &component::InputVoltage { name, datum },
                            )
                        }
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
                                MeasurementKind::InputCurrent => {
                                    "input_current"
                                }
                                MeasurementKind::InputVoltage => {
                                    "input_voltage"
                                }
                                MeasurementKind::Speed => "fan_speed",
                            };
                            let error = match e {
                                MeasurementError::InvalidSensor => {
                                    "invalid_sensor"
                                }
                                MeasurementError::NoReading => "no_reading",
                                MeasurementError::NotPresent => "not_present",
                                MeasurementError::DeviceError => "device_error",
                                MeasurementError::DeviceUnavailable => {
                                    "device_unavailable"
                                }
                                MeasurementError::DeviceTimeout => {
                                    "device_timeout"
                                }
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
                    }?;
                    samples.push(sample);
                }
            }
            // No sense cluttering the ringbuffer with empty vecs...
            if samples.is_empty() {
                continue;
            }
            if let Err(_) = self.sample_tx.send(samples) {
                slog::info!(
                    &self.log,
                    "all sample receiver handles have been dropped! presumably we are shutting down...";
                );
                return Ok(());
            }
        }
    }
}

impl ServerManager {
    async fn run(mut self) -> anyhow::Result<()> {
        if self.args.nexus_address.is_some() || self.args.bind_loopback {
            slog::warn!(
                &self.log,
                "using development metrics configuration overrides!";
                "nexus_address" => ?self.args.nexus_address,
                "bind_loopback" => self.args.bind_loopback,
            );
        }
        let mut current_server: Option<oximeter_producer::Server> = None;
        loop {
            let current_ip = current_server.as_ref().map(|s| s.address().ip());
            let mut new_ip = None;
            for addr in self.addrs.borrow_and_update().iter() {
                let &ip = addr.ip();
                // Don't bind the metrics endpoint on ::1
                if ip.is_loopback() && !self.args.bind_loopback {
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
                slog::info!(
                    &self.log,
                    "rebinding producer server on new IP";
                    "new_ip" => ?ip,
                    "current_ip" => ?current_ip,
                );
                let server = {
                    // Listen on any available socket, using the provided underlay IP.
                    let address = SocketAddr::new(ip.into(), 0);

                    let server_info = ProducerEndpoint {
                        id: self.registry.producer_id(),
                        kind: ProducerKind::ManagementGateway,
                        address,
                        interval: METRIC_COLLECTION_INTERVAL,
                    };
                    let config = oximeter_producer::Config {
                        server_info,
                        registration_address: self.args.nexus_address,
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
