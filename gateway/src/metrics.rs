// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::management_switch::SpIdentifier;
use crate::management_switch::SpType;
use crate::ServerContext;
use anyhow::Context;
use gateway_messages::measurement::MeasurementKind;
use gateway_messages::ComponentDetails;
use gateway_messages::DeviceCapabilities;
use gateway_sp_comms::SpComponent;
use gateway_sp_comms::VersionedSpState;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::types::ProducerRegistry;
use oximeter::types::Sample;
use oximeter::MetricsError;
use std::borrow::Cow;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

/// Actually polls SP sensor readings
struct Poller {
    samples: Arc<Mutex<Vec<Vec<Sample>>>>,
    log: slog::Logger,
    apictx: Arc<ServerContext>,
}

/// Manages a metrics server and stuff.
struct Manager {
    log: slog::Logger,
    addrs: watch::Receiver<Vec<SocketAddrV6>>,
    registry: ProducerRegistry,
}

struct SpPoller {
    apictx: Arc<ServerContext>,
    spid: SpIdentifier,
    my_understanding: Mutex<SpUnderstanding>,
    log: slog::Logger,
    rack_id: Uuid,
}

#[derive(Default)]
struct SpUnderstanding {
    state: Option<VersionedSpState>,
    devices: Vec<(SpComponent, component::Component)>,
}

#[derive(Debug)]
struct Producer(Arc<Mutex<Vec<Vec<Sample>>>>);

/// The interval on which we ask `oximeter` to poll us for metric data.
// N.B.: I picked this pretty arbitrarily...
const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(10);

/// The interval at which we poll sensor readings from SPs. Bryan wants to try
/// 1Hz and see if the SP can handle it.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The maximum Dropshot request size for the metrics server.
const METRIC_REQUEST_MAX_SIZE: usize = 10 * 1024 * 1024;

impl Metrics {
    pub fn new(
        log: &slog::Logger,
        MgsArguments { id, rack_id, addresses, .. }: &MgsArguments,
        apictx: Arc<ServerContext>,
    ) -> anyhow::Result<Self> {
        let registry = ProducerRegistry::with_id(*id);
        let samples = Arc::new(Mutex::new(Vec::new()));

        registry
            .register_producer(Producer(samples.clone()))
            .context("failed to register metrics producer")?;

        // Using a channel for this is, admittedly, a bit of an end-run around
        // the `OnceLock` on the `ServerContext` that *also* stores the rack ID,
        // but it has the nice benefit of allowing the `Poller` task to _await_
        // the rack ID being set...we might want to change other code to use a
        // similar approach in the future.
        let (rack_id_tx, rack_id_rx) = oneshot::channel();
        let rack_id_tx = if let Some(rack_id) = *rack_id {
            rack_id_tx.send(rack_id).expect(
                "we just created the channel; it therefore will not be \
                     closed",
            );
            None
        } else {
            Some(rack_id_tx)
        };
        let poller = tokio::spawn(
            Poller {
                samples,
                apictx,
                log: log.new(slog::o!("component" => "sensor-poller")),
            }
            .run(rack_id_rx),
        );

        let (addrs_tx, addrs_rx) =
            tokio::sync::watch::channel(addresses.clone());
        let manager = tokio::spawn(
            Manager {
                log: log.new(slog::o!("component" => "producer-server")),
                addrs: addrs_rx,
                registry,
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

impl oximeter::Producer for Producer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
        let samples = {
            let mut lock = self.0.lock().unwrap();
            std::mem::take(&mut *lock)
        };
        Ok(Box::new(samples.into_iter().flatten()))
    }
}

impl Poller {
    async fn run(self, rack_id: oneshot::Receiver<Uuid>) -> anyhow::Result<()> {
        let switch = &self.apictx.mgmt_switch;

        // First, wait until we know what the rack ID is...
        let rack_id = rack_id.await.context(
            "rack ID sender has gone away...we must be shutting down",
        )?;

        let mut poll_interval = tokio::time::interval(POLL_INTERVAL);
        let mut sps_as_i_understand_them = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            poll_interval.tick().await;

            // Wait for SP discovery to complete, if it hasn't already.
            // TODO(eliza): presently, we busy-poll here. It would be nicer to
            // replace the `OnceLock<Result<LocationMap, ...>` in `ManagementSwitch`
            // with a `tokio::sync::watch`
            if !switch.is_discovery_complete() {
                continue;
            }

            let sps = match switch.all_sps() {
                Ok(sps) => sps,
                Err(e) => {
                    slog::warn!(
                        &self.log,
                        "failed to enumerate service processors! will try again in a bit";
                        "error" => %e,
                    );
                    continue;
                }
            };

            for (spid, _) in sps {
                let understanding = sps_as_i_understand_them
                    .entry(spid)
                    .or_insert_with(|| {
                        slog::debug!(&self.log, "found a new little friend!"; "sp" => ?spid);
                        Arc::new(SpPoller {
                            spid,
                            rack_id,
                            apictx: self.apictx.clone(),
                            log: self.log.new(slog::o!("slot" => spid.slot, "sp_type" => format!("{:?}", spid.typ))),
                            my_understanding: Mutex::new(Default::default()),
                        })
                })
                    .clone();
                tasks.spawn(understanding.clone().poll_sp());
            }

            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(samples)) => {
                        // No sense copying all the samples into the big vec thing,
                        // just push the vec instead.
                        self.samples.lock().unwrap().push(samples);
                    }
                    Ok(Err(error)) => {
                        // TODO(eliza): actually handle errors polling a SP
                        // nicely...
                        slog::error!(
                            &self.log,
                            "something bad happened";
                            "error" => %error,
                        );
                    }
                    Err(_) => {
                        unreachable!(
                            "tasks on the joinset never get aborted, and we \
                             compile with panic=abort, so they won't panic"
                        )
                    }
                }
            }
        }
    }
}

impl SpPoller {
    async fn poll_sp(self: Arc<Self>) -> anyhow::Result<Vec<Sample>> {
        let switch = &self.apictx.mgmt_switch;
        let sp = switch.sp(self.spid)?;
        // Check if the SP's state has changed. If it has, we need to make sure
        // we still know what all of its sensors are.
        let current_state = sp.state().await?;
        let known_state = self.my_understanding.lock().unwrap().state.clone();

        let devices = if Some(&current_state) != known_state.as_ref() {
            slog::debug!(
                &self.log,
                "our little friend seems to have changed in some kind of way";
                "current_state" => ?current_state,
                "known_state" => ?known_state,
            );
            let inv_devices = sp.inventory().await?.devices;
            let mut devices: Vec<(SpComponent, component::Component)> =
                Vec::with_capacity(inv_devices.len());

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
                    VersionedSpState::V1(v) => (
                        stringify_byte_string(&v.model),
                        stringify_byte_string(&v.serial_number[..]),
                        hex::encode(v.hubris_archive_id),
                        v.revision,
                    ),
                    VersionedSpState::V2(v) => (
                        stringify_byte_string(&v.model),
                        stringify_byte_string(&v.serial_number[..]),
                        hex::encode(v.hubris_archive_id),
                        v.revision,
                    ),
                    VersionedSpState::V3(v) => (
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
                        slog::error!(&self.log, "a SP component ID was not a string! this isn't supposed to happen!"; "device" => ?dev);
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
                    component: Cow::Owned(component_name.to_string()),
                    device: Cow::Owned(dev.device),
                    model: Cow::Owned(model.clone()),
                    revision,
                    serial: Cow::Owned(serial.clone()),
                    rack_id: self.rack_id,
                    firmware_id: Cow::Owned(hubris_archive_id.clone()),
                };
                devices.push((dev.component, target))
            }
            devices
        } else {
            // This is a bit goofy, but we have to release the lock before
            // hitting any `await` points, so just move the inventory out of it.
            // We'll put it back when we're done. This lock is *actually* owned
            // exclusively by this `SpPoller`, but since it lives in a HashMap,
            // rust doesn't understand that.
            std::mem::take(&mut self.my_understanding.lock().unwrap().devices)
        };

        let mut samples = Vec::with_capacity(devices.len());
        for (c, target) in &devices {
            let details = match sp.component_details(*c).await {
                Ok(deets) => deets,
                Err(error) => {
                    slog::warn!(
                        &self.log,
                        "failed to read details on SP component";
                        "component" => %c,
                        "error" => %error,
                    );
                    // TODO(eliza): we should increment a metric here...
                    continue;
                }
            };
            for d in details.entries {
                let ComponentDetails::Measurement(m) = d else { continue };
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
                    (Err(error), _) => todo!(),
                }?;
                samples.push(sample);
            }
        }

        // Update our understanding again.
        let mut understanding = self.my_understanding.lock().unwrap();
        understanding.devices = devices;
        understanding.state = Some(current_state);

        Ok(samples)
    }
}

impl Manager {
    async fn run(mut self) -> anyhow::Result<()> {
        let mut current_server: Option<oximeter_producer::Server> = None;
        loop {
            let current_ip = current_server.as_ref().map(|s| s.address().ip());
            let mut new_ip = None;
            for addr in self.addrs.borrow_and_update().iter() {
                let &ip = addr.ip();
                // Don't bind the metrics endpoint on ::1
                if ip.is_loopback() {
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

                    // Discover Nexus via DNS
                    let registration_address =
                        Some("[]::1]:12223".parse().unwrap());

                    let server_info = ProducerEndpoint {
                        id: self.registry.producer_id(),
                        kind: ProducerKind::ManagementGateway,
                        address,
                        interval: METRIC_COLLECTION_INTERVAL,
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
