// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::ServerContext;
use anyhow::Context;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::types::ProducerRegistry;
use oximeter::types::Sample;
use oximeter::MetricsError;
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
    samples: Arc<Mutex<Vec<Sample>>>,
    log: slog::Logger,
    apictx: Arc<ServerContext>,
}

/// Manages a metrics server and stuff.
struct Manager {
    log: slog::Logger,
    addrs: watch::Receiver<Vec<SocketAddrV6>>,
    registry: ProducerRegistry,
}

#[derive(Debug)]
struct Producer(Arc<Mutex<Vec<Sample>>>);

/// The interval on which we ask `oximeter` to poll us for metric data.
// N.B.: I picked this pretty arbitrarily...
const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(10);

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
        Ok(Box::new(samples.into_iter()))
    }
}

impl Poller {
    async fn run(self, rack_id: oneshot::Receiver<Uuid>) -> anyhow::Result<()> {
        // First, wait until we know what the rack ID is...
        let rack_id = rack_id.await.context(
            "rack ID sender has gone away...we must be shutting down",
        )?;

        anyhow::bail!("TODO(eliza): draw the rest of the owl!")
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
                    let registration_address = None;

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
