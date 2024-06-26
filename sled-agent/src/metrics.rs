// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics produced by the sled-agent for collection by oximeter.

use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter::types::MetricsError;
use oximeter::types::ProducerRegistry;
use oximeter_instruments::kstat::link;
use oximeter_instruments::kstat::CollectionDetails;
use oximeter_instruments::kstat::Error as KstatError;
use oximeter_instruments::kstat::KstatSampler;
use oximeter_instruments::kstat::TargetId;
use oximeter_producer::LogConfig;
use oximeter_producer::Server as ProducerServer;
use sled_hardware_types::Baseboard;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

/// The interval on which we ask `oximeter` to poll us for metric data.
pub(crate) const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(30);

/// The interval on which we sample link metrics.
pub(crate) const LINK_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// The maximum Dropshot request size for the metrics server.
const METRIC_REQUEST_MAX_SIZE: usize = 10 * 1024 * 1024;

/// An error during sled-agent metric production.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kstat-based metric failure")]
    Kstat(#[source] KstatError),

    #[error("Failed to insert metric producer into registry")]
    Registry(#[source] MetricsError),

    #[error("Failed to fetch hostname")]
    Hostname(#[source] std::io::Error),

    #[error("Non-UTF8 hostname")]
    NonUtf8Hostname,

    #[error("Missing NULL byte in hostname")]
    HostnameMissingNull,

    #[error("Failed to start metric producer server")]
    ProducerServer(#[source] oximeter_producer::Error),
}

// Basic metadata about the sled agent used when publishing metrics.
#[derive(Clone, Debug)]
struct SledIdentifiers {
    sled_id: Uuid,
    rack_id: Uuid,
    baseboard: Baseboard,
}

/// Type managing all oximeter metrics produced by the sled-agent.
//
// TODO-completeness: We probably want to get kstats or other metrics in to this
// type from other parts of the code, possibly before the `SledAgent` itself
// exists. This is similar to the storage resources or other objects, most of
// which are essentially an `Arc<Mutex<Inner>>`. It would be nice to avoid that
// pattern, but until we have more statistics, it's not clear whether that's
// worth it right now.
#[derive(Clone)]
pub struct MetricsManager {
    metadata: Arc<SledIdentifiers>,
    _log: Logger,
    kstat_sampler: KstatSampler,
    // TODO-scalability: We may want to generalize this to store any kind of
    // tracked target, and use a naming scheme that allows us pick out which
    // target we're interested in from the arguments.
    //
    // For example, we can use the link name to do this, for any physical or
    // virtual link, because they need to be unique. We could also do the same
    // for disks or memory. If we wanted to guarantee uniqueness, we could
    // namespace them internally, e.g., `"datalink:{link_name}"` would be the
    // real key.
    tracked_links: Arc<Mutex<BTreeMap<String, TargetId>>>,
    producer_server: Arc<ProducerServer>,
}

impl MetricsManager {
    /// Construct a new metrics manager.
    ///
    /// This takes a few key pieces of identifying information that are used
    /// when reporting sled-specific metrics.
    pub fn new(
        sled_id: Uuid,
        rack_id: Uuid,
        baseboard: Baseboard,
        sled_address: Ipv6Addr,
        log: Logger,
    ) -> Result<Self, Error> {
        let producer_server =
            start_producer_server(&log, sled_id, sled_address)?;
        let kstat_sampler = KstatSampler::new(&log).map_err(Error::Kstat)?;
        producer_server
            .registry()
            .register_producer(kstat_sampler.clone())
            .map_err(Error::Registry)?;
        let tracked_links = Arc::new(Mutex::new(BTreeMap::new()));
        Ok(Self {
            metadata: Arc::new(SledIdentifiers { sled_id, rack_id, baseboard }),
            _log: log,
            kstat_sampler,
            tracked_links,
            producer_server,
        })
    }

    /// Return a reference to the contained producer registry.
    pub fn registry(&self) -> &ProducerRegistry {
        self.producer_server.registry()
    }
}

/// Start a metric producer server.
fn start_producer_server(
    log: &Logger,
    sled_id: Uuid,
    sled_address: Ipv6Addr,
) -> Result<Arc<ProducerServer>, Error> {
    let log = log.new(slog::o!("component" => "producer-server"));
    let registry = ProducerRegistry::with_id(sled_id);

    // Listen on any available socket, using our underlay address.
    let address = SocketAddr::new(sled_address.into(), 0);

    // Resolve Nexus via DNS.
    let registration_address = None;
    let config = oximeter_producer::Config {
        server_info: ProducerEndpoint {
            id: registry.producer_id(),
            kind: ProducerKind::SledAgent,
            address,
            interval: METRIC_COLLECTION_INTERVAL,
        },
        registration_address,
        request_body_max_bytes: METRIC_REQUEST_MAX_SIZE,
        log: LogConfig::Logger(log),
    };
    ProducerServer::start(&config).map(Arc::new).map_err(Error::ProducerServer)
}

impl MetricsManager {
    /// Track metrics for a physical datalink.
    pub async fn track_physical_link(
        &self,
        link_name: impl AsRef<str>,
        interval: Duration,
    ) -> Result<(), Error> {
        let hostname = hostname()?;
        let link = link::physical_data_link::PhysicalDataLink {
            rack_id: self.metadata.rack_id,
            sled_id: self.metadata.sled_id,
            serial: self.serial_number().into(),
            hostname: hostname.into(),
            link_name: link_name.as_ref().to_string().into(),
        };
        let details = CollectionDetails::never(interval);
        let id = self
            .kstat_sampler
            .add_target(link, details)
            .await
            .map_err(Error::Kstat)?;
        self.tracked_links
            .lock()
            .unwrap()
            .insert(link_name.as_ref().to_string(), id);
        Ok(())
    }

    /// Stop tracking metrics for a datalink.
    ///
    /// This works for both physical and virtual links.
    #[allow(dead_code)]
    pub async fn stop_tracking_link(
        &self,
        link_name: impl AsRef<str>,
    ) -> Result<(), Error> {
        let maybe_id =
            self.tracked_links.lock().unwrap().remove(link_name.as_ref());
        if let Some(id) = maybe_id {
            self.kstat_sampler.remove_target(id).await.map_err(Error::Kstat)
        } else {
            Ok(())
        }
    }

    // Return the serial number out of the baseboard, if one exists.
    fn serial_number(&self) -> String {
        match &self.metadata.baseboard {
            Baseboard::Gimlet { identifier, .. } => identifier.clone(),
            Baseboard::Unknown => String::from("unknown"),
            Baseboard::Pc { identifier, .. } => identifier.clone(),
        }
    }
}

// Return the current hostname if possible.
fn hostname() -> Result<String, Error> {
    // See netdb.h
    const MAX_LEN: usize = 256;
    let mut out = vec![0u8; MAX_LEN + 1];
    if unsafe {
        libc::gethostname(out.as_mut_ptr() as *mut libc::c_char, MAX_LEN)
    } == 0
    {
        // Split into subslices by NULL bytes.
        //
        // We should have a NULL byte, since we've asked for no more than 255
        // bytes in a 256 byte buffer, but you never know.
        let Some(chunk) = out.split(|x| *x == 0).next() else {
            return Err(Error::HostnameMissingNull);
        };
        let s = std::ffi::CString::new(chunk)
            .map_err(|_| Error::NonUtf8Hostname)?;
        s.into_string().map_err(|_| Error::NonUtf8Hostname)
    } else {
        Err(std::io::Error::last_os_error()).map_err(|_| Error::NonUtf8Hostname)
    }
}
