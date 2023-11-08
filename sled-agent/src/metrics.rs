// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics produced by the sled-agent for collection by oximeter.

use oximeter::types::MetricsError;
use oximeter::types::ProducerRegistry;
use sled_hardware::Baseboard;
use slog::Logger;
use std::time::Duration;
use uuid::Uuid;

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        use oximeter_instruments::kstat::link;
        use oximeter_instruments::kstat::CollectionDetails;
        use oximeter_instruments::kstat::Error as KstatError;
        use oximeter_instruments::kstat::KstatSampler;
        use oximeter_instruments::kstat::TargetId;
        use std::collections::BTreeMap;
    } else {
        use anyhow::anyhow;
    }
}

/// The interval on which we ask `oximeter` to poll us for metric data.
pub(crate) const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(30);

/// The interval on which we sample link metrics.
pub(crate) const LINK_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// An error during sled-agent metric production.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[cfg(target_os = "illumos")]
    #[error("Kstat-based metric failure")]
    Kstat(#[source] KstatError),

    #[cfg(not(target_os = "illumos"))]
    #[error("Kstat-based metric failure")]
    Kstat(#[source] anyhow::Error),

    #[error("Failed to insert metric producer into registry")]
    Registry(#[source] MetricsError),

    #[error("Failed to fetch hostname")]
    Hostname(#[source] std::io::Error),
}

/// Type managing all oximeter metrics produced by the sled-agent.
//
// TODO-completeness: We probably want to get kstats or other metrics in to this
// type from other parts of the code, possibly before the `SledAgent` itself
// exists. This is similar to the storage resources or other objects, most of
// which are essentially an `Arc<Mutex<Inner>>`. It would be nice to avoid that
// pattern, but until we have more statistics, it's not clear whether that's
// worth it right now.
#[derive(Clone, Debug)]
// NOTE: The ID fields aren't used on non-illumos systems, rather than changing
// the name of fields that are not yet used.
#[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
pub struct MetricsManager {
    sled_id: Uuid,
    rack_id: Uuid,
    baseboard: Baseboard,
    hostname: Option<String>,
    _log: Logger,
    #[cfg(target_os = "illumos")]
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
    #[cfg(target_os = "illumos")]
    tracked_links: BTreeMap<String, TargetId>,
    registry: ProducerRegistry,
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
        log: Logger,
    ) -> Result<Self, Error> {
        let registry = ProducerRegistry::with_id(sled_id);

        cfg_if::cfg_if! {
            if #[cfg(target_os = "illumos")] {
                let kstat_sampler = KstatSampler::new(&log).map_err(Error::Kstat)?;
                registry
                    .register_producer(kstat_sampler.clone())
                    .map_err(Error::Registry)?;
                let tracked_links = BTreeMap::new();
            }
        }
        Ok(Self {
            sled_id,
            rack_id,
            baseboard,
            hostname: None,
            _log: log,
            #[cfg(target_os = "illumos")]
            kstat_sampler,
            #[cfg(target_os = "illumos")]
            tracked_links,
            registry,
        })
    }

    /// Return a reference to the contained producer registry.
    pub fn registry(&self) -> &ProducerRegistry {
        &self.registry
    }
}

#[cfg(target_os = "illumos")]
impl MetricsManager {
    /// Track metrics for a physical datalink.
    pub async fn track_physical_link(
        &mut self,
        link_name: impl AsRef<str>,
        interval: Duration,
    ) -> Result<(), Error> {
        let hostname = self.hostname().await?;
        let link = link::PhysicalDataLink {
            rack_id: self.rack_id,
            sled_id: self.sled_id,
            serial: self.serial_number(),
            hostname,
            link_name: link_name.as_ref().to_string(),
        };
        let details = CollectionDetails::never(interval);
        let id = self
            .kstat_sampler
            .add_target(link, details)
            .await
            .map_err(Error::Kstat)?;
        self.tracked_links.insert(link_name.as_ref().to_string(), id);
        Ok(())
    }

    /// Stop tracking metrics for a datalink.
    ///
    /// This works for both physical and virtual links.
    #[allow(dead_code)]
    pub async fn stop_tracking_link(
        &mut self,
        link_name: impl AsRef<str>,
    ) -> Result<(), Error> {
        if let Some(id) = self.tracked_links.remove(link_name.as_ref()) {
            self.kstat_sampler.remove_target(id).await.map_err(Error::Kstat)
        } else {
            Ok(())
        }
    }

    /// Track metrics for a virtual datalink.
    #[allow(dead_code)]
    pub async fn track_virtual_link(
        &self,
        link_name: impl AsRef<str>,
        hostname: impl AsRef<str>,
        interval: Duration,
    ) -> Result<(), Error> {
        let link = link::VirtualDataLink {
            rack_id: self.rack_id,
            sled_id: self.sled_id,
            serial: self.serial_number(),
            hostname: hostname.as_ref().to_string(),
            link_name: link_name.as_ref().to_string(),
        };
        let details = CollectionDetails::never(interval);
        self.kstat_sampler
            .add_target(link, details)
            .await
            .map(|_| ())
            .map_err(Error::Kstat)
    }

    // Return the serial number out of the baseboard, if one exists.
    fn serial_number(&self) -> String {
        match &self.baseboard {
            Baseboard::Gimlet { identifier, .. } => identifier.clone(),
            Baseboard::Unknown => String::from("unknown"),
            Baseboard::Pc { identifier, .. } => identifier.clone(),
        }
    }

    // Return the system's hostname.
    //
    // If we've failed to get it previously, we try again. If _that_ fails,
    // return an error.
    //
    // TODO-cleanup: This will become much simpler once
    // `OnceCell::get_or_try_init` is stabilized.
    async fn hostname(&mut self) -> Result<String, Error> {
        if let Some(hn) = &self.hostname {
            return Ok(hn.clone());
        }
        let hn = tokio::process::Command::new("hostname")
            .env_clear()
            .output()
            .await
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .map_err(Error::Hostname)?;
        self.hostname.replace(hn.clone());
        Ok(hn)
    }
}

#[cfg(not(target_os = "illumos"))]
impl MetricsManager {
    /// Track metrics for a physical datalink.
    pub async fn track_physical_link(
        &mut self,
        _link_name: impl AsRef<str>,
        _interval: Duration,
    ) -> Result<(), Error> {
        Err(Error::Kstat(anyhow!(
            "kstat metrics are not supported on this platform"
        )))
    }

    /// Stop tracking metrics for a datalink.
    ///
    /// This works for both physical and virtual links.
    #[allow(dead_code)]
    pub async fn stop_tracking_link(
        &mut self,
        _link_name: impl AsRef<str>,
    ) -> Result<(), Error> {
        Err(Error::Kstat(anyhow!(
            "kstat metrics are not supported on this platform"
        )))
    }

    /// Track metrics for a virtual datalink.
    #[allow(dead_code)]
    pub async fn track_virtual_link(
        &self,
        _link_name: impl AsRef<str>,
        _hostname: impl AsRef<str>,
        _interval: Duration,
    ) -> Result<(), Error> {
        Err(Error::Kstat(anyhow!(
            "kstat metrics are not supported on this platform"
        )))
    }
}
