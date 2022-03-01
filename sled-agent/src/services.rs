// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for miscellaneous services managed by the sled.

use crate::illumos::running_zone::{InstalledZone, RunningZone};
use crate::illumos::vnic::VnicAllocator;
use crate::illumos::zone::AddressRequest;
use omicron_common::api::internal::sled_agent::{
    ServiceEnsureBody, ServiceRequest,
};
use slog::Logger;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot serialize TOML file: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("Cannot deserialize TOML file: {0}")]
    TomlDeserialize(#[from] toml::de::Error),

    #[error("Error accessing filesystem: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    RunningZone(#[from] crate::illumos::running_zone::Error),

    #[error("Services already configured for this Sled Agent")]
    ServicesAlreadyConfigured,
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        omicron_common::api::external::Error::InternalError {
            internal_message: err.to_string(),
        }
    }
}

fn services_config_path() -> PathBuf {
    Path::new(crate::OMICRON_CONFIG_PATH).join("services.toml")
}

/// Manages miscellaneous Sled-local services.
pub struct ServiceManager {
    log: Logger,
    zones: Mutex<Vec<RunningZone>>,
    vnic_allocator: VnicAllocator,
}

impl ServiceManager {
    /// Creates a service manager, which returns once all requested services
    /// have been started.
    pub async fn new(log: Logger) -> Result<Self, Error> {
        let mgr = Self {
            log,
            zones: Mutex::new(vec![]),
            vnic_allocator: VnicAllocator::new("Service"),
        };

        let config_path = services_config_path();
        if config_path.exists() {
            info!(
                &mgr.log,
                "Sled services found at {}; loading",
                config_path.to_string_lossy()
            );
            let cfg: ServiceEnsureBody = toml::from_str(
                &tokio::fs::read_to_string(&services_config_path()).await?,
            )?;
            let mut existing_zones = mgr.zones.lock().await;
            mgr.initialize_services_locked(&mut existing_zones, &cfg.services)
                .await?;
        } else {
            info!(
                &mgr.log,
                "No sled services found at {}",
                config_path.to_string_lossy()
            );
        }

        Ok(mgr)
    }

    // Populates `existing_zones` according to the requests in `services`.
    async fn initialize_services_locked(
        &self,
        existing_zones: &mut Vec<RunningZone>,
        services: &Vec<ServiceRequest>,
    ) -> Result<(), Error> {
        info!(self.log, "Ensuring services are initialized: {:?}", services);
        // TODO: As long as we ensure the requests don't overlap, we could
        // parallelize this request.
        for service in services {
            // Before we bother allocating anything for this request, check if
            // this service has already been created.
            if existing_zones.iter().any(|z| z.name() == service.name) {
                info!(self.log, "Service {} already exists", service.name);
                continue;
            } else {
                info!(self.log, "Service {} does not yet exist", service.name);
            }

            let installed_zone = InstalledZone::install(
                &self.log,
                &self.vnic_allocator,
                &service.name,
                /* unique_name= */ None,
                /* dataset= */ &[],
                /* devices= */ &[],
                /* vnics= */ vec![],
            )
            .await?;

            let running_zone = RunningZone::boot(installed_zone).await?;

            for addr in &service.addresses {
                info!(self.log, "Ensuring address {} exists", addr.to_string());
                let addr_request = AddressRequest::new_static(addr.ip(), None);
                running_zone.ensure_address(addr_request).await?;
                info!(
                    self.log,
                    "Ensuring address {} exists - OK",
                    addr.to_string()
                );
            }

            running_zone.run_cmd(&[
                crate::illumos::zone::SVCCFG,
                "import",
                &format!(
                    "/var/svc/manifest/site/{}/manifest.xml",
                    service.name
                ),
            ])?;

            running_zone.run_cmd(&[
                crate::illumos::zone::SVCADM,
                "enable",
                "-t",
                &format!("svc:/system/illumos/{}:default", service.name),
            ])?;

            existing_zones.push(running_zone);
        }
        Ok(())
    }

    /// Ensures that particular services should be initialized.
    ///
    /// These services will be instantiated by this function, will be recorded
    /// to a local file to ensure they start automatically on next boot.
    pub async fn ensure(
        &self,
        request: ServiceEnsureBody,
    ) -> Result<(), Error> {
        let mut existing_zones = self.zones.lock().await;
        let config_path = services_config_path();
        if config_path.exists() {
            let cfg: ServiceEnsureBody = toml::from_str(
                &tokio::fs::read_to_string(&services_config_path()).await?,
            )?;
            let known_services = cfg.services;

            let known_set: HashSet<&ServiceRequest> =
                HashSet::from_iter(known_services.iter());
            let requested_set = HashSet::from_iter(request.services.iter());

            if known_set != requested_set {
                // If the caller is requesting we instantiate a
                // zone that exists, but isn't what they're asking for, throw an
                // error.
                //
                // We may want to use a different mechanism for zone removal, in
                // the case of changing configurations, rather than just doing
                // that removal implicitly.
                warn!(
                    self.log,
                    "Cannot request services on this sled, differing configurations: {:?}",
                    known_set.symmetric_difference(&requested_set)
                );
                return Err(Error::ServicesAlreadyConfigured);
            }
        }

        self.initialize_services_locked(&mut existing_zones, &request.services)
            .await?;

        let serialized_services = toml::Value::try_from(&request)
            .expect("Cannot serialize service list");
        tokio::fs::write(
            &services_config_path(),
            toml::to_string(&serialized_services)?,
        )
        .await?;

        Ok(())
    }
}
