// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for miscellaneous services managed by the sled.

use crate::illumos::dladm::{Etherstub, EtherstubVnic};
use crate::illumos::running_zone::{InstalledZone, RunningZone};
use crate::illumos::vnic::VnicAllocator;
use crate::illumos::zone::AddressRequest;
use crate::params::{ServiceEnsureBody, ServiceRequest};
use crate::zone::Zones;
use omicron_common::address::{DNS_PORT, DNS_SERVER_PORT};
use slog::Logger;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::net::{IpAddr, Ipv6Addr};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot serialize TOML to file {path}: {err}")]
    TomlSerialize { path: PathBuf, err: toml::ser::Error },

    #[error("Cannot deserialize TOML from file {path}: {err}")]
    TomlDeserialize { path: PathBuf, err: toml::de::Error },

    #[error("I/O Error accessing {path}: {err}")]
    Io { path: PathBuf, err: std::io::Error },

    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: crate::illumos::running_zone::RunCommandError,
    },

    #[error("Failed to boot zone: {0}")]
    ZoneBoot(#[from] crate::illumos::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] crate::illumos::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] crate::illumos::running_zone::InstallZoneError),

    #[error("Failed to add GZ addresses: {message}: {err}")]
    GzAddress {
        message: String,
        err: crate::illumos::zone::EnsureGzAddressError,
    },

    #[error("Could not initialize service {service} as requested: {message}")]
    BadServiceRequest { service: String, message: String },

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

/// The default path to service configuration, if one is not
/// explicitly provided.
pub fn default_services_config_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH).join("services.toml")
}

/// Manages miscellaneous Sled-local services.
pub struct ServiceManager {
    log: Logger,
    config_path: Option<PathBuf>,
    zones: Mutex<Vec<RunningZone>>,
    vnic_allocator: VnicAllocator,
    underlay_vnic: EtherstubVnic,
    underlay_address: Ipv6Addr,
}

impl ServiceManager {
    /// Creates a service manager, which returns once all requested services
    /// have been started.
    ///
    /// Args:
    /// - `log`: The logger
    /// - `etherstub`: An etherstub on which to allocate VNICs.
    /// - `underlay_vnic`: The underlay's VNIC in the Global Zone.
    /// - `config_path`: An optional path to a configuration file to store
    /// the record of services. By default, [`default_services_config_path`]
    /// is used.
    pub async fn new(
        log: Logger,
        etherstub: Etherstub,
        underlay_vnic: EtherstubVnic,
        underlay_address: Ipv6Addr,
        config_path: Option<PathBuf>,
    ) -> Result<Self, Error> {
        debug!(log, "Creating new ServiceManager");
        let mgr = Self {
            log: log.new(o!("component" => "ServiceManager")),
            config_path,
            zones: Mutex::new(vec![]),
            vnic_allocator: VnicAllocator::new("Service", etherstub),
            underlay_vnic,
            underlay_address,
        };

        let config_path = mgr.services_config_path();
        if config_path.exists() {
            info!(
                &mgr.log,
                "Sled services found at {}; loading",
                config_path.to_string_lossy()
            );
            let cfg: ServiceEnsureBody = toml::from_str(
                &tokio::fs::read_to_string(&config_path).await.map_err(
                    |err| Error::Io { path: config_path.clone(), err },
                )?,
            )
            .map_err(|err| Error::TomlDeserialize {
                path: config_path.clone(),
                err,
            })?;
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

    // Returns either the path to the explicitly provided config path, or
    // chooses the default one.
    fn services_config_path(&self) -> PathBuf {
        if let Some(path) = &self.config_path {
            path.clone()
        } else {
            default_services_config_path()
        }
    }

    // Populates `existing_zones` according to the requests in `services`.
    //
    // At the point this function is invoked, IP addresses have already been
    // allocated (by either RSS or Nexus). However, this function explicitly
    // assigns such addresses to interfaces within zones.
    async fn initialize_services_locked(
        &self,
        existing_zones: &mut Vec<RunningZone>,
        services: &Vec<ServiceRequest>,
    ) -> Result<(), Error> {
        info!(self.log, "Ensuring services are initialized: {:?}", services);
        // TODO(https://github.com/oxidecomputer/omicron/issues/726):
        // As long as we ensure the requests don't overlap, we could
        // parallelize this request.
        for service in services {
            // Before we bother allocating anything for this request, check if
            // this service has already been created.
            let expected_zone_name =
                InstalledZone::get_zone_name(&service.name, None);
            if existing_zones.iter().any(|z| z.name() == expected_zone_name) {
                info!(self.log, "Service {} already exists", service.name);
                continue;
            } else {
                info!(self.log, "Service {} does not yet exist", service.name);
            }

            let installed_zone = InstalledZone::install(
                &self.log,
                &self.vnic_allocator,
                &service.name,
                // unique_name=
                None,
                // dataset=
                &[],
                // devices=
                &[],
                // vnics=
                vec![],
            )
            .await?;

            let running_zone = RunningZone::boot(installed_zone).await?;

            for addr in &service.addresses {
                info!(self.log, "Ensuring address {} exists", addr.to_string());
                let addr_request =
                    AddressRequest::new_static(IpAddr::V6(*addr), None);
                running_zone.ensure_address(addr_request).await?;
                info!(
                    self.log,
                    "Ensuring address {} exists - OK",
                    addr.to_string()
                );
            }

            info!(self.log, "GZ addresses: {:#?}", service.gz_addresses);
            for addr in &service.gz_addresses {
                info!(
                    self.log,
                    "Ensuring GZ address {} exists",
                    addr.to_string()
                );

                let addr_name = service.name.replace(&['-', '_'][..], "");
                Zones::ensure_has_global_zone_v6_address(
                    self.underlay_vnic.clone(),
                    *addr,
                    &addr_name,
                )
                .map_err(|err| Error::GzAddress {
                    message: format!(
                        "adding address on behalf of service '{}'",
                        service.name
                    ),
                    err,
                })?;
            }

            let gateway = if !service.gz_addresses.is_empty() {
                // If this service supplies its own GZ address, add a route.
                //
                // This is currently being used for the DNS service.
                //
                // TODO: consider limitng the number of GZ addresses which
                // can be supplied - now that we're actively using it, we
                // aren't really handling the "many GZ addresses" case, and it
                // doesn't seem necessary now.
                service.gz_addresses[0]
            } else {
                self.underlay_address
            };

            running_zone.add_default_route(gateway).await.map_err(|err| {
                Error::ZoneCommand { intent: "Adding Route".to_string(), err }
            })?;

            // TODO: Related to
            // https://github.com/oxidecomputer/omicron/pull/1124 , should we
            // avoid importing this manifest?
            debug!(self.log, "importing manifest");

            running_zone
                .run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "import",
                    &format!(
                        "/var/svc/manifest/site/{}/manifest.xml",
                        service.name
                    ),
                ])
                .map_err(|err| Error::ZoneCommand {
                    intent: "importing manifest".to_string(),
                    err,
                })?;

            let smf_name = format!("svc:/system/illumos/{}", service.name);
            let default_smf_name = format!("{}:default", smf_name);

            match service.name.as_str() {
                "internal-dns" => {
                    info!(self.log, "Setting up internal-dns service");
                    let address =
                        service.addresses.get(0).ok_or_else(|| {
                            Error::BadServiceRequest {
                                service: service.name.clone(),
                                message: "Not enough addresses".to_string(),
                            }
                        })?;
                    running_zone
                        .run_cmd(&[
                            crate::illumos::zone::SVCCFG,
                            "-s",
                            &smf_name,
                            "setprop",
                            &format!(
                                "config/server_address=[{}]:{}",
                                address, DNS_SERVER_PORT
                            ),
                        ])
                        .map_err(|err| Error::ZoneCommand {
                            intent: format!(
                                "Setting DNS server address [{}]:{}",
                                address, DNS_SERVER_PORT
                            ),
                            err,
                        })?;

                    running_zone
                        .run_cmd(&[
                            crate::illumos::zone::SVCCFG,
                            "-s",
                            &smf_name,
                            "setprop",
                            &format!(
                                "config/dns_address=[{}]:{}",
                                address, DNS_PORT
                            ),
                        ])
                        .map_err(|err| Error::ZoneCommand {
                            intent: format!(
                                "Setting DNS address [{}]:{}",
                                address, DNS_SERVER_PORT
                            ),
                            err,
                        })?;

                    // Refresh the manifest with the new properties we set,
                    // so they become "effective" properties when the service is enabled.
                    running_zone
                        .run_cmd(&[
                            crate::illumos::zone::SVCCFG,
                            "-s",
                            &default_smf_name,
                            "refresh",
                        ])
                        .map_err(|err| Error::ZoneCommand {
                            intent: format!(
                                "Refreshing DNS service config for {}",
                                default_smf_name
                            ),
                            err,
                        })?;
                }
                _ => {
                    info!(
                        self.log,
                        "Service name {} did not match", service.name
                    );
                }
            }

            debug!(self.log, "enabling service");

            running_zone
                .run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    &default_smf_name,
                ])
                .map_err(|err| Error::ZoneCommand {
                    intent: format!("Enable {} service", default_smf_name),
                    err,
                })?;

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
        let config_path = self.services_config_path();

        let services_to_initialize = {
            if config_path.exists() {
                let cfg: ServiceEnsureBody = toml::from_str(
                    &tokio::fs::read_to_string(&config_path).await.map_err(
                        |err| Error::Io { path: config_path.clone(), err },
                    )?,
                )
                .map_err(|err| Error::TomlDeserialize {
                    path: config_path.clone(),
                    err,
                })?;
                let known_services = cfg.services;

                let known_set: HashSet<&ServiceRequest> =
                    HashSet::from_iter(known_services.iter());
                let requested_set = HashSet::from_iter(request.services.iter());

                if !requested_set.is_superset(&known_set) {
                    // The caller may only request services additively.
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
                requested_set
                    .difference(&known_set)
                    .map(|s| (*s).clone())
                    .collect::<Vec<ServiceRequest>>()
            } else {
                request.services.clone()
            }
        };

        self.initialize_services_locked(
            &mut existing_zones,
            &services_to_initialize,
        )
        .await?;

        let serialized_services = toml::Value::try_from(&request)
            .expect("Cannot serialize service list");
        let services_str =
            toml::to_string(&serialized_services).map_err(|err| {
                Error::TomlSerialize { path: config_path.clone(), err }
            })?;
        tokio::fs::write(&config_path, services_str)
            .await
            .map_err(|err| Error::Io { path: config_path.clone(), err })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::illumos::{
        dladm::{Etherstub, MockDladm, ETHERSTUB_NAME, ETHERSTUB_VNIC_NAME},
        svc,
        zone::MockZones,
    };
    use std::os::unix::process::ExitStatusExt;

    const SVC_NAME: &str = "my_svc";
    const EXPECTED_ZONE_NAME: &str = "oxz_my_svc";

    // Returns the expectations for a new service to be created.
    fn expect_new_service() -> Vec<Box<dyn std::any::Any>> {
        // Create a VNIC
        let create_vnic_ctx = MockDladm::create_vnic_context();
        create_vnic_ctx.expect().return_once(
            |physical_link: &Etherstub, _, _, _| {
                assert_eq!(&physical_link.0, &ETHERSTUB_NAME);
                Ok(())
            },
        );
        // Install the Omicron Zone
        let install_ctx = MockZones::install_omicron_zone_context();
        install_ctx.expect().return_once(|_, name, _, _, _, _| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        // Boot the zone
        let boot_ctx = MockZones::boot_context();
        boot_ctx.expect().return_once(|name| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        // Wait for the networking service.
        let wait_ctx = svc::wait_for_service_context();
        wait_ctx.expect().return_once(|_, _| Ok(()));
        // Import the manifest, enable the service
        let execute_ctx = crate::illumos::execute_context();
        execute_ctx.expect().times(3).returning(|_| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: vec![],
                stderr: vec![],
            })
        });

        vec![
            Box::new(create_vnic_ctx),
            Box::new(install_ctx),
            Box::new(boot_ctx),
            Box::new(wait_ctx),
            Box::new(execute_ctx),
        ]
    }

    // Prepare to call "ensure" for a new service, then actually call "ensure".
    async fn ensure_new_service(mgr: &ServiceManager) {
        let _expectations = expect_new_service();

        mgr.ensure(ServiceEnsureBody {
            services: vec![ServiceRequest {
                name: SVC_NAME.to_string(),
                addresses: vec![],
                gz_addresses: vec![],
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to call "ensure" for a service which already exists. We should
    // return the service without actually installing a new zone.
    async fn ensure_existing_service(mgr: &ServiceManager) {
        mgr.ensure(ServiceEnsureBody {
            services: vec![ServiceRequest {
                name: SVC_NAME.to_string(),
                addresses: vec![],
                gz_addresses: vec![],
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to drop the service manager.
    //
    // This will shut down all allocated zones, and delete their
    // associated VNICs.
    fn drop_service_manager(mgr: ServiceManager) {
        let halt_ctx = MockZones::halt_and_remove_logged_context();
        halt_ctx.expect().returning(|_, name| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        let delete_vnic_ctx = MockDladm::delete_vnic_context();
        delete_vnic_ctx.expect().returning(|_| Ok(()));

        // Explicitly drop the servie manager
        drop(mgr);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_ensure_service");
        let log = logctx.log.clone();

        let config_dir = tempfile::TempDir::new().unwrap();
        let config = config_dir.path().join("services.toml");
        let mgr = ServiceManager::new(
            log,
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            Some(config),
        )
        .await
        .unwrap();

        ensure_new_service(&mgr).await;
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service_which_already_exists() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_ensure_service_which_already_exists",
        );
        let log = logctx.log.clone();

        let config_dir = tempfile::TempDir::new().unwrap();
        let config = config_dir.path().join("services.toml");
        let mgr = ServiceManager::new(
            log,
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            Some(config),
        )
        .await
        .unwrap();

        ensure_new_service(&mgr).await;
        ensure_existing_service(&mgr).await;
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_services_are_recreated_on_reboot() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_are_recreated_on_reboot",
        );

        let config_dir = tempfile::TempDir::new().unwrap();
        let config = config_dir.path().join("services.toml");

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            Some(config.clone()),
        )
        .await
        .unwrap();
        ensure_new_service(&mgr).await;
        drop_service_manager(mgr);

        // Before we re-create the service manager - notably, using the same
        // config file! - expect that a service gets initialized.
        let _expectations = expect_new_service();
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            Some(config.clone()),
        )
        .await
        .unwrap();
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_services_do_not_persist_without_config() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_do_not_persist_without_config",
        );

        let config_dir = tempfile::TempDir::new().unwrap();
        let config = config_dir.path().join("services.toml");

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            Some(config.clone()),
        )
        .await
        .unwrap();
        ensure_new_service(&mgr).await;
        drop_service_manager(mgr);

        // Next, delete the config. This means the service we just created will
        // not be remembered on the next initialization.
        std::fs::remove_file(&config).unwrap();

        // Observe that the old service is not re-initialized.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            Etherstub(ETHERSTUB_NAME.to_string()),
            EtherstubVnic(ETHERSTUB_VNIC_NAME.to_string()),
            Ipv6Addr::LOCALHOST,
            Some(config.clone()),
        )
        .await
        .unwrap();
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }
}
