// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service (RSS) implementation
//!
//! RSS triggers the initialization of:
//! - Sled Agents (giving them underlay addresses)
//! - Trust Quorum (coordinating between Sled Agents)
//! - Services (such as internal DNS, CRDB, Nexus)
//! - DNS records for those services
//! - Handoff to Nexus, for control of Control Plane management
//!
//! # Phases and Configuration Files
//!
//! Rack setup occurs in distinct phases which are denoted by the prescence of
//! configuration files.
//!
//! - /pool/int/UUID/config/rss-sled-plan.json (Sled Plan)
//! - /pool/int/UUID/config/rss-service-plan.json (Service Plan)
//! - /pool/int/UUID/config/rss-plan-completed.marker (Plan Execution Complete)
//!
//! ## Sled Plan
//!
//! RSS should start as a service executing on a Sidecar-attached Gimlet
//! (Scrimlet). It must communicate with other sleds on the bootstrap network to
//! discover neighbors. RSS uses the bootstrap network to identify peers, assign
//! them subnets and UUIDs, and initialize a trust quorum. Once RSS decides
//! these values it commits them to a local file as the "Sled Plan", before
//! sending requests.
//!
//! As a result, restarting RSS should result in retransmission of the same
//! values, as long as the same configuration file is used.
//!
//! ## Service Plan
//!
//! After the trust quorum is established and Sled Agents are executing across
//! the rack, RSS can make the call on "what services should run where",
//! ensuring the minimal set of services necessary to execute Nexus are
//! operational. Critically, these include:
//! - Internal DNS: Necessary so internal services can discover each other
//! - CockroachDB: Necessary for Nexus to operate
//! - Nexus itself
//!
//! Once the distribution of these services is decided (which sled should run
//! what service? On what zpools should CockroachDB be provisioned?) it is
//! committed to the Service Plan, and executed.
//!
//! ## Execution Complete
//!
//! Once the both the Sled and Service plans have finished execution, handoff of
//! control to Nexus can occur. <https://rfd.shared.oxide.computer/rfd/0278>
//! covers this in more detail, but in short, RSS creates a "marker" file after
//! completing execution, and unconditionally calls the "handoff to Nexus" API
//! thereafter.

use super::config::SetupServiceConfig as Config;
use crate::bootstrap::config::BOOTSTRAP_AGENT_HTTP_PORT;
use crate::bootstrap::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody, EarlyNetworkSetup,
    EarlyNetworkSetupError,
};
use crate::bootstrap::params::BootstrapAddressDiscovery;
use crate::bootstrap::params::StartSledAgentRequest;
use crate::bootstrap::rss_handle::BootstrapAgentHandle;
use crate::nexus::d2n_params;
use crate::params::{
    AutonomousServiceOnlyError, ServiceType, ServiceZoneRequest,
    ServiceZoneService, TimeSync, ZoneType,
};
use crate::rack_setup::plan::service::{
    Plan as ServicePlan, PlanError as ServicePlanError,
};
use crate::rack_setup::plan::sled::{
    Plan as SledPlan, PlanError as SledPlanError,
};
use crate::storage_manager::StorageResources;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
use internal_dns::resolver::{DnsError, Resolver as DnsResolver};
use internal_dns::ServiceName;
use nexus_client::{
    types as NexusTypes, Client as NexusClient, Error as NexusError,
};
use omicron_common::address::get_sled_address;
use omicron_common::api::internal::shared::ExternalPortDiscovery;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use omicron_common::ledger::{self, Ledger, Ledgerable};
use serde::{Deserialize, Serialize};
use sled_agent_client::{
    types as SledAgentTypes, Client as SledAgentClient, Error as SledAgentError,
};
use sled_hardware::underlay::BootstrapInterface;
use slog::Logger;
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::net::{Ipv6Addr, SocketAddrV6};
use thiserror::Error;

/// Describes errors which may occur while operating the setup service.
#[derive(Error, Debug)]
pub enum SetupServiceError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] ledger::Error),

    #[error("Cannot create plan for sled services: {0}")]
    ServicePlan(#[from] ServicePlanError),

    #[error("Cannot create plan for sled setup: {0}")]
    SledPlan(#[from] SledPlanError),

    #[error("Bad configuration for setting up rack: {0}")]
    BadConfig(String),

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),

    #[error("Error resetting sled: {0}")]
    SledReset(String),

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] SledAgentError<SledAgentTypes::Error>),

    #[error("Error making HTTP request to Nexus: {0}")]
    NexusApi(#[from] NexusError<NexusTypes::Error>),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Failed to monitor for peers: {0}")]
    PeerMonitor(#[from] tokio::sync::broadcast::error::RecvError),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(reqwest::Error),

    #[error("Failed to access DNS servers: {0}")]
    Dns(#[from] DnsError),

    #[error("Error during request to Dendrite: {0}")]
    Dendrite(String),

    #[error("Error during DNS lookup: {0}")]
    DnsResolver(#[from] internal_dns::resolver::ResolveError),

    #[error("Bootstore error: {0}")]
    Bootstore(#[from] bootstore::NodeRequestError),

    // We used transparent, because `EarlyNetworkSetupError` contains a subset
    // of error variants already in this type
    #[error(transparent)]
    EarlyNetworkSetup(#[from] EarlyNetworkSetupError),
}

// The workload / information allocated to a single sled.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct SledAllocation {
    initialization_request: StartSledAgentRequest,
}

/// The interface to the Rack Setup Service.
pub struct RackSetupService {
    handle: tokio::task::JoinHandle<Result<(), SetupServiceError>>,
}

impl RackSetupService {
    /// Creates a new rack setup service, which runs in a background task.
    ///
    /// Arguments:
    /// - `log`: The logger.
    /// - `config`: The config file, which is used to setup the rack.
    /// - `storage_resources`: All the disks and zpools managed by this sled
    /// - `local_bootstrap_agent`: Communication channel by which we can send
    ///   commands to our local bootstrap-agent (e.g., to initialize sled
    /// - `bootstore` - A handle to call bootstore APIs
    ///   agents).
    pub(crate) fn new(
        log: Logger,
        config: Config,
        storage_resources: StorageResources,
        local_bootstrap_agent: BootstrapAgentHandle,
        bootstore: bootstore::NodeHandle,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone());
            if let Err(e) = svc
                .run(
                    &config,
                    &storage_resources,
                    local_bootstrap_agent,
                    bootstore,
                )
                .await
            {
                warn!(log, "RSS injection failed: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        });

        RackSetupService { handle }
    }

    pub(crate) fn new_reset_rack(
        log: Logger,
        local_bootstrap_agent: BootstrapAgentHandle,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone());
            if let Err(e) = svc.reset(local_bootstrap_agent).await {
                warn!(log, "RSS rack reset failed: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        });

        RackSetupService { handle }
    }

    /// Awaits the completion of the RSS service.
    pub async fn join(self) -> Result<(), SetupServiceError> {
        self.handle.await.expect("Rack Setup Service Task panicked")
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct RssCompleteMarker {}

impl Ledgerable for RssCompleteMarker {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}
const RSS_COMPLETED_FILENAME: &str = "rss-plan-completed.marker";

/// The implementation of the Rack Setup Service.
struct ServiceInner {
    log: Logger,
}

impl ServiceInner {
    fn new(log: Logger) -> Self {
        ServiceInner { log }
    }

    async fn initialize_services_on_sled(
        &self,
        sled_address: SocketAddrV6,
        services: &Vec<ServiceZoneRequest>,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .build()
            .map_err(SetupServiceError::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            self.log.new(o!("SledAgentClient" => sled_address.to_string())),
        );

        let services = services
            .iter()
            .map(|s| s.clone().try_into())
            .collect::<Result<Vec<_>, AutonomousServiceOnlyError>>()
            .map_err(|err| {
                SetupServiceError::SledInitialization(err.to_string())
            })?;

        info!(self.log, "sending service requests...");
        let services_put = || async {
            info!(self.log, "initializing sled services: {:?}", services);
            client
                .services_put(&SledAgentTypes::ServiceEnsureBody {
                    services: services.clone(),
                })
                .await
                .map_err(BackoffError::transient)?;
            Ok::<(), BackoffError<SledAgentError<SledAgentTypes::Error>>>(())
        };
        let log_failure = |error, delay| {
            warn!(
                self.log,
                "failed to initialize services";
                "error" => ?error,
                "retry_after" => ?delay,
            );
        };
        retry_notify(
            retry_policy_internal_service_aggressive(),
            services_put,
            log_failure,
        )
        .await?;

        Ok(())
    }

    // Ensure that all services of a particular type are running.
    //
    // This is useful in a rack-setup context, where initial boot ordering
    // can matter for first-time-setup.
    //
    // Note that after first-time setup, the initialization order of
    // services should not matter.
    async fn ensure_all_services_of_type(
        &self,
        service_plan: &ServicePlan,
        zone_types: &HashSet<ZoneType>,
    ) -> Result<(), SetupServiceError> {
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async move {
                let services: Vec<_> = services_request
                    .services
                    .iter()
                    .filter_map(|service| {
                        if zone_types.contains(&service.zone_type) {
                            Some(service.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !services.is_empty() {
                    self.initialize_services_on_sled(*sled_address, &services)
                        .await?;
                }
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<_, SetupServiceError>>()?;
        Ok(())
    }

    // Configure the internal DNS servers with the initial DNS data
    async fn initialize_internal_dns_records(
        &self,
        service_plan: &ServicePlan,
    ) -> Result<(), SetupServiceError> {
        let log = &self.log;

        // Determine the list of DNS servers that are supposed to exist based on
        // the service plan that has just been deployed.
        let dns_server_ips =
            // iterate sleds
            service_plan.services.iter().filter_map(
                |(_, services_request)| {
                    // iterate services for this sled
                    let dns_addrs: Vec<SocketAddrV6> = services_request
                        .services
                        .iter()
                        .filter_map(|service| {
                            match &service.services[0] {
                                ServiceZoneService {
                                    details: ServiceType::InternalDns { http_address, .. },
                                    ..
                                } => {
                                    Some(*http_address)
                                },
                                _ => None,
                            }
                        })
                        .collect();
                    if dns_addrs.len() > 0 {
                        Some(dns_addrs)
                    } else {
                        None
                    }
                }
            )
            .flatten()
            .collect::<Vec<SocketAddrV6>>();

        let dns_config = &service_plan.dns_config;
        for ip_addr in dns_server_ips {
            let log = log.new(o!("dns_config_addr" => ip_addr.to_string()));
            info!(log, "Configuring DNS server");
            let dns_config_client = dns_service_client::Client::new(
                &format!("http://{}", ip_addr),
                log.clone(),
            );

            let do_update = || async {
                let result = dns_config_client.dns_config_put(dns_config).await;
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        if dns_service_client::is_retryable(&e) {
                            Err(BackoffError::transient(e))
                        } else {
                            Err(BackoffError::permanent(e))
                        }
                    }
                }
            };
            let log_failure = move |error, duration| {
                warn!(
                    log,
                    "failed to write DNS configuration (will retry in {:?})",
                    duration;
                    "error_message" => #%error
                );
            };

            retry_notify(
                retry_policy_internal_service_aggressive(),
                do_update,
                log_failure,
            )
            .await?;
        }

        info!(log, "Configured all DNS servers");
        Ok(())
    }

    async fn sled_timesync(
        &self,
        sled_address: &SocketAddrV6,
    ) -> Result<TimeSync, SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(SetupServiceError::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            self.log.new(o!("SledAgentClient" => sled_address.to_string())),
        );

        info!(
            self.log,
            "Checking time synchronization for {}...", sled_address
        );

        let ts = client.timesync_get().await?.into_inner();
        Ok(TimeSync {
            sync: ts.sync,
            ref_id: ts.ref_id,
            ip_addr: ts.ip_addr,
            stratum: ts.stratum,
            ref_time: ts.ref_time,
            correction: ts.correction,
        })
    }

    async fn wait_for_timesync(
        &self,
        sled_addresses: &Vec<SocketAddrV6>,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Waiting for rack time synchronization");

        let timesync_wait = || async {
            let mut synced_peers = 0;
            let mut sync = true;

            for sled_address in sled_addresses {
                if let Ok(ts) = self.sled_timesync(sled_address).await {
                    info!(self.log, "Timesync for {} {:?}", sled_address, ts);
                    if !ts.sync {
                        sync = false;
                    } else {
                        synced_peers += 1;
                    }
                } else {
                    sync = false;
                }
            }

            if sync {
                Ok(())
            } else {
                Err(BackoffError::transient(format!(
                    "Time is synchronized on {}/{} sleds",
                    synced_peers,
                    sled_addresses.len()
                )))
            }
        };
        let log_failure = |error, _| {
            warn!(self.log, "Time is not yet synchronized"; "error" => ?error);
        };

        retry_notify(
            retry_policy_internal_service_aggressive(),
            timesync_wait,
            log_failure,
        )
        // `retry_policy_internal_service_aggressive()` retries indefinitely on
        // transient errors (the only kind we produce), allowing us to
        // `.unwrap()` without panicking
        .await
        .unwrap();

        Ok(())
    }

    async fn handoff_to_nexus(
        &self,
        config: &Config,
        sled_plan: &SledPlan,
        service_plan: &ServicePlan,
        port_discovery_mode: ExternalPortDiscovery,
        nexus_address: SocketAddrV6,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Handing off control to Nexus");

        info!(self.log, "Nexus address: {}", nexus_address.to_string());

        let nexus_client = NexusClient::new(
            &format!("http://{}", nexus_address),
            self.log.new(o!("component" => "NexusClient")),
        );

        // Ensure we can quickly look up "Sled Agent Address" -> "UUID of sled".
        //
        // We need the ID when passing info to Nexus.
        let mut id_map = HashMap::new();
        for (_, sled_request) in sled_plan.sleds.iter() {
            id_map
                .insert(get_sled_address(sled_request.subnet), sled_request.id);
        }

        // Convert all the information we have about services and datasets into
        // a format which can be processed by Nexus.
        let mut services: Vec<NexusTypes::ServicePutRequest> = vec![];
        let mut datasets: Vec<NexusTypes::DatasetCreateRequest> = vec![];
        for (addr, service_request) in service_plan.services.iter() {
            let sled_id = *id_map
                .get(addr)
                .expect("Sled address in service plan, but not sled plan");

            for zone in &service_request.services {
                services.extend(zone.into_nexus_service_req(sled_id).map_err(
                    |err| SetupServiceError::BadConfig(err.to_string()),
                )?);
            }

            for service in service_request.services.iter() {
                if let Some(dataset) = &service.dataset {
                    datasets.push(NexusTypes::DatasetCreateRequest {
                        zpool_id: dataset.name.pool().id(),
                        dataset_id: dataset.id,
                        request: NexusTypes::DatasetPutRequest {
                            address: dataset.service_address.to_string(),
                            kind: dataset.name.dataset().clone().into(),
                        },
                    })
                }
            }
        }
        let internal_services_ip_pool_ranges = config
            .internal_services_ip_pool_ranges
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();

        let rack_network_config = match &config.rack_network_config {
            Some(config) => {
                let value = NexusTypes::RackNetworkConfigV1 {
                    rack_subnet: config.rack_subnet,
                    infra_ip_first: config.infra_ip_first,
                    infra_ip_last: config.infra_ip_last,
                    ports: config
                        .ports
                        .iter()
                        .map(|config| NexusTypes::PortConfigV1 {
                            port: config.port.clone(),
                            routes: config
                                .routes
                                .iter()
                                .map(|r| NexusTypes::RouteConfig {
                                    destination: r.destination,
                                    nexthop: r.nexthop,
                                    vid: r.vid,
                                })
                                .collect(),
                            addresses: config.addresses.clone(),
                            switch: config.switch.into(),
                            uplink_port_speed: config
                                .uplink_port_speed
                                .clone()
                                .into(),
                            uplink_port_fec: config
                                .uplink_port_fec
                                .clone()
                                .into(),
                            bgp_peers: config
                                .bgp_peers
                                .iter()
                                .map(|b| NexusTypes::BgpPeerConfig {
                                    addr: b.addr,
                                    asn: b.asn,
                                    port: b.port.clone(),
                                })
                                .collect(),
                        })
                        .collect(),
                    bgp: config
                        .bgp
                        .iter()
                        .map(|config| NexusTypes::BgpConfig {
                            asn: config.asn,
                            originate: config.originate.clone(),
                        })
                        .collect(),
                };
                Some(value)
            }
            None => None,
        };

        info!(self.log, "rack_network_config: {:#?}", rack_network_config);

        let request = NexusTypes::RackInitializationRequest {
            services,
            datasets,
            internal_services_ip_pool_ranges,
            certs: config.external_certificates.clone(),
            internal_dns_zone_config: d2n_params(&service_plan.dns_config),
            external_dns_zone_name: config.external_dns_zone_name.clone(),
            recovery_silo: config.recovery_silo.clone(),
            rack_network_config,
            external_port_count: port_discovery_mode.into(),
        };

        let notify_nexus = || async {
            nexus_client
                .rack_initialization_complete(&sled_plan.rack_id, &request)
                .await
                .map_err(BackoffError::transient)
        };
        let log_failure = |err, _| {
            info!(self.log, "Failed to handoff to nexus: {err}");
        };

        retry_notify(
            retry_policy_internal_service_aggressive(),
            notify_nexus,
            log_failure,
        )
        .await?;

        info!(self.log, "Handoff to Nexus is complete");
        Ok(())
    }

    async fn reset(
        &self,
        local_bootstrap_agent: BootstrapAgentHandle,
    ) -> Result<(), SetupServiceError> {
        // Gather all peer addresses that we can currently see on the bootstrap
        // network.
        let ddm_admin_client = DdmAdminClient::localhost(&self.log)?;
        let peer_addrs = ddm_admin_client
            .derive_bootstrap_addrs_from_prefixes(&[
                BootstrapInterface::GlobalZone,
            ])
            .await?;
        let our_bootstrap_address = local_bootstrap_agent.our_address();
        let all_addrs = peer_addrs
            .chain(iter::once(our_bootstrap_address))
            .map(|addr| {
                SocketAddrV6::new(addr, BOOTSTRAP_AGENT_HTTP_PORT, 0, 0)
            })
            .collect::<Vec<_>>();

        local_bootstrap_agent
            .reset_sleds(all_addrs)
            .await
            .map_err(SetupServiceError::SledReset)?;

        Ok(())
    }

    async fn initialize_cockroach(
        &self,
        service_plan: &ServicePlan,
    ) -> Result<(), SetupServiceError> {
        // Now that datasets and zones have started for CockroachDB,
        // perform one-time initialization of the cluster.
        let sled_address =
            service_plan
                .services
                .iter()
                .find_map(|(sled_address, sled_request)| {
                    if sled_request.services.iter().any(|service| {
                        service.zone_type == ZoneType::CockroachDb
                    }) {
                        Some(sled_address)
                    } else {
                        None
                    }
                })
                .expect("Should not create service plans without CockroachDb");
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .build()
            .map_err(SetupServiceError::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            self.log.new(o!("SledAgentClient" => sled_address.to_string())),
        );
        let initialize_db = || async {
            client.cockroachdb_init().await.map_err(BackoffError::transient)?;
            Ok::<(), BackoffError<SledAgentError<SledAgentTypes::Error>>>(())
        };
        let log_failure = |error, delay| {
            warn!(
                self.log,
                "Failed to initialize CockroachDB";
                "error" => ?error,
                "retry_after" => ?delay
            );
        };
        retry_notify(
            retry_policy_internal_service_aggressive(),
            initialize_db,
            log_failure,
        )
        .await
        .unwrap();
        Ok(())
    }

    // This method has a few distinct phases, identified by files in durable
    // storage:
    //
    // 1. SLED ALLOCATION PLAN CREATION. When the RSS starts up for the first
    //    time, it creates an allocation plan to provision subnets to an initial
    //    set of sleds.
    //
    // 2. SLED ALLOCATION PLAN EXECUTION. The RSS then carries out this plan, making
    //    requests to the sleds enumerated within the "allocation plan".
    //
    // 3. SERVICE ALLOCATION PLAN CREATION. Now that Sled Agents are executing
    //    on their respective subnets, they can be queried to create an
    //    allocation plan for services.
    //
    // 4. SERVICE ALLOCATION PLAN EXECUTION. RSS requests that the services
    //    outlined in the aforementioned step are created.
    //
    // 5. MARKING SETUP COMPLETE. Once the RSS has successfully initialized the
    //    rack, a marker file is created at "rss_completed_marker_path()". This
    //    indicates that the plan executed successfully, and no work remains.
    async fn run(
        &self,
        config: &Config,
        storage_resources: &StorageResources,
        local_bootstrap_agent: BootstrapAgentHandle,
        bootstore: bootstore::NodeHandle,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);

        let resolver = DnsResolver::new_from_subnet(
            self.log.new(o!("component" => "DnsResolver")),
            config.az_subnet(),
        )?;

        let marker_paths: Vec<Utf8PathBuf> = storage_resources
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_COMPLETED_FILENAME))
            .collect();

        let ledger =
            Ledger::<RssCompleteMarker>::new(&self.log, marker_paths.clone())
                .await;

        // Check if a previous RSS plan has completed successfully.
        //
        // If it has, the system should be up-and-running.
        if ledger.is_some() {
            // TODO(https://github.com/oxidecomputer/omicron/issues/724): If the
            // running configuration doesn't match Config, we could try to
            // update things.
            info!(
                self.log,
                "RSS configuration looks like it has already been applied",
            );

            let sled_plan = SledPlan::load(&self.log, storage_resources)
                .await?
                .expect("Sled plan should exist if completed marker exists");
            if &sled_plan.config != config {
                return Err(SetupServiceError::BadConfig(
                    "Configuration changed".to_string(),
                ));
            }
            let service_plan = ServicePlan::load(&self.log, storage_resources)
                .await?
                .expect("Service plan should exist if completed marker exists");

            let switch_mgmt_addrs = EarlyNetworkSetup::new(&self.log)
                .lookup_switch_zone_underlay_addrs(&resolver)
                .await;

            let nexus_address =
                resolver.lookup_socket_v6(ServiceName::Nexus).await?;

            self.handoff_to_nexus(
                &config,
                &sled_plan,
                &service_plan,
                ExternalPortDiscovery::Auto(switch_mgmt_addrs),
                nexus_address,
            )
            .await?;
            return Ok(());
        } else {
            info!(self.log, "RSS configuration has not been fully applied yet");
        }

        // Wait for either:
        // - All the peers to re-load an old plan (if one exists)
        // - Enough peers to create a new plan (if one does not exist)
        let bootstrap_addrs = match &config.bootstrap_discovery {
            BootstrapAddressDiscovery::OnlyOurs => {
                HashSet::from([local_bootstrap_agent.our_address()])
            }
            BootstrapAddressDiscovery::OnlyThese { addrs } => addrs.clone(),
        };
        let maybe_sled_plan =
            SledPlan::load(&self.log, storage_resources).await?;
        if let Some(plan) = &maybe_sled_plan {
            let stored_peers: HashSet<Ipv6Addr> =
                plan.sleds.keys().map(|a| *a.ip()).collect();
            if stored_peers != bootstrap_addrs {
                let e = concat!(
                    "Set of sleds requested does not match those in",
                    " existing sled plan"
                );
                return Err(SetupServiceError::BadConfig(e.to_string()));
            }
        }
        if bootstrap_addrs.is_empty() {
            return Err(SetupServiceError::BadConfig(
                "Must request at least one peer".to_string(),
            ));
        }

        // If we created a plan, reuse it. Otherwise, create a new plan.
        //
        // NOTE: This is a "point-of-no-return" -- before sending any requests
        // to neighboring sleds, the plan must be recorded to durable storage.
        // This way, if the RSS power-cycles, it can idempotently provide the
        // same subnets to the same sleds.
        let plan = if let Some(plan) = maybe_sled_plan {
            info!(self.log, "Re-using existing allocation plan");
            plan
        } else {
            info!(self.log, "Creating new allocation plan");
            SledPlan::create(
                &self.log,
                config,
                &storage_resources,
                bootstrap_addrs,
                config.trust_quorum_peers.is_some(),
            )
            .await?
        };
        let config = &plan.config;

        // Initialize the trust quorum if there are peers configured.
        if let Some(peers) = &config.trust_quorum_peers {
            let initial_membership: BTreeSet<_> =
                peers.iter().cloned().collect();
            bootstore
                .init_rack(plan.rack_id.into(), initial_membership)
                .await?;
        }

        // Save the relevant network config in the bootstore. We want this to
        // happen before we `initialize_sleds` so each scrimlet (including us)
        // can use its normal boot path of "read network config for our switch
        // from the bootstore".
        let early_network_config = EarlyNetworkConfig {
            generation: 1,
            schema_version: 1,
            body: EarlyNetworkConfigBody {
                ntp_servers: config.ntp_servers.clone(),
                rack_network_config: config.rack_network_config.clone(),
            },
        };
        info!(self.log, "Writing Rack Network Configuration to bootstore");
        bootstore.update_network_config(early_network_config.into()).await?;

        // Forward the sled initialization requests to our sled-agent.
        local_bootstrap_agent
            .initialize_sleds(
                plan.sleds
                    .iter()
                    .map(move |(bootstrap_addr, initialization_request)| {
                        (*bootstrap_addr, initialization_request.clone())
                    })
                    .collect(),
            )
            .await
            .map_err(SetupServiceError::SledInitialization)?;

        // Now that sled agents have been initialized, we can create
        // a service allocation plan.
        let sled_addresses: Vec<_> = plan
            .sleds
            .values()
            .map(|initialization_request| {
                get_sled_address(initialization_request.subnet)
            })
            .collect();
        let service_plan = if let Some(plan) =
            ServicePlan::load(&self.log, storage_resources).await?
        {
            plan
        } else {
            ServicePlan::create(
                &self.log,
                &config,
                &storage_resources,
                &plan.sleds,
            )
            .await?
        };

        // Set up internal DNS services first and write the initial
        // DNS configuration to the internal DNS servers.
        let mut zone_types = HashSet::new();
        zone_types.insert(ZoneType::InternalDns);
        self.ensure_all_services_of_type(&service_plan, &zone_types).await?;
        self.initialize_internal_dns_records(&service_plan).await?;

        // Ask MGS in each switch zone which switch it is.
        let switch_mgmt_addrs = EarlyNetworkSetup::new(&self.log)
            .lookup_switch_zone_underlay_addrs(&resolver)
            .await;

        // Next start up the NTP services.
        // Note we also specify internal DNS services again because it
        // can ony be additive.
        zone_types.insert(ZoneType::Ntp);
        self.ensure_all_services_of_type(&service_plan, &zone_types).await?;

        // Wait until time is synchronized on all sleds before proceeding.
        self.wait_for_timesync(&sled_addresses).await?;

        info!(self.log, "Finished setting up Internal DNS and NTP");

        // Wait until Cockroach has been initialized before running Nexus.
        zone_types.insert(ZoneType::CockroachDb);
        self.ensure_all_services_of_type(&service_plan, &zone_types).await?;

        // Now that datasets and zones have started for CockroachDB,
        // perform one-time initialization of the cluster.
        self.initialize_cockroach(&service_plan).await?;

        // Issue service initialization requests.
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async move {
                // With the current implementation of "initialize_services_on_sled",
                // we must provide the set of *all* services that should be
                // executing on a sled.
                //
                // This means re-requesting the DNS and NTP services, even if
                // they are already running - this is fine, however, as the
                // receiving sled agent doesn't modify the already-running
                // service.
                self.initialize_services_on_sled(
                    *sled_address,
                    &services_request.services,
                )
                .await?;
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<()>, SetupServiceError>>()?;

        info!(self.log, "Finished setting up services");

        // Finally, mark that we've completed executing the plans.
        let mut ledger = Ledger::<RssCompleteMarker>::new_with(
            &self.log,
            marker_paths.clone(),
            RssCompleteMarker::default(),
        );
        ledger.commit().await?;

        let nexus_address =
            resolver.lookup_socket_v6(ServiceName::Nexus).await?;

        // At this point, even if we reboot, we must not try to manage sleds,
        // services, or DNS records.
        self.handoff_to_nexus(
            &config,
            &plan,
            &service_plan,
            ExternalPortDiscovery::Auto(switch_mgmt_addrs),
            nexus_address,
        )
        .await?;

        // TODO Questions to consider:
        // - What if a sled comes online *right after* this setup? How does
        // it get a /64?

        Ok(())
    }
}
