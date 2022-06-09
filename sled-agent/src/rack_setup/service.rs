// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service implementation

use super::config::SetupServiceConfig as Config;
use crate::bootstrap::{
    discovery::PeerMonitorObserver,
    params::SledAgentRequest, rss_handle::BootstrapAgentHandle,
};
use crate::params::{DatasetEnsureBody, ServiceRequest, ServiceType};
use crate::rack_setup::plan::service::{
    PlanError as ServicePlanError,
    Plan as ServicePlan,
};
use crate::rack_setup::plan::sled::{
    PlanError as SledPlanError,
    Plan as SledPlan,
};
use internal_dns_client::names::{AAAA, SRV};
use nexus_client::{
    Client as NexusClient,
    Error as NexusError,
    types as NexusTypes,
};
use omicron_common::address::{get_sled_address, NEXUS_INTERNAL_PORT};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use serde::{Deserialize, Serialize};
use sled_agent_client::{
    Client as SledAgentClient,
    Error as SledAgentError,
    types as SledAgentTypes,
};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell};

// The minimum number of sleds to initialize the rack.
const MINIMUM_SLED_COUNT: usize = 1;

/// Describes errors which may occur while operating the setup service.
#[derive(Error, Debug)]
pub enum SetupServiceError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Cannot create plan for sled services: {0}")]
    ServicePlan(#[from] ServicePlanError),

    #[error("Cannot create plan for sled setup: {0}")]
    SledPlan(#[from] SledPlanError),

    #[error("Bad configuration for setting up rack: {0}")]
    BadConfig(String),

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] SledAgentError<SledAgentTypes::Error>),

    #[error("Error making HTTP request to Nexus: {0}")]
    NexusApi(#[from] NexusError<NexusTypes::Error>),

    #[error("Failed to monitor for peers: {0}")]
    PeerMonitor(#[from] tokio::sync::broadcast::error::RecvError),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(reqwest::Error),

    // XXX CLEAN UP
    #[error(transparent)]
    Dns(#[from] internal_dns_client::Error<internal_dns_client::types::Error>),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SledRequest {
    /// Datasets to be created.
    #[serde(default, rename = "dataset")]
    pub datasets: Vec<DatasetEnsureBody>,

    /// Services to be instantiated.
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceRequest>,

    /// DNS Services to be instantiated.
    #[serde(default, rename = "dns_service")]
    pub dns_services: Vec<ServiceRequest>,
}

// The workload / information allocated to a single sled.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct SledAllocation {
    initialization_request: SledAgentRequest,
    services_request: SledRequest,
}

/// The interface to the Rack Setup Service.
pub struct Service {
    handle: tokio::task::JoinHandle<Result<(), SetupServiceError>>,
}

impl Service {
    /// Creates a new rack setup service, which runs in a background task.
    ///
    /// Arguments:
    /// - `log`: The logger.
    /// - `config`: The config file, which is used to setup the rack.
    /// - `peer_monitor`: The mechanism by which the setup service discovers
    ///   bootstrap agents on nearby sleds.
    /// - `local_bootstrap_agent`: Communication channel by which we can send
    ///   commands to our local bootstrap-agent (e.g., to initialize sled
    ///   agents).
    pub(crate) fn new(
        log: Logger,
        config: Config,
        peer_monitor: PeerMonitorObserver,
        local_bootstrap_agent: BootstrapAgentHandle,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone(), peer_monitor);
            if let Err(e) = svc
                .inject_rack_setup_requests(&config, local_bootstrap_agent)
                .await
            {
                warn!(log, "RSS injection failed: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        });

        Service { handle }
    }

    /// Awaits the completion of the RSS service.
    pub async fn join(self) -> Result<(), SetupServiceError> {
        self.handle.await.expect("Rack Setup Service Task panicked")
    }
}

fn rss_completed_plan_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH)
        .join("rss-plan-completed.marker")
}

// Describes the options when awaiting for peers.
enum PeerExpectation {
    // Await a set of peers that matches this group of IPv6 addresses exactly.
    //
    // TODO: We currently don't deal with the case where:
    //
    // - RSS boots, sees some sleds, comes up with a plan.
    // - RSS reboots, sees a *different* set of sleds, and needs
    // to adjust the plan.
    //
    // This case is fairly tricky because some sleds may have
    // already received requests to initialize - modifying the
    // allocated subnets would be non-trivial.
    LoadOldPlan(HashSet<Ipv6Addr>),
    // Await any peers, as long as there are at least enough to make a new plan.
    CreateNewPlan(usize),
}

/// The implementation of the Rack Setup Service.
struct ServiceInner {
    log: Logger,
    peer_monitor: Mutex<PeerMonitorObserver>,
    dns_servers: OnceCell<internal_dns_client::multiclient::Updater>,
}

impl ServiceInner {
    fn new(log: Logger, peer_monitor: PeerMonitorObserver) -> Self {
        ServiceInner {
            log,
            peer_monitor: Mutex::new(peer_monitor),
            dns_servers: OnceCell::new(),
        }
    }

    async fn initialize_crdb(
        &self,
        sled_address: SocketAddrV6,
        datasets: &Vec<crate::params::DatasetEnsureBody>,
    ) -> Result<(), SetupServiceError> {
        if datasets.iter().any(|dataset| {
            !matches!(
                dataset.dataset_kind,
                crate::params::DatasetKind::CockroachDb { .. }
            )
        }) {
            return Err(SetupServiceError::BadConfig("RSS should only initialize CRDB services".into()));
        }

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

        info!(self.log, "sending dataset requests...");
        for dataset in datasets {
            let filesystem_put = || async {
                info!(self.log, "creating new filesystem: {:?}", dataset);
                client
                    .filesystem_put(&dataset.clone().into())
                    .await
                    .map_err(BackoffError::transient)?;
                Ok::<
                    (),
                    BackoffError<
                        SledAgentError<
                            SledAgentTypes::Error,
                        >,
                    >,
                >(())
            };
            let log_failure = |error, _| {
                warn!(self.log, "failed to create filesystem"; "error" => ?error);
            };
            retry_notify(
                internal_service_policy(),
                filesystem_put,
                log_failure,
            )
            .await?;
        }

        // Initialize DNS records for these datasets.
        //
        // CRDB is treated as a service, since they are interchangeable.

        let aaaa = datasets
            .iter()
            .map(|dataset| {
                (
                    AAAA::Zone(dataset.id),
                    dataset.address,
                )
            })
            .collect::<Vec<_>>();
        let srv_key = SRV::Service("cockroachdb".into());

        self.dns_servers
            .get()
            .expect("DNS servers must be initialized first")
            .insert_dns_records(&self.log, aaaa, srv_key)
            .await?;

        Ok(())
    }

    async fn initialize_services(
        &self,
        sled_address: SocketAddrV6,
        services: &Vec<ServiceRequest>,
    ) -> Result<(), SetupServiceError> {
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

        info!(self.log, "sending service requests...");
        let services_put = || async {
            info!(self.log, "initializing sled services: {:?}", services);
            client
                .services_put(&SledAgentTypes::ServiceEnsureBody {
                    services: services
                        .iter()
                        .map(|s| s.clone().into())
                        .collect(),
                })
                .await
                .map_err(BackoffError::transient)?;
            Ok::<
                (),
                BackoffError<
                    SledAgentError<SledAgentTypes::Error>,
                >,
            >(())
        };
        let log_failure = |error, _| {
            warn!(self.log, "failed to initialize services"; "error" => ?error);
        };
        retry_notify(internal_service_policy(), services_put, log_failure)
            .await?;


        // Initialize DNS records for the Nexus service.
        let services: Vec<_> = services.iter().filter(|svc| {
            matches!(svc.service_type, crate::params::ServiceType::Nexus { .. })
        }).collect();

        // Early-exit for non-Nexus case
        if services.is_empty() {
            return Ok(());
        }

        // Otherwise, insert DNS records for Nexus
        let aaaa = services
            .iter()
            .map(|service| {
                (
                    AAAA::Zone(service.id),
                    SocketAddrV6::new(
                        service.addresses[0],
                        NEXUS_INTERNAL_PORT,
                        0,
                        0,
                    )
                )
            })
            .collect::<Vec<_>>();
        let srv_key = SRV::Service("nexus".into());
        self.dns_servers
            .get()
            .expect("DNS servers must be initialized first")
            .insert_dns_records(&self.log, aaaa, srv_key)
            .await?;

        Ok(())
    }

    // Waits for sufficient neighbors to exist so the initial set of requests
    // can be send out.
    async fn wait_for_peers(
        &self,
        expectation: PeerExpectation,
    ) -> Result<Vec<Ipv6Addr>, SetupServiceError> {
        let mut peer_monitor = self.peer_monitor.lock().await;
        let (mut all_addrs, mut peer_rx) = peer_monitor.subscribe().await;
        all_addrs.insert(peer_monitor.our_address());

        loop {
            {
                match expectation {
                    PeerExpectation::LoadOldPlan(ref expected) => {
                        if all_addrs.is_superset(expected) {
                            return Ok(all_addrs
                                .into_iter()
                                .collect::<Vec<Ipv6Addr>>());
                        }
                        info!(self.log, "Waiting for a LoadOldPlan set of peers; not found yet.");
                    }
                    PeerExpectation::CreateNewPlan(wanted_peer_count) => {
                        if all_addrs.len() >= wanted_peer_count {
                            return Ok(all_addrs
                                .into_iter()
                                .collect::<Vec<Ipv6Addr>>());
                        }
                        info!(
                            self.log,
                            "Waiting for {} peers (currently have {})",
                            wanted_peer_count,
                            all_addrs.len(),
                        );
                    }
                }
            }

            info!(self.log, "Waiting for more peers");
            let new_peer = peer_rx.recv().await?;
            all_addrs.insert(new_peer);
        }
    }

    async fn handoff_to_nexus(
        &self,
        config: &Config,
        sled_plan: &SledPlan,
        service_plan: &ServicePlan,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Handing off control to Nexus");

        let resolver = internal_dns_client::multiclient::create_resolver(config.az_subnet())
            .expect("Failed to create DNS resolver");
        let response = resolver.lookup_ip(
            &SRV::Service("nexus".to_string()).to_string()
        ).await.expect("Failed to lookup IP");

        let nexus_address = response.iter()
            .next()
            .map(|addr| {
                SocketAddr::new(addr, NEXUS_INTERNAL_PORT)
            })
            .expect("no addresses returned from DNS resolver");
        info!(self.log, "Nexus address: {}", nexus_address.to_string());

        let nexus_client = NexusClient::new(
            &format!("http://{}", nexus_address),
            self.log.new(o!("component" => "NexusClient"))
        );

        // Ensure we can quickly look up "Sled Agent Address" -> "UUID of sled".
        //
        // We need the ID when passing info to Nexus.
        let mut id_map = HashMap::new();
        for (_, sled_request) in sled_plan.sleds.iter() {
            id_map.insert(get_sled_address(sled_request.subnet), sled_request.id);
        }

        // Convert all the information we have about services and datasets into
        // a format which can be processed by Nexus.
        let mut services: Vec<NexusTypes::ServicePutRequest> = vec![];
        let mut datasets: Vec<NexusTypes::DatasetCreateRequest> = vec![];
        for (addr, service_request) in service_plan.services.iter() {
            let sled_id = *id_map.get(addr)
                .expect("Sled address in service plan, but not sled plan");

            for svc in service_request.services.iter().chain(service_request.dns_services.iter()) {
                let kind = match svc.service_type {
                    ServiceType::Nexus { .. } => NexusTypes::ServiceKind::Nexus,
                    ServiceType::InternalDns { .. } => NexusTypes::ServiceKind::InternalDNS,
                    ServiceType::Oximeter => NexusTypes::ServiceKind::Oximeter,
                };

                services.push(
                    NexusTypes::ServicePutRequest {
                        service_id: svc.id,
                        sled_id,
                        // TODO: Should this be a vec, or a single value?
                        address: svc.addresses[0],
                        kind,
                    }
                )
            }

            for dataset in service_request.datasets.iter() {
                datasets.push(
                    NexusTypes::DatasetCreateRequest {
                        zpool_id: dataset.zpool_id,
                        dataset_id: dataset.id,
                        request: NexusTypes::DatasetPutRequest {
                            address: dataset.address.to_string(),
                            kind: dataset.dataset_kind.clone().into()
                        },
                    }
                )
            }
        }

        let request = NexusTypes::RackInitializationRequest {
            services,
            datasets,
        };

        let notify_nexus = || async {
            nexus_client.rack_initialization_complete(
                &sled_plan.rack_id,
                &request,
            )
            .await
            .map_err(BackoffError::transient)
        };
        let log_failure = |err, _| {
            info!(self.log, "Failed to handoff to nexus: {err}");
        };

        retry_notify(
            internal_service_policy(),
            notify_nexus,
            log_failure,
        ).await?;

        info!(self.log, "Handoff to Nexus is complete");
        Ok(())
    }

    // In lieu of having an operator send requests to all sleds via an
    // initialization service, the sled-agent configuration may allow for the
    // automated injection of setup requests from a sled.
    //
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
    //    on their respsective subnets, they can be queried to create an
    //    allocation plan for services.
    //
    // 4. SERVICE ALLOCATION PLAN EXECUTION. RSS requests that the services
    //    outlined in the aforementioned step are created.
    //
    // 5. MARKING SETUP COMPLETE. Once the RSS has successfully initialized the
    //    rack, a marker file is created at "rss_completed_plan_path()". This
    //    indicates that the plan executed successfully, and no work remains.
    async fn inject_rack_setup_requests(
        &self,
        config: &Config,
        local_bootstrap_agent: BootstrapAgentHandle,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);

        // Check if a previous RSS plan has completed successfully.
        //
        // If it has, the system should be up-and-running.
        let rss_completed_plan_path = rss_completed_plan_path();
        if rss_completed_plan_path.exists() {
            // TODO(https://github.com/oxidecomputer/omicron/issues/724): If the
            // running configuration doesn't match Config, we could try to
            // update things.
            info!(
                self.log,
                "RSS configuration looks like it has already been applied",
            );

            let sled_plan = SledPlan::load(&self.log).await?
                .expect("Sled plan should exist if completed marker exists");
            let service_plan = ServicePlan::load(&self.log).await?
                .expect("Service plan should exist if completed marker exists");
            self.handoff_to_nexus(
                &config,
                &sled_plan,
                &service_plan
            ).await?;

            return Ok(());
        } else {
            info!(self.log, "RSS configuration has not been fully applied yet",);
        }

        // Wait for either:
        // - All the peers to re-load an old plan (if one exists)
        // - Enough peers to create a new plan (if one does not exist)
        let maybe_sled_plan = SledPlan::load(&self.log).await?;
        let expectation = if let Some(plan) = &maybe_sled_plan {
            PeerExpectation::LoadOldPlan(plan.sleds.keys().map(|a| *a.ip()).collect())
        } else {
            PeerExpectation::CreateNewPlan(MINIMUM_SLED_COUNT)
        };
        let addrs = self.wait_for_peers(expectation).await?;
        info!(self.log, "Enough peers exist to enact RSS plan");

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
            SledPlan::create(&self.log, &config, addrs).await?
        };

        // Forward the sled initialization requests to our sled-agent.
        local_bootstrap_agent
            .initialize_sleds(
                plan.sleds.iter()
                    .map(|(bootstrap_addr, initialization_request)| {
                        (
                            *bootstrap_addr,
                            initialization_request.clone(),
                        )
                    })
                    .collect(),
            )
            .await
            .map_err(SetupServiceError::SledInitialization)?;

        let sled_addresses: Vec<_> = plan.sleds.iter()
            .map(|(_, initialization_request)| {
                get_sled_address(
                    initialization_request.subnet,
                )
            })
            .collect();

        // Now that sled agents have been initialized, we can create
        // a service allocation plan.
        let service_plan = if let Some(plan) = ServicePlan::load(&self.log).await? {
            plan
        } else {
            ServicePlan::create(&self.log, &config, &sled_addresses).await?
        };

        // Set up internal DNS services.
        futures::future::join_all(
            service_plan.services.iter()
                .filter(|(_, service_request)| {
                    // Only send requests to sleds that are supposed to be running
                    // DNS services.
                    !service_request.dns_services.is_empty()
                })
                .map(|(sled_address, services_request)| async move {
                    self.initialize_services(
                        *sled_address,
                        &services_request.dns_services,
                    )
                    .await?;
                    Ok(())
                }),
        )
        .await
        .into_iter()
        .collect::<Result<_, SetupServiceError>>()?;

        let dns_servers = internal_dns_client::multiclient::Updater::new(
            config.az_subnet(),
            self.log.new(o!("client" => "DNS")),
        );
        self.dns_servers
            .set(dns_servers)
            .map_err(|_| ())
            .expect("Already set DNS servers");

        // Issue the crdb initialization requests to all sleds.
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async move {
                self.initialize_crdb(
                    *sled_address,
                    &services_request.datasets,
                )
                .await?;
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<_, SetupServiceError>>()?;

        info!(self.log, "Finished setting up agents and datasets");

        // Issue service initialization requests.
        //
        // Note that this must happen *after* the dataset initialization,
        // to ensure that CockroachDB has been initialized before Nexus
        // starts.
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async move {
                // With the current implementation of "initialize_services",
                // we must provide the set of *all* services that should be
                // executing on a sled.
                //
                // This means re-requesting the DNS service, even if it is
                // already running - this is fine, however, as the receiving
                // sled agent doesn't modify the already-running service.
                let all_services = services_request
                    .services
                    .iter()
                    .chain(services_request.dns_services.iter())
                    .map(|s| s.clone())
                    .collect::<Vec<_>>();

                self.initialize_services(*sled_address, &all_services).await?;
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<()>, SetupServiceError>>()?;

        info!(self.log, "Finished setting up services");

        // Finally, make sure the configuration is saved so we don't inject
        // the requests on the next iteration.
        tokio::fs::File::create(&rss_completed_plan_path).await.map_err(
            |err| SetupServiceError::Io {
                message: format!(
                    "creating {rss_completed_plan_path:?}"
                ),
                err,
            },
        )?;

        // At this point, even if we reboot, we must not try to manage sleds,
        // services, or DNS records.

        self.handoff_to_nexus(
            &config,
            &plan,
            &service_plan
        ).await?;

        // TODO Questions to consider:
        // - What if a sled comes online *right after* this setup? How does
        // it get a /64?

        Ok(())
    }
}
