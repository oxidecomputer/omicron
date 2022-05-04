// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service implementation

use super::config::{SetupServiceConfig as Config, SledRequest};
use crate::bootstrap::{
    client as bootstrap_agent_client, config::BOOTSTRAP_AGENT_PORT,
    discovery::PeerMonitorObserver, params::SledAgentRequest,
};
use crate::params::ServiceRequest;
use omicron_common::address::{get_sled_address, ReservedRackSubnet};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use thiserror::Error;
use tokio::sync::Mutex;

/// Describes errors which may occur while operating the setup service.
#[derive(Error, Debug)]
pub enum SetupServiceError {
    #[error("Error accessing filesystem: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error making HTTP request to Bootstrap Agent: {0}")]
    BootstrapApi(
        #[from]
        bootstrap_agent_client::Error<bootstrap_agent_client::types::Error>,
    ),

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] sled_agent_client::Error<sled_agent_client::types::Error>),

    #[error("Cannot deserialize TOML file")]
    Toml(#[from] toml::de::Error),

    #[error("Failed to monitor for peers: {0}")]
    PeerMonitor(#[from] tokio::sync::broadcast::error::RecvError),

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("Configuration changed")]
    Configuration,
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
    pub fn new(
        log: Logger,
        config: Config,
        peer_monitor: PeerMonitorObserver,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone(), peer_monitor);
            if let Err(e) = svc.inject_rack_setup_requests(&config).await {
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

fn rss_plan_path() -> std::path::PathBuf {
    std::path::Path::new(omicron_common::OMICRON_CONFIG_PATH)
        .join("rss-plan.toml")
}

fn rss_completed_plan_path() -> std::path::PathBuf {
    std::path::Path::new(omicron_common::OMICRON_CONFIG_PATH)
        .join("rss-plan-completed.toml")
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
}

impl ServiceInner {
    fn new(log: Logger, peer_monitor: PeerMonitorObserver) -> Self {
        ServiceInner { log, peer_monitor: Mutex::new(peer_monitor) }
    }

    async fn initialize_sled_agent(
        &self,
        bootstrap_addr: SocketAddrV6,
        request: &SledAgentRequest,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;

        let url = format!("http://{}", bootstrap_addr);
        info!(self.log, "Sending request to peer agent: {}", url);
        let client = bootstrap_agent_client::Client::new_with_client(
            &url,
            client,
            self.log.new(o!("BootstrapAgentClient" => url.clone())),
        );

        let sled_agent_initialize = || async {
            client
                .start_sled(&bootstrap_agent_client::types::SledAgentRequest {
                    subnet: bootstrap_agent_client::types::Ipv6Subnet {
                        net: bootstrap_agent_client::types::Ipv6Net(
                            request.subnet.net().to_string(),
                        ),
                    },
                })
                .await
                .map_err(BackoffError::transient)?;

            Ok::<
                (),
                BackoffError<
                    bootstrap_agent_client::Error<
                        bootstrap_agent_client::types::Error,
                    >,
                >,
            >(())
        };

        let log_failure = |error, _| {
            warn!(self.log, "failed to start sled agent"; "error" => ?error);
        };
        retry_notify(
            internal_service_policy(),
            sled_agent_initialize,
            log_failure,
        )
        .await?;
        info!(self.log, "Peer agent at {} initialized", url);
        Ok(())
    }

    async fn initialize_datasets(
        &self,
        sled_address: SocketAddr,
        datasets: &Vec<crate::params::DatasetEnsureBody>,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let client = sled_agent_client::Client::new_with_client(
            &format!("http://{}", sled_address),
            client,
            self.log.new(o!("SledAgentClient" => sled_address)),
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
                        sled_agent_client::Error<
                            sled_agent_client::types::Error,
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
        Ok(())
    }

    async fn initialize_services(
        &self,
        sled_address: SocketAddr,
        services: &Vec<crate::params::ServiceRequest>,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let client = sled_agent_client::Client::new_with_client(
            &format!("http://{}", sled_address),
            client,
            self.log.new(o!("SledAgentClient" => sled_address)),
        );

        info!(self.log, "sending service requests...");
        let services_put = || async {
            info!(self.log, "initializing sled services: {:?}", services);
            client
                .services_put(&sled_agent_client::types::ServiceEnsureBody {
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
                    sled_agent_client::Error<sled_agent_client::types::Error>,
                >,
            >(())
        };
        let log_failure = |error, _| {
            warn!(self.log, "failed to initialize services"; "error" => ?error);
        };
        retry_notify(internal_service_policy(), services_put, log_failure)
            .await?;
        Ok(())
    }

    async fn load_plan(
        &self,
    ) -> Result<Option<HashMap<SocketAddrV6, SledAllocation>>, SetupServiceError>
    {
        // If we already created a plan for this RSS to allocate
        // subnets/requests to sleds, re-use that existing plan.
        let rss_plan_path = rss_plan_path();
        if rss_plan_path.exists() {
            info!(self.log, "RSS plan already created, loading from file");

            let plan: std::collections::HashMap<SocketAddrV6, SledAllocation> =
                toml::from_str(
                    &tokio::fs::read_to_string(&rss_plan_path).await?,
                )?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    async fn create_plan(
        &self,
        config: &Config,
        bootstrap_addrs: impl IntoIterator<Item = Ipv6Addr>,
    ) -> Result<HashMap<SocketAddrV6, SledAllocation>, SetupServiceError> {
        let bootstrap_addrs = bootstrap_addrs.into_iter().enumerate();
        let reserved_rack_subnet = ReservedRackSubnet::new(config.az_subnet());
        let dns_subnets = reserved_rack_subnet.get_dns_subnets();

        info!(self.log, "dns_subnets: {:#?}", dns_subnets);

        let requests_and_sleds =
            bootstrap_addrs.map(|(idx, bootstrap_addr)| {
                // If a sled was explicitly requested from the RSS configuration,
                // use that. Otherwise, just give it a "default" (empty) set of
                // services.
                let mut request = {
                    if idx < config.requests.len() {
                        config.requests[idx].clone()
                    } else {
                        SledRequest::default()
                    }
                };

                // The first enumerated addresses get assigned the additional
                // responsibility of being internal DNS servers.
                if idx < dns_subnets.len() {
                    let dns_subnet = &dns_subnets[idx];
                    request.dns_services.push(ServiceRequest {
                        name: "internal-dns".to_string(),
                        addresses: vec![dns_subnet.dns_address().ip()],
                        gz_addresses: vec![dns_subnet.gz_address().ip()],
                    });
                }

                (request, (idx, bootstrap_addr))
            });

        let allocations = requests_and_sleds.map(|(request, sled)| {
            let (idx, bootstrap_addr) = sled;
            info!(
                self.log,
                "Creating plan for the sled at {:?}", bootstrap_addr
            );
            let bootstrap_addr =
                SocketAddrV6::new(bootstrap_addr, BOOTSTRAP_AGENT_PORT, 0, 0);
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);

            (
                bootstrap_addr,
                SledAllocation {
                    initialization_request: SledAgentRequest { subnet },
                    services_request: request,
                },
            )
        });

        info!(self.log, "Serializing plan");

        let mut plan = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            plan.insert(addr, allocation);
        }

        // Once we've constructed a plan, write it down to durable storage.
        let serialized_plan = toml::Value::try_from(&plan)
            .expect("Cannot serialize configuration");
        let plan_str = toml::to_string(&serialized_plan)
            .expect("Cannot turn config to string");

        info!(self.log, "Plan serialized as: {}", plan_str);
        tokio::fs::write(&rss_plan_path(), plan_str).await?;
        info!(self.log, "Plan written to storage");

        Ok(plan)
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

    // In lieu of having an operator send requests to all sleds via an
    // initialization service, the sled-agent configuration may allow for the
    // automated injection of setup requests from a sled.
    //
    // This method has a few distinct phases, identified by files in durable
    // storage:
    //
    // 1. ALLOCATION PLAN CREATION. When the RSS starts up for the first time,
    //    it creates an allocation plan to provision subnets and services
    //    to an initial set of sleds.
    //
    //    This plan is stored at "rss_plan_path()".
    //
    // 2. ALLOCATION PLAN EXECUTION. The RSS then carries out this plan, making
    //    requests to the sleds enumerated within the "allocation plan".
    //
    // 3. MARKING SETUP COMPLETE. Once the RSS has successfully initialized the
    //    rack, the "rss_plan_path()" file is renamed to
    //    "rss_completed_plan_path()". This indicates that the plan executed
    //    successfully, and no work remains.
    async fn inject_rack_setup_requests(
        &self,
        config: &Config,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);

        // We expect this directory to exist - ensure that it does, before any
        // subsequent operations which may write configs here.
        tokio::fs::create_dir_all(omicron_common::OMICRON_CONFIG_PATH).await?;

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
            return Ok(());
        } else {
            info!(self.log, "RSS configuration has not been fully applied yet",);
        }

        // Wait for either:
        // - All the peers to re-load an old plan (if one exists)
        // - Enough peers to create a new plan (if one does not exist)
        let maybe_plan = self.load_plan().await?;
        let expectation = if let Some(plan) = &maybe_plan {
            PeerExpectation::LoadOldPlan(plan.keys().map(|a| *a.ip()).collect())
        } else {
            PeerExpectation::CreateNewPlan(config.requests.len())
        };
        let addrs = self.wait_for_peers(expectation).await?;
        info!(self.log, "Enough peers exist to enact RSS plan");

        // If we created a plan, reuse it. Otherwise, create a new plan.
        //
        // NOTE: This is a "point-of-no-return" -- before sending any requests
        // to neighboring sleds, the plan must be recorded to durable storage.
        // This way, if the RSS power-cycles, it can idempotently execute the
        // same allocation plan.
        let plan = if let Some(plan) = maybe_plan {
            info!(self.log, "Re-using existing allocation plan");
            plan
        } else {
            info!(self.log, "Creating new allocation plan");
            self.create_plan(config, addrs).await?
        };

        // Issue the dataset initialization requests to all sleds.
        futures::future::join_all(plan.iter().map(
            |(bootstrap_addr, allocation)| async move {
                info!(
                    self.log,
                    "Sending request: {:#?}", allocation.initialization_request
                );

                // First, connect to the Bootstrap Agent and tell it to
                // initialize the Sled Agent with the specified subnet.
                self.initialize_sled_agent(
                    *bootstrap_addr,
                    &allocation.initialization_request,
                )
                .await?;
                info!(
                    self.log,
                    "Initialized sled agent on sled with bootstrap address: {}",
                    bootstrap_addr
                );
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<_, SetupServiceError>>()?;

        // Set up internal DNS services.
        futures::future::join_all(
            plan.iter()
                .filter(|(_, allocation)| {
                    // Only send requests to sleds that are supposed to be running
                    // DNS services.
                    !allocation.services_request.dns_services.is_empty()
                })
                .map(|(_, allocation)| async move {
                    let sled_address = SocketAddr::V6(get_sled_address(
                        allocation.initialization_request.subnet,
                    ));

                    self.initialize_services(
                        sled_address,
                        &allocation.services_request.dns_services,
                    )
                    .await?;
                    Ok(())
                }),
        )
        .await
        .into_iter()
        .collect::<Result<_, SetupServiceError>>()?;

        // Issue the dataset initialization requests to all sleds.
        futures::future::join_all(plan.iter().map(
            |(_, allocation)| async move {
                let sled_address = SocketAddr::V6(get_sled_address(
                    allocation.initialization_request.subnet,
                ));
                self.initialize_datasets(
                    sled_address,
                    &allocation.services_request.datasets,
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
        futures::future::join_all(plan.iter().map(
            |(_, allocation)| async move {
                let sled_address = SocketAddr::V6(get_sled_address(
                    allocation.initialization_request.subnet,
                ));

                let all_services = allocation
                    .services_request
                    .services
                    .iter()
                    .chain(allocation.services_request.dns_services.iter())
                    .map(|s| s.clone())
                    .collect::<Vec<_>>();

                self.initialize_services(sled_address, &all_services).await?;
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<()>, SetupServiceError>>()?;

        info!(self.log, "Finished setting up services");

        // Finally, make sure the configuration is saved so we don't inject
        // the requests on the next iteration.
        tokio::fs::rename(rss_plan_path(), rss_completed_plan_path).await?;

        // TODO Questions to consider:
        // - What if a sled comes online *right after* this setup? How does
        // it get a /64?

        Ok(())
    }
}
