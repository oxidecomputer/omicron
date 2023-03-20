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
//! - /var/oxide/rss-sled-plan.toml (Sled Plan)
//! - /var/oxide/rss-service-plan.toml (Service Plan)
//! - /var/oxide/rss-plan-completed.marker (Plan Execution Complete)
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
use crate::bootstrap::ddm_admin_client::{DdmAdminClient, DdmError};
use crate::bootstrap::params::{RackInitializeRequest, SledAgentRequest};
use crate::bootstrap::rss_handle::BootstrapAgentHandle;
use crate::bootstrap::trust_quorum::ShareDistribution;
use crate::params::{
    DatasetEnsureBody, ServiceType, ServiceZoneRequest, ZoneType,
};
use crate::rack_setup::dns_interface::{
    DnsInterface, DnsUpdaterInterface, RealDnsAccess,
};
use crate::rack_setup::nexus_interface::{NexusInterface, RealNexusAccess};
use crate::rack_setup::plan::service::{
    Plan as ServicePlan, PlanError as ServicePlanError,
};
use crate::rack_setup::plan::sled::{
    generate_rack_secret, Plan as SledPlan, PlanError as SledPlanError,
};
use crate::rack_setup::sled_interface::{RealSledAccess, SledInterface};
use dns_service_client::multiclient::DnsError;
use internal_dns_names::{ServiceName, SRV};
use nexus_client::types as NexusTypes;
use omicron_common::address::{get_sled_address, NEXUS_INTERNAL_PORT};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::OnceCell;

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

    #[error(transparent)]
    Nexus(#[from] crate::rack_setup::nexus_interface::Error),

    #[error(transparent)]
    Sled(#[from] crate::rack_setup::sled_interface::Error),

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Failed to monitor for peers: {0}")]
    PeerMonitor(#[from] tokio::sync::broadcast::error::RecvError),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(reqwest::Error),

    #[error("Failed to access DNS servers: {0}")]
    Dns(#[from] DnsError),
}

// The workload / information allocated to a single sled.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct SledAllocation {
    initialization_request: SledAgentRequest,
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
    /// - `request`: The requested rack setup
    /// - `local_bootstrap_agent`: Communication channel by which we can send
    ///   commands to our local bootstrap-agent (e.g., to initialize sled
    ///   agents).
    pub(crate) fn new(
        log: Logger,
        request: RackInitializeRequest,
        local_bootstrap_agent: impl BootstrapNetwork,
        // TODO-cleanup: We should be collecting the device ID certs of all
        // trust quorum members over the management network. Currently we don't
        // have a management network, so we hard-code the list of members and
        // accept it as a parameter instead.
        member_device_id_certs: Vec<Ed25519Certificate>,
    ) -> Self {
        let dns_access = RealDnsAccess {};
        let nexus_access = RealNexusAccess {};
        let sled_access = RealSledAccess {};

        let marker_directory = omicron_common::OMICRON_CONFIG_PATH.into();
        Self::new_internal(
            log,
            dns_access,
            nexus_access,
            sled_access,
            marker_directory,
            request,
            local_bootstrap_agent,
            member_device_id_certs,
        )
    }

    fn new_internal(
        log: Logger,
        dns_access: impl DnsInterface,
        nexus_access: impl NexusInterface,
        sled_access: impl SledInterface,
        marker_directory: PathBuf,
        request: RackInitializeRequest,
        local_bootstrap_agent: impl BootstrapNetwork,
        // TODO-cleanup: We should be collecting the device ID certs of all
        // trust quorum members over the management network. Currently we don't
        // have a management network, so we hard-code the list of members and
        // accept it as a parameter instead.
        member_device_id_certs: Vec<Ed25519Certificate>,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone());
            if let Err(e) = svc
                .run(
                    &request,
                    dns_access,
                    nexus_access,
                    sled_access,
                    marker_directory,
                    local_bootstrap_agent,
                    &member_device_id_certs,
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

    /// Awaits the completion of the RSS service.
    pub async fn join(self) -> Result<(), SetupServiceError> {
        self.handle.await.expect("Rack Setup Service Task panicked")
    }
}

fn rss_completed_marker_path(path: &Path) -> PathBuf {
    path.join("rss-plan-completed.marker")
}

// Describes the options when awaiting for peers.
pub(crate) enum PeerExpectation {
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

#[async_trait::async_trait]
pub(crate) trait BootstrapNetwork: Send + Sync + 'static {
    async fn initialize_all_sleds(
        &self,
        requests: Vec<(
            SocketAddrV6,
            SledAgentRequest,
            Option<ShareDistribution>,
        )>,
    ) -> Result<(), String>;

    async fn wait_for_peers(
        &self,
        log: &Logger,
        expectation: PeerExpectation,
    ) -> Result<Vec<Ipv6Addr>, DdmError>;
}

#[async_trait::async_trait]
impl BootstrapNetwork for BootstrapAgentHandle {
    async fn initialize_all_sleds(
        &self,
        requests: Vec<(
            SocketAddrV6,
            SledAgentRequest,
            Option<ShareDistribution>,
        )>,
    ) -> Result<(), String> {
        self.initialize_sleds(requests).await
    }

    async fn wait_for_peers(
        &self,
        log: &Logger,
        expectation: PeerExpectation,
    ) -> Result<Vec<Ipv6Addr>, DdmError> {
        let our_bootstrap_address = self.our_address();
        let ddm_admin_client = DdmAdminClient::new(log.clone())?;
        let addrs = retry_notify(
            retry_policy_internal_service_aggressive(),
            || async {
                let peer_addrs =
                    ddm_admin_client.peer_addrs().await.map_err(|err| {
                        BackoffError::transient(format!(
                            "Failed getting peers from mg-ddm: {err}"
                        ))
                    })?;

                let all_addrs = peer_addrs
                    .chain(iter::once(our_bootstrap_address))
                    .collect::<HashSet<_>>();

                match expectation {
                    PeerExpectation::LoadOldPlan(ref expected) => {
                        if all_addrs.is_superset(expected) {
                            Ok(all_addrs.into_iter().collect())
                        } else {
                            Err(BackoffError::transient(
                                concat!(
                                    "Waiting for a LoadOldPlan set ",
                                    "of peers not found yet."
                                )
                                .to_string(),
                            ))
                        }
                    }
                    PeerExpectation::CreateNewPlan(wanted_peer_count) => {
                        if all_addrs.len() >= wanted_peer_count {
                            Ok(all_addrs.into_iter().collect())
                        } else {
                            Err(BackoffError::transient(format!(
                                "Waiting for {} peers (currently have {})",
                                wanted_peer_count,
                                all_addrs.len()
                            )))
                        }
                    }
                }
            },
            |message, duration| {
                info!(log, "{} (will retry after {:?})", message, duration);
            },
        )
        // `retry_policy_internal_service_aggressive()` retries indefinitely on transient errors
        // (the only kind we produce), allowing us to `.unwrap()` without
        // panicking
        .await
        .unwrap();

        Ok(addrs)
    }
}

/// The implementation of the Rack Setup Service.
struct ServiceInner {
    log: Logger,
    dns_servers: OnceCell<Box<dyn DnsUpdaterInterface>>,
}

impl ServiceInner {
    fn new(log: Logger) -> Self {
        ServiceInner { log, dns_servers: OnceCell::new() }
    }

    async fn initialize_datasets(
        &self,
        sled_access: &impl SledInterface,
        sled_address: SocketAddrV6,
        datasets: &Vec<DatasetEnsureBody>,
    ) -> Result<(), SetupServiceError> {
        sled_access
            .initialize_datasets(&self.log, sled_address, datasets)
            .await?;

        let mut records: HashMap<_, Vec<_>> = HashMap::new();
        for dataset in datasets {
            records
                .entry(dataset.srv())
                .or_default()
                .push((dataset.aaaa(), dataset.address()));
        }
        let records_put = || async {
            self.dns_servers
                .get()
                .expect("DNS servers must be initialized first")
                .insert_dns_records(&records)
                .await
                .map_err(BackoffError::transient)?;
            Ok::<(), BackoffError<DnsError>>(())
        };
        let log_failure = |error, _| {
            warn!(self.log, "failed to set DNS records"; "error" => ?error);
        };
        retry_notify(
            retry_policy_internal_service_aggressive(),
            records_put,
            log_failure,
        )
        .await?;

        Ok(())
    }

    async fn initialize_services(
        &self,
        sled_access: &impl SledInterface,
        sled_address: SocketAddrV6,
        services: &Vec<ServiceZoneRequest>,
    ) -> Result<(), SetupServiceError> {
        sled_access
            .initialize_services(&self.log, sled_address, services)
            .await?;

        // Insert DNS records, if the DNS servers have been initialized
        if let Some(dns_servers) = self.dns_servers.get() {
            let mut records = HashMap::new();
            for zone in services {
                for service in &zone.services {
                    if let Some(addr) = zone.address(&service) {
                        records
                            .entry(zone.srv(&service))
                            .or_insert_with(Vec::new)
                            .push((zone.aaaa(), addr));
                    }
                }
            }
            let records_put = || async {
                dns_servers
                    .insert_dns_records(&records)
                    .await
                    .map_err(BackoffError::transient)?;
                Ok::<(), BackoffError<DnsError>>(())
            };
            let log_failure = |error, _| {
                warn!(self.log, "failed to set DNS records"; "error" => ?error);
            };
            retry_notify(
                retry_policy_internal_service_aggressive(),
                records_put,
                log_failure,
            )
            .await?;
        }

        Ok(())
    }

    async fn handoff_to_nexus(
        &self,
        config: &Config,
        dns_access: &impl DnsInterface,
        nexus_access: &impl NexusInterface,
        sled_plan: &SledPlan,
        service_plan: &ServicePlan,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Handing off control to Nexus");

        let resolver = dns_access.new_resolver(config.az_subnet());
        let ip = resolver
            .lookup_ip(SRV::Service(ServiceName::Nexus))
            .await
            .expect("Failed to lookup IP");
        let nexus_address = SocketAddr::new(ip, NEXUS_INTERNAL_PORT);

        info!(self.log, "Nexus address: {}", nexus_address.to_string());

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
        let mut internal_services_ip_pool_ranges: Vec<NexusTypes::IpRange> =
            vec![];
        for (addr, service_request) in service_plan.services.iter() {
            let sled_id = *id_map
                .get(addr)
                .expect("Sled address in service plan, but not sled plan");

            for zone in &service_request.services {
                for svc in &zone.services {
                    let kind = match svc {
                        ServiceType::Nexus { external_ip, internal_ip: _ } => {
                            // NOTE: Eventually, this IP pool will be entirely
                            // user-supplied. For now, however, it's inferred
                            // based on the input IP addresses.
                            let range = match external_ip {
                                IpAddr::V4(addr) => NexusTypes::IpRange::V4(
                                    NexusTypes::Ipv4Range {
                                        first: *addr,
                                        last: *addr,
                                    },
                                ),
                                IpAddr::V6(addr) => NexusTypes::IpRange::V6(
                                    NexusTypes::Ipv6Range {
                                        first: *addr,
                                        last: *addr,
                                    },
                                ),
                            };
                            internal_services_ip_pool_ranges.push(range);

                            NexusTypes::ServiceKind::Nexus {
                                external_address: *external_ip,
                            }
                        }
                        ServiceType::InternalDns { .. } => {
                            NexusTypes::ServiceKind::InternalDNS
                        }
                        ServiceType::Oximeter => {
                            NexusTypes::ServiceKind::Oximeter
                        }
                        ServiceType::CruciblePantry => {
                            NexusTypes::ServiceKind::CruciblePantry
                        }
                        _ => {
                            return Err(SetupServiceError::BadConfig(format!(
                                "RSS should not request service of type: {}",
                                svc
                            )));
                        }
                    };

                    services.push(NexusTypes::ServicePutRequest {
                        service_id: zone.id,
                        sled_id,
                        // TODO: Should this be a vec, or a single value?
                        address: zone.addresses[0],
                        kind,
                    })
                }
            }

            for dataset in service_request.datasets.iter() {
                datasets.push(NexusTypes::DatasetCreateRequest {
                    zpool_id: dataset.zpool_id,
                    dataset_id: dataset.id,
                    request: NexusTypes::DatasetPutRequest {
                        address: dataset.address.to_string(),
                        kind: dataset.dataset_kind.clone().into(),
                    },
                })
            }
        }

        let request = NexusTypes::RackInitializationRequest {
            services,
            datasets,
            // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Plumb
            // these pools through RSS's API.
            //
            // Currently, we're passing the addresses to accomodate Nexus
            // services, but the operator may want to supply additional
            // addresses.
            internal_services_ip_pool_ranges,
            // TODO(https://github.com/oxidecomputer/omicron/issues/1959): Plumb
            // these paths through RSS's API.
            //
            // These certificates CAN be updated through Nexus' HTTP API, but
            // should be bootstrapped during the rack setup process to avoid
            // the need for unencrypted communication.
            certs: vec![],
        };

        nexus_access
            .rack_initialization_complete(
                &self.log,
                nexus_address,
                sled_plan.rack_id,
                &request,
            )
            .await?;

        info!(self.log, "Handoff to Nexus is complete");
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
        dns_access: impl DnsInterface,
        nexus_access: impl NexusInterface,
        sled_access: impl SledInterface,
        // Directory where "plans" and "markers of completion" files are stored
        marker_directory: PathBuf,
        // Access to our local bootstrap agent and the bootstrap network
        local_bootstrap_agent: impl BootstrapNetwork,
        member_device_id_certs: &[Ed25519Certificate],
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);

        // Check if a previous RSS plan has completed successfully.
        //
        // If it has, the system should be up-and-running.
        let rss_completed_marker_path =
            rss_completed_marker_path(&marker_directory);
        if rss_completed_marker_path.exists() {
            // TODO(https://github.com/oxidecomputer/omicron/issues/724): If the
            // running configuration doesn't match Config, we could try to
            // update things.
            info!(
                self.log,
                "RSS configuration looks like it has already been applied",
            );

            let sled_plan = SledPlan::load(&self.log, &marker_directory)
                .await?
                .expect("Sled plan should exist if completed marker exists");
            if &sled_plan.config != config {
                return Err(SetupServiceError::BadConfig(
                    "Configuration changed".to_string(),
                ));
            }
            let service_plan = ServicePlan::load(&self.log, &marker_directory)
                .await?
                .expect("Service plan should exist if completed marker exists");
            self.handoff_to_nexus(
                &config,
                &dns_access,
                &nexus_access,
                &sled_plan,
                &service_plan,
            )
            .await?;
            return Ok(());
        } else {
            info!(self.log, "RSS configuration has not been fully applied yet",);
        }

        // Wait for either:
        // - All the peers to re-load an old plan (if one exists)
        // - Enough peers to create a new plan (if one does not exist)
        let maybe_sled_plan =
            SledPlan::load(&self.log, &marker_directory).await?;
        let expectation = if let Some(plan) = &maybe_sled_plan {
            PeerExpectation::LoadOldPlan(
                plan.sleds.keys().map(|a| *a.ip()).collect(),
            )
        } else {
            PeerExpectation::CreateNewPlan(MINIMUM_SLED_COUNT)
        };
        let addrs = local_bootstrap_agent
            .wait_for_peers(&self.log, expectation)
            .await?;
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
            SledPlan::create(&self.log, &marker_directory, config, addrs)
                .await?
        };
        let config = &plan.config;

        // Generate our rack secret, unless we're in the single-sled case.
        let mut maybe_rack_secret_shares = generate_rack_secret(
            config.rack_secret_threshold,
            member_device_id_certs,
            &self.log,
        )?;

        // Confirm that the returned iterator (if we got one) is the length we
        // expect.
        if let Some(rack_secret_shares) = maybe_rack_secret_shares.as_ref() {
            // TODO-cleanup Asserting here seems fine as long as
            // `member_device_id_certs` is hard-coded from a config file, but
            // once we start collecting them over the management network we
            // should probably attach them at the type level to the bootstrap
            // addrs, which would remove the need for this assertion.
            assert_eq!(
                rack_secret_shares.len(),
                plan.sleds.len(),
                concat!(
                    "Number of trust quorum members does not match ",
                    "number of sleds in the plan"
                )
            );
        }

        // Forward the sled initialization requests to our sled-agent.
        local_bootstrap_agent
            .initialize_all_sleds(
                plan.sleds
                    .iter()
                    .map(move |(bootstrap_addr, initialization_request)| {
                        (
                            *bootstrap_addr,
                            initialization_request.clone(),
                            maybe_rack_secret_shares
                                .as_mut()
                                .map(|shares| shares.next().unwrap()),
                        )
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
            ServicePlan::load(&self.log, &marker_directory).await?
        {
            plan
        } else {
            ServicePlan::create(
                &self.log,
                &marker_directory,
                &sled_access,
                &config,
                &sled_addresses,
            )
            .await?
        };

        // Set up internal DNS services.
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async {
                let dns_services: Vec<_> = services_request
                    .services
                    .iter()
                    .filter_map(|svc| {
                        if matches!(svc.zone_type, ZoneType::InternalDNS) {
                            Some(svc.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !dns_services.is_empty() {
                    self.initialize_services(
                        &sled_access,
                        *sled_address,
                        &dns_services,
                    )
                    .await?;
                }
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<_, SetupServiceError>>()?;

        let dns_servers = dns_access.new_updater(
            config.az_subnet(),
            self.log.new(o!("client" => "DNS")),
        );
        self.dns_servers
            .set(dns_servers)
            .map_err(|_| ())
            .expect("DNS servers should only be set once");

        // Issue the dataset initialization requests to all sleds.
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async {
                self.initialize_datasets(
                    &sled_access,
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
        // NOTE: This must happen *after* the dataset initialization,
        // to ensure that CockroachDB has been initialized before Nexus
        // starts.
        //
        // If Nexus was more resilient to concurrent initialization
        // of CRDB, this requirement could be relaxed.
        futures::future::join_all(service_plan.services.iter().map(
            |(sled_address, services_request)| async {
                // With the current implementation of "initialize_services",
                // we must provide the set of *all* services that should be
                // executing on a sled.
                //
                // This means re-requesting the DNS service, even if it is
                // already running - this is fine, however, as the receiving
                // sled agent doesn't modify the already-running service.
                self.initialize_services(
                    &sled_access,
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
        tokio::fs::File::create(&rss_completed_marker_path).await.map_err(
            |err| SetupServiceError::Io {
                message: format!("creating {rss_completed_marker_path:?}"),
                err,
            },
        )?;

        // At this point, even if we reboot, we must not try to manage sleds,
        // services, or DNS records.
        self.handoff_to_nexus(
            &config,
            &dns_access,
            &nexus_access,
            &plan,
            &service_plan,
        )
        .await?;

        // TODO Questions to consider:
        // - What if a sled comes online *right after* this setup? How does
        // it get a /64?

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::bootstrap::params::{Gateway, RackInitializeRequest};
    use crate::params::DatasetKind;
    use crate::rack_setup::dns_interface::DnsResolverInterface;
    use crate::rack_setup::nexus_interface::Error as NexusError;
    use crate::rack_setup::sled_interface::Error as SledError;
    use dns_service_client::multiclient::AAAARecord;
    use dns_service_client::multiclient::ResolveError;
    use macaddr::MacAddr6;
    use omicron_common::address::{get_sled_address, Ipv6Subnet, AZ_PREFIX};
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::sync::Arc;
    use std::sync::Mutex;
    use uuid::Uuid;

    struct FakeDnsResolver {
        records: Arc<Mutex<HashMap<internal_dns_names::SRV, Vec<AAAARecord>>>>,
    }

    #[async_trait::async_trait]
    impl DnsResolverInterface for FakeDnsResolver {
        async fn lookup_ip(
            &self,
            srv: internal_dns_names::SRV,
        ) -> Result<IpAddr, ResolveError> {
            let records = self.records.lock().unwrap();
            let aaaa =
                records.get(&srv).ok_or_else(|| ResolveError::NotFound(srv))?;
            Ok(IpAddr::V6(aaaa[0].1.ip().clone()))
        }
    }

    struct FakeDnsUpdater {
        records: Arc<Mutex<HashMap<internal_dns_names::SRV, Vec<AAAARecord>>>>,
    }

    #[async_trait::async_trait]
    impl DnsUpdaterInterface for FakeDnsUpdater {
        async fn insert_dns_records(
            &self,
            records: &HashMap<internal_dns_names::SRV, Vec<AAAARecord>>,
        ) -> Result<(), DnsError> {
            let mut server_records = self.records.lock().unwrap();
            for (k, v) in records {
                server_records.insert(k.clone(), v.clone());
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FakeDnsAccess {
        records: Arc<Mutex<HashMap<internal_dns_names::SRV, Vec<AAAARecord>>>>,
    }
    impl FakeDnsAccess {
        fn new() -> Self {
            Self { records: Arc::new(Mutex::new(HashMap::new())) }
        }
    }

    impl DnsInterface for FakeDnsAccess {
        fn new_resolver(
            &self,
            _address: Ipv6Subnet<AZ_PREFIX>,
        ) -> Box<dyn DnsResolverInterface> {
            Box::new(FakeDnsResolver { records: self.records.clone() })
        }

        fn new_updater(
            &self,
            _address: Ipv6Subnet<AZ_PREFIX>,
            _log: Logger,
        ) -> Box<dyn DnsUpdaterInterface> {
            Box::new(FakeDnsUpdater { records: self.records.clone() })
        }
    }

    #[derive(Clone)]
    struct FakeNexusAccess {
        request: Arc<OnceCell<NexusTypes::RackInitializationRequest>>,
    }

    impl FakeNexusAccess {
        fn new() -> Self {
            Self { request: Arc::new(OnceCell::new()) }
        }
    }

    #[async_trait::async_trait]
    impl NexusInterface for FakeNexusAccess {
        async fn rack_initialization_complete(
            &self,
            _log: &Logger,
            _address: SocketAddr,
            _rack_id: Uuid,
            request: &NexusTypes::RackInitializationRequest,
        ) -> Result<(), NexusError> {
            self.request.set(request.into()).expect("Already initialized");
            Ok(())
        }
    }

    struct FakeSled {
        zpools: HashSet<Uuid>,
        datasets: HashMap<Uuid, DatasetEnsureBody>,
        services: HashMap<Uuid, ServiceZoneRequest>,
    }

    impl FakeSled {
        fn new(zpools_per_sled: usize) -> Self {
            Self {
                zpools: (0..zpools_per_sled).map(|_| Uuid::new_v4()).collect(),
                datasets: HashMap::new(),
                services: HashMap::new(),
            }
        }
    }

    #[derive(Clone)]
    struct AllSleds {
        inner: Arc<Mutex<HashMap<SocketAddrV6, FakeSled>>>,
    }

    impl AllSleds {
        fn new() -> Self {
            Self { inner: Arc::new(Mutex::new(HashMap::new())) }
        }

        // Gather all datasets, across all sleds
        fn datasets(&self) -> Vec<DatasetEnsureBody> {
            let inner = self.inner.lock().unwrap();
            let mut datasets = vec![];
            for sled in inner.values() {
                datasets.extend(sled.datasets.values().cloned());
            }
            datasets
        }

        fn clickhouse(&self) -> Vec<DatasetEnsureBody> {
            self.datasets()
                .into_iter()
                .filter(|d| matches!(d.dataset_kind, DatasetKind::Clickhouse))
                .collect()
        }

        fn cockroach(&self) -> Vec<DatasetEnsureBody> {
            self.datasets()
                .into_iter()
                .filter(|d| {
                    matches!(d.dataset_kind, DatasetKind::CockroachDb { .. })
                })
                .collect()
        }

        fn crucible(&self) -> Vec<DatasetEnsureBody> {
            self.datasets()
                .into_iter()
                .filter(|d| matches!(d.dataset_kind, DatasetKind::Crucible))
                .collect()
        }

        // Gather all services, across all sleds
        fn services(&self) -> Vec<ServiceZoneRequest> {
            let inner = self.inner.lock().unwrap();
            let mut services = vec![];
            for sled in inner.values() {
                services.extend(sled.services.values().cloned());
            }
            services
        }

        fn nexus(&self) -> Vec<ServiceZoneRequest> {
            self.services()
                .into_iter()
                .filter(|s| matches!(s.zone_type, ZoneType::Nexus))
                .collect()
        }
        fn oximeter(&self) -> Vec<ServiceZoneRequest> {
            self.services()
                .into_iter()
                .filter(|s| matches!(s.zone_type, ZoneType::Oximeter))
                .collect()
        }
        fn internal_dns(&self) -> Vec<ServiceZoneRequest> {
            self.services()
                .into_iter()
                .filter(|s| matches!(s.zone_type, ZoneType::InternalDNS))
                .collect()
        }
        fn crucible_pantry(&self) -> Vec<ServiceZoneRequest> {
            self.services()
                .into_iter()
                .filter(|s| matches!(s.zone_type, ZoneType::CruciblePantry))
                .collect()
        }
    }

    struct FakeBootstrapNetwork {
        bootstrap_addresses: Vec<Ipv6Addr>,
        sleds: AllSleds,
        zpools_per_sled: usize,
    }

    impl FakeBootstrapNetwork {
        fn new(
            bootstrap_addresses: Vec<Ipv6Addr>,
            sleds: AllSleds,
            zpools_per_sled: usize,
        ) -> Self {
            Self { bootstrap_addresses, sleds, zpools_per_sled }
        }
    }

    #[async_trait::async_trait]
    impl BootstrapNetwork for FakeBootstrapNetwork {
        async fn initialize_all_sleds(
            &self,
            requests: Vec<(
                SocketAddrV6,
                SledAgentRequest,
                Option<ShareDistribution>,
            )>,
        ) -> Result<(), String> {
            let mut sleds = self.sleds.inner.lock().unwrap();
            for (_, request, _) in &requests {
                sleds.insert(
                    get_sled_address(request.subnet),
                    FakeSled::new(self.zpools_per_sled),
                );
            }

            Ok(())
        }

        async fn wait_for_peers(
            &self,
            _log: &Logger,
            _expectation: PeerExpectation,
        ) -> Result<Vec<Ipv6Addr>, DdmError> {
            Ok(self.bootstrap_addresses.clone())
        }
    }

    struct FakeSledAccess {
        sleds: AllSleds,
    }

    impl FakeSledAccess {
        fn new(sleds: AllSleds) -> Self {
            Self { sleds }
        }
    }

    #[async_trait::async_trait]
    impl SledInterface for FakeSledAccess {
        async fn get_u2_zpools(
            &self,
            _log: &Logger,
            address: SocketAddrV6,
        ) -> Result<Vec<Uuid>, SledError> {
            self.sleds
                .inner
                .lock()
                .unwrap()
                .get(&address)
                .map(|sled| sled.zpools.iter().cloned().collect())
                .ok_or_else(|| {
                    SledError::SledInitialization("Sled not found".to_string())
                })
        }

        async fn initialize_datasets(
            &self,
            _log: &Logger,
            sled_address: SocketAddrV6,
            datasets: &Vec<DatasetEnsureBody>,
        ) -> Result<(), SledError> {
            let mut sleds = self.sleds.inner.lock().unwrap();
            let sled = sleds.get_mut(&sled_address).ok_or_else(|| {
                SledError::SledInitialization("Sled not found".to_string())
            })?;
            for dataset in datasets {
                sled.datasets.insert(dataset.id, dataset.clone());
            }

            Ok(())
        }

        async fn initialize_services(
            &self,
            _log: &Logger,
            sled_address: SocketAddrV6,
            services: &Vec<ServiceZoneRequest>,
        ) -> Result<(), SledError> {
            let mut sleds = self.sleds.inner.lock().unwrap();
            let sled = sleds.get_mut(&sled_address).ok_or_else(|| {
                SledError::SledInitialization("Sled not found".to_string())
            })?;
            for service in services {
                sled.services.insert(service.id, service.clone());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn rack_initialize_one_sled() {
        let logctx = test_setup_log("rack_initialize_one_sled");
        let log = &logctx.log;

        let nexus_external_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let request = RackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            rack_secret_threshold: 1,
            gateway: Gateway { address: None, mac: MacAddr6::nil().into() },
            nexus_external_address,
        };

        let sleds = AllSleds::new();
        let dns_access = FakeDnsAccess::new();
        let nexus_access = FakeNexusAccess::new();
        let sled_access = FakeSledAccess::new(sleds.clone());
        const ZPOOLS_PER_SLED: usize = 3;
        let bootstrap = FakeBootstrapNetwork::new(
            vec![Ipv6Addr::LOCALHOST],
            sleds.clone(),
            ZPOOLS_PER_SLED,
        );

        let tempdir = tempfile::tempdir().expect("Failed to make tempdir");

        assert_eq!(0, sleds.inner.lock().unwrap().len());
        assert_eq!(0, dns_access.records.lock().unwrap().len());

        let rss = RackSetupService::new_internal(
            log.new(o!("component" => "RSS")),
            dns_access.clone(),
            nexus_access.clone(),
            sled_access,
            tempdir.path().into(),
            request,
            bootstrap,
            vec![],
        );
        rss.join().await.unwrap();

        const EXPECTED_SLEDS: usize = 1;
        assert_eq!(EXPECTED_SLEDS, sleds.inner.lock().unwrap().len());

        // Validate provisioned datasets
        const EXPECTED_DATASETS: usize = EXPECTED_SLEDS * ZPOOLS_PER_SLED + 2;
        assert_eq!(ZPOOLS_PER_SLED, sleds.crucible().len());
        assert_eq!(1, sleds.cockroach().len());
        assert_eq!(1, sleds.clickhouse().len());
        assert_eq!(EXPECTED_DATASETS, sleds.datasets().len());

        // Validate provisioned services
        const EXPECTED_SERVICES: usize = 4;
        assert_eq!(1, sleds.nexus().len());
        assert_eq!(1, sleds.oximeter().len());
        assert_eq!(1, sleds.internal_dns().len());
        assert_eq!(1, sleds.crucible_pantry().len());
        assert_eq!(EXPECTED_SERVICES, sleds.services().len());

        // Validate DNS records
        assert_eq!(
            EXPECTED_SERVICES + EXPECTED_DATASETS,
            dns_access.records.lock().unwrap().len()
        );

        // Validate that Nexus heard about the request
        let nexus_request = nexus_access.request.get().unwrap();
        assert_eq!(EXPECTED_SERVICES, nexus_request.services.len());
        assert_eq!(EXPECTED_DATASETS, nexus_request.datasets.len());
        assert_eq!(1, nexus_request.internal_services_ip_pool_ranges.len(),);
        assert_eq!(0, nexus_request.certs.len(),);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_initialize_two_sleds() {
        let logctx = test_setup_log("rack_initialize_two_sleds");
        let log = &logctx.log;

        let nexus_external_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let request = RackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            rack_secret_threshold: 2,
            gateway: Gateway { address: None, mac: MacAddr6::nil().into() },
            nexus_external_address,
        };

        let sleds = AllSleds::new();
        let dns_access = FakeDnsAccess::new();
        let nexus_access = FakeNexusAccess::new();
        let sled_access = FakeSledAccess::new(sleds.clone());
        const ZPOOLS_PER_SLED: usize = 3;
        let bootstrap = FakeBootstrapNetwork::new(
            vec![
                Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 2),
            ],
            sleds.clone(),
            ZPOOLS_PER_SLED,
        );

        let tempdir = tempfile::tempdir().expect("Failed to make tempdir");

        assert_eq!(0, sleds.inner.lock().unwrap().len());
        assert_eq!(0, dns_access.records.lock().unwrap().len());

        let rss = RackSetupService::new_internal(
            log.new(o!("component" => "RSS")),
            dns_access.clone(),
            nexus_access.clone(),
            sled_access,
            tempdir.path().into(),
            request,
            bootstrap,
            vec![],
        );
        rss.join().await.unwrap();

        const EXPECTED_SLEDS: usize = 2;
        assert_eq!(EXPECTED_SLEDS, sleds.inner.lock().unwrap().len());

        // Validate provisioned datasets
        const EXPECTED_DATASETS: usize = EXPECTED_SLEDS * ZPOOLS_PER_SLED + 2;
        assert_eq!(ZPOOLS_PER_SLED * EXPECTED_SLEDS, sleds.crucible().len(),);
        assert_eq!(1, sleds.cockroach().len(),);
        assert_eq!(1, sleds.clickhouse().len(),);
        assert_eq!(EXPECTED_DATASETS, sleds.datasets().len());

        // Validate provisioned services
        const EXPECTED_SERVICES: usize = 4;
        assert_eq!(1, sleds.nexus().len());
        assert_eq!(1, sleds.oximeter().len());
        assert_eq!(1, sleds.internal_dns().len());
        assert_eq!(1, sleds.crucible_pantry().len());
        assert_eq!(EXPECTED_SERVICES, sleds.services().len());

        // Validate DNS records
        assert_eq!(
            EXPECTED_SERVICES + EXPECTED_DATASETS,
            dns_access.records.lock().unwrap().len()
        );

        // Validate that Nexus heard about the request
        let nexus_request = nexus_access.request.get().unwrap();
        assert_eq!(EXPECTED_SERVICES, nexus_request.services.len());
        assert_eq!(EXPECTED_DATASETS, nexus_request.datasets.len());
        assert_eq!(1, nexus_request.internal_services_ip_pool_ranges.len(),);
        assert_eq!(0, nexus_request.certs.len(),);

        logctx.cleanup_successful();
    }

    // TODO: This test should NOT panic, but I'm including it as validation of existing behavior.
    #[tokio::test]
    #[should_panic]
    async fn rack_initialize_no_u2_panics() {
        let logctx = test_setup_log("rack_initialize_no_u2_panics");
        let log = &logctx.log;

        let nexus_external_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let request = RackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            rack_secret_threshold: 2,
            gateway: Gateway { address: None, mac: MacAddr6::nil().into() },
            nexus_external_address,
        };

        let sleds = AllSleds::new();
        let dns_access = FakeDnsAccess::new();
        let nexus_access = FakeNexusAccess::new();
        let sled_access = FakeSledAccess::new(sleds.clone());
        const ZPOOLS_PER_SLED: usize = 0;
        let bootstrap = FakeBootstrapNetwork::new(
            vec![Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 1)],
            sleds.clone(),
            ZPOOLS_PER_SLED,
        );

        let tempdir = tempfile::tempdir().expect("Failed to make tempdir");

        assert_eq!(0, sleds.inner.lock().unwrap().len());
        assert_eq!(0, dns_access.records.lock().unwrap().len());

        let rss = RackSetupService::new_internal(
            log.new(o!("component" => "RSS")),
            dns_access.clone(),
            nexus_access.clone(),
            sled_access,
            tempdir.path().into(),
            request,
            bootstrap,
            vec![],
        );
        rss.join().await.unwrap();

        // XXX The panic happens because we index into "u2_zpools" without validating the length
        // during plan generation.

        logctx.cleanup_successful();
    }

    // TODO: This test should NOT panic, but I'm including it as validation of existing behavior.
    #[tokio::test]
    #[should_panic]
    async fn rack_reinitialize_missing_dns_panics() {
        let logctx = test_setup_log("rack_reinitialize_missing_dns_panics");
        let log = &logctx.log;

        let nexus_external_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let request = RackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            rack_secret_threshold: 2,
            gateway: Gateway { address: None, mac: MacAddr6::nil().into() },
            nexus_external_address,
        };

        let sleds = AllSleds::new();
        let dns_access = FakeDnsAccess::new();
        let nexus_access = FakeNexusAccess::new();
        let sled_access = FakeSledAccess::new(sleds.clone());
        const ZPOOLS_PER_SLED: usize = 3;
        let bootstrap = FakeBootstrapNetwork::new(
            vec![Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 1)],
            sleds.clone(),
            ZPOOLS_PER_SLED,
        );

        let tempdir = tempfile::tempdir().expect("Failed to make tempdir");

        // Initializing the rack should succeed
        let rss = RackSetupService::new_internal(
            log.new(o!("component" => "RSS")),
            dns_access.clone(),
            nexus_access.clone(),
            sled_access,
            tempdir.path().into(),
            request.clone(),
            bootstrap,
            vec![],
        );
        rss.join().await.unwrap();

        // Re-initialize the rack with the same config, in the same temporary directory
        {
            let sleds = AllSleds::new();
            let dns_access = FakeDnsAccess::new();
            let nexus_access = FakeNexusAccess::new();
            let sled_access = FakeSledAccess::new(sleds.clone());
            let bootstrap = FakeBootstrapNetwork::new(
                vec![Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 1)],
                sleds.clone(),
                ZPOOLS_PER_SLED,
            );
            let rss = RackSetupService::new_internal(
                log.new(o!("component" => "RSS")),
                dns_access.clone(),
                nexus_access.clone(),
                sled_access,
                tempdir.path().into(),
                request.clone(),
                bootstrap,
                vec![],
            );
            rss.join().await.unwrap();
        }

        // XXX The panic happens because "handoff_to_nexus" ".expects()" that the Nexus IP
        // address can be looked up, but in reality, it should return an error.

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn rack_reinitialize_reads_configs() {
        let logctx = test_setup_log("rack_reinitialize_reads_configs");
        let log = &logctx.log;

        let nexus_external_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let request = RackInitializeRequest {
            rack_subnet: Ipv6Addr::LOCALHOST,
            rack_secret_threshold: 2,
            gateway: Gateway { address: None, mac: MacAddr6::nil().into() },
            nexus_external_address,
        };

        let sleds = AllSleds::new();
        let dns_access = FakeDnsAccess::new();
        let nexus_access = FakeNexusAccess::new();
        let sled_access = FakeSledAccess::new(sleds.clone());
        const ZPOOLS_PER_SLED: usize = 3;
        let bootstrap = FakeBootstrapNetwork::new(
            vec![Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 1)],
            sleds.clone(),
            ZPOOLS_PER_SLED,
        );

        let tempdir = tempfile::tempdir().expect("Failed to make tempdir");

        // Initializing the rack should succeed
        let rss = RackSetupService::new_internal(
            log.new(o!("component" => "RSS")),
            dns_access.clone(),
            nexus_access.clone(),
            sled_access,
            tempdir.path().into(),
            request.clone(),
            bootstrap,
            vec![],
        );
        rss.join().await.unwrap();

        // Re-initialize the rack with the same config, in the same temporary directory
        //
        // NOTE: We use the same "DNS Access" to look up Nexus - without it, handoff fails,
        // because the IP address cannot be identified.
        {
            let sleds = AllSleds::new();
            let nexus_access = FakeNexusAccess::new();
            let sled_access = FakeSledAccess::new(sleds.clone());
            let bootstrap = FakeBootstrapNetwork::new(
                vec![Ipv6Addr::new(0xfe81, 0, 0, 0, 0, 0, 0, 1)],
                sleds.clone(),
                ZPOOLS_PER_SLED,
            );
            let rss = RackSetupService::new_internal(
                log.new(o!("component" => "RSS")),
                dns_access.clone(),
                nexus_access.clone(),
                sled_access,
                tempdir.path().into(),
                request.clone(),
                bootstrap,
                vec![],
            );
            rss.join().await.unwrap();
        }

        logctx.cleanup_successful();
    }
}
