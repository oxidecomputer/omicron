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
//! # Phases, state files, and restart behavior
//!
//! Rack setup occurs in distinct phases that are denoted by the presence of
//! state files that get generated as RSS executes:
//!
//! - /pool/int/UUID/config/rss-sled-plan.json (Sled Plan)
//! - /pool/int/UUID/config/rss-service-plan-v5.json (Service Plan)
//! - /pool/int/UUID/config/rss-plan-completed.marker (Plan Execution Complete)
//!
//! These phases are described below.  As each phase completes, a corresponding
//! state file is written.  This mechanism is designed so that if RSS restarts
//! (e.g., after a crash) then it will resume execution using the same plans.
//!
//! The service plan file has "-v2" in the filename because its structure
//! changed in omicron#4466.  It is possible that on startup, RSS finds an
//! older-form service plan.  In that case, it fails altogether.  We do not
//! expect this condition to happen in practice.  See the implementation for
//! details.
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

use super::plan::service::SledConfig;
use crate::bootstrap::config::BOOTSTRAP_AGENT_HTTP_PORT;
use crate::bootstrap::early_networking::{
    EarlyNetworkSetup, EarlyNetworkSetupError,
};
use crate::bootstrap::rss_handle::BootstrapAgentHandle;
use crate::nexus::d2n_params;
use crate::rack_setup::plan::service::{
    Plan as ServicePlan, PlanError as ServicePlanError,
};
use crate::rack_setup::plan::sled::{
    Plan as SledPlan, PlanError as SledPlanError,
};
use anyhow::{bail, Context};
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use chrono::Utc;
use internal_dns::resolver::{DnsError, Resolver as DnsResolver};
use internal_dns::ServiceName;
use nexus_client::{
    types as NexusTypes, Client as NexusClient, Error as NexusError,
};
use nexus_sled_agent_shared::inventory::{
    OmicronZoneConfig, OmicronZoneType, OmicronZonesConfig,
};
use nexus_types::deployment::{
    blueprint_zone_type, Blueprint, BlueprintDatasetConfig,
    BlueprintDatasetDisposition, BlueprintDatasetsConfig, BlueprintZoneType,
    BlueprintZonesConfig, CockroachDbPreserveDowngrade,
};
use nexus_types::external_api::views::SledState;
use omicron_common::address::get_sled_address;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::ExternalPortDiscovery;
use omicron_common::api::internal::shared::LldpAdminStatus;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use omicron_common::disk::{
    OmicronPhysicalDiskConfig, OmicronPhysicalDisksConfig,
};
use omicron_common::ledger::{self, Ledger, Ledgerable};
use omicron_ddm_admin_client::{Client as DdmAdminClient, DdmError};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use serde::{Deserialize, Serialize};
use sled_agent_client::{
    types as SledAgentTypes, Client as SledAgentClient, Error as SledAgentError,
};
use sled_agent_types::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody,
};
use sled_agent_types::rack_init::{
    BootstrapAddressDiscovery, RackInitializeRequest as Config,
};
use sled_agent_types::rack_ops::RssStep;
use sled_agent_types::sled::StartSledAgentRequest;
use sled_agent_types::time_sync::TimeSync;
use sled_hardware_types::underlay::BootstrapInterface;
use sled_storage::dataset::CONFIG_DATASET;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::collections::{HashMap, HashSet};
use std::iter;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

/// For tracking the current RSS step and sending notifications about it.
pub struct RssProgress {
    step_tx: watch::Sender<RssStep>,
}

impl RssProgress {
    pub fn new(step_tx: watch::Sender<RssStep>) -> Self {
        step_tx.send_replace(RssStep::Starting);
        RssProgress { step_tx }
    }

    pub fn update(&mut self, new_step: RssStep) {
        self.step_tx.send_replace(new_step);
    }
}

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

    #[error("Failed to convert setup plan to blueprint: {0:#}")]
    ConvertPlanToBlueprint(anyhow::Error),

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
    /// - `storage_manager`: A handle for interacting with the storage manager
    ///   task
    /// - `local_bootstrap_agent`: Communication channel by which we can send
    ///   commands to our local bootstrap-agent (e.g., to start sled-agents)
    /// - `bootstore` - A handle to call bootstore APIs
    pub(crate) fn new(
        log: Logger,
        config: Config,
        storage_manager: StorageHandle,
        local_bootstrap_agent: BootstrapAgentHandle,
        bootstore: bootstore::NodeHandle,
        step_tx: watch::Sender<RssStep>,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone());
            if let Err(e) = svc
                .run(
                    &config,
                    &storage_manager,
                    local_bootstrap_agent,
                    bootstore,
                    step_tx,
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

    // Ensures that all storage for a particular generation is configured.
    //
    // This will either return:
    // - Ok if the requests are all successful (where "successful" also
    // includes any of the sleds having a storage configuration more recent than
    // what we've requested), or
    // - An error from attempting to configure storage on the underlying sleds
    async fn ensure_storage_config_at_least(
        &self,
        plan: &ServicePlan,
    ) -> Result<(), SetupServiceError> {
        cancel_safe_futures::future::join_all_then_try(
            plan.services.iter().map(|(sled_address, config)| async move {
                self.initialize_storage_on_sled(
                    *sled_address,
                    OmicronPhysicalDisksConfig {
                        generation: config.disks.generation,
                        disks: config
                            .disks
                            .disks
                            .iter()
                            .map(|disk| OmicronPhysicalDiskConfig {
                                identity: disk.identity.clone(),
                                id: disk.id,
                                pool_id: disk.pool_id,
                            })
                            .collect(),
                    },
                )
                .await
            }),
        )
        .await?;
        Ok(())
    }

    /// Requests that the specified sled configure storage as described
    /// by `storage_config`.
    ///
    /// This function succeeds if either the configuration is supplied, or if
    /// the configuration on the target sled is newer than what we're supplying.
    // This function shares a lot of implementation details with
    // [Self::initialize_zones_on_sled]. Although it has a different meaning,
    // the usage (and expectations around generation numbers) are similar.
    async fn initialize_storage_on_sled(
        &self,
        sled_address: SocketAddrV6,
        storage_config: OmicronPhysicalDisksConfig,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .build()
            .map_err(SetupServiceError::HttpClient)?;
        let log = self.log.new(o!("sled_address" => sled_address.to_string()));
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            log.clone(),
        );

        let storage_put = || async {
            info!(
                log,
                "attempting to set up sled's storage: {:?}", storage_config,
            );
            let result = client
                .omicron_physical_disks_put(&storage_config.clone())
                .await;
            let Err(error) = result else {
                return Ok::<
                    (),
                    BackoffError<SledAgentError<SledAgentTypes::Error>>,
                >(());
            };

            if let sled_agent_client::Error::ErrorResponse(response) = &error {
                if response.status() == http::StatusCode::CONFLICT {
                    warn!(
                        log,
                        "ignoring attempt to initialize storage because \
                        the server seems to be newer";
                        "attempted_generation" => i64::from(&storage_config.generation),
                        "req_id" => &response.request_id,
                        "server_message" => &response.message,
                    );

                    // If we attempt to initialize storage at generation X, and
                    // the server refuses because it's at some generation newer
                    // than X, then we treat that as success.  See the doc
                    // comment on this function.
                    return Ok(());
                }
            }

            // TODO Many other codes here should not be retried.  See
            // omicron#4578.
            return Err(BackoffError::transient(error));
        };
        let log_failure = |error, delay| {
            warn!(
                log,
                "failed to initialize Omicron storage";
                "error" => #%error,
                "retry_after" => ?delay,
            );
        };
        retry_notify(
            retry_policy_internal_service_aggressive(),
            storage_put,
            log_failure,
        )
        .await?;

        Ok(())
    }

    /// Requests that the specified sled configure zones as described by
    /// `zones_config`
    ///
    /// This function succeeds even if the sled fails to apply the configuration
    /// if the reason is that the sled is already running a newer configuration.
    /// This might sound oddly specific but it's what our sole caller wants.
    /// In particular, the caller is going to call this function a few times
    /// with successive generation numbers.  If we crash and go through the
    /// process again, we might run into this case, and it's simplest to just
    /// ignore it and proceed.
    async fn initialize_zones_on_sled(
        &self,
        sled_address: SocketAddrV6,
        zones_config: &OmicronZonesConfig,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .build()
            .map_err(SetupServiceError::HttpClient)?;
        let log = self.log.new(o!("sled_address" => sled_address.to_string()));
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            log.clone(),
        );

        let services_put = || async {
            info!(
                log,
                "attempting to set up sled's Omicron zones: {:?}", zones_config
            );
            let result = client.omicron_zones_put(zones_config).await;
            let Err(error) = result else {
                return Ok::<
                    (),
                    BackoffError<SledAgentError<SledAgentTypes::Error>>,
                >(());
            };

            if let sled_agent_client::Error::ErrorResponse(response) = &error {
                if response.status() == http::StatusCode::CONFLICT {
                    warn!(
                        log,
                        "ignoring attempt to initialize zones because \
                        the server seems to be newer";
                        "attempted_generation" =>
                            i64::from(&zones_config.generation),
                        "req_id" => &response.request_id,
                        "server_message" => &response.message,
                    );

                    // If we attempt to initialize zones at generation X, and
                    // the server refuses because it's at some generation newer
                    // than X, then we treat that as success.  See the doc
                    // comment on this function.
                    return Ok(());
                }
            }

            // TODO Many other codes here should not be retried.  See
            // omicron#4578.
            return Err(BackoffError::transient(error));
        };
        let log_failure = |error, delay| {
            warn!(
                log,
                "failed to initialize Omicron zones";
                "error" => #%error,
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

    // Ensure that all services for a particular version are running.
    //
    // This is useful in a rack-setup context, where initial boot ordering
    // can matter for first-time-setup.
    //
    // Note that after first-time setup, the initialization order of
    // services should not matter.
    //
    // Further, it's possible that the target sled is already running a newer
    // version.  That's not an error here.
    async fn ensure_zone_config_at_least(
        &self,
        configs: &HashMap<SocketAddrV6, OmicronZonesConfig>,
    ) -> Result<(), SetupServiceError> {
        cancel_safe_futures::future::join_all_then_try(configs.iter().map(
            |(sled_address, zones_config)| async move {
                self.initialize_zones_on_sled(*sled_address, zones_config).await
            },
        ))
        .await?;
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
                |(_, sled_config)| {
                    // iterate zones for this sled
                    let dns_addrs: Vec<SocketAddrV6> = sled_config
                        .zones
                        .iter()
                        .filter_map(|zone_config| {
                            match &zone_config.zone_type {
                                BlueprintZoneType::InternalDns(blueprint_zone_type::InternalDns{ http_address, .. })
                                => {
                                    Some(*http_address)
                                },
                                _ => None,
                            }
                        })
                        .collect();
                    if !dns_addrs.is_empty() {
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

        // Remap our plan into an easier-to-use type...
        let sled_configs_by_id =
            build_sled_configs_by_id(sled_plan, service_plan)
                .map_err(SetupServiceError::ConvertPlanToBlueprint)?;
        // ... and use that to derive the initial blueprint from our plan.
        let blueprint = build_initial_blueprint_from_plan(
            &sled_configs_by_id,
            service_plan,
        )
        .map_err(SetupServiceError::ConvertPlanToBlueprint)?;

        info!(self.log, "Nexus address: {}", nexus_address.to_string());

        const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);
        let client = reqwest::Client::builder()
            .connect_timeout(CLIENT_TIMEOUT)
            .timeout(CLIENT_TIMEOUT)
            .build()
            .map_err(SetupServiceError::HttpClient)?;

        let nexus_client = NexusClient::new_with_client(
            &format!("http://{}", nexus_address),
            client,
            self.log.new(o!("component" => "NexusClient")),
        );

        // Convert all the information we have about datasets into a format
        // which can be processed by Nexus.
        let mut datasets: Vec<NexusTypes::DatasetCreateRequest> = vec![];
        for sled_config in service_plan.services.values() {
            for zone in &sled_config.zones {
                if let Some(dataset) = zone.zone_type.durable_dataset() {
                    datasets.push(NexusTypes::DatasetCreateRequest {
                        zpool_id: dataset
                            .dataset
                            .pool_name
                            .id()
                            .into_untyped_uuid(),
                        dataset_id: zone.id.into_untyped_uuid(),
                        request: NexusTypes::DatasetPutRequest {
                            address: dataset.address.to_string(),
                            kind: dataset.kind,
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

        let rack_network_config = {
            let config = &config.rack_network_config;
            NexusTypes::RackNetworkConfigV2 {
                rack_subnet: config.rack_subnet,
                infra_ip_first: config.infra_ip_first,
                infra_ip_last: config.infra_ip_last,
                ports: config
                    .ports
                    .iter()
                    .map(|config| NexusTypes::PortConfigV2 {
                        port: config.port.clone(),
                        routes: config
                            .routes
                            .iter()
                            .map(|r| NexusTypes::RouteConfig {
                                destination: r.destination,
                                nexthop: r.nexthop,
                                vlan_id: r.vlan_id,
                                local_pref: r.local_pref,
                            })
                            .collect(),
                        addresses: config
                            .addresses
                            .iter()
                            .map(|a| NexusTypes::UplinkAddressConfig {
                                address: a.address,
                                vlan_id: a.vlan_id,
                            })
                            .collect(),
                        switch: config.switch.into(),
                        uplink_port_speed: config.uplink_port_speed.into(),
                        uplink_port_fec: config.uplink_port_fec.into(),
                        autoneg: config.autoneg,
                        bgp_peers: config
                            .bgp_peers
                            .iter()
                            .map(|b| NexusTypes::BgpPeerConfig {
                                addr: b.addr,
                                asn: b.asn,
                                port: b.port.clone(),
                                hold_time: b.hold_time,
                                connect_retry: b.connect_retry,
                                delay_open: b.delay_open,
                                idle_hold_time: b.idle_hold_time,
                                keepalive: b.keepalive,
                                remote_asn: b.remote_asn,
                                min_ttl: b.min_ttl,
                                md5_auth_key: b.md5_auth_key.clone(),
                                multi_exit_discriminator: b
                                    .multi_exit_discriminator,
                                local_pref: b.local_pref,
                                enforce_first_as: b.enforce_first_as,
                                communities: b.communities.clone(),
                                allowed_export: b.allowed_export.clone(),
                                allowed_import: b.allowed_import.clone(),
                                vlan_id: b.vlan_id,
                            })
                            .collect(),
                        lldp: config.lldp.as_ref().map(|lp| {
                            NexusTypes::LldpPortConfig {
                                status: match lp.status {
                                    LldpAdminStatus::Enabled => {
                                        NexusTypes::LldpAdminStatus::Enabled
                                    }
                                    LldpAdminStatus::Disabled => {
                                        NexusTypes::LldpAdminStatus::Disabled
                                    }
                                    LldpAdminStatus::TxOnly => {
                                        NexusTypes::LldpAdminStatus::TxOnly
                                    }
                                    LldpAdminStatus::RxOnly => {
                                        NexusTypes::LldpAdminStatus::RxOnly
                                    }
                                },
                                chassis_id: lp.chassis_id.clone(),
                                port_id: lp.port_id.clone(),
                                system_name: lp.system_name.clone(),
                                system_description: lp
                                    .system_description
                                    .clone(),
                                port_description: lp.port_description.clone(),
                                management_addrs: lp.management_addrs.clone(),
                            }
                        }),
                    })
                    .collect(),
                bgp: config
                    .bgp
                    .iter()
                    .map(|config| NexusTypes::BgpConfig {
                        asn: config.asn,
                        originate: config
                            .originate
                            .iter()
                            .cloned()
                            .map(Into::into)
                            .collect(),
                        shaper: config.shaper.clone(),
                        checker: config.checker.clone(),
                    })
                    .collect(),
                bfd: config
                    .bfd
                    .iter()
                    .map(|spec| {
                        NexusTypes::BfdPeerConfig {
                    detection_threshold: spec.detection_threshold,
                    local: spec.local,
                    mode: match spec.mode {
                        omicron_common::api::external::BfdMode::SingleHop => {
                            nexus_client::types::BfdMode::SingleHop
                        }
                        omicron_common::api::external::BfdMode::MultiHop => {
                            nexus_client::types::BfdMode::MultiHop
                        }
                    },
                    remote: spec.remote,
                    required_rx: spec.required_rx,
                    switch: spec.switch.into(),
                }
                    })
                    .collect(),
            }
        };
        info!(self.log, "rack_network_config: {:#?}", rack_network_config);

        let physical_disks: Vec<_> = sled_configs_by_id
            .iter()
            .flat_map(|(sled_id, config)| {
                config.disks.disks.iter().map(|config| {
                    NexusTypes::PhysicalDiskPutRequest {
                        id: config.id,
                        vendor: config.identity.vendor.clone(),
                        serial: config.identity.serial.clone(),
                        model: config.identity.model.clone(),
                        variant: NexusTypes::PhysicalDiskKind::U2,
                        sled_id: sled_id.into_untyped_uuid(),
                    }
                })
            })
            .collect();

        let zpools = sled_configs_by_id
            .iter()
            .flat_map(|(sled_id, config)| {
                config.disks.disks.iter().map(|config| {
                    NexusTypes::ZpoolPutRequest {
                        id: config.pool_id.into_untyped_uuid(),
                        physical_disk_id: config.id,
                        sled_id: sled_id.into_untyped_uuid(),
                    }
                })
            })
            .collect();

        // Convert the IP allowlist into the Nexus types.
        //
        // This is really infallible. We have a list of IpNet's here, which
        // we're converting to Nexus client types through their string
        // representation.
        let allowed_source_ips =
            NexusTypes::AllowedSourceIps::try_from(&config.allowed_source_ips)
                .expect("Expected valid Nexus IP networks");

        let request = NexusTypes::RackInitializationRequest {
            blueprint,
            physical_disks,
            zpools,
            datasets,
            internal_services_ip_pool_ranges,
            certs: config.external_certificates.clone(),
            internal_dns_zone_config: d2n_params(&service_plan.dns_config),
            external_dns_zone_name: config.external_dns_zone_name.clone(),
            recovery_silo: config.recovery_silo.clone(),
            rack_network_config,
            external_port_count: port_discovery_mode.into(),
            allowed_source_ips,
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
        let sled_address = service_plan
            .services
            .iter()
            .find_map(|(sled_address, sled_config)| {
                if sled_config.zones.iter().any(|zone_config| {
                    matches!(
                        &zone_config.zone_type,
                        BlueprintZoneType::CockroachDb(_)
                    )
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
                "error" => #%error,
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
    // 2. SLED ALLOCATION PLAN EXECUTION. The RSS then carries out this plan,
    //    making requests to the sleds enumerated within the "allocation plan".
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
    //    indicates that the plan executed successfully, and the only work
    //    remaining is to handoff to Nexus.
    async fn run(
        &self,
        config: &Config,
        storage_manager: &StorageHandle,
        local_bootstrap_agent: BootstrapAgentHandle,
        bootstore: bootstore::NodeHandle,
        step_tx: watch::Sender<RssStep>,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);
        let mut rss_step = RssProgress::new(step_tx);

        let resolver = DnsResolver::new_from_subnet(
            self.log.new(o!("component" => "DnsResolver")),
            config.az_subnet(),
        )?;

        let marker_paths: Vec<Utf8PathBuf> = storage_manager
            .get_latest_disks()
            .await
            .all_m2_mountpoints(CONFIG_DATASET)
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

            rss_step.update(RssStep::LoadExistingPlan);
            let sled_plan = SledPlan::load(&self.log, storage_manager)
                .await?
                .expect("Sled plan should exist if completed marker exists");
            if &sled_plan.config != config {
                return Err(SetupServiceError::BadConfig(
                    "Configuration changed".to_string(),
                ));
            }
            let service_plan = ServicePlan::load(&self.log, storage_manager)
                .await?
                .expect("Service plan should exist if completed marker exists");

            let switch_mgmt_addrs = EarlyNetworkSetup::new(&self.log)
                .lookup_switch_zone_underlay_addrs(&resolver)
                .await;

            let nexus_address =
                resolver.lookup_socket_v6(ServiceName::Nexus).await?;

            rss_step.update(RssStep::NexusHandoff);
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

        rss_step.update(RssStep::CreateSledPlan);
        // Wait for either:
        // - All the peers to re-load an old plan (if one exists)
        // - Enough peers to create a new plan (if one does not exist)
        let bootstrap_addrs = match &config.bootstrap_discovery {
            BootstrapAddressDiscovery::OnlyOurs => {
                BTreeSet::from([local_bootstrap_agent.our_address()])
            }
            BootstrapAddressDiscovery::OnlyThese { addrs } => addrs.clone(),
        };
        let maybe_sled_plan =
            SledPlan::load(&self.log, storage_manager).await?;
        if let Some(plan) = &maybe_sled_plan {
            let stored_peers: BTreeSet<Ipv6Addr> =
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
                &storage_manager,
                bootstrap_addrs,
                config.trust_quorum_peers.is_some(),
            )
            .await?
        };
        let config = &plan.config;

        rss_step.update(RssStep::InitTrustQuorum);
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
            schema_version: 2,
            body: EarlyNetworkConfigBody {
                ntp_servers: config.ntp_servers.clone(),
                rack_network_config: Some(config.rack_network_config.clone()),
            },
        };
        info!(self.log, "Writing Rack Network Configuration to bootstore");
        rss_step.update(RssStep::NetworkConfigUpdate);
        bootstore.update_network_config(early_network_config.into()).await?;

        rss_step.update(RssStep::SledInit);
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
                get_sled_address(initialization_request.body.subnet)
            })
            .collect();
        let service_plan = if let Some(plan) =
            ServicePlan::load(&self.log, storage_manager).await?
        {
            plan
        } else {
            ServicePlan::create(
                &self.log,
                &config,
                &storage_manager,
                &plan.sleds,
            )
            .await?
        };

        rss_step.update(RssStep::EnsureStorage);
        // Before we can ask for any services, we need to ensure that storage is
        // operational.
        self.ensure_storage_config_at_least(&service_plan).await?;

        // Set up internal DNS services first and write the initial
        // DNS configuration to the internal DNS servers.
        let v1generator = OmicronZonesConfigGenerator::initial_version(
            &service_plan,
            DeployStepVersion::V1_NOTHING,
        );
        let v2generator = v1generator.new_version_with(
            DeployStepVersion::V2_DNS_ONLY,
            &|zone_type: &OmicronZoneType| {
                matches!(zone_type, OmicronZoneType::InternalDns { .. })
            },
        );
        rss_step.update(RssStep::InitDns);
        self.ensure_zone_config_at_least(v2generator.sled_configs()).await?;
        rss_step.update(RssStep::ConfigureDns);
        self.initialize_internal_dns_records(&service_plan).await?;

        // Ask MGS in each switch zone which switch it is.
        let switch_mgmt_addrs = EarlyNetworkSetup::new(&self.log)
            .lookup_switch_zone_underlay_addrs(&resolver)
            .await;

        rss_step.update(RssStep::InitNtp);
        // Next start up the NTP services.
        let v3generator = v2generator.new_version_with(
            DeployStepVersion::V3_DNS_AND_NTP,
            &|zone_type: &OmicronZoneType| {
                matches!(
                    zone_type,
                    OmicronZoneType::BoundaryNtp { .. }
                        | OmicronZoneType::InternalNtp { .. }
                )
            },
        );
        self.ensure_zone_config_at_least(v3generator.sled_configs()).await?;

        rss_step.update(RssStep::WaitForTimeSync);
        // Wait until time is synchronized on all sleds before proceeding.
        self.wait_for_timesync(&sled_addresses).await?;

        info!(self.log, "Finished setting up Internal DNS and NTP");

        rss_step.update(RssStep::WaitForDatabase);
        // Wait until Cockroach has been initialized before running Nexus.
        let v4generator = v3generator.new_version_with(
            DeployStepVersion::V4_COCKROACHDB,
            &|zone_type: &OmicronZoneType| {
                matches!(zone_type, OmicronZoneType::CockroachDb { .. })
            },
        );
        self.ensure_zone_config_at_least(v4generator.sled_configs()).await?;

        // Now that datasets and zones have started for CockroachDB,
        // perform one-time initialization of the cluster.
        rss_step.update(RssStep::ClusterInit);
        self.initialize_cockroach(&service_plan).await?;

        // Issue the rest of the zone initialization requests.
        rss_step.update(RssStep::ZonesInit);
        let v5generator = v4generator
            .new_version_with(DeployStepVersion::V5_EVERYTHING, &|_| true);
        self.ensure_zone_config_at_least(v5generator.sled_configs()).await?;

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

        rss_step.update(RssStep::NexusHandoff);
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

        Ok(())
    }
}

/// The service plan describes all the zones that we will eventually
/// deploy on each sled.  But we cannot currently just deploy them all
/// concurrently.  We'll do it in a few stages, each corresponding to a
/// version of each sled's configuration.
///
/// - version 1: no services running
///              (We don't have to do anything for this.  But we do
///              reserve this version number for "no services running" so
///              that sled agents can begin with an initial, valid
///              OmicronZonesConfig before they've got anything running.)
/// - version 2: internal DNS only
/// - version 3: internal DNS + NTP servers
/// - version 4: internal DNS + NTP servers + CockroachDB
/// - version 5: everything
///
/// At each stage, we're specifying a complete configuration of what
/// should be running on the sled -- including this version number.
/// And Sled Agents will reject requests for versions older than the
/// one they're currently running.  Thus, the version number is a piece
/// of global, distributed state.
///
/// For now, we hardcode the requests we make to use specific version
/// numbers.
struct DeployStepVersion;

impl DeployStepVersion {
    const V1_NOTHING: Generation = OmicronZonesConfig::INITIAL_GENERATION;
    const V2_DNS_ONLY: Generation = Self::V1_NOTHING.next();
    const V3_DNS_AND_NTP: Generation = Self::V2_DNS_ONLY.next();
    const V4_COCKROACHDB: Generation = Self::V3_DNS_AND_NTP.next();
    const V5_EVERYTHING: Generation = Self::V4_COCKROACHDB.next();
}

// Build a map of sled ID to `SledConfig` based on the two plan types we
// generate. This is a bit of a code smell (why doesn't the plan generate this
// on its own if we need it?); we should be able to get rid of it when
// we get to https://github.com/oxidecomputer/omicron/issues/5272.
fn build_sled_configs_by_id(
    sled_plan: &SledPlan,
    service_plan: &ServicePlan,
) -> anyhow::Result<BTreeMap<SledUuid, SledConfig>> {
    let mut sled_configs = BTreeMap::new();
    for sled_request in sled_plan.sleds.values() {
        let sled_addr = get_sled_address(sled_request.body.subnet);
        let sled_id = SledUuid::from_untyped_uuid(sled_request.body.id);
        let entry = match sled_configs.entry(sled_id) {
            btree_map::Entry::Vacant(entry) => entry,
            btree_map::Entry::Occupied(_) => {
                bail!(
                    "duplicate sled address found while deriving blueprint: \
                     {sled_addr}"
                );
            }
        };
        let sled_config =
            service_plan.services.get(&sled_addr).with_context(|| {
                format!(
                    "missing services in plan for sled {sled_id} ({sled_addr})"
                )
            })?;
        entry.insert(sled_config.clone());
    }

    if sled_configs.len() != service_plan.services.len() {
        bail!(
            "error mapping service plan to sled IDs; converted {} sled \
             addresses into {} sled configs",
            service_plan.services.len(),
            sled_configs.len(),
        );
    }

    Ok(sled_configs)
}

// Build an initial blueprint
fn build_initial_blueprint_from_plan(
    sled_configs_by_id: &BTreeMap<SledUuid, SledConfig>,
    service_plan: &ServicePlan,
) -> anyhow::Result<Blueprint> {
    let internal_dns_version =
        Generation::try_from(service_plan.dns_config.generation)
            .context("invalid internal dns version")?;

    let blueprint = build_initial_blueprint_from_sled_configs(
        sled_configs_by_id,
        internal_dns_version,
    );

    Ok(blueprint)
}

pub(crate) fn build_initial_blueprint_from_sled_configs(
    sled_configs_by_id: &BTreeMap<SledUuid, SledConfig>,
    internal_dns_version: Generation,
) -> Blueprint {
    let blueprint_disks: BTreeMap<_, _> = sled_configs_by_id
        .iter()
        .map(|(sled_id, sled_config)| (*sled_id, sled_config.disks.clone()))
        .collect();

    let mut blueprint_datasets = BTreeMap::new();
    for (sled_id, sled_config) in sled_configs_by_id {
        let mut datasets = BTreeMap::new();
        for d in sled_config.datasets.datasets.values() {
            // Only the "Crucible" dataset needs to know the address
            let address = sled_config.zones.iter().find_map(|z| {
                if let BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible { address, dataset },
                ) = &z.zone_type
                {
                    if &dataset.pool_name == d.name.pool() {
                        return Some(*address);
                    }
                };
                None
            });

            datasets.insert(
                d.id,
                BlueprintDatasetConfig {
                    disposition: BlueprintDatasetDisposition::InService,
                    id: d.id,
                    pool: d.name.pool().clone(),
                    kind: d.name.dataset().clone(),
                    address,
                    compression: d.compression,
                    quota: d.quota,
                    reservation: d.reservation,
                },
            );
        }

        blueprint_datasets.insert(
            *sled_id,
            BlueprintDatasetsConfig {
                generation: sled_config.datasets.generation,
                datasets,
            },
        );
    }

    let mut blueprint_zones = BTreeMap::new();
    let mut sled_state = BTreeMap::new();
    for (sled_id, sled_config) in sled_configs_by_id {
        let zones_config = BlueprintZonesConfig {
            // This is a bit of a hack. We only construct a blueprint after
            // completing RSS, so we need to know the final generation value
            // sent to all sleds. Arguably, we should record this in the
            // serialized RSS plan; however, we have already deployed
            // systems that did not. We know that every such system used
            // `V5_EVERYTHING` as the final generation count, so we can just
            // use that value here. If we ever change this, in particular in
            // a way where newly-deployed systems will have a different
            // value, we will need to revisit storing this in the serialized
            // RSS plan.
            generation: DeployStepVersion::V5_EVERYTHING,
            zones: sled_config.zones.clone(),
        };

        blueprint_zones.insert(*sled_id, zones_config);
        sled_state.insert(*sled_id, SledState::Active);
    }

    Blueprint {
        id: Uuid::new_v4(),
        blueprint_zones,
        blueprint_disks,
        blueprint_datasets,
        sled_state,
        parent_blueprint_id: None,
        internal_dns_version,
        // We don't configure external DNS during RSS, so set it to an initial
        // generation of 1. Nexus will bump this up when it updates external DNS
        // (including creating the recovery silo).
        external_dns_version: Generation::new(),
        // Nexus will fill in the CockroachDB values during initialization.
        cockroachdb_fingerprint: String::new(),
        cockroachdb_setting_preserve_downgrade:
            CockroachDbPreserveDowngrade::DoNotModify,
        time_created: Utc::now(),
        creator: "RSS".to_string(),
        comment: "initial blueprint from rack setup".to_string(),
    }
}

/// Facilitates creating a sequence of OmicronZonesConfig objects for each sled
/// in a service plan to enable phased rollout of services
///
/// The service plan itself defines which zones should appear on every sled.
/// However, we want to deploy these zones in phases: first internal DNS, then
/// NTP, then CockroachDB, etc.  This interface generates sled configs for each
/// phase and enforces that:
///
/// - each version includes all zones deployed in the previous iteration
/// - each sled's version number increases with each iteration
///
struct OmicronZonesConfigGenerator<'a> {
    service_plan: &'a ServicePlan,
    last_configs: HashMap<SocketAddrV6, OmicronZonesConfig>,
}

impl<'a> OmicronZonesConfigGenerator<'a> {
    /// Make a set of sled configurations for an initial version where each sled
    /// has nothing deployed on it
    fn initial_version(
        service_plan: &'a ServicePlan,
        initial_version: Generation,
    ) -> Self {
        let last_configs = service_plan
            .services
            .keys()
            .map(|sled_address| {
                (
                    *sled_address,
                    OmicronZonesConfig {
                        generation: initial_version,
                        zones: vec![],
                    },
                )
            })
            .collect();
        Self { service_plan, last_configs }
    }

    /// Returns the set of sled configurations produced for this version
    fn sled_configs(&self) -> &HashMap<SocketAddrV6, OmicronZonesConfig> {
        &self.last_configs
    }

    /// Produces a new set of configs for each sled based on the current set of
    /// configurations, adding zones from the service plan matching
    /// `zone_filter`.
    ///
    /// # Panics
    ///
    /// If `version` is not larger than the current version
    fn new_version_with(
        self,
        version: Generation,
        zone_filter: &(dyn Fn(&OmicronZoneType) -> bool + Send + Sync),
    ) -> OmicronZonesConfigGenerator<'a> {
        let last_configs = self
            .service_plan
            .services
            .iter()
            .map(|(sled_address, sled_config)| {
                let mut zones = match self.last_configs.get(sled_address) {
                    Some(config) => {
                        assert!(version > config.generation);
                        config.zones.clone()
                    }
                    None => Vec::new(),
                };

                let zones_already =
                    zones.iter().map(|z| z.id).collect::<HashSet<_>>();
                zones.extend(
                    sled_config
                        .zones
                        .iter()
                        .cloned()
                        .map(|bp_zone_config| {
                            OmicronZoneConfig::from(bp_zone_config)
                        })
                        .filter(|z| {
                            !zones_already.contains(&z.id)
                                && zone_filter(&z.zone_type)
                        }),
                );

                let config = OmicronZonesConfig { generation: version, zones };
                (*sled_address, config)
            })
            .collect();
        Self { service_plan: self.service_plan, last_configs }
    }
}

#[cfg(test)]
mod test {
    use super::{Config, OmicronZonesConfigGenerator};
    use crate::rack_setup::plan::service::{Plan as ServicePlan, SledInfo};
    use nexus_sled_agent_shared::inventory::{
        Baseboard, Inventory, InventoryDisk, OmicronZoneType, SledRole,
    };
    use omicron_common::{
        address::{get_sled_address, Ipv6Subnet, SLED_PREFIX},
        api::external::{ByteCount, Generation},
        disk::{DiskIdentity, DiskVariant},
    };
    use omicron_uuid_kinds::{GenericUuid, SledUuid};

    fn make_sled_info(
        sled_id: SledUuid,
        subnet: Ipv6Subnet<SLED_PREFIX>,
        u2_count: usize,
    ) -> SledInfo {
        let sled_agent_address = get_sled_address(subnet);
        SledInfo::new(
            sled_id,
            subnet,
            sled_agent_address,
            Inventory {
                sled_id: sled_id.into_untyped_uuid(),
                sled_agent_address,
                sled_role: SledRole::Scrimlet,
                baseboard: Baseboard::Unknown,
                usable_hardware_threads: 32,
                usable_physical_ram: ByteCount::from_gibibytes_u32(16),
                reservoir_size: ByteCount::from_gibibytes_u32(0),
                disks: (0..u2_count)
                    .map(|i| InventoryDisk {
                        identity: DiskIdentity {
                            vendor: "test-manufacturer".to_string(),
                            serial: format!("test-{sled_id}-#{i}"),
                            model: "v1".to_string(),
                        },
                        variant: DiskVariant::U2,
                        slot: i.try_into().unwrap(),
                        active_firmware_slot: 1,
                        next_active_firmware_slot: None,
                        number_of_firmware_slots: 1,
                        slot1_is_read_only: true,
                        slot_firmware_versions: vec![Some("TEST1".to_string())],
                    })
                    .collect(),
                zpools: vec![],
                datasets: vec![],
            },
            true,
        )
    }

    fn make_test_service_plan() -> ServicePlan {
        let rss_config = Config::test_config();
        let fake_sleds = vec![
            make_sled_info(
                SledUuid::new_v4(),
                Ipv6Subnet::<SLED_PREFIX>::new(
                    "fd00:1122:3344:101::1".parse().unwrap(),
                ),
                5,
            ),
            make_sled_info(
                SledUuid::new_v4(),
                Ipv6Subnet::<SLED_PREFIX>::new(
                    "fd00:1122:3344:102::1".parse().unwrap(),
                ),
                5,
            ),
        ];
        let service_plan =
            ServicePlan::create_transient(&rss_config, fake_sleds)
                .expect("failed to create service plan");

        service_plan
    }

    #[test]
    fn test_omicron_zone_configs() {
        let service_plan = make_test_service_plan();

        // Verify the initial state.
        let g1 = Generation::new();
        let v1 =
            OmicronZonesConfigGenerator::initial_version(&service_plan, g1);
        assert_eq!(
            service_plan.services.keys().len(),
            v1.sled_configs().keys().len()
        );
        for (_, configs) in v1.sled_configs() {
            assert_eq!(configs.generation, g1);
            assert!(configs.zones.is_empty());
        }

        // Verify that we can add a bunch of zones of a given type.
        let g2 = g1.next();
        let v2 = v1.new_version_with(g2, &|zone_type| {
            matches!(zone_type, OmicronZoneType::InternalDns { .. })
        });
        let mut v2_nfound = 0;
        for (_, config) in v2.sled_configs() {
            assert_eq!(config.generation, g2);
            v2_nfound += config.zones.len();
            for z in &config.zones {
                // The only zones we should find are the Internal DNS ones.
                assert!(matches!(
                    &z.zone_type,
                    OmicronZoneType::InternalDns { .. }
                ));
            }
        }
        // There should have been at least one InternalDns zone.
        assert!(v2_nfound > 0);

        // Try again to add zones of the same type.  This should be a no-op.
        let g3 = g2.next();
        let v3 = v2.new_version_with(g3, &|zone_type| {
            matches!(zone_type, OmicronZoneType::InternalDns { .. })
        });
        let mut v3_nfound = 0;
        for (_, config) in v3.sled_configs() {
            assert_eq!(config.generation, g3);
            v3_nfound += config.zones.len();
            for z in &config.zones {
                // The only zones we should find are the Internal DNS ones.
                assert!(matches!(
                    &z.zone_type,
                    OmicronZoneType::InternalDns { .. }
                ));
            }
        }
        assert_eq!(v2_nfound, v3_nfound);

        // Now try adding zones of a different type.  We should still have all
        // the Internal DNS ones, plus a few more.
        let g4 = g3.next();
        let v4 = v3.new_version_with(g4, &|zone_type| {
            matches!(zone_type, OmicronZoneType::Nexus { .. })
        });
        let mut v4_nfound_dns = 0;
        let mut v4_nfound = 0;
        for (_, config) in v4.sled_configs() {
            assert_eq!(config.generation, g4);
            v4_nfound += config.zones.len();
            for z in &config.zones {
                match &z.zone_type {
                    OmicronZoneType::InternalDns { .. } => v4_nfound_dns += 1,
                    OmicronZoneType::Nexus { .. } => (),
                    _ => panic!("unexpectedly found a wrong zone type"),
                }
            }
        }
        assert_eq!(v4_nfound_dns, v3_nfound);
        assert!(v4_nfound > v3_nfound);

        // Now try adding zones that match no filter.  Again, this should be a
        // no-op but we should still have all the same zones we had before.
        let g5 = g4.next();
        let v5 = v4.new_version_with(g5, &|_| false);
        let mut v5_nfound = 0;
        for (_, config) in v5.sled_configs() {
            assert_eq!(config.generation, g5);
            v5_nfound += config.zones.len();
            for z in &config.zones {
                assert!(matches!(
                    &z.zone_type,
                    OmicronZoneType::InternalDns { .. }
                        | OmicronZoneType::Nexus { .. }
                ));
            }
        }
        assert_eq!(v4_nfound, v5_nfound);

        // Finally, try adding the rest of the zones.
        let g6 = g5.next();
        let v6 = v5.new_version_with(g6, &|_| true);
        let mut v6_nfound = 0;
        for (sled_address, config) in v6.sled_configs() {
            assert_eq!(config.generation, g6);
            v6_nfound += config.zones.len();
            assert_eq!(
                config.zones.len(),
                service_plan.services.get(sled_address).unwrap().zones.len()
            );
        }
        assert!(v6_nfound > v5_nfound);
    }
}
