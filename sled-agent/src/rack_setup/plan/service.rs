// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "where should services be initialized".

use crate::params::{DatasetEnsureBody, ServiceRequest, ServiceType};
use crate::rack_setup::config::SetupServiceConfig as Config;
use omicron_common::address::{
    get_sled_address, ReservedRackSubnet, DNS_PORT, DNS_SERVER_PORT,
    NEXUS_EXTERNAL_PORT, NEXUS_INTERNAL_PORT, RSS_RESERVED_ADDRESSES,
};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use serde::{Deserialize, Serialize};
use sled_agent_client::{
    types as SledAgentTypes, Client as SledAgentClient, Error as SledAgentError,
};
use slog::Logger;
use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

// The number of Nexus instances to create from RSS.
const NEXUS_COUNT: usize = 1;

// The number of CRDB instances to create from RSS.
const CRDB_COUNT: usize = 1;

fn rss_service_plan_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH).join("rss-service-plan.toml")
}

/// Describes errors which may occur while generating a plan for services.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Cannot deserialize TOML file at {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] SledAgentError<SledAgentTypes::Error>),

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(reqwest::Error),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub services: HashMap<SocketAddrV6, SledRequest>,
}

impl Plan {
    pub async fn load(log: &Logger) -> Result<Option<Plan>, PlanError> {
        // If we already created a plan for this RSS to allocate
        // services to sleds, re-use that existing plan.
        let rss_service_plan_path = rss_service_plan_path();
        if rss_service_plan_path.exists() {
            info!(log, "RSS plan already created, loading from file");

            let plan: Self = toml::from_str(
                &tokio::fs::read_to_string(&rss_service_plan_path)
                    .await
                    .map_err(|err| PlanError::Io {
                        message: format!(
                            "Loading RSS plan {rss_service_plan_path:?}"
                        ),
                        err,
                    })?,
            )
            .map_err(|err| PlanError::Toml {
                path: rss_service_plan_path,
                err,
            })?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    // Gets a zpool UUID from the sled.
    async fn get_a_zpool_from_sled(
        log: &Logger,
        address: SocketAddrV6,
    ) -> Result<Uuid, PlanError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(PlanError::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", address),
            client,
            log.new(o!("SledAgentClient" => address.to_string())),
        );

        let get_zpools = || async {
            let zpools: Vec<Uuid> = client
                .zpools_get()
                .await
                .map(|response| {
                    response
                        .into_inner()
                        .into_iter()
                        .map(|zpool| zpool.id)
                        .collect()
                })
                .map_err(|err| {
                    BackoffError::transient(PlanError::SledApi(err))
                })?;

            if zpools.is_empty() {
                return Err(BackoffError::transient(
                    PlanError::SledInitialization(
                        "Awaiting zpools".to_string(),
                    ),
                ));
            }

            Ok(zpools)
        };
        let log_failure = |error, _| {
            warn!(log, "failed to get zpools"; "error" => ?error);
        };
        let zpools =
            retry_notify(internal_service_policy(), get_zpools, log_failure)
                .await?;

        Ok(zpools[0])
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        sled_addrs: &Vec<SocketAddrV6>,
    ) -> Result<Self, PlanError> {
        let reserved_rack_subnet = ReservedRackSubnet::new(config.az_subnet());
        let dns_subnets = reserved_rack_subnet.get_dns_subnets();

        let mut allocations = vec![];

        for idx in 0..sled_addrs.len() {
            let sled_address = sled_addrs[idx];
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);
            let mut addr_alloc =
                AddressBumpAllocator::new(*get_sled_address(subnet).ip());

            let mut request = SledRequest::default();

            // The first enumerated sleds get assigned the responsibility
            // of hosting Nexus.
            if idx < NEXUS_COUNT {
                let address = addr_alloc.next().expect("Not enough addrs");
                request.services.push(ServiceRequest {
                    id: Uuid::new_v4(),
                    name: "nexus".to_string(),
                    addresses: vec![address],
                    gz_addresses: vec![],
                    service_type: ServiceType::Nexus {
                        internal_address: SocketAddrV6::new(
                            address,
                            NEXUS_INTERNAL_PORT,
                            0,
                            0,
                        ),
                        external_address: SocketAddr::new(
                            config.nexus_external_address,
                            NEXUS_EXTERNAL_PORT,
                        ),
                    },
                })
            }

            // The first enumerated sleds host the CRDB datasets, using
            // zpools described from the underlying config file.
            if idx < CRDB_COUNT {
                let zpool_id =
                    Self::get_a_zpool_from_sled(log, sled_address).await?;

                let address = SocketAddrV6::new(
                    addr_alloc.next().expect("Not enough addrs"),
                    omicron_common::address::COCKROACH_PORT,
                    0,
                    0,
                );
                request.datasets.push(DatasetEnsureBody {
                    id: Uuid::new_v4(),
                    zpool_id,
                    dataset_kind: crate::params::DatasetKind::CockroachDb {
                        all_addresses: vec![address],
                    },
                    address,
                });
            }

            // The first enumerated sleds get assigned the additional
            // responsibility of being internal DNS servers.
            if idx < dns_subnets.len() {
                let dns_subnet = &dns_subnets[idx];
                let dns_addr = dns_subnet.dns_address().ip();
                request.dns_services.push(ServiceRequest {
                    id: Uuid::new_v4(),
                    name: "internal-dns".to_string(),
                    addresses: vec![dns_addr],
                    gz_addresses: vec![dns_subnet.gz_address().ip()],
                    service_type: ServiceType::InternalDns {
                        server_address: SocketAddrV6::new(
                            dns_addr,
                            DNS_SERVER_PORT,
                            0,
                            0,
                        ),
                        dns_address: SocketAddrV6::new(
                            dns_addr, DNS_PORT, 0, 0,
                        ),
                    },
                });
            }

            allocations.push((sled_address, request));
        }

        let mut services = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            services.insert(addr, allocation);
        }

        let plan = Self { services };

        // Once we've constructed a plan, write it down to durable storage.
        let serialized_plan =
            toml::Value::try_from(&plan).unwrap_or_else(|e| {
                panic!("Cannot serialize configuration: {:#?}: {}", plan, e)
            });
        let plan_str = toml::to_string(&serialized_plan)
            .expect("Cannot turn config to string");

        info!(log, "Plan serialized as: {}", plan_str);
        let path = rss_service_plan_path();
        tokio::fs::write(&path, plan_str).await.map_err(|err| {
            PlanError::Io {
                message: format!("Storing RSS service plan to {path:?}"),
                err,
            }
        })?;
        info!(log, "Service plan written to storage");

        Ok(plan)
    }
}

struct AddressBumpAllocator {
    last_addr: Ipv6Addr,
}

// TODO: Testable?
// TODO: Could exist in another file?
impl AddressBumpAllocator {
    fn new(sled_addr: Ipv6Addr) -> Self {
        Self { last_addr: sled_addr }
    }

    fn next(&mut self) -> Option<Ipv6Addr> {
        let mut segments: [u16; 8] = self.last_addr.segments();
        segments[7] = segments[7].checked_add(1)?;
        if segments[7] > RSS_RESERVED_ADDRESSES {
            return None;
        }
        self.last_addr = Ipv6Addr::from(segments);
        Some(self.last_addr)
    }
}
