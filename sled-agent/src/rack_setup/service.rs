// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service implementation

use super::config::{HardcodedSledRequest, SetupServiceConfig as Config};
use crate::bootstrap::{
    config::BOOTSTRAP_AGENT_PORT,
    discovery::PeerMonitorObserver,
    params::SledAgentRequest,
    rss_handle::BootstrapAgentHandle,
    trust_quorum::{RackSecret, ShareDistribution},
};
use crate::params::{ServiceRequest, ServiceType};
use internal_dns_client::multiclient::{DnsError, Updater as DnsUpdater};
use omicron_common::address::{
    get_sled_address, ReservedRackSubnet, DNS_PORT, DNS_SERVER_PORT,
};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

/// Describes errors which may occur while operating the setup service.
#[derive(Error, Debug)]
pub enum SetupServiceError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] sled_agent_client::Error<sled_agent_client::types::Error>),

    #[error("Cannot deserialize TOML file at {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error("Failed to monitor for peers: {0}")]
    PeerMonitor(#[from] tokio::sync::broadcast::error::RecvError),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(reqwest::Error),

    #[error("Failed to split rack secret: {0:?}")]
    SplitRackSecret(vsss_rs::Error),

    #[error("Failed to access DNS servers: {0}")]
    Dns(#[from] DnsError),
}

// The workload / information allocated to a single sled.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct SledAllocation {
    initialization_request: SledAgentRequest,
    services_request: HardcodedSledRequest,
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
        // TODO-cleanup: We should be collecting the device ID certs of all
        // trust quorum members over the management network. Currently we don't
        // have a management network, so we hard-code the list of members and
        // accept it as a parameter instead.
        member_device_id_certs: Vec<Ed25519Certificate>,
    ) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log.clone(), peer_monitor);
            if let Err(e) = svc
                .inject_rack_setup_requests(
                    &config,
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
    dns_servers: OnceCell<DnsUpdater>,
}

impl ServiceInner {
    fn new(log: Logger, peer_monitor: PeerMonitorObserver) -> Self {
        ServiceInner {
            log,
            peer_monitor: Mutex::new(peer_monitor),
            dns_servers: OnceCell::new(),
        }
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
            .build()
            .map_err(SetupServiceError::HttpClient)?;
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
        services: &Vec<ServiceRequest>,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(SetupServiceError::HttpClient)?;
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
                    &tokio::fs::read_to_string(&rss_plan_path).await.map_err(
                        |err| SetupServiceError::Io {
                            message: format!(
                                "Loading RSS plan {rss_plan_path:?}"
                            ),
                            err,
                        },
                    )?,
                )
                .map_err(|err| SetupServiceError::Toml {
                    path: rss_plan_path,
                    err,
                })?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    async fn create_plan(
        &self,
        config: &Config,
        bootstrap_addrs: Vec<Ipv6Addr>,
        member_device_id_certs: &[Ed25519Certificate],
    ) -> Result<HashMap<SocketAddrV6, SledAllocation>, SetupServiceError> {
        // Create a rack secret, unless we're in the single-sled case.
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
                bootstrap_addrs.len(),
                concat!(
                    "Number of trust quorum members does not match ",
                    "number of bootstrap addresses"
                )
            );
        }

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
                        HardcodedSledRequest::default()
                    }
                };

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

                (request, (idx, bootstrap_addr))
            });

        let rack_id = Uuid::new_v4();
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
                    initialization_request: SledAgentRequest {
                        id: Uuid::new_v4(),
                        subnet,
                        rack_id,
                        trust_quorum_share: maybe_rack_secret_shares
                            .as_mut()
                            .map(|shares_iter| {
                                // We asserted when creating
                                // `maybe_rack_secret_shares` that it contained
                                // exactly the number of shares as we have
                                // bootstrap addrs, so we can unwrap here.
                                shares_iter.next().unwrap()
                            }),
                    },
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
        let serialized_plan =
            toml::Value::try_from(&plan).unwrap_or_else(|e| {
                panic!("Cannot serialize configuration: {:#?}: {}", plan, e)
            });
        let plan_str = toml::to_string(&serialized_plan)
            .expect("Cannot turn config to string");

        info!(self.log, "Plan serialized as: {}", plan_str);
        let path = rss_plan_path();
        tokio::fs::write(&path, plan_str).await.map_err(|err| {
            SetupServiceError::Io {
                message: format!("Storing RSS plan to {path:?}"),
                err,
            }
        })?;
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
        local_bootstrap_agent: BootstrapAgentHandle,
        member_device_id_certs: &[Ed25519Certificate],
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
            self.create_plan(config, addrs, member_device_id_certs).await?
        };

        // Forward the sled initialization requests to our sled-agent.
        local_bootstrap_agent
            .initialize_sleds(
                plan.iter()
                    .map(|(bootstrap_addr, allocation)| {
                        (
                            *bootstrap_addr,
                            allocation.initialization_request.clone(),
                        )
                    })
                    .collect(),
            )
            .await
            .map_err(SetupServiceError::SledInitialization)?;

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

        let dns_servers = DnsUpdater::new(
            &config.az_subnet(),
            self.log.new(o!("client" => "DNS")),
        );
        self.dns_servers
            .set(dns_servers)
            .map_err(|_| ())
            .expect("DNS servers should only be set once");

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

                self.dns_servers
                    .get()
                    .expect("DNS servers must be initialized first")
                    .insert_dns_records(&allocation.services_request.datasets)
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
                self.dns_servers
                    .get()
                    .expect("DNS servers must be initialized first")
                    .insert_dns_records(&all_services)
                    .await?;
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<()>, SetupServiceError>>()?;

        info!(self.log, "Finished setting up services");

        // Finally, make sure the configuration is saved so we don't inject
        // the requests on the next iteration.
        let plan_path = rss_plan_path();
        tokio::fs::rename(&plan_path, &rss_completed_plan_path).await.map_err(
            |err| SetupServiceError::Io {
                message: format!(
                    "renaming {plan_path:?} to {rss_completed_plan_path:?}"
                ),
                err,
            },
        )?;

        // TODO Questions to consider:
        // - What if a sled comes online *right after* this setup? How does
        // it get a /64?

        Ok(())
    }
}

fn generate_rack_secret<'a>(
    rack_secret_threshold: usize,
    member_device_id_certs: &'a [Ed25519Certificate],
    log: &Logger,
) -> Result<
    Option<impl ExactSizeIterator<Item = ShareDistribution> + 'a>,
    SetupServiceError,
> {
    // We do not generate a rack secret if we only have a single sled or if our
    // config specifies that the threshold for unlock is only a single sled.
    let total_shares = member_device_id_certs.len();
    if total_shares <= 1 {
        info!(log, "Skipping rack secret creation (only one sled present)");
        return Ok(None);
    }

    if rack_secret_threshold <= 1 {
        warn!(
            log,
            concat!(
                "Skipping rack secret creation due to config",
                " (despite discovery of {} bootstrap agents)"
            ),
            total_shares,
        );
        return Ok(None);
    }

    let secret = RackSecret::new();
    let (shares, verifier) = secret
        .split(rack_secret_threshold, total_shares)
        .map_err(SetupServiceError::SplitRackSecret)?;

    Ok(Some(shares.into_iter().map(move |share| ShareDistribution {
        threshold: rack_secret_threshold,
        verifier: verifier.clone(),
        share,
        member_device_id_certs: member_device_id_certs.to_vec(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;
    use sprockets_common::certificates::Ed25519Signature;
    use sprockets_common::certificates::KeyType;

    fn dummy_certs(n: usize) -> Vec<Ed25519Certificate> {
        vec![
            Ed25519Certificate {
                subject_key_type: KeyType::DeviceId,
                subject_public_key: sprockets_host::Ed25519PublicKey([0; 32]),
                signer_key_type: KeyType::Manufacturing,
                signature: Ed25519Signature([0; 64]),
            };
            n
        ]
    }

    #[test]
    fn test_generate_rack_secret() {
        let logctx = test_setup_log("test_generate_rack_secret");

        // No secret generated if we have <= 1 sled
        assert!(generate_rack_secret(10, &dummy_certs(1), &logctx.log)
            .unwrap()
            .is_none());

        // No secret generated if threshold <= 1
        assert!(generate_rack_secret(1, &dummy_certs(10), &logctx.log)
            .unwrap()
            .is_none());

        // Secret generation fails if threshold > total sleds
        assert!(matches!(
            generate_rack_secret(10, &dummy_certs(5), &logctx.log),
            Err(SetupServiceError::SplitRackSecret(_))
        ));

        // Secret generation succeeds if threshold <= total shares and both are
        // > 1, and the returned iterator satifies:
        //
        // * total length == total shares
        // * each share is distinct
        for total_shares in 2..=32 {
            for threshold in 2..=total_shares {
                let certs = dummy_certs(total_shares);
                let shares =
                    generate_rack_secret(threshold, &certs, &logctx.log)
                        .unwrap()
                        .unwrap();

                assert_eq!(shares.len(), total_shares);

                // `Share` doesn't implement `Hash`, but it's a newtype around
                // `Vec<u8>` (which does). Unwrap the newtype to check that all
                // shares are distinct.
                let shares_set = shares
                    .map(|share_dist| share_dist.share.0)
                    .collect::<HashSet<_>>();
                assert_eq!(shares_set.len(), total_shares);
            }
        }

        logctx.cleanup_successful();
    }
}
