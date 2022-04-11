// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service implementation

use super::config::SetupServiceConfig as Config;
use crate::bootstrap::discovery::PeerMonitorObserver;
use crate::bootstrap::{
    client as bootstrap_agent_client, config::BOOTSTRAP_AGENT_PORT,
};
use crate::config::get_sled_address;
use omicron_common::api::external::Ipv6Net;
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
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

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("Configuration changed")]
    Configuration,
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
            let svc = ServiceInner::new(log, peer_monitor);
            svc.inject_rack_setup_requests(&config).await
        });

        Service { handle }
    }

    /// Awaits the completion of the RSS service.
    pub async fn join(self) -> Result<(), SetupServiceError> {
        self.handle.await.expect("Rack Setup Service Task panicked")
    }
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
        subnet: ipnetwork::Ipv6Network,
    ) -> Result<(), SetupServiceError> {
        let dur = std::time::Duration::from_secs(60);

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;

        // TODO: Can we just use a type that avoids the need for this
        // conversion?
        let url = format!(
            "http://[{}]:{}",
            bootstrap_addr.ip(),
            BOOTSTRAP_AGENT_PORT,
        );
        info!(self.log, "Sending request to peer agent: {}", url);
        let client = bootstrap_agent_client::Client::new_with_client(
            &url,
            client,
            self.log.new(o!("BootstrapAgentClient" => url.clone())),
        );

        let sled_agent_initialize = || async {
            client
                .start_sled(&bootstrap_agent_client::types::SledAgentRequest {
                    uuid: uuid::Uuid::new_v4(), // TODO: not rando
                    ip: bootstrap_agent_client::types::Ipv6Net(
                        subnet.to_string(),
                    ),
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

    // In lieu of having an operator send requests to all sleds via an
    // initialization service, the sled-agent configuration may allow for the
    // automated injection of setup requests from a sled.
    async fn inject_rack_setup_requests(
        &self,
        config: &Config,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);

        let serialized_config = toml::Value::try_from(&config)
            .expect("Cannot serialize configuration");
        let config_str = toml::to_string(&serialized_config)
            .expect("Cannot turn config to string");

        // First, check if this request has previously been made.
        //
        // Normally, the rack setup service is run with a human-in-the-loop,
        // but with this automated injection, we need a way to determine the
        // (destructive) initialization has occurred.
        //
        // We do this by storing the configuration at "rss_config_path"
        // after successfully performing initialization.
        let rss_config_path =
            std::path::Path::new(omicron_common::OMICRON_CONFIG_PATH)
                .join("config-rss.toml");
        if rss_config_path.exists() {
            info!(
                self.log,
                "RSS configuration already exists at {}",
                rss_config_path.to_string_lossy()
            );
            let old_config: Config = toml::from_str(
                &tokio::fs::read_to_string(&rss_config_path).await?,
            )?;
            if &old_config == config {
                info!(
                    self.log,
                    "RSS config already applied from: {}",
                    rss_config_path.to_string_lossy()
                );
                return Ok(());
            }

            // TODO(https://github.com/oxidecomputer/omicron/issues/724):
            // We could potentially handle this case by deleting all
            // datasets (in preparation for applying the new
            // configuration), but at the moment it's an error.
            warn!(
                self.log,
                "Rack Setup Service Config ({}) was already applied, but has changed.
                This means that you may have datasets set up on this sled, but they
                may not match the ones requested by the supplied configuration.\n
                To re-initialize this sled, re-run 'omicron-package install'.",
                rss_config_path.to_string_lossy()
            );
            return Err(SetupServiceError::Configuration);
        } else {
            info!(
                self.log,
                "No RSS configuration found at {}",
                rss_config_path.to_string_lossy()
            );
        }

        // Wait until we see enough neighbors to be able to set the
        // initial set of requests.
        let mut peer_monitor = self.peer_monitor.lock().await;
        let our_address = peer_monitor.our_address();
        let mut addrs = peer_monitor.peer_addrs().await;
        while addrs.len() + 1 < config.requests.len() {
            info!(
                self.log,
                "# of peers ({}) < # of requests ({}), waiting for more to join...",
                addrs.len(), config.requests.len()
            );
            peer_monitor.recv().await;
            addrs = peer_monitor.peer_addrs().await;
        }
        info!(self.log, "Enough peers to start configuring rack: {:?}", addrs);

        let addrs =
            addrs.into_iter().chain([&our_address].into_iter()).enumerate();

        // XXX Questions to consider:
        // - What if a sled comes online *right after* this setup? How does
        // it get a /64?
        // - What is the RSS fails *after* telling a BA to start a SA?
        // How can it reconcile that lost address? The current scheme
        // is assigning `/64`s based on the order peers have been seen.

        // Issue the dataset initialization requests to all sleds.
        let requests =
            futures::future::join_all(config.requests.iter().zip(addrs).map(
                |(request, sled)| async move {
                    info!(self.log, "observing request: {:#?}", request);
                    let (idx, bootstrap_addr) = sled;
                    let bootstrap_addr = SocketAddrV6::new(
                        *bootstrap_addr,
                        BOOTSTRAP_AGENT_PORT,
                        0,
                        0,
                    );
                    let sled_subnet_index =
                        u8::try_from(idx + 1).expect("Too many peers!");

                    // First, connect to the Bootstrap Agent and tell it to
                    // initialize the Sled Agent with the specified subnet.
                    let subnet = config.sled_subnet(sled_subnet_index);
                    self.initialize_sled_agent(bootstrap_addr, subnet).await?;

                    // Next, initialize any datasets on sleds that need it.
                    let sled_address =
                        SocketAddr::V6(get_sled_address(Ipv6Net(subnet)));
                    self.initialize_datasets(sled_address, &request.datasets)
                        .await?;
                    Ok((request, sled_address))
                },
            ))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, SetupServiceError>>()?;

        // Issue service initialization requests.
        //
        // Note that this must happen *after* the dataset initialization,
        // to ensure that CockroachDB has been initialized before Nexus
        // starts.
        futures::future::join_all(requests.iter().map(
            |(request, sled_address)| async move {
                self.initialize_services(*sled_address, &request.services)
                    .await?;
                Ok(())
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<()>, SetupServiceError>>()?;

        // Finally, make sure the configuration is saved so we don't inject
        // the requests on the next iteration.
        tokio::fs::write(rss_config_path, config_str).await?;
        Ok(())
    }
}
