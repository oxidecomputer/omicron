// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::config::Config;
use super::discovery;
use super::trust_quorum::{
    self, RackSecret, ShareDistribution, TrustQuorumError,
};
use super::views::ShareResponse;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};

use slog::Logger;
use std::io;
use std::path::Path;
use thiserror::Error;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(Error, Debug)]
pub enum BootstrapError {
    #[error("Cannot deserialize TOML file")]
    Toml(#[from] toml::de::Error),

    #[error("Error accessing filesystem: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error configuring SMF: {0}")]
    SmfConfig(#[from] smf::ConfigError),

    #[error("Error modifying SMF service: {0}")]
    SmfAdm(#[from] smf::AdmError),

    #[error("Error making HTTP request")]
    Api(#[from] nexus_client::Error<()>),

    #[error(transparent)]
    TrustQuorum(#[from] TrustQuorumError),

    #[error("Configuration changed")]
    Configuration,
}

impl From<BootstrapError> for ExternalError {
    fn from(err: BootstrapError) -> Self {
        Self::internal_error(&err.to_string())
    }
}

// Attempt to read a key share file. If the file does not exist, we return
// `Ok(None)`, indicating the sled is operating in a single node cluster. If
// the file exists, we parse it and return Ok(ShareDistribution). For any
// other error, we return the error.
//
// TODO: Remove after dynamic key generation. See #513.
fn read_key_share() -> Result<Option<ShareDistribution>, BootstrapError> {
    let key_share_dir = Path::new("/opt/oxide/sled-agent/pkg");

    match ShareDistribution::read(&key_share_dir) {
        Ok(share) => Ok(Some(share)),
        Err(TrustQuorumError::Io(err)) => {
            if err.kind() == io::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(BootstrapError::Io(err))
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// The entity responsible for bootstrapping an Oxide rack.
pub(crate) struct Agent {
    /// Debug log
    log: Logger,
    peer_monitor: discovery::PeerMonitor,
    share: Option<ShareDistribution>,
}

impl Agent {
    pub fn new(log: Logger) -> Result<Self, BootstrapError> {
        let peer_monitor = discovery::PeerMonitor::new(&log)?;
        let share = read_key_share()?;
        Ok(Agent { log, peer_monitor, share })
    }

    /// Implements the "request share" API.
    pub async fn request_share(
        &self,
        identity: Vec<u8>,
    ) -> Result<ShareResponse, BootstrapError> {
        // TODO-correctness: Validate identity, return whatever
        // information is necessary to establish trust quorum.
        //
        // This current implementation is a placeholder.
        info!(&self.log, "request_share, received identity: {:x?}", identity);

        Ok(ShareResponse { shared_secret: vec![] })
    }

    /// Communicates with peers, sharing secrets, until the rack has been
    /// sufficiently unlocked.
    async fn establish_sled_quorum(
        &self,
    ) -> Result<RackSecret, BootstrapError> {
        let rack_secret = retry_notify(
            internal_service_policy(),
            || async {
                let other_agents = self.peer_monitor.addrs().await;
                info!(
                    &self.log,
                    "Bootstrap: Communicating with peers: {:?}", other_agents
                );

                let share = self.share.as_ref().unwrap();

                // "-1" to account for ourselves.
                if other_agents.len() < share.threshold - 1 {
                    warn!(
                        &self.log,
                        "Not enough peers to start establishing quorum"
                    );
                    return Err(BackoffError::Transient(
                        TrustQuorumError::NotEnoughPeers,
                    ));
                }
                info!(
                    &self.log,
                    "Bootstrap: Enough peers to start share transfer"
                );

                // Retrieve verified rack_secret shares from a quorum of agents
                let other_agents: Vec<trust_quorum::Client> = other_agents
                    .into_iter()
                    .map(|mut addr| {
                        addr.set_port(trust_quorum::PORT);
                        trust_quorum::Client::new(
                            &self.log,
                            share.verifier.clone(),
                            addr,
                        )
                    })
                    .collect();

                // TODO: Parallelize this and keep track of whose shares we've already retrieved and
                // don't resend. See https://github.com/oxidecomputer/omicron/issues/514
                let mut shares = vec![share.share.clone()];
                for agent in &other_agents {
                    let share = agent.get_share().await
                        .map_err(|e| {
                            info!(&self.log, "Bootstrap: failed to retreive share from peer: {:?}", e);
                            BackoffError::Transient(e)
                        })?;
                    info!(
                        &self.log,
                        "Bootstrap: retreived share from peer: {}",
                        agent.addr()
                    );
                    shares.push(share);
                }
                let rack_secret = RackSecret::combine_shares(
                    share.threshold,
                    share.total_shares,
                    &shares,
                )
                .map_err(|e| {
                    warn!(
                        &self.log,
                        "Bootstrap: failed to construct rack secret: {:?}", e
                    );
                    // TODO: We probably need to actually write an error
                    // handling routine that gives up in some cases based on
                    // the error returned from `RackSecret::combine_shares`.
                    // See https://github.com/oxidecomputer/omicron/issues/516
                    BackoffError::Transient(
                        TrustQuorumError::RackSecretConstructionFailed(e),
                    )
                })?;
                info!(self.log, "RackSecret computed from shares.");
                Ok(rack_secret)
            },
            |error, duration| {
                warn!(
                    self.log,
                    "Failed to unlock sleds (will retry after {:?}: {:#}",
                    duration,
                    error,
                )
            },
        )
        .await?;

        Ok(rack_secret)
    }

    async fn run_trust_quorum_server(&self) -> Result<(), BootstrapError> {
        let my_share = self.share.as_ref().unwrap().share.clone();
        let mut server = trust_quorum::Server::new(&self.log, my_share)?;
        tokio::spawn(async move { server.run().await });
        Ok(())
    }

    // In lieu of having an operator send requests to all sleds via an
    // initialization service, the sled-agent configuration may allow for the
    // automated injection of setup requests from a sled.
    async fn inject_rack_setup_service_requests(
        &self,
        config: &Config,
    ) -> Result<(), BootstrapError> {
        if let Some(rss_config) = &config.rss_config {
            info!(self.log, "Injecting RSS configuration: {:#?}", rss_config);

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
                std::path::Path::new(crate::OMICRON_CONFIG_PATH)
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

                // TODO: We could potentially handle this case by deleting all
                // partitions (in preparation for applying the new
                // configuration), but at the moment it's an error.
                warn!(
                    self.log,
                    "Rack Setup Service Config was already applied, but has changed.
                    This means that you may have partitions set up on this sled, but they
                    may not match the ones requested by the supplied configuration.\n
                    To re-initialize this sled:
                       - Disable all Oxide services
                       - Delete all partitions within the attached zpool
                       - Delete the configuration file ({})
                       - Restart the sled agent",
                    rss_config_path.to_string_lossy()
                );
                return Err(BootstrapError::Configuration);
            } else {
                info!(
                    self.log,
                    "No RSS configuration found at {}",
                    rss_config_path.to_string_lossy()
                );
            }

            // Issue the dataset initialization requests to all sleds.
            futures::future::join_all(
                rss_config.requests.iter().map(|request| async move {
                    info!(self.log, "observing request: {:#?}", request);
                    let dur = std::time::Duration::from_secs(60);
                    let client = reqwest::ClientBuilder::new()
                        .connect_timeout(dur)
                        .timeout(dur)
                        .build()
                        .map_err(|e| nexus_client::Error::<()>::from(e))?;
                    let client = sled_agent_client::Client::new_with_client(
                        &format!("http://{}", request.sled_address),
                        client,
                        self.log.new(o!("SledAgentClient" => request.sled_address)),
                    );

                    info!(self.log, "sending partition requests...");
                    for partition in &request.partitions {
                        let filesystem_put = || async {
                            info!(self.log, "creating new filesystem: {:?}", partition);
                            client.filesystem_put(&partition.clone().into())
                                .await
                                .map_err(BackoffError::Transient)
                        };
                        let log_failure = |error, _| {
                            warn!(self.log, "failed to create filesystem"; "error" => ?error);
                        };
                        retry_notify(
                            internal_service_policy(),
                            filesystem_put,
                            log_failure,
                        ).await?;
                    }
                    Ok(())
                })
            ).await.into_iter().collect::<Result<Vec<()>, BootstrapError>>()?;

            // Issue service initialization requests.
            //
            // Note that this must happen *after* the partition initialization,
            // to ensure that CockroachDB has been initialized before Nexus
            // starts.
            futures::future::join_all(
                rss_config.requests.iter().map(|request| async move {
                    info!(self.log, "observing request: {:#?}", request);
                    let dur = std::time::Duration::from_secs(60);
                    let client = reqwest::ClientBuilder::new()
                        .connect_timeout(dur)
                        .timeout(dur)
                        .build()
                        .map_err(|e| nexus_client::Error::<()>::from(e))?;
                    let client = sled_agent_client::Client::new_with_client(
                        &format!("http://{}", request.sled_address),
                        client,
                        self.log.new(o!("SledAgentClient" => request.sled_address)),
                    );

                    info!(self.log, "sending service requests...");
                    let services_put = || async {
                        info!(self.log, "initializing sled services: {:?}", request.services);
                        client.services_put(
                            &sled_agent_client::types::ServiceEnsureBody {
                                services: request.services.iter().map(|s| s.clone().into()).collect()
                            })
                            .await
                            .map_err(BackoffError::Transient)
                    };
                    let log_failure = |error, _| {
                        warn!(self.log, "failed to initialize services"; "error" => ?error);
                    };
                    retry_notify(
                        internal_service_policy(),
                        services_put,
                        log_failure,
                    ).await?;
                    Ok::<(), BootstrapError>(())
                })
            ).await.into_iter().collect::<Result<Vec<()>, BootstrapError>>()?;

            // Finally, make sure the configuration is saved so we don't inject
            // the requests on the next iteration.
            tokio::fs::write(rss_config_path, config_str).await?;
        }
        Ok(())
    }

    /// Performs device initialization:
    ///
    /// - Communicates with other sled agents to establish a trust quorum if a
    /// ShareDistribution file exists on the host. Otherwise, the sled operates
    /// as a single node cluster.
    /// - Verifies, unpacks, and launches other services.
    pub async fn initialize(
        &self,
        config: &Config,
    ) -> Result<(), BootstrapError> {
        info!(&self.log, "bootstrap service initializing");

        if self.share.is_some() {
            self.run_trust_quorum_server().await?;
            self.establish_sled_quorum().await?;
        }

        self.inject_rack_setup_service_requests(config).await?;

        Ok(())
    }
}
