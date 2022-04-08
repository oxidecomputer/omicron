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
use crate::rack_setup::service::Service as RackSetupService;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};

use slog::Logger;
use std::io;
use std::path::Path;
use thiserror::Error;
use tokio::sync::Mutex;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(Error, Debug)]
pub enum BootstrapError {
    #[error("Error accessing filesystem: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error configuring SMF: {0}")]
    SmfConfig(#[from] smf::ConfigError),

    #[error("Error modifying SMF service: {0}")]
    SmfAdm(#[from] smf::AdmError),

    #[error(transparent)]
    TrustQuorum(#[from] TrustQuorumError),
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

    rss: Mutex<Option<RackSetupService>>,
}

impl Agent {
    pub fn new(log: Logger) -> Result<Self, BootstrapError> {
        let peer_monitor = discovery::PeerMonitor::new(&log)?;
        let share = read_key_share()?;
        Ok(Agent { log, peer_monitor, share, rss: Mutex::new(None) })
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
                    return Err(BackoffError::transient(
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
                            BackoffError::transient(e)
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
                    BackoffError::transient(
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

    // Initializes the Rack Setup Service.
    async fn start_rss(&self, config: &Config) -> Result<(), BootstrapError> {
        if let Some(rss_config) = &config.rss_config {
            let rss = RackSetupService::new(
                self.log.new(o!("component" => "RSS")),
                rss_config.clone(),
            );
            self.rss.lock().await.replace(rss);
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

        self.start_rss(config).await?;

        Ok(())
    }
}
