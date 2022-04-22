// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::config::{Config, BOOTSTRAP_AGENT_PORT};
use super::discovery;
use super::params::SledAgentRequest;
use super::trust_quorum::{
    self, RackSecret, ShareDistribution, TrustQuorumError,
};
use super::views::{ShareResponse, SledAgentResponse};
use crate::config::Config as SledConfig;
use crate::illumos::dladm::{self, Dladm, PhysicalLink};
use crate::illumos::zone::{self, Zones};
use crate::rack_setup::service::Service as RackSetupService;
use crate::server::Server as SledServer;
use omicron_common::api::external::{Error as ExternalError, MacAddr};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};

use slog::Logger;
use std::io;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::{Path, PathBuf};
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

    #[error("Error starting sled agent: {0}")]
    SledError(String),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error(transparent)]
    TrustQuorum(#[from] TrustQuorumError),

    #[error(transparent)]
    Zone(#[from] zone::Error),
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
    sled_agent: Mutex<Option<SledServer>>,
    sled_config: SledConfig,
}

fn get_subnet_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH).join("subnet.toml")
}

fn mac_to_socket_addr(mac: MacAddr) -> SocketAddrV6 {
    let mac_bytes = mac.into_array();
    assert_eq!(6, mac_bytes.len());

    let address = Ipv6Addr::new(
        0xfdb0,
        ((mac_bytes[0] as u16) << 8) | mac_bytes[1] as u16,
        ((mac_bytes[2] as u16) << 8) | mac_bytes[3] as u16,
        ((mac_bytes[4] as u16) << 8) | mac_bytes[5] as u16,
        0,
        0,
        0,
        1,
    );

    SocketAddrV6::new(address, BOOTSTRAP_AGENT_PORT, 0, 0)
}

// TODO(https://github.com/oxidecomputer/omicron/issues/945): This address
// could be randomly generated when it no longer needs to be durable.
pub fn bootstrap_address(
    link: PhysicalLink,
) -> Result<SocketAddrV6, dladm::Error> {
    let mac = Dladm::get_mac(link)?;
    Ok(mac_to_socket_addr(mac))
}

impl Agent {
    pub async fn new(
        log: Logger,
        sled_config: SledConfig,
        address: Ipv6Addr,
    ) -> Result<Self, BootstrapError> {
        Zones::ensure_has_global_zone_v6_address(
            sled_config.data_link.clone(),
            address,
            "bootstrap6",
        )?;

        let peer_monitor = discovery::PeerMonitor::new(&log, address)?;
        let share = read_key_share()?;
        let agent = Agent {
            log,
            peer_monitor,
            share,
            rss: Mutex::new(None),
            sled_agent: Mutex::new(None),
            sled_config,
        };

        let subnet_path = get_subnet_path();
        if subnet_path.exists() {
            info!(agent.log, "Sled already configured, loading sled agent");
            let sled_request: SledAgentRequest = toml::from_str(
                &tokio::fs::read_to_string(&subnet_path).await?,
            )?;
            agent.request_agent(sled_request).await?;
        }

        Ok(agent)
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

    /// Initializes the Sled Agent on behalf of the RSS, if one has not already
    /// been initialized.
    pub async fn request_agent(
        &self,
        request: SledAgentRequest,
    ) -> Result<SledAgentResponse, BootstrapError> {
        info!(&self.log, "Loading Sled Agent: {:?}", request);

        let sled_address = omicron_common::address::get_sled_address(
            request.subnet.as_ref().0,
        );

        let mut maybe_agent = self.sled_agent.lock().await;
        if let Some(server) = &*maybe_agent {
            // Server already exists, return it.
            info!(&self.log, "Sled Agent already loaded");

            if &server.address().ip() != sled_address.ip() {
                let err_str = format!(
                    "Sled Agent already running on address {}, but {} was requested",
                    server.address().ip(),
                    sled_address.ip(),
                );
                return Err(BootstrapError::SledError(err_str));
            }

            return Ok(SledAgentResponse { id: server.id() });
        }
        // Server does not exist, initialize it.
        let server = SledServer::start(&self.sled_config, sled_address)
            .await
            .map_err(|e| BootstrapError::SledError(e))?;
        maybe_agent.replace(server);
        info!(&self.log, "Sled Agent loaded; recording configuration");

        // Record the subnet, so the sled agent can be automatically
        // initialized on the next boot.
        tokio::fs::write(
            get_subnet_path(),
            &toml::to_string(
                &toml::Value::try_from(&request.subnet)
                    .expect("Cannot serialize IP"),
            )
            .expect("Cannot convert toml to string"),
        )
        .await?;

        Ok(SledAgentResponse { id: self.sled_config.id })
    }

    /// Communicates with peers, sharing secrets, until the rack has been
    /// sufficiently unlocked.
    async fn establish_sled_quorum(
        &self,
    ) -> Result<RackSecret, BootstrapError> {
        let rack_secret = retry_notify(
            internal_service_policy(),
            || async {
                let other_agents = self.peer_monitor.peer_addrs().await;
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
                    .map(|addr| {
                        let addr = SocketAddrV6::new(
                            addr,
                            trust_quorum::PORT,
                            0,
                            0,
                        );
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
                self.peer_monitor.observer().await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use macaddr::MacAddr6;

    #[test]
    fn test_mac_to_socket_addr() {
        let mac = MacAddr("a8:40:25:10:00:01".parse::<MacAddr6>().unwrap());

        assert_eq!(
            mac_to_socket_addr(mac).ip(),
            &"fdb0:a840:2510:1::1".parse::<Ipv6Addr>().unwrap(),
        );
    }
}
