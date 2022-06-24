// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::client::Client as BootstrapAgentClient;
use super::config::{Config, BOOTSTRAP_AGENT_PORT};
use super::discovery;
use super::params::SledAgentRequest;
use super::rss_handle::RssHandle;
use super::server::TrustQuorumMembership;
use super::trust_quorum::{RackSecret, ShareDistribution, TrustQuorumError};
use super::views::SledAgentResponse;
use crate::config::Config as SledConfig;
use crate::illumos::dladm::{self, Dladm, PhysicalLink};
use crate::illumos::zone::Zones;
use crate::server::Server as SledServer;
use crate::sp::SpHandle;
use omicron_common::address::get_sled_address;
use omicron_common::api::external::{Error as ExternalError, MacAddr};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use slog::Logger;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(Error, Debug)]
pub enum BootstrapError {
    #[error("IO error: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Error starting sled agent: {0}")]
    SledError(String),

    #[error("Error deserializing toml from {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error(transparent)]
    TrustQuorum(#[from] TrustQuorumError),

    #[error("Failed to initialize bootstrap address: {err}")]
    BootstrapAddress { err: crate::illumos::zone::EnsureGzAddressError },
}

impl From<BootstrapError> for ExternalError {
    fn from(err: BootstrapError) -> Self {
        Self::internal_error(&err.to_string())
    }
}

/// The entity responsible for bootstrapping an Oxide rack.
pub(crate) struct Agent {
    /// Debug log
    log: Logger,
    /// Store the parent log - without "component = BootstrapAgent" - so
    /// other launched components can set their own value.
    parent_log: Logger,
    peer_monitor: discovery::PeerMonitor,

    /// Our share of the rack secret, if we have one.
    share: Mutex<Option<ShareDistribution>>,

    rss: Mutex<Option<RssHandle>>,
    sled_agent: Mutex<Option<SledServer>>,
    sled_config: SledConfig,
    sp: Option<SpHandle>,
}

fn get_sled_agent_request_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH)
        .join("sled-agent-request.toml")
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
) -> Result<SocketAddrV6, dladm::GetMacError> {
    let mac = Dladm::get_mac(link)?;
    Ok(mac_to_socket_addr(mac))
}

impl Agent {
    pub async fn new(
        log: Logger,
        sled_config: SledConfig,
        address: Ipv6Addr,
        sp: Option<SpHandle>,
    ) -> Result<(Self, TrustQuorumMembership), BootstrapError> {
        let ba_log = log.new(o!(
            "component" => "BootstrapAgent",
            "server" => sled_config.id.to_string(),
        ));

        // We expect this directory to exist - ensure that it does, before any
        // subsequent operations which may write configs here.
        info!(
            log, "Ensuring config directory exists";
            "path" => omicron_common::OMICRON_CONFIG_PATH,
        );
        tokio::fs::create_dir_all(omicron_common::OMICRON_CONFIG_PATH)
            .await
            .map_err(|err| BootstrapError::Io {
                message: format!(
                    "Creating config directory {}",
                    omicron_common::OMICRON_CONFIG_PATH
                ),
                err,
            })?;

        let etherstub = Dladm::create_etherstub().map_err(|e| {
            BootstrapError::SledError(format!(
                "Can't access etherstub device: {}",
                e
            ))
        })?;

        let etherstub_vnic =
            Dladm::create_etherstub_vnic(&etherstub).map_err(|e| {
                BootstrapError::SledError(format!(
                    "Can't access etherstub VNIC device: {}",
                    e
                ))
            })?;

        Zones::ensure_has_global_zone_v6_address(
            etherstub_vnic,
            address,
            "bootstrap6",
        )
        .map_err(|err| BootstrapError::BootstrapAddress { err })?;

        let peer_monitor = discovery::PeerMonitor::new(&ba_log, address)
            .map_err(|err| BootstrapError::Io {
                message: format!("Monitoring for peers from {address}"),
                err,
            })?;

        let agent = Agent {
            log: ba_log,
            parent_log: log,
            peer_monitor,
            share: Mutex::new(None),
            rss: Mutex::new(None),
            sled_agent: Mutex::new(None),
            sled_config,
            sp,
        };

        let request_path = get_sled_agent_request_path();
        let trust_quorum = if request_path.exists() {
            info!(agent.log, "Sled already configured, loading sled agent");
            let sled_request: SledAgentRequest = toml::from_str(
                &tokio::fs::read_to_string(&request_path).await.map_err(
                    |err| BootstrapError::Io {
                        message: format!(
                            "Reading subnet path from {request_path:?}"
                        ),
                        err,
                    },
                )?,
            )
            .map_err(|err| BootstrapError::Toml { path: request_path, err })?;
            agent.request_agent(&sled_request).await?;
            TrustQuorumMembership::Known(Arc::new(
                sled_request.trust_quorum_share,
            ))
        } else {
            TrustQuorumMembership::Uninitialized
        };

        Ok((agent, trust_quorum))
    }

    /// Initializes the Sled Agent on behalf of the RSS, if one has not already
    /// been initialized.
    pub async fn request_agent(
        &self,
        request: &SledAgentRequest,
    ) -> Result<SledAgentResponse, BootstrapError> {
        info!(&self.log, "Loading Sled Agent: {:?}", request);

        let sled_address = get_sled_address(request.subnet);

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

            // Bail out if this request includes a trust quorum share that
            // doesn't match ours. TODO-correctness Do we need to handle a
            // partially-initialized rack where we may have a share from a
            // previously-started-but-not-completed init process? If rerunning
            // it produces different shares this check will fail.
            if request.trust_quorum_share != *self.share.lock().await {
                let err_str = concat!(
                    "Sled Agent already running with",
                    " a different trust quorum share"
                )
                .to_string();
                return Err(BootstrapError::SledError(err_str));
            }

            return Ok(SledAgentResponse { id: server.id() });
        }
        // Server does not exist, initialize it.
        let server = SledServer::start(
            &self.sled_config,
            self.parent_log.clone(),
            sled_address,
            request.rack_id,
        )
        .await
        .map_err(|e| {
            BootstrapError::SledError(format!(
                "Could not start sled agent server: {e}"
            ))
        })?;
        maybe_agent.replace(server);
        info!(&self.log, "Sled Agent loaded; recording configuration");

        *self.share.lock().await = request.trust_quorum_share.clone();

        // Record this request so the sled agent can be automatically
        // initialized on the next boot.
        let path = get_sled_agent_request_path();
        tokio::fs::write(
            &path,
            &toml::to_string(
                &toml::Value::try_from(&request)
                    .expect("Cannot serialize request"),
            )
            .expect("Cannot convert toml to string"),
        )
        .await
        .map_err(|err| BootstrapError::Io {
            message: format!("Recording Sled Agent request to {path:?}"),
            err,
        })?;

        Ok(SledAgentResponse { id: self.sled_config.id })
    }

    /// Communicates with peers, sharing secrets, until the rack has been
    /// sufficiently unlocked.
    async fn establish_sled_quorum(
        &self,
        share: ShareDistribution,
    ) -> Result<RackSecret, BootstrapError> {
        let rack_secret = retry_notify(
            internal_service_policy(),
            || async {
                let other_agents = self.peer_monitor.peer_addrs().await;
                info!(
                    &self.log,
                    "Bootstrap: Communicating with peers: {:?}", other_agents
                );

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
                let other_agents: Vec<BootstrapAgentClient> = other_agents
                    .into_iter()
                    .map(|addr| {
                        let addr = SocketAddrV6::new(
                            addr,
                            BOOTSTRAP_AGENT_PORT,
                            0,
                            0,
                        );
                        BootstrapAgentClient::new(
                            addr,
                            &self.sp,
                            &share.member_device_id_certs,
                            self.log.new(o!(
                                "BootstrapAgentClient" => addr.to_string()),
                            ),
                        )
                    })
                    .collect();

                // TODO: Parallelize this and keep track of whose shares we've already retrieved and
                // don't resend. See https://github.com/oxidecomputer/omicron/issues/514
                let mut shares = vec![share.share.clone()];
                for agent in &other_agents {
                    let share = agent.request_share().await
                        .map_err(|e| {
                            info!(&self.log, "Bootstrap: failed to retreive share from peer: {:?}", e);
                            BackoffError::transient(e.into())
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
                    share.total_shares(),
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

    // Initializes the Rack Setup Service.
    async fn start_rss(&self, config: &Config) -> Result<(), BootstrapError> {
        if let Some(rss_config) = &config.rss_config {
            let rss = RssHandle::start_rss(
                &self.parent_log,
                rss_config.clone(),
                self.peer_monitor.observer().await,
                self.sp.clone(),
                // TODO-cleanup: Remove this arg once RSS can discover the trust
                // quorum members over the management network.
                config
                    .sp_config
                    .as_ref()
                    .map(|sp_config| sp_config.trust_quorum_members.clone())
                    .unwrap_or_default(),
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

        let maybe_share = self.share.lock().await.clone();
        if let Some(share) = maybe_share {
            self.establish_sled_quorum(share).await?;
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
