// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related APIs.

use super::discovery;
use super::spdm::SpdmError;
use super::trust_quorum::{self, RackSecret};
use super::views::ShareResponse;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use omicron_common::packaging::sha256_digest;

use slog::Logger;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::Path;
use tar::Archive;
use thiserror::Error;
use vsss_rs::Share;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(Error, Debug)]
pub enum BootstrapError {
    #[error("Cannot deserialize TOML file")]
    Toml(#[from] toml::de::Error),

    #[error("Unexpected digest for service {0}")]
    UnexpectedDigest(String),

    #[error("Error accessing filesystem: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error configuring SMF: {0}")]
    SmfConfig(#[from] smf::ConfigError),

    #[error("Error modifying SMF service: {0}")]
    SmfAdm(#[from] smf::AdmError),

    #[error("Error making HTTP request")]
    Api(#[from] anyhow::Error),

    #[error("Error running SPDM protocol: {0}")]
    Spdm(#[from] SpdmError),

    #[error("Not enough peers to unlock storage")]
    NotEnoughPeers,

    #[error("Bincode (de)serialization error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),

    #[error("JSON (de)serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid secret share received from {0}")]
    InvalidShare(SocketAddr),

    #[error("Invalid message received from {0}")]
    InvalidMsg(SocketAddr),

    #[error("Rack secret construction failed")]
    RackSecretConstructionFailed,
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
    peer_monitor: discovery::PeerMonitor,
    trust_quorum_config: trust_quorum::Config,
}

impl Agent {
    pub fn new(
        log: Logger,
        rack_secret_dir: &str,
    ) -> Result<Self, BootstrapError> {
        let peer_monitor = discovery::PeerMonitor::new(&log)?;
        let trust_quorum_config = trust_quorum::Config::read(&rack_secret_dir)?;
        Ok(Agent { log, peer_monitor, trust_quorum_config })
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
    ///
    /// - This method retries until [`UNLOCK_THRESHOLD`] other agents are
    /// online, and have successfully responded to "share requests".
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

                // "-1" to account for ourselves.
                if other_agents.len() < self.trust_quorum_config.threshold - 1 {
                    warn!(
                        &self.log,
                        "Not enough peers to start establishing quorum"
                    );
                    return Err(BackoffError::Transient(
                        BootstrapError::NotEnoughPeers,
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
                            self.trust_quorum_config.verifier.clone(),
                            addr,
                        )
                    })
                    .collect();

                // TODO: Parallelize this.
                // TODO: Keep track of who's shares we've already retrieved and
                // don't resend.
                let mut shares = Vec::<Share>::new();
                let my_share_index = self.trust_quorum_config.sled_index;
                shares.push(
                    self.trust_quorum_config.shares[my_share_index].clone(),
                );
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
                    self.trust_quorum_config.threshold,
                    self.trust_quorum_config.total_shares,
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
                    BackoffError::Transient(
                        BootstrapError::RackSecretConstructionFailed,
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

    async fn launch_local_services(&self) -> Result<(), BootstrapError> {
        let tar_source = Path::new("/opt/oxide");
        let destination = Path::new("/opt/oxide");
        // TODO-correctness: Validation should come from ROT, not local file.
        let digests: HashMap<String, Vec<u8>> = toml::from_str(
            &std::fs::read_to_string(tar_source.join("digest.toml"))?,
        )?;

        // TODO-correctness: Nexus may not be enabled on all racks.
        // Some decision-making logic should be used here to make this
        // conditional.
        //
        // Presumably, we'd try to contact a Nexus elsewhere
        // on the rack, or use the unlocked local storage to remember
        // a decision from the previous boot.
        self.launch(&digests, &tar_source, &destination, "nexus")?;

        // TODO-correctness: The same note as above applies to oximeter.
        self.launch(&digests, &tar_source, &destination, "oximeter")?;

        // Note that we extract the propolis-server, but do not launch it.
        // This is the responsibility of the sled agent in response to requests
        // from Nexus.
        self.extract(&digests, &tar_source, &destination, "propolis-server")?;

        Ok(())
    }

    async fn run_trust_quorum_server(&self) -> Result<(), BootstrapError> {
        let my_share_index = self.trust_quorum_config.sled_index;
        let my_share = self.trust_quorum_config.shares[my_share_index].clone();
        let mut server = trust_quorum::Server::new(&self.log, my_share)?;
        tokio::spawn(async move { server.run().await });
        Ok(())
    }

    /// Performs device initialization:
    ///
    /// - Communicates with other sled agents to establish a trust quorum.
    /// - Verifies, unpacks, and launches other services.
    pub async fn initialize(&self) -> Result<(), BootstrapError> {
        info!(&self.log, "bootstrap service initializing");

        if self.trust_quorum_config.enabled {
            self.run_trust_quorum_server().await?;
            self.establish_sled_quorum().await?;
        }

        self.launch_local_services().await?;

        Ok(())
    }

    fn launch<S, P1, P2>(
        &self,
        digests: &HashMap<String, Vec<u8>>,
        tar_source: P1,
        destination: P2,
        service: S,
    ) -> Result<(), BootstrapError>
    where
        S: AsRef<str>,
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        self.extract(digests, tar_source, destination, &service)?;
        self.enable_service(service)
    }

    // Verify and unpack a service.
    // NOTE: Does not enable the service.
    fn extract<S, P1, P2>(
        &self,
        digests: &HashMap<String, Vec<u8>>,
        tar_source: P1,
        destination: P2,
        service: S,
    ) -> Result<(), BootstrapError>
    where
        S: AsRef<str>,
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let tar_source = tar_source.as_ref();
        let destination = destination.as_ref();
        let service = service.as_ref();

        info!(&self.log, "Extracting {} Service", service);
        let tar_name = format!("{}.tar", service);
        let tar_path = tar_source.join(&tar_name);

        let digest_expected = digests.get(service).ok_or_else(|| {
            BootstrapError::UnexpectedDigest(format!(
                "Missing digest for {}",
                service
            ))
        })?;

        // TODO: The tarfile could hypothetically be modified between
        // calculating the digest and extracting the archive.
        // Do we care? Is this a plausible threat, and would it be
        // worthwhile to load the files into memory to attempt to
        // isolate write access?
        let mut tar_file = File::open(&tar_path)?;
        let digest_actual = sha256_digest(&mut tar_file)?;
        if digest_expected.as_slice() != digest_actual.as_ref() {
            return Err(BootstrapError::UnexpectedDigest(service.into()));
        }

        info!(&self.log, "Verified {} Service", service);
        self.extract_archive(&mut tar_file, destination.join(service))
    }

    // Given a verified archive file, unpack it to a location.
    fn extract_archive<P>(
        &self,
        file: &mut File,
        destination: P,
    ) -> Result<(), BootstrapError>
    where
        P: AsRef<Path>,
    {
        file.seek(SeekFrom::Start(0))?;

        // Clear the destination directory.
        //
        // It's likely these files will exist from a prior boot, but we only
        // want to validate / extract files from this current boot sequence.
        // Ignore errors; the directory might not have previously existed.
        let _ = std::fs::remove_dir_all(destination.as_ref());
        std::fs::create_dir_all(destination.as_ref())?;

        let mut archive = Archive::new(file);
        archive.unpack(destination.as_ref())?;
        Ok(())
    }

    // Given a service which has already been verified and unpacked,
    // launch it within the SMF system.
    fn enable_service<S: AsRef<str>>(
        &self,
        service: S,
    ) -> Result<(), BootstrapError> {
        info!(&self.log, "Enabling service: {}", service.as_ref());
        let manifest =
            format!("/opt/oxide/{}/pkg/manifest.xml", service.as_ref());

        // Import and enable the service as distinct steps.
        //
        // This allows the service to remain "transient", which avoids
        // it being auto-initialized by SMF across reboots.
        // Instead, the sled agent remains responsible for verifying
        // and enabling the services on each access.
        smf::Config::import().run(manifest)?;
        smf::Adm::new()
            .enable()
            .synchronous()
            .temporary()
            .run(smf::AdmSelection::ByPattern(&[service]))?;
        Ok(())
    }
}
