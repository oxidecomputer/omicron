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
use omicron_common::packaging::sha256_digest;

use anyhow::anyhow;
use slog::Logger;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::path::Path;
use tar::Archive;
use thiserror::Error;

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
        let my_share = self.share.as_ref().unwrap().share.clone();
        let mut server = trust_quorum::Server::new(&self.log, my_share)?;
        tokio::spawn(async move { server.run().await });
        Ok(())
    }

    // In lieu of having an operator send requests to all sleds via an
    // initialization service, the sled-agent configuration may allow for the
    // automated injection of setup requests from a sled.
    async fn inject_rack_setup_service_requests(&self, config: &Config) -> Result<(), BootstrapError> {
        if let Some(rss_config) = &config.rss_config {
            info!(self.log, "Injecting RSS configuration: {:#?}", rss_config);

            let serialized_config = toml::Value::try_from(&config).expect("Cannot serialize configuration");
            let config_str = toml::to_string(&serialized_config).expect("Cannot turn config to string");

            // First, check if this request has previously been made.
            //
            // Normally, the rack setup service is run with a human-in-the-loop,
            // but with this automated injection, we need a way to determine the
            // (destructive) initialization has occurred.
            //
            // We do this by storing the configuration at "rss_config_path"
            // after successfully performing initialization.
            let rss_config_path = std::path::Path::new(crate::OMICRON_CONFIG_PATH).join("config-rss.toml");
            if rss_config_path.exists() {
                let old_config: Config = toml::from_str(&tokio::fs::read_to_string(&rss_config_path).await?)?;
                if &old_config == config {
                    info!(self.log, "RSS config already applied from: {}", rss_config_path.to_string_lossy());
                    return Ok(());
                }

                // TODO: We could potentially handle this case by deleting all
                // partitions (in preparation for applying the new
                // configuration), but at the moment it's an error.
                warn!(
                    self.log,
                    "Rack Setup Service Config was already applied, but has changed.\n
                     To re-initialize:\n
                       - Disable all Oxide services\n
                       - Delete all partitions within the attached zpool\n
                       - Delete the configuration file ({})\n
                       - Restart the sled agent\n
                    ",
                    rss_config_path.to_string_lossy()
                );
                return Err(BootstrapError::Configuration);
            }

            // Issue the initialization requests to all sleds.
            //
            // Perform the requests concurrently.
            futures::future::join_all(
                rss_config.requests.iter().map(|request| async move {
                    info!(self.log, "observing request: {:#?}", request);
                    let dur = std::time::Duration::from_secs(60);
                    let client = reqwest::ClientBuilder::new()
                        .connect_timeout(dur)
                        .timeout(dur)
                        .build()
                        .map_err(|e| BootstrapError::Api(anyhow!(e)))?;
                    let client = sled_agent_client::Client::new_with_client(
                        &format!("http://{}", request.sled_address),
                        client,
                        self.log.new(o!("SledAgentClient" => request.sled_address)),
                    );

                    info!(self.log, "sending requests...");
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
                    Ok::<(), BootstrapError>(())
                })
            ).await.into_iter().collect::<Result<Vec<()>, BootstrapError>>()?;

            // Finally, make sure the configuration is saved so we don't inject
            // the requests on the next iteration.
            tokio::fs::write(
                rss_config_path,
                config_str,
            ).await?;
        }
        Ok(())
    }

    /// Performs device initialization:
    ///
    /// - Communicates with other sled agents to establish a trust quorum if a
    /// ShareDistribution file exists on the host. Otherwise, the sled operates
    /// as a single node cluster.
    /// - Verifies, unpacks, and launches other services.
    pub async fn initialize(&self, config: &Config) -> Result<(), BootstrapError> {
        info!(&self.log, "bootstrap service initializing");

        if self.share.is_some() {
            self.run_trust_quorum_server().await?;
            self.establish_sled_quorum().await?;
        }

        self.inject_rack_setup_service_requests(config).await?;
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
