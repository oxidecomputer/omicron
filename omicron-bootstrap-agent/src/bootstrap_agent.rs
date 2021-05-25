use crate::bootstrap_agent_client::Client as BootstrapClient;
use omicron_common::error::ApiError;
use omicron_common::model::BootstrapAgentShareResponse;
use omicron_common::packaging::sha256_digest;

use slog::Logger;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::net::SocketAddr;
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
    Api(#[from] ApiError),
}

/// The entity responsible for bootstrapping an Oxide rack.
pub struct BootstrapAgent {
    /// Debug log
    log: Logger,
}

impl BootstrapAgent {
    pub fn new(log: Logger) -> Self {
        BootstrapAgent { log }
    }

    /// Implements the "request share" API.
    pub async fn request_share(
        &self,
        identity: Vec<u8>,
    ) -> Result<BootstrapAgentShareResponse, ApiError> {
        // TODO-correctness: Validate identity, return whatever
        // information is necessary to establish trust quorum.
        //
        // This current implementation is a placeholder.
        info!(&self.log, "request_share, received identity: {:x?}", identity);

        Ok(BootstrapAgentShareResponse { shared_secret: vec![] })
    }

    /// Performs device initialization:
    ///
    /// - TODO: Communicates with other bootstrap services to establish
    /// a trust quorum.
    /// - Verifies, unpacks, and launches the sled agent and Nexus.
    pub async fn initialize(
        &self,
        other_agents: Vec<SocketAddr>,
    ) -> Result<(), BootstrapError> {
        info!(&self.log, "bootstrap service initializing");
        // TODO-correctness:
        // - Establish trust quorum.
        // - Once this is done, "unlock" local storage
        //
        // The current implementation sends a stub request to all known
        // bootstrap agents, but does not actually create a quorum / unlock
        // anything.
        let other_agents: Vec<BootstrapClient> = other_agents
            .into_iter()
            .map(|addr| {
                let addr_str = addr.to_string();
                BootstrapClient::new(
                    addr,
                    self.log.new(o!(
                        "Address" => addr_str,
                    )),
                )
            })
            .collect();
        for agent in &other_agents {
            agent.request_share(vec![]).await?;
        }

        let tar_source = Path::new("/opt/oxide");
        let destination = Path::new("/opt/oxide");

        // TODO-correctness: Validation should come from ROT, not local file.
        let digests: HashMap<String, Vec<u8>> = toml::from_str(
            &std::fs::read_to_string(tar_source.join("digest.toml"))?,
        )?;

        self.launch(&digests, &tar_source, &destination, "sled-agent")?;

        // TODO-correctness: Nexus may not be enabled on all racks.
        // Some decision-making logic should be used here to make this
        // conditional.
        //
        // Presumably, we'd try to contact a Nexus elsewhere
        // on the rack, or use the unlocked local storage to remember
        // a decision from the previous boot.
        self.launch(&digests, &tar_source, &destination, "nexus")?;

        // TODO-correctness: The same note as above applies to oximeter.
        self.launch(&digests, &tar_source, &destination, "oximeter")
    }

    // Verify, unpack, and enable a service.
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
        let tar_source = tar_source.as_ref();
        let destination = destination.as_ref();
        let service = service.as_ref();

        info!(&self.log, "Launching {} Service", service);
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
        self.extract_archive(&mut tar_file, destination.join(service))?;
        self.enable_service(service)
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
        let manifest = format!(
            "/opt/oxide/{}/smf/{}/manifest.xml",
            service.as_ref(),
            service.as_ref()
        );

        // Import and enable the service as distinct steps.
        //
        // This allows the service to remain "transient", which avoids
        // it being auto-initialized by SMF across reboots.
        // Instead, the bootstrap agent remains responsible for verifying
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
