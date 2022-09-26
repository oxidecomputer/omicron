// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface to a (simulated or real) SP / RoT.

use crate::config::Config as SledConfig;
use crate::config::ConfigError;
use crate::illumos;
use crate::illumos::dladm::CreateVnicError;
use crate::zone::EnsureGzAddressError;
use serde::Deserialize;
use serde::Serialize;
use slog::Logger;
use sp_sim::config::GimletConfig;
use sprockets_host::Ed25519Certificate;
use sprockets_host::Ed25519Certificates;
use sprockets_host::Ed25519PublicKey;
use sprockets_host::RotManagerHandle;
use sprockets_host::Session;
use std::net::Ipv6Addr;
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

mod simulated;

use self::simulated::SimRotTransportError;
use self::simulated::SimulatedSp;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimSpConfig {
    pub local_sp: GimletConfig,
    pub trust_quorum_members: Vec<Ed25519Certificate>,
}

impl SimSpConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        let config = toml::from_str(&contents)
            .map_err(|err| ConfigError::Parse { path: path.into(), err })?;
        Ok(config)
    }
}

// These error cases are mostly simulation-specific; the list will grow once we
// have real hardware (and may shrink if/when we remove or collapse simulated
// cases). We mark the enum `non_exhaustive` to save some pain in the future.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SpError {
    #[error("Simulated SP config specifies distinct IP addresses ({0}, {1})")]
    SimulatedSpMultipleIpAddresses(Ipv6Addr, Ipv6Addr),
    #[error("Could not access etherstub for simulated SP: {0}")]
    CreateEtherstub(illumos::ExecutionError),
    #[error("Could not access etherstub VNIC device for simulated SP: {0}")]
    CreateEtherstubVnic(CreateVnicError),
    #[error("Could not ensure IP address {addr} in global zone for simulated SP: {err}")]
    EnsureGlobalZoneAddressError { addr: Ipv6Addr, err: EnsureGzAddressError },
    #[error("Could not start simualted SP: {0}")]
    StartSimSpError(String),
    #[error("Communication with RoT failed: {0}")]
    RotCommunicationError(String),
    #[error("Failed to establish sprockets session: {0}")]
    SprocketsSessionError(String),
    #[error("Remote peer is not a member of our trust quorum")]
    PeerNotInTrustQuorum,
}

#[derive(Clone)]
pub struct SpHandle {
    inner: Inner,
}

impl SpHandle {
    /// Attempt to detect the presence of an SP.
    ///
    /// Currently the only "detection" performed is whether `sp_config` is
    /// `Some(_)`, in which case a simulated SP is started, and a handle to it
    /// is returned.
    ///
    /// A return value of `Ok(None)` means no SP is available.
    pub async fn detect(
        sp_config: Option<&GimletConfig>,
        sled_config: &SledConfig,
        log: &Logger,
    ) -> Result<Option<Self>, SpError> {
        let inner = if let Some(config) = sp_config {
            let sim_sp = SimulatedSp::start(config, sled_config, log).await?;
            Some(Inner::SimulatedSp(sim_sp))
        } else {
            None
        };
        Ok(inner.map(|inner| Self { inner }))
    }

    pub fn manufacturing_public_key(&self) -> Ed25519PublicKey {
        match &self.inner {
            Inner::SimulatedSp(sim) => sim.manufacturing_public_key(),
        }
    }

    // TODO-cleanup The error type here leaks that we only currently support
    // simulated SPs and will need work once we support a real SP.
    pub fn rot_handle(&self) -> RotManagerHandle<SimRotTransportError> {
        match &self.inner {
            Inner::SimulatedSp(sim) => sim.rot_handle(),
        }
    }

    pub fn rot_certs(&self) -> Ed25519Certificates {
        match &self.inner {
            Inner::SimulatedSp(sim) => sim.rot_certs(),
        }
    }

    pub(crate) async fn wrap_stream<T: AsyncReadWrite + 'static>(
        &self,
        stream: T,
        role: SprocketsRole,
        trust_quorum_members: Option<&[Ed25519Certificate]>,
        log: &Logger,
    ) -> Result<Session<T>, SpError> {
        // TODO-cleanup Do we want this timeout to be configurable?
        const ROT_TIMEOUT: Duration = Duration::from_secs(30);

        let session = match role {
            SprocketsRole::Client => sprockets_host::Session::new_client(
                stream,
                self.rot_handle(),
                self.rot_certs(),
                ROT_TIMEOUT,
            )
            .await
            .map_err(|e| SpError::SprocketsSessionError(e.to_string()))?,
            SprocketsRole::Server => sprockets_host::Session::new_server(
                stream,
                self.rot_handle(),
                self.rot_certs(),
                ROT_TIMEOUT,
            )
            .await
            .map_err(|e| SpError::SprocketsSessionError(e.to_string()))?,
        };

        let remote_identity = session.remote_identity();

        // Do we know who our trust quorum members are? If so, check that this
        // peer is one of them. The only time we don't know who our peers are is
        // if we're a bootstrap agent waiting for RSS to send the request to
        // initialize our sled agent; that request _contains_ the list of trust
        // quorum members, so we can't check the sender before receiving it.
        if let Some(trust_quorum_members) = trust_quorum_members {
            if !trust_quorum_members.contains(&remote_identity.certs.device_id)
            {
                return Err(SpError::PeerNotInTrustQuorum);
            }
            info!(
                log, "Confirmed peer is a member of our trust quorum";
                "peer_serial_number" => ?remote_identity.certs.serial_number,
            );
        } else {
            info!(
                log,
                "Trust quorum members unknown; bypassing membership check"
            );
        }

        info!(
            log, "Negotiated sprockets session";
            "peer_serial_number" => ?remote_identity.certs.serial_number,
        );
        Ok(session)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SprocketsRole {
    Client,
    Server,
}

pub(crate) trait AsyncReadWrite:
    AsyncRead + AsyncWrite + Send + Unpin
{
}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// Helper function to wrap a stream in a sprockets session if we have an SP, or
/// return it wrapped in an unauthenticated `BufStream` if not.
///
/// TODO-cleanup This function should be removed when we start requiring an SP
/// (even if simulated) to be present.
pub(crate) async fn maybe_wrap_stream<T: AsyncReadWrite + 'static>(
    stream: T,
    sp: &Option<SpHandle>,
    role: SprocketsRole,
    trust_quorum_members: Option<&[Ed25519Certificate]>,
    log: &Logger,
) -> Result<Box<dyn AsyncReadWrite>, SpError> {
    match sp.as_ref() {
        Some(sp) => {
            info!(log, "SP available; establishing sprockets session");
            let session =
                sp.wrap_stream(stream, role, trust_quorum_members, log).await?;
            Ok(Box::new(session))
        }
        None => {
            info!(log, "No SP available; proceeding without sprockets auth");
            Ok(Box::new(tokio::io::BufStream::new(stream)))
        }
    }
}

#[derive(Clone)]
enum Inner {
    SimulatedSp(SimulatedSp),
}
