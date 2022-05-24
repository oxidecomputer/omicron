// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface to a (currently simulated) SP / RoT.

use crate::config::Config as SledConfig;
use crate::illumos::dladm::Dladm;
use crate::illumos::dladm::FindPhysicalLinkError;
use crate::zone::EnsureGzAddressError;
use crate::zone::Zones;
use slog::Logger;
use sp_sim::config::GimletConfig;
use sp_sim::RotRequestV1;
use sp_sim::RotResponseV1;
use sp_sim::SimulatedSp as SpSimSimulatedSp;
use sprockets_host::Ed25519Certificates;
use sprockets_host::Ed25519PublicKey;
use sprockets_host::RotManager;
use sprockets_host::RotManagerHandle;
use sprockets_host::RotOpV1;
use sprockets_host::RotResultV1;
use sprockets_host::RotTransport;
use std::collections::VecDeque;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use thiserror::Error;

// These error cases are mostly simulation-specific; the list will grow once we
// have real hardware (and may shrink if/when we remove or collapse simulated
// cases). We mark the enum `non_exhaustive` to save some pain in the future.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SpError {
    #[error("Simulated SP config specifies distinct IP addresses ({0}, {1})")]
    SimulatedSpMultipleIpAddresses(Ipv6Addr, Ipv6Addr),
    #[error("Can't access physical link, and none in config: {0}")]
    FindPhysicalLinkError(#[from] FindPhysicalLinkError),
    #[error("Could not ensure IP address {addr} in global zone for simulated SP: {err}")]
    EnsureGlobalZoneAddressError { addr: Ipv6Addr, err: EnsureGzAddressError },
    #[error("Could not start simualted SP: {0}")]
    StartSimSpError(String),
    #[error("Communication with RoT failed: {0}")]
    RotCommunicationError(String),
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
        sp_config: &Option<GimletConfig>,
        sled_config: &SledConfig,
        log: &Logger,
    ) -> Result<Option<Self>, SpError> {
        let inner = if let Some(config) = sp_config.as_ref() {
            let sim_sp = start_simulated_sp(config, sled_config, log).await?;
            Some(Inner::SimulatedSp(sim_sp))
        } else {
            None
        };
        Ok(inner.map(|inner| Self { inner }))
    }

    pub fn manufacturing_public_key(&self) -> Ed25519PublicKey {
        match &self.inner {
            Inner::SimulatedSp(sim) => sim.sp.manufacturing_public_key(),
        }
    }

    // TODO The error type here leaks that we only currently support simulated
    // SPs and will need work once we support a real SP.
    pub fn rot_handle(&self) -> RotManagerHandle<SimRotTransportError> {
        match &self.inner {
            Inner::SimulatedSp(sim) => sim.rot_handle.clone(),
        }
    }

    pub fn rot_certs(&self) -> Ed25519Certificates {
        match &self.inner {
            Inner::SimulatedSp(sim) => sim.rot_certs,
        }
    }
}

#[derive(Clone)]
enum Inner {
    SimulatedSp(SimulatedSp),
}

#[derive(Clone)]
struct SimulatedSp {
    sp: Arc<sp_sim::Gimlet>,
    rot_certs: Ed25519Certificates,
    rot_handle: RotManagerHandle<SimRotTransportError>,
}

async fn start_simulated_sp(
    sp_config: &GimletConfig,
    sled_config: &SledConfig,
    log: &Logger,
) -> Result<SimulatedSp, SpError> {
    // Is our simulated SP going to bind to addresses (acting like management
    // network IPs)?
    if let Some(bind_addrs) = sp_config.common.bind_addrs {
        // Sanity check that the sim SP config only specifies one IP address; we
        // can simulate multiple management network ports by using different TCP
        // ports.
        let sp_addr = bind_addrs[0].ip();
        for addr in bind_addrs[1..].iter().copied().chain(
            sp_config.components.iter().filter_map(|comp| comp.serial_console),
        ) {
            if sp_addr != addr.ip() {
                return Err(SpError::SimulatedSpMultipleIpAddresses(
                    *sp_addr,
                    *addr.ip(),
                ));
            }
        }

        // Ensure we have the global zone IP address we need for the SP.
        let data_link = if let Some(link) = sled_config.data_link.clone() {
            link
        } else {
            Dladm::find_physical()?
        };
        Zones::ensure_has_global_zone_v6_address(data_link, *sp_addr, "simsp")
            .map_err(|err| SpError::EnsureGlobalZoneAddressError {
                addr: *sp_addr,
                err,
            })?;
    }

    // Start up the simulated SP.
    info!(log, "starting simulated gimlet SP");
    let sp_log = log.new(o!(
        "component" => "sp-sim",
        "server" => sled_config.id.clone().to_string(),
    ));
    let sp = Arc::new(
        sp_sim::Gimlet::spawn(&sp_config, sp_log)
            .await
            .map_err(|e| SpError::StartSimSpError(e.to_string()))?,
    );

    // Start up the simulated RoT.
    info!(log, "starting simulated gimlet RoT");
    let rot_log = log.new(o!(
        "component" => "rot-sim",
        "server" => sled_config.id.clone().to_string(),
    ));
    let transport =
        SimRotTransport { sp: Arc::clone(&sp), responses: VecDeque::new() };
    let (rot_manager, rot_handle) = RotManager::new(32, transport, rot_log);

    // Spawn a thread to communicate with the RoT. In real hardware this
    // ultimately uses the UART.
    thread::Builder::new()
        .name("sim-rot".to_string())
        .spawn(move || {
            rot_manager.run();
        })
        .unwrap();

    // Ask the simulated RoT for its certs. The deadline is ignored by our
    // simulated rot transport; just pass "now".
    let rot_certs_result = rot_handle
        .call(RotOpV1::GetCertificates, Instant::now())
        .await
        .map_err(|err| SpError::RotCommunicationError(err.to_string()))?;
    let rot_certs = match rot_certs_result {
        RotResultV1::Certificates(certs) => certs,
        RotResultV1::Err(err) => {
            return Err(SpError::RotCommunicationError(format!("{err:?}")));
        }
        other => {
            return Err(SpError::RotCommunicationError(format!(
                "unexpected response to GetCertificates request: {other:?}"
            )));
        }
    };

    Ok(SimulatedSp { sp, rot_certs, rot_handle })
}

struct SimRotTransport {
    sp: Arc<sp_sim::Gimlet>,
    responses: VecDeque<RotResponseV1>,
}

#[derive(Debug, Error)]
pub enum SimRotTransportError {
    #[error("RoT sprockets error: {0}")]
    RotSprocketError(sp_sim::RotSprocketError),
    #[error("Empty recv queue (recv called more than send?)")]
    EmptyRecvQueue,
}

impl RotTransport for SimRotTransport {
    type Error = SimRotTransportError;

    fn send(
        &mut self,
        req: RotRequestV1,
        _deadline: std::time::Instant,
    ) -> Result<(), Self::Error> {
        let response = self
            .sp
            .rot_request(req)
            .map_err(SimRotTransportError::RotSprocketError)?;
        self.responses.push_back(response);
        Ok(())
    }

    fn recv(
        &mut self,
        _deadline: std::time::Instant,
    ) -> Result<RotResponseV1, Self::Error> {
        self.responses.pop_front().ok_or(SimRotTransportError::EmptyRecvQueue)
    }
}
