// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//!
//! Long-running task responsible for tracking the topology of the SPs connected
//! to the management network switch ports.
//!
//! See RFD 250 for details.
//!

mod location_map;

pub use self::location_map::LocationConfig;
pub use self::location_map::LocationDeterminationConfig;
use self::location_map::LocationMap;
pub use self::location_map::SwitchPortDescription;
use self::location_map::ValidatedLocationConfig;

use crate::error::ConfigError;
use crate::error::SpCommsError;
use gateway_sp_comms::HostPhase2Provider;
use gateway_sp_comms::SingleSp;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use slog::o;
use slog::Logger;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[serde_as]
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct SwitchConfig {
    pub local_ignition_controller_port: usize,
    pub rpc_max_attempts: usize,
    pub rpc_per_attempt_timeout_millis: u64,
    pub location: LocationConfig,
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub port: HashMap<usize, SwitchPortDescription>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct SpIdentifier {
    pub typ: SpType,
    pub slot: usize,
}

impl SpIdentifier {
    pub fn new(typ: SpType, slot: usize) -> Self {
        Self { typ, slot }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpType {
    Switch,
    Sled,
    Power,
}

// We derive `Serialize` to be able to send `SwitchPort`s to usdt probes, but
// critically we do _not_ implement `Deserialize` - the only way to construct a
// `SwitchPort` should be to receive one from a `ManagementSwitch`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize,
)]
pub(crate) struct SwitchPort(usize);

impl SwitchPort {
    pub(crate) fn as_ignition_target(self) -> u8 {
        // TODO should we use a u16 to describe ignition targets instead? rack
        // v1 is limited to 36, unclear what ignition will look like in future
        // products
        assert!(
            self.0 <= usize::from(u8::MAX),
            "cannot exceed 255 ignition targets / switch ports"
        );
        self.0 as u8
    }
}

#[derive(Debug)]
pub(crate) struct ManagementSwitch {
    local_ignition_controller_port: SwitchPort,
    sockets: Arc<HashMap<SwitchPort, SingleSp>>,
    location_map: Arc<OnceCell<Result<LocationMap, String>>>,
    discovery_task: JoinHandle<()>,
}

impl Drop for ManagementSwitch {
    fn drop(&mut self) {
        // We should only be dropped in tests, in general, but dont' leave a
        // stray spawned task running (if it still is).
        self.discovery_task.abort();
    }
}

impl ManagementSwitch {
    pub(crate) async fn new<T: HostPhase2Provider + Clone>(
        config: SwitchConfig,
        host_phase2_provider: T,
        log: &Logger,
    ) -> Result<Self, ConfigError> {
        // begin by binding to all our configured ports; insert them into a map
        // keyed by the switch port they're listening on
        let mut sockets = HashMap::with_capacity(config.port.len());
        // while we're at it, rekey `config.port` to use `SwitchPort` keys
        // instead of `usize`.
        let mut ports = HashMap::with_capacity(config.port.len());
        for (port, port_config) in config.port {
            let single_sp = SingleSp::new(
                port_config.config.clone(),
                config.rpc_max_attempts,
                Duration::from_millis(config.rpc_per_attempt_timeout_millis),
                host_phase2_provider.clone(),
                log.new(o!("switch_port" => port)),
            );
            let port = SwitchPort(port);

            sockets.insert(port, single_sp);
            ports.insert(port, port_config);
        }

        // sanity check the local ignition controller port is bound
        let local_ignition_controller_port =
            SwitchPort(config.local_ignition_controller_port);
        if !ports.contains_key(&local_ignition_controller_port) {
            return Err(ConfigError::InvalidConfig {
                reasons: vec![format!(
                    "missing local ignition controller port {}",
                    local_ignition_controller_port.0
                )],
            });
        }

        // Start running discovery to figure out the physical location of
        // ourselves (and therefore all SPs we talk to). We don't block waiting
        // for this, but we won't be able to service requests until it
        // completes (because we won't be able to map "the SP of sled 7" to a
        // correct switch port).
        let sockets = Arc::new(sockets);
        let location_map = Arc::new(OnceCell::new());
        let discovery_task = {
            let log = log.clone();
            let sockets = Arc::clone(&sockets);
            let location_map = Arc::clone(&location_map);
            let config =
                ValidatedLocationConfig::try_from((&ports, config.location))?;

            tokio::spawn(async move {
                let result = LocationMap::run_discovery(
                    config,
                    ports,
                    Arc::clone(&sockets),
                    &log,
                )
                .await;

                // We are the only setter of the `location_map` once cell; all
                // other access in this file are `get()`s, so we can unwrap.
                location_map.set(result).unwrap();
            })
        };

        Ok(Self {
            local_ignition_controller_port,
            location_map,
            sockets,
            discovery_task,
        })
    }

    /// Have we completed the discovery process to know how to map logical SP
    /// positions to switch ports?
    pub(super) fn is_discovery_complete(&self) -> bool {
        self.location_map.get().is_some()
    }

    fn location_map(&self) -> Result<&LocationMap, SpCommsError> {
        let discovery_result = self
            .location_map
            .get()
            .ok_or(SpCommsError::DiscoveryNotYetComplete)?;
        discovery_result
            .as_ref()
            .map_err(|s| SpCommsError::DiscoveryFailed { reason: s.clone() })
    }

    /// Get the name of our location.
    ///
    /// This matches one of the names specified as a possible location in the
    /// configuration we were given.
    pub(super) fn location_name(&self) -> Result<&str, SpCommsError> {
        self.location_map().map(|m| m.location_name())
    }

    /// Get the socket to use to communicate with an SP and the socket address
    /// of that SP.
    pub(crate) fn sp(&self, port: SwitchPort) -> Option<&SingleSp> {
        self.sockets.get(&port)
    }

    /// Get the socket connected to the local ignition controller.
    pub(crate) fn ignition_controller(&self) -> Option<&SingleSp> {
        self.sp(self.local_ignition_controller_port)
    }

    pub(crate) fn switch_port_from_ignition_target(
        &self,
        target: usize,
    ) -> Option<SwitchPort> {
        let port = SwitchPort(target);
        if self.sockets.contains_key(&port) {
            Some(port)
        } else {
            None
        }
    }

    pub(crate) fn switch_port(
        &self,
        id: SpIdentifier,
    ) -> Result<Option<SwitchPort>, SpCommsError> {
        self.location_map().map(|m| m.id_to_port(id))
    }

    pub(crate) fn switch_port_to_id(
        &self,
        port: SwitchPort,
    ) -> Result<SpIdentifier, SpCommsError> {
        self.location_map().map(|m| m.port_to_id(port))
    }
}
