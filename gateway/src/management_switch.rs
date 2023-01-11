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
use gateway_messages::IgnitionState;
use gateway_sp_comms::HostPhase2Provider;
use gateway_sp_comms::SingleSp;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use slog::o;
use slog::warn;
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
    fn as_ignition_target(self) -> u8 {
        assert!(
            self.0 <= usize::from(u8::MAX),
            "cannot exceed 255 ignition targets / switch ports"
        );
        self.0 as u8
    }
}

#[derive(Debug)]
pub struct ManagementSwitch {
    local_ignition_controller_port: SwitchPort,
    sockets: Arc<HashMap<SwitchPort, SingleSp>>,
    location_map: Arc<OnceCell<Result<LocationMap, String>>>,
    discovery_task: JoinHandle<()>,
    log: Logger,
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
        let log = log.new(o!("component" => "ManagementSwitch"));

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
            log,
        })
    }

    /// Have we completed the discovery process to know how to map logical SP
    /// positions to switch ports?
    pub fn is_discovery_complete(&self) -> bool {
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
    pub fn location_name(&self) -> Result<&str, SpCommsError> {
        self.location_map().map(|m| m.location_name())
    }

    /// Get the handle for communicating with an SP by its switch port.
    ///
    /// This is infallible: [`SwitchPort`] is a newtype that we control, and
    /// we only hand out instances that match our configuration.
    fn port_to_sp(&self, port: SwitchPort) -> &SingleSp {
        self.sockets.get(&port).unwrap()
    }

    /// Get the switch port associated with the given logical identifier.
    ///
    /// # Errors
    ///
    /// This method will fail if discovery is not yet complete (i.e., we don't
    /// know the logical identifiers of any SP yet!) or if `id` specifies an SP
    /// that doesn't exist in our discovered location map.
    fn get_port(&self, id: SpIdentifier) -> Result<SwitchPort, SpCommsError> {
        let location_map = self.location_map()?;
        let port = location_map
            .id_to_port(id)
            .ok_or(SpCommsError::SpDoesNotExist(id))?;
        Ok(port)
    }

    /// Get the handle for communicating with an SP by its logical identifier.
    ///
    /// # Errors
    ///
    /// This method will fail if discovery is not yet complete (i.e., we don't
    /// know the logical identifiers of any SP yet!) or if `id` specifies an SP
    /// that doesn't exist in our discovered location map.
    pub fn sp(&self, id: SpIdentifier) -> Result<&SingleSp, SpCommsError> {
        let port = self.get_port(id)?;
        Ok(self.port_to_sp(port))
    }

    /// Get the ignition target number of an SP by its logical identifier.
    ///
    /// # Errors
    ///
    /// This method will fail if discovery is not yet complete (i.e., we don't
    /// know the logical identifiers of any SP yet!) or if `id` specifies an SP
    /// that doesn't exist in our discovered location map.
    pub fn ignition_target(
        &self,
        id: SpIdentifier,
    ) -> Result<u8, SpCommsError> {
        let port = self.get_port(id)?;
        Ok(port.as_ignition_target())
    }

    /// Get an iterator providing the ID and handle to communicate with every SP
    /// we know about.
    ///
    /// This function can only fail if we have not yet completed discovery (and
    /// therefore can't map our switch ports to SP identities).
    pub(crate) fn all_sps(
        &self,
    ) -> Result<impl Iterator<Item = (SpIdentifier, &SingleSp)>, SpCommsError>
    {
        let location_map = self.location_map()?;
        Ok(location_map
            .all_sp_ids()
            .map(|(port, id)| (id, self.port_to_sp(port))))
    }

    pub(crate) fn ignition_controller(&self) -> &SingleSp {
        self.port_to_sp(self.local_ignition_controller_port)
    }

    fn switch_port_from_ignition_target(
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

    /// Ask the local ignition controller for the ignition state of all SPs.
    pub(crate) async fn bulk_ignition_state(
        &self,
    ) -> Result<
        impl Iterator<Item = (SpIdentifier, IgnitionState)> + '_,
        SpCommsError,
    > {
        let controller = self.ignition_controller();
        let location_map = self.location_map()?;
        let bulk_state = controller.bulk_ignition_state().await?;

        Ok(bulk_state.into_iter().enumerate().filter_map(|(target, state)| {
            // If the SP returns an ignition target we don't have a port
            // for, discard it. This _shouldn't_ happen, but may if:
            //
            // 1. We're getting bogus messages from the SP.
            // 2. We're misconfigured and don't know about all ports.
            //
            // Case 2 may happen intentionally during development and
            // testing.
            match self.switch_port_from_ignition_target(target) {
                Some(port) => {
                    let id = location_map.port_to_id(port);
                    Some((id, state))
                }
                None => {
                    warn!(
                        self.log,
                        concat!(
                            "ignoring unknown ignition target {}",
                            " returned by ignition controller SP"
                        ),
                        target,
                    );
                    None
                }
            }
        }))
    }
}
