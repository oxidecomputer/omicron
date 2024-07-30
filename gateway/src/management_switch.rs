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
pub use self::location_map::SwitchPortConfig;
pub use self::location_map::SwitchPortDescription;
use self::location_map::ValidatedLocationConfig;
use crate::error::SpCommsError;
use crate::error::StartupError;
use gateway_messages::IgnitionState;
use gateway_sp_comms::default_discovery_addr;
use gateway_sp_comms::BindError;
use gateway_sp_comms::HostPhase2Provider;
use gateway_sp_comms::SharedSocket;
use gateway_sp_comms::SingleSp;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serde::Serialize;
use slog::o;
use slog::warn;
use slog::Logger;
use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SwitchConfig {
    #[serde(default = "default_udp_listen_port")]
    pub udp_listen_port: u16,
    pub local_ignition_controller_interface: String,
    pub rpc_max_attempts: usize,
    pub rpc_per_attempt_timeout_millis: u64,
    pub location: LocationConfig,
    pub port: Vec<SwitchPortDescription>,
}

fn default_udp_listen_port() -> u16 {
    gateway_sp_comms::MGS_PORT
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

impl From<gateway_types::component::SpIdentifier> for SpIdentifier {
    fn from(id: gateway_types::component::SpIdentifier) -> Self {
        Self {
            typ: id.typ.into(),
            // id.slot may come from an untrusted source, but usize >= 32 bits
            // on any platform that will run this code, so unwrap is fine
            slot: usize::try_from(id.slot).unwrap(),
        }
    }
}

impl From<SpIdentifier> for gateway_types::component::SpIdentifier {
    fn from(id: SpIdentifier) -> Self {
        Self {
            typ: id.typ.into(),
            // id.slot comes from a trusted source (crate::management_switch)
            // and will not exceed u32::MAX
            slot: u32::try_from(id.slot).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpType {
    Switch,
    Sled,
    Power,
}

impl From<gateway_types::component::SpType> for SpType {
    fn from(typ: gateway_types::component::SpType) -> Self {
        match typ {
            gateway_types::component::SpType::Sled => Self::Sled,
            gateway_types::component::SpType::Power => Self::Power,
            gateway_types::component::SpType::Switch => Self::Switch,
        }
    }
}

impl From<SpType> for gateway_types::component::SpType {
    fn from(typ: SpType) -> Self {
        match typ {
            SpType::Sled => Self::Sled,
            SpType::Power => Self::Power,
            SpType::Switch => Self::Switch,
        }
    }
}

// We derive `Serialize` to be able to send `SwitchPort`s to usdt probes, but
// critically we do _not_ implement `Deserialize` - the only way to construct a
// `SwitchPort` should be to receive one from a `ManagementSwitch`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize,
)]
pub(crate) struct SwitchPort(usize);

#[derive(Debug)]
pub struct ManagementSwitch {
    local_ignition_controller_port: SwitchPort,
    // Both `port_to_*` fields are "keyed" by `SwitchPort`, which are newtype
    // wrappers around an index into those vecs. Both are guaranteed to have
    // length equal to our config file's `[[switch.port]]` section, and we only
    // ever create and hand out `SwitchPort` instances with indices in-range for
    // that length.
    port_to_handle: Arc<Vec<SingleSp>>,
    port_to_ignition_target: Vec<u8>,
    // We create a shared socket in real configs, but never use it after
    // `new()`. We need to hold onto it to prevent it being dropped, though:
    // When it's dropped, it cancels the background tokio task that loops on
    // that socket receiving incoming packets.
    _shared_socket: Option<SharedSocket>,
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
    pub(crate) async fn new<T: HostPhase2Provider>(
        config: SwitchConfig,
        host_phase2_provider: &Arc<T>,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        let log = log.new(o!("component" => "ManagementSwitch"));

        // Skim over our port configs - either all should be simulated or all
        // should be switch zone interfaces. Reject a mix.
        let mut shared_socket = None;
        for (i, port_desc) in config.port.iter().enumerate() {
            if i == 0 {
                // First item: either create a shared socket (if we're going to
                // share one socket among multiple switch zone VLAN interfaces)
                // or leave it as `None` (if we're going to be connecting to
                // simulated SPs by address).
                if let SwitchPortConfig::SwitchZoneInterface { .. } =
                    &port_desc.config
                {
                    shared_socket = Some(
                        SharedSocket::bind(
                            config.udp_listen_port,
                            Arc::clone(&host_phase2_provider),
                            log.clone(),
                        )
                        .await?,
                    );
                }
            } else {
                match (&shared_socket, &port_desc.config) {
                    // OK - config is consistent
                    (Some(_), SwitchPortConfig::SwitchZoneInterface { .. })
                    | (None, SwitchPortConfig::Simulated { .. }) => (),

                    // not OK - config is mixed
                    (None, SwitchPortConfig::SwitchZoneInterface { .. })
                    | (Some(_), SwitchPortConfig::Simulated { .. }) => {
                        return Err(StartupError::InvalidConfig {
                            reasons: vec![concat!(
                                "switch port contains a mixture of `simulated`",
                                " and `switch-zone-interface`"
                            )
                            .to_string()],
                        });
                    }
                }
            }
        }

        // Convert our config into actual SP handles and sockets, keyed by
        // `SwitchPort` (newtype around an index into `config.port`).
        let mut port_to_handle = Vec::with_capacity(config.port.len());
        let mut port_to_desc = Vec::with_capacity(config.port.len());
        let mut port_to_ignition_target = Vec::with_capacity(config.port.len());
        let mut interface_to_port = HashMap::with_capacity(config.port.len());
        for (i, port_desc) in config.port.into_iter().enumerate() {
            let per_attempt_timeout =
                Duration::from_millis(config.rpc_per_attempt_timeout_millis);
            let single_sp = match &port_desc.config {
                SwitchPortConfig::SwitchZoneInterface { interface } => {
                    SingleSp::new(
                        shared_socket.as_ref().unwrap(),
                        gateway_sp_comms::SwitchPortConfig {
                            discovery_addr: default_discovery_addr(),
                            interface: interface.clone(),
                        },
                        config.rpc_max_attempts,
                        per_attempt_timeout,
                    )
                    .await
                }
                SwitchPortConfig::Simulated { addr, .. } => {
                    // Bind a new socket for each simulated switch port.
                    let bind_addr: SocketAddrV6 =
                        SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                    let socket =
                        UdpSocket::bind(bind_addr).await.map_err(|err| {
                            StartupError::BindError(BindError {
                                addr: bind_addr,
                                err,
                            })
                        })?;
                    SingleSp::new_direct_socket_for_testing(
                        socket,
                        *addr,
                        config.rpc_max_attempts,
                        per_attempt_timeout,
                        log.clone(),
                    )
                }
            };

            let port = SwitchPort(i);
            interface_to_port
                .insert(port_desc.config.interface().to_string(), port);
            port_to_ignition_target.push(port_desc.ignition_target);
            port_to_handle.push(single_sp);
            port_to_desc.push(port_desc);
        }

        // Ensure the local ignition interface was listed in config.port
        let local_ignition_controller_port = interface_to_port
            .get(&config.local_ignition_controller_interface)
            .copied()
            .ok_or_else(|| StartupError::InvalidConfig {
                reasons: vec![format!(
                    "missing port local ignition controller {}",
                    config.local_ignition_controller_interface
                )],
            })?;

        // Start running discovery to figure out the physical location of
        // ourselves (and therefore all SPs we talk to). We don't block waiting
        // for this, but we won't be able to service requests until it
        // completes (because we won't be able to map "the SP of sled 7" to a
        // correct switch port).
        let port_to_handle = Arc::new(port_to_handle);
        let location_map = Arc::new(OnceCell::new());
        let discovery_task = {
            let log = log.clone();
            let port_to_handle = Arc::clone(&port_to_handle);
            let location_map = Arc::clone(&location_map);
            let config = ValidatedLocationConfig::validate(
                &port_to_desc,
                &interface_to_port,
                config.location,
            )?;

            tokio::spawn(async move {
                let result = LocationMap::run_discovery(
                    config,
                    port_to_desc,
                    port_to_handle,
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
            _shared_socket: shared_socket,
            port_to_handle,
            port_to_ignition_target,
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

    /// Get the identifier of our local switch.
    pub fn local_switch(&self) -> Result<SpIdentifier, SpCommsError> {
        let location_map = self.location_map()?;
        Ok(location_map.port_to_id(self.local_ignition_controller_port))
    }

    /// Get the handle for communicating with an SP by its switch port.
    ///
    /// This is infallible: [`SwitchPort`] is a newtype that we control, and
    /// we only hand out instances that match our configuration.
    fn port_to_sp(&self, port: SwitchPort) -> &SingleSp {
        &self.port_to_handle[port.0]
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
        Ok(self.port_to_ignition_target[port.0])
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
        // We could build an ignition target -> port map, but for the size of
        // this vec (~36) a scan is probably fine.
        for (i, port_target) in self.port_to_ignition_target.iter().enumerate()
        {
            if target == usize::from(*port_target) {
                return Some(SwitchPort(i));
            }
        }
        None
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
        let bulk_state =
            controller.bulk_ignition_state().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed {
                    sp: location_map
                        .port_to_id(self.local_ignition_controller_port),
                    err,
                }
            })?;

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
                        "ignoring unknown ignition target {target} \
                         returned by ignition controller SP",
                    );
                    None
                }
            }
        }))
    }
}
