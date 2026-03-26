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
pub use self::location_map::LocationDescriptionConfig;
pub use self::location_map::LocationDeterminationConfig;
use self::location_map::LocationMap;
pub use self::location_map::SwitchPortConfig;
pub use self::location_map::SwitchPortDescription;
use self::location_map::ValidatedLocationConfig;
use crate::error::SpCommsError;
use crate::error::SpLookupError;
use crate::error::StartupError;
use gateway_messages::IgnitionState;
use gateway_sp_comms::BindError;
use gateway_sp_comms::HostPhase2Provider;
use gateway_sp_comms::SharedSocket;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::SpRetryConfig;
use gateway_sp_comms::default_discovery_addr;
use gateway_sp_comms::default_ereport_addr;
use gateway_sp_comms::ereport;
use gateway_sp_comms::shared_socket;
use serde::Deserialize;
use serde::Serialize;
use slog::Logger;
use slog::o;
use slog::warn;
use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SwitchConfig {
    #[serde(default = "default_udp_listen_port")]
    pub udp_listen_port: u16,
    #[serde(default = "default_ereport_listen_port")]
    pub ereport_udp_listen_port: u16,
    pub local_ignition_controller_interface: String,
    pub rpc_retry_config: RetryConfig,
    pub location: LocationConfig,
    pub port: Vec<SwitchPortDescription>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RetryConfig {
    pub per_attempt_timeout_millis: u64,
    pub max_attempts_reset: usize,
    pub max_attempts_general: usize,
}

impl From<RetryConfig> for SpRetryConfig {
    fn from(config: RetryConfig) -> Self {
        Self {
            per_attempt_timeout: Duration::from_millis(
                config.per_attempt_timeout_millis,
            ),
            max_attempts_reset: config.max_attempts_reset,
            max_attempts_general: config.max_attempts_general,
        }
    }
}

fn default_udp_listen_port() -> u16 {
    gateway_sp_comms::MGS_PORT
}

fn default_ereport_listen_port() -> u16 {
    gateway_sp_comms::ereport::MGS_PORT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct SpIdentifier {
    pub typ: SpType,
    pub slot: u16,
}

impl SpIdentifier {
    pub fn new(typ: SpType, slot: u16) -> Self {
        Self { typ, slot }
    }
}

impl From<gateway_types::component::SpIdentifier> for SpIdentifier {
    fn from(id: gateway_types::component::SpIdentifier) -> Self {
        Self { typ: id.typ.into(), slot: id.slot }
    }
}

impl From<SpIdentifier> for gateway_types::component::SpIdentifier {
    fn from(id: SpIdentifier) -> Self {
        Self { typ: id.typ.into(), slot: id.slot }
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
    _shared_socket: Option<SharedSocket<shared_socket::SingleSpMessage>>,
    // As above, we must also not drop the shared ereport socket.
    _shared_ereport_socket: Option<SharedSocket<Vec<u8>>>,
    location_map: Arc<OnceLock<Result<LocationMap, String>>>,
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

        // Convert our config into actual SP handles and sockets, keyed by
        // `SwitchPort` (newtype around an index into `config.port`).
        let mut shared_socket = None;
        let mut ereport_socket = None;
        let mut port_to_handle = Vec::with_capacity(config.port.len());
        let mut port_to_desc = Vec::with_capacity(config.port.len());
        let mut port_to_ignition_target = Vec::with_capacity(config.port.len());
        let mut interface_to_port = HashMap::with_capacity(config.port.len());
        let retry_config = config.rpc_retry_config.into();

        for (i, port_desc) in config.port.into_iter().enumerate() {
            let single_sp = match &port_desc.config {
                SwitchPortConfig::SwitchZoneInterface { interface } => {
                    // Create a shared socket if we're going to be sharing one
                    // socket among mulitple switch zone VLAN interfaces
                    let socket = match &mut shared_socket {
                        None => {
                            shared_socket = Some(
                                SharedSocket::bind(
                                    config.udp_listen_port,
                                    shared_socket::ControlPlaneAgentHandler::new(&host_phase2_provider),
                                    log.new(slog::o!("socket" => "control-plane-agent")),
                                )
                                .await?
                            );

                            shared_socket.as_ref().unwrap()
                        }

                        Some(v) => v,
                    };

                    let ereport_socket = match &mut ereport_socket {
                        None => {
                            ereport_socket = Some(
                                SharedSocket::bind(
                                    config.ereport_udp_listen_port,
                                    ereport::EreportHandler::default(),
                                    log.new(slog::o!("socket" => "ereport")),
                                )
                                .await?,
                            );

                            ereport_socket.as_ref().unwrap()
                        }

                        Some(v) => v,
                    };
                    SingleSp::new(
                        &socket,
                        ereport_socket,
                        gateway_sp_comms::SwitchPortConfig {
                            discovery_addr: default_discovery_addr(),
                            ereport_addr: default_ereport_addr(),
                            interface: interface.clone(),
                        },
                        retry_config,
                    )
                    .await
                }

                SwitchPortConfig::Simulated {
                    addr,
                    ereport_addr,
                    fake_interface,
                } => {
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
                    let ereport_bind_addr: SocketAddrV6 =
                        SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                    let ereport_socket = UdpSocket::bind(ereport_bind_addr)
                        .await
                        .map_err(|err| {
                            StartupError::BindError(BindError {
                                addr: ereport_bind_addr,
                                err,
                            })
                        })?;
                    SingleSp::new_direct_socket_for_testing(
                        socket,
                        *addr,
                        ereport_socket,
                        *ereport_addr,
                        retry_config,
                        log.new(
                            slog::o!("interface" => fake_interface.clone()),
                        ),
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
        let location_map = Arc::new(OnceLock::new());
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
            _shared_ereport_socket: ereport_socket,
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

    fn location_map(&self) -> Result<&LocationMap, SpLookupError> {
        let discovery_result = self
            .location_map
            .get()
            .ok_or(SpLookupError::DiscoveryNotYetComplete)?;
        discovery_result
            .as_ref()
            .map_err(|s| SpLookupError::DiscoveryFailed { reason: s.clone() })
    }

    /// Get the identifier of our local switch.
    pub fn local_switch(&self) -> Result<SpIdentifier, SpLookupError> {
        let location_map = self.location_map()?;
        Ok(location_map.port_to_id(self.local_ignition_controller_port))
    }

    /// Determine whether our configuration allows us to reset the specified SP.
    ///
    /// # Errors
    ///
    /// This method will fail if discovery is not yet complete (i.e., we don't
    /// know the logical identifiers of any SP yet!).
    pub fn allowed_to_reset_sp(
        &self,
        id: SpIdentifier,
    ) -> Result<bool, SpLookupError> {
        let location_map = self.location_map()?;

        let allowed = match id.typ {
            // We allow any non-sled reset.
            SpType::Switch | SpType::Power => true,
            SpType::Sled => {
                // We allow resets to any sled that isn't our local sled...
                id.slot != location_map.local_sled()
                    // ... and resets to our local sled if configured to do so.
                    || location_map.allow_local_sled_sp_reset()
            }
        };

        Ok(allowed)
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
    fn get_port(&self, id: SpIdentifier) -> Result<SwitchPort, SpLookupError> {
        let location_map = self.location_map()?;
        let port = location_map
            .id_to_port(id)
            .ok_or(SpLookupError::SpDoesNotExist(id))?;
        Ok(port)
    }

    /// Get the handle for communicating with an SP by its logical identifier.
    ///
    /// # Errors
    ///
    /// This method will fail if discovery is not yet complete (i.e., we don't
    /// know the logical identifiers of any SP yet!) or if `id` specifies an SP
    /// that doesn't exist in our discovered location map.
    pub fn sp(&self, id: SpIdentifier) -> Result<&SingleSp, SpLookupError> {
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
    ) -> Result<u8, SpLookupError> {
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
    ) -> Result<impl Iterator<Item = (SpIdentifier, &SingleSp)>, SpLookupError>
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
