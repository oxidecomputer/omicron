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

use crate::error::StartupError;
use crate::single_sp::SingleSp;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use slog::o;
use slog::Logger;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::Instant;

#[serde_as]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SwitchConfig {
    pub local_ignition_controller_port: usize,
    pub rpc_max_attempts: usize,
    pub rpc_per_attempt_timeout_millis: u64,
    pub location: LocationConfig,
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub port: HashMap<usize, SwitchPortConfig>,
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
    location_map: LocationMap,
}

impl ManagementSwitch {
    pub(crate) async fn new(
        config: SwitchConfig,
        discovery_deadline: Instant,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        // begin by binding to all our configured ports; insert them into a map
        // keyed by the switch port they're listening on
        let mut sockets = HashMap::with_capacity(config.port.len());
        // while we're at it, rekey `config.port` to use `SwitchPort` keys
        // instead of `usize`.
        let mut ports = HashMap::with_capacity(config.port.len());
        for (port, port_config) in config.port {
            let addr = port_config.data_link_addr;
            let socket = UdpSocket::bind(addr)
                .await
                .map_err(|err| StartupError::UdpBind { addr, err })?;

            let port = SwitchPort(port);
            sockets.insert(
                port,
                SingleSp::new(
                    socket,
                    port_config.multicast_addr,
                    config.rpc_max_attempts,
                    Duration::from_millis(
                        config.rpc_per_attempt_timeout_millis,
                    ),
                    log.new(o!("switch_port" => port.0)),
                ),
            );
            ports.insert(port, port_config);
        }

        // sanity check the local ignition controller port is bound
        let local_ignition_controller_port =
            SwitchPort(config.local_ignition_controller_port);
        if !ports.contains_key(&local_ignition_controller_port) {
            return Err(StartupError::InvalidConfig {
                reasons: vec![format!(
                    "missing local ignition controller port {}",
                    local_ignition_controller_port.0
                )],
            });
        }

        // run discovery to figure out the physical location of ourselves (and
        // therefore all SPs we talk to)
        let sockets = Arc::new(sockets);
        let location_map = LocationMap::run_discovery(
            config.location,
            ports,
            Arc::clone(&sockets),
            discovery_deadline,
            log,
        )
        .await?;

        Ok(Self { local_ignition_controller_port, location_map, sockets })
    }

    /// Get the name of our location.
    ///
    /// This matches one of the names specified as a possible location in the
    /// configuration we were given.
    pub(super) fn location_name(&self) -> &str {
        &self.location_map.location_name()
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

    pub(crate) fn switch_port(&self, id: SpIdentifier) -> Option<SwitchPort> {
        self.location_map.id_to_port(id)
    }

    pub(crate) fn switch_port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        self.location_map.port_to_id(port)
    }
}
