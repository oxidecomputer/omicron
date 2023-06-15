// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::updates::ConfigUpdates;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::net::{IpAddr, SocketAddr};
use uuid::Uuid;

/// How a [`SledAgent`](`super::sled_agent::SledAgent`) simulates object states and
/// transitions
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SimMode {
    /// Indicates that asynchronous state transitions should be simulated
    /// automatically using a timer to complete the transition a few seconds in
    /// the future.
    Auto,

    /// Indicates that asynchronous state transitions should be simulated
    /// explicitly, relying on calls through `sled_agent::TestInterfaces`.
    Explicit,
}

/// Configuration for a simulated zpool.
///
/// Currently, each zpool will receive a single Crucible Dataset.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ConfigZpool {
    /// The size of the Zpool in bytes.
    pub size: u64,
}

/// Configuration describing simulated storage.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ConfigStorage {
    pub zpools: Vec<ConfigZpool>,
    pub ip: IpAddr,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ConfigHardware {
    pub hardware_threads: u32,
    pub physical_ram: u64,
    pub reservoir_ram: u64,
}

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// unique id for the sled
    pub id: Uuid,
    /// how to simulate asynchronous Instance and Disk transitions
    pub sim_mode: SimMode,
    /// IP address and TCP port for Nexus instance to register with
    pub nexus_address: SocketAddr,
    /// configuration for the sled agent dropshot server
    pub dropshot: ConfigDropshot,
    /// configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// configuration for the sled agent's storage
    pub storage: ConfigStorage,
    /// configuration for the sled agent's updates
    pub updates: ConfigUpdates,
    /// configuration to emulate the sled agent's hardware
    pub hardware: ConfigHardware,
}
