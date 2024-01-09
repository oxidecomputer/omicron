// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::updates::ConfigUpdates;
use camino::Utf8Path;
use dropshot::ConfigDropshot;
use serde::Deserialize;
use serde::Serialize;
pub use sled_hardware::Baseboard;
use std::net::Ipv6Addr;
use std::net::{IpAddr, SocketAddr};
use uuid::Uuid;

/// The reported amount of hardware threads for an emulated sled agent.
pub const TEST_HARDWARE_THREADS: u32 = 16;
/// The reported amount of physical RAM for an emulated sled agent.
pub const TEST_PHYSICAL_RAM: u64 = 32 * (1 << 30);
/// The reported amount of VMM reservoir RAM for an emulated sled agent.
pub const TEST_RESERVOIR_RAM: u64 = 16 * (1 << 30);

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
    pub baseboard: Baseboard,
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
    /// configuration for the sled agent's storage
    pub storage: ConfigStorage,
    /// configuration for the sled agent's updates
    pub updates: ConfigUpdates,
    /// configuration to emulate the sled agent's hardware
    pub hardware: ConfigHardware,
}

impl Config {
    pub fn for_testing(
        id: Uuid,
        sim_mode: SimMode,
        nexus_address: Option<SocketAddr>,
        update_directory: Option<&Utf8Path>,
    ) -> Config {
        // This IP range is guaranteed by RFC 6666 to discard traffic.
        // For tests that don't use a Nexus, we use this address to simulate a
        // non-functioning Nexus.
        let nexus_address =
            nexus_address.unwrap_or_else(|| "[100::1]:12345".parse().unwrap());
        // If the caller doesn't care to provide a directory in which to put
        // updates, make up a path that doesn't exist.
        let update_directory =
            update_directory.unwrap_or_else(|| "/nonexistent".into());
        Config {
            id,
            sim_mode,
            nexus_address,
            dropshot: ConfigDropshot {
                bind_address: SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0),
                request_body_max_bytes: 1024 * 1024,
                ..Default::default()
            },
            storage: ConfigStorage {
                zpools: vec![],
                ip: IpAddr::from(Ipv6Addr::LOCALHOST),
            },
            updates: ConfigUpdates {
                zone_artifact_path: update_directory.to_path_buf(),
            },
            hardware: ConfigHardware {
                hardware_threads: TEST_HARDWARE_THREADS,
                physical_ram: TEST_PHYSICAL_RAM,
                reservoir_ram: TEST_RESERVOIR_RAM,
                baseboard: Baseboard::Gimlet {
                    identifier: format!("sim-{}", id),
                    model: String::from("sim-gimlet"),
                    revision: 3,
                },
            },
        }
    }
}
