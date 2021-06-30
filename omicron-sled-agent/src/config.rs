//! Interfaces for working with sled agent configuration

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use uuid::Uuid;

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// Unique id for the sled
    pub id: Uuid,
    /// IP address and TCP port for Nexus instance
    pub nexus_address: SocketAddr,
    /// Configuration for the sled agent dropshot server
    pub dropshot: ConfigDropshot,
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
}
