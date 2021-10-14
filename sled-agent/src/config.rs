//! Interfaces for working with sled agent configuration

use crate::common::vlan::VlanID;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use std::net::SocketAddr;
use uuid::Uuid;

/// Configuration for a sled agent
#[derive(Clone, Debug)]
pub struct Config {
    /// Unique id for the sled
    pub id: Uuid,
    /// IP address and TCP port for Nexus instance
    pub nexus_address: SocketAddr,
    /// Configuration for the sled agent dropshot server
    pub dropshot: ConfigDropshot,
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
}
