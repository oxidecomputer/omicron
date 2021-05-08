/*!
 * Interfaces for working with sled agent configuration
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use uuid::Uuid;

/**
 * Configuration for a sled agent
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /** unique id for the sled */
    pub id: Uuid,
    /** IP address and TCP port for Nexus instance to register with */
    pub nexus_address: SocketAddr,
    /** configuration for the sled agent dropshot server */
    pub dropshot: ConfigDropshot,
    /** configuration for the sled agent debug log */
    pub log: ConfigLogging,
}
