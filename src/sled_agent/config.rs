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
 * How a [`SledAgent`](`super::SledAgent`) simulates object states and
 * transitions
 */
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SimMode {
    /**
     * Indicates that asynchronous state transitions should be simulated
     * automatically using a timer to complete the transition a few seconds in
     * the future.
     */
    Auto,

    /**
     * Indicates that asynchronous state transitions should be simulated
     * explicitly, relying on calls through `sled_agent::TestInterfaces`.
     */
    Explicit,
}

/**
 * Configuration for a sled agent
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /** unique id for the sled */
    pub id: Uuid,
    /** how to simulate asynchronous Instance and Disk transitions */
    pub sim_mode: SimMode,
    /** IP address and TCP port for the OXC instance to register with */
    pub nexus_address: SocketAddr,
    /** configuration for the sled agent dropshot server */
    pub dropshot: ConfigDropshot,
    /** configuration for the sled agent debug log */
    pub log: ConfigLogging,
}
