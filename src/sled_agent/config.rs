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
 * How this `SledAgent` simulates object states and transitions.
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
     * explicitly, relying on calls through `SledAgentTestInterfaces`.
     */
    Explicit,
}

/**
 * Configuration for a sled agent.
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfigSledAgent {
    pub id: Uuid,
    pub sim_mode: SimMode,
    pub controller_address: SocketAddr,
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,
}
