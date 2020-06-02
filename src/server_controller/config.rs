/*!
 * Interfaces for working with server controller configuration.
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use uuid::Uuid;

/**
 * How this `ServerController` simulates object states and transitions.
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
     * explicitly, relying on calls through `ServerControllerTestInterfaces`.
     */
    Explicit,
}

/**
 * Configuration for a server controller.
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfigServerController {
    pub id: Uuid,
    pub sim_mode: SimMode,
    pub controller_address: SocketAddr,
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,
}
