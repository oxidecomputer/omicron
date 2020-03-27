/*!
 * Configuration for Dropshot
 */

use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;

/**
 * Dropshot configuration.  This type implements serde's Deserialize and
 * Serialize and it's exposed to consumers.  This way, consumers can declare a
 * block of their config file (whatever format it's in) as a `ConfigDropshot`,
 * parse their whole file, and then hand us the Dropshot part of the config.
 * TODO-doc add examples
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConfigDropshot {
    /** IP address and TCP port to which to bind for accepting connections */
    pub bind_address: SocketAddr,
}
