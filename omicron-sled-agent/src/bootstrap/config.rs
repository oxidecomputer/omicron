/*!
 * Interfaces for working with bootstrap agent configuration
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/**
 * Configuration for a bootstrap agent
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    pub id: Uuid,
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,
}
