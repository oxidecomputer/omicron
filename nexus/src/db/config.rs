/*!
 * Nexus database configuration
 */

use omicron_common::config::PostgresConfigWithUrl;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;

/**
 * Nexus database configuration
 */
#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /** database url */
    #[serde_as(as = "DisplayFromStr")]
    pub url: PostgresConfigWithUrl,
}
