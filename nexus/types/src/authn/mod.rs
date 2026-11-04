// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authentication types for the Nexus API.

pub mod cookies;

use anyhow::anyhow;
use serde_with::DeserializeFromStr;
use serde_with::SerializeDisplay;
use std::fmt;

/// Identifies a particular external authentication scheme.
///
/// This is a closed set of schemes. Adding a new scheme requires updating this
/// enum, which ensures all consumers (config parsing, authn, audit logging)
/// handle it explicitly.
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, Eq, PartialEq, SerializeDisplay,
)]
pub enum SchemeName {
    /// Session cookie authentication (web console)
    SessionCookie,
    /// Access token authentication (API clients)
    AccessToken,
    /// SCIM token authentication (provisioning)
    ScimToken,
    /// Spoof authentication (development/testing only)
    Spoof,
}

impl SchemeName {
    /// String representation used in config files and logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemeName::SessionCookie => "session_cookie",
            SchemeName::AccessToken => "access_token",
            SchemeName::ScimToken => "scim_token",
            SchemeName::Spoof => "spoof",
        }
    }
}

impl fmt::Display for SchemeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SchemeName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "spoof" => Ok(SchemeName::Spoof),
            "session_cookie" => Ok(SchemeName::SessionCookie),
            "access_token" => Ok(SchemeName::AccessToken),
            "scim_token" => Ok(SchemeName::ScimToken),
            _ => Err(anyhow!("unsupported authn scheme: {:?}", s)),
        }
    }
}
