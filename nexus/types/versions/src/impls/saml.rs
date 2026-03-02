// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for SAML types.

use crate::latest::saml::{RelativeUri, RelayState};
use anyhow::Context;
use http::Uri;
use std::str::FromStr;

impl FromStr for RelativeUri {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_string())
    }
}

impl TryFrom<Uri> for RelativeUri {
    type Error = String;

    fn try_from(uri: Uri) -> Result<Self, Self::Error> {
        if uri.host().is_none() && uri.scheme().is_none() {
            Ok(Self(uri.to_string()))
        } else {
            Err(format!("\"{}\" is not a relative URI", uri))
        }
    }
}

impl TryFrom<String> for RelativeUri {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse::<Uri>()
            .map_err(|_| format!("\"{}\" is not a relative URI", s))
            .and_then(|uri| Self::try_from(uri))
    }
}

impl RelayState {
    pub fn to_encoded(&self) -> Result<String, anyhow::Error> {
        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            serde_json::to_string(&self).context("encoding relay state")?,
        ))
    }

    pub fn from_encoded(encoded: String) -> Result<Self, anyhow::Error> {
        serde_json::from_str(
            &String::from_utf8(
                base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    encoded,
                )
                .context("base64 decoding relay state")?,
            )
            .context("creating relay state string")?,
        )
        .context("json from relay state string")
    }
}
