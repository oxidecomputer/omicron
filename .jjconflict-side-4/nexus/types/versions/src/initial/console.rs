// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Console API parameters for the Nexus external API.

use super::saml::RelativeUri;
use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
pub struct RestPathParam {
    pub path: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct LoginToProviderPathParam {
    pub silo_name: Name,
    pub provider_name: Name,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LoginUrlQuery {
    pub redirect_uri: Option<RelativeUri>,
}

#[derive(Deserialize, JsonSchema)]
pub struct LoginPath {
    pub silo_name: Name,
}
