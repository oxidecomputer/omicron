// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SAML-related types for version INITIAL.

use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// This is meant as a security feature. We want to ensure we never redirect to
/// a URI on a different host.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Display)]
#[serde(try_from = "String")]
#[display("{0}")]
pub struct RelativeUri(pub(crate) String);

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RelayState {
    pub redirect_uri: Option<RelativeUri>,
}
