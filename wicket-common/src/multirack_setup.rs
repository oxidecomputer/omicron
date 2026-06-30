// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used for adopting an uninitialized rack into an existing regional
//! cluster.

use crate::rack_setup::{
    BootstrapSledDescription, GetBgpAuthKeyInfoResponse,
    UserSpecifiedRackNetworkConfig,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;

/// Input from a user for adopting an uninitialized rack into an existing
/// regional cluster.
///
/// This type is provided in the form of a TOML file uploaded via the wicket
/// CLI. It does not contain sensitive user input such as BGP keys. Those are
/// input separately.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct MultirackJoinConfigBaseUserInput {
    /// List of slot numbers only.
    pub bootstrap_slots: BTreeSet<u16>,
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
}

/// A version of the multirack join configuration which contains learned
/// bootstrap sleds and a redacted form of BGP auth keys if they exist.
///
/// This is returned to the user via wicketd and displayed in wicket.
///
/// Note that there are no optional fields here unlike in
/// `CurrentRssUserConfigInsensitive`. This is because wicketd defaults
/// to filling in an empty RSS config for backwards compatibility and
/// allows uploading BGP auth keys before actually uploading the RSS
/// config. However, since we never default to an empty version of the
/// `CurrentMultirackJoinConfig`, we can only fill in the BGP auth keys for
/// this structure after the `MultirackJoinConfigBaseUserInput` is uploaded from
/// a user.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentMultirackJoinUserConfig {
    pub bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
    pub bgp_auth_keys: GetBgpAuthKeyInfoResponse,
}
