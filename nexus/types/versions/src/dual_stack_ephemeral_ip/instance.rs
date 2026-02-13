// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version DUAL_STACK_EPHEMERAL_IP.

use omicron_common::address::IpVersion;
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Query parameters for ephemeral IP detach operations.
#[derive(Deserialize, JsonSchema, Clone)]
pub struct EphemeralIpDetachSelector {
    /// Name or ID of the project.
    pub project: Option<NameOrId>,
    /// The IP version of the ephemeral IP to detach.
    ///
    /// Required when the instance has both IPv4 and IPv6 ephemeral IPs.
    /// If only one ephemeral IP is attached, this field may be omitted.
    pub ip_version: Option<IpVersion>,
}

/// Parameters for detaching an external IP from an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpDetach {
    Ephemeral {
        /// The IP version of the ephemeral IP to detach.
        ///
        /// Required when the instance has both IPv4 and IPv6 ephemeral IPs.
        /// If only one ephemeral IP is attached, this field may be omitted.
        #[serde(default)]
        ip_version: Option<IpVersion>,
    },
    Floating {
        floating_ip: NameOrId,
    },
}
