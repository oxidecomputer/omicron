// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//! * `EarlyNetworkConfigBody.rack_network_config` is not optional
//! * `EarlyNetworkConfigBody.ntp_servers` is removed (it was unused)

use crate::latest::early_networking::EarlyNetworkConfigEnvelope;
use crate::v20::early_networking::RackNetworkConfig;
use anyhow::bail;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// This is the actual configuration of EarlyNetworking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfigBody {
    // Rack network configuration as delivered from RSS or Nexus
    pub rack_network_config: RackNetworkConfig,
}

impl EarlyNetworkConfigBody {
    pub const SCHEMA_VERSION: u32 = 3;
}

impl From<EarlyNetworkConfigBody>
    for crate::v20::early_networking::EarlyNetworkConfigBody
{
    fn from(value: EarlyNetworkConfigBody) -> Self {
        Self {
            // NTP servers were also unused in v20, so it's fine that we can't
            // fill them in.
            ntp_servers: Vec::new(),
            rack_network_config: Some(value.rack_network_config),
        }
    }
}

impl TryFrom<crate::v20::early_networking::EarlyNetworkConfigBody>
    for EarlyNetworkConfigBody
{
    type Error = anyhow::Error;

    fn try_from(
        value: crate::v20::early_networking::EarlyNetworkConfigBody,
    ) -> Result<Self, Self::Error> {
        let Some(rack_network_config) = value.rack_network_config else {
            bail!(
                "EarlyNetworkConfigBody has `None` for rack_network_config; \
                 this should be impossible!",
            );
        };
        Ok(Self { rack_network_config })
    }
}

// This lives here instead of under `crate::impls::*` because we need a
// `From<EarlyNetworkConfigBody> for EarlyNetworkConfigEnvelope` implementation
// for every supported version of `EarlyNetworkConfigBody`.
impl From<&'_ EarlyNetworkConfigBody> for EarlyNetworkConfigEnvelope {
    fn from(value: &'_ EarlyNetworkConfigBody) -> Self {
        Self {
            schema_version: EarlyNetworkConfigBody::SCHEMA_VERSION,
            // We're serializing in-memory; this can only fail if
            // `EarlyNetworkConfigBody` contains types that can't be represented
            // as JSON, which (a) should never happened and (b) we should catch
            // immediately in tests.
            body: serde_json::to_value(value)
                .expect("EarlyNetworkConfigBody can be serialized as JSON"),
        }
    }
}

/// Structure for requests from Nexus to sled-agent to write a new
/// `EarlyNetworkConfigBody` into the replicated bootstore.
///
/// [`WriteNetworkConfigRequest`] INTENTIONALLY does not have a `From`
/// implementation from prior API versions. It is critically important that
/// sled-agent not attempt to rewrite old `EarlyNetworkConfigBody` types to the
/// latest version. For more about this, see the comments on the relevant
/// endpoint in `sled-agent-api`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WriteNetworkConfigRequest {
    pub generation: u64,
    pub body: EarlyNetworkConfigBody,
}
