// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * `EarlyNetworkConfigEnvelope` replaces `EarlyNetworkConfig`. The `body` is
//!   now stored as an opaque JSON blob, allowing us to first deserialize the
//!   envelope, inspect `schema_version`, then deserialize `body` with the
//!   appropriate `EarlyNetworkConfigBody` type. This will prevent the need to
//!   rev `EarlyNetworkConfigEnvelope` every time `EarlyNetworkConfigBody`
//!   changes, and makes parsing more precise (we know the exact version of any
//!   serialized `EarlyNetworkConfigBody` kept inside an envelope).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from
/// `RackInitializeRequest` necessary for use beyond RSS.
/// This is just for the initial rack configuration and cold boot purposes.
/// Updates come from Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfigEnvelope {
    // The current generation number of data as stored in CRDB.
    // The initial generation is set during RSS time and then only mutated
    // by Nexus.
    pub(crate) generation: u64,

    // Which version of `EarlyNetworkConfigBody` is serialized into `body`.
    pub(crate) schema_version: u32,

    // The actual early network configuration details.
    //
    // These are a serialized `EarlyNetworkConfigBody` of some version. We must
    // inspect `schema_version` to know how to interpret this value.
    pub(crate) body: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum EarlyNetworkConfigError {
    #[error("failed to serialize body as JSON (this should be impossible!)")]
    SerializeBody(#[source] serde_json::error::Error),
    #[error(
        "unexpected EarlyNetworkConfig schema_version: \
         got {got} but expected {expected}"
    )]
    UnexpectedSchemaVersion { got: u32, expected: u32 },
}

impl TryFrom<crate::v20::early_networking::EarlyNetworkConfig>
    for EarlyNetworkConfigEnvelope
{
    type Error = EarlyNetworkConfigError;

    fn try_from(
        value: crate::v20::early_networking::EarlyNetworkConfig,
    ) -> Result<Self, Self::Error> {
        // Prior to this version where we separated the schema_version envelope
        // from the `EarlyNetworkConfigBody`, all systems were on schema_version
        // 2. We don't know how to convert anything else, nor do we need to.
        if value.schema_version != 2 {
            return Err(EarlyNetworkConfigError::UnexpectedSchemaVersion {
                got: value.schema_version,
                expected: 2,
            });
        }

        let body = serde_json::to_value(&value.body)
            .map_err(EarlyNetworkConfigError::SerializeBody)?;

        Ok(Self {
            generation: value.generation,
            schema_version: value.schema_version,
            body,
        })
    }
}
