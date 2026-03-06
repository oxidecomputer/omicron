// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for deserializing the body stored inside an
//! [`EarlyNetworkConfigEnvelope`], as determined by the envelope's metadata
//! (particularly, [`EarlyNetworkConfigEnvelope::schema_version`].

use bootstore::schemes::v0 as bootstore;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::latest::early_networking as latest;
use sled_agent_types_versions::v20::early_networking as v20;
use slog_error_chain::SlogInlineError;

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum EarlyNetworkConfigEnvelopeError {
    #[error("failed to deserialize early network envelope")]
    DeserializeEnvelope {
        #[source]
        err: serde_json::error::Error,
    },
    #[error(
        "failed to deserialize early network config body \
         with schema version {schema_version}"
    )]
    DeserializeBody {
        schema_version: u32,
        #[source]
        err: serde_json::error::Error,
    },
    #[error("unknown early network config schema version: {schema_version}")]
    UnknownSchemaVersion { schema_version: u32 },
}

/// Envelope containing a versioned JSON blob (an [`EarlyNetworkConfigBody`]).
///
/// A [`WriteNetworkConfigRequest`] received by sled-agent (typically sent by
/// Nexus) results in a new [`bootstore::NetworkConfig`] being written to the
/// bootstore:
///
/// * The [`WriteNetworkConfigRequest::body`] will be wrapped in an
///   [`EarlyNetworkConfigEnvelope`]. `schema_version` records the
///   [`EarlyNetworkConfigBody::SCHEMA_VERSION`] of the particular version of
///   the body, and `body` contains the JSON-ified [`EarlyNetworkConfigBody`]
///   itself.
/// * The [`bootstore::NetworkConfig::generation`] will be set to the generation
///   from the incoming [`WriteNetworkConfigRequest::generation`]. The
///   [`bootstore::NetworkConfig::blob`] contains the JSON-ified
///   [`EarlyNetworkConfigEnvelope`] from the previous bullet.
///
/// [`EarlyNetworkConfigBody`]:
/// sled_agent_types_versions::latest::early_networking::EarlyNetworkConfigBody
/// [`EarlyNetworkConfigBody::SCHEMA_VERSION`]:
/// sled_agent_types_versions::latest::early_networking::EarlyNetworkConfigBody::SCHEMA_VERSION
/// [`WriteNetworkConfigRequest`]:
/// sled_agent_types_versions::latest::early_networking::WriteNetworkConfigRequest
/// [`WriteNetworkConfigRequest::body`]:
/// sled_agent_types_versions::latest::early_networking::WriteNetworkConfigRequest::body
/// [`WriteNetworkConfigRequest::generation`]:
/// sled_agent_types_versions::latest::early_networking::WriteNetworkConfigRequest::generation
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EarlyNetworkConfigEnvelope {
    // Which version of `EarlyNetworkConfigBody` is serialized into `body`.
    pub(crate) schema_version: u32,

    // The actual early network configuration details.
    //
    // These are a serialized `EarlyNetworkConfigBody` of some version. We must
    // inspect `schema_version` to know how to interpret this value.
    pub(crate) body: serde_json::Value,
}

impl EarlyNetworkConfigEnvelope {
    /// Serialize the contents of this envelope into a bootstore-suitable type,
    /// tagged with the given `generation`.
    pub fn serialize_to_bootstore_with_generation(
        &self,
        generation: u64,
    ) -> bootstore::NetworkConfig {
        // Serialize ourself in memory; this can't fail, because we only contain
        // a generation and a JSON blob, both of which can be represented in
        // JSON.
        let blob = serde_json::to_vec(self).expect(
            "EarlyNetworkConfigEnvelope can always be serialized as JSON",
        );
        bootstore::NetworkConfig { generation, blob }
    }

    /// Deserialize the contents of the bootstore config into an
    /// [`EarlyNetworkConfigEnvelope`].
    ///
    /// The returned envelope must be further deserialized by
    /// [`EarlyNetworkConfigEnvelope::deserialize_body()`] to get at the actual
    /// early network configuration.
    ///
    /// This is a thin wrapper around [`serde_json::from_slice()`] to wrap the
    /// error type in something consistent with [`Self::deserialize_body()`].
    pub fn deserialize_from_bootstore(
        config: &bootstore::NetworkConfig,
    ) -> Result<Self, EarlyNetworkConfigEnvelopeError> {
        serde_json::from_slice(&config.blob).map_err(|err| {
            EarlyNetworkConfigEnvelopeError::DeserializeEnvelope { err }
        })
    }

    /// Deserialize the contents of a JSON blob into an
    /// [`EarlyNetworkConfigEnvelope`].
    ///
    /// The returned envelope must be further deserialized by
    /// [`EarlyNetworkConfigEnvelope::deserialize_body()`] to get at the actual
    /// early network configuration.
    ///
    /// This is a thin wrapper around [`serde_json::from_value()`] to wrap the
    /// error type in something consistent with [`Self::deserialize_body()`].
    pub fn deserialize_from_value(
        value: serde_json::Value,
    ) -> Result<Self, EarlyNetworkConfigEnvelopeError> {
        serde_json::from_value(value).map_err(|err| {
            EarlyNetworkConfigEnvelopeError::DeserializeEnvelope { err }
        })
    }

    /// Deserialize the body of this envelope, based on the `schema_version`,
    /// and convert the contained config to the latest version of
    /// `EarlyNetworkConfigBody`.
    pub fn deserialize_body(
        &self,
    ) -> Result<latest::EarlyNetworkConfigBody, EarlyNetworkConfigEnvelopeError>
    {
        // TODO: This only handles the current schema version (2). Once we add a
        // new schema version, as written we'll need to update this function.
        // The plan is to replace the implementation of this function with one
        // that ensures any new `EarlyNetworkConfigBody` types are statically
        // guaranteed to be covered, have correct `SCHEMA_VERSION` values, etc.
        if self.schema_version == latest::EarlyNetworkConfigBody::SCHEMA_VERSION
        {
            let body: latest::EarlyNetworkConfigBody =
                serde_json::from_value(self.body.clone()).map_err(|err| {
                    EarlyNetworkConfigEnvelopeError::DeserializeBody {
                        schema_version: self.schema_version,
                        err,
                    }
                })?;
            Ok(body)
        } else {
            Err(EarlyNetworkConfigEnvelopeError::UnknownSchemaVersion {
                schema_version: self.schema_version,
            })
        }
    }
}

// We need to be able to construct [`EarlyNetworkConfigEnvelope`]s for every
// version of `EarlyNetworkConfigBody` (starting from the current version of
// `EarlyNetworkConfigBody` when `EarlyNetworkConfigEnvelope` was introduced).
//
// Put those `From` impls here.
impl From<&'_ v20::EarlyNetworkConfigBody> for EarlyNetworkConfigEnvelope {
    fn from(value: &'_ v20::EarlyNetworkConfigBody) -> Self {
        Self {
            schema_version: v20::EarlyNetworkConfigBody::SCHEMA_VERSION,
            // We're serializing in-memory; this can only fail if
            // `EarlyNetworkConfigBody` contains types that can't be represented
            // as JSON, which (a) should never happen and (b) we should catch
            // immediately in tests.
            body: serde_json::to_value(value)
                .expect("EarlyNetworkConfigBody can be serialized as JSON"),
        }
    }
}
