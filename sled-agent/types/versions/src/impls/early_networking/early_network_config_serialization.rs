// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for deserializing the body stored inside an
//! [`EarlyNetworkConfigEnvelope`], as determined by the envelope's metadata
//! (particularly, [`EarlyNetworkConfigEnvelope::schema_version`].

use crate::latest::early_networking::EarlyNetworkConfigBody;
use crate::latest::early_networking::EarlyNetworkConfigEnvelope;
use bootstore::schemes::v0 as bootstore;
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

impl EarlyNetworkConfigEnvelope {
    /// Seriealize the contents of this envelope into a bootstore-suitable type,
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
    /// [`EarlyNetworkConfigBody`].
    pub fn deserialize_body(
        &self,
    ) -> Result<EarlyNetworkConfigBody, EarlyNetworkConfigEnvelopeError> {
        // TODO: This only handles the current schema version (2). Once we add a
        // new schema version, as written we'll need to update this function.
        // The plan is to replace the implementation of this function with one
        // that ensures any new `EarlyNetworkConfigBody` types are statically
        // guaranteed to be covered, have correct `SCHEMA_VERSION` values, etc.
        if self.schema_version == EarlyNetworkConfigBody::SCHEMA_VERSION {
            let body: EarlyNetworkConfigBody =
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
