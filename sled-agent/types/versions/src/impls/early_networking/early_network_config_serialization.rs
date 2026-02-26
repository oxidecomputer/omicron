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
use std::str::FromStr;

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
    /// Deserialize the contents of the bootstore config into an
    /// [`EarlyNetworkConfigEnvelope`].
    ///
    /// The returned envelope must be further deserialized by
    /// [`EarlyNetworkConfigEnvelope::deserialize_body()`] to get at the actual
    /// early network configuration.
    pub fn deserialize_from_bootstore(
        config: &bootstore::NetworkConfig,
    ) -> Result<Self, EarlyNetworkConfigEnvelopeError> {
        Self::deserialize_from_slice(&config.blob)
    }

    fn deserialize_from_slice(
        data: &[u8],
    ) -> Result<Self, EarlyNetworkConfigEnvelopeError> {
        serde_json::from_slice(data).map_err(|err| {
            EarlyNetworkConfigEnvelopeError::DeserializeEnvelope { err }
        })
    }

    /// Deserialize the body of this envelope, based on the `schema_version`,
    /// and convert the contained config to the latest version of
    /// [`EarlyNetworkConfigBody`].
    pub fn deserialize_body(
        &self,
    ) -> Result<EarlyNetworkConfigBody, EarlyNetworkConfigEnvelopeError> {
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

impl From<EarlyNetworkConfigEnvelope> for bootstore::NetworkConfig {
    fn from(value: EarlyNetworkConfigEnvelope) -> Self {
        let blob = serde_json::to_vec(&value).expect(
            "EarlyNetworkConfigEnvelope can always be serialized as JSON",
        );

        // This is duplicated from the envelope out so bootstore knows the
        // latest version to replicate, but that seems fine.
        let generation = value.generation;

        Self { generation, blob }
    }
}

impl FromStr for EarlyNetworkConfigEnvelope {
    type Err = EarlyNetworkConfigEnvelopeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::deserialize_from_slice(value.as_bytes())
    }
}
