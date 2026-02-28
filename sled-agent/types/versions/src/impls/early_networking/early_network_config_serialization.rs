// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for deserializing the body stored inside an
//! [`EarlyNetworkConfigEnvelope`], as determined by the envelope's metadata
//! (particularly, [`EarlyNetworkConfigEnvelope::schema_version`].
//!
//! Adding a new `EarlyNetworkConfigBody` has extra requirements beyond normal
//! sled-agent API types, because it's kept as a serialized blob in both the
//! bootstore and CRDB. Many of these requirements are enforced statically by
//! the macros in this module.
//!
//! To add a new `EarlyNetworkConfigBody` version:
//!
//! 1. Follow the normal instructions for adding a new API version at all,
//!    including creating a new `vN` module in this crate and updating the
//!    [`latest::early_networking::EarlyNetworkConfigBody`] to be a re-export of
//!    your new version.
//!
//! 2. Ensure your type has an associated
//!    [`latest::early_networking::EarlyNetworkConfigBody::SCHEMA_VERSION`]
//!    constant. It must have the value of the previous latest
//!    `EarlyNetworkConfigBody::SCHEMA_VERSION`'s plus 1. This is enforced by
//!    the `assert_consecutive_versions!()` macro below.
//!
//! 3. Ensure your type has a `TryFrom<_>` implementation to convert from the
//!    previous latest `EarlyNetworkConfigBody`. This is used during the
//!    deserialization process: if we find an envelope containing the previous
//!    `EarlyNetworkConfigBody`'s schema version, we'll deserialize as that
//!    version and then convert to the latest via this `TryFrom` impl. It must
//!    use `anyhow::Error` as the associated error type, or this module will
//!    fail to compile.
//!
//!    Even though we're using `TryFrom<_>` here instead of `From<_>`, in
//!    practice you must ensure that the conversion will not fail except in
//!    cases where the rack truly cannot start up. Failure to convert here will
//!    result in sled-agent rendering itself inoperable on startup, forever
//!    logging whatever conversion error is being reported. We MUST ensure that
//!    we can always convert old versions to new versions.
//!
//! 4. Update [`EarlyNetworkConfigEnvelope::deserialize_body()`] below, adding
//!    your new version (via the `vN::early_networking::EarlyNetworkConfigBody`
//!    path, NOT the `latest::early_networking::EarlyNetworkConfigBody` path) to
//!    the invocation of `versioned_decode!()`. This is the macro the emits a
//!    `match` statement for all known `EarlyNetworkConfigBody` schema versions
//!    and handles conversion up to the latest via the `TryFrom<_>` impls
//!    described above.

use crate::latest::early_networking::EarlyNetworkConfigEnvelope;
use crate::{latest, v20, v25};
use bootstore::schemes::v0 as bootstore;
use serde::de::DeserializeOwned;
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
    #[error(
        "could not convert EarlyNetworkConfigBody from version {from_version} \
         to version {to_version}"
    )]
    ConvertBody {
        from_version: u32,
        to_version: u32,
        #[source]
        err: anyhow::Error,
    },
}

/// Asserts at compile time that adjacent
/// `EarlyNetworkConfigBody::SCHEMA_VERSION` values are consecutive.
macro_rules! assert_consecutive_versions {
    ($sole:path) => {};
    ($a:path, $b:path $(, $rest:path)*) => {
        const _: () = assert!(
            <$b>::SCHEMA_VERSION == <$a>::SCHEMA_VERSION + 1,
            "SCHEMA_VERSION values must be consecutive"
        );
        assert_consecutive_versions!($b $(, $rest)*);
    };
}

/// Chains `TryFrom` conversions through a sequence of types.
macro_rules! migrate {
    ($val:expr, $from:path, $next:path) => {{
        <$next as TryFrom<$from>>::try_from($val).map_err(|err| {
            EarlyNetworkConfigEnvelopeError::ConvertBody {
                from_version: <$from>::SCHEMA_VERSION,
                to_version: <$next>::SCHEMA_VERSION,
                err,
            }
        })?
    }};
    ($val:expr, $from:path, $next:path, $($rest:path),+) => {{
        let converted: $next = <$next as TryFrom<$from>>::try_from(
            $val,
        ).map_err(|err| {
            EarlyNetworkConfigEnvelopeError::ConvertBody {
                from_version: <$from>::SCHEMA_VERSION,
                to_version: <$next>::SCHEMA_VERSION,
                err,
            }
        })?;
        migrate!(converted, $next, $($rest),+)
    }};
}

/// Emits a match over `EarlyNetworkConfigBody::SCHEMA_VERSION` values. Each
/// match arm body will attempt to deserialize the value as the matching
/// `EarlyNetworkConfigBody` type, and then for all types except the newest,
/// chain through `TryFrom` conversions (via `migrate!`, defined above) until we
/// convert to the newest.
macro_rules! version_match {
    // Final type: emit the complete match
    ($version:ident, $body:ident, [ $($arms:tt)* ] $current:path) => {
        match $version {
            $($arms)*
            <$current>::SCHEMA_VERSION => {
                let val: $current = deserialize_body(
                    $body,
                    <$current>::SCHEMA_VERSION,
                )?;
                Ok(val)
            }
            other => Err(EarlyNetworkConfigEnvelopeError::UnknownSchemaVersion {
                schema_version: other,
            })
        }
    };

    // Non-final type: accumulate an arm and recurse
    (
        $version:ident,
        $body:ident,
        [ $($arms:tt)* ]
        $current:path,
        $($rest:path),+
    ) => {
        version_match!(
            $version,
            $body,
            [
                $($arms)*
                <$current>::SCHEMA_VERSION => {
                    let val: $current = deserialize_body(
                        $body,
                        <$current>::SCHEMA_VERSION,
                    )?;
                    let migrated = migrate!(val, $current, $($rest),+);
                    Ok(migrated)
                }
            ]
            $($rest),+
        )
    };
}

/// Main entry point for [`EarlyNetworkConfigEnvelope::deserialize_body()`].
macro_rules! versioned_decode {
    ( $first:path $(, $rest:path)* $(,)? ) => {{
        // Statically guarantee each successive
        // `EarlyNetworkConfigBody::SCHEMA_VERSION` is equal to the previous
        // type's `SCHEMA_VERSION + 1`.
        assert_consecutive_versions!($first $(, $rest)*);

        // Actual function: emit the match over all known `SCHEMA_VERSION`s.
        |schema_version: u32, body: ::serde_json::Value|
            -> ::std::result::Result<
                latest::early_networking::EarlyNetworkConfigBody,
                EarlyNetworkConfigEnvelopeError
            >
        {
            version_match!(schema_version, body, [] $first $(, $rest)*)
        }
    }};
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
    /// and convert the contained config to
    /// [`latest::early_networking::EarlyNetworkConfigBody`] if necessary.
    pub fn deserialize_body(
        &self,
    ) -> Result<
        latest::early_networking::EarlyNetworkConfigBody,
        EarlyNetworkConfigEnvelopeError,
    > {
        // Ordered list, from oldest to newest, of all known
        // `EarlyNetworkConfigBody` type versions.
        //
        // Important: This method's return type uses the
        // [`latest::early_networking::EarlyNetworkConfigBody`] reexport, but
        // the list here never uses that reexport. This ensures that if someone
        // updates the `latest::*` export _without_ adding that latest version
        // to this invocation, this method will fail to compile.
        //
        // If this has brought you to this comment, please see the block comment
        // at the top of this module for the full set of instructions for adding
        // a new `EarlyNetworkConfigBody` version.
        let f = versioned_decode!(
            v20::early_networking::EarlyNetworkConfigBody,
            v25::early_networking::EarlyNetworkConfigBody,
        );
        f(self.schema_version, self.body.clone())
    }
}

// Helper function: maps errors from `serde_json::from_value()` into an
// appropriate `EarlyNetworkConfigEnvelopeError`.
fn deserialize_body<T: DeserializeOwned>(
    value: serde_json::Value,
    schema_version: u32,
) -> Result<T, EarlyNetworkConfigEnvelopeError> {
    serde_json::from_value(value).map_err(|err| {
        EarlyNetworkConfigEnvelopeError::DeserializeBody { schema_version, err }
    })
}
