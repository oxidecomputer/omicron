// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum configuration types.

use std::collections::{BTreeMap, BTreeSet};

use daft::Diffable;
use gfss::shamir::SplitError;
use iddqd::{IdOrdItem, id_upcast};
use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use slog_error_chain::SlogInlineError;
pub use sled_hardware_types::BaseboardId;

use super::crypto::{EncryptedRackSecrets, Sha3_256Digest};
use super::types::{Epoch, Threshold};

/// A member entry in a trust quorum configuration.
///
/// This type is used for OpenAPI schema generation since OpenAPI v3.0.x
/// doesn't support tuple arrays.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfigurationMember {
    /// The baseboard ID of the member.
    pub id: BaseboardId,
    /// The SHA3-256 hash of the member's key share.
    pub share_digest: Sha3_256Digest,
}

/// Error creating a configuration.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq, SlogInlineError, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationError {
    #[error("rack secret split error")]
    #[schemars(with = "SplitError")]
    RackSecretSplit(
        #[from]
        #[source]
        SplitError,
    ),
    #[error("too many members: must be fewer than 255")]
    TooManyMembers,
}

/// The configuration for a given epoch.
///
/// Only valid for non-lrtq configurations.
#[serde_as]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Diffable,
    JsonSchema,
)]
pub struct Configuration {
    /// Unique Id of the rack.
    pub rack_id: RackUuid,

    /// Unique, monotonically increasing identifier for a configuration.
    pub epoch: Epoch,

    /// Who was the coordinator of this reconfiguration?
    pub coordinator: BaseboardId,

    /// All members of the current configuration and the hash of their key shares.
    #[serde_as(as = "Vec<(_, _)>")]
    #[schemars(with = "Vec<ConfigurationMember>")]
    pub members: BTreeMap<BaseboardId, Sha3_256Digest>,

    /// The number of sleds required to reconstruct the rack secret.
    pub threshold: Threshold,

    /// There are no encrypted rack secrets for the initial configuration.
    pub encrypted_rack_secrets: Option<EncryptedRackSecrets>,
}

impl IdOrdItem for Configuration {
    type Key<'a> = Epoch;

    fn key(&self) -> Self::Key<'_> {
        self.epoch
    }

    id_upcast!();
}

/// Parameters for creating a new configuration.
pub struct NewConfigParams<'a> {
    pub rack_id: RackUuid,
    pub epoch: Epoch,
    pub members: &'a BTreeSet<BaseboardId>,
    pub threshold: Threshold,
    pub coordinator_id: &'a BaseboardId,
}
