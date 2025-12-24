// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-exports of the latest version of each type.
//!
//! Per RFD 619, these re-exports are explicit (no wildcards) to make it clear
//! which types are available at the latest version.

pub mod alarm {
    pub use crate::v1::alarm::Alarm;
}

pub mod configuration {
    pub use crate::v1::configuration::BaseboardId;
    pub use crate::v1::configuration::Configuration;
    pub use crate::v1::configuration::ConfigurationDiff;
    pub use crate::v1::configuration::ConfigurationError;
    pub use crate::v1::configuration::NewConfigParams;
}

pub mod crypto {
    pub use crate::v1::crypto::DecryptionError;
    pub use crate::v1::crypto::EncryptedRackSecrets;
    pub use crate::v1::crypto::InvalidRackSecretSizeError;
    pub use crate::v1::crypto::RackSecretReconstructError;
    pub use crate::v1::crypto::Salt;
    pub use crate::v1::crypto::Sha3_256Digest;
}

pub mod persistent_state {
    pub use crate::v1::persistent_state::ExpungedMetadata;
    pub use crate::v1::persistent_state::PersistentStateSummary;
}

pub mod status {
    pub use crate::v1::status::CommitStatus;
    pub use crate::v1::status::CoordinatorStatus;
    pub use crate::v1::status::NodePersistentStateSummary;
    pub use crate::v1::status::NodeStatus;
}

pub mod types {
    pub use crate::v1::types::Epoch;
    pub use crate::v1::types::Threshold;
}
