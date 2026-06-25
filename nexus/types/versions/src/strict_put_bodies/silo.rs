// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo types for version STRICT_PUT_BODIES.

use omicron_common::api::external::ByteCount;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a Silo's resource limits.
///
/// A `PUT` replaces the resource, so every quota value is required.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasUpdate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: i64,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: ByteCount,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: ByteCount,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion). Each required quota just becomes a present `Option`.
impl From<SiloQuotasUpdate> for crate::v2025_11_20_00::silo::SiloQuotasUpdate {
    fn from(new: SiloQuotasUpdate) -> Self {
        Self {
            cpus: Some(new.cpus),
            memory: Some(new.memory),
            storage: Some(new.storage),
        }
    }
}
