// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::{RackUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The sled's registration identity.
///
/// This returns additional information that isn't present in the `sled_upsert`
/// upcall to Nexus.
///
/// The data that can be returned here has important restrictions! See the
/// comment in sled_registration_info for more information.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SledRegistrationInfo {
    /// The ID of this sled.
    pub sled_id: SledUuid,
    /// The rack ID that this sled is registered with.
    pub rack_id: RackUuid,
}
