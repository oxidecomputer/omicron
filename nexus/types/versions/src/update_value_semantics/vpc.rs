// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC types for version UPDATE_VALUE_SEMANTICS.

use omicron_common::api::external::{Name, NameOrId, Nullable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a `VpcSubnet`
///
/// A `PUT` replaces the resource, so `name` and `description` are required.
/// `custom_router` is clearable: it must be present, but may be explicit
/// `null` to detach any custom router.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    pub name: Name,
    pub description: String,

    /// An optional router, used to direct packets sent from hosts in this subnet
    /// to any destination address.
    pub custom_router: Nullable<NameOrId>,
}
