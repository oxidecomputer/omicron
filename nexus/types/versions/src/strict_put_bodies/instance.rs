// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParamsStrict;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for updating an `InstanceNetworkInterface`
///
/// A `PUT` replaces the resource, so `name` and `description` must both be
/// present. `primary` and `transit_ips` are always present as well.
///
/// Note that modifying IP addresses for an interface is not yet supported, a
/// new interface must be created instead.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,

    /// Make a secondary interface the instance's primary interface.
    ///
    /// If applied to a secondary interface, that interface will become the
    /// primary on the next reboot of the instance. Note that this may have
    /// implications for routing between instances, as the new primary interface
    /// will be on a distinct subnet from the previous primary interface.
    ///
    /// Note that this can only be used to select a new primary interface for an
    /// instance. Requests to change the primary interface into a secondary will
    /// return an error.
    pub primary: bool,

    /// A set of additional networks that this interface may send and receive traffic on
    pub transit_ips: Vec<IpNet>,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<InstanceNetworkInterfaceUpdate>
    for crate::v2025_11_20_00::instance::InstanceNetworkInterfaceUpdate
{
    fn from(new: InstanceNetworkInterfaceUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
            primary: new.primary,
            transit_ips: new.transit_ips,
        }
    }
}
