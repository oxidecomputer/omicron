// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VPC types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParamsStrict;
use omicron_common::api::external::{
    IdentityMetadataUpdateParams, Name, NameOrId, Nullable, RouteDestination,
    RouteTarget,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a `VpcSubnet`
///
/// A `PUT` replaces the resource, so `name` and `description` are required.
/// `custom_router` is clearable: it must be present, but may be explicit
/// `null` to detach any custom router.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,

    /// An optional router, used to direct packets sent from hosts in this subnet
    /// to any destination address.
    pub custom_router: Nullable<NameOrId>,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion). `name` and `description` become present `Option`s.
// `Nullable<NameOrId>` is an `Option<NameOrId>` underneath, and it carries the
// same meaning here: `Some` attaches a router, `None` clears it.
impl From<VpcSubnetUpdate> for crate::v2025_11_20_00::vpc::VpcSubnetUpdate {
    fn from(new: VpcSubnetUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
            custom_router: new.custom_router.0,
        }
    }
}

/// Updateable properties of a `Vpc`
///
/// A `PUT` replaces the resource, so `name`, `description`, and `dns_name` must
/// all be present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,

    pub dns_name: Name,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<VpcUpdate> for crate::v2025_11_20_00::vpc::VpcUpdate {
    fn from(new: VpcUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
            dns_name: Some(new.dns_name),
        }
    }
}

/// Updateable properties of a `VpcRouter`
///
/// A `PUT` replaces the resource, so `name` and `description` must both be
/// present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<VpcRouterUpdate> for crate::v2025_11_20_00::vpc::VpcRouterUpdate {
    fn from(new: VpcRouterUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
        }
    }
}

/// Updateable properties of a `RouterRoute`
///
/// A `PUT` replaces the resource, so `name`, `description`, `target`, and
/// `destination` must all be present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRouteUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,

    /// The location that matched packets should be forwarded to.
    pub target: RouteTarget,
    /// Selects which traffic this routing rule will apply to.
    pub destination: RouteDestination,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<RouterRouteUpdate> for crate::v2025_11_20_00::vpc::RouterRouteUpdate {
    fn from(new: RouterRouteUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
            target: new.target,
            destination: new.destination,
        }
    }
}
