// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::internal::shared::{ResolvedVpcRoute, RouterTarget};
use oxnet::IpNet;
use uuid::Uuid;

/// A VPC route resolved into a concrete target.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Route {
    /// The ID of the control-plane object this route is derived from.
    ///
    /// This may not yet be known for routes in service zones, which
    /// are initialised using a set of bootstrap rules.
    pub id: Option<Uuid>,
    pub dest: IpNet,
    pub target: RouterTarget,
}

impl From<ResolvedVpcRoute> for Route {
    fn from(ResolvedVpcRoute { id, dest, target }: ResolvedVpcRoute) -> Self {
        Self { id: Some(id), dest, target }
    }
}
