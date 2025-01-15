// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for `BlueprintPhysicalDisksConfig`

use super::{BlueprintPhysicalDisksConfig, BpVisitorContext, Change};
use diffus::edit::{map, Edit};
use omicron_common::{api::external::Generation, zpool_name::ZpoolName};

/// A trait to visit a [`BlueprintPhysicalDisksConfig`]
pub trait VisitBlueprintPhysicalDisksConfig<'e> {
    fn visit_root(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, BlueprintPhysicalDisksConfig>,
    ) {
        visit_root(self, ctx, node);
    }

    /// A change to `BlueprintZonesConfig::generation`
    fn visit_generation_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Generation>,
    ) {
        // Leaf node
    }
}

/// The root of the diff for a [`BlueprintPhysicalDisksConfig`]
pub fn visit_root<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: Edit<'e, BlueprintPhysicalDisksConfig>,
) where
    V: VisitBlueprintPhysicalDisksConfig<'e> + ?Sized,
{
    if let Edit::Change { diff, .. } = node {
        if let Edit::Change { diff, .. } = diff.generation {
            v.visit_generation_change(ctx, diff.into());
        }

        if let Edit::Change { diff, .. } = diff.disks {}
    }
}
