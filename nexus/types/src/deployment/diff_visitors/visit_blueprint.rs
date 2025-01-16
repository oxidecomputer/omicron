// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for a `Blueprint`

use super::{BpVisitorContext, Change};
use crate::{
    deployment::{
        Blueprint, BlueprintDatasetsConfig, BlueprintPhysicalDisksConfig,
        BlueprintZonesConfig, EditedBlueprint, SledUuid,
    },
    external_api::views::SledState,
};
use diffus::edit::{map, Edit};
use std::collections::BTreeMap;

/// A trait to visit a [`Blueprint`]
pub trait VisitBlueprint<'e> {
    fn visit_root(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, Blueprint>,
    ) {
        visit_root(self, ctx, node);
    }
}

#[derive(Default)]
struct EditedPerSled<'a, 'e> {
    sled_state: Option<&'a map::Edit<'e, SledState>>,
    zones: Option<&'a map::Edit<'e, BlueprintZonesConfig>>,
    disks: Option<&'a map::Edit<'e, BlueprintPhysicalDisksConfig>>,
    datasets: Option<&'a map::Edit<'e, BlueprintDatasetsConfig>>,
}

pub fn visit_root<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: Edit<'e, Blueprint>,
) where
    V: VisitBlueprint<'e> + ?Sized,
{
    let Edit::Change { diff, .. } = node else {
        return;
    };

    // Build up the set of all edits for a given sled
    let mut sled_edits = BTreeMap::new();
    if let Edit::Change { diff, .. } = &diff.sled_state {
        for (&sled_id, edit) in diff {
            let mut edits = EditedPerSled::default();
            edits.sled_state = Some(edit);
            sled_edits.insert(*sled_id, edits);
        }
    }
    if let Edit::Change { diff, .. } = &diff.blueprint_zones {
        for (&sled_id, edit) in diff {
            sled_edits
                .entry(*sled_id)
                .and_modify(|e| e.zones = Some(edit))
                .or_default();
        }
    }
    if let Edit::Change { diff, .. } = &diff.blueprint_disks {
        for (&sled_id, edit) in diff {
            sled_edits
                .entry(*sled_id)
                .and_modify(|e| e.disks = Some(edit))
                .or_default();
        }
    }
    if let Edit::Change { diff, .. } = &diff.blueprint_datasets {
        for (&sled_id, edit) in diff {
            sled_edits
                .entry(*sled_id)
                .and_modify(|e| e.datasets = Some(edit))
                .or_default();
        }
    }

    if let Edit::Change { diff, .. } = diff.parent_blueprint_id {}
    if let Edit::Change { diff, .. } = diff.internal_dns_version {}
    if let Edit::Change { diff, .. } = diff.external_dns_version {}
    if let Edit::Change { diff, .. } = diff.cockroachdb_fingerprint {}
    if let Edit::Change { diff, .. } =
        diff.cockroachdb_setting_preserve_downgrade
    {}
    if let Edit::Change { diff, .. } = diff.clickhouse_cluster_config {}
    if let Edit::Change { diff, .. } = diff.creator {}
    if let Edit::Change { diff, .. } = diff.comment {}
}
