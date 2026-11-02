// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! NVMe controller locations from libtopo, as `nvmeadm list -L` finds them
//! (`do_list_phys_map_loc_cb()` in illumos `cmd/nvmeadm/nvmeadm.c`).

use anyhow::Result;
use libtopo::hc::{BAY, NVME, SLOT, TOPO_IO_INSTANCE, TOPO_PGROUP_IO};
use libtopo::{Node, Scheme, TopoHdl, WalkAction};
use std::collections::BTreeMap;

/// Map each NVMe controller instance to its location label.
pub fn nvme_locations() -> Result<BTreeMap<i32, String>> {
    let hdl = TopoHdl::open()?;
    let snap = hdl.snapshot()?;

    let mut labels = BTreeMap::new();
    snap.walk(Scheme::Hc, |node| {
        if node.name() != NVME {
            return Ok(WalkAction::Continue);
        }
        let Ok(instance) = node.property_u32(TOPO_PGROUP_IO, TOPO_IO_INSTANCE)
        else {
            return Ok(WalkAction::Continue);
        };
        let Ok(instance) = i32::try_from(instance) else {
            return Ok(WalkAction::Continue);
        };

        // Like nvmeadm, only consider this node's label, or its immediate
        // parent's if that parent is a bay or slot.  Going further risks
        // picking up a label for an unrelated device.
        let found = label(&node).or_else(|| {
            node.parent()
                .filter(|p| p.name() == BAY || p.name() == SLOT)
                .and_then(|p| label(&p))
        });
        if let Some(l) = found {
            labels.insert(instance, l);
        }
        Ok(WalkAction::Continue)
    })?;

    Ok(labels)
}

fn label(node: &Node<'_>) -> Option<String> {
    node.label().ok().filter(|l| !l.is_empty())
}
