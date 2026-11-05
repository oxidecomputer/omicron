// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries against the illumos hardware topology (libtopo).
//!
//! libtopo describes the platform as a tree of nodes with labels and
//! properties, built from platform topology maps, devinfo, libnvme, and
//! other sources. This module copies the nodes that can describe disk bays
//! into [`TopoNode`]s without interpreting them; `topo_bays` decides what
//! they mean.

use crate::disk_bay::ObservedBay;
use crate::topo_bays::{
    BayIdentError, BayProps, NodeKey, Prop, TopoNode, TopoNodeKind,
    assemble_bays,
};
use libtopo::{Error, Node, Scheme, Snapshot, WalkAction, hc};
use slog::{Logger, debug};
use std::time::Instant;

/// `storage` property names from `<fm/topo_hc.h>` that libtopo does not yet
/// export in `libtopo::hc`.
const TOPO_STORAGE_SERIAL_NUM: &str = "serial-number";
const TOPO_STORAGE_LOGICAL_DISK_NAME: &str = "logical-disk";

#[derive(Debug, thiserror::Error)]
pub(super) enum TopoError {
    #[error("failed to read the hardware topology")]
    Topo(#[from] libtopo::Error),

    #[error(transparent)]
    Bay(#[from] BayIdentError),
}

/// Returns every U.2 bay and M.2 socket in the snapshot with what the
/// topology found behind it, in walk order. See [`assemble_bays`].
pub(super) fn read_disk_bays(
    log: &Logger,
    snap: &Snapshot<'_>,
) -> Result<Vec<ObservedBay>, TopoError> {
    let start = Instant::now();

    let mut nodes = Vec::new();
    snap.walk(Scheme::Hc, |node| {
        if let Some(node) = read_node(&node) {
            nodes.push(node);
        }
        Ok(WalkAction::Continue)
    })?;
    let bays = assemble_bays(log, &nodes)?;

    debug!(
        log,
        "read disk bays from hardware topology";
        "count" => bays.len(),
        "elapsed" => ?start.elapsed(),
    );
    Ok(bays)
}

/// Copies out the properties of a node that may describe part of a disk
/// bay, or `None` for any other node.
fn read_node(node: &Node<'_>) -> Option<TopoNode> {
    let kind = match node.name().as_ref() {
        hc::BAY => TopoNodeKind::Bay(read_bay_props(node)),
        hc::SLOT => TopoNodeKind::Slot {
            slot_type: optional_u32(
                node,
                hc::TOPO_PGROUP_SLOT,
                hc::TOPO_PROP_SLOT_TYPE,
            ),
            props: read_bay_props(node),
        },
        hc::NVME => TopoNodeKind::Nvme {
            instance: optional_u32(
                node,
                hc::TOPO_PGROUP_IO,
                hc::TOPO_IO_INSTANCE,
            ),
            driver: optional_string(
                node,
                hc::TOPO_PGROUP_IO,
                hc::TOPO_IO_DRIVER,
            ),
            devfs_path: optional_string(
                node,
                hc::TOPO_PGROUP_IO,
                hc::TOPO_IO_DEV_PATH,
            ),
        },
        hc::DISK => TopoNodeKind::Disk {
            manufacturer: optional_string(
                node,
                hc::TOPO_PGROUP_STORAGE,
                hc::TOPO_STORAGE_MANUFACTURER,
            ),
            model: optional_string(
                node,
                hc::TOPO_PGROUP_STORAGE,
                hc::TOPO_STORAGE_MODEL,
            ),
            serial: optional_string(
                node,
                hc::TOPO_PGROUP_STORAGE,
                TOPO_STORAGE_SERIAL_NUM,
            ),
            devfs_path: optional_string(
                node,
                hc::TOPO_PGROUP_IO,
                hc::TOPO_IO_DEV_PATH,
            ),
            logical_disk: optional_string(
                node,
                hc::TOPO_PGROUP_STORAGE,
                TOPO_STORAGE_LOGICAL_DISK_NAME,
            ),
        },
        hc::BOARD => TopoNodeKind::Board,
        hc::IC => TopoNodeKind::Ic {
            driver: optional_string(
                node,
                hc::TOPO_PGROUP_IO,
                hc::TOPO_IO_DRIVER,
            ),
            devfs_path: optional_string(
                node,
                hc::TOPO_PGROUP_IO,
                hc::TOPO_IO_DEV_PATH,
            ),
        },
        _ => return None,
    };
    Some(TopoNode {
        key: node_key(node),
        parent: node.parent().map(|parent| node_key(&parent)),
        kind,
    })
}

fn read_bay_props(node: &Node<'_>) -> BayProps {
    BayProps {
        label: node.label().map_err(|err| err.to_string()),
        binding_driver: optional_string(
            node,
            hc::TOPO_PGROUP_BINDING,
            hc::TOPO_BINDING_DRIVER,
        ),
        binding_slot: optional_u32(
            node,
            hc::TOPO_PGROUP_BINDING,
            hc::TOPO_BINDING_SLOT,
        ),
    }
}

/// The names and instances of `node` and all its ancestors.
fn node_key(node: &Node<'_>) -> NodeKey {
    let mut parts = vec![format!("{}[{}]", node.name(), node.instance())];
    let mut cursor = node.parent();
    while let Some(ancestor) = cursor {
        parts.push(format!("{}[{}]", ancestor.name(), ancestor.instance()));
        cursor = ancestor.parent();
    }
    parts.reverse();
    NodeKey::new(parts.join("/"))
}

fn optional_string(node: &Node<'_>, group: &str, name: &str) -> Prop<String> {
    match node.property_string(group, name) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PropertyNotFound { .. }) => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}

fn optional_u32(node: &Node<'_>, group: &str, name: &str) -> Prop<u32> {
    match node.property_u32(group, name) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PropertyNotFound { .. }) => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}
