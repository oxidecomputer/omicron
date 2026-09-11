// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reading disk chassis locations from the illumos hardware topology.

use crate::disk_location::NvmeInstance;
use libtopo::{Node, PropValue, Scheme, TopoHdl, WalkAction};
use slog::{Logger, debug, warn};
use std::collections::HashMap;
use std::time::Instant;

// Node and property names from <fm/topo_hc.h>.
const NVME: &str = "nvme";
const BAY: &str = "bay";
const SLOT: &str = "slot";
const TOPO_PGROUP_IO: &str = "io";
const TOPO_IO_INSTANCE: &str = "instance";

/// Takes a fresh topology snapshot and returns the chassis label of every
/// NVMe controller that has one, keyed by driver instance.
///
/// This mirrors what `nvmeadm list -L` does. Each `nvme` node in the `hc`
/// scheme is located by its `io/instance` property. Its label is its own
/// `protocol/label` if it has one, else its parent's label if the parent is
/// a `bay` (a U.2 bay) or a `slot` (an M.2 socket). Controllers with neither
/// are omitted from the result.
///
/// A new handle is opened on every call. Taking a second snapshot on the same
/// handle is unsafe in libtopo (illumos issue 18110), and taking the snapshot
/// is what enumerates the hardware.
pub(super) fn read_disk_locations(
    log: &Logger,
) -> Result<HashMap<NvmeInstance, String>, libtopo::Error> {
    let start = Instant::now();
    let hdl = TopoHdl::open()?;
    let snap = hdl.snapshot()?;

    let mut labels = HashMap::new();
    snap.walk(Scheme::Hc, |node| {
        if node.name() != NVME {
            return Ok(WalkAction::Continue);
        }

        let instance = match node.property(TOPO_PGROUP_IO, TOPO_IO_INSTANCE) {
            Ok(PropValue::UInt32(value)) => match NvmeInstance::try_from(value)
            {
                Ok(instance) => instance,
                Err(err) => {
                    warn!(
                        log,
                        "ignoring nvme topology node with unusable instance";
                        "err" => %err,
                    );
                    return Ok(WalkAction::Continue);
                }
            },
            Ok(other) => {
                warn!(
                    log,
                    "ignoring nvme topology node whose io/instance is not a \
                     uint32";
                    "value" => ?other,
                );
                return Ok(WalkAction::Continue);
            }
            Err(_) => {
                debug!(
                    log,
                    "nvme topology node has no io/instance property";
                    "node_instance" => node.instance(),
                );
                return Ok(WalkAction::Continue);
            }
        };

        let label = label_of(&node).or_else(|| {
            node.parent()
                .filter(|parent| {
                    let name = parent.name();
                    name == BAY || name == SLOT
                })
                .and_then(|parent| label_of(&parent))
        });
        match label {
            Some(label) => {
                labels.insert(instance, label);
            }
            None => debug!(
                log,
                "nvme topology node has no location label";
                "nvme_instance" => %instance,
            ),
        }
        Ok(WalkAction::Continue)
    })?;

    debug!(
        log,
        "read disk locations from hardware topology";
        "count" => labels.len(),
        "elapsed" => ?start.elapsed(),
    );
    Ok(labels)
}

/// A node's label, treating "no label" and an empty label alike.
fn label_of(node: &Node<'_>) -> Option<String> {
    node.label().ok().filter(|label| !label.is_empty())
}
