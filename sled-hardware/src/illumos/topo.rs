// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries against the illumos hardware topology (libtopo).
//!
//! libtopo describes the platform as a tree of nodes with labels and
//! properties, built from platform topology maps, devinfo, libnvme, and
//! other sources. This module reads the part of that tree that describes
//! disk bays: on Oxide boards, ten `bay` nodes under the chassis for the
//! U.2s and two M.2 `slot` nodes under the system board.
//!
//! The `oxhc` enumerator fills each of those in from the PCIe bridge behind
//! it. An empty bay has no children. A bay holding an NVMe device gets
//! `binding/driver = "nvme"` and, if the disk enumerator succeeds, an `nvme`
//! child carrying the driver instance, with one `disk` child per
//! namespace the nvme driver does not ignore. A bay holding anything else
//! gets a `board` child with an `ic` under it as a placeholder.

use crate::disk_bay::{
    BayContents, BayProblem, ObservedBay, ObservedNamespace, ObservedOccupant,
};
use crate::nvme_instance::NvmeInstance;
use libtopo::{Error, Node, Scheme, Snapshot, WalkAction, hc};
use sled_agent_types::disk::DiskVariant;
use slog::{Logger, debug, warn};
use std::collections::HashMap;
use std::time::Instant;

/// The `slot/slot-type` of an M.2 socket: `TOPO_SLOT_TYPE_M2` in
/// `<fm/topo_hc.h>`, which libtopo does not yet export by name.
const TOPO_SLOT_TYPE_M2: u32 = 3;

/// `storage` property names from `<fm/topo_hc.h>` that libtopo does not yet
/// export in `libtopo::hc`.
const TOPO_STORAGE_SERIAL_NUM: &str = "serial-number";
const TOPO_STORAGE_LOGICAL_DISK_NAME: &str = "logical-disk";

#[derive(Debug, thiserror::Error)]
pub(super) enum TopoError {
    #[error("failed to read the hardware topology")]
    Topo(#[from] libtopo::Error),

    #[error("topology node {node} has no location label")]
    UnlabelledBay {
        node: String,
        #[source]
        source: Option<libtopo::Error>,
    },

    #[error("could not read the slot type of topology node {node}")]
    SlotType {
        node: String,
        #[source]
        source: libtopo::Error,
    },
}

/// Returns every U.2 bay and M.2 socket in the snapshot with what the
/// topology found behind it, in walk order. A bay the topology only partly
/// describes, or whose nodes cannot be read, comes back with an `Err` in
/// [`ObservedBay::contents`].
///
/// The whole read fails only if the walk fails or a bay cannot be identified
/// (no label, or a slot whose type cannot be read). Leaving such a bay out
/// instead would drop its disk until it can be read again.
pub(super) fn read_disk_bays(
    log: &Logger,
    snap: &Snapshot<'_>,
) -> Result<Vec<ObservedBay>, TopoError> {
    let start = Instant::now();

    let mut walker = BayWalker::new(log);
    // The walk callback can only return libtopo's error type, so keep ours
    // aside and stop the walk instead.
    let mut failure = None;
    snap.walk(Scheme::Hc, |node| match walker.visit(&node) {
        Ok(()) => Ok(WalkAction::Continue),
        Err(err) => {
            failure = Some(err);
            Ok(WalkAction::Stop)
        }
    })?;
    if let Some(err) = failure {
        return Err(err);
    }
    let bays = walker.finish();

    debug!(
        log,
        "read disk bays from hardware topology";
        "count" => bays.len(),
        "elapsed" => ?start.elapsed(),
    );
    Ok(bays)
}

/// A topology node's identity within one snapshot: the names and instances
/// of the node and all its ancestors, such as `chassis[0]/bay[9]/board[0]`.
///
/// Name and instance alone do not identify a node: every bay's controller is
/// `nvme[0]`, and `board[0]/ic[0]` placeholders appear under bays and under
/// sharkfin slots alike.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NodeKey(String);

impl NodeKey {
    fn of(node: &Node<'_>) -> Self {
        let mut parts = vec![format!("{}[{}]", node.name(), node.instance())];
        let mut cursor = node.parent();
        while let Some(ancestor) = cursor {
            parts.push(format!("{}[{}]", ancestor.name(), ancestor.instance()));
            cursor = ancestor.parent();
        }
        parts.reverse();
        Self(parts.join("/"))
    }

    fn of_parent(node: &Node<'_>) -> Option<Self> {
        node.parent().map(|parent| Self::of(&parent))
    }
}

impl std::fmt::Display for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Builds the list of bays over one pre-order walk of the `hc` tree.
///
/// The walk visits a parent before its children, so a bay is registered
/// before the nodes beneath it arrive, and each occupant finds its bay by
/// looking up its parent's key.
struct BayWalker<'a> {
    log: &'a Logger,
    bays: Vec<ObservedBay>,
    /// Bay or M.2 slot node -> index into `bays`.
    bay_of: HashMap<NodeKey, usize>,
    /// `nvme` node with an instance -> index of its bay.
    nvme_of: HashMap<NodeKey, usize>,
    /// `board` placeholder node -> index of its bay.
    board_of: HashMap<NodeKey, usize>,
}

impl<'a> BayWalker<'a> {
    fn new(log: &'a Logger) -> Self {
        Self {
            log,
            bays: Vec::new(),
            bay_of: HashMap::new(),
            nvme_of: HashMap::new(),
            board_of: HashMap::new(),
        }
    }

    /// Fails only for a bay that cannot be identified. A node beneath a bay
    /// that cannot be read makes that bay unreadable instead.
    fn visit(&mut self, node: &Node<'_>) -> Result<(), TopoError> {
        match node.name().as_ref() {
            hc::BAY => self.visit_bay(node, DiskVariant::U2),
            hc::SLOT => {
                let slot_type = optional_u32(
                    node,
                    hc::TOPO_PGROUP_SLOT,
                    hc::TOPO_PROP_SLOT_TYPE,
                )
                .map_err(|source| TopoError::SlotType {
                    node: NodeKey::of(node).to_string(),
                    source,
                })?;
                if slot_type == Some(TOPO_SLOT_TYPE_M2) {
                    self.visit_bay(node, DiskVariant::M2)
                } else {
                    Ok(())
                }
            }
            hc::NVME => {
                // An nvme node elsewhere in the tree (a PCIe add-in card, or
                // a non-Oxide platform) is not in a disk bay.
                let Some(index) = self.bay_above(node) else {
                    return Ok(());
                };
                if let Err(err) = self.visit_nvme(node, index) {
                    self.mark_unreadable(index, node, err);
                }
                Ok(())
            }
            hc::DISK => {
                let Some(index) = NodeKey::of_parent(node)
                    .and_then(|key| self.nvme_of.get(&key).copied())
                else {
                    return Ok(());
                };
                if let Err(err) = self.visit_disk(node, index) {
                    self.mark_unreadable(index, node, err);
                }
                Ok(())
            }
            hc::BOARD => {
                if let Some(index) = self.bay_above(node) {
                    self.visit_board(node, index);
                }
                Ok(())
            }
            hc::IC => {
                let Some(index) = NodeKey::of_parent(node)
                    .and_then(|key| self.board_of.get(&key).copied())
                else {
                    return Ok(());
                };
                if let Err(err) = self.visit_ic(node, index) {
                    self.mark_unreadable(index, node, err);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn visit_bay(
        &mut self,
        node: &Node<'_>,
        kind: DiskVariant,
    ) -> Result<(), TopoError> {
        let key = NodeKey::of(node);
        let location = match node.label() {
            Ok(label) if !label.is_empty() => label,
            Ok(_) => {
                return Err(TopoError::UnlabelledBay {
                    node: key.to_string(),
                    source: None,
                });
            }
            Err(err) => {
                return Err(TopoError::UnlabelledBay {
                    node: key.to_string(),
                    source: Some(err),
                });
            }
        };

        let (binding_driver, driver_problem) = match optional_string(
            node,
            hc::TOPO_PGROUP_BINDING,
            hc::TOPO_BINDING_DRIVER,
        ) {
            Ok(driver) => (driver, None),
            Err(err) => (None, Some(topology_problem(&key, err))),
        };
        let pcie_slot = match optional_u32(
            node,
            hc::TOPO_PGROUP_BINDING,
            hc::TOPO_BINDING_SLOT,
        ) {
            Ok(Some(slot)) => Ok(i64::from(slot)),
            Ok(None) => Err(BayProblem::NoPcieSlot),
            Err(err) => Err(topology_problem(&key, err)),
        };
        let contents = match (pcie_slot, driver_problem) {
            (Err(problem), _) | (Ok(_), Some(problem)) => Err(problem),
            (Ok(pcie_slot), None) => {
                Ok(BayContents { pcie_slot, occupant: ObservedOccupant::Empty })
            }
        };

        self.bay_of.insert(key, self.bays.len());
        self.bays.push(ObservedBay {
            location,
            kind,
            binding_driver,
            contents,
        });
        Ok(())
    }

    /// The bay directly above `node`, if `node` is a bay's child.
    fn bay_above(&self, node: &Node<'_>) -> Option<usize> {
        NodeKey::of_parent(node).and_then(|key| self.bay_of.get(&key).copied())
    }

    /// Records why the bay is unreadable, unless an earlier problem already
    /// made it so.
    fn set_problem(&mut self, index: usize, problem: BayProblem) {
        let bay = &mut self.bays[index];
        if bay.contents.is_ok() {
            bay.contents = Err(problem);
        }
    }

    fn mark_unreadable(&mut self, index: usize, node: &Node<'_>, err: Error) {
        self.set_problem(index, topology_problem(&NodeKey::of(node), err));
    }

    /// Claims the bay for a new occupant, which should be its first.
    fn occupy(&mut self, index: usize, occupant: ObservedOccupant) {
        let bay = &mut self.bays[index];
        let Ok(contents) = &mut bay.contents else {
            return;
        };
        if contents.occupant == ObservedOccupant::Empty {
            contents.occupant = occupant;
        } else {
            bay.contents = Err(BayProblem::MultipleOccupants);
        }
    }

    fn visit_nvme(
        &mut self,
        node: &Node<'_>,
        index: usize,
    ) -> Result<(), Error> {
        let driver =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DRIVER)?;
        let devfs_path =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DEV_PATH)?;
        match nvme_instance_of(self.log, node) {
            Some(instance) => {
                self.nvme_of.insert(NodeKey::of(node), index);
                self.occupy(
                    index,
                    ObservedOccupant::Nvme {
                        instance,
                        driver,
                        devfs_path,
                        namespaces: Vec::new(),
                    },
                );
            }
            None => self.set_problem(index, BayProblem::NvmeWithoutInstance),
        }
        Ok(())
    }

    fn visit_disk(
        &mut self,
        node: &Node<'_>,
        index: usize,
    ) -> Result<(), Error> {
        // topo sets the `storage` and `io/devfs-path` properties only when
        // the namespace has a blkdev attached.
        let mut missing = None;
        let mut required = |group: &str, name: &str| -> Result<String, Error> {
            let value = optional_string(node, group, name)?;
            if value.is_none() && missing.is_none() {
                missing = Some(format!("{group}/{name}"));
            }
            Ok(value.unwrap_or_default())
        };
        let namespace = ObservedNamespace {
            manufacturer: required(
                hc::TOPO_PGROUP_STORAGE,
                hc::TOPO_STORAGE_MANUFACTURER,
            )?,
            model: required(hc::TOPO_PGROUP_STORAGE, hc::TOPO_STORAGE_MODEL)?,
            serial: required(hc::TOPO_PGROUP_STORAGE, TOPO_STORAGE_SERIAL_NUM)?,
            devfs_path: required(hc::TOPO_PGROUP_IO, hc::TOPO_IO_DEV_PATH)?,
            logical_disk: optional_string(
                node,
                hc::TOPO_PGROUP_STORAGE,
                TOPO_STORAGE_LOGICAL_DISK_NAME,
            )?,
        };

        if let Some(property) = missing {
            self.set_problem(
                index,
                BayProblem::IncompleteDisk {
                    node: NodeKey::of(node).to_string(),
                    property,
                },
            );
        } else if let Ok(BayContents {
            occupant: ObservedOccupant::Nvme { namespaces, .. },
            ..
        }) = &mut self.bays[index].contents
        {
            namespaces.push(namespace);
        }
        Ok(())
    }

    fn visit_board(&mut self, node: &Node<'_>, index: usize) {
        self.board_of.insert(NodeKey::of(node), index);
        self.occupy(
            index,
            ObservedOccupant::Other { driver: None, devfs_path: None },
        );
    }

    fn visit_ic(&mut self, node: &Node<'_>, index: usize) -> Result<(), Error> {
        let io_driver =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DRIVER)?;
        let io_path =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DEV_PATH)?;
        if let Ok(BayContents {
            occupant: ObservedOccupant::Other { driver, devfs_path },
            ..
        }) = &mut self.bays[index].contents
        {
            *driver = io_driver;
            *devfs_path = io_path;
        }
        Ok(())
    }

    /// Marks bays the topology only half described: it found an NVMe device
    /// behind the bridge (and said so on the bay) but its disk enumerator
    /// produced nothing.
    fn finish(mut self) -> Vec<ObservedBay> {
        for bay in &mut self.bays {
            let empty = matches!(
                &bay.contents,
                Ok(BayContents { occupant: ObservedOccupant::Empty, .. })
            );
            if empty && bay.binding_driver.as_deref() == Some("nvme") {
                bay.contents = Err(BayProblem::NvmeNotEnumerated);
            }
        }
        self.bays
    }
}

fn topology_problem(node: &NodeKey, err: Error) -> BayProblem {
    BayProblem::Topology { node: node.to_string(), error: err.to_string() }
}

/// The driver instance of an `nvme` node, from its `io/instance` property.
/// `None`, with a log line, if the node has no such property or it is not the
/// uint32 topo documents it as.
fn nvme_instance_of(log: &Logger, node: &Node<'_>) -> Option<NvmeInstance> {
    let value =
        match node.property_u32(hc::TOPO_PGROUP_IO, hc::TOPO_IO_INSTANCE) {
            Ok(value) => value,
            Err(Error::PropertyNotFound { .. }) => {
                debug!(
                    log,
                    "nvme topology node has no io/instance property";
                    "node_instance" => node.instance(),
                );
                return None;
            }
            Err(err) => {
                warn!(
                    log,
                    "ignoring nvme topology node with unreadable io/instance";
                    "err" => %err,
                );
                return None;
            }
        };
    match NvmeInstance::from_topo(value) {
        Ok(instance) => Some(instance),
        Err(err) => {
            warn!(
                log,
                "ignoring nvme topology node with unusable instance";
                "err" => %err,
            );
            None
        }
    }
}

/// A string property, or `None` if the node does not have it.
fn optional_string(
    node: &Node<'_>,
    group: &str,
    name: &str,
) -> Result<Option<String>, Error> {
    match node.property_string(group, name) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PropertyNotFound { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

/// A uint32 property, or `None` if the node does not have it.
fn optional_u32(
    node: &Node<'_>,
    group: &str,
    name: &str,
) -> Result<Option<u32>, Error> {
    match node.property_u32(group, name) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PropertyNotFound { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}
