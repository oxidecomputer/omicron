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
//! child carrying the driver instance, with one `disk` child per active
//! namespace. A bay holding anything else gets a `board` child with an `ic`
//! under it as a placeholder.

use crate::disk_bay::{ObservedBay, ObservedNamespace, ObservedOccupant};
use crate::nvme_instance::NvmeInstance;
use libtopo::{Error, Node, Scheme, TopoHdl, WalkAction, hc};
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
    UnlabelledBay { node: String },

    #[error("topology node {node} ({location}) has no binding/slot")]
    BayWithoutSlot { node: String, location: String },

    #[error("{location} has more than one occupant in the topology")]
    MultipleOccupants { location: String },

    #[error("topology disk node {node} in {location} has no {property}")]
    IncompleteDisk { node: String, location: String, property: String },

    #[error("{location} holds an NVMe device the topology failed to enumerate")]
    NvmeNotEnumerated { location: String },
}

/// Takes a fresh topology snapshot and returns every U.2 bay and M.2 socket
/// with what the topology found behind it, in walk order.
///
/// A new handle is opened on every call: the wrapper allows one snapshot per
/// handle (see <https://github.com/oxidecomputer/libtopo/issues/14>), and a
/// snapshot costs far more than the handle anyway.
pub(super) fn read_disk_bays(
    log: &Logger,
) -> Result<Vec<ObservedBay>, TopoError> {
    let start = Instant::now();
    let hdl = TopoHdl::open()?;
    let snap = hdl.snapshot()?;

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
    let bays = walker.finish()?;

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

    fn visit(&mut self, node: &Node<'_>) -> Result<(), TopoError> {
        match node.name().as_ref() {
            hc::BAY => self.visit_bay(node, DiskVariant::U2),
            hc::SLOT => {
                let slot_type = optional_u32(
                    node,
                    hc::TOPO_PGROUP_SLOT,
                    hc::TOPO_PROP_SLOT_TYPE,
                )?;
                if slot_type == Some(TOPO_SLOT_TYPE_M2) {
                    self.visit_bay(node, DiskVariant::M2)
                } else {
                    Ok(())
                }
            }
            hc::NVME => self.visit_nvme(node),
            hc::DISK => self.visit_disk(node),
            hc::BOARD => self.visit_board(node),
            hc::IC => self.visit_ic(node),
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
            _ => {
                return Err(TopoError::UnlabelledBay { node: key.to_string() });
            }
        };
        let Some(pcie_slot) =
            optional_u32(node, hc::TOPO_PGROUP_BINDING, hc::TOPO_BINDING_SLOT)?
        else {
            return Err(TopoError::BayWithoutSlot {
                node: key.to_string(),
                location,
            });
        };
        let binding_driver = optional_string(
            node,
            hc::TOPO_PGROUP_BINDING,
            hc::TOPO_BINDING_DRIVER,
        )?;

        self.bay_of.insert(key, self.bays.len());
        self.bays.push(ObservedBay {
            location,
            kind,
            pcie_slot: i64::from(pcie_slot),
            binding_driver,
            occupant: ObservedOccupant::Empty,
        });
        Ok(())
    }

    /// The bay directly above `node`, if `node` is a bay's child.
    fn bay_above(&self, node: &Node<'_>) -> Option<usize> {
        NodeKey::of_parent(node).and_then(|key| self.bay_of.get(&key).copied())
    }

    /// Claims the bay for a new occupant, which must be its first.
    fn occupy(
        &mut self,
        index: usize,
        occupant: ObservedOccupant,
    ) -> Result<(), TopoError> {
        let bay = &mut self.bays[index];
        if bay.occupant != ObservedOccupant::Empty {
            return Err(TopoError::MultipleOccupants {
                location: bay.location.clone(),
            });
        }
        bay.occupant = occupant;
        Ok(())
    }

    fn visit_nvme(&mut self, node: &Node<'_>) -> Result<(), TopoError> {
        // An nvme node elsewhere in the tree (a PCIe add-in card, or a
        // non-Oxide platform) is not in a disk bay.
        let Some(index) = self.bay_above(node) else {
            return Ok(());
        };
        let driver =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DRIVER)?;
        let devfs_path =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DEV_PATH)?;
        let occupant = match nvme_instance_of(self.log, node) {
            Some(instance) => {
                self.nvme_of.insert(NodeKey::of(node), index);
                ObservedOccupant::Nvme {
                    instance,
                    driver,
                    devfs_path,
                    namespaces: Vec::new(),
                }
            }
            // Without an instance this controller can never be opened, so
            // report the device and let the poll carry on.
            None => ObservedOccupant::Other {
                driver: driver.or_else(|| Some("nvme".to_string())),
                devfs_path,
            },
        };
        self.occupy(index, occupant)
    }

    fn visit_disk(&mut self, node: &Node<'_>) -> Result<(), TopoError> {
        let Some(index) = NodeKey::of_parent(node)
            .and_then(|key| self.nvme_of.get(&key).copied())
        else {
            return Ok(());
        };
        let bay = &mut self.bays[index];
        let ObservedOccupant::Nvme { namespaces, .. } = &mut bay.occupant
        else {
            // `nvme_of` only holds bays whose occupant is `Nvme`.
            unreachable!(
                "nvme node {} indexed without an Nvme occupant",
                index
            );
        };

        let key = NodeKey::of(node);
        let required = |group: &str, name: &str| -> Result<String, TopoError> {
            optional_string(node, group, name)?.ok_or_else(|| {
                TopoError::IncompleteDisk {
                    node: key.to_string(),
                    location: bay.location.clone(),
                    property: format!("{group}/{name}"),
                }
            })
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
        namespaces.push(namespace);
        Ok(())
    }

    fn visit_board(&mut self, node: &Node<'_>) -> Result<(), TopoError> {
        let Some(index) = self.bay_above(node) else {
            return Ok(());
        };
        self.board_of.insert(NodeKey::of(node), index);
        self.occupy(
            index,
            ObservedOccupant::Other { driver: None, devfs_path: None },
        )
    }

    fn visit_ic(&mut self, node: &Node<'_>) -> Result<(), TopoError> {
        let Some(index) = NodeKey::of_parent(node)
            .and_then(|key| self.board_of.get(&key).copied())
        else {
            return Ok(());
        };
        let io_driver =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DRIVER)?;
        let io_path =
            optional_string(node, hc::TOPO_PGROUP_IO, hc::TOPO_IO_DEV_PATH)?;
        if let ObservedOccupant::Other { driver, devfs_path } =
            &mut self.bays[index].occupant
        {
            *driver = io_driver;
            *devfs_path = io_path;
        }
        Ok(())
    }

    /// Checks the finished list for a bay the topology only half described:
    /// it found an NVMe device behind the bridge (and said so on the bay)
    /// but its disk enumerator produced nothing. That snapshot cannot be
    /// trusted to say which disks exist.
    fn finish(self) -> Result<Vec<ObservedBay>, TopoError> {
        for bay in &self.bays {
            if bay.binding_driver.as_deref() == Some("nvme")
                && bay.occupant == ObservedOccupant::Empty
            {
                return Err(TopoError::NvmeNotEnumerated {
                    location: bay.location.clone(),
                });
            }
        }
        Ok(self.bays)
    }
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
) -> Result<Option<String>, TopoError> {
    match node.property_string(group, name) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PropertyNotFound { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// A uint32 property, or `None` if the node does not have it.
fn optional_u32(
    node: &Node<'_>,
    group: &str,
    name: &str,
) -> Result<Option<u32>, TopoError> {
    match node.property_u32(group, name) {
        Ok(value) => Ok(Some(value)),
        Err(Error::PropertyNotFound { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}
