// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Owned copies of hardware topology nodes, read on illumos and interpreted
//! elsewhere.

// The topology is only read on illumos.
#![cfg_attr(not(target_os = "illumos"), allow(dead_code))]

/// A property of a topology node: `Ok(None)` if the node does not have it,
/// `Err` with libtopo's message if it could not be read.
pub(crate) type Prop<T> = Result<Option<T>, String>;

/// A topology node's identity within one snapshot: the names and instances
/// of the node and all its ancestors, such as `chassis[0]/bay[9]/board[0]`.
///
/// Name and instance alone do not identify a node: every bay's controller
/// is `nvme[0]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NodeKey(String);

impl NodeKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }
}

impl std::fmt::Display for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One topology node, with the properties sled-hardware reads from it.
#[derive(Clone, Debug)]
pub(crate) struct TopoNode {
    pub key: NodeKey,
    pub parent: Option<NodeKey>,
    pub kind: TopoNodeKind,
}

/// Properties oxhc sets on a `bay`, and on a `slot` it fills in from a PCIe
/// bridge.
#[derive(Clone, Debug)]
pub(crate) struct BayProps {
    /// `protocol/label`.
    pub label: Result<String, String>,
    /// `binding/driver`: the driver bound to the device behind the bay.
    /// oxhc records it before it tries to enumerate the device.
    pub binding_driver: Prop<String>,
    /// `binding/slot`: the PCIe physical slot number of the bridge.
    pub binding_slot: Prop<u32>,
}

#[derive(Clone, Debug)]
pub(crate) enum TopoNodeKind {
    /// A U.2 bay.
    Bay(BayProps),
    /// Only a slot whose `slot/slot-type` is M.2 is a disk bay.
    Slot {
        slot_type: Prop<u32>,
        props: BayProps,
    },
    Nvme {
        /// `io/instance`.
        instance: Prop<u32>,
        /// `io/driver`.
        driver: Prop<String>,
        /// `io/devfs-path`.
        devfs_path: Prop<String>,
    },
    /// One NVMe namespace. topo sets the `storage` and `io/devfs-path`
    /// properties only when the namespace has a blkdev attached.
    Disk {
        manufacturer: Prop<String>,
        model: Prop<String>,
        serial: Prop<String>,
        devfs_path: Prop<String>,
        logical_disk: Prop<String>,
    },
    Board,
    Ic {
        /// `io/driver`.
        driver: Prop<String>,
        /// `io/devfs-path`.
        devfs_path: Prop<String>,
    },
}
