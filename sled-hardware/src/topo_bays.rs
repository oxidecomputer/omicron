// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Builds the disk bays from the nodes of the hardware topology.
//!
//! On Oxide boards the U.2 bays are `bay` nodes under the chassis and the
//! M.2 sockets are `slot` nodes whose slot type is M.2; other slots, such as
//! CEM slots, are not disk bays. The `oxhc`
//! enumerator fills each of those in from the PCIe bridge behind it. An
//! empty bay has no children. A bay holding an NVMe device gets
//! `binding/driver = "nvme"` and, if the disk enumerator succeeds, an `nvme`
//! child carrying the driver instance, with one `disk` child per namespace
//! the nvme driver does not ignore. A bay holding anything else gets a
//! `board` child with an `ic` under it as a placeholder.
//!
//! The illumos module reads the topology into [`TopoNode`]s; everything
//! here is plain data manipulation so it can be tested anywhere.

// The topology is only read on illumos.
#![cfg_attr(not(target_os = "illumos"), allow(dead_code))]

use crate::DiskLocation;
use crate::disk_bay::{
    BayContents, BayProblem, ObservedBay, ObservedNamespace, ObservedOccupant,
};
use crate::nvme_instance::NvmeInstance;
use sled_agent_types::disk::DiskVariant;
use slog::{Logger, debug, warn};
use std::collections::HashMap;

/// A property of a topology node: `Ok(None)` if the node does not have it,
/// `Err` with libtopo's message if it could not be read.
pub(crate) type Prop<T> = Result<Option<T>, String>;

/// `TOPO_SLOT_TYPE_M2` in `<fm/topo_hc.h>`, which libtopo does not yet
/// export by name.
const TOPO_SLOT_TYPE_M2: u32 = 3;

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

/// One topology node that may describe part of a disk bay, with the
/// properties sled-hardware reads from it.
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

/// A bay that cannot be identified, which fails the whole read: a bay left
/// out of a successful read would look like one the topology no longer has.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BayIdentError {
    #[error("topology node {node} has an empty location label")]
    EmptyLabel { node: String },

    #[error(
        "could not read the location label of topology node {node}: {error}"
    )]
    UnreadableLabel { node: String, error: String },

    #[error(
        "topology node {node} has location \"{location}\", as does another \
         bay"
    )]
    DuplicateLocation { node: String, location: DiskLocation },

    #[error("could not read the slot type of topology node {node}: {error}")]
    SlotType { node: String, error: String },
}

/// Returns every bay among `nodes`, in order, with what the topology found
/// behind it. A bay the topology only partly describes, or whose nodes could
/// not be read, comes back with an `Err` in [`ObservedBay::contents`].
///
/// A node belongs to a bay through its parent: an `nvme` or `board` directly
/// under the bay, a `disk` under that `nvme`, an `ic` under that `board`.
/// Nodes anywhere else are ignored.
pub(crate) fn assemble_bays(
    log: &Logger,
    nodes: &[TopoNode],
) -> Result<Vec<ObservedBay>, BayIdentError> {
    let mut children: HashMap<&NodeKey, Vec<&TopoNode>> = HashMap::new();
    for node in nodes {
        if let Some(parent) = &node.parent {
            children.entry(parent).or_default().push(node);
        }
    }
    let children_of =
        |key: &NodeKey| children.get(key).map(Vec::as_slice).unwrap_or(&[]);

    let mut bays: Vec<ObservedBay> = Vec::new();
    for node in nodes {
        let Some((kind, props)) = bay_kind(node)? else {
            continue;
        };
        let location = identify_bay(&node.key, &props.label, &bays)?;
        let mut bay = new_bay(&node.key, location, kind, props);

        for child in children_of(&node.key) {
            match &child.kind {
                TopoNodeKind::Nvme { instance, driver, devfs_path } => {
                    let added = add_nvme(
                        log, &mut bay, child, instance, driver, devfs_path,
                    );
                    if added {
                        for disk in children_of(&child.key) {
                            add_disk(&mut bay, disk);
                        }
                    }
                }
                TopoNodeKind::Board => {
                    occupy(
                        &mut bay,
                        ObservedOccupant::Other {
                            driver: None,
                            devfs_path: None,
                        },
                    );
                    for ic in children_of(&child.key) {
                        add_ic(&mut bay, ic);
                    }
                }
                _ => {}
            }
        }

        // The topology found an NVMe device behind the bridge, and said so
        // on the bay, but its disk enumerator produced nothing.
        let empty = matches!(
            &bay.contents,
            Ok(BayContents { occupant: ObservedOccupant::Empty, .. })
        );
        if empty && bay.binding_driver.as_deref() == Some("nvme") {
            bay.contents = Err(BayProblem::NvmeNotEnumerated);
        }
        bays.push(bay);
    }
    Ok(bays)
}

/// Whether `node` is a disk bay, and of which kind.
fn bay_kind(
    node: &TopoNode,
) -> Result<Option<(DiskVariant, &BayProps)>, BayIdentError> {
    match &node.kind {
        TopoNodeKind::Bay(props) => Ok(Some((DiskVariant::U2, props))),
        TopoNodeKind::Slot { slot_type, props } => match slot_type {
            Ok(Some(TOPO_SLOT_TYPE_M2)) => Ok(Some((DiskVariant::M2, props))),
            Ok(_) => Ok(None),
            Err(error) => Err(BayIdentError::SlotType {
                node: node.key.to_string(),
                error: error.clone(),
            }),
        },
        _ => Ok(None),
    }
}

fn identify_bay(
    key: &NodeKey,
    label: &Result<String, String>,
    bays: &[ObservedBay],
) -> Result<DiskLocation, BayIdentError> {
    let location = match label {
        Ok(label) if !label.is_empty() => DiskLocation::new(label.clone()),
        Ok(_) => {
            return Err(BayIdentError::EmptyLabel { node: key.to_string() });
        }
        Err(error) => {
            return Err(BayIdentError::UnreadableLabel {
                node: key.to_string(),
                error: error.clone(),
            });
        }
    };
    if bays.iter().any(|bay| bay.location == location) {
        return Err(BayIdentError::DuplicateLocation {
            node: key.to_string(),
            location,
        });
    }
    Ok(location)
}

fn new_bay(
    key: &NodeKey,
    location: DiskLocation,
    kind: DiskVariant,
    props: &BayProps,
) -> ObservedBay {
    let (binding_driver, driver_problem) = match &props.binding_driver {
        Ok(driver) => (driver.clone(), None),
        Err(error) => (None, Some(topology_problem(key, error))),
    };
    let pcie_slot = match &props.binding_slot {
        Ok(Some(slot)) => Ok(i64::from(*slot)),
        Ok(None) => Err(BayProblem::NoPcieSlot),
        Err(error) => Err(topology_problem(key, error)),
    };
    let contents = match (pcie_slot, driver_problem) {
        (Err(problem), _) | (Ok(_), Some(problem)) => Err(problem),
        (Ok(pcie_slot), None) => {
            Ok(BayContents { pcie_slot, occupant: ObservedOccupant::Empty })
        }
    };
    ObservedBay { location, kind, binding_driver, contents }
}

/// Records the `nvme` node as the bay's occupant. Returns whether it was
/// recorded, so that its namespaces belong to the bay.
fn add_nvme(
    log: &Logger,
    bay: &mut ObservedBay,
    node: &TopoNode,
    instance: &Prop<u32>,
    driver: &Prop<String>,
    devfs_path: &Prop<String>,
) -> bool {
    let (driver, devfs_path) = match (driver, devfs_path) {
        (Ok(driver), Ok(devfs_path)) => (driver.clone(), devfs_path.clone()),
        (Err(error), _) | (_, Err(error)) => {
            set_problem(bay, topology_problem(&node.key, error));
            return false;
        }
    };
    let Some(instance) = nvme_instance(log, &node.key, instance) else {
        set_problem(bay, BayProblem::NvmeWithoutInstance);
        return false;
    };
    occupy(
        bay,
        ObservedOccupant::Nvme {
            instance,
            driver,
            devfs_path,
            namespaces: Vec::new(),
        },
    );
    true
}

/// The driver instance of an `nvme` node, or `None`, with a log line, if it
/// has none or it cannot be used.
fn nvme_instance(
    log: &Logger,
    key: &NodeKey,
    instance: &Prop<u32>,
) -> Option<NvmeInstance> {
    let value = match instance {
        Ok(Some(value)) => *value,
        Ok(None) => {
            debug!(
                log,
                "nvme topology node has no io/instance property";
                "node" => %key,
            );
            return None;
        }
        Err(error) => {
            warn!(
                log,
                "ignoring nvme topology node with unreadable io/instance";
                "node" => %key,
                "err" => error,
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
                "node" => %key,
                "err" => %err,
            );
            None
        }
    }
}

fn add_disk(bay: &mut ObservedBay, node: &TopoNode) {
    let TopoNodeKind::Disk {
        manufacturer,
        model,
        serial,
        devfs_path,
        logical_disk,
    } = &node.kind
    else {
        return;
    };

    // A property that cannot be read makes the bay unreadable before one
    // that is missing does.
    let props = [
        (manufacturer, "storage/manufacturer"),
        (model, "storage/model"),
        (serial, "storage/serial-number"),
        (devfs_path, "io/devfs-path"),
        (logical_disk, "storage/logical-disk"),
    ];
    if let Some(error) = props.iter().find_map(|(prop, _)| prop.as_ref().err())
    {
        set_problem(bay, topology_problem(&node.key, error));
        return;
    }

    let value = |prop: &Prop<String>| prop.clone().ok().flatten();
    let (Some(manufacturer), Some(model), Some(serial), Some(devfs_path)) =
        (value(manufacturer), value(model), value(serial), value(devfs_path))
    else {
        // Only the logical disk name is optional.
        let missing = props[..4]
            .iter()
            .find(|(prop, _)| value(prop).is_none())
            .map(|(_, name)| name.to_string())
            .unwrap_or_default();
        set_problem(
            bay,
            BayProblem::IncompleteDisk {
                node: node.key.to_string(),
                property: missing,
            },
        );
        return;
    };
    let namespace = ObservedNamespace {
        manufacturer,
        model,
        serial,
        devfs_path,
        logical_disk: value(logical_disk),
    };
    if let Ok(BayContents {
        occupant: ObservedOccupant::Nvme { namespaces, .. },
        ..
    }) = &mut bay.contents
    {
        namespaces.push(namespace);
    }
}

fn add_ic(bay: &mut ObservedBay, node: &TopoNode) {
    let TopoNodeKind::Ic { driver, devfs_path } = &node.kind else {
        return;
    };
    let (driver, devfs_path) = match (driver, devfs_path) {
        (Ok(driver), Ok(devfs_path)) => (driver, devfs_path),
        (Err(error), _) | (_, Err(error)) => {
            set_problem(bay, topology_problem(&node.key, error));
            return;
        }
    };
    if let Ok(BayContents {
        occupant: ObservedOccupant::Other { driver: d, devfs_path: p },
        ..
    }) = &mut bay.contents
    {
        *d = driver.clone();
        *p = devfs_path.clone();
    }
}

/// Records why the bay is unreadable, unless an earlier problem already
/// made it so.
fn set_problem(bay: &mut ObservedBay, problem: BayProblem) {
    if bay.contents.is_ok() {
        bay.contents = Err(problem);
    }
}

/// Claims the bay for a new occupant, which should be its first.
fn occupy(bay: &mut ObservedBay, occupant: ObservedOccupant) {
    let Ok(contents) = &mut bay.contents else {
        return;
    };
    if contents.occupant == ObservedOccupant::Empty {
        contents.occupant = occupant;
    } else {
        bay.contents = Err(BayProblem::MultipleOccupants);
    }
}

fn topology_problem(node: &NodeKey, error: &str) -> BayProblem {
    BayProblem::Topology { node: node.to_string(), error: error.to_string() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> Logger {
        Logger::root(slog::Discard, slog::o!())
    }

    /// Topology nodes in walk order. Each key's parent is the key without
    /// its last component.
    #[derive(Default)]
    struct Tree(Vec<TopoNode>);

    impl Tree {
        fn add(mut self, key: &str, kind: TopoNodeKind) -> Self {
            let parent = key.rsplit_once('/').map(|(p, _)| NodeKey::new(p));
            self.0.push(TopoNode { key: NodeKey::new(key), parent, kind });
            self
        }

        fn assemble(&self) -> Result<Vec<ObservedBay>, BayIdentError> {
            assemble_bays(&log(), &self.0)
        }

        /// The only bay in the tree.
        fn bay(&self) -> ObservedBay {
            let mut bays = self.assemble().unwrap();
            assert_eq!(bays.len(), 1, "{bays:?}");
            bays.pop().unwrap()
        }
    }

    const N3: &str = "chassis[0]/bay[3]";
    const N3_NVME: &str = "chassis[0]/bay[3]/nvme[0]";
    const N3_DISK: &str = "chassis[0]/bay[3]/nvme[0]/disk[0]";
    const N3_BOARD: &str = "chassis[0]/bay[3]/board[0]";
    const N3_IC: &str = "chassis[0]/bay[3]/board[0]/ic[0]";

    fn u2(label: &str, driver: Option<&str>) -> TopoNodeKind {
        TopoNodeKind::Bay(BayProps {
            label: Ok(label.to_string()),
            binding_driver: Ok(driver.map(str::to_string)),
            binding_slot: Ok(Some(3)),
        })
    }

    fn slot(label: &str, slot_type: Prop<u32>) -> TopoNodeKind {
        TopoNodeKind::Slot {
            slot_type,
            props: BayProps {
                label: Ok(label.to_string()),
                binding_driver: Ok(None),
                binding_slot: Ok(Some(17)),
            },
        }
    }

    fn nvme_bay() -> TopoNodeKind {
        u2("N3", Some("nvme"))
    }

    fn nvme(instance: u32) -> TopoNodeKind {
        TopoNodeKind::Nvme {
            instance: Ok(Some(instance)),
            driver: Ok(Some("nvme".to_string())),
            devfs_path: Ok(Some("/pci@0,0/pci1344,3100@0".to_string())),
        }
    }

    fn disk(serial: &str) -> TopoNodeKind {
        TopoNodeKind::Disk {
            manufacturer: Ok(Some("NVMe".to_string())),
            model: Ok(Some("Micron_7450".to_string())),
            serial: Ok(Some(serial.to_string())),
            devfs_path: Ok(Some("/pci@0,0/pci1344,3100@0/blkdev@w0,0".into())),
            logical_disk: Ok(Some("c1t0d0".to_string())),
        }
    }

    fn ic(driver: Option<&str>, devfs_path: Option<&str>) -> TopoNodeKind {
        TopoNodeKind::Ic {
            driver: Ok(driver.map(str::to_string)),
            devfs_path: Ok(devfs_path.map(str::to_string)),
        }
    }

    fn problem(bay: &ObservedBay) -> &BayProblem {
        bay.contents.as_ref().expect_err("bay should be unreadable")
    }

    fn occupant(bay: &ObservedBay) -> &ObservedOccupant {
        &bay.contents.as_ref().expect("bay should be readable").occupant
    }

    fn namespaces(bay: &ObservedBay) -> &[ObservedNamespace] {
        match occupant(bay) {
            ObservedOccupant::Nvme { namespaces, .. } => namespaces,
            other => panic!("expected an nvme occupant, found {other:?}"),
        }
    }

    #[test]
    fn empty_bay() {
        let bay = Tree::default().add(N3, u2("N3", None)).bay();
        assert_eq!(bay.location.as_str(), "N3");
        assert_eq!(bay.kind, DiskVariant::U2);
        assert_eq!(bay.binding_driver, None);
        assert_eq!(
            bay.contents,
            Ok(BayContents { pcie_slot: 3, occupant: ObservedOccupant::Empty })
        );
    }

    #[test]
    fn nvme_disk() {
        let bay = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(N3_DISK, disk("S1"))
            .bay();
        assert_eq!(bay.binding_driver.as_deref(), Some("nvme"));
        let ObservedOccupant::Nvme { instance, driver, devfs_path, namespaces } =
            occupant(&bay)
        else {
            panic!("expected an nvme occupant: {bay:?}");
        };
        assert_eq!(*instance, NvmeInstance::from_topo(4).unwrap());
        assert_eq!(driver.as_deref(), Some("nvme"));
        assert_eq!(devfs_path.as_deref(), Some("/pci@0,0/pci1344,3100@0"));
        assert_eq!(
            namespaces,
            &[ObservedNamespace {
                manufacturer: "NVMe".to_string(),
                model: "Micron_7450".to_string(),
                serial: "S1".to_string(),
                devfs_path: "/pci@0,0/pci1344,3100@0/blkdev@w0,0".to_string(),
                logical_disk: Some("c1t0d0".to_string()),
            }]
        );
    }

    #[test]
    fn every_namespace_is_recorded_in_order() {
        let bay = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(N3_DISK, disk("S1"))
            .add("chassis[0]/bay[3]/nvme[0]/disk[1]", disk("S2"))
            .bay();
        let serials: Vec<_> =
            namespaces(&bay).iter().map(|ns| ns.serial.as_str()).collect();
        assert_eq!(serials, ["S1", "S2"]);
    }

    #[test]
    fn logical_disk_is_optional() {
        let mut no_link = disk("S1");
        if let TopoNodeKind::Disk { logical_disk, .. } = &mut no_link {
            *logical_disk = Ok(None);
        }
        let bay = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(N3_DISK, no_link)
            .bay();
        assert_eq!(namespaces(&bay)[0].logical_disk, None);
    }

    #[test]
    fn m2_slots_are_bays_and_other_slots_are_not() {
        // `TOPO_SLOT_TYPE_CEM` in `<fm/topo_hc.h>`.
        const CEM: u32 = 4;
        let bays = Tree::default()
            .add(
                "chassis[0]/systemboard[0]/slot[0]",
                slot("M.2 East", Ok(Some(TOPO_SLOT_TYPE_M2))),
            )
            .add("chassis[0]/systemboard[0]/slot[1]", slot("No type", Ok(None)))
            .add(
                "chassis[0]/systemboard[0]/slot[5]",
                slot("CEM 5", Ok(Some(CEM))),
            )
            .add(
                "chassis[0]/systemboard[0]/slot[5]/board[0]",
                TopoNodeKind::Board,
            )
            .add(
                "chassis[0]/systemboard[0]/slot[5]/board[0]/ic[0]",
                ic(Some("max5970"), None),
            )
            .assemble()
            .unwrap();
        assert_eq!(bays.len(), 1);
        assert_eq!(bays[0].location.as_str(), "M.2 East");
        assert_eq!(bays[0].kind, DiskVariant::M2);
        assert_eq!(occupant(&bays[0]), &ObservedOccupant::Empty);
    }

    #[test]
    fn nvme_outside_a_bay_is_ignored() {
        let bays = Tree::default()
            .add(N3, u2("N3", None))
            .add("chassis[0]/systemboard[0]/slot[5]/nvme[0]", nvme(9))
            .add("chassis[0]/systemboard[0]/slot[5]/nvme[0]/disk[0]", disk("X"))
            .assemble()
            .unwrap();
        assert_eq!(bays.len(), 1);
        assert_eq!(occupant(&bays[0]), &ObservedOccupant::Empty);
    }

    #[test]
    fn placeholder_device() {
        let bay = Tree::default()
            .add(N3, u2("N3", Some("pciex8086,1234")))
            .add(N3_BOARD, TopoNodeKind::Board)
            .add(N3_IC, ic(Some("pciex8086,1234"), Some("/pci@0,0/x@0")))
            .bay();
        assert_eq!(
            occupant(&bay),
            &ObservedOccupant::Other {
                driver: Some("pciex8086,1234".to_string()),
                devfs_path: Some("/pci@0,0/x@0".to_string()),
            }
        );
    }

    #[test]
    fn placeholder_without_ic() {
        let bay = Tree::default()
            .add(N3, u2("N3", None))
            .add(N3_BOARD, TopoNodeKind::Board)
            .bay();
        assert_eq!(
            occupant(&bay),
            &ObservedOccupant::Other { driver: None, devfs_path: None }
        );
    }

    #[test]
    fn two_occupants() {
        let bay = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(N3_BOARD, TopoNodeKind::Board)
            .bay();
        assert_eq!(problem(&bay), &BayProblem::MultipleOccupants);
    }

    #[test]
    fn nvme_without_a_usable_instance() {
        let instances = [Ok(None), Ok(Some(1 << 31)), Err("bad".to_string())];
        for instance in instances {
            let mut node = nvme(0);
            if let TopoNodeKind::Nvme { instance: i, .. } = &mut node {
                *i = instance.clone();
            }
            let bay = Tree::default()
                .add(N3, nvme_bay())
                .add(N3_NVME, node)
                .add(N3_DISK, disk("S1"))
                .bay();
            assert_eq!(
                problem(&bay),
                &BayProblem::NvmeWithoutInstance,
                "instance {instance:?}"
            );
        }
    }

    #[test]
    fn incomplete_disk_names_the_first_missing_property() {
        let cases = [
            ("storage/manufacturer", 0),
            ("storage/model", 1),
            ("storage/serial-number", 2),
            ("io/devfs-path", 3),
        ];
        for (property, index) in cases {
            let mut node = disk("S1");
            if let TopoNodeKind::Disk {
                manufacturer,
                model,
                serial,
                devfs_path,
                ..
            } = &mut node
            {
                *[manufacturer, model, serial, devfs_path][index] = Ok(None);
            }
            let bay = Tree::default()
                .add(N3, nvme_bay())
                .add(N3_NVME, nvme(4))
                .add(N3_DISK, node)
                .bay();
            assert_eq!(
                problem(&bay),
                &BayProblem::IncompleteDisk {
                    node: N3_DISK.to_string(),
                    property: property.to_string(),
                }
            );
        }
    }

    #[test]
    fn unreadable_property_beats_missing_one() {
        let mut node = disk("S1");
        if let TopoNodeKind::Disk { manufacturer, logical_disk, .. } = &mut node
        {
            *manufacturer = Ok(None);
            *logical_disk = Err("type mismatch".to_string());
        }
        let bay = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(N3_DISK, node)
            .bay();
        assert_eq!(
            problem(&bay),
            &BayProblem::Topology {
                node: N3_DISK.to_string(),
                error: "type mismatch".to_string(),
            }
        );
    }

    #[test]
    fn unreadable_node_affects_only_its_bay() {
        let mut broken = nvme(4);
        if let TopoNodeKind::Nvme { driver, .. } = &mut broken {
            *driver = Err("type mismatch".to_string());
        }
        let bays = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, broken)
            .add(N3_DISK, disk("S1"))
            .add("chassis[0]/bay[4]", u2("N4", Some("nvme")))
            .add("chassis[0]/bay[4]/nvme[0]", nvme(5))
            .add("chassis[0]/bay[4]/nvme[0]/disk[0]", disk("S2"))
            .assemble()
            .unwrap();
        assert_eq!(
            problem(&bays[0]),
            &BayProblem::Topology {
                node: N3_NVME.to_string(),
                error: "type mismatch".to_string(),
            }
        );
        assert_eq!(namespaces(&bays[1])[0].serial, "S2");
    }

    #[test]
    fn unreadable_ic() {
        let mut broken = ic(None, None);
        if let TopoNodeKind::Ic { devfs_path, .. } = &mut broken {
            *devfs_path = Err("type mismatch".to_string());
        }
        let bay = Tree::default()
            .add(N3, u2("N3", None))
            .add(N3_BOARD, TopoNodeKind::Board)
            .add(N3_IC, broken)
            .bay();
        assert!(
            matches!(problem(&bay), BayProblem::Topology { node, .. } if node == N3_IC),
            "{bay:?}"
        );
    }

    #[test]
    fn first_problem_in_walk_order_wins() {
        // The walk visits the nvme's disk before the bay's second occupant.
        let bay = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(
                N3_DISK,
                TopoNodeKind::Disk {
                    manufacturer: Err("first".to_string()),
                    model: Ok(None),
                    serial: Ok(None),
                    devfs_path: Ok(None),
                    logical_disk: Ok(None),
                },
            )
            .add(N3_BOARD, TopoNodeKind::Board)
            .bay();
        assert_eq!(
            problem(&bay),
            &BayProblem::Topology {
                node: N3_DISK.to_string(),
                error: "first".to_string(),
            }
        );
    }

    #[test]
    fn nvme_not_enumerated() {
        let bay = Tree::default().add(N3, nvme_bay()).bay();
        assert_eq!(problem(&bay), &BayProblem::NvmeNotEnumerated);

        // Without `binding/driver = "nvme"` an empty bay is just empty.
        let bay = Tree::default().add(N3, u2("N3", Some("other"))).bay();
        assert_eq!(occupant(&bay), &ObservedOccupant::Empty);
    }

    #[test]
    fn bay_binding_problems() {
        let bay_with = |driver: Prop<String>, slot: Prop<u32>| {
            Tree::default()
                .add(
                    N3,
                    TopoNodeKind::Bay(BayProps {
                        label: Ok("N3".to_string()),
                        binding_driver: driver,
                        binding_slot: slot,
                    }),
                )
                .bay()
        };
        let topology = |error: &str| BayProblem::Topology {
            node: N3.to_string(),
            error: error.to_string(),
        };

        let bay = bay_with(Ok(None), Ok(None));
        assert_eq!(problem(&bay), &BayProblem::NoPcieSlot);

        let bay = bay_with(Ok(None), Err("slot".to_string()));
        assert_eq!(problem(&bay), &topology("slot"));

        let bay = bay_with(Err("driver".to_string()), Ok(Some(3)));
        assert_eq!(problem(&bay), &topology("driver"));
        assert_eq!(bay.binding_driver, None);

        // The slot's problem is reported before the driver's.
        let bay = bay_with(Err("driver".to_string()), Err("slot".to_string()));
        assert_eq!(problem(&bay), &topology("slot"));
    }

    #[test]
    fn unidentifiable_bays_fail_the_read() {
        let labelled = |label: Result<String, String>| {
            TopoNodeKind::Bay(BayProps {
                label,
                binding_driver: Ok(None),
                binding_slot: Ok(Some(3)),
            })
        };

        let err = Tree::default()
            .add(N3, labelled(Ok(String::new())))
            .assemble()
            .unwrap_err();
        assert_eq!(err, BayIdentError::EmptyLabel { node: N3.to_string() });

        let err = Tree::default()
            .add(N3, labelled(Err("no label".to_string())))
            .assemble()
            .unwrap_err();
        assert_eq!(
            err,
            BayIdentError::UnreadableLabel {
                node: N3.to_string(),
                error: "no label".to_string(),
            }
        );

        let err = Tree::default()
            .add(N3, u2("N3", None))
            .add("chassis[0]/bay[4]", u2("N3", None))
            .assemble()
            .unwrap_err();
        assert_eq!(
            err,
            BayIdentError::DuplicateLocation {
                node: "chassis[0]/bay[4]".to_string(),
                location: DiskLocation::new("N3"),
            }
        );

        let key = "chassis[0]/systemboard[0]/slot[0]";
        let err = Tree::default()
            .add(N3, u2("N3", None))
            .add(key, slot("M.2 East", Err("bad".to_string())))
            .assemble()
            .unwrap_err();
        assert_eq!(
            err,
            BayIdentError::SlotType {
                node: key.to_string(),
                error: "bad".to_string(),
            }
        );
    }

    #[test]
    fn children_attach_regardless_of_order() {
        let in_walk_order = Tree::default()
            .add(N3, nvme_bay())
            .add(N3_NVME, nvme(4))
            .add(N3_DISK, disk("S1"))
            .add("chassis[0]/bay[4]", u2("N4", None));
        let mut reordered = Tree::default();
        reordered.0 = in_walk_order.0.iter().rev().cloned().collect();

        let mut expected = in_walk_order.assemble().unwrap();
        expected.reverse();
        assert_eq!(reordered.assemble().unwrap(), expected);
    }
}
