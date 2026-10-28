// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The U.2 bays and M.2 sockets of a sled's chassis, and what occupies each.
//!
//! On an Oxide sled the hardware topology (libtopo) is the source of truth
//! for which bays exist and what sits behind each one. sled-hardware turns
//! that into a [`DiskBay`] per bay and, where the occupant is an NVMe disk
//! with a namespace, an [`UnparsedDisk`] to manage. The pieces that need the
//! platform (walking the topology, asking libnvme about a controller) live in
//! the illumos module; the join between them, `classify_bay`, is plain data
//! manipulation and is kept here so it can be tested anywhere.

// The observed types below are only produced on illumos.
#![cfg_attr(not(target_os = "illumos"), allow(dead_code))]

use crate::DiskFirmware;
use crate::UnparsedDisk;
use crate::nvme_instance::NvmeInstance;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sled_agent_types::disk::DiskIdentity;
use sled_agent_types::disk::DiskVariant;
use sled_agent_types::disk::M2Slot;
use slog::{Logger, warn};
use std::collections::HashMap;

/// One U.2 bay or M.2 socket of the chassis, as the hardware topology
/// describes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskBay {
    /// The label printed on the chassis, such as "N3" or "M.2 East".
    pub location: String,
    /// Whether this is a U.2 bay or an M.2 socket.
    pub kind: DiskVariant,
    pub occupant: DiskBayOccupant,
}

/// What the hardware topology found behind a bay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiskBayOccupant {
    /// Nothing is behind this bay. A drive whose PCIe link is down also
    /// looks like this until the host OS gains presence detection.
    Empty,
    /// An NVMe disk that sled-agent manages. It appears in the sled's disk
    /// list under this identity.
    Disk { identity: DiskIdentity },
    /// A device is attached but there is no disk sled-agent can manage: an
    /// NVMe controller with no usable namespace, something that is not NVMe
    /// at all, or a device sled-agent could not read.
    Device {
        /// The driver bound to the device, if one is.
        driver: Option<String>,
        /// The device's path under `/devices`, if the topology recorded one.
        devfs_path: Option<String>,
    },
}

/// One bay as the hardware topology reports it, before sled-hardware has
/// asked the NVMe controller anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObservedBay {
    pub location: String,
    pub kind: DiskVariant,
    /// `binding/driver` on the bay node: the driver the topology found bound
    /// to the device behind the bay. oxhc records it before it tries to
    /// enumerate the device, so it is present even when enumeration failed.
    pub binding_driver: Option<String>,
    /// What is behind the bay, or why the topology could not say.
    pub contents: Result<BayContents, BayProblem>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BayContents {
    /// The PCIe physical slot number of the bridge behind the bay
    /// (`binding/slot`).
    pub pcie_slot: i64,
    pub occupant: ObservedOccupant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ObservedOccupant {
    /// The bay node has no children.
    Empty,
    /// An `nvme` node with a driver instance. `namespaces` has one entry per
    /// `disk` child, which the topology creates per namespace the nvme driver
    /// does not ignore.
    Nvme {
        instance: NvmeInstance,
        driver: Option<String>,
        devfs_path: Option<String>,
        namespaces: Vec<ObservedNamespace>,
    },
    /// The topology's placeholder for a device it does not enumerate.
    Other { driver: Option<String>, devfs_path: Option<String> },
}

/// Why the topology could not describe one bay.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BayProblem {
    #[error("the topology reports more than one occupant")]
    MultipleOccupants,

    #[error("topology disk node {node} has no {property}")]
    IncompleteDisk { node: String, property: String },

    #[error("the topology found an NVMe device but did not enumerate it")]
    NvmeNotEnumerated,

    #[error("the topology's nvme node has no usable driver instance")]
    NvmeWithoutInstance,

    #[error("the topology has no binding/slot for the bay")]
    NoPcieSlot,

    #[error("could not read topology node {node}: {error}")]
    Topology { node: String, error: String },
}

/// A `disk` node under an `nvme` node: one NVMe namespace.
///
/// The three identity strings are the ones the topology copies from the
/// `blkdev` device node's `inquiry-*` properties, so they equal what devinfo
/// reports for the same namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObservedNamespace {
    /// `storage/manufacturer`, from `inquiry-vendor-id`.
    pub manufacturer: String,
    /// `storage/model`, from `inquiry-product-id`.
    pub model: String,
    /// `storage/serial-number`, from `inquiry-serial-no`.
    pub serial: String,
    /// `io/devfs-path`: the `blkdev` node's path, without the `/devices`
    /// prefix.
    pub devfs_path: String,
    /// `storage/logical-disk`: the name under `/dev/dsk`, such as
    /// `c1t00A0750130D5B4C7d0`, when the topology found the link.
    pub logical_disk: Option<String>,
}

/// What libnvme reports for the controller in a bay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NvmeFacts {
    pub pci_vid: u16,
    /// Namespaces the nvme driver does not ignore. It ignores those it
    /// cannot attach `blkdev` to, such as ones formatted with protection
    /// information, and the topology describes none of those.
    pub usable_namespaces: usize,
    pub firmware: DiskFirmware,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClassifyError {
    #[error("{location}: {problem}")]
    Unreadable { location: String, problem: BayProblem },

    #[error(
        "{location} ({nvme_instance}) has {usable_namespaces} usable \
         namespace(s) but the hardware topology describes none"
    )]
    MissingNamespace {
        location: String,
        nvme_instance: NvmeInstance,
        usable_namespaces: usize,
    },

    #[error(
        "{location} ({nvme_instance}) holds an NVMe controller but libnvme \
         was not consulted about it"
    )]
    MissingNvmeFacts { location: String, nvme_instance: NvmeInstance },

    #[error("{location}: topology devfs path {path:?} is not absolute")]
    RelativeDevfsPath { location: String, path: String },
}

/// Combines what the topology and libnvme said about a bay into the bay as
/// sled-agent reports it and, for an NVMe disk with a namespace, the disk to
/// manage.
///
/// `nvme` must be `Some` when the observed occupant is an NVMe controller.
/// `boot_storage_unit` is the M.2 the host booted from.
pub(crate) fn classify_bay(
    bay: &ObservedBay,
    nvme: Option<&NvmeFacts>,
    boot_storage_unit: M2Slot,
) -> Result<(DiskBay, Option<UnparsedDisk>), ClassifyError> {
    let describe = |occupant| DiskBay {
        location: bay.location.clone(),
        kind: bay.kind,
        occupant,
    };

    let contents = match &bay.contents {
        Ok(contents) => contents,
        Err(problem) => {
            return Err(ClassifyError::Unreadable {
                location: bay.location.clone(),
                problem: problem.clone(),
            });
        }
    };

    let (instance, driver, devfs_path, namespaces) = match &contents.occupant {
        ObservedOccupant::Empty => {
            return Ok((describe(DiskBayOccupant::Empty), None));
        }
        ObservedOccupant::Other { driver, devfs_path } => {
            let device = DiskBayOccupant::Device {
                driver: driver.clone(),
                devfs_path: devfs_path.clone(),
            };
            return Ok((describe(device), None));
        }
        ObservedOccupant::Nvme { instance, driver, devfs_path, namespaces } => {
            (*instance, driver, devfs_path, namespaces)
        }
    };

    let Some(nvme) = nvme else {
        return Err(ClassifyError::MissingNvmeFacts {
            location: bay.location.clone(),
            nvme_instance: instance,
        });
    };

    // The topology creates one `disk` node per usable namespace. If it has
    // none but the controller says it has some, the topology's namespace
    // enumeration failed silently.
    let Some(namespace) = namespaces.first() else {
        if nvme.usable_namespaces == 0 {
            let device = DiskBayOccupant::Device {
                driver: driver.clone(),
                devfs_path: devfs_path.clone(),
            };
            return Ok((describe(device), None));
        }
        return Err(ClassifyError::MissingNamespace {
            location: bay.location.clone(),
            nvme_instance: instance,
            usable_namespaces: nvme.usable_namespaces,
        });
    };

    // libdevinfo reports this path as absolute from the kernel's point of
    // view; userspace sees it under "/devices".
    if !namespace.devfs_path.starts_with('/') {
        return Err(ClassifyError::RelativeDevfsPath {
            location: bay.location.clone(),
            path: namespace.devfs_path.clone(),
        });
    }
    let devfs_path =
        Utf8PathBuf::from(format!("/devices{}", namespace.devfs_path));
    let dev_path = namespace
        .logical_disk
        .as_ref()
        .map(|name| Utf8PathBuf::from(format!("/dev/dsk/{name}")));

    let identity = DiskIdentity {
        vendor: format!("{:x}", nvme.pci_vid),
        serial: namespace.serial.clone(),
        model: disk_model(&namespace.manufacturer, &namespace.model),
    };

    let is_boot_disk = bay.kind == DiskVariant::M2
        && M2Slot::from_chassis_location(&bay.location)
            == Some(boot_storage_unit);

    let disk = UnparsedDisk::new(
        devfs_path,
        dev_path,
        contents.pcie_slot,
        bay.kind,
        identity.clone(),
        is_boot_disk,
        nvme.firmware.clone(),
        bay.location.clone(),
    );
    Ok((describe(DiskBayOccupant::Disk { identity }), Some(disk)))
}

/// One bay from a single hardware poll.
#[derive(Clone, Debug)]
pub(crate) enum PolledBay {
    Described {
        bay: DiskBay,
        disk: Option<Box<UnparsedDisk>>,
    },
    /// The topology or libnvme could not describe the bay on this poll.
    Unreadable {
        location: String,
        kind: DiskVariant,
        driver: Option<String>,
    },
}

/// Combines one poll's bays with the previous view into the disks and bays to
/// report.
///
/// A bay this poll could not describe keeps what the previous view said about
/// it, so a fault in one bay neither drops its disk nor holds up the others.
/// With no previous entry, it is reported as a device with no disk.
///
/// If two bays claim the same disk identity, a bay that saw the disk on this
/// poll wins over one carrying it over from the previous view (the disk may
/// have moved), and otherwise the first bay wins.
pub(crate) fn resolve_bays(
    log: &Logger,
    polled: Vec<PolledBay>,
    previous_bays: &[DiskBay],
    previous_disks: &HashMap<DiskIdentity, UnparsedDisk>,
) -> (HashMap<DiskIdentity, UnparsedDisk>, Vec<DiskBay>) {
    // Each bay, the disk it claims, and whether that claim is carried over.
    let mut claims: Vec<(DiskBay, Option<UnparsedDisk>, bool)> = polled
        .into_iter()
        .map(|polled_bay| match polled_bay {
            PolledBay::Described { bay, disk } => {
                (bay, disk.map(|d| *d), false)
            }
            PolledBay::Unreadable { location, kind, driver } => {
                match previous_bays.iter().find(|b| b.location == location) {
                    Some(previous) => {
                        let disk = match &previous.occupant {
                            DiskBayOccupant::Disk { identity } => {
                                previous_disks.get(identity).cloned()
                            }
                            _ => None,
                        };
                        (previous.clone(), disk, true)
                    }
                    None => {
                        let occupant = DiskBayOccupant::Device {
                            driver,
                            devfs_path: None,
                        };
                        (DiskBay { location, kind, occupant }, None, true)
                    }
                }
            }
        })
        .collect();

    let mut disks = HashMap::new();
    let mut bay_of_disk: HashMap<DiskIdentity, String> = HashMap::new();
    for carried_pass in [false, true] {
        for (bay, disk, carried) in &mut claims {
            if *carried != carried_pass {
                continue;
            }
            let Some(disk) = disk.take() else {
                continue;
            };
            let identity = disk.identity().clone();
            if let Some(winner) = bay_of_disk.get(&identity) {
                warn!(
                    log,
                    "disk appears in more than one bay";
                    "identity" => ?identity,
                    "kept" => winner,
                    "ignored" => &bay.location,
                );
                bay.occupant =
                    DiskBayOccupant::Device { driver: None, devfs_path: None };
            } else {
                bay_of_disk.insert(identity.clone(), bay.location.clone());
                disks.insert(identity, disk);
            }
        }
    }
    let bays = claims.into_iter().map(|(bay, _, _)| bay).collect();
    (disks, bays)
}

/// The model string of a disk, from the `inquiry-vendor-id` and
/// `inquiry-product-id` strings the `blkdev` driver publishes.
///
/// The model is generally the vendor id and the product id separated by a
/// space. The kernel substitutes a placeholder vendor id for NVMe devices,
/// in which case only the product id is used.
pub(crate) fn disk_model(
    inquiry_vendor_id: &str,
    inquiry_product_id: &str,
) -> String {
    match inquiry_vendor_id {
        "" | "NVMe" => inquiry_product_id.to_string(),
        _ => format!("{inquiry_vendor_id} {inquiry_product_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn firmware() -> DiskFirmware {
        DiskFirmware::new(1, None, true, 1, vec![Some("FW1".to_string())])
    }

    fn facts(usable_namespaces: usize) -> NvmeFacts {
        NvmeFacts { pci_vid: 0x1344, usable_namespaces, firmware: firmware() }
    }

    fn instance() -> NvmeInstance {
        NvmeInstance::from_topo(4).unwrap()
    }

    fn namespace() -> ObservedNamespace {
        ObservedNamespace {
            manufacturer: "NVMe".to_string(),
            model: "Micron_7450_MTFDKCC3T2TFS".to_string(),
            serial: "2345ABCD".to_string(),
            devfs_path: "/pci@0,0/pci1de,fff9@1,1/pci1344,3100@0/blkdev@w0000000000000000,0".to_string(),
            logical_disk: Some("c1t00A0750130D5B4C7d0".to_string()),
        }
    }

    fn bay(occupant: ObservedOccupant) -> ObservedBay {
        ObservedBay {
            location: "N3".to_string(),
            kind: DiskVariant::U2,
            binding_driver: None,
            contents: Ok(BayContents { pcie_slot: 3, occupant }),
        }
    }

    fn unreadable_bay(problem: BayProblem) -> ObservedBay {
        ObservedBay {
            location: "N3".to_string(),
            kind: DiskVariant::U2,
            binding_driver: None,
            contents: Err(problem),
        }
    }

    fn nvme_bay(namespaces: Vec<ObservedNamespace>) -> ObservedBay {
        bay(ObservedOccupant::Nvme {
            instance: instance(),
            driver: Some("nvme".to_string()),
            devfs_path: Some(
                "/pci@0,0/pci1de,fff9@1,1/pci1344,3100@0".to_string(),
            ),
            namespaces,
        })
    }

    #[test]
    fn empty_bay() {
        let (described, disk) =
            classify_bay(&bay(ObservedOccupant::Empty), None, M2Slot::A)
                .unwrap();
        assert_eq!(described.location, "N3");
        assert_eq!(described.kind, DiskVariant::U2);
        assert_eq!(described.occupant, DiskBayOccupant::Empty);
        assert!(disk.is_none());
    }

    #[test]
    fn unrecognized_device() {
        let observed = bay(ObservedOccupant::Other {
            driver: Some("pciex8086,1234".to_string()),
            devfs_path: Some(
                "/pci@0,0/pci1de,fff9@1,1/pci8086,1234@0".to_string(),
            ),
        });
        let (described, disk) =
            classify_bay(&observed, None, M2Slot::A).unwrap();
        assert_eq!(
            described.occupant,
            DiskBayOccupant::Device {
                driver: Some("pciex8086,1234".to_string()),
                devfs_path: Some(
                    "/pci@0,0/pci1de,fff9@1,1/pci8086,1234@0".to_string()
                ),
            }
        );
        assert!(disk.is_none());
    }

    #[test]
    fn nvme_disk_takes_kind_and_label_from_bay() {
        let mut observed = nvme_bay(vec![namespace()]);
        observed.location = "M.2 West".to_string();
        observed.kind = DiskVariant::M2;
        observed.contents.as_mut().unwrap().pcie_slot = 18;

        let (described, disk) =
            classify_bay(&observed, Some(&facts(1)), M2Slot::B).unwrap();
        let disk = disk.expect("an NVMe disk with a namespace is a disk");

        let identity = DiskIdentity {
            vendor: "1344".to_string(),
            serial: "2345ABCD".to_string(),
            model: "Micron_7450_MTFDKCC3T2TFS".to_string(),
        };
        assert_eq!(
            described.occupant,
            DiskBayOccupant::Disk { identity: identity.clone() }
        );
        assert_eq!(disk.identity(), &identity);
        assert_eq!(disk.variant(), DiskVariant::M2);
        assert_eq!(disk.location(), "M.2 West");
        assert_eq!(disk.pcie_slot(), 18);
        assert!(disk.is_boot_disk());
        assert_eq!(disk.firmware(), &firmware());
        assert_eq!(
            disk.devfs_path().as_str(),
            "/devices/pci@0,0/pci1de,fff9@1,1/pci1344,3100@0/blkdev@w0000000000000000,0"
        );
        assert_eq!(
            disk.paths().dev_path.as_ref().map(|p| p.as_str()),
            Some("/dev/dsk/c1t00A0750130D5B4C7d0")
        );
    }

    #[test]
    fn boot_disk_is_the_m2_labelled_for_the_boot_storage_unit() {
        let classify = |location: &str, kind, boot_storage_unit| {
            let mut observed = nvme_bay(vec![namespace()]);
            observed.location = location.to_string();
            observed.kind = kind;
            let (_, disk) =
                classify_bay(&observed, Some(&facts(1)), boot_storage_unit)
                    .unwrap();
            disk.unwrap().is_boot_disk()
        };

        assert!(classify("M.2 East", DiskVariant::M2, M2Slot::A));
        assert!(!classify("M.2 East", DiskVariant::M2, M2Slot::B));
        assert!(classify("M.2 West", DiskVariant::M2, M2Slot::B));
        assert!(!classify("M.2 West", DiskVariant::M2, M2Slot::A));
        assert!(!classify("N0", DiskVariant::U2, M2Slot::A));
        // Only an M.2 socket can hold the boot disk, whatever its label.
        assert!(!classify("M.2 East", DiskVariant::U2, M2Slot::A));
    }

    #[test]
    fn model_keeps_a_real_vendor_string() {
        let mut ns = namespace();
        ns.manufacturer = "Samsung".to_string();
        ns.model = "MZWLR3T8HBLS-00007".to_string();
        let (_, disk) =
            classify_bay(&nvme_bay(vec![ns]), Some(&facts(1)), M2Slot::A)
                .unwrap();
        assert_eq!(
            disk.unwrap().identity().model,
            "Samsung MZWLR3T8HBLS-00007"
        );
        assert_eq!(disk_model("", "X"), "X");
        assert_eq!(disk_model("NVMe", "X"), "X");
        assert_eq!(disk_model("V", "X"), "V X");
    }

    #[test]
    fn controller_without_namespace_is_a_device() {
        let (described, disk) =
            classify_bay(&nvme_bay(vec![]), Some(&facts(0)), M2Slot::A)
                .unwrap();
        assert_eq!(
            described.occupant,
            DiskBayOccupant::Device {
                driver: Some("nvme".to_string()),
                devfs_path: Some(
                    "/pci@0,0/pci1de,fff9@1,1/pci1344,3100@0".to_string()
                ),
            }
        );
        assert!(disk.is_none());
    }

    #[test]
    fn namespace_missing_from_topology_is_an_error() {
        let err = classify_bay(&nvme_bay(vec![]), Some(&facts(1)), M2Slot::A)
            .unwrap_err();
        assert!(
            matches!(
                err,
                ClassifyError::MissingNamespace { usable_namespaces: 1, .. }
            ),
            "{err}"
        );
    }

    #[test]
    fn nvme_needs_libnvme_facts() {
        let err = classify_bay(&nvme_bay(vec![namespace()]), None, M2Slot::A)
            .unwrap_err();
        assert!(matches!(err, ClassifyError::MissingNvmeFacts { .. }), "{err}");
    }

    #[test]
    fn first_namespace_wins() {
        let mut second = namespace();
        second.serial = "OTHER".to_string();
        let (_, disk) = classify_bay(
            &nvme_bay(vec![namespace(), second]),
            Some(&facts(2)),
            M2Slot::A,
        )
        .unwrap();
        assert_eq!(disk.unwrap().identity().serial, "2345ABCD");
    }

    #[test]
    fn relative_devfs_path_is_rejected() {
        let mut ns = namespace();
        ns.devfs_path = "pci@0,0/oops".to_string();
        let err = classify_bay(&nvme_bay(vec![ns]), Some(&facts(1)), M2Slot::A)
            .unwrap_err();
        assert!(
            matches!(err, ClassifyError::RelativeDevfsPath { .. }),
            "{err}"
        );
    }

    #[test]
    fn missing_logical_disk_leaves_dev_path_unset() {
        let mut ns = namespace();
        ns.logical_disk = None;
        let (_, disk) =
            classify_bay(&nvme_bay(vec![ns]), Some(&facts(1)), M2Slot::A)
                .unwrap();
        assert_eq!(disk.unwrap().paths().dev_path, None);
    }

    #[test]
    fn unreadable_bay_is_an_error() {
        let problems = [
            BayProblem::NvmeNotEnumerated,
            BayProblem::NoPcieSlot,
            BayProblem::Topology {
                node: "chassis[0]/bay[3]/nvme[0]".to_string(),
                error: "libtopo: out of memory".to_string(),
            },
        ];
        for problem in problems {
            let observed = unreadable_bay(problem.clone());
            let err = classify_bay(&observed, None, M2Slot::A).unwrap_err();
            assert!(
                matches!(
                    &err,
                    ClassifyError::Unreadable { location, problem: p }
                        if location == "N3" && *p == problem
                ),
                "{err}"
            );
        }
    }

    fn log() -> Logger {
        Logger::root(slog::Discard, slog::o!())
    }

    fn described(
        observed: &ObservedBay,
        facts: Option<&NvmeFacts>,
    ) -> PolledBay {
        let (bay, disk) = classify_bay(observed, facts, M2Slot::A).unwrap();
        PolledBay::Described { bay, disk: disk.map(Box::new) }
    }

    fn unreadable(location: &str) -> PolledBay {
        PolledBay::Unreadable {
            location: location.to_string(),
            kind: DiskVariant::U2,
            driver: Some("nvme".to_string()),
        }
    }

    #[test]
    fn unreadable_bay_keeps_previous_disk() {
        let (previous_disks, previous_bays) = resolve_bays(
            &log(),
            vec![described(&nvme_bay(vec![namespace()]), Some(&facts(1)))],
            &[],
            &HashMap::new(),
        );
        assert_eq!(previous_disks.len(), 1);

        let (disks, bays) = resolve_bays(
            &log(),
            vec![unreadable("N3")],
            &previous_bays,
            &previous_disks,
        );
        assert_eq!(disks, previous_disks);
        assert_eq!(bays, previous_bays);
    }

    #[test]
    fn unreadable_bay_without_history_is_a_device() {
        let (disks, bays) = resolve_bays(
            &log(),
            vec![
                described(&bay(ObservedOccupant::Empty), None),
                unreadable("N4"),
            ],
            &[],
            &HashMap::new(),
        );
        assert!(disks.is_empty());
        assert_eq!(bays.len(), 2);
        assert_eq!(bays[0].occupant, DiskBayOccupant::Empty);
        assert_eq!(bays[1].location, "N4");
        assert_eq!(
            bays[1].occupant,
            DiskBayOccupant::Device {
                driver: Some("nvme".to_string()),
                devfs_path: None,
            }
        );
    }

    #[test]
    fn fresh_disk_beats_carried_over_disk() {
        // The disk was in N3; it has moved to N4, and N3 now holds something
        // unreadable that is walked first.
        let (previous_disks, previous_bays) = resolve_bays(
            &log(),
            vec![described(&nvme_bay(vec![namespace()]), Some(&facts(1)))],
            &[],
            &HashMap::new(),
        );
        let mut moved = nvme_bay(vec![namespace()]);
        moved.location = "N4".to_string();

        let (disks, bays) = resolve_bays(
            &log(),
            vec![unreadable("N3"), described(&moved, Some(&facts(1)))],
            &previous_bays,
            &previous_disks,
        );
        assert_eq!(disks.len(), 1);
        assert_eq!(disks.values().next().unwrap().location(), "N4");
        assert_eq!(
            bays[0].occupant,
            DiskBayOccupant::Device { driver: None, devfs_path: None }
        );
        assert!(matches!(bays[1].occupant, DiskBayOccupant::Disk { .. }));
    }

    #[test]
    fn duplicate_disk_identity_keeps_first_bay() {
        let first = nvme_bay(vec![namespace()]);
        let mut second = nvme_bay(vec![namespace()]);
        second.location = "N4".to_string();
        let (disks, bays) = resolve_bays(
            &log(),
            vec![
                described(&first, Some(&facts(1))),
                described(&second, Some(&facts(1))),
            ],
            &[],
            &HashMap::new(),
        );
        assert_eq!(disks.len(), 1);
        assert_eq!(disks.values().next().unwrap().location(), "N3");
        assert!(matches!(bays[0].occupant, DiskBayOccupant::Disk { .. }));
        assert_eq!(
            bays[1].occupant,
            DiskBayOccupant::Device { driver: None, devfs_path: None }
        );
    }
}
