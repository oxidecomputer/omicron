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
//! the illumos module; the join between them, [`classify_bay`], is plain data
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
    /// NVMe controller with no active namespace, or something that is not
    /// NVMe at all.
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
    /// The PCIe physical slot number of the bridge behind the bay
    /// (`binding/slot`).
    pub pcie_slot: i64,
    /// `binding/driver` on the bay node: the driver the topology found bound
    /// to the device behind the bay. oxhc records it before it tries to
    /// enumerate the device, so it is present even when enumeration failed.
    pub binding_driver: Option<String>,
    pub occupant: ObservedOccupant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ObservedOccupant {
    /// The bay node has no children.
    Empty,
    /// An `nvme` node with a driver instance. `namespaces` has one entry per
    /// `disk` child, which the topology creates per active namespace.
    Nvme {
        instance: NvmeInstance,
        driver: Option<String>,
        devfs_path: Option<String>,
        namespaces: Vec<ObservedNamespace>,
    },
    /// The topology's placeholder for a device it does not enumerate, or an
    /// `nvme` node it left without a driver instance.
    Other { driver: Option<String>, devfs_path: Option<String> },
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
    pub active_namespaces: usize,
    pub firmware: DiskFirmware,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClassifyError {
    #[error(
        "{location} ({nvme_instance}) has {active_namespaces} active \
         namespace(s) but the hardware topology describes none"
    )]
    MissingNamespace {
        location: String,
        nvme_instance: NvmeInstance,
        active_namespaces: usize,
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
/// `is_boot_disk` is the caller's answer for this bay's PCIe slot.
pub(crate) fn classify_bay(
    bay: &ObservedBay,
    nvme: Option<&NvmeFacts>,
    is_boot_disk: bool,
) -> Result<(DiskBay, Option<UnparsedDisk>), ClassifyError> {
    let describe = |occupant| DiskBay {
        location: bay.location.clone(),
        kind: bay.kind,
        occupant,
    };

    let (instance, driver, devfs_path, namespaces) = match &bay.occupant {
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

    // The topology creates one `disk` node per active namespace. If it has
    // none but the controller says it has some, the topology's namespace
    // enumeration failed silently and this snapshot cannot be trusted.
    let Some(namespace) = namespaces.first() else {
        if nvme.active_namespaces == 0 {
            let device = DiskBayOccupant::Device {
                driver: driver.clone(),
                devfs_path: devfs_path.clone(),
            };
            return Ok((describe(device), None));
        }
        return Err(ClassifyError::MissingNamespace {
            location: bay.location.clone(),
            nvme_instance: instance,
            active_namespaces: nvme.active_namespaces,
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

    let disk = UnparsedDisk::new(
        devfs_path,
        dev_path,
        bay.pcie_slot,
        bay.kind,
        identity.clone(),
        is_boot_disk,
        nvme.firmware.clone(),
        bay.location.clone(),
    );
    Ok((describe(DiskBayOccupant::Disk { identity }), Some(disk)))
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

    fn facts(active_namespaces: usize) -> NvmeFacts {
        NvmeFacts { pci_vid: 0x1344, active_namespaces, firmware: firmware() }
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
            pcie_slot: 3,
            binding_driver: None,
            occupant,
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
            classify_bay(&bay(ObservedOccupant::Empty), None, false).unwrap();
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
        let (described, disk) = classify_bay(&observed, None, false).unwrap();
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
        observed.pcie_slot = 18;

        let (described, disk) =
            classify_bay(&observed, Some(&facts(1)), true).unwrap();
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
    fn model_keeps_a_real_vendor_string() {
        let mut ns = namespace();
        ns.manufacturer = "Samsung".to_string();
        ns.model = "MZWLR3T8HBLS-00007".to_string();
        let (_, disk) =
            classify_bay(&nvme_bay(vec![ns]), Some(&facts(1)), false).unwrap();
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
            classify_bay(&nvme_bay(vec![]), Some(&facts(0)), false).unwrap();
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
        let err = classify_bay(&nvme_bay(vec![]), Some(&facts(1)), false)
            .unwrap_err();
        assert!(
            matches!(
                err,
                ClassifyError::MissingNamespace { active_namespaces: 1, .. }
            ),
            "{err}"
        );
    }

    #[test]
    fn nvme_needs_libnvme_facts() {
        let err = classify_bay(&nvme_bay(vec![namespace()]), None, false)
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
            false,
        )
        .unwrap();
        assert_eq!(disk.unwrap().identity().serial, "2345ABCD");
    }

    #[test]
    fn relative_devfs_path_is_rejected() {
        let mut ns = namespace();
        ns.devfs_path = "pci@0,0/oops".to_string();
        let err = classify_bay(&nvme_bay(vec![ns]), Some(&facts(1)), false)
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
            classify_bay(&nvme_bay(vec![ns]), Some(&facts(1)), false).unwrap();
        assert_eq!(disk.unwrap().paths().dev_path, None);
    }
}
