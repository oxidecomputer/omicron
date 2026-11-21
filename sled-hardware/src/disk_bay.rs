// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The U.2 bays and M.2 sockets of a sled's chassis, and what occupies each.
//!
//! On an Oxide sled, the hardware topology (libtopo) is the source of truth
//! for which bays exist, and for what is in each bay.
//!
//! Each hardware poll becomes the reported bays in two steps:
//!
//! 1. The topology is read into `ObservedBay`s, and each one is described,
//!    using what libnvme says about its controller if it holds one, giving a
//!    `PolledBay`. A bay that cannot be described is `PolledBay::Unreadable`.
//!    The goal for this step is to describe "what can we see from the
//!    hardware, right now?"
//! 2. `resolve_bays` merges the poll with the previous view. A disk stays
//!    in the view until a poll reads its bay and finds it gone.
//!    The goal for this step is a stable view of the world: difficulties
//!    scanning topology/NVMe information shouldn't cause flapping (e.g., a
//!    failure to read a disk doesn't definitively mean a disk no longer
//!    exists).

// The observed types below are only produced on illumos.
#![cfg_attr(not(target_os = "illumos"), allow(dead_code))]

use crate::DiskFirmware;
use crate::DiskHealthMap;
use crate::DiskLocation;
use crate::NvmeHealthLogPage;
use crate::UnparsedDisk;
use crate::nvme_instance::NvmeInstance;
use camino::Utf8PathBuf;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use serde::{Deserialize, Serialize};
use sled_agent_types::disk::DiskIdentity;
use sled_agent_types::disk::DiskVariant;
use sled_agent_types::inventory::NvmeHealthError;
use slog::{Logger, warn};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// One U.2 bay or M.2 socket of the chassis, as the hardware topology
/// describes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskBay {
    pub location: DiskLocation,
    /// Whether this is a U.2 bay or an M.2 socket.
    pub kind: DiskVariant,
    pub occupant: DiskBayOccupant,
}

impl IdOrdItem for DiskBay {
    type Key<'a> = &'a DiskLocation;

    fn key(&self) -> Self::Key<'_> {
        &self.location
    }

    id_upcast!();
}

/// The contents of a disk bay, as reported by the hardware topology and
/// libnvme.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiskBayOccupant {
    /// The bay is empty.
    ///
    /// A drive whose PCIe link is down also looks like this. The hardware
    /// topology finds a bay's occupant through the device node under its
    /// PCIe bridge, and has no separate presence detection.
    Empty,
    /// A usable NVMe disk.
    Disk { identity: DiskIdentity },
    /// A device is present, but it cannot be used as a disk.
    UnusableDevice {
        reason: UnusableReason,
        /// The device's devfs path, relative to `/devices`, if the topology
        /// recorded one.
        devfs_path: Option<String>,
    },
    /// The topology or libnvme could not describe the bay on the latest
    /// poll.
    Unreadable {
        /// The driver the topology found bound to the device, if any.
        driver: Option<String>,
        error: String,
    },
}

/// Why sled-agent does not use the device in a bay as a disk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UnusableReason {
    /// The topology records no attached driver for the device: either no
    /// driver is bound to it, or the bound driver has not attached.
    NoDriver,
    /// A driver other than `nvme` is attached to the device.
    NotNvme { driver: String },
    /// An NVMe controller with no namespace the driver can use.
    NoUsableNamespace,
    /// An NVMe controller with more than one namespace the driver can use.
    /// sled-agent manages a drive as one disk, not per namespace.
    MultipleNamespaces { count: usize },
    /// The disk here has the same identity as the disk in another bay, which
    /// is the one in the sled's disk list.
    DuplicateIdentity { identity: DiskIdentity, kept_in: DiskLocation },
}

/// One bay as the hardware topology reports it, before sled-hardware has
/// asked the NVMe controller anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObservedBay {
    pub location: DiskLocation,
    pub kind: DiskVariant,
    /// The `driver` property in the `binding` property group of the bay's
    /// topology node.
    ///
    /// oxhc, the Oxide hardware chassis enumerator in illumos, sets this to
    /// `nvme` when the device in the bay is bound to that driver. It does so
    /// before enumerating the device, so the value is present even if
    /// enumeration fails. It is not set for other devices.
    pub binding_driver: Option<String>,
    /// What is in the bay, or why the topology could not say.
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
    /// An `nvme` node with a driver instance.
    Nvme {
        instance: NvmeInstance,
        driver: Option<String>,
        devfs_path: Option<String>,
        /// The usable NVMe namespaces reported by the topology for this
        /// device.
        ///
        /// These are reported as `disk` nodes nested under the `nvme` node.
        /// Namespaces ignored by the nvme driver are not included in this
        /// list.
        namespaces: Vec<ObservedNamespace>,
    },
    /// A device that is not an NVMe controller.
    Other { driver: Option<String>, devfs_path: Option<String> },
}

/// Why the topology could not describe one bay.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BayProblem {
    #[error("the topology reports more than one occupant")]
    MultipleOccupants,

    #[error("topology disk node {node} has no {property}")]
    IncompleteDisk { node: String, property: String },

    /// The bay's `binding/driver` is `nvme`, but it has no `nvme` node.
    ///
    /// The topology creates that node only after it opens the controller
    /// and reads its identity, so this means the device is bound to the
    /// nvme driver but could not be opened or queried.
    #[error("the topology found an NVMe device but did not enumerate it")]
    NvmeNotEnumerated,

    #[error("the topology's nvme node has no usable driver instance")]
    NvmeWithoutInstance,

    #[error("the topology has no PCIe slot (binding/slot) for the bay")]
    NoPcieSlot,

    #[error("could not read topology node {node}: {error}")]
    Topology { node: String, error: String },
}

/// A `disk` node under an `nvme` node: one NVMe namespace.
///
/// The three identity strings reported by the topology are copied from
/// the `blkdev` device node's `inquiry-*` properties. Therefore, they are
/// identical to the identity reported by devinfo for this NVMe namespace.
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
    /// `storage/logical-disk`: the name under `/dev/dsk` (such as
    /// `c1t00A0750130D5B4C7d0`), if the topology found the link.
    pub logical_disk: Option<String>,
}

/// What libnvme reports for the controller in a bay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NvmeFacts {
    pub pci_vid: u16,
    /// The number of namespaces considered usable by the nvme driver.
    ///
    /// This count ignores any namespaces to which the nvme driver cannot
    /// attach a `blkdev` node, such as those formatted with protection
    /// information. Any such namespaces are not included in the hardware
    /// topology.
    pub usable_namespaces: usize,
    pub firmware: DiskFirmware,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DescribeError {
    #[error(transparent)]
    Unreadable { problem: BayProblem },

    #[error(
        "libnvme reports {usable_namespaces} usable namespace(s) on \
         {nvme_instance}, but the hardware topology describes none"
    )]
    MissingNamespace { nvme_instance: NvmeInstance, usable_namespaces: usize },

    #[error("topology devfs path {path:?} is not absolute")]
    RelativeDevfsPath { path: String },
}

/// What the topology alone can say about a bay.
#[derive(Debug)]
pub(crate) enum BayDescription<'a> {
    /// The bay is empty, or holds a device that is not an NVMe controller.
    Complete(DiskBay),
    /// The bay holds an NVMe controller, which needs libnvme's facts to
    /// finish describing.
    NeedsNvmeFacts(ObservedNvmeBay<'a>),
}

/// A bay the topology found an NVMe controller in, before libnvme has been
/// asked about that controller.
#[derive(Debug)]
pub(crate) struct ObservedNvmeBay<'a> {
    bay: &'a ObservedBay,
    pcie_slot: i64,
    instance: NvmeInstance,
    devfs_path: &'a Option<String>,
    namespaces: &'a [ObservedNamespace],
}

/// Describes a bay from what the topology says about it.
///
/// A bay holding an NVMe controller comes back as
/// [`BayDescription::NeedsNvmeFacts`], to be finished with
/// [`ObservedNvmeBay::combine_with_nvme_facts`].
pub(crate) fn describe_bay_from_topology(
    bay: &ObservedBay,
) -> Result<BayDescription<'_>, DescribeError> {
    let contents = match &bay.contents {
        Ok(contents) => contents,
        Err(problem) => {
            return Err(DescribeError::Unreadable { problem: problem.clone() });
        }
    };

    let occupant = match &contents.occupant {
        ObservedOccupant::Empty => DiskBayOccupant::Empty,
        ObservedOccupant::Other { driver, devfs_path } => {
            let reason = match driver {
                Some(driver) => {
                    UnusableReason::NotNvme { driver: driver.clone() }
                }
                None => UnusableReason::NoDriver,
            };
            DiskBayOccupant::UnusableDevice {
                reason,
                devfs_path: devfs_path.clone(),
            }
        }
        ObservedOccupant::Nvme { instance, devfs_path, namespaces, .. } => {
            return Ok(BayDescription::NeedsNvmeFacts(ObservedNvmeBay {
                bay,
                pcie_slot: contents.pcie_slot,
                instance: *instance,
                devfs_path,
                namespaces,
            }));
        }
    };
    Ok(BayDescription::Complete(DiskBay {
        location: bay.location.clone(),
        kind: bay.kind,
        occupant,
    }))
}

impl ObservedNvmeBay<'_> {
    pub(crate) fn instance(&self) -> NvmeInstance {
        self.instance
    }

    /// The PCIe physical slot number of the bridge behind the bay.
    pub(crate) fn pcie_slot(&self) -> i64 {
        self.pcie_slot
    }

    /// Combines the controller with what libnvme said about it.
    ///
    /// Returns the bay and, if the controller has exactly one usable
    /// namespace, the disk in it. A controller with any other number of
    /// namespaces is a [`DiskBayOccupant::UnusableDevice`] with no disk.
    ///
    /// `is_boot_disk` is the caller's answer for this bay's PCIe slot.
    pub(crate) fn combine_with_nvme_facts(
        self,
        nvme_facts: &NvmeFacts,
        is_boot_disk: bool,
    ) -> Result<(DiskBay, Option<UnparsedDisk>), DescribeError> {
        let ObservedNvmeBay {
            bay,
            pcie_slot,
            instance,
            devfs_path,
            namespaces,
        } = self;
        let describe = |occupant| DiskBay {
            location: bay.location.clone(),
            kind: bay.kind,
            occupant,
        };

        // The topology creates one `disk` node per usable namespace. If it
        // has none but the controller says it has some, the topology's
        // namespace enumeration failed silently.
        let namespace = match namespaces {
            [] if nvme_facts.usable_namespaces == 0 => {
                let device = DiskBayOccupant::UnusableDevice {
                    reason: UnusableReason::NoUsableNamespace,
                    devfs_path: devfs_path.clone(),
                };
                return Ok((describe(device), None));
            }
            [] => {
                return Err(DescribeError::MissingNamespace {
                    nvme_instance: instance,
                    usable_namespaces: nvme_facts.usable_namespaces,
                });
            }
            [namespace] => namespace,
            // Every namespace on a controller shares its DiskIdentity. See
            // https://github.com/oxidecomputer/omicron/issues/5241.
            _ => {
                let device = DiskBayOccupant::UnusableDevice {
                    reason: UnusableReason::MultipleNamespaces {
                        count: namespaces.len(),
                    },
                    devfs_path: devfs_path.clone(),
                };
                return Ok((describe(device), None));
            }
        };

        // libdevinfo reports this path as absolute from the kernel's point
        // of view; userspace sees it under "/devices".
        if !namespace.devfs_path.starts_with('/') {
            return Err(DescribeError::RelativeDevfsPath {
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
            vendor: format!("{:x}", nvme_facts.pci_vid),
            serial: namespace.serial.clone(),
            model: disk_model(&namespace.manufacturer, &namespace.model),
        };

        let disk = UnparsedDisk::new(
            devfs_path,
            dev_path,
            pcie_slot,
            bay.kind,
            identity.clone(),
            is_boot_disk,
            nvme_facts.firmware.clone(),
            bay.location.clone(),
        );
        Ok((describe(DiskBayOccupant::Disk { identity }), Some(disk)))
    }
}

/// A disk found in a bay by a single hardware poll.
#[derive(Clone, Debug)]
pub(crate) struct PolledDisk {
    pub disk: UnparsedDisk,
    /// The disk's health, or why it could not be read.
    pub health: Result<NvmeHealthLogPage, NvmeHealthError>,
}

/// One bay from a single hardware poll.
#[derive(Clone, Debug)]
pub(crate) enum PolledBay {
    Described {
        bay: DiskBay,
        disk: Option<Box<PolledDisk>>,
    },
    /// The topology or libnvme could not describe the bay on this poll.
    Unreadable {
        location: DiskLocation,
        kind: DiskVariant,
        driver: Option<String>,
        error: String,
    },
}

impl PolledBay {
    fn location(&self) -> &DiskLocation {
        match self {
            PolledBay::Described { bay, .. } => &bay.location,
            PolledBay::Unreadable { location, .. } => location,
        }
    }
}

impl IdOrdItem for PolledBay {
    type Key<'a> = &'a DiskLocation;

    fn key(&self) -> Self::Key<'_> {
        self.location()
    }

    id_upcast!();
}

/// Collects one poll's bays by location. If more than one bay has the same
/// location, the first is kept.
pub(crate) fn collect_polled_bays(
    log: &Logger,
    bays: impl IntoIterator<Item = PolledBay>,
) -> IdOrdMap<PolledBay> {
    let mut polled = IdOrdMap::new();
    for bay in bays {
        if let Err(duplicate) = polled.insert_unique(bay) {
            warn!(
                log,
                "more than one bay has the same location; keeping the first";
                "location" => %duplicate.new_item().location(),
            );
        }
    }
    polled
}

/// Combines one poll's bays, or the error that kept the poll from reading
/// any, with the previous view into the disks and bays to report.
///
/// A bay this poll could not describe is reported as unreadable.
/// The disk previously seen at its location stays in the disk list, so a fault
/// in one bay neither drops its disk nor holds up the others.
///
/// Health is reported only for a disk this poll read, from the bay that
/// keeps the disk. It is never carried over from the previous view.
///
/// If two bays have a disk with the same identity, a bay that saw the disk on
/// this poll keeps it over a bay whose disk is from the previous poll (the
/// disk may have moved). Otherwise the bay whose location sorts first keeps
/// it.
pub(crate) fn resolve_bays(
    log: &Logger,
    polled: Result<IdOrdMap<PolledBay>, String>,
    previous_bays: &IdOrdMap<DiskBay>,
    previous_disks: &HashMap<DiskIdentity, UnparsedDisk>,
) -> (HashMap<DiskIdentity, UnparsedDisk>, IdOrdMap<DiskBay>, DiskHealthMap) {
    let (mut polled, missing_error) = match polled {
        Ok(polled) => (polled, "not in the hardware topology".to_string()),
        Err(error) => {
            (IdOrdMap::new(), format!("could not read the disk bays: {error}"))
        }
    };
    // A bay in the previous view that this poll did not report is treated
    // as unreadable, so that a disk last seen in that bay is not dropped.
    for previous in previous_bays {
        if !polled.contains_key(&previous.location) {
            polled.insert_overwrite(PolledBay::Unreadable {
                location: previous.location.clone(),
                kind: previous.kind,
                driver: None,
                error: missing_error.clone(),
            });
        }
    }

    // A bay and the disk contained within it.
    struct ResolvedBay {
        bay: DiskBay,
        disk: Option<UnparsedDisk>,
        // Only for a disk read on this poll.
        health: Option<Result<NvmeHealthLogPage, NvmeHealthError>>,
        disk_source: DiskSource,
    }

    // Where the disk attributed to a `ResolvedBay` comes from.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum DiskSource {
        // The bay was read on this poll, and held this disk.
        ThisPoll,
        // The bay was unreadable on this poll.
        // We're using the result from a prior poll which was successful.
        PreviousPoll,
    }

    // For all bays we have seen, in this poll or a previous one:
    // - If the bay was read on this poll, use what it held (a disk or none)
    // - If it was unreadable, use the disk it held previously, if one exists
    let mut resolved: Vec<ResolvedBay> = polled
        .into_iter()
        .map(|polled_bay| match polled_bay {
            PolledBay::Described { bay, disk } => {
                let (disk, health) = match disk.map(|d| *d) {
                    Some(PolledDisk { disk, health }) => {
                        (Some(disk), Some(health))
                    }
                    None => (None, None),
                };
                ResolvedBay {
                    bay,
                    disk,
                    health,
                    disk_source: DiskSource::ThisPoll,
                }
            }
            PolledBay::Unreadable { location, kind, driver, error } => {
                let disk = previous_disks
                    .values()
                    .find(|d| d.location() == &location)
                    .cloned();
                let occupant = DiskBayOccupant::Unreadable { driver, error };
                ResolvedBay {
                    bay: DiskBay { location, kind, occupant },
                    disk,
                    health: None,
                    disk_source: DiskSource::PreviousPoll,
                }
            }
        })
        .collect();

    // Make sure each disk ends up in exactly one bay. If several bays have a
    // disk with the same identity, the bay that keeps it is:
    // 1. A bay that read the disk on this poll, over a bay using a disk from
    //    a previous poll (the disk may have moved)
    // 2. Then, the bay whose location sorts first
    //
    // This sort puts the bays in that order: it is stable, and `polled` was
    // ordered by location. So the first bay visited with a disk keeps it.
    resolved.sort_by_key(|bay| match bay.disk_source {
        DiskSource::ThisPoll => 0,
        DiskSource::PreviousPoll => 1,
    });

    let mut disks = HashMap::new();
    let mut bays = IdOrdMap::new();
    let mut disk_health = DiskHealthMap::new();
    for ResolvedBay { mut bay, disk, health, disk_source } in resolved {
        if let Some(disk) = disk {
            match disks.entry(disk.identity().clone()) {
                Entry::Vacant(entry) => {
                    if let Some(health) = health {
                        disk_health.insert(entry.key().clone(), health);
                    }
                    entry.insert(disk);
                }
                Entry::Occupied(entry) => {
                    let kept_in = entry.get().location().clone();
                    warn!(
                        log,
                        "disk appears in more than one bay";
                        "identity" => ?entry.key(),
                        "kept" => %kept_in,
                        "ignored" => %bay.location,
                    );
                    // This bay does not keep the disk. If it read the disk
                    // on this poll, report the duplicate; an unreadable bay
                    // stays unreadable.
                    if disk_source == DiskSource::ThisPoll {
                        bay.occupant = DiskBayOccupant::UnusableDevice {
                            reason: UnusableReason::DuplicateIdentity {
                                identity: entry.key().clone(),
                                kept_in,
                            },
                            devfs_path: None,
                        };
                    }
                }
            }
        }
        // `polled` had one bay per location, so no bay overwrites another.
        bays.insert_overwrite(bay);
    }
    (disks, bays, disk_health)
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
    use sled_agent_types::inventory::NvmeHealthErrorKind;

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
            location: loc("N3"),
            kind: DiskVariant::U2,
            binding_driver: None,
            contents: Ok(BayContents { pcie_slot: 3, occupant }),
        }
    }

    fn unreadable_bay(problem: BayProblem) -> ObservedBay {
        ObservedBay {
            location: loc("N3"),
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

    /// Describes a bay that needs nothing from libnvme.
    fn describe_without_nvme(observed: &ObservedBay) -> DiskBay {
        match describe_bay_from_topology(observed).unwrap() {
            BayDescription::Complete(bay) => bay,
            BayDescription::NeedsNvmeFacts(nvme) => {
                panic!("expected no NVMe controller, got {nvme:?}")
            }
        }
    }

    /// Describes a bay holding an NVMe controller, given libnvme's facts.
    fn describe_nvme(
        observed: &ObservedBay,
        facts: NvmeFacts,
        is_boot_disk: bool,
    ) -> Result<(DiskBay, Option<UnparsedDisk>), DescribeError> {
        match describe_bay_from_topology(observed).unwrap() {
            BayDescription::NeedsNvmeFacts(nvme) => {
                nvme.combine_with_nvme_facts(&facts, is_boot_disk)
            }
            BayDescription::Complete(bay) => {
                panic!("expected an NVMe controller, got {bay:?}")
            }
        }
    }

    #[test]
    fn empty_bay() {
        let described = describe_without_nvme(&bay(ObservedOccupant::Empty));
        assert_eq!(described.location.as_str(), "N3");
        assert_eq!(described.kind, DiskVariant::U2);
        assert_eq!(described.occupant, DiskBayOccupant::Empty);
    }

    #[test]
    fn unrecognized_device() {
        let observed = bay(ObservedOccupant::Other {
            driver: Some("pciex8086,1234".to_string()),
            devfs_path: Some(
                "/pci@0,0/pci1de,fff9@1,1/pci8086,1234@0".to_string(),
            ),
        });
        let described = describe_without_nvme(&observed);
        assert_eq!(
            described.occupant,
            DiskBayOccupant::UnusableDevice {
                reason: UnusableReason::NotNvme {
                    driver: "pciex8086,1234".to_string()
                },
                devfs_path: Some(
                    "/pci@0,0/pci1de,fff9@1,1/pci8086,1234@0".to_string()
                ),
            }
        );
    }

    #[test]
    fn device_without_driver() {
        let observed =
            bay(ObservedOccupant::Other { driver: None, devfs_path: None });
        let described = describe_without_nvme(&observed);
        assert_eq!(
            described.occupant,
            DiskBayOccupant::UnusableDevice {
                reason: UnusableReason::NoDriver,
                devfs_path: None,
            }
        );
    }

    #[test]
    fn nvme_disk_takes_kind_and_label_from_bay() {
        let mut observed = nvme_bay(vec![namespace()]);
        observed.location = loc("M.2 West");
        observed.kind = DiskVariant::M2;
        observed.contents.as_mut().unwrap().pcie_slot = 18;

        let BayDescription::NeedsNvmeFacts(nvme) =
            describe_bay_from_topology(&observed).unwrap()
        else {
            panic!("expected an NVMe controller");
        };
        assert_eq!(nvme.instance(), instance());
        assert_eq!(nvme.pcie_slot(), 18);

        let (described, disk) =
            nvme.combine_with_nvme_facts(&facts(1), true).unwrap();
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
        assert_eq!(disk.location().as_str(), "M.2 West");
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
            describe_nvme(&nvme_bay(vec![ns]), facts(1), false).unwrap();
        assert_eq!(
            disk.unwrap().identity().model,
            "Samsung MZWLR3T8HBLS-00007"
        );
        assert_eq!(disk_model("", "X"), "X");
        assert_eq!(disk_model("NVMe", "X"), "X");
        assert_eq!(disk_model("V", "X"), "V X");
    }

    #[test]
    fn controller_without_namespace_is_unusable() {
        let (described, disk) =
            describe_nvme(&nvme_bay(vec![]), facts(0), false).unwrap();
        assert_eq!(
            described.occupant,
            DiskBayOccupant::UnusableDevice {
                reason: UnusableReason::NoUsableNamespace,
                devfs_path: Some(
                    "/pci@0,0/pci1de,fff9@1,1/pci1344,3100@0".to_string()
                ),
            }
        );
        assert!(disk.is_none());
    }

    #[test]
    fn namespace_missing_from_topology_is_an_error() {
        let err =
            describe_nvme(&nvme_bay(vec![]), facts(1), false).unwrap_err();
        assert!(
            matches!(
                err,
                DescribeError::MissingNamespace { usable_namespaces: 1, .. }
            ),
            "{err}"
        );
    }

    #[test]
    fn multiple_namespaces_are_unusable() {
        let (described, disk) = describe_nvme(
            &nvme_bay(vec![namespace(), namespace()]),
            facts(2),
            false,
        )
        .unwrap();
        assert_eq!(
            described.occupant,
            DiskBayOccupant::UnusableDevice {
                reason: UnusableReason::MultipleNamespaces { count: 2 },
                devfs_path: Some(
                    "/pci@0,0/pci1de,fff9@1,1/pci1344,3100@0".to_string()
                ),
            }
        );
        assert!(disk.is_none());
    }

    #[test]
    fn relative_devfs_path_is_rejected() {
        let mut ns = namespace();
        ns.devfs_path = "pci@0,0/oops".to_string();
        let err =
            describe_nvme(&nvme_bay(vec![ns]), facts(1), false).unwrap_err();
        assert!(
            matches!(err, DescribeError::RelativeDevfsPath { .. }),
            "{err}"
        );
    }

    #[test]
    fn missing_logical_disk_leaves_dev_path_unset() {
        let mut ns = namespace();
        ns.logical_disk = None;
        let (_, disk) =
            describe_nvme(&nvme_bay(vec![ns]), facts(1), false).unwrap();
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
            let err = describe_bay_from_topology(&observed).unwrap_err();
            assert!(
                matches!(
                    &err,
                    DescribeError::Unreadable { problem: p } if *p == problem
                ),
                "{err}"
            );
        }
    }

    fn loc(label: &str) -> DiskLocation {
        DiskLocation::new(label)
    }

    fn at<'a>(bays: &'a IdOrdMap<DiskBay>, label: &str) -> &'a DiskBay {
        bays.get(&loc(label)).expect("bay is reported")
    }

    fn log() -> Logger {
        Logger::root(slog::Discard, slog::o!())
    }

    fn described(
        observed: &ObservedBay,
        facts: Option<&NvmeFacts>,
    ) -> PolledBay {
        described_with_health(observed, facts, Ok(fake_health()))
    }

    fn described_with_health(
        observed: &ObservedBay,
        facts: Option<&NvmeFacts>,
        health: Result<NvmeHealthLogPage, NvmeHealthError>,
    ) -> PolledBay {
        let (bay, disk) = match describe_bay_from_topology(observed).unwrap() {
            BayDescription::Complete(bay) => (bay, None),
            BayDescription::NeedsNvmeFacts(nvme) => {
                let facts = facts.expect("an NVMe bay needs libnvme facts");
                nvme.combine_with_nvme_facts(facts, false).unwrap()
            }
        };
        let disk = disk.map(|disk| Box::new(PolledDisk { disk, health }));
        PolledBay::Described { bay, disk }
    }

    fn fake_health() -> NvmeHealthLogPage {
        NvmeHealthLogPage::new_fake(chrono::DateTime::UNIX_EPOCH)
    }

    fn unreadable(location: &str) -> PolledBay {
        PolledBay::Unreadable {
            location: loc(location),
            kind: DiskVariant::U2,
            driver: Some("nvme".to_string()),
            error: "controller locked".to_string(),
        }
    }

    fn unreadable_occupant() -> DiskBayOccupant {
        DiskBayOccupant::Unreadable {
            driver: Some("nvme".to_string()),
            error: "controller locked".to_string(),
        }
    }

    type View = (HashMap<DiskIdentity, UnparsedDisk>, IdOrdMap<DiskBay>);

    fn poll(polled: Result<Vec<PolledBay>, String>, previous: &View) -> View {
        let (view, _health) = poll_with_health(polled, previous);
        view
    }

    fn poll_with_health(
        polled: Result<Vec<PolledBay>, String>,
        (previous_disks, previous_bays): &View,
    ) -> (View, DiskHealthMap) {
        let polled = polled.map(|bays| collect_polled_bays(&log(), bays));
        let (disks, bays, health) =
            resolve_bays(&log(), polled, previous_bays, previous_disks);
        ((disks, bays), health)
    }

    /// The view after one poll that found a disk in N3.
    fn disk_in_n3() -> View {
        let view = poll(
            Ok(vec![described(&nvme_bay(vec![namespace()]), Some(&facts(1)))]),
            &View::default(),
        );
        assert_eq!(view.0.len(), 1);
        view
    }

    #[test]
    fn unreadable_bay_keeps_previous_disk() {
        let first = disk_in_n3();

        // The disk stays across consecutive unreadable polls, while the bay
        // reports only what this poll saw.
        let mut view = first.clone();
        for _ in 0..2 {
            view = poll(Ok(vec![unreadable("N3")]), &view);
            assert_eq!(view.0, first.0);
            assert_eq!(view.1.len(), 1);
            assert_eq!(at(&view.1, "N3").occupant, unreadable_occupant());
        }
    }

    #[test]
    fn unreadable_bay_without_history_has_no_disk() {
        let (disks, bays) = poll(
            Ok(vec![
                described(&bay(ObservedOccupant::Empty), None),
                unreadable("N4"),
            ]),
            &View::default(),
        );
        assert!(disks.is_empty());
        assert_eq!(bays.len(), 2);
        assert_eq!(at(&bays, "N3").occupant, DiskBayOccupant::Empty);
        assert_eq!(at(&bays, "N4").occupant, unreadable_occupant());
    }

    #[test]
    fn readable_bay_drops_previous_disk() {
        let (disks, bays) = poll(
            Ok(vec![described(&bay(ObservedOccupant::Empty), None)]),
            &disk_in_n3(),
        );
        assert!(disks.is_empty());
        assert_eq!(at(&bays, "N3").occupant, DiskBayOccupant::Empty);
    }

    #[test]
    fn missing_bay_keeps_previous_disk() {
        let first = disk_in_n3();
        let missing = DiskBayOccupant::Unreadable {
            driver: None,
            error: "not in the hardware topology".to_string(),
        };

        let mut view = first.clone();
        for _ in 0..2 {
            view = poll(Ok(vec![]), &view);
            assert_eq!(view.0, first.0);
            assert_eq!(view.1.len(), 1);
            assert_eq!(at(&view.1, "N3").kind, DiskVariant::U2);
            assert_eq!(at(&view.1, "N3").occupant, missing);
        }

        // Once the bay is back, it reports what it holds again.
        let (disks, bays) = poll(
            Ok(vec![described(&bay(ObservedOccupant::Empty), None)]),
            &view,
        );
        assert!(disks.is_empty());
        assert_eq!(at(&bays, "N3").occupant, DiskBayOccupant::Empty);
    }

    #[test]
    fn failed_read_marks_every_bay_unreadable() {
        let mut empty = bay(ObservedOccupant::Empty);
        empty.location = loc("N4");
        let first = poll(
            Ok(vec![
                described(&nvme_bay(vec![namespace()]), Some(&facts(1))),
                described(&empty, None),
            ]),
            &View::default(),
        );

        let (disks, bays) = poll(Err("snapshot failed".to_string()), &first);
        assert_eq!(disks, first.0);
        assert_eq!(bays.len(), 2);
        for bay in &bays {
            assert_eq!(
                bay.occupant,
                DiskBayOccupant::Unreadable {
                    driver: None,
                    error: "could not read the disk bays: snapshot failed"
                        .to_string(),
                }
            );
        }
    }

    #[test]
    fn failed_first_read_reports_nothing() {
        let (disks, bays) =
            poll(Err("snapshot failed".to_string()), &View::default());
        assert!(disks.is_empty());
        assert!(bays.is_empty());
    }

    #[test]
    fn fresh_disk_beats_carried_over_disk() {
        // The disk was in N3; it has moved to N4, and N3 now holds something
        // unreadable that sorts first.
        let mut moved = nvme_bay(vec![namespace()]);
        moved.location = loc("N4");

        let (disks, bays) = poll(
            Ok(vec![unreadable("N3"), described(&moved, Some(&facts(1)))]),
            &disk_in_n3(),
        );
        assert_eq!(disks.len(), 1);
        assert_eq!(disks.values().next().unwrap().location().as_str(), "N4");
        assert_eq!(at(&bays, "N3").occupant, unreadable_occupant());
        assert!(matches!(
            at(&bays, "N4").occupant,
            DiskBayOccupant::Disk { .. }
        ));
    }

    #[test]
    fn fresh_disk_beats_disk_of_missing_bay() {
        // The disk was in N3; N3 is gone from the topology and the disk is
        // now seen in N4.
        let mut moved = nvme_bay(vec![namespace()]);
        moved.location = loc("N4");

        let (disks, bays) =
            poll(Ok(vec![described(&moved, Some(&facts(1)))]), &disk_in_n3());
        assert_eq!(disks.len(), 1);
        assert_eq!(disks.values().next().unwrap().location().as_str(), "N4");
        assert!(matches!(
            at(&bays, "N3").occupant,
            DiskBayOccupant::Unreadable { .. }
        ));
        assert!(matches!(
            at(&bays, "N4").occupant,
            DiskBayOccupant::Disk { .. }
        ));
    }

    #[test]
    fn duplicate_disk_identity_keeps_first_bay() {
        let first = nvme_bay(vec![namespace()]);
        let mut second = nvme_bay(vec![namespace()]);
        second.location = loc("N4");
        let (disks, bays) = poll(
            Ok(vec![
                described(&first, Some(&facts(1))),
                described(&second, Some(&facts(1))),
            ]),
            &View::default(),
        );
        assert_eq!(disks.len(), 1);
        assert_eq!(disks.values().next().unwrap().location().as_str(), "N3");
        assert!(matches!(
            at(&bays, "N3").occupant,
            DiskBayOccupant::Disk { .. }
        ));
        assert_eq!(
            at(&bays, "N4").occupant,
            DiskBayOccupant::UnusableDevice {
                reason: UnusableReason::DuplicateIdentity {
                    identity: disks.values().next().unwrap().identity().clone(),
                    kept_in: loc("N3"),
                },
                devfs_path: None,
            }
        );
    }

    #[test]
    fn duplicate_disk_identity_reports_health_of_kept_bay() {
        let first = nvme_bay(vec![namespace()]);
        let mut second = nvme_bay(vec![namespace()]);
        second.location = loc("N4");
        let first_health =
            NvmeHealthLogPage { media_errors: 3, ..fake_health() };
        let second_health =
            NvmeHealthLogPage { media_errors: 4, ..fake_health() };
        // The bay that loses the disk is polled first.
        let ((disks, _bays), health) = poll_with_health(
            Ok(vec![
                described_with_health(
                    &second,
                    Some(&facts(1)),
                    Ok(second_health),
                ),
                described_with_health(
                    &first,
                    Some(&facts(1)),
                    Ok(first_health.clone()),
                ),
            ]),
            &View::default(),
        );
        let disk = disks.values().next().unwrap();
        assert_eq!(disk.location().as_str(), "N3");
        assert_eq!(
            health,
            DiskHealthMap::from([(disk.identity().clone(), Ok(first_health))])
        );
    }

    #[test]
    fn health_read_error_is_reported_for_the_disk() {
        let error = NvmeHealthError {
            kind: NvmeHealthErrorKind::LogPageReadFailed,
            message: "log page too short".into(),
        };
        let ((disks, _bays), health) = poll_with_health(
            Ok(vec![described_with_health(
                &nvme_bay(vec![namespace()]),
                Some(&facts(1)),
                Err(error.clone()),
            )]),
            &View::default(),
        );
        let disk = disks.values().next().unwrap();
        assert_eq!(
            health,
            DiskHealthMap::from([(disk.identity().clone(), Err(error))])
        );
    }

    #[test]
    fn unreadable_bay_keeps_previous_disk_without_health() {
        let previous = disk_in_n3();
        let ((disks, _bays), health) =
            poll_with_health(Ok(vec![unreadable("N3")]), &previous);
        assert_eq!(disks, previous.0);
        assert!(health.is_empty());
    }

    #[test]
    fn duplicate_location_keeps_first_bay() {
        let (disks, bays) = poll(
            Ok(vec![
                described(&bay(ObservedOccupant::Empty), None),
                described(&nvme_bay(vec![namespace()]), Some(&facts(1))),
            ]),
            &View::default(),
        );
        assert!(disks.is_empty());
        assert_eq!(bays.len(), 1);
        assert_eq!(at(&bays, "N3").occupant, DiskBayOccupant::Empty);
    }
}
