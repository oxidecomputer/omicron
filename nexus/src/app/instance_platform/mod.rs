// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Logic for computing the virtual hardware platform to expose to an instance
//! given Nexus's records of its configuration.
//!
//! A VM's "virtual hardware platform" describes a set of virtual hardware
//! components to expose to a virtual guest and how those components are
//! configured. The code in this module is responsible for taking database
//! records describing an instance, its properties, and its attached peripherals
//! (e.g. disks and NICs) and constructing an "instance specification" that
//! sled-agent can use to initialize a Propolis VM that exposes the necessary
//! components.
//!
//! For more background on virtual platforms, see [RFD
//! 505](https://505.rfd.oxide.computer/).
//!
//! # Component identification
//!
//! Propolis VM specifications contain a "mainboard" that describes a VM's CPUs,
//! memory, and chipset and a "components" map that describes all of the VM's
//! attached virtual peripherals. Some peripherals are split into "device" and
//! "backend" components: the device tells Propolis what hardware to emulate for
//! the guest, and the backend describes what host resources (e.g. host VNICs)
//! and Oxide services (e.g. Crucible servers) the device should use to provide
//! its abstractions.
//!
//! Each component in a VM spec has a name, which is also its key in the
//! component map. Generally speaking, Propolis does not care how its clients
//! name components, but it does require that callers identify those components
//! by name in API calls that refer to a specific component. To make this as
//! easy as possible for the rest of Nexus, this module names components as
//! follows:
//!
//! - If a component corresponds to a specific control plane object (i.e.
//!   something like a disk or a NIC that has its own database record and a
//!   corresponding unique identifier):
//!   - If the component requires both a device and a backend, the *backend*
//!     uses the object's ID as a name, and the device uses a module-generated
//!     name.
//!   - If the component is unitary (i.e. it only has one component entry in the
//!     instance spec), this module uses the object ID as its name.
//! - "Default" components that don't correspond to control plane objects, such
//!   as a VM's serial ports, are named using the constants in the
//!   [`component_names`] module.
//!
//! Using component IDs as backend names makes it easy for other parts of Nexus
//! to know what ID to use to refer to a particular Propolis component: changes
//! to Crucible configuration use the disk ID; changes to a host VNIC name use
//! the relevant network interface ID; and so on. Device frontend configuration
//! changes are much less common and so are less important to optimize for.
//!
//! ## Live migration
//!
//! When a VM migrates, the source Propolis passes the VM's specification
//! directly to the target Propolis. For the most part, this allows Nexus to
//! avoid having to re-create a running VM's configuration in order to migrate
//! it. The exceptions are, once again, component backends: Nexus and sled-agent
//! may need to change how backends are configured when they migrate from one
//! sled to another. To facilitate this, Propolis's migration-request API allows
//! the caller to pass a list of "replacement" components that the target should
//! substitute into the specification it receives from the source. Substitutions
//! are done by component name, so this module must make sure to use a
//! consistent naming scheme for any components that might be replaced during a
//! migration. Since (at this writing) the only substitutable components are
//! backends, this is easily done by using component IDs as backend names, as
//! described above.

// CPU platforms are broken out only because they're wordy.
mod cpu_platform;

use std::collections::{BTreeMap, HashMap};

use crate::app::instance::InstanceRegisterReason;
use crate::cidata::InstanceCiData;

use nexus_db_queries::db;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::internal::shared::NetworkInterface;
use sled_agent_client::types::{
    BlobStorageBackend, Board, BootOrderEntry, BootSettings, Chipset,
    ComponentV0, Cpuid, CpuidVendor, CrucibleStorageBackend,
    I440Fx, InstanceSpecV0, NvmeDisk, PciPath, QemuPvpanic, SerialPort,
    SerialPortNumber, SpecKey, VirtioDisk, VirtioNetworkBackend, VirtioNic,
    VmmSpec,
};
use uuid::Uuid;

/// Constants and functions used to assign names to assorted VM components.
mod component_names {
    pub(super) const COM1: &'static str = "com1";
    pub(super) const COM2: &'static str = "com2";
    pub(super) const COM3: &'static str = "com3";
    pub(super) const COM4: &'static str = "com4";
    pub(super) const PVPANIC: &'static str = "pvpanic";
    pub(super) const BOOT_SETTINGS: &'static str = "boot-settings";
    pub(super) const CLOUD_INIT_DEVICE: &'static str = "cloud-init-dev";
    pub(super) const CLOUD_INIT_BACKEND: &'static str = "cloud-init-backend";

    /// Given an object ID, derives a name for the "device" half of the
    /// device/backend component pair that describes that object.
    pub(super) fn device_name_from_id(id: &uuid::Uuid) -> String {
        format!("{id}:device")
    }
}

enum PciDeviceKind {
    Disk,
    Nic,
    CloudInitDisk,
}

impl std::fmt::Display for PciDeviceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Disk => "disk",
                Self::Nic => "network interface",
                Self::CloudInitDisk => "cloud-init data disk",
            }
        )
    }
}

/// Computes the PCI attachment point for a device of the given `kind` which
/// carries the supplied `logical_slot` in its database record.
///
/// WARNING: Guests may write their virtual devices' PCI addresses to persistent
/// storage and expect that those devices will always appear in the same places
/// when the system is stopped and restarted. Changing these mappings for
/// existing instances may break them!
fn slot_to_pci_bdf(
    logical_slot: u8,
    kind: PciDeviceKind,
) -> Result<PciPath, Error> {
    // Use the mappings Propolis used when it was responsible for converting
    // slot numbers to device numbers: NICs get device numbers 8 through 15,
    // disks get 16 through 23, and the cloud-init disk is device 24.
    let device = match kind {
        PciDeviceKind::Disk if logical_slot < 8 => logical_slot + 0x10,
        PciDeviceKind::Nic if logical_slot < 8 => logical_slot + 0x8,
        PciDeviceKind::CloudInitDisk if logical_slot == 0 => 0x18,
        _ => {
            return Err(Error::invalid_value(
                format!("{kind} with slot {logical_slot}"),
                "slot number out of range for device",
            ));
        }
    };

    Ok(PciPath { bus: 0, device, function: 0 })
}

/// Generates a 20-byte NVMe device serial number from the bytes in a string
/// slice and pads the remaining bytes with zeroes. If the supplied slice is
/// more than 20 bytes, it is truncated.
///
/// NOTE: Zero-padding serial numbers is non-compliant behavior per version 1.2
/// of the NVMe spec (June 5, 2016), section 1.5. It is provided here for
/// compatibility with prior versions of Propolis that produced zero-padded
/// serial numbers from control plane-supplied disk names. Preserving
/// compatibility here is important because some guests may read an NVMe disk's
/// serial number to produce a logical "disk ID" that those guests (and their
/// users) may assume will remain stable for the lifetime of the disk.
pub fn zero_padded_nvme_serial_from_str(s: &str) -> [u8; 20] {
    let mut sn = [0u8; 20];

    let bytes_from_slice = sn.len().min(s.len());
    sn[..bytes_from_slice].copy_from_slice(&s.as_bytes()[..bytes_from_slice]);
    sn
}

/// Describes a Crucible-backed disk that should be added to an instance
/// specification.
#[derive(Debug)]
struct CrucibleDisk {
    device_name: String,
    device: ComponentV0,
    backend: CrucibleStorageBackend,
}

/// Stores a mapping from Nexus disk IDs to Crucible disk descriptors. This
/// allows the platform construction process to quickly determine the *device
/// name* for a disk with a given ID so that that name can be inserted into the
/// instance spec's boot settings.
struct DisksById(BTreeMap<Uuid, CrucibleDisk>);

impl DisksById {
    /// Creates a disk list from an iterator over a set of disk and volume
    /// records.
    ///
    /// The caller must ensure that the supplied `Volume`s have been checked out
    /// (i.e., that their Crucible generation numbers are up-to-date) before
    /// calling this function.
    fn from_disks<'i>(
        disks: impl Iterator<Item = (&'i db::model::Disk, &'i db::model::Volume)>,
    ) -> Result<Self, Error> {
        let mut map = BTreeMap::new();
        for (disk, volume) in disks {
            let slot = match disk.slot {
                Some(s) => s.0,
                None => {
                    return Err(Error::internal_error(&format!(
                        "disk {} is attached but has no PCI slot assignment",
                        disk.id()
                    )));
                }
            };

            let pci_path = slot_to_pci_bdf(slot, PciDeviceKind::Disk)?;
            let device = ComponentV0::NvmeDisk(NvmeDisk {
                backend_id: SpecKey::Uuid(disk.id()),
                pci_path,
                serial_number: zero_padded_nvme_serial_from_str(
                    disk.name().as_str(),
                ),
            });

            let backend = CrucibleStorageBackend {
                readonly: false,
                request_json: volume.data().to_owned(),
            };

            let device_name = component_names::device_name_from_id(&disk.id());
            if map
                .insert(
                    disk.id(),
                    CrucibleDisk { device_name, device, backend },
                )
                .is_some()
            {
                return Err(Error::internal_error(&format!(
                    "instance has multiple attached disks with ID {}",
                    disk.id()
                )));
            }
        }

        Ok(Self(map))
    }
}

/// A list of named components to add to an instance's spec.
//
// This is a HashMap so that it can be moved directly into a sled-agent instance
// spec (Progenitor generates HashMaps when it needs a map type).
struct Components(HashMap<String, ComponentV0>);

impl Default for Components {
    /// Adds the default set of VM components that are expected to exist for all
    /// Oxide VMs (serial ports attached to COM1-COM4 and a paravirtualized
    /// panic device).
    fn default() -> Self {
        let map = [
            (
                component_names::COM1,
                ComponentV0::SerialPort(SerialPort {
                    num: SerialPortNumber::Com1,
                }),
            ),
            (
                component_names::COM2,
                ComponentV0::SerialPort(SerialPort {
                    num: SerialPortNumber::Com2,
                }),
            ),
            (
                component_names::COM3,
                ComponentV0::SerialPort(SerialPort {
                    num: SerialPortNumber::Com3,
                }),
            ),
            (
                component_names::COM4,
                ComponentV0::SerialPort(SerialPort {
                    num: SerialPortNumber::Com4,
                }),
            ),
            (
                component_names::PVPANIC,
                ComponentV0::QemuPvpanic(QemuPvpanic { enable_isa: true }),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();

        Self(map)
    }
}

impl Components {
    /// Adds a named component to this component list. Returns a 500 error if
    /// the component name was already present in the list.
    fn add(
        &mut self,
        key: String,
        component: ComponentV0,
    ) -> Result<(), Error> {
        use std::collections::hash_map::Entry;

        // Component names are an implementation detail internal to this module,
        // so they should always be unique.
        match self.0.entry(key) {
            Entry::Occupied(occupied_entry) => {
                Err(Error::internal_error(&format!(
                    "duplicate instance spec component key: {}",
                    occupied_entry.key()
                )))
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(component);
                Ok(())
            }
        }
    }

    /// Adds the set of disks in the supplied disk list to this component
    /// list.
    fn add_disks(&mut self, disks: DisksById) -> Result<(), Error> {
        // This operation will add a device and a backend for every disk in the
        // input set.
        self.0.reserve(disks.0.len() * 2);
        for (id, CrucibleDisk { device_name, device, backend }) in
            disks.0.into_iter()
        {
            self.add(device_name, device)?;
            self.add(
                id.to_string(),
                ComponentV0::CrucibleStorageBackend(backend),
            )?;
        }

        Ok(())
    }

    /// Adds the supplied set of NICs to this component manifest.
    fn add_nics(&mut self, nics: &[NetworkInterface]) -> Result<(), Error> {
        // This operation will add a device and a backend for every NIC in the
        // input slice.
        self.0.reserve(nics.len() * 2);
        for nic in nics.iter() {
            let device_name = component_names::device_name_from_id(&nic.id);
            let device = ComponentV0::VirtioNic(VirtioNic {
                backend_id: SpecKey::Uuid(nic.id),
                interface_id: nic.id,
                pci_path: slot_to_pci_bdf(nic.slot, PciDeviceKind::Nic)?,
            });

            // Sled-agent creates OPTE ports during instance startup using its
            // per-sled port allocator, so it needs to (and will) fill in the
            // correct port name once one is assigned.
            let backend =
                ComponentV0::VirtioNetworkBackend(VirtioNetworkBackend {
                    vnic_name: "".to_string(),
                });

            // N.B. It's crucial to use the NIC ID as the backend name here so
            // that sled-agent can correlate this component entry with the OPTE
            // port it created for the guest network interface with this ID.
            self.add(nic.id.to_string(), backend)?;
            self.add(device_name, device)?;
        }

        Ok(())
    }

    /// Adds a cloud-init disk to this component manifest. The disk will expose
    /// the supplied `ssh_keys` to the guest.
    fn add_cloud_init(
        &mut self,
        instance: &db::model::Instance,
        ssh_keys: &[db::model::SshKey],
    ) -> Result<(), Error> {
        let keys: Vec<String> =
            ssh_keys.iter().map(|k| k.public_key.clone()).collect();
        let base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            instance.generate_cidata(&keys)?,
        );

        let device = ComponentV0::VirtioDisk(VirtioDisk {
            backend_id: SpecKey::Name(
                component_names::CLOUD_INIT_BACKEND.to_string(),
            ),
            pci_path: slot_to_pci_bdf(0, PciDeviceKind::CloudInitDisk)
                .expect("slot 0 is always valid for cloud-init disks"),
        });

        let backend = ComponentV0::BlobStorageBackend(BlobStorageBackend {
            base64,
            readonly: true,
        });

        self.add(component_names::CLOUD_INIT_DEVICE.to_string(), device)?;
        self.add(component_names::CLOUD_INIT_BACKEND.to_string(), backend)?;

        Ok(())
    }
}

impl super::Nexus {
    /// Generates a Propolis VM specificaation from the supplied database
    /// records.
    pub(crate) async fn generate_vmm_spec(
        &self,
        reason: &InstanceRegisterReason,
        instance: &db::model::Instance,
        vmm: &db::model::Vmm,
        disks: &[db::model::Disk],
        nics: &[NetworkInterface],
        ssh_keys: &[db::model::SshKey],
    ) -> Result<VmmSpec, Error> {
        let cpus = u8::try_from(instance.ncpus.0.0).map_err(|c| {
            Error::invalid_value(
                c.to_string(),
                "failed to convert instance CPU count to a u8",
            )
        })?;

        let mut components = Components::default();

        // Get the volume information needed to fill in the disks' backends'
        // volume construction requests. Calling `volume_checkout` bumps
        // the volumes' generation numbers.
        let mut volumes = Vec::with_capacity(disks.len());
        for disk in disks {
            use db::datastore::VolumeCheckoutReason;
            let volume = self
                .db_datastore
                .volume_checkout(
                    disk.volume_id(),
                    match reason {
                        InstanceRegisterReason::Start { vmm_id } => {
                            VolumeCheckoutReason::InstanceStart {
                                vmm_id: *vmm_id,
                            }
                        }
                        InstanceRegisterReason::Migrate {
                            vmm_id,
                            target_vmm_id,
                        } => VolumeCheckoutReason::InstanceMigrate {
                            vmm_id: *vmm_id,
                            target_vmm_id: *target_vmm_id,
                        },
                    },
                )
                .await?;

            volumes.push(volume);
        }

        let disks = DisksById::from_disks(disks.iter().zip(volumes.iter()))?;

        // Add the instance's boot settings. Propolis expects boot order entries
        // that specify disks to refer to the *device* components of those
        // disks, not the backend components, so use the disk map to get this
        // module's selected device name for the appropriate disk.
        if let Some(boot_disk_id) = instance.boot_disk_id {
            if let Some(disk) = disks.0.get(&boot_disk_id) {
                let entry = BootOrderEntry {
                    id: SpecKey::Name(disk.device_name.clone()),
                };

                components.add(
                    component_names::BOOT_SETTINGS.to_owned(),
                    ComponentV0::BootSettings(BootSettings {
                        order: vec![entry],
                    }),
                )?;
            } else {
                return Err(Error::internal_error(&format!(
                    "instance's boot disk {boot_disk_id} is not attached"
                )));
            }
        }

        components.add_disks(disks)?;
        components.add_nics(nics)?;
        components.add_cloud_init(instance, ssh_keys)?;

        let spec = InstanceSpecV0 {
            board: Board {
                chipset: Chipset::I440Fx(I440Fx { enable_pcie: false }),
                cpuid: cpuid_from_vmm_cpu_platform(vmm.cpu_platform),
                cpus,
                guest_hv_interface: None,
                memory_mb: instance.memory.to_whole_mebibytes(),
            },
            components: components.0,
        };

        Ok(VmmSpec(spec))
    }
}

/// Yields the CPUID configuration to use for a VMM that specifies the supplied
/// CPU `platform`.
//
// This is a free function (and not an `Into` impl on `VmmCpuPlatform`) to keep
// all of the gnarly CPUID details out of the DB model crate, which defines that
// type.
fn cpuid_from_vmm_cpu_platform(
    platform: db::model::VmmCpuPlatform,
) -> Option<Cpuid> {
    let cpuid = match platform {
        db::model::VmmCpuPlatform::SledDefault => return None,
        db::model::VmmCpuPlatform::AmdMilan
        | db::model::VmmCpuPlatform::AmdTurin => {
            Cpuid { entries: cpu_platform::milan_rfd314(), vendor: CpuidVendor::Amd }
        }
    };

    Some(cpuid)
}
