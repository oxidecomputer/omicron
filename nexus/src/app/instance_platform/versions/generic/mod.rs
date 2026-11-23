// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// TODO: GO MAKE SURE THE ORDER HERE MATCHES THE ORDER THESE CAME FROM IN THE DIFF

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::app::instance::InstanceRegisterReason;
use crate::cidata::InstanceCiData;

use crate::app::LOCAL_STORAGE_WORKERS;
use crate::db::datastore::Disk;
use nexus_db_queries::db;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use sled_agent_client::types::VirtioSocket;
use sled_agent_client::types::{
    BlobStorageBackend, Board, BootOrderEntry, BootSettings, Chipset,
    Component, CrucibleStorageBackend, FileStorageBackend, I440Fx,
    InstanceSpec, NvmeDisk, QemuPvpanic, SerialPort, SerialPortNumber,
    SmbiosType1Input, SpecKey, VirtioDisk, VirtioNetworkBackend, VirtioNic,
    VmmSpec,
};
// TODO: this is gross, and a lie, but this is how sled-agent-client re-exports the propolis-client
// type
use sled_agent_client::CpuTopology;
use sled_agent_types::inventory::NetworkInterface;
use uuid::Uuid;

/// Default `guest_cid` for a propolis guest.
const VSOCK_GUEST_CID: u64 = 16;

use crate::app::instance_platform::versions::initial::{
    DiskComponents, DisksById, PciDeviceKind, component_names, slot_to_pci_bdf,
};

/// Generates a 20-byte NVMe device serial number from the bytes in a string
/// slice and pads the remaining bytes with ASCII space. If the supplied slice
/// is more than 20 bytes, it is truncated.
///
/// Note that this is different from the initial NVMe-to-serial-number approach,
/// which violated the NVMe spec. Flipping between these padding forms changes
/// the NVMe device's serial number from the EDK2/guest OS perspective, and
/// (among other things) can break boot settings if guests don't have explicit
/// boot settings. For this, and other, reasons, flipping between "Initial" and
/// "Generic" virtual platforms can be expected to be disruptive.
pub fn nvme_serial_from_str(s: &str) -> [u8; 20] {
    let mut sn = [b' '; 20];

    let bytes_from_slice = sn.len().min(s.len());
    sn[..bytes_from_slice].copy_from_slice(&s.as_bytes()[..bytes_from_slice]);
    sn
}

/// A list of named components to add to an instance's spec.
//
// This is a HashMap so that it can be moved directly into a sled-agent instance
// spec (Progenitor generates HashMaps when it needs a map type).
struct Components(HashMap<String, Component>);

impl Components {
    /// Adds a named component to this component list. Returns a 500 error if
    /// the component name was already present in the list.
    fn add(&mut self, key: String, component: Component) -> Result<(), Error> {
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

        for (id, disk_components) in disks.0.into_iter() {
            let DiskComponents { device_name, device, backend } =
                disk_components;

            self.add(device_name, device)?;
            self.add(id.to_string(), backend)?;
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
            let device = Component::VirtioNic(VirtioNic {
                backend_id: SpecKey::Uuid(nic.id),
                interface_id: nic.id,
                pci_path: slot_to_pci_bdf(nic.slot, PciDeviceKind::Nic)?,
            });

            // Sled-agent creates OPTE ports during instance startup using its
            // per-sled port allocator, so it needs to (and will) fill in the
            // correct port name once one is assigned.
            let backend =
                Component::VirtioNetworkBackend(VirtioNetworkBackend {
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

        let device = Component::VirtioDisk(VirtioDisk {
            backend_id: SpecKey::Name(
                component_names::CLOUD_INIT_BACKEND.to_string(),
            ),
            pci_path: slot_to_pci_bdf(0, PciDeviceKind::CloudInitDisk)
                .expect("slot 0 is always valid for cloud-init disks"),
        });

        let backend = Component::BlobStorageBackend(BlobStorageBackend {
            base64,
            readonly: true,
        });

        self.add(component_names::CLOUD_INIT_DEVICE.to_string(), device)?;
        self.add(component_names::CLOUD_INIT_BACKEND.to_string(), backend)?;

        Ok(())
    }
}

struct DisksByIdBuilder {
    map: BTreeMap<Uuid, DiskComponents>,
    slot_usage: BTreeSet<u8>,
}

impl From<DisksByIdBuilder> for DisksById {
    fn from(v: DisksByIdBuilder) -> DisksById {
        DisksById(v.map)
    }
}

impl DisksByIdBuilder {
    fn new() -> Self {
        Self { map: BTreeMap::new(), slot_usage: BTreeSet::new() }
    }

    fn add_nvme_disk(
        &mut self,
        disk: &Disk,
        backend: Component,
    ) -> Result<(), Error> {
        let slot = disk.slot().ok_or(Error::internal_error(&format!(
            "disk {} is attached but has no PCI slot assignment",
            disk.id()
        )))?;

        if self.slot_usage.contains(&slot) {
            return Err(Error::internal_error(&format!(
                "disk PCI slot {slot} used more than once, cannot attach {}",
                disk.id(),
            )));
        }

        self.slot_usage.insert(slot);

        let pci_path = slot_to_pci_bdf(slot, PciDeviceKind::Disk)?;

        // Generally we report that storage devices have volatile write cache
        // semantics. This is a conservative default that matches most system
        // behaviors. Crucible fast-acks writes and requires flushes to persist
        // writes to non-volatile storage, and - theoretically - file-backed
        // disks could be *any file* which may include normal POSIX "you must
        // fdatasync() for writes to not be lost" semantics.
        //
        // This being anything other than "true" must be carefully considered;
        // incorrectly claiming there is no write cache while the backing
        // storage has volatile write cache semantics risks guest data loss in
        // the event of power loss or crashes.
        //
        // On the other hand, when we can avoid claiming volatile write cache
        // semantics, guest OSes know to not send spurious flushes. This can
        // have important performance consequences from avoided VM exits,
        // Propolis syscalls, interrupts, etc.
        let volatile_write_cache = match &backend {
            // We match on all fields so that if FileStorageBackend changes,
            // those changes must consider if volatile-write-cache semantics are
            // correctly captured here.
            Component::FileStorageBackend(FileStorageBackend {
                path,
                readonly: _,
                block_size: _,
                workers: _,
            }) => {
                // In the product, for the forseeable future, local storage raw
                // zvols are on enterprise U.2s which do not report volatile
                // write caches. "/rdsk/" here refers to the character device
                // for that raw volume with unbuffered semantics (versus
                // `/dsk/`, the block device, which can buffer writes when not
                // opened O_DIRECT - see spec_write(), vpm_data_copy(), and
                // vpm_sync_pages()).
                //
                // XXX: In development and non-product environments where
                // storage may be commodity M.2s or worse, this can claim "no
                // VWC semantics" when the underlying storage actually does.
                // This could be improved. See Omicron#10933.
                let vwc_semantics = !path.starts_with("/dev/zvol/rdsk/");

                vwc_semantics
            }
            _ => true,
        };

        let device = Component::NvmeDisk(NvmeDisk {
            backend_id: SpecKey::Uuid(disk.id()),
            pci_path,
            serial_number: nvme_serial_from_str(disk.name().as_str()),
            has_write_cache: volatile_write_cache,
        });

        let device_name = component_names::device_name_from_id(&disk.id());

        let components = DiskComponents { device, device_name, backend };

        if self.map.insert(disk.id(), components).is_some() {
            return Err(Error::internal_error(&format!(
                "instance has multiple attached disks with ID {}",
                disk.id()
            )));
        }

        Ok(())
    }

    fn add_crucible_disk(
        &mut self,
        disk: &Disk,
        volume: &db::model::Volume,
    ) -> Result<(), Error> {
        let backend =
            Component::CrucibleStorageBackend(CrucibleStorageBackend {
                readonly: disk.is_read_only(),
                request_json: volume.data().to_owned(),
            });

        self.add_nvme_disk(disk, backend)
    }

    fn add_local_disk(
        &mut self,
        disk: &Disk,
        path: String,
    ) -> Result<(), Error> {
        let backend = Component::FileStorageBackend(FileStorageBackend {
            path,
            // Presently, this will always be false for local storage disks, but
            // we may as well ask the disk rather than hard-coding it...
            readonly: disk.is_read_only(),
            block_size: disk.block_size().to_bytes(),
            workers: Some(LOCAL_STORAGE_WORKERS),
        });

        self.add_nvme_disk(disk, backend)
    }
}

impl Components {
    /// Adds the default set of VM components that are expected to exist for all
    /// Oxide VMs (serial ports attached to COM1-COM4 and a paravirtualized
    /// panic device).
    fn default_initial() -> Self {
        let map = [
            (
                component_names::COM1,
                Component::SerialPort(SerialPort {
                    num: SerialPortNumber::Com1,
                }),
            ),
            (
                component_names::COM2,
                Component::SerialPort(SerialPort {
                    num: SerialPortNumber::Com2,
                }),
            ),
            (
                component_names::COM3,
                Component::SerialPort(SerialPort {
                    num: SerialPortNumber::Com3,
                }),
            ),
            (
                component_names::COM4,
                Component::SerialPort(SerialPort {
                    num: SerialPortNumber::Com4,
                }),
            ),
            (
                component_names::PVPANIC,
                Component::QemuPvpanic(QemuPvpanic { enable_isa: true }),
            ),
            (
                component_names::VSOCK,
                Component::VirtioSocket(VirtioSocket {
                    guest_cid: VSOCK_GUEST_CID,
                    pci_path: slot_to_pci_bdf(0, PciDeviceKind::Vsock)
                        .expect("vsock bdf (0, 25, 0)"),
                }),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();

        Self(map)
    }
}

pub(crate) async fn generate_vmm_spec(
    nexus: &crate::app::Nexus,
    reason: &InstanceRegisterReason,
    instance: &db::model::Instance,
    vmm: &db::model::Vmm,
    disks: &[db::datastore::Disk],
    nics: &[NetworkInterface],
    ssh_keys: &[db::model::SshKey],
) -> Result<VmmSpec, Error> {
    let cpus = instance.ncpus.0.0;

    let mut builder = DisksByIdBuilder::new();

    for disk in disks {
        match &disk {
            db::datastore::Disk::Crucible(crucible_disk) => {
                // Get the volume information needed to fill in the disks'
                // backends' volume construction requests. Calling
                // `volume_checkout` bumps the volumes' generation numbers.

                use db::datastore::VolumeCheckoutReason;

                let volume = nexus
                    .db_datastore
                    .volume_checkout(
                        crucible_disk.volume_id(),
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

                builder.add_crucible_disk(disk, &volume)?;
            }

            db::datastore::Disk::LocalStorage(local_storage_disk) => {
                builder.add_local_disk(
                    disk,
                    // Use the delegated zvol as the target for the file
                    // backed disk
                    local_storage_disk.zvol_path()?,
                )?;
            }
        }
    }

    let disks: DisksById = builder.into();
    let mut components = Components::default_initial();

    // Add the instance's boot settings. Propolis expects boot order entries
    // that specify disks to refer to the *device* components of those
    // disks, not the backend components, so use the disk map to get this
    // module's selected device name for the appropriate disk.
    if let Some(boot_disk_id) = instance.boot_disk_id {
        if let Some(disk) = disks.0.get(&boot_disk_id) {
            let entry = BootOrderEntry {
                id: SpecKey::Name(disk.device_name().to_owned()),
            };

            components.add(
                component_names::BOOT_SETTINGS.to_owned(),
                Component::BootSettings(BootSettings { order: vec![entry] }),
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

    let cpuid = crate::app::instance_platform::cpuid_from_vmm_cpu_platform(
        vmm.cpu_platform,
        cpus,
    )
    .map_err(|e| {
        let message =
            format!("failed to instantiate CPU platform {}", vmm.cpu_platform);
        Error::invalid_value(e.to_string(), message)
    })?;

    let smbios = SmbiosType1Input {
        manufacturer: "Oxide".to_string(),
        product_name: "Oxide Generic VM".to_string(),
        serial_number: instance.id().to_string(),
        version: 0,
    };

    let spec = InstanceSpec {
        board: Board {
            chipset: Chipset::I440Fx(I440Fx { enable_pcie: false }),
            cpuid,
            cpus,
            guest_hv_interface: None,
            memory_mb: instance.memory.to_whole_mebibytes(),
            cpu_topology: CpuTopology::single_socket_smt(),
        },
        components: components.0,
        smbios: Some(smbios),
    };

    Ok(VmmSpec(spec))
}
