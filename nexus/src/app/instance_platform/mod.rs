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

// CPU platforms are broken out from other nouns here because they're wordy.
// Note that a given virtual platform (in `mod versions`) may not accept all CPU
// platforms, or may necessitate virtual-platform-specific adaptations.
mod cpu_platform;

// We expect that all historical versions of virtual platforms are expressible
// in terms of the latest propolis-client (and so, sled-agent-client)
// `InstanceSpec`. What we *do* retain for virtual platform versions are the
// individual combinations of modern nouns that describe prior virtual
// platforms, as well as older configurations of VM components which have
// changed across virtual platform versions.
mod versions;

use crate::app::instance::InstanceRegisterReason;

use nexus_db_queries::db;
use omicron_common::api::external::Error;
use sled_agent_client::types::{Cpuid, CpuidVendor, VmmSpec};
use sled_agent_types::inventory::NetworkInterface;
use uuid::Uuid;

impl super::Nexus {
    /// Generates a Propolis VM specificaation from the supplied database
    /// records.
    pub(crate) async fn generate_vmm_spec(
        &self,
        reason: &InstanceRegisterReason,
        instance: &db::model::Instance,
        vmm: &db::model::Vmm,
        disks: &[db::datastore::Disk],
        nics: &[NetworkInterface],
        ssh_keys: &[db::model::SshKey],
    ) -> Result<VmmSpec, Error> {
        match vmm.virtual_platform {
            db::model::VmmVirtualPlatform::Initial => {
                versions::initial::generate_vmm_spec(
                    self, reason, instance, vmm, disks, nics, ssh_keys,
                )
                .await
            }
            db::model::VmmVirtualPlatform::Generic20261122 => {
                versions::generic::generate_vmm_spec(
                    self, reason, instance, vmm, disks, nics, ssh_keys,
                )
                .await
            }
        }
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
    ncpu: u16,
) -> Result<Option<Cpuid>, cpu_platform::CpuPlatformError> {
    let cpuid = match platform {
        db::model::VmmCpuPlatform::SledDefault => return Ok(None),
        db::model::VmmCpuPlatform::AmdMilan => Cpuid {
            entries: cpu_platform::dump_to_cpuid_entries(
                cpu_platform::milan_rfd314(),
            ),
            vendor: CpuidVendor::Amd,
        },
        db::model::VmmCpuPlatform::AmdTurin => Cpuid {
            entries: cpu_platform::dump_to_cpuid_entries(
                cpu_platform::turin_v1(),
            ),
            vendor: CpuidVendor::Amd,
        },
        db::model::VmmCpuPlatform::AmdTurinV2 => Cpuid {
            entries: cpu_platform::dump_to_cpuid_entries(
                cpu_platform::turin_v2(ncpu)?,
            ),
            vendor: CpuidVendor::Amd,
        },
    };

    Ok(Some(cpuid))
}
