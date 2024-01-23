// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Types for tracking statistics about virtual machine instances.

use crate::kstat::hrtime_to_utc;
use crate::kstat::ConvertNamedData;
use crate::kstat::Error;
use crate::kstat::KstatList;
use crate::kstat::KstatTarget;
use chrono::DateTime;
use chrono::Utc;
use kstat_rs::Data;
use kstat_rs::Kstat;
use kstat_rs::Named;
use kstat_rs::NamedData;
use oximeter::types::Cumulative;
use oximeter::FieldType;
use oximeter::FieldValue;
use oximeter::Metric;
use oximeter::Sample;
use oximeter::Target;
use uuid::Uuid;

/// A single virtual machine instance.
#[derive(Clone, Debug)]
pub struct VirtualMachine {
    /// The silo to which the instance belongs.
    pub silo_id: Uuid,
    /// The project to which the instance belongs.
    pub project_id: Uuid,
    /// The ID of the instance.
    pub instance_id: Uuid,

    // This field is not published as part of the target field definitions. It
    // is needed because the hypervisor currently creates kstats for each vCPU,
    // regardless of whether they're activated. There is no way to tell from
    // userland today which vCPU kstats are "real". We include this value here,
    // and implement `oximeter::Target` manually.
    pub n_vcpus: u32,
}

impl Target for VirtualMachine {
    fn name(&self) -> &'static str {
        "virtual_machine"
    }

    fn field_names(&self) -> &'static [&'static str] {
        &["silo_id", "project_id", "instance_id"]
    }

    fn field_types(&self) -> Vec<FieldType> {
        vec![FieldType::Uuid, FieldType::Uuid, FieldType::Uuid]
    }

    fn field_values(&self) -> Vec<FieldValue> {
        vec![
            self.silo_id.into(),
            self.project_id.into(),
            self.instance_id.into(),
        ]
    }
}

/// Metric tracking vCPU usage by state.
#[derive(Clone, Debug, Metric)]
pub struct VcpuUsage {
    /// The vCPU ID.
    pub vcpu_id: u32,
    /// The state of the vCPU.
    pub state: String,
    /// The cumulative time spent in this state, in nanoseconds.
    pub datum: Cumulative<u64>,
}

// The name of the kstat module containing virtual machine kstats.
const VMM_KSTAT_MODULE_NAME: &str = "vmm";

// The name of the kstat with virtual machine metadata (VM name currently).
const VM_KSTAT_NAME: &str = "vm";

// The named kstat holding the virtual machine's name. This is currently the
// UUID assigned by the control plane to the virtual machine instance.
const VM_NAME_KSTAT: &str = "vm_name";

// The name of kstat containing vCPU usage data.
const VCPU_KSTAT_PREFIX: &str = "vcpu";

// Prefix for all named data with a valid vCPU microstate that we track.
const VCPU_MICROSTATE_PREFIX: &str = "time_";

// The number of expected vCPU microstates we track. This isn't load-bearing,
// and only used to help preallocate an array holding the `VcpuUsage` samples.
const N_VCPU_MICROSTATES: usize = 6;

impl KstatTarget for VirtualMachine {
    // The VMM kstats are organized like so:
    //
    // - module: vmm
    // - instance: a kernel-assigned integer
    // - name: vm -> generic VM info, vcpuX -> info for each vCPU
    //
    // At this part of the code, we don't have that kstat instance, only the
    // virtual machine instance's control plane UUID. However, the VM's "name"
    // is assigned to be that control plane UUID in the hypervisor. See
    // https://github.com/oxidecomputer/propolis/blob/759bf4a19990404c135e608afbe0d38b70bfa370/bin/propolis-server/src/lib/vm/mod.rs#L420
    // for the current code which does that.
    //
    // So we need to indicate interest in any VMM-related kstat here, and we are
    // forced to filter to the right instance by looking up the VM name inside
    // the `to_samples()` method below.
    fn interested(&self, kstat: &Kstat<'_>) -> bool {
        kstat.ks_module == VMM_KSTAT_MODULE_NAME
    }

    fn to_samples(
        &self,
        kstats: KstatList<'_, '_>,
    ) -> Result<Vec<Sample>, Error> {
        // First, we need to map the instance's control plane UUID to the
        // instance ID. We'll find this through the `vmm:<instance>:vm:vm_name`
        // kstat, which lists the instance's UUID as its name.
        //
        // Note that if this code is run from within a Propolis zone, there is
        // exactly one `vmm` kstat instance in any case.
        let instance_id = self.instance_id.to_string();
        let instance = kstats
            .iter()
            .find_map(|(_, kstat, data)| {
                kstat_instance_from_instance_id(kstat, data, &instance_id)
            })
            .ok_or_else(|| Error::NoSuchKstat)?;

        // Armed with the kstat instance, find all relevant metrics related to
        // this particular VM. For now, we produce only vCPU usage metrics, but
        // others may be chained in the future.
        let vcpu_stats = kstats.iter().filter(|(_, kstat, _)| {
            // Filter out those that don't match our kstat instance.
            if kstat.ks_instance != instance {
                return false;
            }

            // Filter out those which are neither a vCPU stat of any kind, nor
            // for one of the vCPU IDs we know to be active.
            let Some(suffix) = kstat.ks_name.strip_prefix(VCPU_KSTAT_PREFIX)
            else {
                return false;
            };
            let Ok(vcpu_id) = suffix.parse::<u32>() else {
                return false;
            };
            vcpu_id < self.n_vcpus
        });
        produce_vcpu_usage(self, vcpu_stats)
    }
}

// Given a kstat and an instance's ID, return the kstat instance if it matches.
pub fn kstat_instance_from_instance_id(
    kstat: &Kstat<'_>,
    data: &Data<'_>,
    instance_id: &str,
) -> Option<i32> {
    if kstat.ks_module != VMM_KSTAT_MODULE_NAME {
        return None;
    }
    if kstat.ks_name != VM_KSTAT_NAME {
        return None;
    }
    let Data::Named(named) = data else {
        return None;
    };
    if named.iter().any(|nd| {
        if nd.name != VM_NAME_KSTAT {
            return false;
        }
        let NamedData::String(name) = &nd.value else {
            return false;
        };
        instance_id == *name
    }) {
        return Some(kstat.ks_instance);
    }
    None
}

// Produce `Sample`s for the `VcpuUsage` metric from the relevant kstats.
pub fn produce_vcpu_usage<'a>(
    vm: &'a VirtualMachine,
    vcpu_stats: impl Iterator<Item = &'a (DateTime<Utc>, Kstat<'a>, Data<'a>)> + 'a,
) -> Result<Vec<Sample>, Error> {
    let mut out = Vec::with_capacity(N_VCPU_MICROSTATES);
    for (creation_time, kstat, data) in vcpu_stats {
        let Data::Named(named) = data else {
            return Err(Error::ExpectedNamedKstat);
        };
        let snapshot_time = hrtime_to_utc(kstat.ks_snaptime)?;

        // Find the vCPU ID, from the relevant named data item.
        let vcpu_id = named
            .iter()
            .find_map(|named| {
                if named.name == VCPU_KSTAT_PREFIX {
                    named.value.as_u32().ok()
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::NoSuchKstat)?;

        // We'll track all statistics starting with `time_` as the microstate.
        for Named { name, value } in named
            .iter()
            .filter(|nv| nv.name.starts_with(VCPU_MICROSTATE_PREFIX))
        {
            // Safety: We're filtering in the loop on this prefix, so it must
            // exist.
            let state =
                name.strip_prefix(VCPU_MICROSTATE_PREFIX).unwrap().to_string();
            let datum =
                Cumulative::with_start_time(*creation_time, value.as_u64()?);
            let metric = VcpuUsage { vcpu_id, state, datum };
            let sample =
                Sample::new_with_timestamp(snapshot_time, vm, &metric)?;
            out.push(sample);
        }
    }
    Ok(out)
}
