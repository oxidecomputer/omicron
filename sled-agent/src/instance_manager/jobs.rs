// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`Jobs`] is a wrapper around a `BTreeMap` + watch channel pair that handles:
//!
//! 1. Rejecting new VMM registrations when we're supposed to be evacuating
//! 2. Keeping the count of currently-registered VMMs reported by the watch
//!    channel in sync with the map of Propolis IDs to [`Instance`]s

use crate::instance::Instance;
use crate::instance_manager::VmmRegistrationDisallowedReason;
use omicron_uuid_kinds::PropolisUuid;
use sled_agent_config_reconciler::CurrentUpdateDisposition;
use sled_agent_types::inventory::OmicronSledUpdateDisposition;
use std::collections::BTreeMap;
use std::collections::btree_map;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceManagerJobsStatus {
    pub update_disposition: CurrentUpdateDisposition,
    pub num_registered_vmms: usize,
}

pub(super) enum CanEnsureVmmResult<'a> {
    Exists(&'a Instance),
    CannotRegister(VmmRegistrationDisallowedReason),
    CanRegister(RegisterNewVmm<'a>),
}

pub(super) struct Jobs {
    // Invariant: `jobs.len()` is always equal to
    // `status_tx.num_registered_vmms`. This is enforced by `remove()` and
    // `insert()` below, which always update `status_tx.num_registered_vmms`
    // when modifying the contents of `jobs`.
    jobs: BTreeMap<PropolisUuid, Instance>,
    status_tx: watch::Sender<InstanceManagerJobsStatus>,
}

impl Jobs {
    pub(super) fn new(
        update_disposition: CurrentUpdateDisposition,
    ) -> (Self, watch::Receiver<InstanceManagerJobsStatus>) {
        let (status_tx, status_rx) =
            watch::channel(InstanceManagerJobsStatus {
                update_disposition,
                num_registered_vmms: 0,
            });
        (Self { jobs: BTreeMap::new(), status_tx }, status_rx)
    }

    pub(super) fn set_update_disposition(
        &self,
        disposition: CurrentUpdateDisposition,
    ) {
        self.status_tx.send_modify(|status| {
            status.update_disposition = disposition;
        });
    }

    pub(super) fn get(&self, propolis_id: &PropolisUuid) -> Option<&Instance> {
        self.jobs.get(propolis_id)
    }

    pub(super) fn remove(
        &mut self,
        propolis_id: &PropolisUuid,
    ) -> Option<Instance> {
        let old = self.jobs.remove(propolis_id);
        if old.is_some() {
            self.status_tx.send_modify(|status| {
                status.num_registered_vmms -= 1;
            });
        }
        old
    }

    pub(super) fn iter(&self) -> btree_map::Iter<'_, PropolisUuid, Instance> {
        self.jobs.iter()
    }

    pub(super) fn can_ensure_vmm(
        &mut self,
        propolis_id: PropolisUuid,
    ) -> CanEnsureVmmResult<'_> {
        match self.jobs.entry(propolis_id) {
            btree_map::Entry::Occupied(entry) => {
                // If the instance already exists, just return it.
                CanEnsureVmmResult::Exists(entry.into_mut())
            }
            btree_map::Entry::Vacant(entry) => {
                // Otherwise, are we allowed to ensure new VMMs?
                match self.status_tx.borrow().update_disposition {
                    CurrentUpdateDisposition::ConfigNotAvailable => {
                        CanEnsureVmmResult::CannotRegister(
                            VmmRegistrationDisallowedReason::ConfigNotYetLoaded,
                        )
                    }
                    CurrentUpdateDisposition::Known(
                        OmicronSledUpdateDisposition::Evacuating,
                    ) => CanEnsureVmmResult::CannotRegister(
                        VmmRegistrationDisallowedReason::SledEvacuating,
                    ),
                    CurrentUpdateDisposition::Known(
                        OmicronSledUpdateDisposition::Available,
                    ) => CanEnsureVmmResult::CanRegister(RegisterNewVmm {
                        entry,
                        status_tx: &self.status_tx,
                    }),
                }
            }
        }
    }
}

pub(super) struct RegisterNewVmm<'a> {
    entry: btree_map::VacantEntry<'a, PropolisUuid, Instance>,
    status_tx: &'a watch::Sender<InstanceManagerJobsStatus>,
}

impl<'a> RegisterNewVmm<'a> {
    pub(super) fn insert(self, instance: Instance) -> &'a Instance {
        self.status_tx.send_modify(|status| {
            status.num_registered_vmms += 1;
        });
        self.entry.insert(instance)
    }
}
