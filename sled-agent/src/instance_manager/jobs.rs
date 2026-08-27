// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module to ensure invariants of [`Jobs`] are upheld.

use crate::instance::Instance;
use crate::instance_manager::VmmRegistrationDisallowedReason;
use omicron_uuid_kinds::PropolisUuid;
use sled_agent_config_reconciler::CurrentUpdateDisposition;
use sled_agent_types::inventory::OmicronSledUpdateDisposition;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceManagerJobsStatus {
    pub update_disposition: CurrentUpdateDisposition,
    pub num_registered_vmms: usize,
}

#[derive(Debug)]
pub struct InstanceManagerJobsStatusReceiver {
    status: Arc<RwLock<InstanceManagerJobsStatus>>,
}

impl InstanceManagerJobsStatusReceiver {
    pub fn read(&self) -> InstanceManagerJobsStatus {
        *self.status.read().unwrap()
    }
}

#[derive(Debug)]
pub(super) enum CanEnsureVmmResult<'a, T> {
    Exists(&'a T),
    CannotRegister(VmmRegistrationDisallowedReason),
    CanRegister(RegisterNewVmm<'a, T>),
}

/// [`Jobs`] stores a set of VMM registrations (i.e., [`Instance`]s keyed by
/// their propolis ID).
///
/// Callers should treat this as a fancy `BTreeMap<PropolisUuid, Instance>` with
/// some special sauce:
///
/// 1. `Jobs` is able to act on the sled's update disposition, and knows that
///    new VMM registrations may be disallowed depending on the disposition.
/// 2. Instead of a direct `insert()` method or `entry()` API,
///    callers should use [`Jobs::can_ensure_vmm()`] when they want to ensure a
///    VMM registration exists. Its return value can represent all three
///    relevant cases: the VMM is already registered, VMM registrations are
///    disallowed by the disposition, or the VMM can be registered; in the third
///    case the returned value includes a [`RegisterNewVmm`] which provides
///    [`RegisterNewVmm::insert()`].
/// 3. The status of VMM registrations is available via receivers returned by
///    [`Jobs::status_receiver()`]. This contains information relevant to
///    Reconfigurator for determining when a sled is fully evacuated for an
///    update.
//
// This type is generic only to support easy testing without having to construct
// `Instance`s; prod code always uses the default type.
pub(super) struct Jobs<T = Instance> {
    // Invariant: `jobs.len()` is always equal to
    // `status.num_registered_vmms`. This is enforced by `Jobs::remove()` and
    // `RegisterNewVmm::insert()` below, which always update `status` when
    // modifying the contents of `jobs`.
    jobs: BTreeMap<PropolisUuid, T>,
    status: Arc<RwLock<InstanceManagerJobsStatus>>,
}

impl<T> Jobs<T> {
    pub(super) fn new(update_disposition: CurrentUpdateDisposition) -> Self {
        let status = Arc::new(RwLock::new(InstanceManagerJobsStatus {
            update_disposition,
            num_registered_vmms: 0,
        }));
        Self { jobs: BTreeMap::new(), status }
    }

    pub(super) fn status_receiver(&self) -> InstanceManagerJobsStatusReceiver {
        InstanceManagerJobsStatusReceiver { status: Arc::clone(&self.status) }
    }

    pub(super) fn set_update_disposition(
        &mut self,
        disposition: CurrentUpdateDisposition,
    ) {
        let mut status = self.status.write().unwrap();
        status.update_disposition = disposition;
    }

    pub(super) fn get(&self, propolis_id: &PropolisUuid) -> Option<&T> {
        self.jobs.get(propolis_id)
    }

    pub(super) fn remove(&mut self, propolis_id: &PropolisUuid) -> Option<T> {
        let old = self.jobs.remove(propolis_id);
        if old.is_some() {
            let mut status = self.status.write().unwrap();
            status.num_registered_vmms -= 1;
        }
        old
    }

    pub(super) fn iter(&self) -> btree_map::Iter<'_, PropolisUuid, T> {
        self.jobs.iter()
    }

    pub(super) fn can_ensure_vmm(
        &mut self,
        propolis_id: PropolisUuid,
    ) -> CanEnsureVmmResult<'_, T> {
        match self.jobs.entry(propolis_id) {
            btree_map::Entry::Occupied(entry) => {
                // If the instance already exists, just return it.
                CanEnsureVmmResult::Exists(entry.into_mut())
            }
            btree_map::Entry::Vacant(entry) => {
                // Otherwise, are we allowed to ensure new VMMs?
                match self.status.read().unwrap().update_disposition {
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
                        status: &self.status,
                    }),
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct RegisterNewVmm<'a, T> {
    entry: btree_map::VacantEntry<'a, PropolisUuid, T>,
    status: &'a RwLock<InstanceManagerJobsStatus>,
}

impl<'a, T> RegisterNewVmm<'a, T> {
    pub(super) fn insert(self, instance: T) -> &'a T {
        {
            let mut status = self.status.write().unwrap();
            status.num_registered_vmms += 1;
        }
        self.entry.insert(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_strategy::proptest;

    // Our proptests use `u8` as the propolis ID type (allowing proptest to
    // easily generate some duplicate IDs) and `usize` as the instance type.
    fn fake_id_to_propolis_uuid(id: u8) -> PropolisUuid {
        PropolisUuid::from_u128(u128::from(id))
    }

    #[derive(Debug, Clone, Copy, test_strategy::Arbitrary)]
    enum Op {
        UpdateDisposition(CurrentUpdateDisposition),
        TryEnsure { id: u8, instance: usize, insert_if_allowed: bool },
        Remove { id: u8 },
    }

    #[derive(Debug)]
    struct Model {
        disposition: CurrentUpdateDisposition,
        jobs: BTreeMap<u8, usize>,
    }

    impl Model {
        fn new(disposition: CurrentUpdateDisposition) -> Self {
            Self { disposition, jobs: BTreeMap::new() }
        }

        fn expected_can_ensure_result(
            &self,
            id: &u8,
        ) -> Result<Option<&usize>, VmmRegistrationDisallowedReason> {
            if let Some(n) = self.jobs.get(id) {
                return Ok(Some(n));
            }
            match self.disposition {
                CurrentUpdateDisposition::ConfigNotAvailable => {
                    Err(VmmRegistrationDisallowedReason::ConfigNotYetLoaded)
                }
                CurrentUpdateDisposition::Known(
                    OmicronSledUpdateDisposition::Evacuating,
                ) => Err(VmmRegistrationDisallowedReason::SledEvacuating),
                CurrentUpdateDisposition::Known(
                    OmicronSledUpdateDisposition::Available,
                ) => Ok(None),
            }
        }

        // Insert a new "instance" by its "propolis ID".
        //
        // Must only be called if the test confirmed inserting is okay; i.e.,
        // the return value of `expected_can_ensure_result(&id)` is
        // `Ok(None)`.
        fn insert(&mut self, id: u8, instance: usize) {
            assert_eq!(self.expected_can_ensure_result(&id), Ok(None));
            self.jobs.insert(id, instance);
        }

        fn apply(&mut self, op: Op) {
            match op {
                Op::UpdateDisposition(disposition) => {
                    self.disposition = disposition;
                }
                Op::TryEnsure { id, instance, insert_if_allowed } => {
                    if insert_if_allowed
                        && self.expected_can_ensure_result(&id) == Ok(None)
                    {
                        self.insert(id, instance);
                    }
                }
                Op::Remove { id } => {
                    self.jobs.remove(&id);
                }
            }
        }

        // Assert that the contents of the model (`self`) exactly match what's
        // been tracked by `jobs`.
        fn assert_matches(&self, jobs: &Jobs<usize>) {
            let expected_jobs = self
                .jobs
                .iter()
                .map(|(k, v)| (fake_id_to_propolis_uuid(*k), *v))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(expected_jobs, jobs.jobs);

            let status = jobs.status.read().unwrap();
            assert_eq!(self.disposition, status.update_disposition);
            assert_eq!(expected_jobs.len(), status.num_registered_vmms);
        }
    }

    #[proptest]
    fn jobs_upholds_invariants(
        initial_disposition: CurrentUpdateDisposition,
        ops: Vec<Op>,
    ) {
        let mut model = Model::new(initial_disposition);
        let mut jobs = Jobs::<usize>::new(initial_disposition);

        for op in ops {
            match op {
                Op::UpdateDisposition(disposition) => {
                    jobs.set_update_disposition(disposition);
                }
                Op::TryEnsure { id, instance, insert_if_allowed } => {
                    match (
                        model.expected_can_ensure_result(&id),
                        jobs.can_ensure_vmm(fake_id_to_propolis_uuid(id)),
                    ) {
                        (
                            Ok(Some(expected_instance)),
                            CanEnsureVmmResult::Exists(actual_instance),
                        ) => {
                            assert_eq!(expected_instance, actual_instance);
                        }
                        (Ok(None), CanEnsureVmmResult::CanRegister(entry)) => {
                            if insert_if_allowed {
                                entry.insert(instance);
                            }
                        }
                        (
                            Err(expected_reason),
                            CanEnsureVmmResult::CannotRegister(actual_reason),
                        ) => {
                            assert_eq!(expected_reason, actual_reason);
                        }
                        (expected, actual) => {
                            panic!("expected {expected:?} but got {actual:?}");
                        }
                    }
                }
                Op::Remove { id } => {
                    let exists = model.jobs.get(&id);
                    let removed = jobs.remove(&fake_id_to_propolis_uuid(id));
                    assert_eq!(exists, removed.as_ref());
                }
            }
            model.apply(op);
            model.assert_matches(&jobs);
        }
    }
}
