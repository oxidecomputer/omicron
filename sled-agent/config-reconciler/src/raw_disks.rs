// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Provides thin wrappers around a watch channel managing the set of
//! [`RawDisk`]s sled-agent is aware of.

use id_map::Entry;
use id_map::IdMap;
use id_map::IdMappable;
use omicron_common::disk::DiskIdentity;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::info;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) fn new() -> (RawDisksSender, watch::Receiver<IdMap<RawDiskWithId>>) {
    let (tx, rx) = watch::channel(IdMap::default());
    (RawDisksSender(tx), rx)
}

#[derive(Debug, Clone)]
pub struct RawDisksSender(watch::Sender<IdMap<RawDiskWithId>>);

impl RawDisksSender {
    /// Set the complete set of raw disks visible to sled-agent.
    pub fn set_raw_disks<I>(&self, raw_disks: I, log: &Logger)
    where
        I: Iterator<Item = RawDisk>,
    {
        let mut new_disks =
            raw_disks.map(From::from).collect::<IdMap<RawDiskWithId>>();
        self.0.send_if_modified(|disks| {
            // We can't just set `*disks = new_disks` here because we may have
            // disks that shouldn't be removed even if they're not present in
            // `new_disks`; check for that first.
            for old_disk in disks.iter() {
                if !new_disks.contains_key(&old_disk.identity)
                    && !can_remove_disk(old_disk, log)
                {
                    new_disks.insert(old_disk.clone());
                }
            }

            if *disks == new_disks {
                false
            } else {
                *disks = new_disks;
                true
            }
        });
    }

    /// Add or update the properties of a raw disk visible to sled-agent.
    pub fn add_or_update_raw_disk(&self, disk: RawDisk, log: &Logger) -> bool {
        let disk = RawDiskWithId::from(disk);
        self.0.send_if_modified(|disks| {
            match disks.entry(Arc::clone(&disk.identity)) {
                Entry::Vacant(entry) => {
                    info!(
                        log, "Adding new raw disk";
                        "identity" => ?disk.identity,
                    );
                    entry.insert(disk);
                    true
                }
                Entry::Occupied(mut entry) => {
                    if *entry.get() == disk {
                        false
                    } else {
                        info!(
                            log, "Updating raw disk";
                            "old" => ?entry.get().disk,
                            "new" => ?disk.disk,
                        );
                        entry.insert(disk);
                        true
                    }
                }
            }
        })
    }

    /// Remove a raw disk that is no longer visible to sled-agent.
    pub fn remove_raw_disk(
        &self,
        identity: &DiskIdentity,
        log: &Logger,
    ) -> bool {
        self.0.send_if_modified(|disks| {
            let Some(disk) = disks.get(identity) else {
                info!(
                    log, "Ignoring request to remove nonexistent disk";
                    "identity" => ?identity,
                );
                return false;
            };

            if !can_remove_disk(disk, log) {
                return false;
            }

            info!(log, "Removing disk"; "identity" => ?identity);
            disks.remove(identity);
            true
        })
    }
}

// Synthetic disks added by sled-agent on startup in test/dev environments are
// never added again; refuse to remove them.
fn can_remove_disk(disk: &RawDiskWithId, log: &Logger) -> bool {
    if disk.is_synthetic() {
        // Synthetic disks are only added once; don't remove them.
        info!(
            log, "Not removing synthetic disk";
            "identity" => ?disk.identity,
        );
        false
    } else {
        true
    }
}

// Adapter to store `RawDisk` in an `IdMap` with cheap key cloning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawDiskWithId {
    identity: Arc<DiskIdentity>,
    disk: RawDisk,
}

impl IdMappable for RawDiskWithId {
    type Id = Arc<DiskIdentity>;

    fn id(&self) -> Self::Id {
        Arc::clone(&self.identity)
    }
}

impl From<RawDisk> for RawDiskWithId {
    fn from(disk: RawDisk) -> Self {
        Self { identity: Arc::new(disk.identity().clone()), disk }
    }
}

impl Deref for RawDiskWithId {
    type Target = RawDisk;

    fn deref(&self) -> &Self::Target {
        &self.disk
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use omicron_common::disk::DiskVariant;
    use omicron_test_utils::dev;
    use proptest::collection::btree_map;
    use proptest::prelude::*;
    use proptest::sample::Index;
    use proptest::sample::size_range;
    use sled_hardware::DiskFirmware;
    use sled_hardware::UnparsedDisk;
    use sled_storage::disk::RawSyntheticDisk;
    use std::collections::BTreeMap;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;

    #[derive(Debug, Arbitrary)]
    enum ArbitraryDiskKind {
        Real,
        Synthetic,
    }

    #[derive(Debug, Arbitrary)]
    struct ArbitraryRawDisks {
        #[strategy(
            btree_map(any::<String>(), any::<ArbitraryDiskKind>(), 5..50)
        )]
        by_serial: BTreeMap<String, ArbitraryDiskKind>,
    }

    impl ArbitraryRawDisks {
        fn into_all_disks(self) -> Vec<RawDisk> {
            self.by_serial
                .into_iter()
                .map(|(serial, kind)| {
                    let identity = DiskIdentity {
                        vendor: "proptest-vendor".to_string(),
                        model: "proptest-model".to_string(),
                        serial,
                    };

                    // None of these properties matter for the purposes of
                    // `RawDisksSender`.
                    let path =
                        Utf8PathBuf::from("/tmp/raw-disks-proptest/bogus");
                    let slot = 0;
                    let is_boot_disk = false;
                    let variant = DiskVariant::U2;
                    let firmware =
                        DiskFirmware::new(0, None, false, 255, Vec::new());

                    match kind {
                        ArbitraryDiskKind::Real => {
                            RawDisk::Real(UnparsedDisk::new(
                                path.clone(),
                                Some(path),
                                slot,
                                variant,
                                identity,
                                is_boot_disk,
                                firmware,
                            ))
                        }
                        ArbitraryDiskKind::Synthetic => {
                            RawDisk::Synthetic(RawSyntheticDisk {
                                path,
                                identity,
                                variant,
                                slot,
                                firmware,
                            })
                        }
                    }
                })
                .collect()
        }
    }

    #[derive(Debug)]
    struct DiskState {
        present: bool,
        firmware_active_slot: u8,
    }

    #[derive(Debug, Arbitrary)]
    enum Operation {
        Add(Index),
        Update(Index),
        Remove(Index),
        Set { start: Index, num: usize },
    }

    fn increment_active_slot(firmware: &mut DiskFirmware) {
        let new = DiskFirmware::new(
            firmware.active_slot().wrapping_add(1),
            firmware.next_active_slot(),
            firmware.slot1_read_only(),
            firmware.number_of_slots(),
            firmware.slots().to_vec(),
        );
        *firmware = new;
    }

    impl Operation {
        // Apply the operation to both the in-memory `states` and the "real"
        // `disks`, and forward the operation into `tx`. Returns `true` if we
        // expect the operation to have produced a `changed()` notification to
        // the receivers of `tx`.
        fn apply(
            &self,
            states: &mut [DiskState],
            disks: &mut [RawDisk],
            tx: &RawDisksSender,
            log: &Logger,
        ) -> bool {
            assert_eq!(states.len(), disks.len());
            match self {
                Operation::Add(index) => {
                    let index = index.index(states.len());
                    eprintln!("adding disk {index}");
                    let was_present = states[index].present;
                    tx.add_or_update_raw_disk(disks[index].clone(), log);
                    states[index].present = true;
                    !was_present
                }
                Operation::Update(index) => {
                    let index = index.index(states.len());
                    eprintln!("updating disk {index}");
                    let new_firmware_active_slot =
                        states[index].firmware_active_slot.wrapping_add(1);
                    states[index].firmware_active_slot =
                        new_firmware_active_slot;
                    increment_active_slot(disks[index].firmware_mut());
                    tx.add_or_update_raw_disk(disks[index].clone(), log);
                    states[index].present = true;
                    true
                }
                Operation::Remove(index) => {
                    let index = index.index(states.len());
                    eprintln!("removing disk {index}");
                    let was_present = states[index].present;
                    tx.remove_raw_disk(disks[index].identity(), log);
                    // Synthetic disks should never be removed
                    if disks[index].is_synthetic() {
                        false
                    } else {
                        states[index].present = false;
                        was_present
                    }
                }
                Operation::Set { start, num } => {
                    let start = start.index(states.len());
                    let end =
                        usize::min(start.saturating_add(*num), states.len());
                    eprintln!("setting contents to disks {start}..{end}");
                    let mut any_changed = false;

                    // Mark any disks outside our range as being removed (unless
                    // they're synthetic!).
                    for before in 0..start {
                        if !disks[before].is_synthetic() {
                            any_changed = any_changed || states[before].present;
                            states[before].present = false;
                        }
                    }
                    for after in end..states.len() {
                        if !disks[after].is_synthetic() {
                            any_changed = any_changed || states[after].present;
                            states[after].present = false;
                        }
                    }

                    // Mark disks inside our range as being present.
                    for included in start..end {
                        any_changed = any_changed || !states[included].present;
                        states[included].present = true;
                    }

                    tx.set_raw_disks(disks[start..end].iter().cloned(), log);
                    any_changed
                }
            }
        }
    }

    #[proptest]
    fn proptest_raw_disk_changes(
        disks: ArbitraryRawDisks,
        #[any(size_range(10..50).lift())] ops: Vec<Operation>,
    ) {
        let logctx = dev::test_setup_log("proptest_raw_disk_changes");

        let mut all_disks = disks.into_all_disks();
        let mut states = all_disks
            .iter()
            .map(|d| DiskState {
                present: false,
                firmware_active_slot: d.firmware().active_slot(),
            })
            .collect::<Vec<_>>();

        let (tx, mut rx) = super::new();
        rx.mark_unchanged();

        for op in ops {
            let expect_changes =
                op.apply(&mut states, &mut all_disks, &tx, &logctx.log);

            // Confirm that RawDisksSender notified receivers if any changes
            // were made.
            assert_eq!(rx.has_changed().expect("channel open"), expect_changes);

            // After this operation, check that the disk is either present or
            // not (the thing changed by our add/remove/set operations) and has
            // the expected firmware slot (the thing changed by our update
            // operation).
            let current = rx.borrow_and_update();
            for i in 0..states.len() {
                let disk = &all_disks[i];
                if states[i].present {
                    let contained =
                        current.get(disk.identity()).expect("disk is present");
                    assert_eq!(
                        disk.firmware().active_slot(),
                        contained.firmware().active_slot()
                    );
                } else {
                    assert!(!current.contains_key(disk.identity()));
                }
            }
        }
    }
}
