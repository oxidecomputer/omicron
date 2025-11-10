// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Provides thin wrappers around a watch channel managing the set of
//! [`RawDisk`]s sled-agent is aware of.

use iddqd::IdOrdMap;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use omicron_common::disk::DiskIdentity;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::info;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) fn new() -> (RawDisksSender, RawDisksReceiver) {
    let (tx, rx) = watch::channel(Arc::default());
    (RawDisksSender(tx), RawDisksReceiver(rx))
}

#[derive(Debug, Clone)]
pub(crate) struct RawDisksReceiver(
    pub(crate) watch::Receiver<Arc<IdOrdMap<RawDisk>>>,
);

impl Deref for RawDisksReceiver {
    type Target = watch::Receiver<Arc<IdOrdMap<RawDisk>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RawDisksReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone)]
pub struct RawDisksSender(watch::Sender<Arc<IdOrdMap<RawDisk>>>);

impl RawDisksSender {
    /// Set the complete set of raw disks visible to sled-agent.
    pub fn set_raw_disks<I>(&self, raw_disks: I, log: &Logger)
    where
        I: Iterator<Item = RawDisk>,
    {
        let mut new_disks = raw_disks.collect::<IdOrdMap<RawDisk>>();
        self.0.send_if_modified(|disks| {
            // We can't just set `*disks = new_disks` here because we may have
            // disks that shouldn't be removed even if they're not present in
            // `new_disks`; check for that first.
            for old_disk in disks.iter() {
                if !new_disks.contains_key(old_disk.identity())
                    && !can_remove_disk(old_disk, log)
                {
                    new_disks.insert_overwrite(old_disk.clone());
                }
            }

            if **disks == new_disks {
                false
            } else {
                *disks = Arc::new(new_disks);
                true
            }
        });
    }

    /// Add or update the properties of a raw disk visible to sled-agent.
    pub fn add_or_update_raw_disk(&self, disk: RawDisk, log: &Logger) -> bool {
        self.0.send_if_modified(|disks| {
            match disks.get(disk.identity()) {
                Some(existing) if *existing == disk => {
                    return false;
                }
                Some(existing) => {
                    info!(
                        log, "Updating raw disk";
                        "old" => ?existing,
                        "new" => ?disk,
                    );
                }
                None => {
                    info!(
                        log, "Adding new raw disk";
                        "disk" => ?disk,
                    );
                }
            }

            Arc::make_mut(disks).insert_overwrite(disk);
            true
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
            Arc::make_mut(disks).remove(identity);
            true
        })
    }

    pub(crate) fn to_inventory(&self) -> Vec<InventoryDisk> {
        self.0
            .borrow()
            .iter()
            .map(|disk| {
                let firmware = disk.firmware();
                InventoryDisk {
                    identity: disk.identity().clone(),
                    variant: disk.variant(),
                    slot: disk.slot(),
                    active_firmware_slot: firmware.active_slot(),
                    next_active_firmware_slot: firmware.next_active_slot(),
                    number_of_firmware_slots: firmware.number_of_slots(),
                    slot1_is_read_only: firmware.slot1_read_only(),
                    slot_firmware_versions: firmware.slots().to_vec(),
                }
            })
            .collect()
    }
}

// Synthetic disks added by sled-agent on startup in test/dev environments are
// never added again; refuse to remove them.
fn can_remove_disk(disk: &RawDisk, log: &Logger) -> bool {
    if disk.is_synthetic() {
        // Synthetic disks are only added once; don't remove them.
        info!(
            log, "Not removing synthetic disk";
            "identity" => ?disk.identity(),
        );
        false
    } else {
        true
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
        // Call `RawDisksSender::add_or_update_raw_disk` with the disk at
        // the given index
        Add(Index),
        // Change the active firmware slot of the disk at the given index, then
        // pass it to `RawDisksSender::add_or_update_raw_disk`
        Update(Index),
        // Call `RawDisksSender::remove_raw_disk` with the disk at the given
        // index
        Remove(Index),
        // Call `RawDisksSender::set_raw_disks` using the set of disks in the
        // range `[start, start + num)`
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
                    let contained = current
                        .get(disk.identity())
                        .expect("disk should be present");
                    assert_eq!(
                        disk.firmware().active_slot(),
                        contained.firmware().active_slot()
                    );
                } else {
                    assert!(!current.contains_key(disk.identity()));
                }
            }
        }

        logctx.cleanup_successful();
    }
}
