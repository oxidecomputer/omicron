// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for building inventory [`Collection`] dynamically
//!
//! This separates the concerns of _collection_ (literally just fetching data
//! from sources like MGS) from assembling a representation of what was
//! collected.

use anyhow::anyhow;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use gateway_client::types::RotSlot;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::Caboose;
use nexus_types::inventory::CabooseFound;
use nexus_types::inventory::Collection;
use nexus_types::inventory::RotState;
use nexus_types::inventory::ServiceProcessor;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use strum::EnumIter;
use uuid::Uuid;

/// Identifies one of a service processor's two firmware slots
#[derive(Debug, Clone, Copy, EnumIter)]
pub enum SpSlot {
    Slot0,
    Slot1,
}

/// Build an inventory [`Collection`]
///
/// This interface is oriented around the interfaces used by an actual
/// collector.  Where possible, it accepts types directly provided by the data
/// sources (e.g., `gateway_client`).
#[derive(Debug)]
pub struct CollectionBuilder {
    // For field documentation, see the corresponding fields in `Collection`.
    errors: Vec<anyhow::Error>,
    time_started: DateTime<Utc>,
    collector: String,
    baseboards: BTreeSet<Arc<BaseboardId>>,
    cabooses: BTreeSet<Arc<Caboose>>,
    sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
    rots: BTreeMap<Arc<BaseboardId>, RotState>,
}

impl CollectionBuilder {
    /// Start building a new `Collection`
    ///
    /// `collector` is an arbitrary string describing the agent that collected
    /// this data.  It's generally a Nexus instance uuid but it can be anything.
    /// It's just for debugging.
    pub fn new(collector: &str) -> Self {
        CollectionBuilder {
            errors: vec![],
            time_started: Utc::now(),
            collector: collector.to_owned(),
            baseboards: BTreeSet::new(),
            cabooses: BTreeSet::new(),
            sps: BTreeMap::new(),
            rots: BTreeMap::new(),
        }
    }

    /// Assemble a complete `Collection` representation
    pub fn build(self) -> Collection {
        Collection {
            id: Uuid::new_v4(),
            errors: self.errors,
            time_started: self.time_started,
            time_done: Utc::now(),
            collector: self.collector,
            baseboards: self.baseboards,
            cabooses: self.cabooses,
            sps: self.sps,
            rots: self.rots,
        }
    }

    /// Record service processor state `sp_state` reported by MGS
    ///
    /// `sp_type` and `slot` identify which SP this was.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_sp_state(
        &mut self,
        source: &str,
        sp_type: SpType,
        slot: u32,
        sp_state: SpState,
    ) -> Option<Arc<BaseboardId>> {
        // Much ado about very little: MGS reports that "slot" is a u32, though
        // in practice this seems very unlikely to be bigger than a u8.  (How
        // many slots can there be within one rack?)  The database only supports
        // signed integers, so if we assumed this really could span the range of
        // a u32, we'd need to store it in an i64.  Instead, assume here that we
        // can stick it into a u16 (which still seems generous).  This will
        // allow us to store it into an Int32 in the database.
        let Ok(sp_slot) = u16::try_from(slot) else {
            self.found_error(anyhow!(
                "MGS {:?}: SP {:?} slot {}: slot number did not fit into u16",
                source,
                sp_type,
                slot
            ));
            return None;
        };

        // Normalize the baseboard id: i.e., if we've seen this baseboard
        // before, use the same baseboard id record.  Otherwise, make a new one.
        let baseboard = Self::normalize_item(
            &mut self.baseboards,
            BaseboardId {
                serial_number: sp_state.serial_number,
                part_number: sp_state.model,
            },
        );

        // Separate the SP state into the SP-specific state and the RoT state,
        // if any.
        let now = Utc::now();
        let _ = self.sps.entry(baseboard.clone()).or_insert_with(|| {
            ServiceProcessor {
                time_collected: now,
                source: source.to_owned(),

                sp_type,
                sp_slot,

                baseboard_revision: sp_state.revision,
                hubris_archive: sp_state.hubris_archive_id,
                power_state: sp_state.power_state,

                slot0_caboose: None,
                slot1_caboose: None,
            }
        });

        match sp_state.rot {
            gateway_client::types::RotState::Enabled {
                active,
                pending_persistent_boot_preference,
                persistent_boot_preference,
                slot_a_sha3_256_digest,
                slot_b_sha3_256_digest,
                transient_boot_preference,
            } => {
                let _ =
                    self.rots.entry(baseboard.clone()).or_insert_with(|| {
                        RotState {
                            time_collected: now,
                            source: source.to_owned(),
                            active_slot: active,
                            persistent_boot_preference,
                            pending_persistent_boot_preference,
                            transient_boot_preference,
                            slot_a_sha3_256_digest,
                            slot_b_sha3_256_digest,
                            slot_a_caboose: None,
                            slot_b_caboose: None,
                        }
                    });
            }
            gateway_client::types::RotState::CommunicationFailed {
                message,
            } => {
                self.found_error(anyhow!(
                    "MGS {:?}: reading RoT state for {:?}: {}",
                    source,
                    baseboard,
                    message
                ));
            }
        }

        Some(baseboard)
    }

    /// Returns true if we already found the SP caboose for slot `slot` for
    /// baseboard `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn found_sp_caboose_already(
        &self,
        baseboard: &BaseboardId,
        slot: SpSlot,
    ) -> bool {
        self.sps
            .get(baseboard)
            .map(|sp| {
                let sp_slot = match slot {
                    SpSlot::Slot0 => &sp.slot0_caboose,
                    SpSlot::Slot1 => &sp.slot1_caboose,
                };
                sp_slot.is_some()
            })
            .unwrap_or(false)
    }

    /// Returns true if we already found the RoT caboose for slot `slot` for
    /// baseboard `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn found_rot_caboose_already(
        &self,
        baseboard: &BaseboardId,
        slot: RotSlot,
    ) -> bool {
        self.rots
            .get(baseboard)
            .map(|rot| {
                let rot_slot = match slot {
                    RotSlot::A => &rot.slot_a_caboose,
                    RotSlot::B => &rot.slot_b_caboose,
                };
                rot_slot.is_some()
            })
            .unwrap_or(false)
    }

    /// Record the given caboose information found for the given baseboard
    ///
    /// The baseboard must previously have been reported using
    /// `found_sp_state()`.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_sp_caboose(
        &mut self,
        baseboard: &BaseboardId,
        slot: SpSlot,
        source: &str,
        caboose: SpComponentCaboose,
    ) -> Result<(), anyhow::Error> {
        // Normalize the caboose contents: i.e., if we've seen this exact caboose
        // contents before, use the same record from before.  Otherwise, make a
        // new one.
        let sw_caboose =
            Self::normalize_item(&mut self.cabooses, Caboose::from(caboose));

        // Find the SP.
        let sp = self.sps.get_mut(baseboard).ok_or_else(|| {
            anyhow!(
                "reporting caboose for unknown baseboard: {:?} ({:?})",
                baseboard,
                sw_caboose
            )
        })?;
        let sp_slot = match slot {
            SpSlot::Slot0 => &mut sp.slot0_caboose,
            SpSlot::Slot1 => &mut sp.slot1_caboose,
        };
        Self::record_caboose(sp_slot, source, sw_caboose)
            .context(format!("baseboard {:?} SP caboose {:?}", baseboard, slot))
    }

    /// Record the given root of trust caboose information found for the given
    /// baseboard
    ///
    /// The baseboard must previously have been reported using
    /// `found_sp_state()`.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_rot_caboose(
        &mut self,
        baseboard: &BaseboardId,
        slot: RotSlot,
        source: &str,
        caboose: SpComponentCaboose,
    ) -> Result<(), anyhow::Error> {
        // Normalize the caboose contents: i.e., if we've seen this exact caboose
        // contents before, use the same record from before.  Otherwise, make a
        // new one.
        let sw_caboose =
            Self::normalize_item(&mut self.cabooses, Caboose::from(caboose));

        // Find the RoT state.  Note that it's possible that we _do_ have
        // caboose information for an RoT that we have no information about
        // because the SP couldn't talk to the RoT when we asked for its state,
        // but was able to do so when we got the caboose.  This seems unlikely.
        let rot = self.rots.get_mut(baseboard).ok_or_else(|| {
            anyhow!(
                "reporting caboose for unknown baseboard: {:?} ({:?})",
                baseboard,
                sw_caboose
            )
        })?;
        let rot_slot = match slot {
            RotSlot::A => &mut rot.slot_a_caboose,
            RotSlot::B => &mut rot.slot_b_caboose,
        };
        Self::record_caboose(rot_slot, source, sw_caboose).context(format!(
            "baseboard {:?} RoT caboose {:?}",
            baseboard, slot
        ))
    }

    fn record_caboose(
        slot: &mut Option<Arc<CabooseFound>>,
        source: &str,
        sw_caboose: Arc<Caboose>,
    ) -> Result<(), anyhow::Error> {
        let old = slot.replace(Arc::new(CabooseFound {
            id: Uuid::new_v4(),
            time_collected: Utc::now(),
            source: source.to_owned(),
            caboose: sw_caboose.clone(),
        }));
        match old {
            None => Ok(()),
            Some(previous) if *previous.caboose == *sw_caboose => {
                Err(anyhow!("reported multiple times (same value)"))
            }
            Some(previous) => Err(anyhow!(
                "reported caboose multiple times (previously {:?}, \
                    now {:?})",
                previous,
                sw_caboose
            )),
        }
    }

    /// Helper function for normalizing items
    ///
    /// If `item` (or its equivalent) is not already in `items`, insert it.
    /// Either way, return the item from `items`.  (This will either be `item`
    /// itself or whatever was already in `items`.)
    fn normalize_item<T: Clone + Ord>(
        items: &mut BTreeSet<Arc<T>>,
        item: T,
    ) -> Arc<T> {
        match items.get(&item) {
            Some(found_item) => found_item.clone(),
            None => {
                let new_item = Arc::new(item);
                items.insert(new_item.clone());
                new_item
            }
        }
    }

    /// Record a collection error
    ///
    /// This is used for operational errors encountered during the collection
    /// process (e.g., a down MGS instance).  It's not intended for mis-uses of
    /// this API, which are conveyed instead through returned errors (and should
    /// probably cause the caller to stop collection altogether).
    pub fn found_error(&mut self, error: anyhow::Error) {
        self.errors.push(error);
    }
}

#[cfg(test)]
mod test {
    use super::CollectionBuilder;
    use crate::builder::SpSlot;
    use chrono::Utc;
    use gateway_client::types::PowerState;
    use gateway_client::types::RotSlot;
    use gateway_client::types::RotState;
    use gateway_client::types::SpComponentCaboose;
    use gateway_client::types::SpState;
    use gateway_client::types::SpType;
    use nexus_types::inventory::BaseboardId;
    use nexus_types::inventory::Caboose;
    use strum::IntoEnumIterator;

    // Verify the contents of an empty collection.
    #[test]
    fn test_empty() {
        let time_before = Utc::now();
        let builder = CollectionBuilder::new("test_empty");
        let collection = builder.build();
        let time_after = Utc::now();

        assert!(collection.errors.is_empty());
        assert!(time_before <= collection.time_started);
        assert!(collection.time_started <= collection.time_done);
        assert!(collection.time_done <= time_after);
        assert_eq!(collection.collector, "test_empty");
        assert!(collection.baseboards.is_empty());
        assert!(collection.cabooses.is_empty());
        assert!(collection.sps.is_empty());
        assert!(collection.rots.is_empty());
    }

    // Simple test of a single, fairly typical collection that contains just
    // about all kinds of valid data.  That includes exercising:
    //
    // - all three baseboard types (switch, sled, PSC)
    // - various valid values for all fields (sources, slot numbers, power
    //   states, baseboard revisions, cabooses, etc.)
    // - some empty slots
    // - some missing cabooses
    // - some cabooses common to multiple baseboards; others not
    // - serial number reused across different model numbers
    //
    // This test is admittedly pretty tedious and maybe not worthwhile but it's
    // a useful quick check.
    #[test]
    fn test_basic() {
        let time_before = Utc::now();
        let mut builder = CollectionBuilder::new("test_basic");

        // an ordinary, working sled
        let sled1_bb = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 0,
                    rot: RotState::Enabled {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: Some(String::from(
                            "slotAdigest1",
                        )),
                        slot_b_sha3_256_digest: Some(String::from(
                            "slotBdigest1",
                        )),
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();

        // another ordinary sled with different values for ordinary fields
        let sled2_bb = builder
            .found_sp_state(
                "fake MGS 2",
                SpType::Sled,
                4,
                SpState {
                    base_mac_address: [1; 6],
                    hubris_archive_id: String::from("hubris2"),
                    model: String::from("model2"),
                    power_state: PowerState::A2,
                    revision: 1,
                    rot: RotState::Enabled {
                        active: RotSlot::B,
                        pending_persistent_boot_preference: Some(RotSlot::A),
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: Some(String::from(
                            "slotAdigest2",
                        )),
                        slot_b_sha3_256_digest: Some(String::from(
                            "slotBdigest2",
                        )),
                        transient_boot_preference: Some(RotSlot::B),
                    },
                    // same serial number, which is okay because it's a different
                    // model number
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();

        // a switch
        let switch1_bb = builder
            .found_sp_state(
                "fake MGS 2",
                SpType::Switch,
                0,
                SpState {
                    base_mac_address: [2; 6],
                    hubris_archive_id: String::from("hubris3"),
                    model: String::from("model3"),
                    power_state: PowerState::A1,
                    revision: 2,
                    rot: RotState::Enabled {
                        active: RotSlot::B,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: Some(String::from(
                            "slotAdigest3",
                        )),
                        slot_b_sha3_256_digest: Some(String::from(
                            "slotBdigest3",
                        )),
                        transient_boot_preference: None,
                    },
                    // same serial number, which is okay because it's a different
                    // model number
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();

        // a PSC
        let psc_bb = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Power,
                1,
                SpState {
                    base_mac_address: [3; 6],
                    hubris_archive_id: String::from("hubris4"),
                    model: String::from("model4"),
                    power_state: PowerState::A2,
                    revision: 3,
                    rot: RotState::Enabled {
                        active: RotSlot::B,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: Some(String::from(
                            "slotAdigest4",
                        )),
                        slot_b_sha3_256_digest: Some(String::from(
                            "slotBdigest4",
                        )),
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s2"),
                },
            )
            .unwrap();

        // a sled with no RoT state or other optional fields
        let sled3_bb = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                5,
                SpState {
                    base_mac_address: [4; 6],
                    hubris_archive_id: String::from("hubris5"),
                    model: String::from("model1"),
                    power_state: PowerState::A2,
                    revision: 1,
                    rot: RotState::CommunicationFailed {
                        message: String::from("test suite injected error"),
                    },
                    serial_number: String::from("s2"),
                },
            )
            .unwrap();

        // Report some cabooses.

        // We'll use the same cabooses for most of these components, although
        // that's not possible in a real system.  We deliberately construct a
        // new value each time to make sure the builder correctly normalizes it.
        let common_caboose_baseboards = [&sled1_bb, &sled2_bb, &switch1_bb];
        for bb in &common_caboose_baseboards {
            for slot in SpSlot::iter() {
                assert!(!builder.found_sp_caboose_already(bb, slot));
                let _ = builder
                    .found_sp_caboose(
                        bb,
                        slot,
                        "test suite",
                        SpComponentCaboose {
                            board: String::from("board1"),
                            git_commit: String::from("git_commit1"),
                            name: String::from("name1"),
                            version: String::from("version1"),
                        },
                    )
                    .unwrap();
                assert!(builder.found_sp_caboose_already(bb, slot));
            }

            for slot in RotSlot::iter() {
                assert!(!builder.found_rot_caboose_already(bb, slot));
                let _ = builder.found_rot_caboose(
                    bb,
                    slot,
                    "test suite",
                    SpComponentCaboose {
                        board: String::from("board1"),
                        git_commit: String::from("git_commit1"),
                        name: String::from("name1"),
                        version: String::from("version1"),
                    },
                );
                assert!(builder.found_rot_caboose_already(bb, slot));
            }
        }

        // For the PSC, use different cabooses for both slots of both the SP and
        // RoT, just to exercise that we correctly keep track of different
        // cabooses.
        let _ = builder
            .found_sp_caboose(
                &psc_bb,
                SpSlot::Slot0,
                "test suite",
                SpComponentCaboose {
                    board: String::from("psc_sp_0"),
                    git_commit: String::from("psc_sp_0"),
                    name: String::from("psc_sp_0"),
                    version: String::from("psc_sp_0"),
                },
            )
            .unwrap();
        let _ = builder
            .found_sp_caboose(
                &psc_bb,
                SpSlot::Slot1,
                "test suite",
                SpComponentCaboose {
                    board: String::from("psc_sp_1"),
                    git_commit: String::from("psc_sp_1"),
                    name: String::from("psc_sp_1"),
                    version: String::from("psc_sp_1"),
                },
            )
            .unwrap();
        let _ = builder
            .found_rot_caboose(
                &psc_bb,
                RotSlot::A,
                "test suite",
                SpComponentCaboose {
                    board: String::from("psc_rot_a"),
                    git_commit: String::from("psc_rot_a"),
                    name: String::from("psc_rot_a"),
                    version: String::from("psc_rot_a"),
                },
            )
            .unwrap();
        let _ = builder
            .found_rot_caboose(
                &psc_bb,
                RotSlot::B,
                "test suite",
                SpComponentCaboose {
                    board: String::from("psc_rot_b"),
                    git_commit: String::from("psc_rot_b"),
                    name: String::from("psc_rot_b"),
                    version: String::from("psc_rot_b"),
                },
            )
            .unwrap();

        // We deliberately provide no cabooses for sled3.

        // Finish the collection and verify the basics.
        let collection = builder.build();
        let time_after = Utc::now();
        println!("{:#?}", collection);
        assert!(time_before <= collection.time_started);
        assert!(collection.time_started <= collection.time_done);
        assert!(collection.time_done <= time_after);
        assert_eq!(collection.collector, "test_basic");

        // Verify the one error that ought to have been produced for the SP with
        // no RoT information.
        assert_eq!(
            collection.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            ["MGS \"fake MGS 1\": reading RoT state for BaseboardId \
                { part_number: \"model1\", serial_number: \"s2\" }: test suite \
                injected error"]
        );

        // Verify the baseboard ids found.
        let expected_baseboards =
            &[&sled1_bb, &sled2_bb, &sled3_bb, &switch1_bb, &psc_bb];
        for bb in expected_baseboards {
            assert!(collection.baseboards.contains(*bb));
        }
        assert_eq!(collection.baseboards.len(), expected_baseboards.len());

        // Verify the stuff that's easy to verify for all SPs: timestamps.
        assert_eq!(collection.sps.len(), collection.baseboards.len());
        for (bb, sp) in collection.sps.iter() {
            assert!(collection.time_started <= sp.time_collected);
            assert!(sp.time_collected <= collection.time_done);

            if let Some(rot) = collection.rots.get(bb) {
                assert_eq!(rot.source, sp.source);
                assert_eq!(rot.time_collected, sp.time_collected);
            }

            for c in
                [&sp.slot0_caboose, &sp.slot1_caboose].into_iter().flatten()
            {
                assert!(collection.time_started <= c.time_collected);
                assert!(c.time_collected <= collection.time_done);
            }
        }

        // Verify the common caboose.
        let common_caboose = Caboose {
            board: String::from("board1"),
            git_commit: String::from("git_commit1"),
            name: String::from("name1"),
            version: String::from("version1"),
        };
        for bb in &common_caboose_baseboards {
            let sp = collection.sps.get(*bb).unwrap();
            let c0 = sp.slot0_caboose.as_ref().unwrap();
            let c1 = sp.slot0_caboose.as_ref().unwrap();
            assert_eq!(c0.source, "test suite");
            assert_eq!(*c0.caboose, common_caboose);
            assert_eq!(c1.source, "test suite");
            assert_eq!(*c1.caboose, common_caboose);

            let rot = collection.rots.get(*bb).unwrap();
            let c0 = rot.slot_a_caboose.as_ref().unwrap();
            let c1 = rot.slot_b_caboose.as_ref().unwrap();
            assert_eq!(c0.source, "test suite");
            assert_eq!(*c0.caboose, common_caboose);
            assert_eq!(c1.source, "test suite");
            assert_eq!(*c1.caboose, common_caboose);
        }
        assert!(collection.cabooses.contains(&common_caboose));

        // Verify the specific, different data for the healthy SPs and RoTs that
        // we reported.
        // sled1
        let sp = collection.sps.get(&sled1_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 1");
        assert_eq!(sp.sp_type, SpType::Sled);
        assert_eq!(sp.sp_slot, 3);
        assert_eq!(sp.baseboard_revision, 0);
        assert_eq!(sp.hubris_archive, "hubris1");
        assert_eq!(sp.power_state, PowerState::A0);
        let rot = collection.rots.get(&sled1_bb).unwrap();
        assert_eq!(rot.active_slot, RotSlot::A);
        assert_eq!(rot.pending_persistent_boot_preference, None);
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest1"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest1"
        );
        assert_eq!(rot.transient_boot_preference, None);

        // sled2
        let sp = collection.sps.get(&sled2_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 2");
        assert_eq!(sp.sp_type, SpType::Sled);
        assert_eq!(sp.sp_slot, 4);
        assert_eq!(sp.baseboard_revision, 1);
        assert_eq!(sp.hubris_archive, "hubris2");
        assert_eq!(sp.power_state, PowerState::A2);
        let rot = collection.rots.get(&sled2_bb).unwrap();
        assert_eq!(rot.active_slot, RotSlot::B);
        assert_eq!(rot.pending_persistent_boot_preference, Some(RotSlot::A));
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest2"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest2"
        );
        assert_eq!(rot.transient_boot_preference, Some(RotSlot::B));

        // switch
        let sp = collection.sps.get(&switch1_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 2");
        assert_eq!(sp.sp_type, SpType::Switch);
        assert_eq!(sp.sp_slot, 0);
        assert_eq!(sp.baseboard_revision, 2);
        assert_eq!(sp.hubris_archive, "hubris3");
        assert_eq!(sp.power_state, PowerState::A1);
        let rot = collection.rots.get(&switch1_bb).unwrap();
        assert_eq!(rot.active_slot, RotSlot::B);
        assert_eq!(rot.pending_persistent_boot_preference, None);
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest3"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest3"
        );
        assert_eq!(rot.transient_boot_preference, None);

        // PSC
        let sp = collection.sps.get(&psc_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 1");
        assert_eq!(sp.sp_type, SpType::Power);
        assert_eq!(sp.sp_slot, 1);
        assert_eq!(sp.baseboard_revision, 3);
        assert_eq!(sp.hubris_archive, "hubris4");
        assert_eq!(sp.power_state, PowerState::A2);
        let rot = collection.rots.get(&psc_bb).unwrap();
        assert_eq!(rot.active_slot, RotSlot::B);
        assert_eq!(rot.pending_persistent_boot_preference, None);
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest4"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest4"
        );
        assert_eq!(rot.transient_boot_preference, None);

        // The PSC has four different cabooses!
        let c = &sp.slot0_caboose.as_ref().unwrap().caboose;
        assert_eq!(c.board, "psc_sp_0");
        assert!(collection.cabooses.contains(c));
        let c = &sp.slot1_caboose.as_ref().unwrap().caboose;
        assert!(collection.cabooses.contains(c));
        assert_eq!(c.board, "psc_sp_1");
        let c = &rot.slot_a_caboose.as_ref().unwrap().caboose;
        assert!(collection.cabooses.contains(c));
        assert_eq!(c.board, "psc_rot_a");
        let c = &rot.slot_b_caboose.as_ref().unwrap().caboose;
        assert!(collection.cabooses.contains(c));
        assert_eq!(c.board, "psc_rot_b");

        // Verify the reported SP state for sled3, which did not have a healthy
        // RoT, nor any cabooses.
        let sp = collection.sps.get(&sled3_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 1");
        assert_eq!(sp.sp_type, SpType::Sled);
        assert_eq!(sp.sp_slot, 5);
        assert_eq!(sp.baseboard_revision, 1);
        assert_eq!(sp.hubris_archive, "hubris5");
        assert_eq!(sp.power_state, PowerState::A2);
        assert_eq!(sp.slot0_caboose, None);
        assert_eq!(sp.slot1_caboose, None);
        assert!(!collection.rots.contains_key(&sled3_bb));

        // There shouldn't be any other RoTs.
        assert_eq!(collection.sps.len(), collection.rots.len() + 1);

        // There should be five cabooses: the four used for the PSC (see above),
        // plus the common one.
        assert_eq!(collection.cabooses.len(), 5);
    }

    // Exercises all the failure cases that shouldn't happen in real systems.
    // Despite all of these failures, we should get a valid collection at the
    // end.
    #[test]
    fn test_problems() {
        let mut builder = CollectionBuilder::new("test_problems");

        let sled1_bb = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 0,
                    rot: RotState::Enabled {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: None,
                        slot_b_sha3_256_digest: None,
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();

        // report the same SP again with the same contents
        let sled1_bb_dup = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 0,
                    rot: RotState::Enabled {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: None,
                        slot_b_sha3_256_digest: None,
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();
        assert_eq!(sled1_bb, sled1_bb_dup);

        // report the same SP again with different contents
        let sled1_bb_dup = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 1,
                    rot: RotState::Enabled {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: None,
                        slot_b_sha3_256_digest: None,
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();
        assert_eq!(sled1_bb, sled1_bb_dup);

        // report an SP with an impossible slot number
        let sled2_sp = builder.found_sp_state(
            "fake MGS 1",
            SpType::Sled,
            u32::from(u16::MAX) + 1,
            SpState {
                base_mac_address: [0; 6],
                hubris_archive_id: String::from("hubris1"),
                model: String::from("model1"),
                power_state: PowerState::A0,
                revision: 1,
                rot: RotState::Enabled {
                    active: RotSlot::A,
                    pending_persistent_boot_preference: None,
                    persistent_boot_preference: RotSlot::A,
                    slot_a_sha3_256_digest: None,
                    slot_b_sha3_256_digest: None,
                    transient_boot_preference: None,
                },
                serial_number: String::from("s2"),
            },
        );
        assert_eq!(sled2_sp, None);

        // report SP caboose for an unknown baseboard
        let bogus_baseboard = BaseboardId {
            part_number: String::from("p1"),
            serial_number: String::from("bogus"),
        };
        let caboose1 = SpComponentCaboose {
            board: String::from("board1"),
            git_commit: String::from("git_commit1"),
            name: String::from("name1"),
            version: String::from("version1"),
        };
        assert!(
            !builder.found_sp_caboose_already(&bogus_baseboard, SpSlot::Slot0)
        );
        let error = builder
            .found_sp_caboose(
                &bogus_baseboard,
                SpSlot::Slot0,
                "dummy",
                caboose1.clone(),
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "reporting caboose for unknown baseboard: \
            BaseboardId { part_number: \"p1\", serial_number: \"bogus\" } \
            (Caboose { board: \"board1\", git_commit: \"git_commit1\", \
            name: \"name1\", version: \"version1\" })"
        );
        assert!(
            !builder.found_sp_caboose_already(&bogus_baseboard, SpSlot::Slot0)
        );

        // report RoT caboose for an unknown baseboard
        let error2 = builder
            .found_rot_caboose(
                &bogus_baseboard,
                RotSlot::A,
                "dummy",
                caboose1.clone(),
            )
            .unwrap_err();
        assert_eq!(error.to_string(), error2.to_string(),);

        // report the same caboose twice with the same contents
        let _ = builder
            .found_sp_caboose(
                &sled1_bb,
                SpSlot::Slot0,
                "dummy",
                caboose1.clone(),
            )
            .unwrap();
        let error = builder
            .found_sp_caboose(
                &sled1_bb,
                SpSlot::Slot0,
                "dummy",
                caboose1.clone(),
            )
            .unwrap_err();
        assert_eq!(
            format!("{:#}", error),
            "baseboard BaseboardId { part_number: \"model1\", \
            serial_number: \"s1\" } SP caboose Slot0: reported multiple \
            times (same value)"
        );
        // report the same caboose again with different contents
        let error = builder
            .found_sp_caboose(
                &sled1_bb,
                SpSlot::Slot0,
                "dummy",
                SpComponentCaboose {
                    board: String::from("board2"),
                    git_commit: String::from("git_commit2"),
                    name: String::from("name2"),
                    version: String::from("version2"),
                },
            )
            .unwrap_err();
        let message = format!("{:#}", error);
        println!("found error: {}", message);
        assert!(message.contains(
            "SP caboose Slot0: reported caboose multiple times (previously"
        ));
        assert!(message.contains(", now "));

        // We should still get a valid collection.
        let collection = builder.build();
        println!("{:#?}", collection);
        assert_eq!(collection.collector, "test_problems");

        // We should still have the one sled and its SP slot0 caboose.
        assert!(collection.baseboards.contains(&sled1_bb));
        let sp = collection.sps.get(&sled1_bb).unwrap();
        let caboose = sp.slot0_caboose.as_ref().unwrap();
        assert_eq!(caboose.caboose.board, "board2");
        assert!(collection.cabooses.contains(&caboose.caboose));
        assert_eq!(sp.slot1_caboose, None);
        let rot = collection.rots.get(&sled1_bb).unwrap();
        assert_eq!(rot.slot_a_caboose, None);
        assert_eq!(rot.slot_b_caboose, None);

        // We should see an error.
        assert_eq!(
            collection
                .errors
                .iter()
                .map(|e| format!("{:#}", e))
                .collect::<Vec<_>>(),
            vec![
                "MGS \"fake MGS 1\": SP Sled slot 65536: \
                slot number did not fit into u16"
            ]
        );
    }
}
