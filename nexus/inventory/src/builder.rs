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
