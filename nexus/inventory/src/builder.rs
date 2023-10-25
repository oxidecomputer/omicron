// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for building inventory [`Collection`] dynamically
//!
//! This separates the concerns of _collection_ (literally just fetching data
//! from sources like MGS) from assembling a representation of what was
//! collected.

use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::Caboose;
use nexus_types::inventory::CabooseFound;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use nexus_types::inventory::RotState;
use nexus_types::inventory::ServiceProcessor;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

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
    cabooses_found:
        BTreeMap<CabooseWhich, BTreeMap<Arc<BaseboardId>, CabooseFound>>,
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
            cabooses_found: BTreeMap::new(),
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
            cabooses_found: self.cabooses_found,
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

    /// Returns true if we already found the caboose for `which` for baseboard
    /// `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn sp_found_caboose_already(
        &self,
        baseboard: &BaseboardId,
        which: CabooseWhich,
    ) -> bool {
        self.cabooses_found
            .get(&which)
            .map(|map| map.contains_key(baseboard))
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
        which: CabooseWhich,
        source: &str,
        caboose: SpComponentCaboose,
    ) -> Result<(), anyhow::Error> {
        // Normalize the caboose contents: i.e., if we've seen this exact caboose
        // contents before, use the same record from before.  Otherwise, make a
        // new one.
        let sw_caboose =
            Self::normalize_item(&mut self.cabooses, Caboose::from(caboose));
        let (baseboard, _) =
            self.sps.get_key_value(baseboard).ok_or_else(|| {
                anyhow!(
                    "reporting caboose for unknown baseboard: {:?} ({:?})",
                    baseboard,
                    sw_caboose
                )
            })?;
        let by_id =
            self.cabooses_found.entry(which).or_insert_with(|| BTreeMap::new());
        if let Some(previous) = by_id.insert(
            baseboard.clone(),
            CabooseFound {
                time_collected: Utc::now(),
                source: source.to_owned(),
                caboose: sw_caboose.clone(),
            },
        ) {
            let error = if *previous.caboose == *sw_caboose {
                anyhow!("reported multiple times (same value)",)
            } else {
                anyhow!(
                    "reported caboose multiple times (previously {:?}, \
                    now {:?})",
                    previous,
                    sw_caboose
                )
            };
            Err(error.context(format!(
                "baseboard {:?} caboose {:?}",
                baseboard, which
            )))
        } else {
            Ok(())
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
