// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for building [`Collection`] dynamically

use crate::BaseboardId;
use crate::Caboose;
use crate::Collection;
use crate::RotState;
use crate::ServiceProcessor;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use nexus_types::inventory::CabooseFound;
use nexus_types::inventory::CabooseWhich;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct CollectionBuilder {
    errors: Vec<anyhow::Error>,
    time_started: DateTime<Utc>,
    collector: String,
    comment: String,
    baseboards: BTreeSet<Arc<BaseboardId>>,
    cabooses: BTreeSet<Arc<Caboose>>,
    sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
    rots: BTreeMap<Arc<BaseboardId>, RotState>,
    cabooses_found:
        BTreeMap<CabooseWhich, BTreeMap<Arc<BaseboardId>, CabooseFound>>,
}

impl CollectionBuilder {
    pub fn new(collector: &str, comment: &str) -> Self {
        CollectionBuilder {
            errors: vec![],
            time_started: Utc::now(),
            collector: collector.to_owned(),
            comment: comment.to_owned(),
            baseboards: BTreeSet::new(),
            cabooses: BTreeSet::new(),
            sps: BTreeMap::new(),
            rots: BTreeMap::new(),
            cabooses_found: BTreeMap::new(),
        }
    }

    pub fn build(self) -> Collection {
        Collection {
            id: Uuid::new_v4(),
            errors: self.errors,
            time_started: self.time_started,
            time_done: Utc::now(),
            collector: self.collector,
            comment: self.comment,
            baseboards: self.baseboards,
            cabooses: self.cabooses,
            sps: self.sps,
            rots: self.rots,
            cabooses_found: self.cabooses_found,
        }
    }

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

        let baseboard = Self::enum_item(
            &mut self.baseboards,
            BaseboardId {
                serial_number: sp_state.serial_number,
                part_number: sp_state.model,
            },
        );

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

    pub fn found_sp_caboose(
        &mut self,
        baseboard: &BaseboardId,
        which: CabooseWhich,
        source: &str,
        caboose: SpComponentCaboose,
    ) -> Result<(), anyhow::Error> {
        let sw_caboose =
            Self::enum_item(&mut self.cabooses, Caboose::from(caboose));
        let (baseboard, _) =
            self.sps.get_key_value(baseboard).ok_or_else(|| {
                anyhow!(
                    "reporting caboose for unknown baseboard: {:?}",
                    baseboard
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

    fn enum_item<T: Clone + Ord>(
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

    pub fn found_error(&mut self, error: anyhow::Error) {
        self.errors.push(error);
    }
}
