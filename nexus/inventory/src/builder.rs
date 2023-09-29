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
use gateway_client::types::SpState;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use strum::EnumIter;

// XXX-dap add rack id

#[derive(Clone, Copy, Debug, EnumIter)]
pub enum CabooseWhich {
    SpSlot0,
    SpSlot1,
    RotSlotA,
    RotSlotB,
}

#[derive(Debug)]
pub struct CollectionBuilder {
    errors: Vec<anyhow::Error>,
    time_started: DateTime<Utc>,
    creator: String,
    comment: String,
    baseboards: BTreeSet<Arc<BaseboardId>>,
    cabooses: BTreeSet<Arc<Caboose>>,
    sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
}

impl CollectionBuilder {
    pub fn new(creator: &str, comment: &str) -> Self {
        CollectionBuilder {
            errors: vec![],
            time_started: Utc::now(),
            creator: creator.to_owned(),
            comment: comment.to_owned(),
            baseboards: BTreeSet::new(),
            cabooses: BTreeSet::new(),
            sps: BTreeMap::new(),
        }
    }

    pub fn build(self) -> Collection {
        Collection {
            errors: self.errors,
            time_started: self.time_started,
            time_done: Utc::now(),
            creator: self.creator,
            comment: self.comment,
            baseboards: self.baseboards,
            cabooses: self.cabooses,
            sps: self.sps,
        }
    }

    pub fn found_sp_state(
        &mut self,
        source: &str,
        sp_state: SpState,
    ) -> Arc<BaseboardId> {
        let baseboard = Self::enum_item(
            &mut self.baseboards,
            BaseboardId {
                serial_number: sp_state.serial_number,
                part_number: sp_state.model,
            },
        );

        let rot = RotState::try_from(sp_state.rot).ok();
        let _ = self.sps.entry(baseboard.clone()).or_insert_with(|| {
            ServiceProcessor {
                baseboard: baseboard.clone(),
                time_collected: Utc::now(),
                source: source.to_owned(),
                hubris_archive: sp_state.hubris_archive_id,
                power_state: sp_state.power_state,
                rot,
                sp_slot0_caboose: None,
                sp_slot1_caboose: None,
                rot_slot_a_caboose: None,
                rot_slot_b_caboose: None,
            }
        });

        baseboard
    }

    pub fn sp_found_caboose_already(
        &self,
        baseboard: &BaseboardId,
        which: CabooseWhich,
    ) -> bool {
        self.sps
            .get(baseboard)
            .map(|sp| {
                let ptr = match which {
                    CabooseWhich::SpSlot0 => &sp.sp_slot0_caboose,
                    CabooseWhich::SpSlot1 => &sp.sp_slot1_caboose,
                    CabooseWhich::RotSlotA => &sp.rot_slot_a_caboose,
                    CabooseWhich::RotSlotB => &sp.rot_slot_b_caboose,
                };
                ptr.is_some()
            })
            .unwrap_or(false)
    }

    pub fn found_sp_caboose(
        &mut self,
        baseboard: &BaseboardId,
        which: CabooseWhich,
        caboose: Caboose,
    ) -> Result<(), anyhow::Error> {
        let caboose = Self::enum_item(&mut self.cabooses, caboose);
        let sp = self.sps.get_mut(baseboard).ok_or_else(|| {
            anyhow!("reporting caboose for unknown baseboard: {:?}", baseboard)
        })?;
        let ptr = match which {
            CabooseWhich::SpSlot0 => &mut sp.sp_slot0_caboose,
            CabooseWhich::SpSlot1 => &mut sp.sp_slot1_caboose,
            CabooseWhich::RotSlotA => &mut sp.rot_slot_a_caboose,
            CabooseWhich::RotSlotB => &mut sp.rot_slot_b_caboose,
        };

        if let Some(already) = ptr {
            let error = if *already == caboose {
                anyhow!("reported multiple times (same value)",)
            } else {
                anyhow!(
                    "reported caboose multiple times (previously {:?}, \
                    now {:?}, keeping only the first one)",
                    already,
                    caboose
                )
            };
            Err(error.context(format!(
                "baseboard {:?} caboose {:?}",
                baseboard, which
            )))
        } else {
            *ptr = Some(caboose);
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
