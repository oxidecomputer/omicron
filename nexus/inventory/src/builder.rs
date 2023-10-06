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
use nexus_types::inventory::CabooseWhich;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use gateway_client::types::SpComponentCaboose;

// XXX-dap add rack id

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
        BTreeMap<CabooseWhich, BTreeMap<Arc<BaseboardId>, Arc<Caboose>>>,
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
        sp_state: SpState,
    ) -> Arc<BaseboardId> {
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

        baseboard
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
        // XXX-dap I messed around with the Caboose structure in nexus/types to
        // include time_collected, source, etc. and now I need to unpack what
        // needs to be fixed here.
        let caboose = Self::enum_item(&mut self.cabooses, caboose);
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
            Caboose {
                time_collected: Utc::now(),
                source: source.to_owned(),
                board: caboose.board,
                git_commit: caboose.git_commit,
                name: caboose.name,
                // XXX-dap TODO-doc
                version: caboose
                    .version
                    .unwrap_or_else(|| String::from("unspecified")),
            },
        ) {
            let error = if *previous == *caboose {
                anyhow!("reported multiple times (same value)",)
            } else {
                anyhow!(
                    "reported caboose multiple times (previously {:?}, \
                    now {:?})",
                    previous,
                    caboose
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
