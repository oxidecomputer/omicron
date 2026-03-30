// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
use nexus_types::fm::{self, AnalysisInputReport, Sitrep, SitrepVersion};
use nexus_types::inventory;
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct Input {
    parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
    inv: Arc<inventory::Collection>,
    new_ereports: IdOrdMap<fm::Ereport>,
}

impl Input {
    pub fn parent_sitrep(&self) -> Option<&Sitrep> {
        self.parent_sitrep.as_ref().map(|s| &s.1)
    }

    pub fn inventory(&self) -> &inventory::Collection {
        &self.inv
    }

    pub fn new_ereports(&self) -> &IdOrdMap<fm::Ereport> {
        &self.new_ereports
    }

    pub fn builder(
        parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
        inv: Arc<inventory::Collection>,
    ) -> InputBuilder {
        InputBuilder {
            input: Input {
                parent_sitrep,
                inv,
                new_ereports: IdOrdMap::default(),
            },
            already_seen_ereports: BTreeSet::default(),
        }
    }
}

pub struct InputBuilder {
    input: Input,
    already_seen_ereports: BTreeSet<fm::EreportId>,
}

impl InputBuilder {
    pub fn add_new_ereports(
        &mut self,
        ereports: impl IntoIterator<Item = fm::Ereport>,
    ) {
        let parent_sitrep = self.input.parent_sitrep.as_ref().map(|s| &s.1);
        self.input.new_ereports.extend(ereports.into_iter().filter_map(
            |ereport| {
                if let Some(sitrep) = parent_sitrep {
                    let id = ereport.id();
                    if sitrep.ereports_by_id.contains_key(&id) {
                        self.already_seen_ereports.insert(*id);
                        return None;
                    }
                }

                Some(ereport)
            },
        ))
    }

    pub fn num_ereports(&self) -> usize {
        self.input.new_ereports.len()
    }

    pub fn finish(self) -> (Input, AnalysisInputReport) {
        let (parent_sitrep_id, parent_inv_id) = match self.input.parent_sitrep {
            Some(ref s) => {
                let (_, ref sitrep) = **s;
                let id = sitrep.id();
                let inv_id = sitrep.metadata.inv_collection_id;
                (Some(id), Some(inv_id))
            }
            None => (None, None),
        };
        let new_ereport_ids =
            self.input.new_ereports.iter().map(|e| *e.id()).collect();
        let report = AnalysisInputReport {
            parent_sitrep_id,
            parent_inv_id,
            inv_id: self.input.inv.id,
            new_ereport_ids,
            already_seen_ereport_ids: self.already_seen_ereports,
        };
        (self.input, report)
    }
}
