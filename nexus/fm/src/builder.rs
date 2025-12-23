// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sitrep builder

use nexus_types::fm;
use nexus_types::inventory;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;
use slog::Logger;

mod case;
pub use case::{AllCases, CaseBuilder};
pub(crate) mod rng;
pub use rng::SitrepBuilderRng;

#[derive(Debug)]
pub struct SitrepBuilder<'a> {
    pub log: Logger,
    pub inventory: &'a inventory::Collection,
    pub parent_sitrep: Option<&'a fm::Sitrep>,
    pub sitrep_id: SitrepUuid,
    pub cases: case::AllCases,
    comment: String,
}

impl<'a> SitrepBuilder<'a> {
    pub fn new(
        log: &Logger,
        inventory: &'a inventory::Collection,
        parent_sitrep: Option<&'a fm::Sitrep>,
    ) -> Self {
        Self::new_with_rng(
            log,
            inventory,
            parent_sitrep,
            SitrepBuilderRng::from_entropy(),
        )
    }

    pub fn new_with_rng(
        log: &Logger,
        inventory: &'a inventory::Collection,
        parent_sitrep: Option<&'a fm::Sitrep>,
        mut rng: SitrepBuilderRng,
    ) -> Self {
        // TODO(eliza): should the RNG also be seeded with the parent sitrep
        // UUID and/or the Omicron zone UUID? Hmm.
        let sitrep_id = rng.sitrep_id();
        let log = log.new(slog::o!(
            "sitrep_id" => format!("{sitrep_id:?}"),
            "parent_sitrep_id" => format!("{:?}", parent_sitrep.as_ref().map(|s| s.id())),
            "inv_collection_id" => format!("{:?}", inventory.id),
        ));

        let cases =
            case::AllCases::new(log.clone(), sitrep_id, parent_sitrep, rng);

        slog::info!(
            &log,
            "preparing sitrep {sitrep_id:?}";
            "existing_open_cases" => cases.cases.len(),
        );

        SitrepBuilder {
            log,
            sitrep_id,
            inventory,
            parent_sitrep,
            comment: String::new(),
            cases,
        }
    }

    pub fn build(
        self,
        creator_id: OmicronZoneUuid,
        time_created: chrono::DateTime<chrono::Utc>,
    ) -> fm::Sitrep {
        fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: self.sitrep_id,
                parent_sitrep_id: self.parent_sitrep.map(|s| s.metadata.id),
                inv_collection_id: self.inventory.id,
                creator_id,
                comment: self.comment,
                time_created,
            },
            cases: self
                .cases
                .cases
                .into_iter()
                .map(|builder| fm::Case::from(builder))
                .collect(),
        }
    }
}
