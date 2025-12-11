// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RNGs for sitrep generation to allow reproduceable UUID generation
//! (particularly for tests).
//!
//! This is similar to the `nexus_reconfigurator_planning::planner::rng`
//! module.

use omicron_uuid_kinds::AlertKind;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseKind;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::SitrepUuid;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use std::hash::Hash;
use typed_rng::TypedUuidRng;

#[derive(Clone, Debug)]
pub struct SitrepBuilderRng {
    parent: StdRng,
    case_rng: TypedUuidRng<CaseKind>,
}

impl SitrepBuilderRng {
    pub fn from_entropy() -> Self {
        Self::new_from_parent(StdRng::from_os_rng())
    }

    pub fn from_seed<H: Hash>(seed: H) -> Self {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "sitrep-builder";
        Self::new_from_parent(typed_rng::from_seed(seed, SEED_EXTRA))
    }

    pub fn new_from_parent(mut parent: StdRng) -> Self {
        let case_rng = TypedUuidRng::from_parent_rng(&mut parent, "case");

        Self { parent, case_rng }
    }

    pub(super) fn sitrep_id(&mut self) -> SitrepUuid {
        // we only need a single sitrep UUID, so no sense storing a whole RNG
        // for it in the builder RNGs...
        TypedUuidRng::from_parent_rng(&mut self.parent, "sitrep").next()
    }

    pub(super) fn next_case(&mut self) -> (CaseUuid, CaseBuilderRng) {
        let case_id = self.case_rng.next();
        let rng = CaseBuilderRng::new(case_id, self);
        (case_id, rng)
    }
}

#[derive(Clone, Debug)]
pub(super) struct CaseBuilderRng {
    alert_rng: TypedUuidRng<AlertKind>,
}

impl CaseBuilderRng {
    pub(super) fn new(
        case_id: CaseUuid,
        sitrep: &mut SitrepBuilderRng,
    ) -> Self {
        let alert_rng = TypedUuidRng::from_parent_rng(
            &mut sitrep.parent,
            (case_id, "alert"),
        );
        Self { alert_rng }
    }

    pub(super) fn next_alert(&mut self) -> AlertUuid {
        self.alert_rng.next()
    }
}
