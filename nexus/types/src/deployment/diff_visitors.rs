// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An API for visiting diffs between blueprints generated via [diffus](https://github.com/oxidecomputer/diffus)
//!
//! Modelled after [`syn::visit`](https://docs.rs/syn/1/syn/visit).

mod visit_blueprint_zones_config;

use diffus::edit::enm;
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};

use super::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, EditedBlueprintZoneConfig,
    EditedBlueprintZonesConfig,
};

/// A context for blueprint related visitors
#[derive(Debug, Clone, Default)]
pub struct BpVisitorContext {
    pub sled_id: Option<SledUuid>,
    pub zone_id: Option<OmicronZoneUuid>,
}

#[derive(Debug, Clone, Copy)]
pub struct Change<'e, T> {
    pub before: &'e T,
    pub after: &'e T,
}

impl<'e, T> Change<'e, T> {
    pub fn new(before: &'e T, after: &'e T) -> Change<'e, T> {
        Change { before, after }
    }
}

impl<'e, T> From<(&'e T, &'e T)> for Change<'e, T> {
    fn from(value: (&'e T, &'e T)) -> Self {
        Change::new(value.0, value.1)
    }
}

impl<'e, T, Diff> From<&enm::Edit<'e, T, Diff>> for Change<'e, T> {
    fn from(value: &enm::Edit<'e, T, Diff>) -> Self {
        match value {
            enm::Edit::VariantChanged(before, after) => {
                Change::new(before, after)
            }
            enm::Edit::AssociatedChanged { before, after, .. } => {
                Change::new(before, after)
            }
        }
    }
}
