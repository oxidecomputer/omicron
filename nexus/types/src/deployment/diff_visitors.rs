// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An API for visiting diffs between blueprints generated via [diffus](https://github.com/oxidecomputer/diffus)
//!
//! Modelled after [`syn::visit`](https://docs.rs/syn/1/syn/visit).

pub mod visit_blueprint;
pub mod visit_blueprint_datasets_config;
pub mod visit_blueprint_physical_disks_config;
pub mod visit_blueprint_zones_config;

use diffus::edit::enm;
use omicron_uuid_kinds::{
    DatasetUuid, OmicronZoneUuid, PhysicalDiskUuid, SledUuid,
};
use thiserror::Error;

use super::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, EditedBlueprintZoneConfig,
};

#[derive(Debug, Error)]
pub enum BpVisitorError {
    #[error(
        "BlueprintZonesConfig inserted for sled {} with missing sled_state",
        sled_id
    )]
    MissingSledStateOnZoneInsert { sled_id: SledUuid },
}

/// A context for blueprint related visitors
#[derive(Debug, Default)]
pub struct BpVisitorContext {
    pub sled_id: Option<SledUuid>,
    pub zone_id: Option<OmicronZoneUuid>,
    pub disk_id: Option<PhysicalDiskUuid>,
    pub dataset_id: Option<DatasetUuid>,
    // Structural errors must be reported through the context, as we can't really
    // return results from visitor methods.
    pub errors: Vec<BpVisitorError>,
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
