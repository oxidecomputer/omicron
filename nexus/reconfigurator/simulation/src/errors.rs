// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::{Generation, Name};
use omicron_uuid_kinds::CollectionUuid;
use thiserror::Error;

use crate::{BlueprintId, ResolvedBlueprintId};

/// The caller attempted to insert a duplicate key.
#[derive(Clone, Debug, Error)]
#[error("attempted to insert duplicate value: {}", self.id.to_error_string())]
pub struct DuplicateError {
    id: ObjectId,
}

impl DuplicateError {
    pub fn id(&self) -> &ObjectId {
        &self.id
    }

    pub(crate) fn collection(id: CollectionUuid) -> Self {
        Self { id: ObjectId::Collection(id) }
    }

    pub(crate) fn blueprint(id: BlueprintId) -> Self {
        Self { id: ObjectId::Blueprint(id) }
    }

    pub(crate) fn internal_dns(generation: Generation) -> Self {
        Self { id: ObjectId::InternalDns(generation) }
    }

    pub(crate) fn external_dns(generation: Generation) -> Self {
        Self { id: ObjectId::ExternalDns(generation) }
    }

    pub(crate) fn silo_name(name: Name) -> Self {
        Self { id: ObjectId::SiloName(name) }
    }
}

#[derive(Clone, Debug)]
pub enum ObjectId {
    Collection(CollectionUuid),
    Blueprint(BlueprintId),
    ResolvedBlueprint(ResolvedBlueprintId),
    InternalDns(Generation),
    ExternalDns(Generation),
    SiloName(Name),
}

impl ObjectId {
    fn to_error_string(&self) -> String {
        match self {
            ObjectId::Collection(id) => {
                format!("collection ID {id}")
            }
            ObjectId::Blueprint(BlueprintId::Latest) => {
                "no latest blueprint found".to_string()
            }
            ObjectId::Blueprint(BlueprintId::Target) => {
                "no target blueprint found".to_string()
            }
            ObjectId::Blueprint(BlueprintId::Id(id)) => {
                format!("blueprint ID {id}")
            }
            ObjectId::ResolvedBlueprint(id) => id.to_string(),
            ObjectId::InternalDns(generation) => {
                format!("internal DNS at generation {generation}")
            }
            ObjectId::ExternalDns(generation) => {
                format!("external DNS at generation {generation}")
            }
            ObjectId::SiloName(name) => {
                format!("silo name {name}")
            }
        }
    }
}

/// The caller attempted to access a key that does not exist.
#[derive(Clone, Debug, Error)]
#[error("no such key: {}", self.id.to_error_string())]
pub struct KeyError {
    id: ObjectId,
}

impl KeyError {
    pub fn id(&self) -> &ObjectId {
        &self.id
    }

    pub(crate) fn collection(id: CollectionUuid) -> Self {
        Self { id: ObjectId::Collection(id) }
    }

    pub(crate) fn blueprint(id: BlueprintId) -> Self {
        Self { id: ObjectId::Blueprint(id) }
    }

    pub(crate) fn resolved_blueprint(id: ResolvedBlueprintId) -> Self {
        Self { id: ObjectId::ResolvedBlueprint(id) }
    }

    pub(crate) fn internal_dns(generation: Generation) -> Self {
        Self { id: ObjectId::InternalDns(generation) }
    }

    pub(crate) fn external_dns(generation: Generation) -> Self {
        Self { id: ObjectId::ExternalDns(generation) }
    }

    pub(crate) fn silo_name(name: Name) -> Self {
        Self { id: ObjectId::SiloName(name) }
    }
}

/// An operation that requires an empty system was performed on a non-empty
/// system.
#[derive(Clone, Debug, Error)]
#[error("operation requires an empty system")]
pub struct NonEmptySystemError {}

impl NonEmptySystemError {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
