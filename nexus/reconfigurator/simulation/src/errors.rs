// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeSet;

use indent_write::indentable::Indentable as _;
use itertools::Itertools;
use omicron_common::api::external::{Generation, Name};
use omicron_uuid_kinds::ReconfiguratorSimStateUuid;
use swrite::{SWrite, swriteln};
use thiserror::Error;

use crate::{
    BlueprintId, CollectionId, ResolvedBlueprintId, ResolvedCollectionId,
};

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

    pub(crate) fn collection(id: CollectionId) -> Self {
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
    Collection(CollectionId),
    Blueprint(BlueprintId),
    ResolvedCollection(ResolvedCollectionId),
    ResolvedBlueprint(ResolvedBlueprintId),
    InternalDns(Generation),
    ExternalDns(Generation),
    SiloName(Name),
}

impl ObjectId {
    fn to_error_string(&self) -> String {
        match self {
            ObjectId::Collection(CollectionId::Latest) => {
                "no latest collection found".to_string()
            }
            ObjectId::Collection(CollectionId::Id(id)) => {
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
            ObjectId::ResolvedCollection(id) => id.to_string(),
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

    pub(crate) fn collection(id: CollectionId) -> Self {
        Self { id: ObjectId::Collection(id) }
    }

    pub(crate) fn resolved_collection(id: ResolvedCollectionId) -> Self {
        Self { id: ObjectId::ResolvedCollection(id) }
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

/// Unknown zone names were provided to `SimTufRepoSource::simulate_zone_error`.
#[derive(Clone, Debug, Error)]
#[error(
    "unknown zone names `{}` (valid zone names: {})",
    self.unknown.join(", "),
    self.known.iter().join(", "),
)]
pub struct UnknownZoneNamesError {
    /// The names of the unknown zones.
    pub unknown: Vec<String>,

    /// The set of known zone names.
    pub known: BTreeSet<String>,
}

impl UnknownZoneNamesError {
    pub(crate) fn new(unknown: Vec<String>, known: BTreeSet<String>) -> Self {
        Self { unknown, known }
    }
}

/// A state that matched a prefix query.
#[derive(Clone, Debug)]
pub struct StateMatch {
    /// The state ID.
    pub id: ReconfiguratorSimStateUuid,
    /// The state generation.
    pub generation: Generation,
    /// The state description.
    pub description: String,
}

/// Error when resolving a state ID.
#[derive(Clone, Debug, Error)]
pub enum StateIdResolveError {
    /// No state found with the given prefix.
    #[error("no state found with prefix '{0}'")]
    NoMatch(String),

    /// Multiple states found with the given prefix.
    #[error("prefix '{prefix}' is ambiguous: matches {count} states\n{}", format_matches(.matches))]
    Ambiguous { prefix: String, count: usize, matches: Vec<StateMatch> },

    /// State not found by ID.
    #[error("state not found: {0}")]
    NotFound(ReconfiguratorSimStateUuid),
}

fn format_matches(matches: &[StateMatch]) -> String {
    let mut output = String::new();
    for state_match in matches {
        swriteln!(
            output,
            "  - {} generation {}:",
            state_match.id,
            state_match.generation
        );
        swriteln!(
            output,
            "{}",
            state_match.description.trim_end().indented("    ")
        );
    }
    output
}
