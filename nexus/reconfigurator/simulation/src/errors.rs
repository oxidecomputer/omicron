// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use nexus_types::{
    deployment::Blueprint, internal_api::params::DnsConfigParams,
    inventory::Collection,
};
use omicron_common::api::external::Name;
use omicron_uuid_kinds::CollectionUuid;
use thiserror::Error;
use uuid::Uuid;

/// The caller attempted to insert a duplicate key.
#[derive(Clone, Debug, Error)]
#[error("attempted to insert duplicate value: {}", self.kind.to_error_string())]
pub struct DuplicateError {
    kind: DuplicateErrorKind,
}

impl DuplicateError {
    pub fn collection(collection: Arc<Collection>) -> Self {
        Self { kind: DuplicateErrorKind::Collection(collection) }
    }

    pub fn blueprint(blueprint: Arc<Blueprint>) -> Self {
        Self { kind: DuplicateErrorKind::Blueprint(blueprint) }
    }

    pub fn internal_dns(dns: Arc<DnsConfigParams>) -> Self {
        Self { kind: DuplicateErrorKind::InternalDns(dns) }
    }

    pub fn external_dns(dns: Arc<DnsConfigParams>) -> Self {
        Self { kind: DuplicateErrorKind::ExternalDns(dns) }
    }

    pub fn silo_name(name: Name) -> Self {
        Self { kind: DuplicateErrorKind::SiloName(name) }
    }
}

#[derive(Clone, Debug)]
pub enum DuplicateErrorKind {
    // TODO(rain): just store IDs here
    Collection(Arc<Collection>),
    Blueprint(Arc<Blueprint>),
    InternalDns(Arc<DnsConfigParams>),
    ExternalDns(Arc<DnsConfigParams>),
    SiloName(Name),
}

impl DuplicateErrorKind {
    fn to_error_string(&self) -> String {
        match self {
            DuplicateErrorKind::Collection(c) => {
                format!("collection ID {}", c.id)
            }
            DuplicateErrorKind::Blueprint(b) => {
                format!("blueprint ID {}", b.id)
            }
            DuplicateErrorKind::InternalDns(params) => {
                format!("internal DNS at generation {}", params.generation)
            }
            DuplicateErrorKind::ExternalDns(params) => {
                format!("external DNS at generation {}", params.generation)
            }
            DuplicateErrorKind::SiloName(name) => {
                format!("silo name {}", name)
            }
        }
    }
}

/// The caller attempted to remove a key that does not exist.
#[derive(Clone, Debug, Error)]
#[error("no such value: {}", self.kind.to_error_string())]
pub struct MissingError {
    kind: MissingErrorKind,
}

impl MissingError {
    pub fn collection(id: CollectionUuid) -> Self {
        Self { kind: MissingErrorKind::Collection(id) }
    }

    pub fn blueprint(id: Uuid) -> Self {
        Self { kind: MissingErrorKind::Blueprint(id) }
    }

    pub fn silo_name(name: Name) -> Self {
        Self { kind: MissingErrorKind::SiloName(name) }
    }
}

#[derive(Clone, Debug)]
pub enum MissingErrorKind {
    Collection(CollectionUuid),
    Blueprint(Uuid),
    SiloName(Name),
}

impl MissingErrorKind {
    fn to_error_string(&self) -> String {
        match self {
            MissingErrorKind::Collection(id) => {
                format!("collection ID {}", id)
            }
            MissingErrorKind::Blueprint(id) => {
                format!("blueprint ID {}", id)
            }
            MissingErrorKind::SiloName(name) => {
                format!("silo name {}", name)
            }
        }
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
