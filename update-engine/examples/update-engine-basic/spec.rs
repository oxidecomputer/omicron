// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{fmt, net::Ipv6Addr};

use camino::Utf8PathBuf;
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::{Deserialize, Serialize};
use update_engine::StepSpec;

// ---
// Type aliases to make the engine easy to use in the rest of the code.
// ---

pub(crate) type UpdateEngine<'a> = update_engine::UpdateEngine<'a, ExampleSpec>;
pub(crate) type ComponentRegistrar<'exec, 'a> =
    update_engine::ComponentRegistrar<'exec, 'a, ExampleSpec>;
pub(crate) type Event = update_engine::events::Event<ExampleSpec>;
pub(crate) type StepEventKind =
    update_engine::events::StepEventKind<ExampleSpec>;
pub(crate) type ProgressEventKind =
    update_engine::events::ProgressEventKind<ExampleSpec>;
pub(crate) type StepInfo = update_engine::events::StepInfo<ExampleSpec>;
pub(crate) type StepInfoWithMetadata =
    update_engine::events::StepInfoWithMetadata<ExampleSpec>;
pub(crate) type StepProgress = update_engine::events::StepProgress<ExampleSpec>;
pub(crate) type StepResult<T> = update_engine::StepResult<ExampleSpec, T>;
pub(crate) type StepOutcome = update_engine::events::StepOutcome<ExampleSpec>;

/// Create a type to hang the engine specification off of. This is an empty enum
/// (no possible values) because we never construct the type.
#[derive(JsonSchema)]
pub(crate) enum ExampleSpec {}

impl StepSpec for ExampleSpec {
    /// This defines a larger component under which a particular set of steps is
    /// grouped.
    type Component = ExampleComponent;

    /// This defines a smaller step ID. It is recommended, but not enforced,
    /// that (component, step ID) form a unique index.
    ///
    /// XXX: maybe we should enforce this.
    type StepId = ExampleStepId;

    /// Metadata associated with each step. This becomes part of the `StepInfo`
    /// returned by.
    type StepMetadata = ExampleStepMetadata;

    /// Metadata associated with an individual progress event.
    ///
    /// In this example this is just a generic `serde_json::Value`, but it can
    /// be a more concrete type as well.
    type ProgressMetadata = serde_json::Value;

    /// Metadata associated with a completion event.
    type CompletionMetadata = ExampleCompletionMetadata;

    type Error = anyhow::Error;
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExampleComponent {
    Component1,
    Component2,
}

impl fmt::Display for ExampleComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Component1 => write!(f, "Component 1"),
            Self::Component2 => write!(f, "Component 2"),
        }
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExampleStepId {
    Download,
    Write,
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExampleStepMetadata {
    Address { peer: Ipv6Addr },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExampleCompletionMetadata {
    Download {
        num_bytes: u64,
    },
    Write {
        num_bytes: u64,
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,
    },
}

fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
