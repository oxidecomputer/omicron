// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::fmt;

use camino::Utf8PathBuf;
use schemars::{
    JsonSchema,
    r#gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
};
use serde::{Deserialize, Serialize};
use update_engine::StepSpec;

update_engine::define_update_engine!(pub(crate) ExampleSpec);

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

    /// Metadata associated with a skipped event.
    ///
    /// In this example there is no metadata attached, so this is simply the
    /// unit type.
    type SkippedMetadata = ();

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
    CreateTempDir,
    Write,
    Skipped,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExampleStepMetadata {
    Write { num_bytes: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExampleCompletionMetadata {
    Download {
        num_bytes: u64,
    },
    CreateTempDir {
        #[schemars(schema_with = "paths_schema")]
        paths: Vec<Utf8PathBuf>,
    },
    Write {
        num_bytes: u64,
        #[schemars(schema_with = "paths_schema")]
        destinations: Vec<Utf8PathBuf>,
    },
}

/// A new schema for the write step.
///
/// This is used as a nested step.
#[derive(JsonSchema)]
pub(crate) enum ExampleWriteSpec {}

impl StepSpec for ExampleWriteSpec {
    type Component = ExampleComponent;
    type StepId = ExampleWriteStepId;
    type StepMetadata = ();
    type ProgressMetadata = ();
    type CompletionMetadata = ();
    type SkippedMetadata = ();
    type Error = anyhow::Error;
}

#[derive(
    Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum ExampleWriteStepId {
    Write {
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,
    },
}

fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

fn paths_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <Vec<String>>::json_schema(generator).into();
    schema.format = Some("Vec<Utf8PathBuf>".to_owned());
    schema.into()
}
