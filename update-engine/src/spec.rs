// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::fmt;

use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Serialize};

/// A specification for an [`UpdateEngine`](crate::UpdateEngine).
///
/// This defines the set of types required to use an `UpdateEngine`.
///
/// NOTE: `StepSpec` is only required to implement `JsonSchema` to obtain the
/// name of the schema. This is an upstream limitation in `JsonSchema`.
pub trait StepSpec: JsonSchema {
    /// A component associated with each step.
    type Component: Clone
        + fmt::Debug
        + DeserializeOwned
        + Serialize
        + Eq
        + JsonSchema;

    /// The step identifier.
    type StepId: Clone
        + fmt::Debug
        + DeserializeOwned
        + Serialize
        + Eq
        + JsonSchema;

    /// Metadata associated with each step.
    ///
    /// This can be `()` if there's no metadata associated with the step, or
    /// `serde_json::Value` for freeform metadata.
    type StepMetadata: Clone
        + fmt::Debug
        + DeserializeOwned
        + Serialize
        + Eq
        + JsonSchema;

    /// Metadata associated with an individual progress event.
    ///
    /// This can be `()` if there's no metadata associated with the step, or
    /// `serde_json::Value` for freeform metadata.
    type ProgressMetadata: Clone
        + fmt::Debug
        + DeserializeOwned
        + Serialize
        + Eq
        + JsonSchema;

    /// Metadata associated with each step's completion.
    ///
    /// This can be `()` if there's no metadata associated with the step, or
    /// `serde_json::Value` for freeform metadata.
    type CompletionMetadata: Clone
        + fmt::Debug
        + DeserializeOwned
        + Serialize
        + Eq
        + JsonSchema;

    /// Metadata associated with a step being skipped.
    ///
    /// This can be `()` if there's no metadata associated with the step, or
    /// `serde_json::Value` for freeform metadata.
    type SkippedMetadata: Clone
        + fmt::Debug
        + DeserializeOwned
        + Serialize
        + Eq
        + JsonSchema;

    /// The error type associated with each step.
    ///
    /// Ideally this would just have a trait bound of `std::error::Error` --
    /// however, `anyhow::Error` doesn't implement `std::error::Error`. Both can
    /// be converted to a dynamic `Error`, though. We use `AsError` to abstract
    /// over both sorts of errors.
    type Error: AsError + fmt::Debug;
}

/// Trait that abstracts over concrete errors and `anyhow::Error`.
///
/// This needs to be manually implemented for any custom error types.
pub trait AsError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static);
}

impl AsError for anyhow::Error {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self.as_ref()
    }
}
