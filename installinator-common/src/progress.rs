// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, net::SocketAddrV6, time::Duration};

use anyhow::bail;
use camino::Utf8PathBuf;
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::{Deserialize, Serialize};
use serde_with::rust::deserialize_ignore_any;
use thiserror::Error;
use update_engine::{AsError, StepSpec};

// ---
// Type definitions for use by installinator code.
// ---

update_engine::define_update_engine!(pub InstallinatorSpec);

/// The specification for installinator events.
#[derive(JsonSchema)]
pub enum InstallinatorSpec {}

impl StepSpec for InstallinatorSpec {
    type Component = InstallinatorComponent;
    type StepId = InstallinatorStepId;
    type StepMetadata = InstallinatorStepMetadata;
    type ProgressMetadata = InstallinatorProgressMetadata;
    type CompletionMetadata = InstallinatorCompletionMetadata;
    type SkippedMetadata = ();
    type Error = anyhow::Error;
}

/// Installinator components.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum InstallinatorComponent {
    /// The host phase 2 component.
    HostPhase2,

    /// The control plane component.
    ControlPlane,

    /// A component that means "both the host and the control plane", used for
    /// writes for now. It is possible that this component will go away in the
    /// future.
    Both,

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

/// Installinator step identifier.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum InstallinatorStepId {
    Download,
    Format,
    Write,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum InstallinatorStepMetadata {
    Write {
        /// The destination being formatted or written to.
        ///
        /// Available with format and destination events.
        #[schemars(schema_with = "path_schema_opt")]
        destination: Option<Utf8PathBuf>,
    },

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum InstallinatorProgressMetadata {
    Download {
        /// The peer being downloaded from.
        ///
        /// Available with downlad events.
        peer: SocketAddrV6,
    },

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum InstallinatorCompletionMetadata {
    Download {
        /// The address the artifact was downloaded from.
        address: SocketAddrV6,
    },
    Write {
        /// The slots to which data has been written. This is non-empty.
        slots_written: Vec<M2Slot>,
    },

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

/// An M.2 slot that was written.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub enum M2Slot {
    A,
    B,
}

impl fmt::Display for M2Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::A => f.write_str("A"),
            Self::B => f.write_str("B"),
        }
    }
}

impl TryFrom<i64> for M2Slot {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            // Gimlet should have 2 M.2 drives: drive A is assigned slot 17, and
            // drive B is assigned slot 18.
            17 => Ok(Self::A),
            18 => Ok(Self::B),
            _ => bail!("unexpected M.2 slot {value}"),
        }
    }
}

/// The specification for write events.
#[derive(JsonSchema)]
pub enum WriteSpec {}

impl StepSpec for WriteSpec {
    type Component = WriteComponent;
    type StepId = WriteStepId;
    type StepMetadata = ();
    type ProgressMetadata = ();
    type CompletionMetadata = ();
    type SkippedMetadata = ();
    type Error = WriteError;
}

/// The write component.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum WriteComponent {
    /// The host phase 2 component.
    HostPhase2,

    /// The control plane component.
    ControlPlane,

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

impl fmt::Display for WriteComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostPhase2 => f.write_str("host phase 2"),
            Self::ControlPlane => f.write_str("control plane"),
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

/// A step identifier for the write operation.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WriteStepId {
    /// Writing the component.
    Writing { slot: M2Slot },

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

/// The error that occurred.
#[derive(Debug, Error)]
#[error(
    "writing {component} to slot {slot} failed \
     after {written_bytes}/{total_bytes} bytes"
)]
pub struct WriteError {
    pub component: WriteComponent,
    pub slot: M2Slot,
    pub written_bytes: u64,
    pub total_bytes: u64,
    #[source]
    pub error: std::io::Error,
}

impl AsError for WriteError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

/// A report, consisting of a list of events and some more metadata.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub struct ProgressReport {
    /// How long it's been since the installinator started running.
    pub total_elapsed: Duration,

    /// A list of step events.
    ///
    /// Step events include success and failure events.
    pub step_events: Vec<StepEvent>,

    /// A list of progress events.
    ///
    /// For now, this is zero or one events since the installinator performs its
    /// actions serially.
    pub progress_events: Vec<ProgressEvent>,
}

fn path_schema_opt(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <Option<String>>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
