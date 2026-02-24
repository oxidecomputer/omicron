// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeSet, fmt, net::SocketAddr};

use camino::Utf8PathBuf;
use illumos_utils::zpool;
use omicron_common::disk::M2Slot;
use schemars::{
    JsonSchema,
    r#gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
};
use serde::{Deserialize, Serialize};
use serde_with::rust::deserialize_ignore_any;
use thiserror::Error;
use update_engine::{AsError, StepSpec, errors::NestedEngineError};

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
    type SkippedMetadata = serde_json::Value;
    type Error = anyhow::Error;
}

/// Installinator components.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum InstallinatorComponent {
    /// The installinator document.
    InstallinatorDocument,

    /// The host phase 2 component.
    HostPhase2,

    /// The control plane zone component.
    ControlPlaneZone,

    /// The measurement corpus component.
    MeasurementCorpus,

    /// A component that means "all components", used for downloads and writes
    /// for now. It is possible that this component will go away in the future.
    All,

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
    Scan,
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
        peer: SocketAddr,
    },

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum InstallinatorCompletionMetadata {
    HardwareScan {
        /// Number of disks found.
        disks_found: usize,
    },

    Download {
        /// The address the artifact was downloaded from.
        address: SocketAddr,
    },

    Write {
        /// The output of the write operation.
        output: WriteOutput,
    },

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

/// The output of a write operation.
///
/// Forms part of [`InstallinatorCompletionMetadata::Write`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct WriteOutput {
    /// The slots that were requested to be written.
    pub slots_attempted: BTreeSet<M2Slot>,

    /// The slots that were actually written.
    pub slots_written: BTreeSet<M2Slot>,
}

impl WriteOutput {
    /// Returns a list of the slots not written.
    pub fn slots_not_written(&self) -> Vec<M2Slot> {
        let mut not_written = Vec::new();
        for slot in &self.slots_attempted {
            if !self.slots_written.contains(slot) {
                not_written.push(*slot);
            }
        }

        not_written
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

    /// The measurement corpus.
    MeasurementCorpus,

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

impl fmt::Display for WriteComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostPhase2 => f.write_str("host phase 2"),
            Self::ControlPlane => f.write_str("control plane"),
            Self::MeasurementCorpus => f.write_str("measurement corpus"),
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
pub enum WriteError {
    #[error(
        "writing {component} to slot {slot} failed \
         after {written_bytes}/{total_bytes} bytes"
    )]
    WriteError {
        component: WriteComponent,
        slot: M2Slot,
        written_bytes: u64,
        total_bytes: u64,
        #[source]
        error: std::io::Error,
    },
    #[error("error validating checksum of written file")]
    ChecksumValidationError(#[source] anyhow::Error),
    #[error("error removing files from {path}: {error}")]
    RemoveFilesError { path: Utf8PathBuf, error: std::io::Error },
    #[error("error fsyncing output directory: {error}")]
    SyncOutputDirError { error: std::io::Error },
    #[error("error interacting with zpool: {error}")]
    ZpoolError {
        #[from]
        error: zpool::Error,
    },
    #[error("error writing control plane")]
    ControlPlaneWriteError {
        #[source]
        error: Box<NestedEngineError<ControlPlaneZonesSpec>>,
    },
    #[error("error creating directory")]
    CreateDirError {
        #[source]
        error: std::io::Error,
    },
}

impl From<NestedEngineError<ControlPlaneZonesSpec>> for WriteError {
    fn from(error: NestedEngineError<ControlPlaneZonesSpec>) -> Self {
        Self::ControlPlaneWriteError { error: Box::new(error) }
    }
}

impl AsError for WriteError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

/// The specification for writing control plane zones.
#[derive(JsonSchema)]
pub enum ControlPlaneZonesSpec {}

// This is a nested spec used within a `WriteSpec` engine, and we reuse a couple
// of `WriteSpec`'s types for simplicity.
impl StepSpec for ControlPlaneZonesSpec {
    type Component = WriteComponent;
    type StepId = ControlPlaneZonesStepId;
    type StepMetadata = ();
    type ProgressMetadata = ();
    type CompletionMetadata = ();
    type SkippedMetadata = ();
    type Error = WriteError;
}

/// A step identifier for the control plane zones operation.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ControlPlaneZonesStepId {
    /// Removing any files from the target directory.
    CleanTargetDirectory {
        #[schemars(schema_with = "path_schema")]
        path: Utf8PathBuf,
    },

    /// Writing a zone.
    Zone { name: String },

    /// Writing the MUPdate override file.
    MupdateOverride,

    /// Writing the zone manifest.
    ZoneManifest,

    /// Writing the measurement manifest.
    MeasurementManifest,

    /// Writing the measurement corpus.
    Measurement { name: String },

    /// Ensure the measurement directory exists
    CreateMeasurementDir,

    /// Syncing writes to disk.
    Fsync,

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

fn path_schema_opt(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject =
        <Option<String>>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
