// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeSet, fmt, net::SocketAddr};

use camino::Utf8PathBuf;
use illumos_utils::zpool;
use omicron_common::disk::M2Slot;
use schemars::{
    JsonSchema,
    gen::SchemaGenerator,
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
    Scan,
    // There are multiple "composite" artifacts in the tuf repository the user
    // gives to wicketd: the RoT (A/B images), the host (phase1/phase2), and the
    // control plane (the collection of zones). wicketd handles unpacking the
    // RoT and host composite artifacts, because it needs to give pieces from
    // inside them to MGS. However, it does not unpack the control plane
    // artifact: only installinator needs access to the zone images inside, so
    // we have an explicit step here for that unpacking. If the user uploads a
    // tuf repository with a malformed control plane composite artifact, this
    // step is the point at which we'd discover that and fail.
    UnpackControlPlaneArtifact,
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

    ControlPlaneZones {
        /// Number of zone images that will be installed.
        zones_to_install: usize,
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

    /// Syncing writes to disk.
    Fsync,

    /// Future variants that might be unknown.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Unknown,
}

fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

fn path_schema_opt(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <Option<String>>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
