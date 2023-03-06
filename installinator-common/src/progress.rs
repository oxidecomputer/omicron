// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::SocketAddrV6, time::Duration};

use camino::Utf8PathBuf;
use omicron_common::update::ArtifactKind;
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::Deserialize;

/// A report, consisting of a list of events and some more metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub struct ProgressReport {
    /// How long it's been since the installinator started running.
    pub total_elapsed: Duration,

    /// A list of completion events.
    ///
    /// Completion events include success and failure events.
    pub completion_events: Vec<CompletionEvent>,

    /// A list of progress events.
    ///
    /// For now, this is zero or one events since the installinator performs its
    /// actions serially.
    pub progress_events: Vec<ProgressEvent>,
}

/// An individual completion event.
///
/// Forms part of [`ProgressReport`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub struct CompletionEvent {
    /// When this event was generated, in duration since the start of the program.
    ///
    /// This can be compared against [`ProgressReport::total_elapsed`] to figure
    /// out how old the event was.
    pub total_elapsed: Duration,

    /// The kind of event this is.
    pub kind: CompletionEventKind,
}

/// An individual kind of completion event.
///
/// Forms part of [`CompletionEvent`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum CompletionEventKind {
    /// The download of an artifact failed.
    DownloadFailed {
        /// The download attempt that failed.
        attempt: usize,

        /// The kind of artifact being downloaded.
        kind: ArtifactKind,

        /// The peer the artifact was being downloaded from, if any.
        peer: SocketAddrV6,

        /// The number of bytes downloaded before failure.
        downloaded_bytes: u64,

        /// How long the download took before failing.
        elapsed: Duration,

        /// A message indicating the reason for failure.
        message: String,
    },

    /// An artifact download was completed.
    DownloadCompleted {
        /// The download attempt that completed.
        attempt: usize,

        /// The artifact downloaded.
        kind: ArtifactKind,

        /// The peer the artifact was downloaded from.
        peer: SocketAddrV6,

        /// The number of bytes downloaded.
        artifact_size: u64,

        /// How long the download took.
        elapsed: Duration,
    },

    /// Failed to format a disk.
    FormatFailed {
        /// The format attempt that failed.
        attempt: usize,

        /// The path to the disk.
        #[schemars(schema_with = "path_schema")]
        path: Utf8PathBuf,

        /// How long the format attempt took.
        elapsed: Duration,

        /// A message indicating the reason for failure.
        message: String,
    },

    /// Completed formatting a disk.
    FormatCompleted {
        /// The format attempt.
        attempt: usize,

        /// The path to the disk.
        #[schemars(schema_with = "path_schema")]
        path: Utf8PathBuf,

        /// How long the format attempt took.
        elapsed: Duration,
    },

    /// Failed to write an artifact.
    WriteFailed {
        /// The write attempt that failed.
        attempt: usize,

        /// The kind of artifact being written out.
        kind: ArtifactKind,

        /// The destination the artifact is being written out to.
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,

        /// The number of bytes written out before failure.
        written_bytes: u64,

        /// The total number of bytes that should have been written out.
        total_bytes: u64,

        /// How long the write took before it failed.
        elapsed: Duration,

        /// A message indicating why the write failed.
        message: String,
    },

    /// Completed writing an artifact.
    WriteCompleted {
        /// The write attempt that completed.
        attempt: usize,

        /// The kind of artifact that was written out.
        kind: ArtifactKind,

        /// The destination for the artifact.
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,

        /// The number of bytes written out.
        artifact_size: u64,

        /// How long the write took to complete.
        elapsed: Duration,
    },

    /// A miscellaneous error occurred.
    ///
    /// This is a catch-all for errors that aren't described by any of the variants.
    MiscError {
        /// The name of the operation that failed.
        operation: String,

        /// The attempt that failed.
        attempt: usize,

        /// Data about the operation, serialized as JSON.
        data: serde_json::Value,

        /// How long the operation took before it failed.
        elapsed: Duration,

        /// A message indicating why the operation failed.
        message: String,

        /// Whether this operation is fatal, i.e. will not be retried.
        is_fatal: bool,
    },

    /// Completed the entire operation.
    Completed,
}

impl CompletionEventKind {
    /// Returns the attempt number for this event.
    pub fn attempt(&self) -> Option<usize> {
        match self {
            Self::DownloadCompleted { attempt, .. }
            | Self::DownloadFailed { attempt, .. }
            | Self::FormatFailed { attempt, .. }
            | Self::FormatCompleted { attempt, .. }
            | Self::WriteCompleted { attempt, .. }
            | Self::WriteFailed { attempt, .. }
            | Self::MiscError { attempt, .. } => Some(*attempt),
            Self::Completed => None,
        }
    }

    /// Returns true if the event is a success.
    pub fn is_success(&self) -> bool {
        match self {
            Self::DownloadCompleted { .. }
            | Self::FormatCompleted { .. }
            | Self::WriteCompleted { .. }
            | Self::Completed => true,
            Self::DownloadFailed { .. }
            | Self::FormatFailed { .. }
            | Self::WriteFailed { .. }
            | Self::MiscError { .. } => false,
        }
    }

    /// Returns the peer corresponding to this event.
    ///
    /// Returns None if this is not a download event.
    pub fn peer(&self) -> Option<SocketAddrV6> {
        match self {
            Self::DownloadCompleted { peer, .. }
            | Self::DownloadFailed { peer, .. } => Some(*peer),
            Self::FormatCompleted { .. }
            | Self::FormatFailed { .. }
            | Self::WriteCompleted { .. }
            | Self::WriteFailed { .. }
            | Self::MiscError { .. }
            | Self::Completed => None,
        }
    }
}

/// An individual progress event.
///
/// Forms part of [`ProgressReport`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub struct ProgressEvent {
    /// When this event was generated, in duration since the start of the program.
    ///
    /// This can be compared against [`ProgressReport::total_elapsed`] to figure
    /// out how old the event was.
    pub total_elapsed: Duration,

    /// The kind of event this is.
    pub kind: ProgressEventKind,
}

/// An individual kind of progress event.
///
/// Forms part of [`ProgressEvent`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum ProgressEventKind {
    /// An artifact is being downloaded.
    DownloadProgress {
        /// The download attempt currently occurring.
        attempt: usize,

        /// The kind of artifact being downloaded.
        kind: ArtifactKind,

        /// The peer from which the artifact is being downloaded.
        peer: SocketAddrV6,

        /// The number of bytes downloaded so far.
        downloaded_bytes: u64,

        /// The size of the artifact, as provided in the Content-Length header.
        total_bytes: u64,

        /// How long it's been since the download started.
        elapsed: Duration,
    },

    /// A disk is being formatted.
    FormatProgress {
        /// The format attempt.
        attempt: usize,

        /// The path to the disk.
        #[schemars(schema_with = "path_schema")]
        path: Utf8PathBuf,

        /// Percentage progress.
        percentage: usize,

        /// How long it's been since the format started.
        elapsed: Duration,
    },

    /// An artifact is being written out.
    WriteProgress {
        /// The write attempt currently occurring.
        attempt: usize,

        /// The kind of artifact being written out.
        kind: ArtifactKind,

        /// The destination the artifact is being written out to.
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,

        /// The number of bytes written out.
        written_bytes: u64,

        /// The total number of bytes to write out.
        total_bytes: u64,

        /// How long it's been since the write started.
        elapsed: Duration,
    },
}

fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
